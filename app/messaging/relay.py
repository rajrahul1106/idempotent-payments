"""The outbox relay.

Its whole job: take events the write path committed and get them onto
Kafka, losing none of them. It is allowed to publish the same event twice;
it is never allowed to publish zero times. That asymmetry is the design.
The consumer deduplicates on event_id, which is what lets this process
stay simple enough to reason about.

The loop, per tick:

    BEGIN
      claim up to N unpublished rows, FOR UPDATE SKIP LOCKED, id order
      publish them one at a time, stopping at the first failure
      stamp published_at on the ones that were acknowledged
    COMMIT

A crash anywhere in that block leaves the claimed rows unpublished in the
database (the transaction rolls back), so the next tick picks them up. A
crash after the broker acknowledged but before the commit republishes
those events, which is the duplicate the consumer is built to absorb.

Publishing stops at the first failure rather than skipping past it. Rows
are claimed in id order, which is commit order, so continuing past a
failed row would publish a later event for the same account before an
earlier one and break per-account ordering.
"""

from __future__ import annotations

import json
import logging
import signal
import threading
from dataclasses import dataclass, field

from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.repositories import outbox as outbox_repo
from app.db.session import unit_of_work
from app.messaging import topics
from app.messaging.producer import Producer, PublishError, build_producer

logger = logging.getLogger(__name__)


@dataclass
class RelayResult:
    """What one tick of the relay did."""

    claimed: int = 0
    published: int = 0
    failed_event_id: str | None = None
    error: str | None = None
    event_ids: list[str] = field(default_factory=list)

    @property
    def had_failure(self) -> bool:
        return self.error is not None


def _serialise(event) -> tuple[bytes, dict[str, str]]:
    """The wire format: JSON body, metadata in headers.

    Metadata goes in headers as well as the body so a consumer can route
    or filter without deserialising the payload.
    """
    value = json.dumps(event.payload, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )
    headers = {
        topics.HEADER_EVENT_ID: event.event_id,
        topics.HEADER_EVENT_TYPE: event.event_type,
        topics.HEADER_SCHEMA_VERSION: str(event.schema_version),
    }
    if event.traceparent:
        # Carried across the broker so the consumer's spans join the same
        # trace as the HTTP request that created the payment.
        headers[topics.HEADER_TRACEPARENT] = event.traceparent
    return value, headers


def publish_batch(
    session: Session,
    producer: Producer,
    *,
    limit: int | None = None,
    topic: str = topics.MAIN_TOPIC,
) -> RelayResult:
    """Claim, publish and mark one batch inside the caller's transaction."""
    batch = outbox_repo.claim_batch(
        session, limit if limit is not None else settings.outbox_batch_size
    )
    result = RelayResult(claimed=len(batch))
    if not batch:
        return result

    acknowledged: list[int] = []

    for event in batch:
        value, headers = _serialise(event)
        try:
            producer.publish(
                topic=topic,
                key=event.partition_key,
                value=value,
                headers=headers,
            )
        except PublishError as exc:
            # Leave this row and every row after it unpublished. They are
            # the next tick's first rows, in the same order.
            result.failed_event_id = event.event_id
            result.error = str(exc)
            logger.warning(
                "relay publish failed, stopping batch at event %s: %s",
                event.event_id,
                exc,
            )
            break

        acknowledged.append(event.id)
        result.event_ids.append(event.event_id)

    result.published = outbox_repo.mark_published(session, acknowledged)

    if result.published != len(acknowledged):
        # Should be impossible: the rows are locked by this transaction.
        raise RuntimeError(
            f"marked {result.published} rows but acknowledged {len(acknowledged)}"
        )

    return result


def run_once(producer: Producer, *, limit: int | None = None) -> RelayResult:
    """One tick, in its own transaction."""
    with unit_of_work() as session:
        return publish_batch(session, producer, limit=limit)


def run_forever(
    producer: Producer | None = None,
    stop: threading.Event | None = None,
) -> None:
    """Poll until told to stop.

    Polling, not LISTEN/NOTIFY. A missed notification would strand a row
    indefinitely, so the poll is the correctness guarantee. Phase 7 adds
    NOTIFY on top of it to cut latency, never in place of it.
    """
    producer = producer or build_producer()
    stop = stop or threading.Event()
    interval = settings.outbox_poll_interval_ms / 1000.0
    backoff = interval

    logger.info(
        "relay started: topic=%s batch=%d interval=%dms",
        topics.MAIN_TOPIC,
        settings.outbox_batch_size,
        settings.outbox_poll_interval_ms,
    )

    try:
        while not stop.is_set():
            try:
                result = run_once(producer)
            except Exception:
                # A database problem, not a publish problem. Back off and
                # keep the process alive; the rows are safe where they are.
                logger.exception("relay tick failed")
                stop.wait(min(backoff * 2, 5.0))
                backoff = min(backoff * 2, 5.0)
                continue

            backoff = interval

            if result.published:
                logger.info(
                    "relay published %d/%d event(s)", result.published, result.claimed
                )
            if result.had_failure:
                # Broker trouble. Wait before hammering it again.
                stop.wait(min(interval * 5, 5.0))
            elif result.claimed == 0:
                # Idle: nothing committed since the last tick.
                stop.wait(interval)
    finally:
        producer.close()
        logger.info("relay stopped")


def main() -> None:
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    stop = threading.Event()

    def _handle(signum, _frame) -> None:
        logger.info("signal %s received, draining", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)

    run_forever(stop=stop)


if __name__ == "__main__":
    main()
