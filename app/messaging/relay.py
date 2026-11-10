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
are claimed in id order, which for any one account is commit order (see
outbox.claim_batch), so continuing past a failed row would publish a later
event for the same account before an earlier one and break per-account
ordering.

Exactly one relay instance runs (blueprint decision D1), and the database
enforces it: run_forever publishes only while it holds a Postgres advisory
lock. A second relay logs that the lock is taken and waits, and takes over
only when the first one's connection closes. The relay holds the claimed
outbox rows' locks while it publishes. Those locks never block
a payment, which only inserts new outbox rows, and each publish is bounded
by the producer's 10-second flush.
"""

from __future__ import annotations

import json
import logging
import signal
import threading
from dataclasses import dataclass, field

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.repositories import outbox as outbox_repo
from app.db.session import engine, unit_of_work
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


# One relay per database (decision D1). Any fixed value works, as long as
# every relay uses the same one; advisory locks are scoped to a database.
RELAY_LOCK_ID = 506_032_705
LOCK_RETRY_SECONDS = 2.0

_LOCK_HELD = text(
    "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' "
    "AND pid = pg_backend_pid() AND objid = :id AND granted"
)


def _acquire_relay_lock(stop: threading.Event, retry_seconds: float) -> Connection | None:
    """Block until this process holds the relay lock, or until told to stop.

    The lock is session-level, held by a dedicated autocommit connection
    for as long as this relay publishes. Closing that connection, or the
    process dying, releases it.
    """
    announced = False
    while not stop.is_set():
        conn: Connection | None = None
        try:
            conn = engine.connect().execution_options(isolation_level="AUTOCOMMIT")
            acquired = conn.execute(
                text("SELECT pg_try_advisory_lock(:id)"), {"id": RELAY_LOCK_ID}
            ).scalar_one()
        except Exception:
            logger.exception("relay could not reach the database to take its lock")
            acquired = False
        if acquired:
            logger.info("relay lock %d acquired; this relay publishes", RELAY_LOCK_ID)
            return conn
        if conn is not None:
            conn.close()
        if not announced:
            logger.warning(
                "another relay holds the relay lock %d; waiting, not publishing",
                RELAY_LOCK_ID,
            )
            announced = True
        stop.wait(retry_seconds)
    return None


def _still_holds(lock: Connection) -> bool:
    try:
        return bool(lock.execute(_LOCK_HELD, {"id": RELAY_LOCK_ID}).scalar_one())
    except Exception:
        logger.exception("relay lock connection failed")
        return False


def _publish_while_locked(
    lock: Connection, producer: Producer, stop: threading.Event
) -> None:
    interval = settings.outbox_poll_interval_ms / 1000.0
    backoff = interval
    while not stop.is_set():
        if not _still_holds(lock):
            # The lock went with its connection, so another relay may now
            # hold it. Stop publishing before anything else happens.
            logger.error("relay lost lock %d; stopped publishing", RELAY_LOCK_ID)
            return
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


def run_forever(
    producer: Producer | None = None,
    stop: threading.Event | None = None,
    *,
    lock_retry_seconds: float = LOCK_RETRY_SECONDS,
) -> None:
    """Poll until told to stop, publishing only while holding the relay lock.

    Polling, not LISTEN/NOTIFY. A missed notification would strand a row
    indefinitely, so the poll is the correctness guarantee. Phase 7 adds
    NOTIFY on top of it to cut latency, never in place of it.

    run_once does not take the lock; it is the unit the tests drive.
    """
    stop = stop or threading.Event()
    logger.info(
        "relay started: topic=%s batch=%d interval=%dms",
        topics.MAIN_TOPIC,
        settings.outbox_batch_size,
        settings.outbox_poll_interval_ms,
    )
    try:
        while not stop.is_set():
            lock = _acquire_relay_lock(stop, lock_retry_seconds)
            if lock is None:
                break
            try:
                # Built only once this relay is the one publishing.
                producer = producer or build_producer()
                _publish_while_locked(lock, producer, stop)
            finally:
                lock.close()  # releases the advisory lock
    finally:
        if producer is not None:
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
