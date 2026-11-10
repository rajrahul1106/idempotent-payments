"""The main consumer: each event's side effect applied exactly once.

Blueprint section 8, steps 12 and 13. Per message:

    parse and validate the event (event_id header must match the payload)
    BEGIN
      INSERT processed_events (event_id, consumer_group)  -- conflict: skip
      INSERT payment_events (the side effect)
    COMMIT
    commit the Kafka offset, synchronously

The offset moves only after the database commit. A crash before the
database commit leaves nothing behind and the message is redelivered. A
crash after it but before the offset commit redelivers a message whose
processed_events row already exists, so it is skipped. Either way the
effect happens once. Delivery is at least once; processing is exactly once.

On a processing error, Phase 5 logs at ERROR, does not commit the offset
and stops. Phase 6 replaces that with the retry topics and the DLQ.

Run with: python -m app.messaging.consumer
"""

from __future__ import annotations

import json
import logging
import signal
import sys
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from app.core.config import settings
from app.messaging import topics
from app.services.payment_events import PaymentPosted, apply_payment_posted

logger = logging.getLogger(__name__)

EVENT_TYPE = "payment.posted"
SUPPORTED_SCHEMA_VERSIONS = frozenset({1})


class EventError(ValueError):
    """The message cannot be processed as it stands."""


class ProcessingError(RuntimeError):
    """A message failed. Its offset was not committed and the consumer stopped."""


# --- parsing ---------------------------------------------------------------


def _decode_headers(raw: list[tuple[str, bytes]] | None) -> dict[str, str]:
    headers: dict[str, str] = {}
    for key, value in raw or []:
        headers[key] = value.decode("utf-8") if isinstance(value, bytes) else value
    return headers


def _text(payload: dict[str, Any], name: str, event_id: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value:
        raise EventError(f"event {event_id}: {name} must be a non-empty string")
    return value


def parse_payment_posted(
    raw_headers: list[tuple[str, bytes]] | None, value: bytes | None
) -> PaymentPosted:
    """Validate one message and return the event it carries."""
    headers = _decode_headers(raw_headers)
    event_id = headers.get(topics.HEADER_EVENT_ID)
    if not event_id:
        raise EventError("message has no event_id header")
    if value is None:
        raise EventError(f"event {event_id}: message has no body")
    try:
        # parse_float=Decimal: a binary float never enters the money path,
        # even when a producer sends one. The type checks below reject it.
        payload = json.loads(value, parse_float=Decimal)
    except (ValueError, UnicodeDecodeError) as exc:
        raise EventError(f"event {event_id}: body is not JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise EventError(f"event {event_id}: body is not a JSON object")

    if payload.get("event_id") != event_id:
        raise EventError(
            f"header event_id {event_id} does not match payload event_id "
            f"{payload.get('event_id')!r}"
        )
    version = payload.get("schema_version")
    if type(version) is not int or version not in SUPPORTED_SCHEMA_VERSIONS:
        raise EventError(f"event {event_id}: unknown schema_version {version!r}")
    header_version = headers.get(topics.HEADER_SCHEMA_VERSION)
    if header_version is not None and header_version != str(version):
        raise EventError(
            f"event {event_id}: header schema_version {header_version} does not "
            f"match payload schema_version {version}"
        )
    if payload.get("event_type") != EVENT_TYPE:
        raise EventError(f"event {event_id}: unknown event_type {payload.get('event_type')!r}")

    amount_minor = payload.get("amount_minor")
    if type(amount_minor) is not int:
        raise EventError(f"event {event_id}: amount_minor must be an integer")

    return PaymentPosted(
        event_id=event_id,
        transaction_id=_text(payload, "transaction_id", event_id),
        from_account_id=_text(payload, "from_account_id", event_id),
        to_account_id=_text(payload, "to_account_id", event_id),
        amount_minor=amount_minor,
        currency=_text(payload, "currency", event_id),
        # Carried through untouched. OpenTelemetry picks it up in Phase 7.
        traceparent=headers.get(topics.HEADER_TRACEPARENT),
    )


# --- the consumer -----------------------------------------------------------


@dataclass
class ConsumerStats:
    applied: int = 0
    skipped: int = 0
    skipped_event_ids: list[str] = field(default_factory=list)

    @property
    def handled(self) -> int:
        return self.applied + self.skipped


#: Called after the database transaction commits and before the offset
#: commit, with the event and whether it was applied. Tests use it to crash
#: in exactly that window.
AfterDbCommit = Callable[[PaymentPosted, bool], None]


class PaymentEventConsumer:
    def __init__(
        self,
        *,
        topic: str | None = None,
        group_id: str | None = None,
        bootstrap_servers: str | None = None,
        config_overrides: dict[str, Any] | None = None,
        after_db_commit: AfterDbCommit | None = None,
    ) -> None:
        from confluent_kafka import Consumer

        self.topic = topic or topics.MAIN_TOPIC
        self.group_id = group_id or settings.kafka_consumer_group
        config: dict[str, Any] = {
            "bootstrap.servers": bootstrap_servers or settings.kafka_bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "client.id": "payments-ledger-consumer",
            **(config_overrides or {}),
            # Correctness settings, applied last so no override can undo
            # them: offsets move only when this module commits them.
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
        self._consumer = Consumer(config)
        self._after_db_commit = after_db_commit
        self.stats = ConsumerStats()
        self.assignment: set[int] = set()

    # Rebalances. The loop commits each message's offset synchronously
    # before it polls again, and these callbacks only run inside poll, so a
    # revoked partition never has a handled-but-uncommitted message.
    def _on_assign(self, _consumer, partitions) -> None:
        self.assignment |= {p.partition for p in partitions}
        logger.info("assigned %s partitions %s", self.topic, sorted(self.assignment))

    def _on_revoke(self, _consumer, partitions) -> None:
        self.assignment -= {p.partition for p in partitions}
        logger.info("revoked %s partitions %s", self.topic, sorted(p.partition for p in partitions))

    def run(
        self,
        stop: threading.Event | None = None,
        *,
        max_messages: int | None = None,
    ) -> ConsumerStats:
        """Consume until stopped. Raises ProcessingError on a failed message."""
        from confluent_kafka import KafkaError

        stop = stop or threading.Event()
        self._consumer.subscribe(
            [self.topic],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_revoke,
        )
        logger.info("consumer started: topic=%s group=%s", self.topic, self.group_id)
        try:
            while not stop.is_set():
                if max_messages is not None and self.stats.handled >= max_messages:
                    break
                message = self._consumer.poll(0.5)
                if message is None:
                    continue
                error = message.error()
                if error is not None:
                    if error.code() == KafkaError._PARTITION_EOF:
                        continue
                    if error.fatal():
                        raise ProcessingError(f"fatal consumer error: {error}")
                    logger.warning("consumer error: %s", error)
                    continue
                self._handle(message)
        finally:
            # Leaves the group so the partitions move promptly. With auto
            # commit off, closing commits nothing.
            self._consumer.close()
            logger.info(
                "consumer stopped: applied=%d skipped=%d",
                self.stats.applied,
                self.stats.skipped,
            )
        return self.stats

    def _handle(self, message) -> None:
        from confluent_kafka import KafkaException

        where = f"{message.topic()}[{message.partition()}]@{message.offset()}"
        try:
            event = parse_payment_posted(message.headers(), message.value())
            applied = apply_payment_posted(event, self.group_id)
        except Exception as exc:
            headers = _decode_headers(message.headers())
            logger.error(
                "could not process %s (event_id=%s traceparent=%s): %s. "
                "Offset not committed; stopping.",
                where,
                headers.get(topics.HEADER_EVENT_ID),
                headers.get(topics.HEADER_TRACEPARENT),
                exc,
            )
            raise ProcessingError(f"could not process {where}") from exc

        # The database transaction has committed. Only now may the offset move.
        if self._after_db_commit is not None:
            self._after_db_commit(event, applied)
        try:
            self._consumer.commit(message=message, asynchronous=False)
        except KafkaException as exc:
            # Typically the partition was revoked mid-rebalance. Safe: the
            # message will be redelivered and skipped as a duplicate.
            logger.warning("offset commit failed for %s, will be redelivered: %s", where, exc)

        if applied:
            self.stats.applied += 1
        else:
            self.stats.skipped += 1
            self.stats.skipped_event_ids.append(event.event_id)
            logger.info("skipped already-processed event %s at %s", event.event_id, where)


def main() -> int:
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    stop = threading.Event()

    def _handle_signal(signum, _frame) -> None:
        logger.info("signal %s received, finishing the current message", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        PaymentEventConsumer().run(stop)
    except ProcessingError:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
