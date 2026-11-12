"""The main consumer: each event's side effect applied exactly once.

docs/ARCHITECTURE.md §8, steps 12 and 13. Per message:

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

A failed message never blocks the ones behind it (docs/ARCHITECTURE.md §5.4, decision
D5 in §16). The error decides where it goes:

    permanent       publish to the DLQ, then commit the offset
    transient       publish to the next retry tier, then commit the offset
    infrastructure  publish nowhere, commit nothing; hold the partition and
                    retry the same message until the database is back

Publishing waits for the broker's acknowledgement (acks=all) before the
offset commit (decision D7). A crash between the two leaves the message
both forwarded and still on this topic: a duplicate, which dedup absorbs.
The other order could lose it.

Run with: python -m app.messaging.consumer
"""

from __future__ import annotations

import json
import logging
import re
import signal
import sys
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from opentelemetry.trace import SpanKind, Status, StatusCode

from app.core import metrics
from app.core.config import settings
from app.core.money import MAX_AMOUNT_MINOR
from app.core.telemetry import (
    context_from,
    current_trace_id,
    current_traceparent,
    setup_tracing,
    shutdown_tracing,
    tracer,
)
from app.messaging import topics
from app.messaging.consumer_base import (
    BROKER_RETRY_SECONDS,
    ConsumerLoop,
    ProcessingError,
    linger_before_exit,
)
from app.messaging.failures import (
    EventError,
    FailureKind,
    attempt_count,
    classify,
    failure_headers,
)
from app.messaging.producer import KafkaProducer, Producer, PublishError
from app.services.payment_events import PaymentPosted, apply_payment_posted

logger = logging.getLogger(__name__)
_tracer = tracer(__name__)

EVENT_TYPE = "payment.posted"
SUPPORTED_SCHEMA_VERSIONS = frozenset({1})
CURRENCY_RE = re.compile(r"[A-Z]{3}")


__all__ = [
    "EventError",
    "PaymentEventConsumer",
    "ProcessingError",
    "parse_payment_posted",
]


# --- parsing ---------------------------------------------------------------


def decode_headers(raw: list[tuple[str, bytes]] | None) -> dict[str, str]:
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
    headers = decode_headers(raw_headers)
    event_id = headers.get(topics.HEADER_EVENT_ID)
    if not event_id:
        raise EventError("message has no event_id header")
    try:
        uuid.UUID(event_id)
    except ValueError:
        raise EventError(f"event_id header {event_id!r} is not a UUID") from None
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
    if not 0 < amount_minor <= MAX_AMOUNT_MINOR:
        raise EventError(f"event {event_id}: amount_minor {amount_minor} is out of range")
    currency = _text(payload, "currency", event_id)
    if not CURRENCY_RE.fullmatch(currency):
        raise EventError(f"event {event_id}: currency {currency!r} is not an ISO 4217 code")

    return PaymentPosted(
        event_id=event_id,
        transaction_id=_text(payload, "transaction_id", event_id),
        from_account_id=_text(payload, "from_account_id", event_id),
        to_account_id=_text(payload, "to_account_id", event_id),
        amount_minor=amount_minor,
        currency=currency,
        # Carried through untouched. OpenTelemetry picks it up in Phase 7.
        traceparent=headers.get(topics.HEADER_TRACEPARENT),
    )


# --- the consumer -----------------------------------------------------------


@dataclass
class ConsumerStats:
    applied: int = 0
    skipped: int = 0
    skipped_event_ids: list[str] = field(default_factory=list)
    #: Moved to a retry tier, or to the DLQ, and committed here.
    retried: int = 0
    dead_lettered: int = 0
    #: Times a message was held because the database was unreachable.
    held_for_database: int = 0

    @property
    def handled(self) -> int:
        return self.applied + self.skipped + self.retried + self.dead_lettered


#: Called after the database transaction commits and before the offset
#: commit, with the event and whether it was applied. Tests use it to crash
#: in exactly that window.
AfterDbCommit = Callable[[PaymentPosted, bool], None]

#: Runs inside the event's database transaction, after the dedup insert and
#: before the projection, with the event and its attempt number (1 for a
#: first delivery). It stands for the external call a real side effect
#: would make. Whatever it raises is classified like any other error.
#: Tests inject failures here; production passes nothing.
SideEffect = Callable[[PaymentPosted, int], None]

#: Called after a failed message is acknowledged on its next topic and
#: before this topic's offset commit, with the message and that topic.
AfterForward = Callable[[Any, str], None]


class PaymentEventConsumer(ConsumerLoop):
    client_id = "payments-ledger-consumer"

    def __init__(
        self,
        *,
        ladder: topics.TopicLadder | None = None,
        group_id: str | None = None,
        bootstrap_servers: str | None = None,
        config_overrides: dict[str, Any] | None = None,
        producer: Producer | None = None,
        after_db_commit: AfterDbCommit | None = None,
        side_effect: SideEffect | None = None,
        after_forward: AfterForward | None = None,
        db_backoff: tuple[float, float] | None = None,
    ) -> None:
        self.ladder = ladder or topics.DEFAULT_LADDER
        super().__init__(
            topic=self.ladder.main,
            group_id=group_id or settings.kafka_consumer_group,
            bootstrap_servers=bootstrap_servers,
            config_overrides=config_overrides,
            db_backoff=db_backoff,
        )
        self._producer = producer or KafkaProducer(
            self.bootstrap_servers, client_id=self.client_id
        )
        self._after_db_commit = after_db_commit
        self._side_effect = side_effect
        self._after_forward = after_forward
        self.stats = ConsumerStats()

    def _on_close(self) -> None:
        self._producer.close()
        s = self.stats
        logger.info(
            "consumer stopped: applied=%d skipped=%d retried=%d dead_lettered=%d "
            "held_for_database=%d",
            s.applied, s.skipped, s.retried, s.dead_lettered, s.held_for_database,
        )

    def _handle(self, message) -> bool:
        headers = decode_headers(message.headers())
        attempts = attempt_count(headers)
        # A child of the traceparent the message carries: the relay's publish
        # span on a first delivery, a retry tier's span after that, an
        # operator replay's span on a replay. One payment stays one trace.
        with _tracer.start_as_current_span(
            "payment.posted process",
            context=context_from(headers.get(topics.HEADER_TRACEPARENT)),
            kind=SpanKind.CONSUMER,
            attributes={
                "event_id": headers.get(topics.HEADER_EVENT_ID, ""),
                "attempt_count": attempts,
                "messaging.system": "kafka",
                "messaging.source.name": message.topic(),
                "messaging.kafka.partition": message.partition(),
                "messaging.kafka.offset": message.offset(),
            },
        ) as span:
            return self._process(message, headers, attempts, span)

    def _process(self, message, headers: dict[str, str], attempts: int, span) -> bool:
        try:
            event = parse_payment_posted(message.headers(), message.value())
            span.set_attribute("account_id", event.from_account_id)
            effect = None
            if self._side_effect is not None:
                side_effect = self._side_effect

                def effect() -> None:
                    side_effect(event, attempts + 1)

            applied = apply_payment_posted(event, self.group_id, before_effect=effect)
        except Exception as exc:
            kind = classify(exc)
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, kind.value))
            span.set_attribute("failure.kind", kind.value)
            if kind is FailureKind.INFRASTRUCTURE:
                span.set_attribute("outcome", "held_for_database")
                self.stats.held_for_database += 1
                metrics.EVENTS_PROCESSED.labels(topic=message.topic(), outcome="held").inc()
                self._hold_for_database(message, exc)
                return False
            return self._forward(message, headers, kind, exc, attempts, span)

        self._database_ok(message)
        # The database transaction has committed. Only now may the offset move.
        if self._after_db_commit is not None:
            self._after_db_commit(event, applied)
        self._commit(message)

        span.set_attribute("outcome", "applied" if applied else "skipped")
        metrics.EVENTS_PROCESSED.labels(
            topic=message.topic(), outcome="applied" if applied else "skipped"
        ).inc()
        if applied:
            self.stats.applied += 1
        else:
            self.stats.skipped += 1
            self.stats.skipped_event_ids.append(event.event_id)
            logger.info(
                "skipped already-processed event %s at %s", event.event_id, self.where(message)
            )
        return True

    def _forward(
        self,
        message,
        headers: dict[str, str],
        kind: FailureKind,
        exc: Exception,
        attempts: int,
        span,
    ) -> bool:
        """Decision D7: acknowledged on the next topic first, offset second."""
        self._database_ok(message)  # the database answered; it refused this message
        target = (
            self.ladder.dlq if kind is FailureKind.PERMANENT else self.ladder.next_topic(attempts)
        )
        forwarded = failure_headers(
            headers, kind=kind, exc=exc, source_topic=message.topic(), destination=target
        )
        # The next hop's span is a child of this attempt's span.
        traceparent = current_traceparent()
        if traceparent:
            forwarded[topics.HEADER_TRACEPARENT] = traceparent
        to_dlq = target == self.ladder.dlq
        span.set_attribute("outcome", "dead_lettered" if to_dlq else "retried")
        span.set_attribute("forwarded_to", target)
        logger.log(
            logging.ERROR if to_dlq else logging.WARNING,
            "event %s at %s failed on attempt %d (%s), moving it to %s: %s "
            "(trace_id=%s)",
            headers.get(topics.HEADER_EVENT_ID),
            self.where(message),
            attempts + 1,
            kind.value,
            target,
            forwarded[topics.HEADER_LAST_ERROR],
            current_trace_id() or None,
        )
        try:
            self._producer.publish(target, message.key(), message.value(), forwarded)
        except PublishError as publish_error:
            # Not acknowledged, so the offset must not move. Hold and retry.
            logger.error(
                "could not move %s to %s, holding it: %s",
                self.where(message),
                target,
                publish_error,
            )
            span.set_attribute("outcome", "held_for_broker")
            metrics.EVENTS_PROCESSED.labels(topic=message.topic(), outcome="held").inc()
            self._hold_for(message, BROKER_RETRY_SECONDS)
            return False

        if self._after_forward is not None:
            self._after_forward(message, target)
        self._commit(message)
        metrics.EVENTS_PROCESSED.labels(
            topic=message.topic(), outcome="dead_lettered" if to_dlq else "retried"
        ).inc()
        if to_dlq:
            self.stats.dead_lettered += 1
        else:
            self.stats.retried += 1
        return True


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

    setup_tracing("consumer")
    metrics.start_metrics_server("consumer")
    try:
        PaymentEventConsumer().run(stop)
    except ProcessingError:
        logger.exception("consumer cannot continue")
        linger_before_exit(stop)
        return 1
    finally:
        shutdown_tracing()
    return 0


if __name__ == "__main__":
    sys.exit(main())
