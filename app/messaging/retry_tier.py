"""One retry tier: hold each message until it is old enough, then send it
back to the main topic (blueprint §5.4).

A message is due `delay` seconds after it was written to this tier, by its
Kafka timestamp. Messages in one partition arrive in that order, so when
the head of a partition is not due, nothing behind it is either: the
partition is held (paused and rewound, see consumer_base) until the head is
due. The loop keeps polling while it waits, so even the 10-minute tier
never exceeds max.poll.interval.ms and never causes a rebalance.

A due message goes back to the main topic with its key, value and headers
unchanged, except traceparent, which becomes this tier's forward span. The main consumer reads attempt_count from those headers to pick
the next tier. The republish is acknowledged (acks=all) before this tier's
offset is committed (decision D7). A crash between the two republishes it
again on restart, and the main consumer's dedup absorbs the copy.

Run with: python -m app.messaging.retry_tier <tier index or topic name>
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from opentelemetry.trace import SpanKind, Status, StatusCode

from app.core.config import settings
from app.core.telemetry import (
    context_from,
    current_traceparent,
    setup_tracing,
    shutdown_tracing,
    tracer,
)
from app.messaging import topics
from app.messaging.consumer_base import BROKER_RETRY_SECONDS, ConsumerLoop, ProcessingError
from app.messaging.consumer import decode_headers
from app.messaging.failures import attempt_count
from app.messaging.producer import KafkaProducer, Producer, PublishError

logger = logging.getLogger(__name__)
_tracer = tracer(__name__)


@dataclass
class TierStats:
    forwarded: int = 0
    forwarded_event_ids: list[str] = field(default_factory=list)
    #: Times a partition was held because its head was not yet due.
    waits: int = 0


#: Called after the main topic acknowledged a republish and before this
#: tier's offset commit. Tests use it to crash in exactly that window.
AfterForward = Callable[[Any], None]


class RetryTierConsumer(ConsumerLoop):
    client_id = "payments-retry-tier"

    def __init__(
        self,
        retry_topic: str,
        *,
        ladder: topics.TopicLadder | None = None,
        group_id: str | None = None,
        bootstrap_servers: str | None = None,
        config_overrides: dict[str, Any] | None = None,
        producer: Producer | None = None,
        after_forward: AfterForward | None = None,
    ) -> None:
        self.ladder = ladder or topics.DEFAULT_LADDER
        if retry_topic not in self.ladder.retries:
            raise ValueError(f"{retry_topic} is not a retry tier of {self.ladder.main}")
        self.delay = self.ladder.delay_of(retry_topic)
        super().__init__(
            topic=retry_topic,
            group_id=group_id or f"{settings.kafka_consumer_group}-retry-{retry_topic.rsplit('.', 1)[-1]}",
            bootstrap_servers=bootstrap_servers,
            config_overrides=config_overrides,
        )
        self._producer = producer or KafkaProducer(self.bootstrap_servers, client_id=self.client_id)
        self._after_forward = after_forward
        self.stats = TierStats()

    def _on_close(self) -> None:
        self._producer.close()
        logger.info(
            "retry tier %s stopped: forwarded=%d waits=%d",
            self.topic,
            self.stats.forwarded,
            self.stats.waits,
        )

    def _due_in(self, message) -> float:
        """Seconds until this message may go back to the main topic."""
        from confluent_kafka import TIMESTAMP_NOT_AVAILABLE

        kind, millis = message.timestamp()
        if kind == TIMESTAMP_NOT_AVAILABLE:
            # Never seen in practice: our producers always stamp. Waiting the
            # full delay from now is the safe reading.
            return self.delay
        return millis / 1000 + self.delay - time.time()

    def _handle(self, message) -> bool:
        wait = self._due_in(message)
        if wait > 0:
            self.stats.waits += 1
            logger.info(
                "%s not due for %.1fs; holding partition %d",
                self.where(message),
                wait,
                message.partition(),
            )
            self._hold_for(message, wait)
            return False

        headers = decode_headers(message.headers())
        # One span per forward, not per wait: a held message is re-read many
        # times before it is due, and only the forward is an event.
        with _tracer.start_as_current_span(
            "retry forward",
            context=context_from(headers.get(topics.HEADER_TRACEPARENT)),
            kind=SpanKind.PRODUCER,
            attributes={
                "event_id": headers.get(topics.HEADER_EVENT_ID, ""),
                "attempt_count": attempt_count(headers),
                "retry.topic": self.topic,
                "retry.delay_seconds": self.delay,
                "messaging.system": "kafka",
                "messaging.destination.name": self.ladder.main,
            },
        ) as span:
            traceparent = current_traceparent()
            if traceparent:
                headers[topics.HEADER_TRACEPARENT] = traceparent
            try:
                self._producer.publish(self.ladder.main, message.key(), message.value(), headers)
            except PublishError as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, "not acknowledged"))
                logger.error(
                    "could not return %s to %s, holding it: %s",
                    self.where(message),
                    self.ladder.main,
                    exc,
                )
                self._hold_for(message, BROKER_RETRY_SECONDS)
                return False

        if self._after_forward is not None:
            self._after_forward(message)
        self._commit(message)
        event_id = headers.get(topics.HEADER_EVENT_ID, "")
        self.stats.forwarded += 1
        self.stats.forwarded_event_ids.append(event_id)
        logger.info(
            "returned event %s (attempt_count=%s) from %s to %s",
            event_id,
            headers.get(topics.HEADER_ATTEMPT_COUNT),
            self.where(message),
            self.ladder.main,
        )
        return True


def _tier_topic(arg: str) -> str:
    if arg in topics.RETRY_TOPICS:
        return arg
    try:
        return topics.RETRY_TOPICS[int(arg)]
    except (ValueError, IndexError):
        raise SystemExit(
            f"unknown retry tier {arg!r}; expected an index or one of {topics.RETRY_TOPICS}"
        ) from None


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        raise SystemExit("usage: python -m app.messaging.retry_tier <tier index or topic>")
    stop = threading.Event()

    def _handle_signal(signum, _frame) -> None:
        logger.info("signal %s received, stopping", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    topic = _tier_topic(args[0])
    # One service per tier: retry-5s, retry-1m, retry-10m.
    setup_tracing(f"retry-{topic.rsplit('.', 1)[-1]}")
    try:
        RetryTierConsumer(topic).run(stop)
    except ProcessingError:
        logger.exception("retry tier cannot continue")
        return 1
    finally:
        shutdown_tracing()
    return 0


if __name__ == "__main__":
    sys.exit(main())
