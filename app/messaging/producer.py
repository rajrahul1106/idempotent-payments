"""The Kafka producer wrapper.

Two things matter here and nothing else does.

First, a publish is not complete until the broker has replicated it. The
relay marks an outbox row published only after this module confirms the
write, so `acks=all` and a synchronous flush are correctness settings, not
tuning knobs.

Second, order within a partition must survive a retry. With
`enable.idempotence=True` librdkafka keeps one in-flight batch per
partition and the broker drops producer-level duplicates, so a retried
send cannot reorder two events for the same account.

The `Producer` protocol exists so the relay can be tested against a
recording double. Only this module knows that Kafka is Kafka.
"""

from __future__ import annotations

import logging
from typing import Protocol, runtime_checkable

from app.core.config import settings

logger = logging.getLogger(__name__)


class PublishError(RuntimeError):
    """A message was not acknowledged by the broker.

    The relay treats this as "leave the row unpublished and try again".
    """


@runtime_checkable
class Producer(Protocol):
    """What the relay needs from a broker client."""

    def publish(
        self,
        topic: str,
        key: str | bytes | None,
        value: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Send one message and return only once it is acknowledged.

        Raises PublishError if the broker did not acknowledge it.
        """

    def flush(self, timeout_seconds: float = 10.0) -> int:
        """Block until the queue drains. Returns messages still in flight."""

    def close(self) -> None:
        """Release the connection."""


class KafkaProducer:
    """Production implementation, backed by confluent-kafka (librdkafka)."""

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        *,
        client_id: str = "payments-outbox-relay",
    ) -> None:
        # Imported lazily so that unit tests and the API process do not
        # need the native library present.
        from confluent_kafka import Producer as ConfluentProducer

        self._config = {
            "bootstrap.servers": bootstrap_servers
            or settings.kafka_bootstrap_servers,
            # Correctness settings. Do not relax these for throughput.
            "acks": "all",
            "enable.idempotence": True,
            "max.in.flight.requests.per.connection": 5,
            "retries": 10,
            "delivery.timeout.ms": 30_000,
            # Keep batching small; the relay already batches at the database
            # level and latency matters more than compression here.
            "linger.ms": 5,
            "compression.type": "lz4",
            "client.id": client_id,
        }
        self._producer = ConfluentProducer(self._config)
        self._closed = False

    def publish(
        self,
        topic: str,
        key: str | bytes | None,
        value: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        from confluent_kafka import KafkaException

        result: dict[str, object] = {}

        def _on_delivery(err: object, msg: object) -> None:
            result["err"] = err
            result["msg"] = msg

        try:
            self._producer.produce(
                topic=topic,
                # The retry path forwards the original key bytes unchanged.
                key=key.encode("utf-8") if isinstance(key, str) else key,
                value=value,
                headers=[(k, v.encode("utf-8")) for k, v in (headers or {}).items()],
                on_delivery=_on_delivery,
            )
        except BufferError as exc:
            # Local queue full: flush and let the caller retry the row.
            self._producer.flush(5.0)
            raise PublishError(f"producer queue full for {topic}") from exc
        except KafkaException as exc:
            raise PublishError(f"produce failed for {topic}: {exc}") from exc

        # Synchronous by design: the relay must not mark a row published
        # before the broker has it.
        remaining = self._producer.flush(10.0)
        if remaining:
            raise PublishError(
                f"{remaining} message(s) unacknowledged after flush on {topic}"
            )
        if result.get("err") is not None:
            raise PublishError(f"delivery failed on {topic}: {result['err']}")

    def flush(self, timeout_seconds: float = 10.0) -> int:
        return self._producer.flush(timeout_seconds)

    def close(self) -> None:
        """Flush what is queued, then release the broker connections.

        Idempotent: a second call does nothing. The producer cannot publish
        after this.
        """
        if self._closed:
            return
        self._closed = True
        remaining = self._producer.flush(10.0)
        if remaining:
            logger.warning("%d message(s) unacknowledged when the producer closed", remaining)
        self._producer.close()


def build_producer() -> Producer:
    """The producer the relay uses when it runs for real."""
    return KafkaProducer()
