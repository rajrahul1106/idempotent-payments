"""The poll loop every consumer in this system shares.

Three things live here so the main consumer, the retry tiers and the DLQ
writer cannot each get them slightly wrong:

1. Offsets move only when a subclass commits them. Auto commit and auto
   offset store are forced off, after any override.

2. Waiting never blocks the poll loop. A consumer that sleeps longer than
   max.poll.interval.ms is thrown out of its group, which rebalances every
   partition. So a message that must wait (a retry tier's delay, or a
   database outage) is *held*: its partition is paused and rewound to the
   message's offset, and the loop keeps polling, at most POLL_SECONDS at a
   time, until the hold is due. Then the partition resumes and the same
   message is fetched again. Other partitions keep flowing meanwhile.

3. A rebalance drops the holds on revoked partitions. The new owner starts
   from the last committed offset, which is still the held message.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any

from app.core import metrics
from app.core.config import settings

logger = logging.getLogger(__name__)

#: The longest the loop blocks in one poll. Well under any session timeout.
POLL_SECONDS = 0.5

#: How long to hold a message when the broker would not take a forward.
BROKER_RETRY_SECONDS = 1.0


class ProcessingError(RuntimeError):
    """A consumer cannot continue. Its current offset was not committed."""


@dataclass
class RebalanceLog:
    assigned: int = 0
    revoked: int = 0
    lost: int = 0


class PartitionHolds:
    """Partitions paused at a message's offset, and when each may resume."""

    def __init__(self, consumer) -> None:
        self._consumer = consumer
        self._until: dict[tuple[str, int], float] = {}

    def hold(self, message, until: float) -> None:
        """Pause the message's partition and rewind it to the message.

        `until` is a time.monotonic() value. Anything already fetched past
        this message is discarded by the loop (see is_held) and fetched
        again after the resume.
        """
        from confluent_kafka import TopicPartition

        tp = TopicPartition(message.topic(), message.partition(), message.offset())
        self._consumer.pause([tp])
        self._consumer.seek(tp)
        self._until[(message.topic(), message.partition())] = until

    def is_held(self, message) -> bool:
        return (message.topic(), message.partition()) in self._until

    def release_due(self, now: float) -> None:
        from confluent_kafka import TopicPartition

        due = [key for key, until in self._until.items() if until <= now]
        if due:
            self._consumer.resume([TopicPartition(t, p) for t, p in due])
            for key in due:
                del self._until[key]

    def poll_timeout(self, now: float, ceiling: float = POLL_SECONDS) -> float:
        if not self._until:
            return ceiling
        return max(0.0, min(ceiling, min(self._until.values()) - now))

    def forget(self, partitions) -> None:
        for tp in partitions:
            self._until.pop((tp.topic, tp.partition), None)

    def __len__(self) -> int:
        return len(self._until)


class ConsumerLoop:
    """Subscribe, poll, hand each message to `_handle`, close.

    `_handle` returns True when it finished the message (its offset is
    committed, or it deliberately moved on) and False when it held it.
    """

    client_id = "payments-consumer"

    def __init__(
        self,
        *,
        topic: str,
        group_id: str,
        bootstrap_servers: str | None = None,
        config_overrides: dict[str, Any] | None = None,
        db_backoff: tuple[float, float] | None = None,
    ) -> None:
        from confluent_kafka import Consumer

        self.topic = topic
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        config: dict[str, Any] = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "client.id": self.client_id,
            **(config_overrides or {}),
            # Correctness settings, applied last so no override can undo
            # them: offsets move only when a subclass commits them.
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
        self._consumer = Consumer(config)
        self.holds = PartitionHolds(self._consumer)
        self.assignment: set[int] = set()
        self.rebalances = RebalanceLog()
        self.finished = 0
        initial, ceiling = db_backoff or (
            settings.consumer_db_backoff_initial_ms / 1000,
            settings.consumer_db_backoff_max_ms / 1000,
        )
        self._db_backoff_initial = initial
        self._db_backoff_max = ceiling
        self._db_backoff: dict[tuple[str, int], float] = {}

    # --- subclass hooks ------------------------------------------------------

    def _handle(self, message) -> bool:  # pragma: no cover - abstract
        raise NotImplementedError

    def _on_close(self) -> None:
        """Called once, after the consumer has left its group."""

    # --- rebalances ------------------------------------------------------------
    # Every _handle either commits its message's offset synchronously or
    # holds it before the next poll, and these callbacks run only inside
    # poll. So a revoked partition never has a finished-but-uncommitted
    # message, and a held one is simply re-read by its next owner.

    def _on_assign(self, _consumer, partitions) -> None:
        self.rebalances.assigned += 1
        self.assignment |= {p.partition for p in partitions}
        logger.info("assigned %s partitions %s", self.topic, sorted(self.assignment))

    def _on_revoke(self, _consumer, partitions) -> None:
        self.rebalances.revoked += 1
        self._drop(partitions)
        logger.info("revoked %s partitions %s", self.topic, sorted(p.partition for p in partitions))

    def _on_lost(self, _consumer, partitions) -> None:
        self.rebalances.lost += 1
        self._drop(partitions)
        logger.warning("lost %s partitions %s", self.topic, sorted(p.partition for p in partitions))

    def _drop(self, partitions) -> None:
        self.assignment -= {p.partition for p in partitions}
        self.holds.forget(partitions)
        for tp in partitions:
            self._db_backoff.pop((tp.topic, tp.partition), None)

    # --- the loop ----------------------------------------------------------------

    def run(self, stop: threading.Event | None = None, *, max_messages: int | None = None):
        from confluent_kafka import KafkaError

        stop = stop or threading.Event()
        self._consumer.subscribe(
            [self.topic],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
        )
        logger.info("%s started: topic=%s group=%s", type(self).__name__, self.topic, self.group_id)
        try:
            while not stop.is_set():
                metrics.heartbeat()
                if max_messages is not None and self.finished >= max_messages:
                    break
                self.holds.release_due(time.monotonic())
                message = self._consumer.poll(self.holds.poll_timeout(time.monotonic()))
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
                if self.holds.is_held(message):
                    # Fetched before its partition was paused. The partition
                    # was rewound, so this message comes round again.
                    continue
                if self._handle(message):
                    self.finished += 1
        finally:
            # Leaves the group so the partitions move promptly. With auto
            # commit off, closing commits nothing.
            self._consumer.close()
            self._on_close()
        return self

    # --- helpers for subclasses ------------------------------------------------------

    @staticmethod
    def where(message) -> str:
        return f"{message.topic()}[{message.partition()}]@{message.offset()}"

    def _commit(self, message) -> None:
        from confluent_kafka import KafkaException

        try:
            self._consumer.commit(message=message, asynchronous=False)
        except KafkaException as exc:
            # Typically the partition was revoked mid-rebalance. Safe: the
            # message is redelivered, and every consumer here dedups.
            logger.warning(
                "offset commit failed for %s, will be redelivered: %s", self.where(message), exc
            )

    def _hold_for(self, message, seconds: float) -> None:
        self.holds.hold(message, time.monotonic() + seconds)

    def _hold_for_database(self, message, exc: BaseException) -> None:
        """Decision D5: the database is down. Keep the message, retry it later."""
        key = (message.topic(), message.partition())
        previous = self._db_backoff.get(key)
        delay = self._db_backoff_initial if previous is None else min(previous * 2, self._db_backoff_max)
        self._db_backoff[key] = delay
        logger.warning(
            "database unavailable at %s, offset not committed, retrying the same "
            "message in %.1fs: %s",
            self.where(message),
            delay,
            " ".join(str(exc).split())[:300],
        )
        self._hold_for(message, delay)

    def _database_ok(self, message) -> None:
        if self._db_backoff.pop((message.topic(), message.partition()), None) is not None:
            logger.info("database reachable again at %s, resuming", self.where(message))


def linger_before_exit(stop: threading.Event) -> None:
    """After a fatal error, keep /metrics up briefly before exiting.

    A worker that dies within a second of starting is never scraped, so a
    crash loop would look like a target that is merely down. Lingering long
    enough for a scrape makes each restart visible to
    changes(process_start_time_seconds[10m]). SIGTERM still ends it at once.
    """
    seconds = settings.fatal_exit_linger_seconds
    logger.error("exiting in %ds (kept up so this failure is scraped)", seconds)
    stop.wait(seconds)
