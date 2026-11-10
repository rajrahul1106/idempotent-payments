"""Create the topics the system owns, with the partition count it relies on.

Blueprint section 5.3: payments.main has 6 partitions, and that number
cannot change without a rebalance plan, because more partitions remap keys
and break per-account ordering. So this module never alters a topic. It
creates what is missing and refuses to continue when a topic exists with a
different partition count. Running it twice is a no-op.

Run once before the relay and the consumer start:

    python -m app.messaging.topic_setup
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Iterable

from app.core.config import settings
from app.messaging import topics

logger = logging.getLogger(__name__)

# Blueprint section 9: retention is 7 days, so a day of events can be
# replayed by resetting a consumer group.
RETENTION_MS = 7 * 24 * 60 * 60 * 1000


class TopicMismatch(RuntimeError):
    """A topic exists with a partition count the system does not expect."""


def _check_partitions(name: str, actual: int, expected: int) -> None:
    if actual != expected:
        raise TopicMismatch(
            f"topic {name} has {actual} partition(s), expected {expected}. "
            "Changing it remaps keys to partitions and breaks per-account "
            "ordering, so this needs a rebalance plan, not an automatic fix."
        )


def ensure_topics(
    names: Iterable[str],
    *,
    partitions: int,
    bootstrap_servers: str | None = None,
    replication_factor: int = 1,
    timeout_seconds: float = 30.0,
) -> dict[str, str]:
    """Create each missing topic. Returns {topic: "created" | "exists"}.

    Raises TopicMismatch when an existing topic has a different partition
    count, and KafkaException when the broker cannot be reached.
    """
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient(
        {"bootstrap.servers": bootstrap_servers or settings.kafka_bootstrap_servers}
    )
    existing = admin.list_topics(timeout=timeout_seconds).topics

    outcome: dict[str, str] = {}
    missing: list[NewTopic] = []
    for name in names:
        meta = existing.get(name)
        if meta is not None and meta.error is None:
            _check_partitions(name, len(meta.partitions), partitions)
            outcome[name] = "exists"
        else:
            missing.append(
                NewTopic(
                    name,
                    num_partitions=partitions,
                    replication_factor=replication_factor,
                    config={"retention.ms": str(RETENTION_MS)},
                )
            )

    if missing:
        futures = admin.create_topics(
            missing,
            operation_timeout=timeout_seconds,
            request_timeout=timeout_seconds,
        )
        for name, future in futures.items():
            try:
                future.result()
                outcome[name] = "created"
            except KafkaException as exc:
                if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    raise
                # Someone created it between our listing and our request.
                meta = admin.list_topics(topic=name, timeout=timeout_seconds).topics[name]
                _check_partitions(name, len(meta.partitions), partitions)
                outcome[name] = "exists"
    return outcome


def main() -> int:
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        outcome = ensure_topics(topics.ALL_TOPICS, partitions=settings.kafka_partitions)
    except Exception:
        logger.exception("topic setup failed")
        return 1
    for name, state in outcome.items():
        logger.info("topic %s: %s (%d partitions)", name, state, settings.kafka_partitions)
    return 0


if __name__ == "__main__":
    sys.exit(main())
