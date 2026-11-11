"""Consumer lag from the broker's point of view (blueprint §11).

Lag is the partition's high watermark minus the group's committed offset,
read with the admin client. It needs nothing from the consumer itself, so
it stays correct, and keeps rising, while that consumer is down. The
reconciliation process calls this every few seconds.

A group that has never committed on a partition is treated as starting at
the low watermark, which is where auto.offset.reset=earliest would put it.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from app.core.config import settings
from app.messaging import topics

TIMEOUT_SECONDS = 10.0


@dataclass(frozen=True)
class PartitionLag:
    group: str
    topic: str
    partition: int
    lag: int


def watched_groups(ladder: topics.TopicLadder | None = None) -> dict[str, str]:
    """{group: topic} for every consumer this system runs."""
    ladder = ladder or topics.DEFAULT_LADDER
    groups = {settings.kafka_consumer_group: ladder.main}
    for retry_topic in ladder.retries:
        groups[f"{settings.kafka_consumer_group}-retry-{retry_topic.rsplit('.', 1)[-1]}"] = (
            retry_topic
        )
    groups[settings.kafka_dlq_writer_group] = ladder.dlq
    return groups


class LagReader:
    def __init__(self, bootstrap_servers: str | None = None) -> None:
        from confluent_kafka import Consumer
        from confluent_kafka.admin import AdminClient

        servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self._admin = AdminClient({"bootstrap.servers": servers})
        # Only for watermarks: never subscribes, never commits.
        self._probe = Consumer(
            {
                "bootstrap.servers": servers,
                "group.id": f"lag-probe-{uuid.uuid4().hex[:8]}",
                "enable.auto.commit": False,
            }
        )

    def read(self, groups: dict[str, str]) -> list[PartitionLag]:
        from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition

        result: list[PartitionLag] = []
        metadata = self._admin.list_topics(timeout=TIMEOUT_SECONDS)
        for group, topic in groups.items():
            partitions = sorted(metadata.topics[topic].partitions)
            request = [ConsumerGroupTopicPartitions(
                group, [TopicPartition(topic, p) for p in partitions]
            )]
            committed = {
                tp.partition: tp.offset
                for tp in self._admin.list_consumer_group_offsets(request)[group]
                .result(timeout=TIMEOUT_SECONDS)
                .topic_partitions
            }
            for partition in partitions:
                low, high = self._probe.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=TIMEOUT_SECONDS, cached=False
                )
                position = committed.get(partition, -1)
                if position < 0:  # never committed
                    position = low
                result.append(PartitionLag(group, topic, partition, max(high - position, 0)))
        return result

    def close(self) -> None:
        self._probe.close()
