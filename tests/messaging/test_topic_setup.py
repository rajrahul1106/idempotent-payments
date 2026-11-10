"""G8: topics are created with the partition count the system relies on.

Creation is idempotent, and a topic that exists with a different partition
count stops everything rather than being silently used or changed.
"""

from __future__ import annotations

import uuid

import pytest

from app.messaging.topic_setup import TopicMismatch, ensure_topics
from tests.messaging.conftest import RUN_ID, delete_topics

pytestmark = pytest.mark.messaging


@pytest.fixture
def topic_names(kafka):
    names: list[str] = []

    def make(label: str) -> str:
        name = f"test.{RUN_ID}.setup.{label}.{uuid.uuid4().hex[:6]}"
        names.append(name)
        return name

    yield make
    if names:
        delete_topics(kafka, names)


def _partitions(kafka: str, name: str) -> int:
    from confluent_kafka.admin import AdminClient

    meta = AdminClient({"bootstrap.servers": kafka}).list_topics(timeout=10)
    return len(meta.topics[name].partitions)


def test_creation_is_idempotent_with_six_partitions(kafka, topic_names):
    main, retry, dlq = topic_names("main"), topic_names("retry"), topic_names("dlq")

    first = ensure_topics([main, retry, dlq], partitions=6, bootstrap_servers=kafka)
    second = ensure_topics([main, retry, dlq], partitions=6, bootstrap_servers=kafka)

    assert set(first.values()) == {"created"}
    assert set(second.values()) == {"exists"}
    assert [_partitions(kafka, n) for n in (main, retry, dlq)] == [6, 6, 6]


def test_existing_topic_with_other_partition_count_fails_loudly(kafka, topic_names):
    name = topic_names("wrong")
    ensure_topics([name], partitions=1, bootstrap_servers=kafka)

    with pytest.raises(TopicMismatch, match="1 partition"):
        ensure_topics([name], partitions=6, bootstrap_servers=kafka)
    assert _partitions(kafka, name) == 1  # never altered


def test_every_topic_the_system_owns_is_covered():
    from app.messaging import topics

    assert topics.MAIN_TOPIC in topics.ALL_TOPICS
    assert topics.DLQ_TOPIC in topics.ALL_TOPICS
    assert set(topics.RETRY_TOPICS) <= set(topics.ALL_TOPICS)
