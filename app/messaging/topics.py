"""Topic names and message header keys, in one place.

Every topic name and every header key the system uses is defined here so
that the relay, the consumer and the retry tiers cannot disagree about a
spelling. Nothing in this module talks to a broker.
"""

from __future__ import annotations

from app.core.config import settings

# --- topics --------------------------------------------------------------

MAIN_TOPIC: str = settings.kafka_main_topic
DLQ_TOPIC: str = settings.kafka_dlq_topic

#: One topic per delay tier, e.g. ("payments.retry.5s", "payments.retry.1m",
#: "payments.retry.10m"). Derived from RETRY_TIER_SECONDS so the tier count
#: is a configuration decision rather than a hard-coded list.
RETRY_TOPICS: tuple[str, ...] = settings.retry_topics

#: Every topic the system owns. Used by topic creation in Phase 5 and by
#: the readiness check.
ALL_TOPICS: tuple[str, ...] = (MAIN_TOPIC, *RETRY_TOPICS, DLQ_TOPIC)


# --- header keys ---------------------------------------------------------
# Kafka headers are bytes on the wire. These are the string keys; the
# producer encodes them.

HEADER_EVENT_ID = "event_id"
HEADER_EVENT_TYPE = "event_type"
HEADER_SCHEMA_VERSION = "schema_version"
HEADER_TRACEPARENT = "traceparent"

# Set by the retry tiers in Phase 6, read here so the constant has one home.
HEADER_ATTEMPT_COUNT = "attempt_count"
HEADER_FIRST_FAILED_AT = "first_failed_at"
HEADER_LAST_ERROR = "last_error"
HEADER_ORIGINAL_TOPIC = "original_topic"


def next_retry_topic(attempt_count: int) -> str | None:
    """The topic a message goes to after `attempt_count` failures.

    Returns the DLQ topic once the tiers are exhausted. Phase 6 uses this;
    it lives here because the tier ladder is topic knowledge.

    >>> next_retry_topic(0) == RETRY_TOPICS[0]
    True
    """
    if attempt_count < 0:
        raise ValueError("attempt_count cannot be negative")
    if attempt_count < len(RETRY_TOPICS):
        return RETRY_TOPICS[attempt_count]
    return DLQ_TOPIC
