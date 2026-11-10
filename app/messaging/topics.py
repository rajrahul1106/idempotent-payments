"""Topic names and message header keys, in one place.

Every topic name and every header key the system uses is defined here so
that the relay, the consumer and the retry tiers cannot disagree about a
spelling. Nothing in this module talks to a broker.
"""

from __future__ import annotations

from dataclasses import dataclass

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

# The failure history a message carries through the retry tiers (§5.4).
#: Failed attempts so far. Absent on a fresh message, "0" on a replay.
HEADER_ATTEMPT_COUNT = "attempt_count"
#: ISO 8601 UTC time of the first failure. Kept unchanged through the tiers.
HEADER_FIRST_FAILED_AT = "first_failed_at"
#: "<kind>: <ExceptionClass>: <message>", kind being permanent or transient.
HEADER_LAST_ERROR = "last_error"
#: The topic the message was first consumed from.
HEADER_ORIGINAL_TOPIC = "original_topic"
#: JSON list, one entry per failed attempt: {attempt, topic, error, at}.
#: `topic` is where the message was sent after that attempt. Each forward
#: appends one entry; the DLQ writer stores the whole list.
HEADER_FAILURE_HISTORY = "failure_history"
#: Set by an operator replay to the dlq_messages id being replayed. A replay
#: that fails again is a new failure, so the DLQ writer dedups per replay.
HEADER_REPLAY_OF = "replay_of"


@dataclass(frozen=True)
class TopicLadder:
    """The main topic, its delay tiers in order, and the DLQ.

    Production uses DEFAULT_LADDER. Tests build their own with unique names,
    so a test failure can never land on a topic `make up` consumes.
    """

    main: str
    retries: tuple[str, ...]
    retry_delays: tuple[float, ...]
    dlq: str

    def __post_init__(self) -> None:
        if len(self.retries) != len(self.retry_delays):
            raise ValueError("one delay per retry topic")

    @property
    def all(self) -> tuple[str, ...]:
        return (self.main, *self.retries, self.dlq)

    def next_topic(self, attempt_count: int) -> str:
        """Where a message goes when it fails with `attempt_count` earlier
        failures behind it: the first tier for a fresh message, the DLQ once
        every tier has been tried."""
        if attempt_count < 0:
            raise ValueError("attempt_count cannot be negative")
        if attempt_count < len(self.retries):
            return self.retries[attempt_count]
        return self.dlq

    def delay_of(self, retry_topic: str) -> float:
        return self.retry_delays[self.retries.index(retry_topic)]


DEFAULT_LADDER = TopicLadder(
    main=MAIN_TOPIC,
    retries=RETRY_TOPICS,
    retry_delays=tuple(float(s) for s in settings.retry_tier_seconds),
    dlq=DLQ_TOPIC,
)


def next_retry_topic(attempt_count: int) -> str:
    """The production ladder's next topic. See TopicLadder.next_topic.

    >>> next_retry_topic(0) == RETRY_TOPICS[0]
    True
    """
    return DEFAULT_LADDER.next_topic(attempt_count)
