"""What to do with a message that failed (decision D5, blueprint §16).

Every processing error is one of three kinds:

    PERMANENT       the message itself is wrong: unparseable, an unknown
                    schema_version, an event_id that disagrees with its
                    header, an invalid money value. Straight to the DLQ,
                    because no retry can fix it.
    INFRASTRUCTURE  the database cannot be reached. Not the message's fault,
                    so it goes nowhere: the consumer holds it, offset
                    uncommitted, and retries the same message until the
                    database returns. An outage must never empty the main
                    topic into the DLQ.
    TRANSIENT       anything else. Through the retry tiers, then the DLQ.

This module also builds the failure-history headers a message carries
through the tiers. Nothing here talks to a broker or a database.
"""

from __future__ import annotations

import datetime as dt
import enum

from app.db.errors import database_unavailable
from app.messaging import topics

#: last_error is a header, and the DLQ writer stores it as the reason.
MAX_ERROR_LENGTH = 1000


class EventError(ValueError):
    """The message cannot be processed as it stands. Always PERMANENT."""


class FailureKind(enum.Enum):
    PERMANENT = "permanent"
    TRANSIENT = "transient"
    INFRASTRUCTURE = "infrastructure"


def classify(exc: BaseException) -> FailureKind:
    if isinstance(exc, EventError):
        return FailureKind.PERMANENT
    if database_unavailable(exc) or isinstance(exc, ConnectionError):
        return FailureKind.INFRASTRUCTURE
    return FailureKind.TRANSIENT


def describe(kind: FailureKind, exc: BaseException) -> str:
    """The last_error header: "<kind>: <ExceptionClass>: <message>".

    It starts with the kind, so GET /v1/dlq?reason=permanent finds every
    message that was never retried.
    """
    message = " ".join(str(exc).split())  # one line, whatever the driver said
    text = f"{kind.value}: {type(exc).__name__}: {message}"
    return text[:MAX_ERROR_LENGTH]


def attempt_count(headers: dict[str, str]) -> int:
    """Failed attempts so far. A missing or garbled header counts as zero."""
    try:
        return max(0, int(headers.get(topics.HEADER_ATTEMPT_COUNT, "0")))
    except ValueError:
        return 0


def failure_headers(
    headers: dict[str, str],
    *,
    kind: FailureKind,
    exc: BaseException,
    source_topic: str,
    now: dt.datetime | None = None,
) -> dict[str, str]:
    """The message's headers for its next hop, history updated.

    Every original header (event_id, traceparent, replay_of, ...) is kept.
    attempt_count goes up by one, last_error is replaced, and
    first_failed_at and original_topic are set once and then kept.
    """
    now = now or dt.datetime.now(dt.timezone.utc)
    forwarded = dict(headers)
    forwarded[topics.HEADER_ATTEMPT_COUNT] = str(attempt_count(headers) + 1)
    forwarded[topics.HEADER_LAST_ERROR] = describe(kind, exc)
    forwarded.setdefault(topics.HEADER_FIRST_FAILED_AT, now.isoformat())
    forwarded.setdefault(topics.HEADER_ORIGINAL_TOPIC, source_topic)
    return forwarded
