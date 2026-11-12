"""The dead-letter store and operator replay (docs/ARCHITECTURE.md §5.4 and §7).

Recording. The DLQ writer persists each message from payments.dlq in one
transaction with a processed_events row under its own consumer group, the
same dedup the main consumer uses. A redelivered DLQ message conflicts on
that row and writes nothing.

The dedup key is the event_id combined with the replay it came from, if
any. Two copies of one failing event (a crash between a forward and an
offset commit makes one, decision D7) collapse into one row. A message an
operator replays that then fails again is a new failure and gets a new row,
so a replay can never be silently swallowed.

Replay. Republish to the main topic with attempt_count 0 and the original
event_id, inside a span that continues the dead letter's stored trace,
then stamp replayed_at. No database transaction is open during
the publish. Two concurrent replays may both publish; the main consumer
dedups on event_id, so the effect still happens once, and only the replay
that stamps replayed_at gets 200.
"""

from __future__ import annotations

import base64
import dataclasses
import datetime as dt
import json
import logging
import uuid
from dataclasses import dataclass
from decimal import Decimal

from opentelemetry import trace
from opentelemetry.trace import Link, SpanKind, Status, StatusCode

from app.core.exceptions import BrokerUnavailable, DlqAlreadyReplayed, DlqMessageNotFound
from app.core.telemetry import context_from, current_traceparent, tracer
from app.db.repositories import dlq as dlq_repo
from app.db.repositories import events as events_repo
from app.db.repositories.dlq import DlqRow
from app.db.session import read_session, unit_of_work
from app.messaging import topics
from app.messaging.producer import Producer, PublishError

logger = logging.getLogger(__name__)
_tracer = tracer(__name__)

#: A body that is not storable JSON is kept as {RAW_BODY_KEY: base64}, so the
#: exact bytes survive into the table and back out on a replay.
RAW_BODY_KEY = "undecodable_body_base64"

#: event_id for a message that carried no usable one.
NIL_EVENT_ID = "00000000-0000-0000-0000-000000000000"

_DEDUP_NAMESPACE = uuid.UUID("5f1d7c1e-4a43-4c7e-9a8e-6a0c2f1d9b11")


@dataclass(frozen=True)
class DeadLetter:
    dedup_id: str
    event_id: str
    original_topic: str
    payload_json: str
    reason: str
    attempts: int
    first_failed_at: dt.datetime
    traceparent: str | None = None
    failure_history: list = dataclasses.field(default_factory=list)


# --- pure helpers -------------------------------------------------------------


def is_uuid(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        uuid.UUID(value)
    except ValueError:
        return False
    return True


def payload_json_for(value: bytes | None) -> str:
    """The JSON text to store for a message body.

    Valid JSON is stored as it is (Postgres parses it, numbers stay exact).
    Anything jsonb cannot hold is wrapped: bytes that are not UTF-8 JSON,
    and the \\u0000 escape, which jsonb rejects.
    """
    raw = value or b""
    try:
        text = raw.decode("utf-8")
        json.loads(text, parse_float=Decimal)
        if "\\u0000" not in text:
            return text
    except (UnicodeDecodeError, ValueError):
        pass
    return json.dumps({RAW_BODY_KEY: base64.b64encode(raw).decode("ascii")})


def body_from_payload_json(payload_json: str) -> bytes:
    """The inverse of payload_json_for, for a replay."""
    payload = json.loads(payload_json, parse_float=Decimal)
    if isinstance(payload, dict) and set(payload) == {RAW_BODY_KEY}:
        return base64.b64decode(payload[RAW_BODY_KEY])
    return payload_json.encode("utf-8")


def dedup_id_for(event_id: str, replay_of: str | None, fallback: str) -> str:
    """processed_events key for the DLQ writer. See the module docstring.

    `fallback` names the DLQ message itself (topic, partition, offset). It
    is used only when there is no real event_id, so a redelivery of such a
    message still dedups.
    """
    if event_id == NIL_EVENT_ID:
        return str(uuid.uuid5(_DEDUP_NAMESPACE, f"message|{fallback}"))
    return str(uuid.uuid5(_DEDUP_NAMESPACE, f"event|{event_id}|{replay_of or ''}"))


# --- recording ---------------------------------------------------------------


def record_dead_letter(letter: DeadLetter, consumer_group: str) -> bool:
    """Persist one dead letter. False if this one was already recorded."""
    with unit_of_work() as session:
        if not events_repo.mark_processed(session, letter.dedup_id, consumer_group):
            return False
        dlq_repo.insert(
            session,
            event_id=letter.event_id,
            original_topic=letter.original_topic,
            payload_json=letter.payload_json,
            reason=letter.reason,
            attempts=letter.attempts,
            first_failed_at=letter.first_failed_at,
            traceparent=letter.traceparent,
            failure_history_json=json.dumps(letter.failure_history),
        )
    return True


# --- operator reads and replay ------------------------------------------------


def list_dead_letters(
    *, reason: str | None, replayed: bool | None, after_id: int | None, limit: int
) -> list[DlqRow]:
    with read_session() as session:
        return dlq_repo.page(
            session, reason=reason, replayed=replayed, after_id=after_id, limit=limit
        )


def replay_message(row: DlqRow) -> tuple[str | None, bytes, dict[str, str]]:
    """Key, value and headers for republishing a stored dead letter."""
    value = body_from_payload_json(row.payload_json)
    headers = {
        topics.HEADER_EVENT_ID: row.event_id,
        topics.HEADER_ATTEMPT_COUNT: "0",
        topics.HEADER_REPLAY_OF: str(row.id),
    }
    key = None
    payload = json.loads(row.payload_json, parse_float=Decimal)
    if isinstance(payload, dict):
        # Keyed by the debited account, like the relay (decision D3).
        if isinstance(payload.get("from_account_id"), str):
            key = payload["from_account_id"]
        if isinstance(payload.get("event_type"), str):
            headers[topics.HEADER_EVENT_TYPE] = payload["event_type"]
        if type(payload.get("schema_version")) is int:
            headers[topics.HEADER_SCHEMA_VERSION] = str(payload["schema_version"])
    return key, value, headers


def _publish_replay(
    row: DlqRow,
    topic: str,
    key: str | None,
    value: bytes,
    headers: dict[str, str],
    producer: Producer,
) -> None:
    """Publish inside a span that continues the dead letter's own trace.

    The operator's HTTP request is a separate trace; the replay span links
    to it, so either trace leads to the other. The replayed message carries
    the replay span's traceparent, so its processing joins the original
    trace too.
    """
    request = trace.get_current_span().get_span_context()
    original = context_from(row.traceparent)
    links = [Link(request)] if original is not None and request.is_valid else []
    with _tracer.start_as_current_span(
        "dlq replay",
        context=original,  # None: no stored trace, so a child of the request
        kind=SpanKind.PRODUCER,
        links=links,
        attributes={
            "event_id": row.event_id,
            "dlq.message_id": row.id,
            "attempt_count": 0,
            "messaging.system": "kafka",
            "messaging.destination.name": topic,
            **({"account_id": key} if key else {}),
        },
    ) as span:
        traceparent = current_traceparent()
        if traceparent:
            headers[topics.HEADER_TRACEPARENT] = traceparent
        try:
            producer.publish(topic, key, value, headers)
        except PublishError as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, "not acknowledged"))
            logger.error("replay of DLQ message %d not acknowledged: %s", row.id, exc)
            raise BrokerUnavailable("The broker did not accept the replay; retry later") from exc


def replay_dead_letter(message_id: int, producer: Producer, topic: str) -> DlqRow:
    with read_session() as session:
        row = dlq_repo.get(session, message_id)
    # The session is closed: no transaction is open across the publish.
    if row is None:
        raise DlqMessageNotFound(f"DLQ message {message_id} not found")
    if row.replayed_at is not None:
        raise DlqAlreadyReplayed(
            f"DLQ message {message_id} was already replayed at {row.replayed_at.isoformat()}"
        )

    key, value, headers = replay_message(row)
    _publish_replay(row, topic, key, value, headers, producer)

    with unit_of_work() as session:
        replayed_at = dlq_repo.mark_replayed(session, message_id)
    if replayed_at is None:
        logger.warning(
            "DLQ message %d was replayed concurrently; this request's publish of event "
            "%s is a duplicate the consumer will skip",
            message_id,
            row.event_id,
        )
        raise DlqAlreadyReplayed(f"DLQ message {message_id} was replayed concurrently")
    logger.info(
        "replayed DLQ message %d (event %s) to %s", message_id, row.event_id, topic
    )
    return dataclasses.replace(row, replayed_at=replayed_at)
