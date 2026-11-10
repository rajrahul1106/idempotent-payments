"""dlq_messages reads and writes. Repositories never commit.

The payload column is JSONB. It is written from the message's JSON text
with a cast, and read back as text, so Postgres parses the numbers and no
Python float ever holds one on the way in or out.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.orm import Session

_COLUMNS = (
    "id, event_id::text AS event_id, original_topic, payload::text AS payload_json, "
    "reason, attempts, first_failed_at, created_at, replayed_at"
)


@dataclass(frozen=True)
class DlqRow:
    id: int
    event_id: str
    original_topic: str
    payload_json: str
    reason: str
    attempts: int
    first_failed_at: dt.datetime
    created_at: dt.datetime
    replayed_at: dt.datetime | None


def insert(
    session: Session,
    *,
    event_id: str,
    original_topic: str,
    payload_json: str,
    reason: str,
    attempts: int,
    first_failed_at: dt.datetime,
) -> int:
    return session.execute(
        text(
            "INSERT INTO dlq_messages "
            "(event_id, original_topic, payload, reason, attempts, first_failed_at) "
            "VALUES (:event_id, :original_topic, CAST(:payload AS jsonb), :reason, "
            ":attempts, :first_failed_at) RETURNING id"
        ),
        {
            "event_id": event_id,
            "original_topic": original_topic,
            "payload": payload_json,
            "reason": reason,
            "attempts": attempts,
            "first_failed_at": first_failed_at,
        },
    ).scalar_one()


def get(session: Session, message_id: int) -> DlqRow | None:
    row = session.execute(
        text(f"SELECT {_COLUMNS} FROM dlq_messages WHERE id = :id"), {"id": message_id}
    ).mappings().first()
    return DlqRow(**row) if row else None


def _like_prefix(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return escaped + "%"


def page(
    session: Session,
    *,
    reason: str | None,
    replayed: bool | None,
    after_id: int | None,
    limit: int,
) -> list[DlqRow]:
    """Oldest first, keyset-paginated on id."""
    clauses, params = [], {"limit": limit}
    if reason:
        clauses.append("reason LIKE :reason ESCAPE '\\'")
        params["reason"] = _like_prefix(reason)
    if replayed is True:
        clauses.append("replayed_at IS NOT NULL")
    elif replayed is False:
        clauses.append("replayed_at IS NULL")
    if after_id is not None:
        clauses.append("id > :after_id")
        params["after_id"] = after_id
    where = f"WHERE {' AND '.join(clauses)} " if clauses else ""
    rows = session.execute(
        text(f"SELECT {_COLUMNS} FROM dlq_messages {where}ORDER BY id LIMIT :limit"), params
    ).mappings()
    return [DlqRow(**row) for row in rows]


def mark_replayed(session: Session, message_id: int) -> dt.datetime | None:
    """Stamp replayed_at once. None if another replay already stamped it."""
    return session.execute(
        text(
            "UPDATE dlq_messages SET replayed_at = now() "
            "WHERE id = :id AND replayed_at IS NULL RETURNING replayed_at"
        ),
        {"id": message_id},
    ).scalar_one_or_none()
