"""Outbox reads for the relay.

Repositories never commit. The relay owns the transaction.

The claim query is the interesting part. `FOR UPDATE SKIP LOCKED` lets
several relay instances run at once: each one locks the rows it takes and
the others step over them rather than blocking. Without SKIP LOCKED a
second relay would serialise behind the first and add nothing.
"""

from __future__ import annotations

import datetime as dt

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from app.db.models.messaging import OutboxEvent


def claim_batch(session: Session, limit: int) -> list[OutboxEvent]:
    """Lock and return up to `limit` unpublished events, oldest first.

    Ordering by id is what preserves per-account ordering: events are
    inserted in commit order, so publishing in id order publishes in
    commit order. The lock is held until the caller's transaction ends.
    """
    stmt = (
        select(OutboxEvent)
        .where(OutboxEvent.published_at.is_(None))
        .order_by(OutboxEvent.id)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    return list(session.execute(stmt).scalars())


def mark_published(session: Session, event_row_ids: list[int]) -> int:
    """Stamp `published_at` on the given outbox rows. Returns rows touched."""
    if not event_row_ids:
        return 0
    stmt = (
        update(OutboxEvent)
        .where(OutboxEvent.id.in_(event_row_ids))
        .values(published_at=func.now())
    )
    return int(session.execute(stmt).rowcount or 0)


def backlog_size(session: Session) -> int:
    """How many events are waiting. Feeds outbox_unpublished_rows in Phase 7."""
    stmt = select(func.count()).select_from(OutboxEvent).where(
        OutboxEvent.published_at.is_(None)
    )
    return int(session.execute(stmt).scalar_one())


def oldest_unpublished_age_seconds(session: Session) -> float | None:
    """Age of the oldest waiting event, or None when the backlog is empty.

    This is the number that matters operationally. A backlog of 10,000 rows
    that drains in a second is fine; one row stuck for five minutes is not.
    """
    stmt = select(func.min(OutboxEvent.created_at)).where(
        OutboxEvent.published_at.is_(None)
    )
    oldest = session.execute(stmt).scalar_one_or_none()
    if oldest is None:
        return None
    if oldest.tzinfo is None:
        oldest = oldest.replace(tzinfo=dt.timezone.utc)
    return (dt.datetime.now(dt.timezone.utc) - oldest).total_seconds()
