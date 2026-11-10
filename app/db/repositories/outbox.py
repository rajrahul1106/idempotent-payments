"""Outbox reads for the relay.

Repositories never commit. The relay owns the transaction.

The claim query is the interesting part. It takes `FOR UPDATE SKIP LOCKED`,
and exactly one relay instance runs (blueprint decision D1). A second
relay would claim the next batch and could publish an account's later
event before the first relay publishes its earlier one, which breaks
per-account ordering. SKIP LOCKED stays because it is harmless with one
relay: a restarting relay steps over rows its predecessor still holds
instead of blocking behind them.
"""

from __future__ import annotations

import datetime as dt

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from app.db.models.messaging import OutboxEvent


def claim_batch(session: Session, limit: int) -> list[OutboxEvent]:
    """Lock and return up to `limit` unpublished events, oldest first.

    Ordering by id preserves per-account ordering, though id order is not
    commit order in general: ids are assigned at INSERT. It holds per
    partition key because two payments debiting the same account are
    serialised by that account's row lock, and the outbox row is inserted
    while the lock is held. The lock taken here is held until the caller's
    transaction ends.
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
