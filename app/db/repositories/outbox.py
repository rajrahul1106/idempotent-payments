"""Outbox reads for the relay.

Repositories never commit. The relay owns the transaction.

The claim query is the interesting part. It takes `FOR UPDATE SKIP LOCKED`,
and exactly one relay instance runs (decision D1, docs/ARCHITECTURE.md §16). A second
relay would claim the next batch and could publish an account's later
event before the first relay publishes its earlier one, which breaks
per-account ordering. SKIP LOCKED stays because it is harmless with one
relay: a restarting relay steps over rows its predecessor still holds
instead of blocking behind them.
"""

from __future__ import annotations

import datetime as dt

from sqlalchemy import func, select, text, update
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


#: The LISTEN/NOTIFY channel the relay wakes on.
NOTIFY_CHANNEL = "outbox"


def notify(session: Session, event_id: str) -> None:
    """Wake the relay when this transaction commits.

    Postgres delivers a NOTIFY only on COMMIT and drops it on ROLLBACK, so
    the relay is never woken for a row that does not exist. It is a hint:
    a relay that misses it still finds the row on its next poll.
    """
    session.execute(
        text("SELECT pg_notify(:channel, :event_id)"),
        {"channel": NOTIFY_CHANNEL, "event_id": event_id},
    )


def recent_publish_delay_p95(session: Session, *, window_seconds: int = 300) -> float | None:
    """p95 publish delay of rows published in the last `window_seconds`.

    Seconds as a float: a duration, not money. The created_at bound keeps the
    scan on ix_outbox_created; a row stuck longer than an hour is reported by
    the oldest-unpublished-age gauge instead.
    """
    return session.execute(
        text(
            "SELECT percentile_cont(0.95) WITHIN GROUP "
            "(ORDER BY extract(epoch FROM published_at - created_at)) FROM outbox "
            "WHERE created_at > now() - interval '1 hour' "
            "AND published_at > now() - make_interval(secs => :window)"
        ),
        {"window": window_seconds},
    ).scalar_one()
