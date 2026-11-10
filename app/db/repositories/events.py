"""Consumer-side writes: the dedup record and the payment_events projection.

Repositories never commit. The caller owns the transaction, and both writes
below must share one, so a side effect never exists without its dedup
record or the other way round.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.models.messaging import PaymentEventProjection


def mark_processed(session: Session, event_id: str, consumer_group: str) -> bool:
    """Record that this group handled the event. False if it already had.

    ON CONFLICT DO NOTHING uses the processed_events primary key, so the
    unique constraint is still what decides. A concurrent insert of the same
    event waits for the first transaction, then sees the conflict.
    """
    row = session.execute(
        text(
            "INSERT INTO processed_events (event_id, consumer_group) "
            "VALUES (:event_id, :consumer_group) "
            "ON CONFLICT (event_id, consumer_group) DO NOTHING "
            "RETURNING event_id"
        ),
        {"event_id": event_id, "consumer_group": consumer_group},
    ).first()
    return row is not None


def add_projection(session: Session, projection: PaymentEventProjection) -> None:
    session.add(projection)
    session.flush()
