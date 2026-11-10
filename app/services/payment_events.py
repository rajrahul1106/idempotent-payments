"""The consumer's side effect, applied at most once per event and group.

Blueprint sections 2 and 8, steps 12 and 13. One database transaction
records the event in processed_events and writes the payment_events
projection. If the event is already recorded, nothing is written. The
caller commits the Kafka offset only after this function returns, so a
crash anywhere before that leads to redelivery, which this function then
skips.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from app.core.money import to_decimal
from app.db.models.messaging import PaymentEventProjection
from app.db.repositories import events as events_repo
from app.db.session import unit_of_work


@dataclass(frozen=True)
class PaymentPosted:
    """A validated payment.posted event, schema version 1."""

    event_id: str
    transaction_id: str
    from_account_id: str
    to_account_id: str
    amount_minor: int
    currency: str
    traceparent: str | None = None  # carried untouched; tracing is Phase 7


def apply_payment_posted(
    event: PaymentPosted,
    consumer_group: str,
    *,
    before_effect: Callable[[], None] | None = None,
) -> bool:
    """Apply the event in one transaction. True if applied, False if skipped.

    `before_effect` runs inside the transaction, only for an event not seen
    before. If it raises, the transaction rolls back, dedup record included,
    so the event can be tried again.
    """
    with unit_of_work() as session:
        if not events_repo.mark_processed(session, event.event_id, consumer_group):
            return False
        if before_effect is not None:
            before_effect()
        events_repo.add_projection(
            session,
            PaymentEventProjection(
                event_id=event.event_id,
                transaction_id=event.transaction_id,
                from_account_id=event.from_account_id,
                to_account_id=event.to_account_id,
                amount=to_decimal(event.amount_minor, event.currency),
                currency=event.currency,
            ),
        )
    return True
