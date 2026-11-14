"""Record each failed attempt the main consumer forwards (migration 0005).

Best-effort by design: the row is written in its own short transaction,
after the message's offset commit, and any error is logged and swallowed.
It is a record for people (the demo page's retry timeline), never an input
to delivery: losing a row changes nothing about where the message goes.
"""

from __future__ import annotations

import logging
import uuid

from app.db.models.messaging import DeliveryFailure
from app.db.session import unit_of_work

logger = logging.getLogger(__name__)


def _uuid_or_none(value: str | None) -> str | None:
    try:
        return str(uuid.UUID(value)) if value else None
    except ValueError:
        return None


def record(*, event_id: str | None, attempt: int, kind: str, error: str, sent_to: str) -> bool:
    """Write one row. True if it was written; never raises."""
    try:
        with unit_of_work() as session:
            session.add(
                DeliveryFailure(
                    event_id=_uuid_or_none(event_id),
                    attempt=attempt,
                    kind=kind,
                    error=error,
                    sent_to=sent_to,
                )
            )
        return True
    except Exception:  # noqa: BLE001 - best-effort: log and carry on
        logger.warning(
            "could not record delivery failure for event %s attempt %d (sent to %s)",
            event_id, attempt, sent_to, exc_info=True,
        )
        return False
