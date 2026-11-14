"""Demo faults: a payment that asks the consumer to fail on purpose (D27).

Only for the local demo page. The main consumer installs this check only
when DEMO_FAULTS_ENABLED is true, and the setting is refused outside a local
environment (app/core/config.py). With the switch off nothing here runs.

The fault is read from the payment row, not from the event: the consumer
looks up transactions.metadata->>'demo_fault' for the event's transaction,
so the Kafka event format is unchanged. A missing row, or any value other
than the three below, means no fault.

    fail_once        a transient error on the first attempt (attempt 0)
    fail_twice       a transient error on attempts 0 and 1
    fail_permanent   a permanent error, straight to the DLQ

The errors go through the normal D5 classification and retry ladder; nothing
about D5, D7 or D9 changes. An operator replay (the replay_of header, which
the retry tiers carry forward) is never faulted, so a replayed demo message
completes.
"""

from __future__ import annotations

from sqlalchemy import text

from app.db.session import read_session
from app.messaging import topics
from app.messaging.failures import EventError
from app.services.payment_events import PaymentPosted

FAULTS = frozenset({"fail_once", "fail_twice", "fail_permanent"})
_TRANSIENT_ATTEMPTS = {"fail_once": 1, "fail_twice": 2}


class DemoTransientFault(RuntimeError):
    """A simulated temporary failure. Classified TRANSIENT (D5)."""


class DemoPermanentFault(EventError):
    """A simulated permanent failure. Classified PERMANENT (D5)."""


def fault_for(transaction_id: str) -> str | None:
    """The payment's demo_fault, or None for no fault."""
    with read_session() as session:
        value = session.execute(
            text("SELECT metadata->>'demo_fault' FROM transactions WHERE id = :id"),
            {"id": transaction_id},
        ).scalar_one_or_none()
    return value if value in FAULTS else None


def check(event: PaymentPosted, attempt: int, headers: dict[str, str]) -> None:
    """Raise the requested fault for this attempt (0 for a first delivery)."""
    if headers.get(topics.HEADER_REPLAY_OF):
        return
    fault = fault_for(event.transaction_id)
    if fault is None:
        return
    if fault == "fail_permanent":
        raise DemoPermanentFault(f"event {event.event_id}: simulated permanent failure (demo_fault)")
    if attempt < _TRANSIENT_ATTEMPTS[fault]:
        raise DemoTransientFault(
            f"event {event.event_id}: simulated temporary failure on attempt {attempt + 1} (demo_fault {fault})"
        )
