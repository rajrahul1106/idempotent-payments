"""Read-only views for the demo page: accounts, recent payments, one payment's journey.

Nothing here writes. Who may see what is decided by the routes (decision D4
and D26, docs/ARCHITECTURE.md §16); this module only reads rows. Money stays
Decimal until it is formatted as a string.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.money import format_amount, to_minor
from app.db.models.ledger import Account, Posting, Transaction
from app.db.models.messaging import DlqMessage, IdempotencyKey, OutboxEvent, ProcessedEvent
from app.db.session import SessionFactory


# --- accounts ------------------------------------------------------------------------


def accounts_page(
    session: Session,
    *,
    limit: int,
    customers_only: bool,
    always_include: str | None = None,
) -> list[Account]:
    """Accounts oldest first, so the seeded ones lead. `always_include` (a
    client's own account) is added even when it falls outside the page."""
    stmt = select(Account).order_by(Account.created_at, Account.id).limit(limit)
    if customers_only:
        stmt = stmt.where(Account.account_type == "customer")
    accounts = list(session.execute(stmt).scalars().all())
    if always_include and always_include not in {a.id for a in accounts}:
        own = session.get(Account, always_include)
        if own is not None:
            accounts.insert(0, own)
    return accounts


def balances(session: Session, account_ids: Iterable[str]) -> dict[str, Decimal]:
    """Derived balances for the given accounts, in one grouped query."""
    ids = list(account_ids)
    if not ids:
        return {}
    rows = session.execute(
        select(Posting.account_id, func.coalesce(func.sum(Posting.amount), 0))
        .where(Posting.account_id.in_(ids))
        .group_by(Posting.account_id)
    ).all()
    found = {account_id: Decimal(total) for account_id, total in rows}
    return {account_id: found.get(account_id, Decimal(0)) for account_id in ids}


# --- recent payments -----------------------------------------------------------------


def _from_to(postings: list[Posting]) -> tuple[str | None, str | None]:
    sender = next((p.account_id for p in postings if p.amount < 0), None)
    receiver = next((p.account_id for p in postings if p.amount > 0), None)
    return sender, receiver


def recent_payments(session: Session, *, account_id: str | None, limit: int) -> list[dict[str, Any]]:
    """Newest first. With `account_id`, only payments with a posting on it."""
    stmt = select(Transaction).order_by(Transaction.created_at.desc(), Transaction.id.desc())
    if account_id is not None:
        touching = select(Posting.transaction_id).where(Posting.account_id == account_id)
        stmt = stmt.where(Transaction.id.in_(touching))
    transactions = list(session.execute(stmt.limit(limit)).scalars().all())
    if not transactions:
        return []

    postings: dict[str, list[Posting]] = {t.id: [] for t in transactions}
    for posting in session.execute(
        select(Posting).where(Posting.transaction_id.in_(list(postings)))
    ).scalars():
        postings[posting.transaction_id].append(posting)

    out = []
    for t in transactions:
        sender, receiver = _from_to(postings[t.id])
        out.append(
            {
                "payment_id": t.id,
                "status": t.status,
                "amount": format_amount(t.amount),
                "amount_minor": to_minor(t.amount, t.currency),
                "currency": t.currency,
                "from_account_id": sender,
                "to_account_id": receiver,
                "created_at": t.created_at.isoformat(),
            }
        )
    return out


# --- one payment's journey -----------------------------------------------------------

STAGES = (
    ("received", "Request received"),
    ("recorded", "Money moved in the ledger"),
    ("queued", "Queued for delivery"),
    ("sent", "Delivered to processor"),
    ("processed", "Processed"),
)


@dataclass(frozen=True)
class Journey:
    body: dict[str, Any]
    party_ids: frozenset[str]


def _iso(value: dt.datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _readable(amount: Decimal, currency: str) -> str:
    """For the summary sentence: two places when that is exact, e.g. 100.00 INR.
    Never rounds; a value with more precision keeps all four places."""
    two = amount.quantize(Decimal("0.01"))
    return f"{two if two == amount else format_amount(amount)} {currency}"


def _elapsed(seconds: float) -> str:
    seconds = int(max(seconds, 0))
    if seconds < 60:
        return f"{seconds} s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes} min {seconds} s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours} h {minutes} min"


def journey(payment_id: str) -> Journey | None:
    """Everything recorded about one payment, read in one REPEATABLE READ
    snapshot so the stages cannot contradict each other. None if unknown.

    Every field comes from a row. A stage that has no row is reported as not
    done, with the time elapsed since the payment was recorded, and nothing
    is guessed about why.
    """
    session = SessionFactory()
    try:
        session.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        session.execute(text("SET TRANSACTION READ ONLY"))
        now = session.execute(text("SELECT now()")).scalar_one()

        transaction = session.get(Transaction, payment_id)
        if transaction is None:
            return None
        postings = list(
            session.execute(
                select(Posting).where(Posting.transaction_id == payment_id).order_by(Posting.id)
            ).scalars()
        )
        names = {
            a.id: a
            for a in session.execute(
                select(Account).where(Account.id.in_({p.account_id for p in postings}))
            ).scalars()
        }
        event_id = postings[0].event_id if postings else None

        key = session.execute(
            select(IdempotencyKey)
            .where(IdempotencyKey.transaction_id == payment_id)
            .order_by(IdempotencyKey.created_at)
            .limit(1)
        ).scalar_one_or_none()
        outbox = (
            session.execute(select(OutboxEvent).where(OutboxEvent.event_id == event_id)).scalar_one_or_none()
            if event_id
            else None
        )
        processed = (
            session.get(ProcessedEvent, (event_id, settings.kafka_consumer_group)) if event_id else None
        )
        dead_letters = (
            list(
                session.execute(
                    select(DlqMessage).where(DlqMessage.event_id == event_id).order_by(DlqMessage.id)
                ).scalars()
            )
            if event_id
            else []
        )
    finally:
        session.close()

    at = {
        "received": key.created_at if key else None,
        "recorded": transaction.created_at,
        "queued": outbox.created_at if outbox else None,
        "sent": outbox.published_at if outbox else None,
        "processed": processed.processed_at if processed else None,
    }
    since = _elapsed((now - transaction.created_at).total_seconds())
    stages = []
    for stage_key, label in STAGES:
        done = at[stage_key] is not None
        note = None
        if not done:
            note = (
                "No request record: this payment was not made through the API"
                if stage_key == "received"
                else f"Not processed yet ({since} since the money moved)"
            )
        stages.append({"key": stage_key, "label": label, "done": done, "at": _iso(at[stage_key]), "note": note})

    sender_id, receiver_id = _from_to(postings)

    def party(account_id: str | None) -> dict[str, Any] | None:
        if account_id is None:
            return None
        account = names.get(account_id)
        return {
            "id": account_id,
            "holder_name": account.holder_name if account else None,
            "account_type": account.account_type if account else None,
        }

    total = sum((p.amount for p in postings), Decimal(0))
    entries = [
        {
            "account_id": p.account_id,
            "holder_name": names[p.account_id].holder_name if p.account_id in names else None,
            "amount": format_amount(p.amount),
            "direction": "money out" if p.amount < 0 else "money in",
        }
        for p in postings
    ]
    letters = [
        {
            "id": d.id,
            "reason": d.reason,
            "attempts": d.attempts,
            "failure_history": d.failure_history,
            "created_at": _iso(d.created_at),
            "replayed_at": _iso(d.replayed_at),
        }
        for d in dead_letters
    ]

    amount = format_amount(transaction.amount)
    readable = _readable(transaction.amount, transaction.currency)
    if any(d.replayed_at is None for d in dead_letters):
        overall = "failed"
        summary = (
            f"The money moved in the ledger, but the follow-up processing failed and "
            f"is parked for staff review ({len(dead_letters)} recorded failure"
            f"{'' if len(dead_letters) == 1 else 's'})."
        )
    elif processed is not None:
        overall = "completed"
        summary = f"{readable} moved and every step is complete."
    else:
        overall = "in_progress"
        last_done = max((i for i, s in enumerate(stages) if s["done"]), default=-1)
        next_label = stages[last_done + 1]["label"] if last_done + 1 < len(stages) else "Processed"
        summary = (
            f"{readable} moved in the ledger. "
            f"Next step not recorded yet: {next_label.lower()} ({since} so far)."
        )

    body = {
        "payment": {
            "id": transaction.id,
            "status": transaction.status,
            "amount": amount,
            "amount_minor": to_minor(transaction.amount, transaction.currency),
            "currency": transaction.currency,
            "reference": transaction.reference,
            "created_at": _iso(transaction.created_at),
        },
        "parties": {"sender": party(sender_id), "receiver": party(receiver_id)},
        "ledger_entries": entries,
        "ledger_sum": format_amount(total),
        "event_id": event_id,
        "stages": stages,
        "dead_letters": letters,
        "overall": overall,
        "summary": summary,
        "checked_at": _iso(now),
    }
    return Journey(body=body, party_ids=frozenset(p.account_id for p in postings))
