"""Account reads and the balance derivation.

Repositories never commit. The caller owns the transaction.
"""

from __future__ import annotations

from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db.models.ledger import Account, Posting


def get(session: Session, account_id: str) -> Account | None:
    return session.get(Account, account_id)


def lock_many(session: Session, account_ids: list[str]) -> list[Account]:
    """Lock the given accounts FOR UPDATE, always in ascending id order.

    The ordering is the deadlock prevention. A transfer A->B and a
    simultaneous B->A would deadlock if each locked its own source first;
    locking in a global order means one transaction always waits rather
    than both blocking.
    """
    ordered = sorted(set(account_ids))
    stmt = (
        select(Account)
        .where(Account.id.in_(ordered))
        .order_by(Account.id)
        .with_for_update()
    )
    return list(session.execute(stmt).scalars().all())


def balance(session: Session, account_id: str) -> Decimal:
    """Derived balance. There is no stored balance column, by design."""
    stmt = select(func.coalesce(func.sum(Posting.amount), 0)).where(
        Posting.account_id == account_id
    )
    return Decimal(session.execute(stmt).scalar_one())


def postings_page(
    session: Session, account_id: str, *, limit: int = 50, offset: int = 0
) -> list[Posting]:
    stmt = (
        select(Posting)
        .where(Posting.account_id == account_id)
        .order_by(Posting.created_at.desc(), Posting.id.desc())
        .limit(limit)
        .offset(offset)
    )
    return list(session.execute(stmt).scalars().all())


def create(
    session: Session,
    *,
    account_id: str,
    holder_name: str,
    currency: str,
    account_type: str = "customer",
) -> Account:
    account = Account(
        id=account_id,
        holder_name=holder_name,
        currency=currency.upper(),
        account_type=account_type,
    )
    session.add(account)
    session.flush()
    return account
