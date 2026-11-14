"""Account creation, detail with derived balance, and posting history."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.deps import (
    READ_ALL_ROLES,
    Principal,
    can_read_account,
    db_session,
    require_account_access,
    require_role,
)
from app.core.exceptions import AccountNotFound
from app.core.money import format_amount, to_minor
from app.db.repositories import accounts as repo
from app.db.session import unit_of_work
from app.schemas.common import AccountCreate, AccountListItem, AccountOut, PostingOut
from app.services import payment_views
from app.services.payment import Actor, can_debit

router = APIRouter(tags=["accounts"])


@router.get("/accounts", response_model=list[AccountListItem])
def list_accounts(
    limit: int = Query(100, ge=1, le=500),
    principal: Principal = Depends(require_role("viewer")),
    session: Session = Depends(db_session),
) -> list[AccountListItem]:
    """The accounts the caller may see, oldest first.

    viewer and operator: every account, with balances. client (D26): its own
    account with its balance, plus every customer account by id and holder
    name only, so it can choose whom to pay; no system accounts. can_debit
    is the payment service's own D4 rule, not a copy of it.
    """
    read_all = principal.role in READ_ALL_ROLES
    accounts = payment_views.accounts_page(
        session,
        limit=limit,
        customers_only=not read_all,
        always_include=None if read_all else principal.account_id,
    )
    visible = [a.id for a in accounts if can_read_account(principal, a.id)]
    balances = payment_views.balances(session, visible)
    actor = Actor(role=principal.role, account_id=principal.account_id)

    out = []
    for account in accounts:
        details = can_read_account(principal, account.id)
        bal = balances.get(account.id)
        out.append(
            AccountListItem(
                id=account.id,
                holder_name=account.holder_name,
                currency=account.currency if details else None,
                account_type=account.account_type if details else None,
                balance=format_amount(bal) if bal is not None else None,
                balance_minor=to_minor(bal, account.currency) if bal is not None else None,
                can_debit=can_debit(actor, account.id, account.account_type),
            )
        )
    return out


def _to_out(session: Session, account) -> AccountOut:
    bal = repo.balance(session, account.id)
    return AccountOut(
        id=account.id,
        holder_name=account.holder_name,
        currency=account.currency,
        account_type=account.account_type,
        status=account.status,
        balance=format_amount(bal),
        balance_minor=to_minor(bal, account.currency),
        created_at=account.created_at,
    )


@router.post("/accounts", response_model=AccountOut, status_code=201)
def create_account(
    body: AccountCreate,
    _: Principal = Depends(require_role("operator")),
) -> AccountOut:
    account_id = f"acc_{uuid.uuid4().hex[:20]}"
    with unit_of_work() as session:
        account = repo.create(
            session,
            account_id=account_id,
            holder_name=body.holder_name,
            currency=body.currency,
            account_type=body.account_type,
        )
        out = _to_out(session, account)
    return out


@router.get("/accounts/{account_id}", response_model=AccountOut)
def get_account(
    account_id: str,
    _: Principal = Depends(require_account_access),
    session: Session = Depends(db_session),
) -> AccountOut:
    account = repo.get(session, account_id)
    if account is None:
        raise AccountNotFound(f"Account {account_id} not found")
    return _to_out(session, account)


@router.get("/accounts/{account_id}/postings", response_model=list[PostingOut])
def list_postings(
    account_id: str,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _: Principal = Depends(require_account_access),
    session: Session = Depends(db_session),
) -> list[PostingOut]:
    account = repo.get(session, account_id)
    if account is None:
        raise AccountNotFound(f"Account {account_id} not found")

    rows = repo.postings_page(session, account_id, limit=limit, offset=offset)
    return [
        PostingOut(
            id=p.id,
            transaction_id=p.transaction_id,
            event_id=str(p.event_id),
            amount=str(p.amount),
            currency=p.currency,
            created_at=p.created_at,
        )
        for p in rows
    ]
