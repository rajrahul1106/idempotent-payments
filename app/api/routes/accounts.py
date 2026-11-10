"""Account creation, detail with derived balance, and posting history."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.deps import Principal, db_session, require_account_access, require_role
from app.core.exceptions import AccountNotFound
from app.core.money import format_amount, to_minor
from app.db.repositories import accounts as repo
from app.db.session import unit_of_work
from app.schemas.common import AccountCreate, AccountOut, PostingOut

router = APIRouter(tags=["accounts"])


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
