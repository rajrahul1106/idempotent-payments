"""Local token issuer. Seeded users only."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.api.deps import db_session
from app.core.security import issue_token, verify_password
from app.db.models.messaging import AuthUser
from app.schemas.common import TokenOut, TokenRequest

router = APIRouter(tags=["auth"])


@router.post("/auth/token", response_model=TokenOut)
def create_token(
    body: TokenRequest, session: Session = Depends(db_session)
) -> TokenOut:
    user = session.get(AuthUser, body.username)
    # Verify even when the user is missing, so a timing difference does not
    # reveal which usernames exist.
    stored_hash = user.password_hash if user else "$argon2id$v=19$m=65536,t=3,p=4$x$x"
    ok = verify_password(body.password, stored_hash)

    if not user or not ok:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token, expires_in = issue_token(
        user.username, user.role, account_id=user.account_id
    )
    return TokenOut(access_token=token, expires_in=expires_in, role=user.role)
