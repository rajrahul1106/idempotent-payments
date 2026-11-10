"""Request-scoped dependencies: database sessions and role checks.

Role checks live here as dependencies rather than inside handlers, so
every route states its required role in its signature and an audit is a
grep rather than a code read.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

import jwt
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.orm import Session

from app.core.security import Role, decode_token, role_satisfies
from app.db.session import SessionFactory

bearer = HTTPBearer(auto_error=False)


@dataclass(frozen=True)
class Principal:
    username: str
    role: str
    account_id: str | None = None


def db_session() -> Iterator[Session]:
    """Read session for handlers. Write paths open their own unit_of_work."""
    session = SessionFactory()
    try:
        yield session
    finally:
        session.close()


def current_principal(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> Principal:
    if credentials is None or not credentials.credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:
        claims = decode_token(credentials.credentials)
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    return Principal(
        username=claims.get("sub", ""),
        role=claims.get("role", ""),
        account_id=claims.get("account_id"),
    )


def require_role(required: Role):
    """Dependency factory. Usage: Depends(require_role('operator'))."""

    def _check(principal: Principal = Depends(current_principal)) -> Principal:
        if not role_satisfies(principal.role, required):
            raise HTTPException(
                status_code=403,
                detail=f"Role '{principal.role}' cannot perform this action",
            )
        return principal

    return _check


# Blueprint decision D4. The role order above says which endpoints a role
# may call; this says which rows it may see. viewer and operator read
# everything. Any other role, which today means client, reads only the
# account its token names.
READ_ALL_ROLES: frozenset[str] = frozenset({"viewer", "operator"})


def can_read_account(principal: Principal, account_id: str) -> bool:
    if principal.role in READ_ALL_ROLES:
        return True
    return principal.account_id is not None and principal.account_id == account_id


def require_account_access(
    account_id: str,
    principal: Principal = Depends(require_role("viewer")),
) -> Principal:
    """Read access to one account: viewer and operator any, client its own."""
    if not can_read_account(principal, account_id):
        raise HTTPException(
            status_code=403,
            detail="Clients may read only their own account",
        )
    return principal


def rate_limit(request: Request) -> None:
    """In-memory token bucket, per subject.

    Correct for a single API process only. Running two API containers
    gives each its own bucket. This limitation is stated in the blueprint
    rather than papered over.
    """
    from app.core.rate_limit import limiter

    auth = request.headers.get("Authorization", "")
    subject = auth[-24:] if auth else (request.client.host if request.client else "-")
    if not limiter.allow(subject):
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers={"Retry-After": "60"},
        )
