"""JWT issuing and verification, argon2 password hashing.

Deliberately simple, and the limits are stated in the blueprint: no
refresh tokens, no key rotation, no external identity provider.
"""

from __future__ import annotations

import datetime as dt
from typing import Literal

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError, VerificationError

from app.core.config import settings

Role = Literal["viewer", "client", "operator"]

# Roles in increasing order of privilege. A role satisfies a requirement
# when it appears at or after the required role in this ordering.
ROLE_ORDER: tuple[Role, ...] = ("viewer", "client", "operator")

_hasher = PasswordHasher()


def hash_password(password: str) -> str:
    return _hasher.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return _hasher.verify(password_hash, password)
    except (VerifyMismatchError, VerificationError):
        return False


def issue_token(
    subject: str, role: Role, *, account_id: str | None = None
) -> tuple[str, int]:
    """Return (token, expires_in_seconds)."""
    now = dt.datetime.now(dt.timezone.utc)
    expires_in = settings.jwt_expiry_minutes * 60
    payload = {
        "sub": subject,
        "role": role,
        "iat": int(now.timestamp()),
        "exp": int((now + dt.timedelta(seconds=expires_in)).timestamp()),
    }
    if account_id:
        payload["account_id"] = account_id
    token = jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)
    return token, expires_in


def decode_token(token: str) -> dict:
    """Decode and verify. Raises jwt.PyJWTError on anything invalid."""
    return jwt.decode(
        token, settings.jwt_secret, algorithms=[settings.jwt_algorithm]
    )


def role_satisfies(actual: str, required: Role) -> bool:
    try:
        return ROLE_ORDER.index(actual) >= ROLE_ORDER.index(required)
    except ValueError:
        return False
