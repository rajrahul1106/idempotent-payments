"""JWT issuing and verification, argon2 password hashing.

Deliberately simple, and the limits are stated in docs/ARCHITECTURE.md §9: no
refresh tokens, no key rotation, no external identity provider.
"""

from __future__ import annotations

import datetime as dt
import secrets
from typing import Literal

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError

from app.core.config import settings

Role = Literal["viewer", "client", "operator"]

# Roles in increasing order of privilege. A role satisfies a requirement
# when it appears at or after the required role in this ordering.
ROLE_ORDER: tuple[Role, ...] = ("viewer", "client", "operator")

_hasher = PasswordHasher()

#: A real argon2id hash, with this hasher's parameters, of a random password
#: nobody knows. Login verifies against it when the username does not exist,
#: so an unknown user costs the same argon2 work as a wrong password and the
#: response time does not reveal which usernames exist. A malformed
#: placeholder would fail parsing in microseconds instead.
DUMMY_PASSWORD_HASH = _hasher.hash(secrets.token_urlsafe(32))


def hash_password(password: str) -> str:
    return _hasher.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return _hasher.verify(password_hash, password)
    except (VerifyMismatchError, VerificationError, InvalidHashError):
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
