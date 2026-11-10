"""Domain exceptions. Framework-free: nothing here imports FastAPI.

Services raise these and the API maps them onto the one error body shape
in app/api/errors.py. Keeping them out of app.api means a process that is
not the API, such as the Phase 5 consumer, can raise and catch them
without importing the web framework.

Each class carries the HTTP status and error code from the blueprint's
error table (section 7), so the mapping lives next to the meaning.
"""

from __future__ import annotations


class DomainError(Exception):
    """Base for errors that map onto a specific HTTP status and error code."""

    status_code: int = 400
    code: str = "invalid_request"

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        if code:
            self.code = code


class AccountNotFound(DomainError):
    status_code = 404
    code = "account_not_found"


class AccountNotActive(DomainError):
    status_code = 422
    code = "account_not_active"


class CurrencyMismatch(DomainError):
    status_code = 422
    code = "currency_mismatch"


class InsufficientFunds(DomainError):
    status_code = 422
    code = "insufficient_funds"


class IdempotencyKeyReuse(DomainError):
    status_code = 422
    code = "idempotency_key_reuse"


class RequestInProgress(DomainError):
    status_code = 409
    code = "request_in_progress"


class SameAccountTransfer(DomainError):
    status_code = 422
    code = "same_account_transfer"


class Forbidden(DomainError):
    status_code = 403
    code = "forbidden"
