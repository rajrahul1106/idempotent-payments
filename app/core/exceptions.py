"""Domain exceptions. Framework-free: nothing here imports FastAPI.

Services raise these and the API maps them onto the one error body shape
in app/api/errors.py. Keeping them out of app.api means a process that is
not the API, such as the Phase 5 consumer, can raise and catch them
without importing the web framework.

Each class carries the HTTP status and error code from the API's
error table (docs/ARCHITECTURE.md §7, docs/API.md), so the mapping lives next to the meaning.
"""

from __future__ import annotations


class DomainError(Exception):
    """Base for errors that map onto a specific HTTP status and error code."""

    status_code: int = 400
    code: str = "invalid_request"
    #: Extra response headers, e.g. Retry-After.
    headers: dict[str, str] | None = None

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


class DlqMessageNotFound(DomainError):
    status_code = 404
    code = "dlq_message_not_found"


class DlqAlreadyReplayed(DomainError):
    status_code = 409
    code = "dlq_already_replayed"


class BrokerUnavailable(DomainError):
    status_code = 503
    code = "service_unavailable"


class AccountBusy(DomainError):
    """An account row lock was not granted within lock_timeout.

    Not 409: request_in_progress means this same idempotency key is in
    flight (D2), and this is a different request. Not 500: nothing is
    broken, the account is busy. The transaction rolled back, idempotency
    key reservation included, so retrying with the same key is processed
    normally.
    """

    status_code = 503
    code = "service_busy"
    headers = {"Retry-After": "1"}


class DemoFaultsDisabled(DomainError):
    """A payment asked for a demo fault while DEMO_FAULTS_ENABLED is off (D27).
    Refused before anything is written."""

    status_code = 422
    code = "demo_faults_disabled"
