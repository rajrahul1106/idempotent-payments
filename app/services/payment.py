"""The write path. This module owns the transaction boundary.

Blueprint section 8, steps 5 to 10. Everything below happens inside one
Postgres transaction: the transaction row, both postings, the idempotency
key and the outbox row. Either all five land or none do, which is what
removes the dual-write problem.
"""

from __future__ import annotations

import datetime as dt
import logging
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError
from sqlalchemy.orm import Session

from app.core.exceptions import (
    AccountBusy,
    AccountNotActive,
    AccountNotFound,
    CurrencyMismatch,
    Forbidden,
    IdempotencyKeyReuse,
    InsufficientFunds,
    RequestInProgress,
    SameAccountTransfer,
)
from app.core.money import format_amount, to_decimal, to_minor
from app.db.models.ledger import Account, Posting, Transaction
from app.db.models.messaging import IdempotencyKey, OutboxEvent
from app.db.repositories import accounts as account_repo
from app.db.repositories import outbox as outbox_repo
from app.db.session import unit_of_work
from app.services import idempotency

logger = logging.getLogger(__name__)

EVENT_TYPE = "payment.posted"
SCHEMA_VERSION = 1

# How long a loser of the idempotency race waits for the winner to finish
# before giving up and returning 409.
_RACE_POLL_ATTEMPTS = 20
_RACE_POLL_INTERVAL_S = 0.05

# The SQLSTATEs the write path tells apart. Anything else is a 500.
UNIQUE_VIOLATION = "23505"
CHECK_VIOLATION = "23514"
LOCK_NOT_AVAILABLE = "55P03"
IDEMPOTENCY_KEY_CONSTRAINT = "idempotency_keys_pkey"


def _sqlstate(exc: DBAPIError) -> str | None:
    return getattr(exc.orig, "sqlstate", None)


def _is_idempotency_key_conflict(exc: IntegrityError) -> bool:
    """True only for a duplicate on the idempotency key itself."""
    diag = getattr(exc.orig, "diag", None)
    return (
        _sqlstate(exc) == UNIQUE_VIOLATION
        and getattr(diag, "constraint_name", None) == IDEMPOTENCY_KEY_CONSTRAINT
    )


@dataclass(frozen=True)
class Actor:
    """Who is asking, taken from the verified token.

    Blueprint decision D4: a client debits only the account its token names,
    and only an operator debits a system account.
    """

    role: str
    account_id: str | None = None


@dataclass(frozen=True)
class PaymentCommand:
    idempotency_key: str
    from_account_id: str
    to_account_id: str
    amount_minor: int
    currency: str
    reference: str | None = None
    metadata: dict[str, Any] | None = None
    traceparent: str | None = None
    # Required and keyword-only, so no caller can forget to say who it is.
    actor: Actor = field(kw_only=True)

    def body_for_fingerprint(self) -> dict[str, Any]:
        """Exactly the fields a client sent. The key is not part of its own
        fingerprint, and traceparent is transport, not request content."""
        return {
            "from_account_id": self.from_account_id,
            "to_account_id": self.to_account_id,
            "amount_minor": self.amount_minor,
            "currency": self.currency,
            "reference": self.reference,
            "metadata": self.metadata or {},
        }


@dataclass(frozen=True)
class PaymentResult:
    body: dict[str, Any]
    status_code: int
    replayed: bool


def _authorize_source(command: PaymentCommand) -> None:
    """D4, first half: an operator debits any account, a client only its own.

    Runs before the idempotency lookup, so a caller who does not own the
    source account cannot replay someone else's stored response either.
    The system-account half needs the account row and runs under the lock.
    """
    actor = command.actor
    if actor.role == "operator":
        return
    if (
        actor.role == "client"
        and actor.account_id is not None
        and actor.account_id == command.from_account_id
    ):
        return
    raise Forbidden(
        f"Role '{actor.role}' may not debit account {command.from_account_id}"
    )


def create_payment(command: PaymentCommand) -> PaymentResult:
    """Apply a payment at most once for the given idempotency key."""
    _authorize_source(command)
    body_fingerprint = idempotency.fingerprint(command.body_for_fingerprint())

    try:
        return _attempt(command, body_fingerprint)
    except IntegrityError as exc:
        if not _is_idempotency_key_conflict(exc):
            if _sqlstate(exc) == CHECK_VIOLATION:
                # The database refused a write that breaks a ledger rule,
                # most likely the deferred balance trigger at COMMIT. That is
                # a correctness break, not a race, so it surfaces as a 500.
                logger.error(
                    "database rejected payment for idempotency key %s "
                    "(SQLSTATE %s): %s",
                    command.idempotency_key,
                    CHECK_VIOLATION,
                    exc.orig,
                )
            raise
        # Another request with the same key won the INSERT and has committed.
        # Serve its stored response.
        logger.info(
            "idempotency race on key %s, waiting for the winner",
            command.idempotency_key,
        )
        return _await_winner(command.idempotency_key, body_fingerprint)


def _attempt(command: PaymentCommand, body_fingerprint: str) -> PaymentResult:
    with unit_of_work() as session:
        lookup = idempotency.inspect(
            session, command.idempotency_key, body_fingerprint
        )

        if lookup.outcome is idempotency.Outcome.MISMATCH:
            raise IdempotencyKeyReuse(
                "This idempotency key was already used with a different "
                "request body"
            )

        if lookup.outcome is idempotency.Outcome.REPLAY:
            assert lookup.record is not None
            return PaymentResult(
                body=lookup.record.response_body or {},
                status_code=200,
                replayed=True,
            )

        if lookup.outcome is idempotency.Outcome.IN_PROGRESS:
            raise RequestInProgress(
                "A request with this idempotency key is still being processed"
            )

        record = lookup.record
        if record is None:
            try:
                record = idempotency.reserve(
                    session, command.idempotency_key, body_fingerprint
                )
            except OperationalError as exc:
                if _sqlstate(exc) != LOCK_NOT_AVAILABLE:
                    raise
                # Decision D2: a duplicate waits on the first request's key.
                # If that request is still running when lock_timeout expires,
                # it is in flight, which is exactly what 409 means.
                raise RequestInProgress(
                    "A request with this idempotency key is still being processed"
                ) from exc
        else:
            # Previous attempt failed; this key is free to use again.
            record.state = "in_progress"
            record.fingerprint = body_fingerprint
            session.flush()

        body = _post_payment(session, command, record)

    return PaymentResult(body=body, status_code=201, replayed=False)


def _post_payment(
    session: Session, command: PaymentCommand, record: IdempotencyKey
) -> dict[str, Any]:
    """Steps 5 to 9. Called with an open transaction; never commits."""
    if command.from_account_id == command.to_account_id:
        raise SameAccountTransfer("Source and destination must differ")

    # Step 5: lock both accounts, always in ascending id order. This
    # ordering is the deadlock prevention for A->B against B->A.
    try:
        locked = account_repo.lock_many(
            session, [command.from_account_id, command.to_account_id]
        )
    except OperationalError as exc:
        if _sqlstate(exc) != LOCK_NOT_AVAILABLE:
            raise
        # The account is busy: other payments held its row lock for longer
        # than lock_timeout. Raising rolls back this whole transaction, the
        # idempotency key reservation with it, so the client can retry with
        # the same key. A lock timeout on the KEY itself is D2's 409, above.
        logger.info(
            "account lock not granted within lock_timeout (%s -> %s); 503 service_busy",
            command.from_account_id,
            command.to_account_id,
        )
        raise AccountBusy(
            "An account in this payment is busy; retry with the same Idempotency-Key"
        ) from exc
    by_id = {account.id: account for account in locked}

    source = by_id.get(command.from_account_id)
    destination = by_id.get(command.to_account_id)
    if source is None:
        raise AccountNotFound(f"Account {command.from_account_id} not found")
    if destination is None:
        raise AccountNotFound(f"Account {command.to_account_id} not found")

    # D4, second half: system accounts skip the balance check below, so
    # debiting one creates money. Only an operator may do it.
    if source.account_type == "system" and command.actor.role != "operator":
        raise Forbidden(f"Only an operator may debit system account {source.id}")

    for account in (source, destination):
        if account.status != "active":
            raise AccountNotActive(
                f"Account {account.id} is {account.status}"
            )

    currency = command.currency.upper()
    if source.currency != currency or destination.currency != currency:
        raise CurrencyMismatch(
            f"Accounts hold {source.currency}/{destination.currency}, "
            f"request is {currency}"
        )

    amount: Decimal = to_decimal(command.amount_minor, currency)

    # Step 6: balance check INSIDE the lock. Outside it, two concurrent
    # withdrawals both read the old balance and both pass.
    if not source.allows_negative_balance:
        current = account_repo.balance(session, source.id)
        if current < amount:
            raise InsufficientFunds(
                f"Account {source.id} has balance {format_amount(current)}, "
                f"requested {format_amount(amount)}"
            )

    event_id = str(uuid.uuid4())
    transaction_id = f"pay_{uuid.uuid4().hex[:24]}"

    # Step 7: the transaction row.
    session.add(
        Transaction(
            id=transaction_id,
            status="posted",
            amount=amount,
            currency=currency,
            reference=command.reference,
            metadata_json=command.metadata or {},
        )
    )

    # Step 8: two postings, equal and opposite. The deferred trigger
    # asserts they sum to zero at COMMIT.
    session.add_all(
        [
            Posting(
                transaction_id=transaction_id,
                account_id=source.id,
                event_id=event_id,
                amount=-amount,
                currency=currency,
            ),
            Posting(
                transaction_id=transaction_id,
                account_id=destination.id,
                event_id=event_id,
                amount=amount,
                currency=currency,
            ),
        ]
    )

    response_body = {
        "payment_id": transaction_id,
        "status": "posted",
        "from_account_id": source.id,
        "to_account_id": destination.id,
        "amount_minor": command.amount_minor,
        "currency": currency,
        "event_id": event_id,
        "postings": [
            {"account_id": source.id, "amount": format_amount(-amount)},
            {"account_id": destination.id, "amount": format_amount(amount)},
        ],
        "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }

    # Step 9: the outbox row, committed with the money. Publishing is now a
    # consequence of this transaction rather than a second write that can
    # diverge from it.
    session.add(
        OutboxEvent(
            event_id=event_id,
            event_type=EVENT_TYPE,
            schema_version=SCHEMA_VERSION,
            # Partition by the debited account so every event touching one
            # account lands on one partition in order.
            partition_key=source.id,
            payload={
                "event_id": event_id,
                "event_type": EVENT_TYPE,
                "schema_version": SCHEMA_VERSION,
                "transaction_id": transaction_id,
                "from_account_id": source.id,
                "to_account_id": destination.id,
                "amount_minor": command.amount_minor,
                "currency": currency,
                "reference": command.reference,
                "occurred_at": response_body["created_at"],
            },
            traceparent=command.traceparent,
        )
    )
    # Wakes the relay on commit (§15: NOTIFY on top of polling, never alone).
    outbox_repo.notify(session, event_id)

    idempotency.complete(
        record,
        response_body=response_body,
        response_code=201,
        transaction_id=transaction_id,
    )

    return response_body


def _await_winner(key: str, body_fingerprint: str) -> PaymentResult:
    """Poll for the concurrent winner's stored response."""
    from app.db.session import read_session

    for _ in range(_RACE_POLL_ATTEMPTS):
        with read_session() as session:
            record = session.get(IdempotencyKey, key)
            if record is not None:
                if record.fingerprint != body_fingerprint:
                    raise IdempotencyKeyReuse(
                        "This idempotency key was already used with a "
                        "different request body"
                    )
                if record.state == "completed":
                    return PaymentResult(
                        body=record.response_body or {},
                        status_code=200,
                        replayed=True,
                    )
        time.sleep(_RACE_POLL_INTERVAL_S)

    raise RequestInProgress(
        "A request with this idempotency key is still being processed"
    )
