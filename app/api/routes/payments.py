"""POST /v1/payments: the only endpoint that moves money."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from fastapi.responses import JSONResponse
from opentelemetry import trace
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app.api.deps import (
    READ_ALL_ROLES,
    Principal,
    db_session,
    rate_limit,
    require_role,
)
from app.core import metrics
from app.core.config import settings
from app.core.exceptions import DemoFaultsDisabled, DomainError, IdempotencyKeyReuse
from app.core.telemetry import current_traceparent, key_digest
from app.db.errors import database_unavailable
from app.db.models.ledger import Transaction
from app.schemas.payments import PaymentRequest
from app.services import payment_views
from app.services.payment import Actor, PaymentCommand, create_payment

router = APIRouter(tags=["payments"])


class MissingIdempotencyKey(DomainError):
    status_code = 400
    code = "invalid_request"


@router.post("/payments")
def create(
    body: PaymentRequest,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    traceparent: str | None = Header(default=None),
    _: None = Depends(rate_limit),
    principal: Principal = Depends(require_role("client")),
) -> JSONResponse:
    try:
        result = _create(body, idempotency_key, traceparent, principal)
    except DomainError as exc:
        # Labels are bounded: a fixed set of error codes, and no currency for
        # a refused request (its currency is whatever the caller sent).
        metrics.PAYMENTS.labels(status=exc.code, currency="none").inc()
        if isinstance(exc, IdempotencyKeyReuse):
            metrics.IDEMPOTENCY_REPLAYS.labels(outcome="mismatch").inc()
        raise
    except Exception as exc:
        unavailable = isinstance(exc, OperationalError) and database_unavailable(exc)
        metrics.PAYMENTS.labels(
            status="service_unavailable" if unavailable else "internal_error", currency="none"
        ).inc()
        raise
    metrics.PAYMENTS.labels(
        status="replayed" if result.replayed else "created", currency=body.currency
    ).inc()
    if result.replayed:
        metrics.IDEMPOTENCY_REPLAYS.labels(outcome="replayed").inc()

    headers = {"Idempotency-Replayed": "true"} if result.replayed else {}
    return JSONResponse(
        status_code=result.status_code, content=result.body, headers=headers
    )


def _create(body: PaymentRequest, idempotency_key, traceparent, principal: Principal):
    # A missing key is rejected rather than processed unprotected. Letting
    # it through would make retry safety optional, which is the whole point.
    if not idempotency_key:
        raise MissingIdempotencyKey("Idempotency-Key header is required")
    try:
        uuid.UUID(idempotency_key)
    except ValueError:
        raise MissingIdempotencyKey("Idempotency-Key must be a UUID")

    # D27: the demo page's fault switch is this process's own setting. With
    # it off, a payment asking for a fault is refused here, before the
    # idempotency key or anything else is written.
    if not settings.demo_faults_enabled and "demo_fault" in (body.metadata or {}):
        raise DemoFaultsDisabled("Demo faults are disabled on this server")

    # Attributes on the request's server span (§5.8). The key is hashed:
    # a raw idempotency key never goes into a trace.
    span = trace.get_current_span()
    span.set_attribute("account_id", body.from_account_id)
    span.set_attribute("idempotency_key.sha256_prefix", key_digest(idempotency_key))

    command = PaymentCommand(
        idempotency_key=idempotency_key,
        from_account_id=body.from_account_id,
        to_account_id=body.to_account_id,
        amount_minor=body.amount_minor,
        currency=body.currency,
        reference=body.reference,
        metadata=body.metadata,
        # This request's own span context, so the relay and consumer spans
        # join this trace. It is a child of the caller's traceparent when
        # one was sent. Without tracing, the caller's header is kept as is.
        traceparent=current_traceparent() or traceparent,
        actor=Actor(role=principal.role, account_id=principal.account_id),
    )

    result = create_payment(command)
    if result.body.get("event_id"):
        span.set_attribute("event_id", result.body["event_id"])
    span.set_attribute("payment.replayed", result.replayed)
    return result


class PaymentNotFound(DomainError):
    status_code = 404
    code = "payment_not_found"


def _require_party(principal: Principal, parties) -> None:
    """D4: viewer and operator read any payment; a client only one it pays
    or receives. Shared by GET /v1/payments/{id} and its journey."""
    if principal.role not in READ_ALL_ROLES and principal.account_id not in parties:
        raise HTTPException(
            status_code=403,
            detail="Clients may read only payments they pay or receive",
        )


@router.get("/payments")
def list_payments(
    account_id: str | None = Query(None, description="Only payments with a posting on this account"),
    limit: int = Query(20, ge=1, le=100),
    principal: Principal = Depends(require_role("viewer")),
    session: Session = Depends(db_session),
) -> JSONResponse:
    """Recent payments, newest first. A client sees only payments touching
    its own account, and asking for another account is 403."""
    if principal.role not in READ_ALL_ROLES:
        if account_id is not None and account_id != principal.account_id:
            raise HTTPException(
                status_code=403,
                detail="Clients may list only payments on their own account",
            )
        account_id = principal.account_id
        if account_id is None:
            return JSONResponse(status_code=200, content=[])
    return JSONResponse(
        status_code=200,
        content=payment_views.recent_payments(session, account_id=account_id, limit=limit),
    )


@router.get("/payments/{payment_id}/journey")
def payment_journey(
    payment_id: str,
    principal: Principal = Depends(require_role("viewer")),
) -> JSONResponse:
    """Every recorded stage of one payment, from one database snapshot."""
    result = payment_views.journey(payment_id)
    if result is None:
        raise PaymentNotFound(f"Payment {payment_id} not found")
    _require_party(principal, result.party_ids)
    return JSONResponse(status_code=200, content=result.body)


@router.get("/payments/{payment_id}")
def get_payment(
    payment_id: str,
    # D4: viewer and operator read any payment; a client reads only the
    # payments it pays or receives, checked against the postings below.
    principal: Principal = Depends(require_role("viewer")),
    session: Session = Depends(db_session),
) -> JSONResponse:
    transaction = session.get(Transaction, payment_id)
    if transaction is None:
        raise PaymentNotFound(f"Payment {payment_id} not found")
    _require_party(principal, {posting.account_id for posting in transaction.postings})

    return JSONResponse(
        status_code=200,
        content={
            "payment_id": transaction.id,
            "status": transaction.status,
            "amount": str(transaction.amount),
            "currency": transaction.currency,
            "reference": transaction.reference,
            "postings": [
                {
                    "account_id": p.account_id,
                    "amount": str(p.amount),
                    "event_id": str(p.event_id),
                }
                for p in transaction.postings
            ],
            "created_at": transaction.created_at.isoformat(),
        },
    )
