"""How the write path reports database outcomes that are not plain success.

G5: a check violation (SQLSTATE 23514), such as the deferred balance trigger
rejecting an unbalanced transaction, is a correctness break. It returns
500 internal_error and is logged at ERROR. Only a unique violation on the
idempotency key takes the race path.

G9, decision D2: a duplicate waits on the first request's key. When the
first request is still running at lock_timeout, the duplicate gets 409
request_in_progress.
"""

from __future__ import annotations

import logging
import time
import uuid
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

pytestmark = pytest.mark.integration


def _headers(token: str, key: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Idempotency-Key": key}


def _body(source: str, destination: str) -> dict:
    return {
        "from_account_id": source,
        "to_account_id": destination,
        "amount_minor": 10_000,
        "currency": "INR",
    }


def test_ledger_imbalance_is_500_logged_at_error_and_rolled_back(
    owner_token, make_account, monkeypatch, caplog, ledger_total
):
    import app.services.payment as payment_service
    from app.main import app

    real_posting = payment_service.Posting

    def unbalanced_posting(**kwargs):
        # The bug the deferred trigger exists to catch: the credit leg is
        # 0.0001 larger than the debit, so the transaction does not net to 0.
        if kwargs["amount"] > 0:
            kwargs["amount"] += Decimal("0.0001")
        return real_posting(**kwargs)

    source, destination = make_account(500_000), make_account(0)
    key = str(uuid.uuid4())
    headers = _headers(owner_token(source), key)
    before = ledger_total()
    # We want the 500 response itself, not the re-raised exception.
    client = TestClient(app, raise_server_exceptions=False)

    monkeypatch.setattr(payment_service, "Posting", unbalanced_posting)
    with caplog.at_level(logging.INFO, logger="app.services.payment"):
        resp = client.post("/v1/payments", json=_body(source, destination), headers=headers)
    monkeypatch.undo()

    assert resp.status_code == 500
    assert resp.json()["error"]["code"] == "internal_error"

    service_errors = [
        r
        for r in caplog.records
        if r.name == "app.services.payment" and r.levelno == logging.ERROR
    ]
    assert service_errors, "the rejection must be logged at ERROR"
    assert "23514" in service_errors[0].getMessage()
    assert not any("idempotency race" in r.getMessage() for r in caplog.records)

    # The trigger rolled the whole transaction back: no money moved and the
    # key was not stored, so the same request succeeds once the bug is gone.
    assert ledger_total() == before
    retry = client.post("/v1/payments", json=_body(source, destination), headers=headers)
    assert retry.status_code == 201


def test_duplicate_that_outwaits_the_lock_timeout_gets_409(
    client, owner_token, make_account
):
    from app.core.config import settings
    from app.db.session import engine

    source, destination = make_account(500_000), make_account(0)
    key = str(uuid.uuid4())
    headers = _headers(owner_token(source), key)

    # A first request with this key that is still in flight: its key row is
    # inserted and not yet committed, exactly as inside create_payment.
    with engine.connect() as first_request:
        first_request.execute(
            text(
                "INSERT INTO idempotency_keys (key, fingerprint, state) "
                "VALUES (:key, :fingerprint, 'in_progress')"
            ),
            {"key": key, "fingerprint": "0" * 64},
        )
        started = time.monotonic()
        resp = client.post("/v1/payments", json=_body(source, destination), headers=headers)
        waited = time.monotonic() - started
        first_request.rollback()

    assert resp.status_code == 409, resp.text
    assert resp.json()["error"]["code"] == "request_in_progress"
    # It really waited on the key rather than answering straight away.
    assert waited >= 0.8 * settings.db_lock_timeout_ms / 1000

    # The first request never committed, so the key is free and the
    # payment goes through on the next attempt.
    retry = client.post("/v1/payments", json=_body(source, destination), headers=headers)
    assert retry.status_code == 201
