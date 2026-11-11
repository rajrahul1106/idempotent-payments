"""A busy account answers 503 service_busy with Retry-After, never 500.

When a payment cannot get an ACCOUNT row lock within lock_timeout (3 s),
the transaction rolls back, idempotency key reservation included, and the
API returns 503 with Retry-After: 1. A client that retries with the same
key is processed normally. A lock timeout on the idempotency KEY is still
D2's 409 (tests/api/test_payment_error_mapping.py).

To make the account reliably busy, a separate session holds the source
account's row lock past lock_timeout while the 20 payments arrive.
"""

from __future__ import annotations

import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import pytest
from sqlalchemy import text

from app.db.session import engine, read_session
from app.services.reconciliation import FAIL, run_checks

pytestmark = pytest.mark.integration

PAYMENTS = 20
AMOUNT_MINOR = 1_000  # 10.00 INR


def _post(client, token, source, destination, key):
    return client.post(
        "/v1/payments",
        json={"from_account_id": source, "to_account_id": destination,
              "amount_minor": AMOUNT_MINOR, "currency": "INR"},
        headers={"Authorization": f"Bearer {token}", "Idempotency-Key": key},
    )


def _hold_lock(account: str, seconds: float, held: threading.Event) -> None:
    with engine.connect() as conn:
        conn.execute(text("SET LOCAL lock_timeout = 0"))
        conn.execute(text("SELECT id FROM accounts WHERE id = :a FOR UPDATE"), {"a": account})
        held.set()
        time.sleep(seconds)
        conn.rollback()


def _balance(account: str) -> Decimal:
    with read_session() as session:
        return session.execute(
            text("SELECT COALESCE(SUM(amount), 0) FROM postings WHERE account_id = :a"),
            {"a": account},
        ).scalar_one()


def test_busy_account_returns_503_and_same_key_retries_all_succeed(
    client, owner_token, make_account, ledger_total
):
    source, destination = make_account(100_000), make_account(0)  # 1,000.00 INR
    token = owner_token(source)
    keys = [str(uuid.uuid4()) for _ in range(PAYMENTS)]
    ledger_before = ledger_total()

    held = threading.Event()
    holder = threading.Thread(target=_hold_lock, args=(source, 4.0, held))
    holder.start()
    assert held.wait(5)
    with ThreadPoolExecutor(max_workers=PAYMENTS) as pool:
        first = list(pool.map(lambda k: _post(client, token, source, destination, k), keys))
    holder.join()

    codes = [r.status_code for r in first]
    assert set(codes) <= {201, 503}, codes  # never 500
    busy = [k for k, r in zip(keys, first) if r.status_code == 503]
    assert busy, "the account was never busy; the test did not exercise the 503 path"
    for response in first:
        if response.status_code == 503:
            assert response.headers["Retry-After"] == "1"
            assert response.json()["error"]["code"] == "service_busy"

    # The 503s left nothing behind: no idempotency key, no transaction.
    with read_session() as session:
        reserved = session.execute(
            text("SELECT count(*) FROM idempotency_keys WHERE key = ANY(CAST(:k AS uuid[]))"),
            {"k": busy},
        ).scalar_one()
    assert reserved == 0

    # Retry every 503 with its ORIGINAL key until it succeeds.
    pending, rounds, outcomes = list(busy), 0, {}
    while pending:
        rounds += 1
        assert rounds <= 10, f"{len(pending)} still busy after 10 rounds"
        time.sleep(1)  # Retry-After
        with ThreadPoolExecutor(max_workers=len(pending)) as pool:
            again = list(pool.map(lambda k: _post(client, token, source, destination, k), pending))
        for key, response in zip(pending, again):
            assert response.status_code in (201, 503), response.text
            outcomes[key] = response.status_code
        pending = [k for k, r in zip(pending, again) if r.status_code == 503]

    # Exactly one payment per key, and the arithmetic holds.
    with read_session() as session:
        completed = session.execute(
            text("SELECT count(*) FROM idempotency_keys "
                 "WHERE key = ANY(CAST(:k AS uuid[])) AND state = 'completed'"),
            {"k": keys},
        ).scalar_one()
        outbox_rows = session.execute(
            text("SELECT count(*) FROM outbox WHERE payload->>'from_account_id' = :a"),
            {"a": source},
        ).scalar_one()
    assert completed == PAYMENTS
    assert outbox_rows == PAYMENTS  # I3's precondition: one committed event per payment
    assert _balance(source) == Decimal("1000.0000") - PAYMENTS * Decimal("10.0000")
    assert _balance(destination) == PAYMENTS * Decimal("10.0000")
    assert ledger_total() == ledger_before  # I2

    # I1, I2 and I4 over the whole database. (I3 is the relay's to keep; no
    # relay runs against the test database.)
    checks = {c.name: c.status for c in run_checks(record=False).checks}
    for name in ("no_duplicate_event_ids", "ledger_balances_to_zero", "no_negative_balances"):
        assert checks[name] != FAIL, checks


def test_same_key_retry_after_503_is_processed_normally(client, owner_token, make_account):
    """One request, made busy, then retried with the same key."""
    source, destination = make_account(10_000), make_account(0)
    token, key = owner_token(source), str(uuid.uuid4())

    held = threading.Event()
    holder = threading.Thread(target=_hold_lock, args=(source, 3.5, held))
    holder.start()
    assert held.wait(5)
    busy = _post(client, token, source, destination, key)
    holder.join()

    assert busy.status_code == 503, busy.text
    assert busy.headers["Retry-After"] == "1"
    retried = _post(client, token, source, destination, key)
    assert retried.status_code == 201, retried.text  # processed, not replayed
    assert "Idempotency-Replayed" not in retried.headers
    replayed = _post(client, token, source, destination, key)
    assert replayed.status_code == 200 and replayed.headers["Idempotency-Replayed"] == "true"
    assert replayed.json()["payment_id"] == retried.json()["payment_id"]
