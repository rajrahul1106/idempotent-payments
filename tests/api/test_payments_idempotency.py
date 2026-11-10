"""Phase 3 gate: the four idempotency cases from the blueprint."""

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest
from sqlalchemy import text

from app.db.session import read_session

pytestmark = pytest.mark.integration


def _headers(token: str, key: str | None = None) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    if key:
        headers["Idempotency-Key"] = key
    return headers


def _outbox_rows(event_id: str) -> int:
    with read_session() as session:
        return int(
            session.execute(
                text("SELECT count(*) FROM outbox WHERE event_id = :e"),
                {"e": event_id},
            ).scalar_one()
        )


def test_same_key_same_body_replays(client, owner_token, make_account):
    source, destination = make_account(500_000), make_account(0)
    key = str(uuid.uuid4())
    body = {
        "from_account_id": source,
        "to_account_id": destination,
        "amount_minor": 100_000,
        "currency": "INR",
    }

    first = client.post("/v1/payments", json=body, headers=_headers(owner_token(source), key))
    assert first.status_code == 201
    assert "Idempotency-Replayed" not in first.headers

    second = client.post(
        "/v1/payments", json=body, headers=_headers(owner_token(source), key)
    )
    assert second.status_code == 200
    assert second.headers["Idempotency-Replayed"] == "true"
    assert second.json() == first.json()


def test_same_key_different_body_is_422(client, owner_token, make_account):
    source, destination = make_account(500_000), make_account(0)
    key = str(uuid.uuid4())
    base = {
        "from_account_id": source,
        "to_account_id": destination,
        "amount_minor": 100_000,
        "currency": "INR",
    }

    assert (
        client.post("/v1/payments", json=base, headers=_headers(owner_token(source), key))
    ).status_code == 201

    changed = {**base, "amount_minor": 999_999}
    resp = client.post(
        "/v1/payments", json=changed, headers=_headers(owner_token(source), key)
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "idempotency_key_reuse"

    # And the second payment did not happen
    with read_session() as session:
        count = session.execute(
            text(
                "SELECT count(*) FROM postings WHERE account_id = :a AND amount < 0"
            ),
            {"a": source},
        ).scalar_one()
    assert count == 1


def test_different_keys_same_body_create_two_payments(
    client, owner_token, make_account
):
    """Two genuinely identical payments are legitimate and must both post."""
    source, destination = make_account(500_000), make_account(0)
    body = {
        "from_account_id": source,
        "to_account_id": destination,
        "amount_minor": 100_000,
        "currency": "INR",
    }

    first = client.post(
        "/v1/payments", json=body, headers=_headers(owner_token(source), str(uuid.uuid4()))
    )
    second = client.post(
        "/v1/payments", json=body, headers=_headers(owner_token(source), str(uuid.uuid4()))
    )
    assert first.status_code == 201 and second.status_code == 201
    assert first.json()["payment_id"] != second.json()["payment_id"]


def test_key_reusable_after_a_failed_request(client, owner_token, make_account):
    """A request that failed reserves nothing; the key is free again."""
    source, destination = make_account(10_000), make_account(0)
    key = str(uuid.uuid4())

    too_much = {
        "from_account_id": source,
        "to_account_id": destination,
        "amount_minor": 999_999,
        "currency": "INR",
    }
    failed = client.post(
        "/v1/payments", json=too_much, headers=_headers(owner_token(source), key)
    )
    assert failed.status_code == 422
    assert failed.json()["error"]["code"] == "insufficient_funds"

    affordable = {**too_much, "amount_minor": 5_000}
    retry = client.post(
        "/v1/payments", json=affordable, headers=_headers(owner_token(source), key)
    )
    assert retry.status_code == 201


def test_missing_idempotency_key_is_rejected(client, owner_token, make_account):
    source, destination = make_account(500_000), make_account(0)
    resp = client.post(
        "/v1/payments",
        json={
            "from_account_id": source,
            "to_account_id": destination,
            "amount_minor": 1_000,
            "currency": "INR",
        },
        headers=_headers(owner_token(source)),
    )
    assert resp.status_code == 400
    assert "Idempotency-Key" in resp.json()["error"]["message"]


def test_non_uuid_idempotency_key_rejected(client, owner_token, make_account):
    source, destination = make_account(500_000), make_account(0)
    resp = client.post(
        "/v1/payments",
        json={
            "from_account_id": source,
            "to_account_id": destination,
            "amount_minor": 1_000,
            "currency": "INR",
        },
        headers=_headers(owner_token(source), "not-a-uuid"),
    )
    assert resp.status_code == 400


def test_float_amount_is_rejected_not_coerced(client, owner_token, make_account):
    """200000.0 must not become 200000. StrictInt is doing this."""
    source, destination = make_account(500_000), make_account(0)
    resp = client.post(
        "/v1/payments",
        json={
            "from_account_id": source,
            "to_account_id": destination,
            "amount_minor": 200000.5,
            "currency": "INR",
        },
        headers=_headers(owner_token(source), str(uuid.uuid4())),
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_request"


def test_outbox_row_written_with_the_payment(client, owner_token, make_account):
    """I3 precondition: the event commits with the money, in one transaction."""
    source, destination = make_account(500_000), make_account(0)
    resp = client.post(
        "/v1/payments",
        json={
            "from_account_id": source,
            "to_account_id": destination,
            "amount_minor": 100_000,
            "currency": "INR",
        },
        headers=_headers(owner_token(source), str(uuid.uuid4())),
    )
    assert resp.status_code == 201
    event_id = resp.json()["event_id"]
    assert _outbox_rows(event_id) == 1

    with read_session() as session:
        row = session.execute(
            text(
                "SELECT partition_key, published_at, event_type "
                "FROM outbox WHERE event_id = :e"
            ),
            {"e": event_id},
        ).one()
    # Partitioned by the debited account so per-account order is preserved
    assert row.partition_key == source
    assert row.published_at is None  # the relay has not run yet
    assert row.event_type == "payment.posted"


def test_same_account_transfer_rejected(client, owner_token, make_account):
    account = make_account(500_000)
    resp = client.post(
        "/v1/payments",
        json={
            "from_account_id": account,
            "to_account_id": account,
            "amount_minor": 1_000,
            "currency": "INR",
        },
        headers=_headers(owner_token(account), str(uuid.uuid4())),
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "same_account_transfer"


def test_unknown_account_is_404(client, owner_token, make_account):
    source = make_account(500_000)
    resp = client.post(
        "/v1/payments",
        json={
            "from_account_id": source,
            "to_account_id": "acc_nope",
            "amount_minor": 1_000,
            "currency": "INR",
        },
        headers=_headers(owner_token(source), str(uuid.uuid4())),
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "account_not_found"


def test_currency_mismatch_rejected(client, owner_token, make_account):
    source = make_account(500_000, currency="INR")
    destination = make_account(0, currency="INR")
    resp = client.post(
        "/v1/payments",
        json={
            "from_account_id": source,
            "to_account_id": destination,
            "amount_minor": 1_000,
            "currency": "USD",
        },
        headers=_headers(owner_token(source), str(uuid.uuid4())),
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "currency_mismatch"


def test_ledger_still_balances_after_all_of_this(ledger_total):
    assert ledger_total() == Decimal("0.0000")
