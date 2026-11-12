"""Phase 2 gate: auth, role enforcement, and derived balances over HTTP."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


def test_token_issued_for_valid_credentials(client, seeded):
    resp = client.post(
        "/v1/auth/token",
        json={"username": "operator", "password": "operator-dev-password"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["role"] == "operator"
    assert body["token_type"] == "bearer"
    assert body["expires_in"] > 0


def test_bad_password_rejected(client, seeded):
    resp = client.post(
        "/v1/auth/token",
        json={"username": "operator", "password": "wrong"},
    )
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "unauthenticated"


def test_unknown_user_rejected(client, seeded):
    resp = client.post(
        "/v1/auth/token", json={"username": "nobody", "password": "x"}
    )
    assert resp.status_code == 401


def test_unknown_user_takes_as_long_as_a_wrong_password(client, seeded):
    """The response time must not reveal which usernames exist."""
    import time

    def median_seconds(username: str) -> float:
        times = []
        for _ in range(5):
            started = time.perf_counter()
            resp = client.post(
                "/v1/auth/token", json={"username": username, "password": "wrong"}
            )
            times.append(time.perf_counter() - started)
            assert resp.status_code == 401
        return sorted(times)[2]

    known, unknown = median_seconds("operator"), median_seconds("nobody")
    assert 0.5 < unknown / known < 2.0, (known, unknown)


def test_missing_token_is_401(client, seeded):
    assert client.get("/v1/accounts/acc_alice").status_code == 401


def test_viewer_cannot_create_account(client, viewer_token):
    resp = client.post(
        "/v1/accounts",
        json={"holder_name": "Mallory", "currency": "INR"},
        headers={"Authorization": f"Bearer {viewer_token}"},
    )
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "forbidden"


def test_operator_creates_account_with_zero_balance(client, auth):
    resp = client.post(
        "/v1/accounts",
        json={"holder_name": "Dev Patel", "currency": "inr"},
        headers=auth,
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["currency"] == "INR"
    assert body["balance"] == "0.0000"
    assert body["balance_minor"] == 0
    assert body["status"] == "active"


def test_balance_matches_postings(client, auth, make_account):
    account_id = make_account(opening_minor=250_000)
    resp = client.get(f"/v1/accounts/{account_id}", headers=auth)
    assert resp.status_code == 200
    body = resp.json()
    assert body["balance"] == "2500.0000"
    assert body["balance_minor"] == 250_000


def test_unknown_account_returns_404_with_error_shape(client, auth):
    resp = client.get("/v1/accounts/acc_does_not_exist", headers=auth)
    assert resp.status_code == 404
    error = resp.json()["error"]
    assert error["code"] == "account_not_found"
    assert "request_id" in error and "trace_id" in error


def test_postings_history(client, auth, make_account):
    account_id = make_account(opening_minor=50_000)
    resp = client.get(f"/v1/accounts/{account_id}/postings", headers=auth)
    assert resp.status_code == 200
    rows = resp.json()
    assert len(rows) == 1
    assert rows[0]["amount"] == "500.0000"


def test_invalid_currency_rejected(client, auth):
    resp = client.post(
        "/v1/accounts",
        json={"holder_name": "X", "currency": "RUPEE"},
        headers=auth,
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_request"


def test_extra_field_rejected(client, auth):
    resp = client.post(
        "/v1/accounts",
        json={"holder_name": "X", "currency": "INR", "balance": 1000},
        headers=auth,
    )
    assert resp.status_code == 400
