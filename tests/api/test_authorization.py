"""Blueprint decision D4: who may read and debit which account.

viewer and operator read everything. A client reads only its own account,
and only the payments it pays or receives. A client debits only its own
account, and only an operator debits a system account.
"""

from __future__ import annotations

import uuid

import pytest

pytestmark = pytest.mark.integration


def _bearer(token: str, key: str | None = None) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    if key:
        headers["Idempotency-Key"] = key
    return headers


def _body(source: str, destination: str, amount_minor: int = 1_000) -> dict:
    return {
        "from_account_id": source,
        "to_account_id": destination,
        "amount_minor": amount_minor,
        "currency": "INR",
    }


def _pay(client, token, source, destination, amount_minor=1_000):
    return client.post(
        "/v1/payments",
        json=_body(source, destination, amount_minor),
        headers=_bearer(token, str(uuid.uuid4())),
    )


def _code(resp) -> str:
    return resp.json()["error"]["code"]


# --- reading accounts (B1) --------------------------------------------------


def test_seeded_client_reads_its_own_account_and_no_other(client, client_token):
    """The B1 reproduction, with a token from the real login endpoint."""
    for path in ("/v1/accounts/acc_alice", "/v1/accounts/acc_alice/postings"):
        assert client.get(path, headers=_bearer(client_token)).status_code == 200, path

    for path in ("/v1/accounts/acc_bob", "/v1/accounts/acc_bob/postings"):
        resp = client.get(path, headers=_bearer(client_token))
        assert resp.status_code == 403, path
        assert _code(resp) == "forbidden"


@pytest.mark.parametrize("token_fixture", ["viewer_token", "operator_token"])
def test_viewer_and_operator_read_any_account(client, request, token_fixture):
    token = request.getfixturevalue(token_fixture)
    for path in ("/v1/accounts/acc_bob", "/v1/accounts/acc_bob/postings"):
        assert client.get(path, headers=_bearer(token)).status_code == 200, path


# --- reading payments (B1, G13) ---------------------------------------------


def test_client_reads_only_payments_it_pays_or_receives(
    client, owner_token, viewer_token, operator_token, make_account
):
    me, other, third = make_account(500_000), make_account(500_000), make_account(0)

    paid = _pay(client, owner_token(me), me, third)
    received = _pay(client, owner_token(other), other, me)
    unrelated = _pay(client, owner_token(other), other, third)
    assert [paid.status_code, received.status_code, unrelated.status_code] == [201] * 3

    def read(token, payment):
        return client.get(
            f"/v1/payments/{payment.json()['payment_id']}", headers=_bearer(token)
        )

    assert read(owner_token(me), paid).status_code == 200
    assert read(owner_token(me), received).status_code == 200
    denied = read(owner_token(me), unrelated)
    assert denied.status_code == 403
    assert _code(denied) == "forbidden"

    # G13: the blueprint names client for this endpoint; D4 adds that viewer
    # and operator read every payment.
    assert read(viewer_token, unrelated).status_code == 200
    assert read(operator_token, unrelated).status_code == 200


# --- debiting (B2) ------------------------------------------------------------


def test_client_cannot_debit_an_account_it_does_not_own(
    client, owner_token, make_account
):
    me, victim = make_account(0), make_account(500_000)

    resp = _pay(client, owner_token(me), victim, me)

    assert resp.status_code == 403
    assert _code(resp) == "forbidden"
    balance = client.get(
        f"/v1/accounts/{victim}", headers=_bearer(owner_token(victim))
    ).json()["balance"]
    assert balance == "5000.0000"


def test_client_cannot_mint_money_from_the_settlement_account(client, client_token):
    """The Stage 1 reproduction: alice paying herself 10^12 from settlement."""
    resp = _pay(client, client_token, "sys_settlement", "acc_alice", 10**12)

    assert resp.status_code == 403
    assert _code(resp) == "forbidden"


def test_owning_a_system_account_is_not_enough(client, owner_token):
    """System accounts are operator-only even for a token that names one."""
    token = owner_token("sys_settlement")

    resp = _pay(client, token, "sys_settlement", "acc_alice")

    assert resp.status_code == 403
    assert _code(resp) == "forbidden"


def test_operator_can_debit_a_system_account(client, operator_token, make_account):
    """A deposit: settlement is the counterparty for money entering the ledger."""
    destination = make_account(0)

    resp = _pay(client, operator_token, "sys_settlement", destination, 50_000)

    assert resp.status_code == 201, resp.text


def test_viewer_cannot_create_payments(client, viewer_token, make_account):
    resp = _pay(client, viewer_token, make_account(500_000), make_account(0))

    assert resp.status_code == 403
    assert _code(resp) == "forbidden"


def test_non_owner_cannot_replay_someone_elses_response(
    client, owner_token, make_account
):
    """Ownership is checked before the idempotency lookup, so a stored
    response is never served to a caller who could not have made it."""
    owner, intruder, destination = (
        make_account(500_000),
        make_account(0),
        make_account(0),
    )
    key = str(uuid.uuid4())
    body = _body(owner, destination)

    first = client.post("/v1/payments", json=body, headers=_bearer(owner_token(owner), key))
    replay = client.post(
        "/v1/payments", json=body, headers=_bearer(owner_token(intruder), key)
    )

    assert first.status_code == 201
    assert replay.status_code == 403


def test_denied_request_does_not_reserve_the_key(client, owner_token, make_account):
    """A 403 happens before the key is touched, so the owner can still use it."""
    me, victim = make_account(0), make_account(500_000)
    key = str(uuid.uuid4())
    body = _body(victim, me)

    denied = client.post("/v1/payments", json=body, headers=_bearer(owner_token(me), key))
    allowed = client.post(
        "/v1/payments", json=body, headers=_bearer(owner_token(victim), key)
    )

    assert denied.status_code == 403
    assert allowed.status_code == 201
