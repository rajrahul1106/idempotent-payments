"""The demo page's read-only endpoints and the page itself.

GET /v1/accounts (D4 and D26), GET /v1/payments, GET /v1/payments/{id}/journey
and GET /demo. None of them writes; the journey is built only from rows.
"""

from __future__ import annotations

import datetime as dt
import re
import uuid

import pytest
from sqlalchemy import text

from app.core.config import settings
from app.db.session import unit_of_work
from tests.conftest import ROOT

pytestmark = pytest.mark.integration


def _bearer(token: str, key: str | None = None) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    if key:
        headers["Idempotency-Key"] = key
    return headers


def _pay(client, token, source, destination, amount_minor=1_000):
    return client.post(
        "/v1/payments",
        json={
            "from_account_id": source,
            "to_account_id": destination,
            "amount_minor": amount_minor,
            "currency": "INR",
        },
        headers=_bearer(token, str(uuid.uuid4())),
    )


def _accounts(client, token) -> dict[str, dict]:
    resp = client.get("/v1/accounts?limit=500", headers=_bearer(token))
    assert resp.status_code == 200, resp.text
    return {row["id"]: row for row in resp.json()}


# --- GET /v1/accounts: who sees what ------------------------------------------------


def test_accounts_needs_a_token(client, seeded):
    assert client.get("/v1/accounts").status_code == 401


def test_client_sees_customer_names_but_not_their_balances(client, client_token):
    rows = _accounts(client, client_token)
    own, other = rows["acc_alice"], rows["acc_bob"]

    assert own["balance"] is not None and own["balance_minor"] is not None
    assert own["currency"] == "INR" and own["account_type"] == "customer"

    # Directory entry: id and holder name only. Hidden fields are null, never zero.
    assert other["holder_name"] == "Bob Mehta"
    for field in ("balance", "balance_minor", "currency", "account_type"):
        assert other[field] is None, field


def test_client_sees_no_system_accounts(client, client_token):
    rows = _accounts(client, client_token)
    assert "sys_settlement" not in rows and "sys_fees" not in rows


@pytest.mark.parametrize("token_fixture", ["viewer_token", "operator_token"])
def test_viewer_and_operator_see_every_account_with_balances(client, request, token_fixture):
    rows = _accounts(client, request.getfixturevalue(token_fixture))
    for account_id in ("acc_alice", "acc_bob", "sys_settlement", "sys_fees"):
        assert rows[account_id]["balance"] is not None, account_id
        assert rows[account_id]["account_type"] is not None, account_id
    assert rows["sys_settlement"]["account_type"] == "system"


def test_can_debit_per_role(client, client_token, viewer_token, operator_token):
    alice = _accounts(client, client_token)
    assert alice["acc_alice"]["can_debit"] is True
    assert [a for a, row in alice.items() if row["can_debit"]] == ["acc_alice"]

    viewer = _accounts(client, viewer_token)
    assert not any(row["can_debit"] for row in viewer.values())

    operator = _accounts(client, operator_token)
    assert all(row["can_debit"] for row in operator.values())
    assert operator["sys_settlement"]["can_debit"] is True


def test_client_own_account_is_listed_even_beyond_the_limit(client, owner_token, make_account):
    newest = make_account(5_000)  # created last, so outside a limit of 1
    rows = client.get("/v1/accounts?limit=1", headers=_bearer(owner_token(newest))).json()
    own = [row for row in rows if row["id"] == newest]
    assert own and own[0]["balance"] == "50.0000" and own[0]["can_debit"] is True


# --- the API still enforces the rule, whatever the page shows ----------------------


def test_client_calling_the_api_directly_cannot_debit_another_customer(client, client_token):
    resp = _pay(client, client_token, "acc_bob", "acc_alice")
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "forbidden"


def test_client_cannot_debit_a_system_account_even_one_its_token_names(client, owner_token):
    resp = _pay(client, owner_token("sys_fees"), "sys_fees", "acc_alice")
    assert resp.status_code == 403
    assert "Only an operator" in resp.json()["error"]["message"]


def test_viewer_cannot_send_a_payment(client, viewer_token):
    assert _pay(client, viewer_token, "acc_alice", "acc_bob").status_code == 403


# --- GET /v1/payments ---------------------------------------------------------------


def test_client_lists_only_payments_on_its_own_account(client, owner_token, make_account):
    mine, other, third = make_account(50_000), make_account(50_000), make_account(0)
    paid = _pay(client, owner_token(mine), mine, third).json()["payment_id"]
    unrelated = _pay(client, owner_token(other), other, third).json()["payment_id"]

    rows = client.get("/v1/payments?limit=100", headers=_bearer(owner_token(mine))).json()
    ids = {row["payment_id"] for row in rows}
    assert paid in ids and unrelated not in ids
    for row in rows:
        assert mine in (row["from_account_id"], row["to_account_id"])

    first = next(row for row in rows if row["payment_id"] == paid)
    assert first == {
        "payment_id": paid,
        "status": "posted",
        "amount": "10.0000",
        "amount_minor": 1_000,
        "currency": "INR",
        "from_account_id": mine,
        "to_account_id": third,
        "created_at": first["created_at"],
    }


def test_client_asking_for_another_account_is_403(client, client_token):
    resp = client.get("/v1/payments?account_id=acc_bob", headers=_bearer(client_token))
    assert resp.status_code == 403


@pytest.mark.parametrize("token_fixture", ["viewer_token", "operator_token"])
def test_viewer_and_operator_list_any_account(client, request, token_fixture, owner_token, make_account):
    source, destination = make_account(50_000), make_account(0)
    paid = _pay(client, owner_token(source), source, destination).json()["payment_id"]
    token = request.getfixturevalue(token_fixture)
    rows = client.get(f"/v1/payments?account_id={destination}", headers=_bearer(token)).json()
    assert [row["payment_id"] for row in rows] == [paid]
    assert client.get("/v1/payments", headers=_bearer(token)).status_code == 200


def test_payments_are_newest_first_and_limited(client, operator_token, owner_token, make_account):
    source, destination = make_account(50_000), make_account(0)
    ids = [_pay(client, owner_token(source), source, destination).json()["payment_id"] for _ in range(3)]
    rows = client.get(f"/v1/payments?account_id={source}&limit=2", headers=_bearer(operator_token)).json()
    assert [row["payment_id"] for row in rows] == [ids[2], ids[1]]
    assert client.get("/v1/payments?limit=101", headers=_bearer(operator_token)).status_code == 400


# --- GET /v1/payments/{id}/journey --------------------------------------------------


def _journey(client, token, payment_id):
    resp = client.get(f"/v1/payments/{payment_id}/journey", headers=_bearer(token))
    assert resp.status_code == 200, resp.text
    return resp.json()


def _stages(journey) -> dict[str, bool]:
    return {stage["key"]: stage["done"] for stage in journey["stages"]}


def test_unknown_payment_journey_is_404(client, operator_token):
    resp = client.get("/v1/payments/pay_does_not_exist/journey", headers=_bearer(operator_token))
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "payment_not_found"


def test_journey_needs_the_same_access_as_the_payment(client, owner_token, make_account, client_token):
    source, destination = make_account(50_000), make_account(0)
    paid = _pay(client, owner_token(source), source, destination).json()["payment_id"]
    assert client.get(f"/v1/payments/{paid}/journey").status_code == 401
    # Alice is neither payer nor payee.
    assert client.get(f"/v1/payments/{paid}/journey", headers=_bearer(client_token)).status_code == 403
    assert client.get(f"/v1/payments/{paid}/journey", headers=_bearer(owner_token(destination))).status_code == 200


def test_journey_stages_follow_the_recorded_rows(client, owner_token, operator_token, make_account):
    source, destination = make_account(50_000), make_account(0)
    created = _pay(client, owner_token(source), source, destination, amount_minor=12_345)
    assert created.status_code == 201
    payment_id, event_id = created.json()["payment_id"], created.json()["event_id"]
    token = owner_token(source)

    # Before publish. Nothing here relays the test database, so it stays put.
    journey = _journey(client, token, payment_id)
    assert _stages(journey) == {
        "received": True, "recorded": True, "queued": True, "sent": False, "processed": False,
    }
    assert [s["key"] for s in journey["stages"]] == ["received", "recorded", "queued", "sent", "processed"]
    assert journey["overall"] == "in_progress"
    assert journey["summary"].startswith("123.45 INR moved in the ledger. Next step not recorded yet")
    sent = next(s for s in journey["stages"] if s["key"] == "sent")
    assert sent["at"] is None and sent["note"].startswith("Not processed yet (")
    assert journey["event_id"] == event_id
    assert journey["payment"]["amount"] == "123.4500" and journey["payment"]["amount_minor"] == 12_345
    assert journey["parties"]["sender"]["id"] == source
    assert journey["parties"]["receiver"]["id"] == destination
    assert journey["dead_letters"] == []

    # The ledger: both legs, and they sum to exactly zero.
    entries = {e["account_id"]: e for e in journey["ledger_entries"]}
    assert entries[source]["amount"] == "-123.4500" and entries[source]["direction"] == "money out"
    assert entries[destination]["amount"] == "123.4500" and entries[destination]["direction"] == "money in"
    assert journey["ledger_sum"] == "0.0000"

    # After publish.
    with unit_of_work() as session:
        session.execute(text("UPDATE outbox SET published_at = now() WHERE event_id = :e"), {"e": event_id})
    journey = _journey(client, token, payment_id)
    assert _stages(journey)["sent"] is True and _stages(journey)["processed"] is False
    assert journey["overall"] == "in_progress"

    # A row for another consumer group is not this consumer's processing.
    with unit_of_work() as session:
        session.execute(
            text("INSERT INTO processed_events (event_id, consumer_group) VALUES (:e, 'some-other-group')"),
            {"e": event_id},
        )
    assert _stages(_journey(client, token, payment_id))["processed"] is False

    # After processing.
    with unit_of_work() as session:
        session.execute(
            text("INSERT INTO processed_events (event_id, consumer_group) VALUES (:e, :g)"),
            {"e": event_id, "g": settings.kafka_consumer_group},
        )
    journey = _journey(client, token, payment_id)
    assert all(_stages(journey).values())
    assert journey["overall"] == "completed"
    assert journey["summary"] == "123.45 INR moved and every step is complete."
    assert all(s["note"] is None and s["at"] for s in journey["stages"])

    # With a dead letter that has not been replayed.
    with unit_of_work() as session:
        dlq_id = session.execute(
            text(
                "INSERT INTO dlq_messages (event_id, original_topic, payload, reason, attempts, first_failed_at)"
                " VALUES (:e, 'payments.main', '{}'::jsonb, 'permanent: test', 1, now()) RETURNING id"
            ),
            {"e": event_id},
        ).scalar_one()
    try:
        journey = _journey(client, operator_token, payment_id)
        assert journey["overall"] == "failed"
        assert [d["id"] for d in journey["dead_letters"]] == [dlq_id]
        assert journey["dead_letters"][0]["replayed_at"] is None
        assert journey["dead_letters"][0]["reason"] == "permanent: test"
    finally:
        with unit_of_work() as session:
            session.execute(text("DELETE FROM dlq_messages WHERE id = :i"), {"i": dlq_id})


def test_a_payment_made_outside_the_api_has_no_request_stage(client, operator_token, make_account):
    account_id = make_account(5_000)  # funded by the fixture directly, no idempotency key
    with unit_of_work() as session:
        tx_id = session.execute(
            text("SELECT transaction_id FROM postings WHERE account_id = :a"), {"a": account_id}
        ).scalar_one()
    journey = _journey(client, operator_token, tx_id)
    received = next(s for s in journey["stages"] if s["key"] == "received")
    assert received["done"] is False and received["at"] is None
    assert journey["ledger_sum"] == "0.0000"


# --- GET /demo ----------------------------------------------------------------------


def test_demo_page_is_served_without_auth_and_holds_no_password(client):
    resp = client.get("/demo")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/html")
    page = resp.text
    assert "Send money" in page and "/v1/auth/token" in page

    seed = (ROOT / "scripts" / "seed.py").read_text()
    passwords = re.findall(r'\(\s*"\w+",\s*"([^"]+)",\s*"(?:operator|client|viewer)"', seed)
    assert len(passwords) == 3, passwords
    for password in passwords:
        assert password not in page


def test_demo_page_text_has_no_em_dash(client):
    assert "—" not in client.get("/demo").text
