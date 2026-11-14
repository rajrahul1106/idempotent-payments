"""Decision D27 over HTTP: the switch status, the API refusal, the fault
lookup, and the journey's recorded attempts."""

from __future__ import annotations

import json
import uuid

import pytest
from sqlalchemy import text

from app.core.config import settings
from app.db.session import read_session, unit_of_work
from app.messaging import demo_faults

pytestmark = pytest.mark.integration


def _bearer(token: str, key: str | None = None) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    if key:
        headers["Idempotency-Key"] = key
    return headers


def _pay(client, token, source, destination, metadata=None, key=None):
    body = {"from_account_id": source, "to_account_id": destination, "amount_minor": 1_000, "currency": "INR"}
    if metadata is not None:
        body["metadata"] = metadata
    return client.post("/v1/payments", json=body, headers=_bearer(token, key or str(uuid.uuid4())))


@pytest.fixture
def switch(monkeypatch):
    def _set(on: bool) -> None:
        monkeypatch.setattr(settings, "demo_faults_enabled", on)

    return _set


# --- GET /v1/demo/config ------------------------------------------------------------


def test_demo_config_needs_a_token(client, seeded):
    assert client.get("/v1/demo/config").status_code == 401


@pytest.mark.parametrize("token_fixture", ["client_token", "viewer_token", "operator_token"])
@pytest.mark.parametrize("on", [True, False])
def test_demo_config_is_exactly_the_apis_own_switch(client, request, token_fixture, switch, on):
    switch(on)
    resp = client.get("/v1/demo/config", headers=_bearer(request.getfixturevalue(token_fixture)))
    assert resp.status_code == 200
    assert resp.json() == {"demo_faults_enabled": on}


# --- the API with the switch off and on ---------------------------------------------


def test_switch_off_refuses_demo_fault_before_any_write(client, owner_token, make_account, switch):
    switch(False)
    source, destination = make_account(50_000), make_account(0)
    key = str(uuid.uuid4())
    resp = _pay(client, owner_token(source), source, destination, {"demo_fault": "fail_once"}, key)
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "demo_faults_disabled"

    with read_session() as session:
        assert session.execute(text("SELECT count(*) FROM idempotency_keys WHERE key = :k"), {"k": key}).scalar_one() == 0
        touched = session.execute(
            text("SELECT count(*) FROM postings WHERE account_id = :d"), {"d": destination}
        ).scalar_one()
    assert touched == 0

    # The same key is free for a payment without the fault: nothing was reserved.
    assert _pay(client, owner_token(source), source, destination, {"note": "ok"}, key).status_code == 201


def test_switch_on_accepts_demo_fault_and_stores_it_on_the_payment(client, owner_token, make_account, switch):
    switch(True)
    source, destination = make_account(50_000), make_account(0)
    resp = _pay(client, owner_token(source), source, destination, {"demo_fault": "fail_twice"})
    assert resp.status_code == 201
    assert demo_faults.fault_for(resp.json()["payment_id"]) == "fail_twice"


# --- the fault lookup ---------------------------------------------------------------


def _transaction(metadata) -> str:
    tx_id = f"tx_demo_{uuid.uuid4().hex[:12]}"
    with unit_of_work() as session:
        session.execute(
            text("INSERT INTO transactions (id, amount, currency, metadata) VALUES (:i, 1, 'INR', CAST(:m AS jsonb))"),
            {"i": tx_id, "m": json.dumps(metadata)},
        )
    return tx_id


@pytest.mark.parametrize(
    "metadata, expected",
    [
        ({"demo_fault": "fail_once"}, "fail_once"),
        ({"demo_fault": "fail_twice"}, "fail_twice"),
        ({"demo_fault": "fail_permanent"}, "fail_permanent"),
        ({"demo_fault": "explode"}, None),
        ({"demo_fault": 1}, None),
        ({}, None),
    ],
)
def test_fault_is_read_from_the_payment_row(database, metadata, expected):
    assert demo_faults.fault_for(_transaction(metadata)) == expected


def test_a_missing_payment_row_is_no_fault(database):
    assert demo_faults.fault_for("tx_does_not_exist") is None


# --- the journey's recorded attempts ------------------------------------------------


def _record(event_id, attempt, kind, sent_to):
    with unit_of_work() as session:
        session.execute(
            text(
                "INSERT INTO delivery_failures (event_id, attempt, kind, error, sent_to)"
                " VALUES (:e, :a, :k, :err, :s)"
            ),
            {"e": event_id, "a": attempt, "k": kind, "err": f"{kind}: DemoFault: test", "s": sent_to},
        )


def test_journey_shows_recorded_attempts_and_where_the_event_waits(client, owner_token, make_account):
    source, destination = make_account(50_000), make_account(0)
    created = _pay(client, owner_token(source), source, destination).json()
    payment_id, event_id = created["payment_id"], created["event_id"]
    token = owner_token(source)

    def journey():
        resp = client.get(f"/v1/payments/{payment_id}/journey", headers=_bearer(token))
        assert resp.status_code == 200
        return resp.json()

    def processed_note(j):
        return next(s for s in j["stages"] if s["key"] == "processed")["note"]

    first = journey()
    assert first["attempts"] == [] and first["waiting"] is None
    assert processed_note(first).startswith("Not processed yet (")

    _record(event_id, 1, "transient", settings.retry_topics[0])
    j = journey()
    assert [(a["attempt"], a["kind"], a["sent_to"], a["retry_in_seconds"], a["parked"]) for a in j["attempts"]] == [
        (1, "transient", settings.retry_topics[0], 5, False)
    ]
    assert j["waiting"]["queue"] == "5-second" and j["waiting"]["seconds"] == 5
    assert processed_note(j) == "Waiting in the 5-second retry queue"
    assert j["overall"] == "in_progress"
    assert j["summary"] == "10.00 INR moved in the ledger. The follow-up step failed 1 time and is waiting in the 5-second retry queue."

    _record(event_id, 2, "transient", settings.retry_topics[1])
    j = journey()
    assert processed_note(j) == "Waiting in the 1-minute retry queue"
    assert [a["attempt"] for a in j["attempts"]] == [1, 2]

    _record(event_id, 3, "permanent", settings.kafka_dlq_topic)
    j = journey()
    assert j["waiting"] is None and j["attempts"][-1]["parked"] is True
    assert processed_note(j) == "Parked for staff review"

    with unit_of_work() as session:
        session.execute(
            text("INSERT INTO processed_events (event_id, consumer_group) VALUES (:e, :g)"),
            {"e": event_id, "g": settings.kafka_consumer_group},
        )
    j = journey()
    assert j["overall"] == "completed" and j["waiting"] is None
    assert len(j["attempts"]) == 3  # the record of what happened stays


def test_a_replayed_dead_letter_is_no_longer_parked(client, owner_token, make_account):
    source, destination = make_account(50_000), make_account(0)
    created = _pay(client, owner_token(source), source, destination).json()
    payment_id, event_id = created["payment_id"], created["event_id"]
    _record(event_id, 1, "permanent", settings.kafka_dlq_topic)
    with unit_of_work() as session:
        dlq_id = session.execute(
            text(
                "INSERT INTO dlq_messages (event_id, original_topic, payload, reason, attempts, first_failed_at)"
                " VALUES (:e, 'payments.main', '{}'::jsonb, 'permanent: DemoPermanentFault: test', 1, now()) RETURNING id"
            ),
            {"e": event_id},
        ).scalar_one()

    def state():
        j = client.get(f"/v1/payments/{payment_id}/journey", headers=_bearer(owner_token(source))).json()
        return j["overall"], next(s for s in j["stages"] if s["key"] == "processed")["note"]

    try:
        assert state() == ("failed", "Parked for staff review")
        with unit_of_work() as session:
            session.execute(text("UPDATE dlq_messages SET replayed_at = now() WHERE id = :i"), {"i": dlq_id})
        overall, note = state()
        assert overall == "in_progress" and note.startswith("Not processed yet (")
    finally:
        with unit_of_work() as session:
            session.execute(text("DELETE FROM dlq_messages WHERE id = :i"), {"i": dlq_id})
