"""GET /v1/dlq and POST /v1/dlq/{message_id}/replay against real Postgres.

The broker is the recording double here; the replay's effect through a real
broker and consumer is tests/messaging/test_retry_dlq.py.
"""

from __future__ import annotations

import datetime as dt
import json
import uuid

import pytest

from app.services.dlq import DeadLetter, dedup_id_for, record_dead_letter
from tests.messaging.conftest import RecordingProducer

pytestmark = pytest.mark.integration

GROUP = "test-api-dlq-writer"


@pytest.fixture
def recorded():
    """Recorded dead letters, each with a reason unique to this test."""
    tag = f"TestTag{uuid.uuid4().hex[:10]}"

    def _record(kind: str = "transient", *, payload: dict | None = None) -> dict:
        event_id = str(uuid.uuid4())
        body = payload or {
            "event_id": event_id,
            "event_type": "payment.posted",
            "schema_version": 1,
            "from_account_id": "acc_dlq_api",
            "amount_minor": 1000,
        }
        assert record_dead_letter(
            DeadLetter(
                dedup_id=dedup_id_for(event_id, None, "unused"),
                event_id=event_id,
                original_topic="payments.main",
                payload_json=json.dumps(body),
                reason=f"{kind}: {tag}: injected",
                attempts=4 if kind == "transient" else 1,
                first_failed_at=dt.datetime(2026, 11, 11, 9, 0, tzinfo=dt.timezone.utc),
            ),
            GROUP,
        )
        return {"event_id": event_id, "kind": kind}

    _record.tag = tag
    return _record


@pytest.fixture
def producer(client):
    from app.api.routes.dlq import replay_target
    from app.main import app

    recording = RecordingProducer()
    app.dependency_overrides[replay_target] = lambda: (recording, "test.replay.main")
    yield recording
    app.dependency_overrides.pop(replay_target, None)


def _list(client, auth, **params):
    return client.get("/v1/dlq", headers=auth, params=params)


def _ids_for(client, auth, prefix: str, **params) -> list[str]:
    resp = _list(client, auth, reason=prefix, **params)
    assert resp.status_code == 200, resp.text
    return [item["event_id"] for item in resp.json()["items"]]


def test_dlq_is_operator_only(client, client_token, viewer_token):
    for token in (client_token, viewer_token):
        headers = {"Authorization": f"Bearer {token}"}
        assert client.get("/v1/dlq", headers=headers).status_code == 403
        assert client.post("/v1/dlq/1/replay", headers=headers).status_code == 403
    assert client.get("/v1/dlq").status_code == 401


def test_list_filters_by_reason_prefix_and_replayed(client, auth, recorded, producer):
    transient = [recorded("transient")["event_id"] for _ in range(2)]
    permanent = [recorded("permanent")["event_id"] for _ in range(2)]
    tag = recorded.tag

    assert _ids_for(client, auth, f"transient: {tag}") == transient
    assert _ids_for(client, auth, f"permanent: {tag}") == permanent

    item = _list(client, auth, reason=f"permanent: {tag}").json()["items"][0]
    assert item["attempts"] == 1 and item["replayed_at"] is None
    assert item["original_topic"] == "payments.main"
    assert item["payload"]["amount_minor"] == 1000

    replayed = client.post(f"/v1/dlq/{item['id']}/replay", headers=auth)
    assert replayed.status_code == 200, replayed.text
    assert _ids_for(client, auth, f"permanent: {tag}", replayed="true") == [permanent[0]]
    assert _ids_for(client, auth, f"permanent: {tag}", replayed="false") == [permanent[1]]


def test_reason_filter_treats_wildcards_literally(client, auth, recorded):
    recorded("transient")
    assert _ids_for(client, auth, "%") == []
    assert _ids_for(client, auth, "transient: TestTag_") == []


def test_list_paginates_by_id(client, auth, recorded):
    ids = [recorded("transient")["event_id"] for _ in range(5)]
    prefix = f"transient: {recorded.tag}"
    seen, after = [], None
    while True:
        params = {"reason": prefix, "limit": 2}
        if after is not None:
            params["after_id"] = after
        page = _list(client, auth, **params).json()
        seen += [item["event_id"] for item in page["items"]]
        after = page["next_after_id"]
        if after is None:
            break
    assert seen == ids


def test_payload_numbers_are_never_floats(client, auth, recorded):
    recorded("permanent", payload={"amount_minor": 1000.5, "event_id": "x"})
    [item] = _list(client, auth, reason=f"permanent: {recorded.tag}").json()["items"]
    assert item["payload"]["amount_minor"] == "1000.5"


def test_replay_publishes_attempt_zero_with_the_original_event_id(client, auth, recorded, producer):
    event_id = recorded("transient")["event_id"]
    [item] = _list(client, auth, reason=f"transient: {recorded.tag}").json()["items"]

    resp = client.post(f"/v1/dlq/{item['id']}/replay", headers=auth)
    assert resp.status_code == 200, resp.text
    assert resp.json()["replayed_at"] is not None

    [message] = producer.messages
    assert message["topic"] == "test.replay.main"
    assert message["key"] == "acc_dlq_api"
    assert message["headers"]["event_id"] == event_id
    assert message["headers"]["attempt_count"] == "0"
    assert message["headers"]["replay_of"] == str(item["id"])
    assert json.loads(message["value"])["event_id"] == event_id

    again = client.post(f"/v1/dlq/{item['id']}/replay", headers=auth)
    assert again.status_code == 409
    assert again.json()["error"]["code"] == "dlq_already_replayed"
    assert len(producer.messages) == 1  # a refused replay publishes nothing


def test_replay_of_unknown_id_is_404(client, auth, producer):
    resp = client.post("/v1/dlq/987654321/replay", headers=auth)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "dlq_message_not_found"
    assert producer.messages == []


def test_unacknowledged_replay_is_503_and_leaves_the_row_replayable(
    client, auth, recorded, producer
):
    recorded("transient")
    [item] = _list(client, auth, reason=f"transient: {recorded.tag}").json()["items"]
    producer.fail_on_nth = 1
    producer.fail_once = True

    failed = client.post(f"/v1/dlq/{item['id']}/replay", headers=auth)
    assert failed.status_code == 503, failed.text
    assert failed.json()["error"]["code"] == "service_unavailable"
    [still] = _list(client, auth, reason=f"transient: {recorded.tag}").json()["items"]
    assert still["replayed_at"] is None

    assert client.post(f"/v1/dlq/{item['id']}/replay", headers=auth).status_code == 200


def test_redelivered_dead_letter_writes_one_row(recorded):
    from tests.messaging.conftest import dlq_rows

    event_id = str(uuid.uuid4())
    letter = DeadLetter(
        dedup_id=dedup_id_for(event_id, None, "unused"),
        event_id=event_id,
        original_topic="payments.main",
        payload_json="{}",
        reason="transient: X: y",
        attempts=4,
        first_failed_at=dt.datetime.now(dt.timezone.utc),
    )
    assert record_dead_letter(letter, GROUP) is True
    assert record_dead_letter(letter, GROUP) is False
    assert len(dlq_rows([event_id])) == 1



def test_api_shutdown_flushes_and_closes_the_replay_producer(client):
    from fastapi.testclient import TestClient

    from app.api.routes import dlq as dlq_routes
    from app.main import app

    recording = RecordingProducer()
    dlq_routes._producer = recording
    try:
        with TestClient(app):  # runs the lifespan: startup, then shutdown
            assert not recording.closed
        assert recording.closed
        assert dlq_routes._producer is None
    finally:
        dlq_routes._producer = None


def test_closing_without_a_replay_is_harmless():
    from app.api.routes import dlq as dlq_routes

    dlq_routes._producer = None
    dlq_routes.close_replay_producer()
