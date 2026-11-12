"""Tracing in the API process (docs/ARCHITECTURE.md §5.8, Phase 7a).

The error body's trace_id is the request's real trace id; the payment's
server span carries the agreed attributes and nothing secret; the outbox
row stores this request's trace context; and an exporter that is down never
fails a payment.
"""

from __future__ import annotations

import json
import time
import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.core.telemetry import key_digest, trace_id_of
from app.db.session import read_session
from tests.tracing import EXTRA, SPANS, parent_id, span_id, trace_id

pytestmark = pytest.mark.integration


def _pay(client, token: str, source: str, destination: str, *, key: str | None = None,
         amount: int = 10_000, extra_headers: dict | None = None, reference: str | None = None):
    body = {
        "from_account_id": source,
        "to_account_id": destination,
        "amount_minor": amount,
        "currency": "INR",
    }
    if reference:
        body["reference"] = reference
    headers = {"Authorization": f"Bearer {token}", "Idempotency-Key": key or str(uuid.uuid4())}
    headers.update(extra_headers or {})
    return client.post("/v1/payments", json=body, headers=headers)


def _server_spans(captured, route: str) -> list[dict]:
    return [
        s for s in captured.dicts()
        if s["kind"] == "SpanKind.SERVER" and s["attributes"].get("http.route") == route
    ]


# --- trace_id in the error body ---------------------------------------------------------


def test_error_body_trace_id_is_the_request_trace(client, owner_token, make_account, spans):
    source, destination = make_account(1_000), make_account(0)

    refused = _pay(client, owner_token(source), source, destination, amount=50_000)
    assert refused.status_code == 422, refused.text
    missing = client.get("/v1/payments/pay_does_not_exist", headers={
        "Authorization": f"Bearer {owner_token(source)}"})
    assert missing.status_code == 404
    unauthenticated = client.get("/v1/dlq")
    assert unauthenticated.status_code == 401

    for response, route in (
        (refused, "/v1/payments"),
        (missing, "/v1/payments/{payment_id}"),
        (unauthenticated, "/v1/dlq"),
    ):
        body_trace = response.json()["error"]["trace_id"]
        assert len(body_trace) == 32 and body_trace != "0" * 32
        assert body_trace in {trace_id(s) for s in _server_spans(spans, route)}, route


def test_unhandled_error_500_also_carries_the_trace_id(make_account, owner_token, monkeypatch, spans):
    import app.api.routes.payments as payments_route
    from app.main import app

    def explode(_command):
        raise RuntimeError("injected bug")

    monkeypatch.setattr(payments_route, "create_payment", explode)
    source, destination = make_account(10_000), make_account(0)
    response = _pay(TestClient(app, raise_server_exceptions=False), owner_token(source),
                    source, destination)
    assert response.status_code == 500
    body_trace = response.json()["error"]["trace_id"]
    assert body_trace in {trace_id(s) for s in _server_spans(spans, "/v1/payments")}


# --- what a payment span carries -------------------------------------------------------------


def test_payment_span_attributes_and_nothing_secret(client, owner_token, make_account, spans):
    source, destination = make_account(100_000), make_account(0)
    key = str(uuid.uuid4())
    token = owner_token(source)
    secret_reference = f"ref-{uuid.uuid4().hex[:12]}"
    response = _pay(client, token, source, destination, key=key, reference=secret_reference)
    assert response.status_code == 201, response.text
    event_id = response.json()["event_id"]

    [server] = _server_spans(spans, "/v1/payments")
    attributes = server["attributes"]
    assert attributes["account_id"] == source
    assert attributes["event_id"] == event_id
    assert attributes["idempotency_key.sha256_prefix"] == key_digest(key)
    assert attributes["payment.replayed"] is False

    # The database spans of this request are children in the same trace.
    children = [s for s in spans.dicts() if trace_id(s) == trace_id(server)]
    assert any(s["kind"] == "SpanKind.CLIENT" for s in children), "no SQLAlchemy spans"

    # Never the raw key, the token, or anything from the request body.
    everything = json.dumps(spans.dicts())
    assert key not in everything
    assert token not in everything
    assert secret_reference not in everything
    assert "Bearer" not in everything


def test_outbox_row_stores_this_requests_trace_context(client, owner_token, make_account, spans):
    source, destination = make_account(100_000), make_account(0)
    response = _pay(client, owner_token(source), source, destination)
    assert response.status_code == 201
    with read_session() as session:
        stored = session.execute(
            text("SELECT traceparent FROM outbox WHERE event_id = :e"),
            {"e": response.json()["event_id"]},
        ).scalar_one()

    [server] = _server_spans(spans, "/v1/payments")
    assert trace_id_of(stored) == trace_id(server)
    assert stored.split("-")[2] == span_id(server)  # the server span is the parent


def test_a_callers_traceparent_is_continued(client, owner_token, make_account, spans):
    caller_trace = uuid.uuid4().hex
    caller = f"00-{caller_trace}-{uuid.uuid4().hex[:16]}-01"
    source, destination = make_account(100_000), make_account(0)
    response = _pay(client, owner_token(source), source, destination,
                    extra_headers={"traceparent": caller})
    assert response.status_code == 201
    [server] = _server_spans(spans, "/v1/payments")
    assert trace_id(server) == caller_trace
    assert parent_id(server) == caller.split("-")[2]


# --- an exporter outage never fails a payment -------------------------------------------------


def test_span_exporter_outage_never_fails_a_payment(client, owner_token, make_account, spans):
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    source, destination = make_account(500_000), make_account(0)
    # Both ways an exporter can be down: a synchronous exporter that raises
    # on every export, and the production path, a batch processor whose
    # OTLP collector does not answer (nothing listens on port 1).
    SPANS.fail = True
    EXTRA.inner = BatchSpanProcessor(
        OTLPSpanExporter(endpoint="http://127.0.0.1:1", insecure=True, timeout=1),
        schedule_delay_millis=50,
    )
    try:
        started = time.monotonic()
        codes = [_pay(client, owner_token(source), source, destination).status_code
                 for _ in range(5)]
        elapsed = time.monotonic() - started
        balance = client.get(f"/v1/accounts/{source}",
                             headers={"Authorization": f"Bearer {owner_token(source)}"})
    finally:
        SPANS.fail = False
        processor, EXTRA.inner = EXTRA.inner, None
        processor.shutdown()

    assert codes == [201] * 5
    assert balance.status_code == 200 and balance.json()["balance_minor"] == 450_000
    # Exporting happens off the request path: five payments are not slowed
    # by five failing exports (each would take its 1-second timeout).
    assert elapsed < 3.0, elapsed
