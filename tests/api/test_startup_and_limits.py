"""A broken router fails startup; request bodies are capped at 64 KB."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

from app.api.body_limit import MAX_BODY_BYTES
from tests.conftest import ROOT

pytestmark = pytest.mark.integration

V1_ROUTES = {
    ("POST", "/v1/auth/token"),
    ("POST", "/v1/accounts"),
    ("GET", "/v1/accounts/{account_id}"),
    ("GET", "/v1/accounts/{account_id}/postings"),
    ("POST", "/v1/payments"),
    ("GET", "/v1/payments/{payment_id}"),
    ("GET", "/v1/dlq"),
    ("POST", "/v1/dlq/{message_id}/replay"),
    ("GET", "/v1/admin/reconciliation"),
}


def test_every_v1_endpoint_is_mounted():
    from app.main import app

    mounted = {
        (method.upper(), path)
        for path, operations in app.openapi()["paths"].items()
        for method in operations
    }
    assert V1_ROUTES <= mounted, V1_ROUTES - mounted


@pytest.mark.parametrize("module", ["auth", "accounts", "payments", "dlq", "admin"])
def test_a_router_that_cannot_be_imported_stops_startup(module):
    """A router module that cannot be imported used to be skipped, and the
    API started without its endpoints."""
    code = (
        "import sys\n"
        f"sys.modules['app.api.routes.{module}'] = None\n"
        "import app.main\n"
    )
    env = {**os.environ, "OTEL_ENABLED": "false"}
    result = subprocess.run(
        [sys.executable, "-c", code], cwd=ROOT, env=env, capture_output=True, text=True, timeout=60
    )
    assert result.returncode != 0
    assert f"app.api.routes.{module}" in result.stderr


def _assert_413(resp):
    assert resp.status_code == 413
    error = resp.json()["error"]
    assert error["code"] == "payload_too_large"
    assert set(error) == {"code", "message", "request_id", "trace_id"}
    assert resp.headers["X-Request-Id"] == error["request_id"]


def test_body_over_64_kb_is_413_before_auth(client, seeded):
    # No token: the cap answers first, so nothing behind it reads the body.
    resp = client.post(
        "/v1/payments",
        content=b"x" * (MAX_BODY_BYTES + 1),
        headers={"Content-Type": "application/json"},
    )
    _assert_413(resp)


def test_chunked_body_over_64_kb_is_413(client):
    def chunks():
        for _ in range(MAX_BODY_BYTES // 1024 + 1):
            yield b"x" * 1024

    resp = client.post(
        "/v1/payments", content=chunks(), headers={"Content-Type": "application/json"}
    )
    assert "content-length" not in {k.lower() for k in resp.request.headers}
    _assert_413(resp)


def test_body_at_the_cap_is_let_through(client):
    resp = client.post(
        "/v1/payments",
        content=b"x" * MAX_BODY_BYTES,
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code != 413  # reaches auth, which refuses it


def test_a_normal_payment_body_still_reaches_the_route(client, owner_token, make_account):
    import uuid

    source, destination = make_account(10_000), make_account(0)
    resp = client.post(
        "/v1/payments",
        json={
            "from_account_id": source,
            "to_account_id": destination,
            "amount_minor": 100,
            "currency": "INR",
            "metadata": {"note": "y" * 1500},
        },
        headers={
            "Authorization": f"Bearer {owner_token(source)}",
            "Idempotency-Key": str(uuid.uuid4()),
        },
    )
    assert resp.status_code == 201, resp.text
