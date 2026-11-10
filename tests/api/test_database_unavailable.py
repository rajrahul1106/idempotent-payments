"""G10: when PostgreSQL is unreachable the API answers 503, not 500.

Needs no database. It points the session factory at a port nothing listens
on, which is what the API sees while Postgres is down or restarting.
"""

from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine


@pytest.fixture
def unreachable_database():
    from app.db.session import SessionFactory, engine

    dead = create_engine(
        "postgresql+psycopg://nobody:nothing@127.0.0.1:1/unreachable_test",
        connect_args={"connect_timeout": 2},
    )
    SessionFactory.configure(bind=dead)
    try:
        yield
    finally:
        SessionFactory.configure(bind=engine)
        dead.dispose()


def test_reads_and_writes_return_503_with_the_error_shape(unreachable_database):
    from app.core.security import issue_token
    from app.main import app

    client = TestClient(app, raise_server_exceptions=False)
    token, _ = issue_token("owner-acc_alice", "client", account_id="acc_alice")
    auth = {"Authorization": f"Bearer {token}"}

    read = client.get("/v1/accounts/acc_alice", headers=auth)
    write = client.post(
        "/v1/payments",
        json={
            "from_account_id": "acc_alice",
            "to_account_id": "acc_bob",
            "amount_minor": 1_000,
            "currency": "INR",
        },
        headers={**auth, "Idempotency-Key": str(uuid.uuid4())},
    )

    for resp in (read, write):
        assert resp.status_code == 503, resp.text
        error = resp.json()["error"]
        assert error["code"] == "service_unavailable"
        assert error["request_id"]
        assert "trace_id" in error
