"""Shared fixtures. Integration tests run against a real PostgreSQL.

There are no mocked databases or brokers here. A mocked broker cannot
redeliver a message, and redelivery is the behaviour under test.
"""

from __future__ import annotations

import os
import uuid
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

os.environ.setdefault("ENVIRONMENT", "ci")


@pytest.fixture(scope="session")
def client() -> TestClient:
    from app.main import app

    return TestClient(app)


@pytest.fixture(scope="session")
def db_available() -> bool:
    from app.db.session import ping

    return ping()


@pytest.fixture(autouse=True)
def _require_db(request, db_available):
    """Skip integration-marked tests when no database is reachable."""
    if request.node.get_closest_marker("integration") and not db_available:
        pytest.skip("PostgreSQL not reachable")


@pytest.fixture(scope="session")
def seeded(db_available):
    if not db_available:
        pytest.skip("PostgreSQL not reachable")
    from scripts.seed import seed

    seed()
    return True


@pytest.fixture
def operator_token(client, seeded) -> str:
    resp = client.post(
        "/v1/auth/token",
        json={"username": "operator", "password": "operator-dev-password"},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["access_token"]


@pytest.fixture
def client_token(client, seeded) -> str:
    resp = client.post(
        "/v1/auth/token",
        json={"username": "alice", "password": "alice-dev-password"},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["access_token"]


@pytest.fixture
def viewer_token(client, seeded) -> str:
    resp = client.post(
        "/v1/auth/token",
        json={"username": "auditor", "password": "auditor-dev-password"},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["access_token"]


@pytest.fixture
def auth(operator_token):
    return {"Authorization": f"Bearer {operator_token}"}


@pytest.fixture
def make_account(seeded):
    """Create a funded customer account and return its id."""
    from app.db.models.ledger import Account, Posting, Transaction
    from app.db.session import unit_of_work

    created: list[str] = []

    def _make(opening_minor: int = 100_000, currency: str = "INR") -> str:
        account_id = f"acc_t_{uuid.uuid4().hex[:16]}"
        with unit_of_work() as session:
            session.add(
                Account(
                    id=account_id,
                    holder_name="Test Holder",
                    currency=currency,
                )
            )
            session.flush()
            if opening_minor:
                from app.core.money import to_decimal

                amount = to_decimal(opening_minor, currency)
                tx_id = f"tx_t_{uuid.uuid4().hex[:16]}"
                event_id = str(uuid.uuid4())
                session.add(
                    Transaction(id=tx_id, amount=amount, currency=currency)
                )
                session.add_all(
                    [
                        Posting(
                            transaction_id=tx_id,
                            account_id="sys_settlement",
                            event_id=event_id,
                            amount=-amount,
                            currency=currency,
                        ),
                        Posting(
                            transaction_id=tx_id,
                            account_id=account_id,
                            event_id=event_id,
                            amount=amount,
                            currency=currency,
                        ),
                    ]
                )
        created.append(account_id)
        return account_id

    return _make


@pytest.fixture
def ledger_total():
    """Invariant I2 helper: SUM(amount) over every posting."""
    from app.db.session import read_session

    def _total() -> Decimal:
        with read_session() as session:
            return Decimal(
                session.execute(
                    text("SELECT COALESCE(SUM(amount), 0) FROM postings")
                ).scalar_one()
            )

    return _total


@pytest.fixture(autouse=True)
def _reset_rate_limiter():
    from app.core.rate_limit import limiter

    limiter.reset()
    yield
    limiter.reset()
