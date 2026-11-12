"""Shared fixtures. Integration tests run against a real PostgreSQL.

There are no mocked databases or brokers here. A mocked broker cannot
redeliver a message, and redelivery is the behaviour under test.

The suite never touches the database `make up` serves. It reads
TEST_DATABASE_URL, refuses any database whose name does not end in
`_test`, and points the application at it before any app module loads.
Tests marked integration or messaging FAIL, never skip, when that database
is missing or unreachable, and messaging tests fail the same way when Kafka
is: a green run has to mean the gates ran.
"""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys
import uuid
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

ROOT = pathlib.Path(__file__).resolve().parent.parent

os.environ.setdefault("ENVIRONMENT", "ci")

# app.core.config reads DATABASE_URL once, at import, so it is set here,
# before any test module imports the app. When there is no usable test
# database it points somewhere unreachable, so nothing can fall back to a
# DATABASE_URL from the shell or from .env.
_UNREACHABLE = "postgresql+psycopg://no-test-database@127.0.0.1:1/unset_test"
_TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL", "").strip()
_REFUSAL: str | None = None  # the whole run refuses to start
_DB_PROBLEM: str | None = None  # tests that need the database fail

if not _TEST_DATABASE_URL:
    _DB_PROBLEM = "TEST_DATABASE_URL is not set, so the database tests cannot run"
else:
    try:
        _name = make_url(_TEST_DATABASE_URL).database or ""
    except ArgumentError:
        _name = ""
        _REFUSAL = "TEST_DATABASE_URL is not a valid database URL"
    if _REFUSAL is None and not _name.endswith("_test"):
        _REFUSAL = (
            f"TEST_DATABASE_URL names database {_name!r}. The suite only runs "
            "against a database whose name ends in _test, so it can never "
            "touch the one `make up` serves."
        )

os.environ["DATABASE_URL"] = (
    _UNREACHABLE if (_REFUSAL or _DB_PROBLEM) else _TEST_DATABASE_URL
)


# The test process is the api process for tracing. Its tracer provider is
# installed here, before any test module imports app.main, so app.main's
# setup_tracing("api") finds it and does nothing. Spans are exported
# synchronously into memory, and never to a real collector.
from app.core.telemetry import setup_tracing  # noqa: E402
from tests.tracing import EXTRA, SPANS  # noqa: E402

setup_tracing("api", exporter=SPANS, synchronous=True).add_span_processor(EXTRA)


@pytest.fixture
def spans():
    """Spans ended during this test, as dicts (see tests/tracing.py)."""
    SPANS.clear()
    SPANS.recording = True
    yield SPANS
    SPANS.recording = False
    SPANS.clear()


def pytest_configure(config):
    if _REFUSAL:
        raise pytest.UsageError(_REFUSAL)


def _ensure_test_database_exists() -> None:
    """Create the test database on first use. Its name always ends in _test."""
    target = make_url(_TEST_DATABASE_URL)
    admin = create_engine(
        target.set(database="postgres"),
        isolation_level="AUTOCOMMIT",
        connect_args={"connect_timeout": 5},
    )
    try:
        with admin.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": target.database},
            ).first()
            if exists is None:
                conn.execute(text(f'CREATE DATABASE "{target.database}"'))
    finally:
        admin.dispose()


def _migrate_test_database_to_head() -> None:
    # A subprocess, because alembic's env.py reconfigures logging and that
    # would change what caplog sees in this process.
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        pytest.fail(
            f"alembic upgrade head failed on the test database:\n{result.stderr}",
            pytrace=False,
        )


@pytest.fixture(scope="session")
def database() -> str:
    """The test database: reachable, migrated to head and seeded.

    Fails rather than skips, so a run in which the gates did not execute
    can never be green.
    """
    if _DB_PROBLEM:
        pytest.fail(_DB_PROBLEM, pytrace=False)
    shown = make_url(_TEST_DATABASE_URL).render_as_string(hide_password=True)
    try:
        _ensure_test_database_exists()
    except Exception as exc:  # unreachable server, bad credentials, ...
        pytest.fail(f"test database {shown} is not reachable: {exc}", pytrace=False)

    from app.db.session import ping

    if not ping():
        pytest.fail(f"test database {shown} is not reachable", pytrace=False)
    _migrate_test_database_to_head()

    from scripts.seed import seed

    seed()
    return _TEST_DATABASE_URL


@pytest.fixture(scope="session")
def kafka() -> str:
    """A reachable broker, or a failure. Never a skip.

    Messaging tests create their own topics with unique names, so they
    never read or write the topics `make up` uses.
    """
    from confluent_kafka.admin import AdminClient

    from app.core.config import settings

    bootstrap = settings.kafka_bootstrap_servers
    try:
        AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=5)
    except Exception as exc:
        pytest.fail(f"Kafka at {bootstrap} is not reachable: {exc}", pytrace=False)
    return bootstrap


@pytest.fixture(autouse=True)
def _require_db(request):
    """Database and broker tests fail, never skip, when either is unusable."""
    node = request.node
    if any(node.get_closest_marker(m) for m in ("integration", "messaging", "property")):
        request.getfixturevalue("database")
    if node.get_closest_marker("messaging") or node.get_closest_marker("property"):
        request.getfixturevalue("kafka")


@pytest.fixture(scope="session")
def client() -> TestClient:
    from app.main import app

    return TestClient(app)


@pytest.fixture(scope="session")
def seeded(database):
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
def owner_token():
    """A client token that owns the given account (decision D4, docs/ARCHITECTURE.md §16).

    Signed with the same secret the API verifies, so it is a real token; it
    just skips the seeded-user lookup, which test_accounts.py covers.
    """
    from app.core.security import issue_token

    def _token(account_id: str) -> str:
        token, _ = issue_token(f"owner-{account_id}", "client", account_id=account_id)
        return token

    return _token


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
