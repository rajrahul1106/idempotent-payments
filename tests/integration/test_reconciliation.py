"""The four invariant checks, and proof that each one can fail.

A check that cannot fail is worthless, so for each invariant this commits a
real violation to the test database, runs the checks through the service,
asserts that check (and only that check) reports it, then removes the
violation. Postings are append-only by rule and guarded by the deferred
balance trigger; `SET LOCAL session_replication_role = replica` turns both
off for that one transaction, which is how the I2 violation gets in and how
every injected posting gets out again.
"""

from __future__ import annotations

import threading
import time
import uuid
from contextlib import contextmanager
from decimal import Decimal

import pytest
from sqlalchemy import text

from app.db.session import engine, read_session
from app.services.reconciliation import FAIL, PASS, WARN, run_checks, run_every

pytestmark = pytest.mark.integration

NAMES = (
    "no_duplicate_event_ids",
    "ledger_balances_to_zero",
    "outbox_fully_published",
    "no_negative_balances",
)


@contextmanager
def replica_role():
    """One transaction with triggers and rules off. Test database only."""
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL session_replication_role = replica"))
        yield conn


def _statuses() -> dict[str, str]:
    return {c.name: c.status for c in run_checks(record=False).checks}


def _insert_transaction(conn, amount: str = "1") -> str:
    tx = f"tx_recon_{uuid.uuid4().hex[:16]}"
    conn.execute(
        text("INSERT INTO transactions (id, amount, currency) VALUES (:id, :a, 'INR')"),
        {"id": tx, "a": Decimal(amount)},
    )
    return tx


def _insert_posting(conn, tx: str, account: str, amount: str, event_id: str | None = None) -> None:
    conn.execute(
        text(
            "INSERT INTO postings (transaction_id, account_id, event_id, amount, currency) "
            "VALUES (:tx, :account, :event, :amount, 'INR')"
        ),
        {"tx": tx, "account": account, "event": event_id or str(uuid.uuid4()),
         "amount": Decimal(amount)},
    )


def _remove_transaction(tx: str) -> None:
    with replica_role() as conn:
        conn.execute(text("DELETE FROM postings WHERE transaction_id = :tx"), {"tx": tx})
        conn.execute(text("DELETE FROM transactions WHERE id = :tx"), {"tx": tx})


@pytest.fixture
def consistent(database):
    """Start from a database where all four checks pass.

    Other tests leave payment outbox rows unpublished (no relay runs against
    the test database), which I3 rightly reports. They are cleared first.
    """
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM outbox WHERE published_at IS NULL"))
    statuses = _statuses()
    assert statuses == dict.fromkeys(NAMES, PASS), statuses
    yield
    assert _statuses() == dict.fromkeys(NAMES, PASS), "a test left a violation behind"


def _only(name: str, status: str) -> dict[str, str]:
    expected = dict.fromkeys(NAMES, PASS)
    expected[name] = status
    return expected


# --- each check fails on its own violation ----------------------------------------------------


def test_i1_an_event_applied_to_two_transactions_fails(consistent, make_account):
    payer, payee = make_account(10_000), make_account(0)
    with read_session() as session:
        reused = str(session.execute(
            text("SELECT event_id FROM postings WHERE account_id = :a LIMIT 1"), {"a": payer}
        ).scalar_one())
    # A second, balanced transaction under an event_id already in the ledger:
    # the shape of an event applied twice.
    with engine.begin() as conn:
        tx = _insert_transaction(conn, "1")
        _insert_posting(conn, tx, payer, "-1", reused)
        _insert_posting(conn, tx, payee, "1", reused)
    try:
        report = run_checks(record=False)
        assert {c.name: c.status for c in report.checks} == _only("no_duplicate_event_ids", FAIL)
        [check] = [c for c in report.checks if c.name == "no_duplicate_event_ids"]
        assert check.detail == {"duplicates": 1}
    finally:
        _remove_transaction(tx)


def test_i2_an_unbalanced_ledger_fails(consistent, make_account):
    account = make_account(0)
    # One leg only. The deferred balance trigger would refuse this at COMMIT;
    # the replica role is the only way it gets in, which is the point.
    with replica_role() as conn:
        tx = _insert_transaction(conn, "0.0001")
        _insert_posting(conn, tx, account, "0.0001")
    try:
        report = run_checks(record=False)
        assert {c.name: c.status for c in report.checks} == _only("ledger_balances_to_zero", FAIL)
        [check] = [c for c in report.checks if c.name == "ledger_balances_to_zero"]
        assert check.detail == {"value": "0.0001"}  # a string, never a float
    finally:
        _remove_transaction(tx)


def test_i3_an_unpublished_outbox_row_warns(consistent):
    event_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO outbox (event_id, event_type, partition_key, payload, created_at) "
                "VALUES (:e, 'payment.posted', 'acc_recon', '{}', now() - interval '5 minutes')"
            ),
            {"e": event_id},
        )
    try:
        report = run_checks(record=False)
        # §9: a backlog warns. It is not a broken invariant yet; the row is
        # there and will be published.
        assert {c.name: c.status for c in report.checks} == _only("outbox_fully_published", WARN)
        [check] = [c for c in report.checks if c.name == "outbox_fully_published"]
        assert check.detail == {"unpublished_older_than_60s": 1}
    finally:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM outbox WHERE event_id = :e"), {"e": event_id})


def test_i3_a_fresh_unpublished_row_is_not_yet_a_warning(consistent):
    event_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO outbox (event_id, event_type, partition_key, payload) "
                "VALUES (:e, 'payment.posted', 'acc_recon', '{}')"
            ),
            {"e": event_id},
        )
    try:
        assert _statuses()["outbox_fully_published"] == PASS
    finally:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM outbox WHERE event_id = :e"), {"e": event_id})


def test_i4_a_negative_customer_balance_fails(consistent, make_account):
    empty, other = make_account(0), make_account(0)
    # Balanced, so the trigger accepts it and I2 still holds; it just takes
    # a customer below zero, which only the API's in-lock check prevents.
    with engine.begin() as conn:
        tx = _insert_transaction(conn, "5")
        _insert_posting(conn, tx, empty, "-5")
        _insert_posting(conn, tx, other, "5")
    try:
        report = run_checks(record=False)
        assert {c.name: c.status for c in report.checks} == _only("no_negative_balances", FAIL)
        [check] = [c for c in report.checks if c.name == "no_negative_balances"]
        assert check.detail == {"offending_accounts": 1}
    finally:
        _remove_transaction(tx)


# --- recording, the endpoint, the schedule ----------------------------------------------------


def _runs_since(started) -> list[tuple]:
    with read_session() as session:
        return session.execute(
            text(
                "SELECT check_name, status, detail, ran_at FROM reconciliation_runs "
                "WHERE ran_at >= :t ORDER BY id"
            ),
            {"t": started},
        ).all()


def _db_now():
    with read_session() as session:
        return session.execute(text("SELECT now()")).scalar_one()


def test_each_result_is_recorded(consistent):
    started = _db_now()
    report = run_checks()
    rows = _runs_since(started)
    assert [(r.check_name, r.status) for r in rows] == [(n, PASS) for n in NAMES]
    assert {r.ran_at for r in rows} == {report.checked_at}  # one snapshot, one time
    assert rows[1].detail == {"value": "0.0000"}


def test_endpoint_is_operator_only_and_returns_the_section_7_shape(
    consistent, client, operator_token, viewer_token, client_token
):
    for token in (viewer_token, client_token):
        denied = client.get("/v1/admin/reconciliation",
                            headers={"Authorization": f"Bearer {token}"})
        assert denied.status_code == 403
    response = client.get("/v1/admin/reconciliation",
                          headers={"Authorization": f"Bearer {operator_token}"})
    assert response.status_code == 200, response.text
    body = response.json()
    assert set(body) == {"checked_at", "checks"}
    assert body["checks"] == [
        {"name": "no_duplicate_event_ids", "status": "pass", "duplicates": 0},
        {"name": "ledger_balances_to_zero", "status": "pass", "value": "0.0000"},
        {"name": "outbox_fully_published", "status": "pass", "unpublished_older_than_60s": 0},
        {"name": "no_negative_balances", "status": "pass", "offending_accounts": 0},
    ]


def test_the_job_runs_on_its_schedule(consistent):
    started = _db_now()
    stop = threading.Event()
    job = threading.Thread(target=run_every, args=(0.3, stop), daemon=True)
    job.start()
    time.sleep(1.0)
    stop.set()
    job.join(timeout=10)
    runs = {r.ran_at for r in _runs_since(started)}
    assert len(runs) >= 3, runs  # at 0, 0.3, 0.6 and 0.9 s
