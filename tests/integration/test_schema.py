"""Phase 1 gate: the migrations apply and roll back cleanly, and the
database itself rejects what the ledger forbids, with no application code
involved.

This replaces scripts/verify_phase1.sql, which printed its results for a
person to read and could not fail.
"""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys
import uuid
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError

from app.db.session import engine

pytestmark = pytest.mark.integration

ROOT = pathlib.Path(__file__).resolve().parents[2]

TABLES = {
    "accounts",
    "transactions",
    "postings",
    "idempotency_keys",
    "outbox",
    "processed_events",
    "dlq_messages",
    "reconciliation_runs",
    "payment_events",
    "auth_users",
    "delivery_failures",
}

INSERT_TRANSACTION = text(
    "INSERT INTO transactions (id, amount, currency) VALUES (:id, :amount, 'INR')"
)
INSERT_POSTING = text(
    "INSERT INTO postings (transaction_id, account_id, event_id, amount, currency) "
    "VALUES (:tx, :account, :event, :amount, 'INR')"
)
INSERT_KEY = text(
    "INSERT INTO idempotency_keys (key, fingerprint, state) "
    "VALUES (:key, :fingerprint, 'completed')"
)


def _tx_id() -> str:
    return f"tx_schema_{uuid.uuid4().hex[:16]}"


def _posting(tx: str, account: str, amount: str) -> dict:
    return {"tx": tx, "account": account, "event": str(uuid.uuid4()), "amount": Decimal(amount)}


# --- migrations ---------------------------------------------------------------


def _alembic(url, *args: str) -> None:
    env = {**os.environ, "DATABASE_URL": url.render_as_string(hide_password=False)}
    result = subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr


def _snapshot(url) -> dict[str, set[str]]:
    inspector = create_engine(url)
    try:
        with inspector.connect() as conn:

            def names(sql: str) -> set[str]:
                return set(conn.execute(text(sql)).scalars())

            return {
                "tables": names("SELECT tablename FROM pg_tables WHERE schemaname = 'public'"),
                "enums": names(
                    "SELECT t.typname FROM pg_type t JOIN pg_namespace n "
                    "ON n.oid = t.typnamespace WHERE n.nspname = 'public' AND t.typtype = 'e'"
                ),
                "functions": names(
                    "SELECT p.proname FROM pg_proc p JOIN pg_namespace n "
                    "ON n.oid = p.pronamespace WHERE n.nspname = 'public'"
                ),
                "sequences": names(
                    "SELECT sequencename FROM pg_sequences WHERE schemaname = 'public'"
                ),
                "deferred_triggers": names(
                    "SELECT t.tgname FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
                    "WHERE c.relname = 'postings' AND t.tgdeferrable AND t.tginitdeferred"
                ),
                "rules": names("SELECT rulename FROM pg_rules WHERE tablename = 'postings'"),
                "partial_outbox_index": names(
                    "SELECT indexname FROM pg_indexes WHERE tablename = 'outbox' "
                    "AND indexdef LIKE '%WHERE (published_at IS NULL)%'"
                ),
            }
    finally:
        inspector.dispose()


@pytest.fixture
def scratch_database(database):
    """A throwaway database for the round trip, so downgrading never wipes
    the data the rest of the suite is using. Its name also ends in _test."""
    test_url = make_url(database)
    name = test_url.database.removesuffix("_test") + "_migrations_test"
    admin = create_engine(test_url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f'DROP DATABASE IF EXISTS "{name}"'))
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    try:
        yield test_url.set(database=name)
    finally:
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}"'))
        admin.dispose()


def test_migrations_apply_roll_back_and_reapply_cleanly(scratch_database):
    _alembic(scratch_database, "upgrade", "head")
    applied = _snapshot(scratch_database)
    assert applied["tables"] == TABLES | {"alembic_version"}
    assert applied["deferred_triggers"] == {"postings_balance_check"}
    assert applied["rules"] == {"postings_no_update", "postings_no_delete"}
    assert applied["partial_outbox_index"] == {"ix_outbox_unpublished"}

    _alembic(scratch_database, "downgrade", "base")
    rolled_back = _snapshot(scratch_database)
    assert rolled_back["tables"] == {"alembic_version"}
    assert rolled_back["enums"] == set()
    assert rolled_back["functions"] == set()
    assert rolled_back["sequences"] == set()

    _alembic(scratch_database, "upgrade", "head")
    assert _snapshot(scratch_database) == applied


def test_migration_0004_adds_and_drops_the_transaction_id_index(scratch_database):
    def indexes() -> dict[str, str]:
        eng = create_engine(scratch_database)
        try:
            with eng.connect() as conn:
                return dict(
                    conn.execute(
                        text("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'idempotency_keys'")
                    ).all()
                )
        finally:
            eng.dispose()

    _alembic(scratch_database, "upgrade", "0004")
    after = indexes()
    assert "ix_idempotency_transaction_id" in after
    assert after["ix_idempotency_transaction_id"].endswith("USING btree (transaction_id)")

    _alembic(scratch_database, "downgrade", "0003")
    assert "ix_idempotency_transaction_id" not in indexes()
    assert "ix_idempotency_expires" in indexes()


def test_payment_events_money_is_numeric_20_4():
    """Migration 0002: one representation of money in the database."""
    with engine.connect() as conn:
        columns = dict(
            conn.execute(
                text(
                    "SELECT column_name, data_type || '(' || coalesce(numeric_precision, 0) "
                    "|| ',' || coalesce(numeric_scale, 0) || ')' FROM information_schema.columns "
                    "WHERE table_name = 'payment_events' AND column_name LIKE 'amount%'"
                )
            ).all()
        )
    assert columns == {"amount": "numeric(20,4)"}


# --- what the database rejects on its own ------------------------------------------


def test_duplicate_idempotency_key_is_rejected_by_the_database():
    key = str(uuid.uuid4())
    with engine.connect() as conn:
        conn.execute(INSERT_KEY, {"key": key, "fingerprint": "a" * 64})
        with pytest.raises(IntegrityError) as caught:
            conn.execute(INSERT_KEY, {"key": key, "fingerprint": "b" * 64})
        conn.rollback()

    assert caught.value.orig.sqlstate == "23505"
    assert caught.value.orig.diag.constraint_name == "idempotency_keys_pkey"


@pytest.mark.parametrize(
    "legs",
    [
        pytest.param(["-100"], id="one-leg"),
        pytest.param(["-100", "100.0001"], id="off-by-a-ten-thousandth"),
    ],
)
def test_unbalanced_transaction_cannot_commit(make_account, legs):
    accounts = [make_account(0) for _ in legs]
    tx = _tx_id()
    with engine.connect() as conn:
        conn.execute(INSERT_TRANSACTION, {"id": tx, "amount": Decimal("100")})
        for account, amount in zip(accounts, legs):
            conn.execute(INSERT_POSTING, _posting(tx, account, amount))
        # Each insert is accepted: the trigger is deferred to COMMIT.
        with pytest.raises(IntegrityError) as caught:
            conn.commit()

    assert caught.value.orig.sqlstate == "23514"
    with engine.connect() as conn:
        left = conn.execute(
            text("SELECT count(*) FROM transactions WHERE id = :id"), {"id": tx}
        ).scalar_one()
    assert left == 0


def test_balanced_transaction_commits_and_postings_are_append_only(make_account):
    # Fund the payer first. This test writes postings directly, past the
    # API's balance check, and must not leave a negative customer balance
    # behind (I4) in the shared test database.
    payer, payee = make_account(10_000), make_account(0)
    tx = _tx_id()
    with engine.begin() as conn:
        conn.execute(INSERT_TRANSACTION, {"id": tx, "amount": Decimal("100")})
        conn.execute(INSERT_POSTING, _posting(tx, payer, "-100"))
        conn.execute(INSERT_POSTING, _posting(tx, payee, "100"))

    with engine.begin() as conn:
        updated = conn.execute(
            text("UPDATE postings SET amount = 999999 WHERE transaction_id = :tx"), {"tx": tx}
        ).rowcount
        deleted = conn.execute(
            text("DELETE FROM postings WHERE transaction_id = :tx"), {"tx": tx}
        ).rowcount
        amounts = sorted(
            conn.execute(
                text("SELECT amount FROM postings WHERE transaction_id = :tx"), {"tx": tx}
            ).scalars()
        )

    assert (updated, deleted) == (0, 0)
    assert amounts == [Decimal("-100.0000"), Decimal("100.0000")]


def test_zero_amount_posting_is_rejected(make_account):
    tx = _tx_id()
    with engine.connect() as conn:
        conn.execute(INSERT_TRANSACTION, {"id": tx, "amount": Decimal("1")})
        with pytest.raises(IntegrityError) as caught:
            conn.execute(INSERT_POSTING, _posting(tx, make_account(0), "0"))
        conn.rollback()
    assert caught.value.orig.sqlstate == "23514"


def test_non_positive_transaction_amount_is_rejected():
    with engine.connect() as conn:
        with pytest.raises(IntegrityError) as caught:
            conn.execute(INSERT_TRANSACTION, {"id": _tx_id(), "amount": Decimal("-5")})
        conn.rollback()
    assert caught.value.orig.sqlstate == "23514"


def test_numeric_sums_without_drift():
    with engine.connect() as conn:
        total = conn.execute(
            text(
                "SELECT SUM(v) FROM (SELECT 0.1::NUMERIC(20,4) AS v "
                "FROM generate_series(1, 1000)) s"
            )
        ).scalar_one()
    assert total == Decimal("100.0000")
