"""Every pooled connection keeps its statement and lock timeouts.

Decision D2 depends on lock_timeout: a duplicate request gets 409 only
because its wait on the first request's key is bounded. The timeouts used
to be SET inside a connection's first transaction, so a rollback of that
transaction removed them for the connection's whole life.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from app.core.config import settings
from app.db.session import engine, unit_of_work

pytestmark = pytest.mark.integration

_SETTING = "SELECT setting FROM pg_settings WHERE name = :name"


def test_timeouts_survive_a_rolled_back_first_transaction():
    engine.dispose()  # the next checkout opens a brand-new connection

    with pytest.raises(DBAPIError):
        with unit_of_work() as session:
            first_pid = session.execute(text("SELECT pg_backend_pid()")).scalar_one()
            session.execute(text("SELECT 1/0"))

    with unit_of_work() as session:
        pid = session.execute(text("SELECT pg_backend_pid()")).scalar_one()
        lock_timeout = session.execute(text(_SETTING), {"name": "lock_timeout"}).scalar_one()
        statement_timeout = session.execute(
            text(_SETTING), {"name": "statement_timeout"}
        ).scalar_one()

    assert pid == first_pid, "expected the same pooled connection"
    assert int(lock_timeout) == settings.db_lock_timeout_ms
    assert int(statement_timeout) == settings.db_statement_timeout_ms
