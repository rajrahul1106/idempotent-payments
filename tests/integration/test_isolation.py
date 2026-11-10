"""G6: the write path needs READ COMMITTED, so the engine pins it.

The balance check reads postings after the account lock is granted. Only
READ COMMITTED's per-statement snapshot sees what the previous lock holder
committed; under REPEATABLE READ two concurrent withdrawals both pass.

This test makes REPEATABLE READ the database default and checks that the
application still runs READ COMMITTED. It fails if anyone removes the pin
from app/db/session.py or changes it to another level.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text

from app.db.session import engine, unit_of_work

pytestmark = pytest.mark.integration


def _set_database_default(level: str | None) -> None:
    name = engine.url.database
    statement = (
        f"ALTER DATABASE \"{name}\" SET default_transaction_isolation = '{level}'"
        if level
        else f'ALTER DATABASE "{name}" RESET default_transaction_isolation'
    )
    with engine.connect() as conn:
        conn.execute(text(statement))
        conn.commit()
    engine.dispose()  # only new connections see a changed default


def test_write_path_runs_read_committed_even_when_the_database_default_differs():
    _set_database_default("repeatable read")
    try:
        # Control: a connection that pins nothing really does get the new
        # default, so the assertion below cannot pass by accident.
        unpinned = create_engine(engine.url)
        try:
            with unpinned.connect() as conn:
                default = conn.execute(text("SHOW transaction_isolation")).scalar_one()
        finally:
            unpinned.dispose()
        assert default == "repeatable read"

        with unit_of_work() as session:
            level = session.execute(text("SHOW transaction_isolation")).scalar_one()
        assert level == "read committed"
    finally:
        _set_database_default(None)
