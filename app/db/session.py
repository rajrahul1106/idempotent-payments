"""Engine, session factory, and the one place transactions are opened.

Rule (docs/ARCHITECTURE.md §12): services own transaction boundaries, repositories
never commit. `unit_of_work()` is the only commit point in the write path.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import settings

# READ COMMITTED is pinned here, not left to the server default, because
# the write path depends on it. The balance check runs after the account
# lock is granted, and it must see the postings the previous lock holder
# committed. READ COMMITTED takes a fresh snapshot per statement, so it
# does. REPEATABLE READ keeps the snapshot from before the lock wait, so
# two concurrent withdrawals both pass the check: measured at 20 out of 20
# overdrafts in the audit. tests/integration/test_isolation.py fails if
# this line changes or goes away.
ISOLATION_LEVEL = "READ COMMITTED"

engine: Engine = create_engine(
    settings.database_url,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_max_overflow,
    pool_pre_ping=True,
    isolation_level=ISOLATION_LEVEL,
    # Bound every statement and every lock wait. Without a lock_timeout a
    # stuck transaction holds a row lock until someone notices, and a
    # duplicate request waits on its key forever instead of getting 409
    # (decision D2). Three seconds is long enough for a healthy transfer
    # and short enough to surface a problem.
    #
    # These are libpq startup options, so they are session defaults from
    # the moment the connection opens. They used to be SET in a connect
    # hook, which ran inside a transaction: when a connection's first
    # transaction rolled back, the timeouts were silently undone for the
    # rest of its life.
    connect_args={
        "options": (
            f"-c statement_timeout={settings.db_statement_timeout_ms} "
            f"-c lock_timeout={settings.db_lock_timeout_ms}"
        )
    },
    future=True,
)


SessionFactory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


@contextmanager
def unit_of_work() -> Iterator[Session]:
    """One database transaction. Commits on success, rolls back on any error."""
    session = SessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@contextmanager
def read_session() -> Iterator[Session]:
    """Read-only session. Never commits."""
    session = SessionFactory()
    try:
        yield session
    finally:
        session.close()


def ping() -> bool:
    """True when the database answers. Used by /ready."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
