"""Engine, session factory, and the one place transactions are opened.

Rule from the blueprint: services own transaction boundaries, repositories
never commit. `unit_of_work()` is the only commit point in the write path.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import settings

engine: Engine = create_engine(
    settings.database_url,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_max_overflow,
    pool_pre_ping=True,
    future=True,
)


@event.listens_for(engine, "connect")
def _set_timeouts(dbapi_connection, _connection_record) -> None:
    """Bound every statement and every lock wait.

    Without a lock_timeout a deadlock or a stuck transaction holds a row
    lock until someone notices. Three seconds is long enough for a healthy
    transfer and short enough to surface a problem.
    """
    with dbapi_connection.cursor() as cur:
        cur.execute(f"SET statement_timeout = {settings.db_statement_timeout_ms}")
        cur.execute(f"SET lock_timeout = {settings.db_lock_timeout_ms}")


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
