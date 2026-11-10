"""What a database error means, in one place.

The API maps "the database is not there" to 503 (blueprint §8). The
consumers map the same condition to "hold the message and retry it"
(decision D5). Both ask this module, so they cannot disagree. Nothing here
imports the web framework.
"""

from __future__ import annotations

from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeout

# SQLSTATEs that mean the database is not there, as opposed to the database
# refusing a statement. Class 08 is connection exceptions; 57P01 to 57P03
# are admin shutdown, crash shutdown and "cannot connect now" (starting up).
_UNAVAILABLE_SQLSTATES = frozenset({"57P01", "57P02", "57P03"})


def database_unavailable(exc: BaseException) -> bool:
    """True when the error means Postgres cannot be reached.

    A lock or statement timeout is also an OperationalError, but the database
    answered, so it is not "unavailable".
    """
    if isinstance(exc, PoolTimeout):
        # Every pooled connection is checked out and none came back in time.
        return True
    if isinstance(exc, InterfaceError):
        return True
    if not isinstance(exc, OperationalError):
        return isinstance(exc, DBAPIError) and exc.connection_invalidated
    if exc.connection_invalidated:
        return True
    sqlstate = getattr(exc.orig, "sqlstate", None)
    if sqlstate is None:
        # Raised by the driver itself: refused, reset or timed out connecting.
        return True
    return sqlstate.startswith("08") or sqlstate in _UNAVAILABLE_SQLSTATES
