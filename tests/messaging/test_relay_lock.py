"""Decision D1, enforced: exactly one relay publishes.

Two relays run the real loop against the test database. The second finds
the advisory lock taken, says so, and never publishes. It takes over only
once the first stops and its lock connection closes.
"""

from __future__ import annotations

import logging
import threading
import time

import pytest
from sqlalchemy import text

from app.db.session import read_session
from app.messaging import relay
from tests.messaging.conftest import RecordingProducer

pytestmark = pytest.mark.integration


def _wait_for(condition, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.05)
    return condition()


def _lock_holders() -> int:
    with read_session() as session:
        return session.execute(
            text(
                # pg_locks spans the whole server; advisory locks are per
                # database, and the `make up` relay holds this id in its own.
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' "
                "AND objid = :id AND granted AND database = "
                "(SELECT oid FROM pg_database WHERE datname = current_database())"
            ),
            {"id": relay.RELAY_LOCK_ID},
        ).scalar_one()


def _start(producer: RecordingProducer) -> tuple[threading.Thread, threading.Event]:
    stop = threading.Event()
    thread = threading.Thread(
        target=relay.run_forever,
        kwargs={"producer": producer, "stop": stop, "lock_retry_seconds": 0.1},
        daemon=True,
    )
    thread.start()
    return thread, stop


def test_second_relay_waits_and_never_publishes(make_outbox_rows, caplog):
    first_producer, second_producer = RecordingProducer(), RecordingProducer()
    first, stop_first = _start(first_producer)
    second = stop_second = None
    try:
        assert _wait_for(lambda: _lock_holders() == 1), "first relay never took the lock"

        with caplog.at_level(logging.INFO, logger="app.messaging.relay"):
            second, stop_second = _start(second_producer)
            assert _wait_for(
                lambda: any("another relay holds" in r.getMessage() for r in caplog.records)
            ), "second relay did not report the lock as taken"

            batch = make_outbox_rows(20)
            assert _wait_for(lambda: first_producer.event_ids == batch)
            time.sleep(0.5)  # give the second relay every chance to misbehave
            assert second_producer.messages == []
            assert _lock_holders() == 1

            # The first relay stops: its lock connection closes, the second
            # takes over and publishes what comes next.
            stop_first.set()
            first.join(timeout=10)
            assert _wait_for(
                lambda: any("acquired" in r.getMessage() for r in caplog.records
                            if r.thread == second.ident)
            ), "second relay did not take over"
            later = make_outbox_rows(5)
            assert _wait_for(lambda: second_producer.event_ids == later)
            assert first_producer.event_ids == batch
    finally:
        stop_first.set()
        if stop_second is not None:
            stop_second.set()
        first.join(timeout=10)
        if second is not None:
            second.join(timeout=10)
