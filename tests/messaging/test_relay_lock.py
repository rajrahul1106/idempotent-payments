"""Decision D1, enforced: exactly one relay publishes.

The lock is a Postgres advisory lock on a dedicated connection that never
enters the pool. These tests check it the way an operator would: by asking
the server who holds it (pg_locks), with relays running as separate OS
processes where that matters, and by killing the lock's backend.
"""

from __future__ import annotations

import logging
import pathlib
import signal
import subprocess
import sys
import threading
import time
import uuid

import pytest
from sqlalchemy import text

from app.db.session import engine, read_session
from app.messaging import relay
from tests.messaging.conftest import RecordingProducer, wait_for

pytestmark = pytest.mark.integration

ROOT = pathlib.Path(__file__).resolve().parents[2]


# --- asking the server ------------------------------------------------------------------


def _lock_holders() -> list[int]:
    """Backend pids holding the relay lock in this database (pg_locks is
    server-wide; the `make up` relay holds the same id in its own database)."""
    with read_session() as session:
        return list(session.execute(
            text(
                "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND objid = :id "
                "AND granted AND database = "
                "(SELECT oid FROM pg_database WHERE datname = current_database())"
            ),
            {"id": relay.RELAY_LOCK_ID},
        ).scalars())


def _backends(application_name: str) -> list[int]:
    with read_session() as session:
        return list(session.execute(
            text(
                "SELECT pid FROM pg_stat_activity WHERE application_name = :name "
                "AND datname = current_database()"
            ),
            {"name": application_name},
        ).scalars())


def _terminate(pid: int) -> None:
    with engine.connect() as conn:
        assert conn.execute(text("SELECT pg_terminate_backend(:p)"), {"p": pid}).scalar_one()


# --- relays as OS processes ----------------------------------------------------------------

_RELAY_PROCESS = """
import logging, signal, sys, threading
logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                    format="%(asctime)s %(levelname)s %(message)s")
from app.messaging import relay

class FileProducer:
    def __init__(self, path):
        self.out = open(path, "a", buffering=1)
    def publish(self, topic, key, value, headers=None):
        self.out.write(headers["event_id"] + "\\n")
    def flush(self, timeout_seconds=10.0):
        return 0
    def close(self):
        self.out.close()

stop = threading.Event()
signal.signal(signal.SIGTERM, lambda *_: stop.set())
relay.run_forever(producer=FileProducer(sys.argv[1]), stop=stop,
                  lock_retry_seconds=0.2, poll_interval=0.2)
"""


class RelayProcess:
    def __init__(self, tmp_path: pathlib.Path, name: str) -> None:
        self.published_file = tmp_path / f"{name}.published"
        self.log_file = tmp_path / f"{name}.log"
        self.published_file.touch()
        self._stderr = open(self.log_file, "w")  # closed in stop()
        self.proc = subprocess.Popen(
            [sys.executable, "-c", _RELAY_PROCESS, str(self.published_file)],
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=self._stderr,
        )

    @property
    def published(self) -> list[str]:
        return self.published_file.read_text().split()

    @property
    def log(self) -> str:
        return self.log_file.read_text()

    def lock_pid(self) -> int | None:
        """This process's lock backend, from pg_stat_activity's client port."""
        holders = _lock_holders()
        return holders[0] if holders else None

    def stop(self, sig: int = signal.SIGTERM) -> None:
        if self.proc.poll() is None:
            self.proc.send_signal(sig)
            self.proc.wait(timeout=20)
        self._stderr.close()


@pytest.fixture
def relay_processes(tmp_path, clear_outbox):
    started: list[RelayProcess] = []

    def start(name: str) -> RelayProcess:
        process = RelayProcess(tmp_path, name)
        started.append(process)
        return process

    yield start
    for process in started:
        process.stop(signal.SIGKILL)


@pytest.mark.parametrize("how", ["sigterm", "sigkill"])
def test_second_relay_process_waits_and_takes_over(relay_processes, make_outbox_rows, how):
    first = relay_processes("first")
    assert wait_for(lambda: len(_lock_holders()) == 1, timeout=20), first.log
    first_holder = _lock_holders()[0]

    second = relay_processes("second")
    assert wait_for(lambda: "another relay holds" in second.log, timeout=20), second.log

    batch = make_outbox_rows(20)
    assert wait_for(lambda: first.published == batch, timeout=20)
    time.sleep(1.0)  # every chance for the second relay to misbehave
    assert second.published == []
    assert _lock_holders() == [first_holder]

    if how == "sigterm":
        first.stop(signal.SIGTERM)
        assert "relay lock 506032705 released" in first.log  # explicit unlock
    else:
        first.stop(signal.SIGKILL)  # no unlock: the server frees it with the connection
    assert wait_for(lambda: "acquired" in second.log, timeout=20), second.log
    assert wait_for(lambda: len(_lock_holders()) == 1 and _lock_holders()[0] != first_holder)

    later = make_outbox_rows(5)
    assert wait_for(lambda: second.published == later, timeout=20)
    assert first.published == batch


# --- in-process relays ----------------------------------------------------------------------


class ThreadRelay:
    def __init__(self, **kwargs) -> None:
        self.producer = RecordingProducer()
        self.stop_event = threading.Event()
        kwargs.setdefault("lock_retry_seconds", 0.1)
        kwargs.setdefault("poll_interval", 0.2)
        self.thread = threading.Thread(
            target=relay.run_forever,
            kwargs={"producer": self.producer, "stop": self.stop_event, **kwargs},
            daemon=True,
        )
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=15)
        assert not self.thread.is_alive()


@pytest.fixture
def thread_relays(clear_outbox):
    started: list[ThreadRelay] = []

    def start(**kwargs) -> ThreadRelay:
        running = ThreadRelay(**kwargs)
        started.append(running)
        return running

    yield start
    for running in started:
        running.stop_event.set()
    for running in started:
        running.thread.join(timeout=15)


def test_a_relay_restarted_in_the_same_process_reacquires(thread_relays, make_outbox_rows):
    first = thread_relays(listen=False)
    assert wait_for(lambda: len(_lock_holders()) == 1)
    lock_backend = _lock_holders()[0]
    first.stop()

    # Released on the server and its connection closed, not parked in a pool
    # where this process could inherit it.
    assert _lock_holders() == []
    assert lock_backend not in _backends(relay.LOCK_APPLICATION_NAME)

    second = thread_relays(listen=False)
    assert wait_for(lambda: len(_lock_holders()) == 1)
    assert _lock_holders()[0] != lock_backend
    rows = make_outbox_rows(5)
    assert wait_for(lambda: second.producer.event_ids == rows, timeout=10)


def test_terminated_lock_connection_stops_publishing_then_reacquires(
    thread_relays, make_outbox_rows, caplog
):
    running = thread_relays(listen=False, poll_interval=0.5)
    assert wait_for(lambda: len(_lock_holders()) == 1)
    [lock_backend] = _lock_holders()

    # An operator, or a failover, kills the lock's backend. To see the relay
    # stop, something else takes the lock the instant it is free.
    import psycopg

    thief = psycopg.connect(relay._conninfo(), autocommit=True)
    try:
        with caplog.at_level(logging.INFO, logger="app.messaging.relay"):
            _terminate(lock_backend)
            assert wait_for(
                lambda: thief.execute("SELECT pg_try_advisory_lock(%s)",
                                      (relay.RELAY_LOCK_ID,)).fetchone()[0],
                timeout=5, interval=0.01,
            )
            # Within one tick the relay sees, from the server, that it lost it.
            assert wait_for(lambda: any("relay lost lock" in r.getMessage()
                                        for r in caplog.records), timeout=2)
            rows = make_outbox_rows(5)
            time.sleep(1.5)
            assert running.producer.event_ids == []  # not publishing without the lock

            thief.execute("SELECT pg_advisory_unlock(%s)", (relay.RELAY_LOCK_ID,))
            assert wait_for(lambda: running.producer.event_ids == rows, timeout=10)
    finally:
        thief.close()
    [new_backend] = _lock_holders()
    assert new_backend != lock_backend


def test_terminated_listen_connection_falls_back_to_polling_and_reconnects(
    thread_relays, make_outbox_rows, client, owner_token, make_account, caplog
):
    running = thread_relays(listen=True, poll_interval=2.0)
    assert wait_for(lambda: len(_backends(relay.LISTEN_APPLICATION_NAME)) == 1, timeout=10)
    [listen_backend] = _backends(relay.LISTEN_APPLICATION_NAME)

    with caplog.at_level(logging.WARNING, logger="app.messaging.relay"):
        _terminate(listen_backend)
        # Rows with no notification at all: only the poll can find them.
        rows = make_outbox_rows(5)
        assert wait_for(lambda: running.producer.event_ids == rows, timeout=15)
        assert any("LISTEN unavailable" in r.getMessage() for r in caplog.records)

    # It reconnects, on a new backend, and NOTIFY wakes it again.
    assert wait_for(lambda: _backends(relay.LISTEN_APPLICATION_NAME) not in ([], [listen_backend]),
                    timeout=15)
    time.sleep(0.5)  # back in its idle wait
    source, destination = make_account(10_000), make_account(0)
    started = time.monotonic()
    response = client.post(
        "/v1/payments",
        json={"from_account_id": source, "to_account_id": destination,
              "amount_minor": 100, "currency": "INR"},
        headers={"Authorization": f"Bearer {owner_token(source)}",
                 "Idempotency-Key": str(uuid.uuid4())},
    )
    assert response.status_code == 201
    event_id = response.json()["event_id"]
    assert wait_for(lambda: event_id in running.producer.event_ids, timeout=10, interval=0.01)
    assert time.monotonic() - started < 1.0  # woken, not polled (poll is 2 s)
