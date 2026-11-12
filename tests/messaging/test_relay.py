"""Phase 4 gate: a relay killed with 50 unpublished rows recovers all of them.

I3: every committed outbox row is eventually published at least once.

The broker is a recording double (see conftest.py). What is under test is
whether the relay loses a committed row, which is a question about
PostgreSQL and the relay's transaction boundary, and both of those are
real. The first test kills a real relay process with SIGKILL mid-batch, so
it is the database, not an exception handler, that gets the rows back.
"""

from __future__ import annotations

import pathlib
import subprocess
import sys
import time

import pytest
from sqlalchemy import text

from app.db.session import read_session
from app.messaging import relay
from app.messaging.topics import HEADER_EVENT_ID, HEADER_TRACEPARENT, MAIN_TOPIC
from tests.messaging.conftest import CrashingProducer, RecordingProducer

pytestmark = pytest.mark.integration

ROOT = pathlib.Path(__file__).resolve().parents[2]

# A real relay tick in its own process, with a broker double that
# acknowledges slowly enough to be killed halfway through the batch.
_SLOW_RELAY = """
import time
from app.messaging import relay

class SlowProducer:
    def publish(self, topic, key, value, headers=None):
        print("ack", flush=True)
        time.sleep(0.05)

    def flush(self, timeout_seconds=10.0):
        return 0

    def close(self):
        pass

relay.run_once(SlowProducer())
print("finished", flush=True)
"""


def _unpublished() -> int:
    with read_session() as session:
        return session.execute(
            text("SELECT count(*) FROM outbox WHERE published_at IS NULL")
        ).scalar_one()


def _unlocked_unpublished() -> int:
    """Unpublished rows that no transaction currently holds."""
    with read_session() as session:
        return session.execute(
            text(
                "SELECT count(*) FROM (SELECT id FROM outbox "
                "WHERE published_at IS NULL FOR UPDATE SKIP LOCKED) free"
            )
        ).scalar_one()


def test_relay_process_killed_mid_batch_recovers_all_50(make_outbox_rows):
    event_ids = make_outbox_rows(50)

    victim = subprocess.Popen(
        [sys.executable, "-c", _SLOW_RELAY],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        acknowledged = 0
        while acknowledged < 20:
            line = victim.stdout.readline()
            assert line, f"relay exited early: {victim.stderr.read()}"
            acknowledged += line.strip() == "ack"
    finally:
        victim.kill()  # SIGKILL: no finally blocks, no rollback from Python
        victim.wait(timeout=10)
        victim.stdout.close()
        victim.stderr.close()

    # Postgres sees the dropped connection and aborts the relay's batch.
    deadline = time.monotonic() + 10
    while _unlocked_unpublished() < 50 and time.monotonic() < deadline:
        time.sleep(0.05)
    assert _unpublished() == 50, "a killed relay must not have marked anything"

    restarted = RecordingProducer()
    result = relay.run_once(restarted)

    assert result.published == 50
    assert _unpublished() == 0  # I3
    assert restarted.event_ids == event_ids  # every row, in id order


def test_crash_after_broker_acks_republishes_rather_than_loses(make_outbox_rows):
    """The window the relay cannot close: the broker has 20 messages whose
    rows are not marked. They go out again, and the Phase 5 consumer
    deduplicates them. Losing them would be the bug."""
    event_ids = make_outbox_rows(50)
    crashing = CrashingProducer(crash_after=20)

    with pytest.raises(CrashingProducer.Crash):
        relay.run_once(crashing)
    assert _unpublished() == 50

    restarted = RecordingProducer()
    relay.run_once(restarted)

    assert _unpublished() == 0  # I3
    assert restarted.event_ids == event_ids
    assert crashing.event_ids == event_ids[:20]  # published twice, by design


def test_broker_refusal_stops_the_batch_and_keeps_order(make_outbox_rows):
    event_ids = make_outbox_rows(50)
    producer = RecordingProducer()
    producer.fail_on_nth = 10
    producer.fail_once = True

    first = relay.run_once(producer)

    assert first.published == 9
    assert first.failed_event_id == event_ids[9]
    assert _unpublished() == 41

    second = relay.run_once(producer)

    assert second.published == 41
    assert _unpublished() == 0  # I3
    assert producer.event_ids == event_ids  # nothing skipped, nothing reordered


def test_message_key_is_the_partition_key_and_headers_carry_the_event(
    make_outbox_rows,
):
    traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    event_ids = make_outbox_rows(3, partition_key="acc_debited", traceparent=traceparent)
    producer = RecordingProducer()

    relay.run_once(producer)

    assert producer.keys == ["acc_debited"] * 3
    for message, event_id in zip(producer.messages, event_ids):
        assert message["topic"] == MAIN_TOPIC
        assert message["headers"][HEADER_EVENT_ID] == event_id
        # Same trace, re-parented: the header is the relay's own publish span,
        # a child of the stored context (decision D11).
        _, trace, span, _ = message["headers"][HEADER_TRACEPARENT].split("-")
        assert trace == traceparent.split("-")[1]
        assert span != traceparent.split("-")[2]
