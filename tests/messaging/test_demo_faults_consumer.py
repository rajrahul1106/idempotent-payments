"""Decision D27 against a real broker: the demo fault switch in the consumer,
and the delivery_failures record (migration 0005).

Each event's demo_fault is read from its transaction row, so every test
writes a transactions row with the metadata it needs and points the event
at it. The event format itself is unchanged.
"""

from __future__ import annotations

import json
import uuid

import pytest
from sqlalchemy import text

from app.core.config import settings
from app.db.session import read_session, unit_of_work
from app.messaging import demo_faults
from app.messaging.consumer import build_consumer
from tests.messaging.conftest import (
    FAST_GROUP,
    RunningLoop,
    dlq_rows,
    message_count,
    payment_event,
    processed_count,
    publish,
    wait_for,
)

pytestmark = pytest.mark.messaging


def _event_with_fault(fault: str | None):
    """A payment.posted event whose transaction row carries demo_fault."""
    event = payment_event("acc_demo_src", "acc_demo_dst", 1_000)
    metadata = {"demo_fault": fault} if fault else {}
    with unit_of_work() as session:
        session.execute(
            text("INSERT INTO transactions (id, amount, currency, metadata) VALUES (:i, 10, 'INR', CAST(:m AS jsonb))"),
            {"i": event.payload["transaction_id"], "m": json.dumps(metadata)},
        )
    return event


def _failures(event_id: str) -> list[tuple]:
    with read_session() as session:
        return [
            tuple(row)
            for row in session.execute(
                text("SELECT attempt, kind, sent_to FROM delivery_failures WHERE event_id = :e ORDER BY id"),
                {"e": event_id},
            )
        ]


def _applied(event_id: str, group: str) -> bool:
    with read_session() as session:
        return bool(
            session.execute(
                text("SELECT count(*) FROM processed_events WHERE event_id = :e AND consumer_group = :g"),
                {"e": event_id, "g": group},
            ).scalar_one()
        )


# --- switch off ---------------------------------------------------------------------


def test_build_consumer_installs_the_check_only_when_the_switch_is_on(monkeypatch, make_ladder, group_id):
    ladder = make_ladder()
    monkeypatch.setattr(settings, "demo_faults_enabled", False)
    off = build_consumer(ladder=ladder, group_id=group_id)
    monkeypatch.setattr(settings, "demo_faults_enabled", True)
    on = build_consumer(ladder=ladder, group_id=group_id)
    try:
        assert off._demo_fault is None  # noqa: SLF001
        assert on._demo_fault is demo_faults.check  # noqa: SLF001
    finally:
        off._producer.close()  # noqa: SLF001 - built, never run
        on._producer.close()  # noqa: SLF001


def test_switch_off_the_consumer_ignores_demo_fault(monkeypatch, kafka, make_ladder, group_id):
    monkeypatch.setattr(settings, "demo_faults_enabled", False)
    ladder = make_ladder()
    event = _event_with_fault("fail_permanent")
    publish(kafka, ladder.main, [event])

    running = RunningLoop(build_consumer(ladder=ladder, group_id=group_id, config_overrides=FAST_GROUP))
    try:
        assert wait_for(lambda: _applied(event.event_id, group_id), timeout=60)
    finally:
        running.close()
    assert running.stats.applied == 1 and running.stats.dead_lettered == 0 and running.stats.retried == 0
    assert _failures(event.event_id) == []
    assert message_count(kafka, ladder.dlq) == 0


# --- switch on ----------------------------------------------------------------------


def test_fail_once_completes_after_one_retry_with_one_recorded_failure(kafka, make_ladder, group_id, pipeline):
    ladder = make_ladder(delays=(1.0, 1.0, 1.0))
    event = _event_with_fault("fail_once")
    publish(kafka, ladder.main, [event])

    running = pipeline(ladder, group_id, demo_fault=demo_faults.check)
    assert wait_for(lambda: _applied(event.event_id, group_id), timeout=90)
    assert wait_for(lambda: len(_failures(event.event_id)) == 1, timeout=10)
    running.close()

    assert _failures(event.event_id) == [(1, "transient", ladder.retries[0])]
    assert running.main.stats.retried == 1 and running.main.stats.applied == 1
    assert dlq_rows([event.event_id]) == []
    assert processed_count(group_id) == 1


def test_fail_twice_needs_two_retries(kafka, make_ladder, group_id, pipeline):
    ladder = make_ladder(delays=(1.0, 1.0, 1.0))
    event = _event_with_fault("fail_twice")
    publish(kafka, ladder.main, [event])

    running = pipeline(ladder, group_id, demo_fault=demo_faults.check)
    assert wait_for(lambda: _applied(event.event_id, group_id), timeout=90)
    assert wait_for(lambda: len(_failures(event.event_id)) == 2, timeout=10)
    running.close()
    assert _failures(event.event_id) == [
        (1, "transient", ladder.retries[0]),
        (2, "transient", ladder.retries[1]),
    ]


def test_fail_permanent_lands_in_the_dlq_with_its_attempt_recorded(kafka, make_ladder, group_id, pipeline):
    ladder = make_ladder(delays=(1.0, 1.0, 1.0))
    event = _event_with_fault("fail_permanent")
    publish(kafka, ladder.main, [event])

    running = pipeline(ladder, group_id, demo_fault=demo_faults.check)
    assert wait_for(lambda: len(dlq_rows([event.event_id])) == 1, timeout=90)
    running.close()

    assert _failures(event.event_id) == [(1, "permanent", ladder.dlq)]
    row = dlq_rows([event.event_id])[0]
    assert row["reason"].startswith("permanent: DemoPermanentFault")
    assert row["attempts"] == 1 and row["replayed_at"] is None
    assert not _applied(event.event_id, group_id)
    assert message_count(kafka, ladder.retries[0]) == 0


def test_a_failing_record_does_not_stop_the_forward(kafka, make_ladder, group_id, pipeline):
    """The delivery_failures write is best-effort: when it raises, the message
    is still forwarded, its offset still committed, and it still completes."""
    calls = []

    def broken_record(**kwargs):
        calls.append(kwargs)
        raise RuntimeError("delivery_failures is unwritable (injected)")

    ladder = make_ladder(delays=(1.0, 1.0, 1.0))
    event = _event_with_fault("fail_once")
    publish(kafka, ladder.main, [event])

    running = pipeline(ladder, group_id, demo_fault=demo_faults.check, record_failure=broken_record)
    assert wait_for(lambda: _applied(event.event_id, group_id), timeout=90)
    running.close()

    assert len(calls) == 1 and calls[0]["sent_to"] == ladder.retries[0]
    assert message_count(kafka, ladder.retries[0]) == 1
    assert running.main.stats.retried == 1 and running.main.stats.applied == 1
    assert _failures(event.event_id) == []  # the record was lost, the payment was not


def test_record_failure_itself_never_raises(monkeypatch, database):
    from app.services import delivery_failures

    def broken_unit_of_work():
        raise RuntimeError("no database (injected)")

    monkeypatch.setattr(delivery_failures, "unit_of_work", broken_unit_of_work)
    assert delivery_failures.record(
        event_id=str(uuid.uuid4()), attempt=1, kind="transient", error="x", sent_to="t"
    ) is False
