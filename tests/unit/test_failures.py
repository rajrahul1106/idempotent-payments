"""Decision D5 classification, the retry ladder, the failure-history
headers and the dead-letter row, before any I/O."""

from __future__ import annotations

import base64
import datetime as dt
import json

import psycopg
import pytest
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeout

from app.messaging.dlq_writer import dead_letter_from
from app.messaging.failures import (
    EventError,
    FailureKind,
    attempt_count,
    classify,
    failure_headers,
)
from app.messaging.topics import TopicLadder
from app.services.dlq import (
    NIL_EVENT_ID,
    RAW_BODY_KEY,
    body_from_payload_json,
    dedup_id_for,
    payload_json_for,
    replay_message,
)

EVENT_ID = "c803182a-e978-40b7-84db-32f003b7f0c1"
LADDER = TopicLadder(
    main="m", retries=("m.r5s", "m.r1m", "m.r10m"), retry_delays=(5, 60, 600), dlq="m.dlq"
)


def _Sqlstate(sqlstate: str) -> psycopg.Error:
    """The driver's own exception class for this SQLSTATE."""
    return psycopg.errors.lookup(sqlstate)("injected")


def _operational(orig) -> OperationalError:
    return OperationalError("SELECT 1", None, orig)


# --- classification --------------------------------------------------------------


@pytest.mark.parametrize(
    "exc, kind",
    [
        (EventError("bad schema_version"), FailureKind.PERMANENT),
        (_operational(psycopg.OperationalError("connection refused")), FailureKind.INFRASTRUCTURE),
        (_operational(_Sqlstate("08006")), FailureKind.INFRASTRUCTURE),
        (_operational(_Sqlstate("57P01")), FailureKind.INFRASTRUCTURE),
        (PoolTimeout("pool exhausted"), FailureKind.INFRASTRUCTURE),
        (ConnectionRefusedError("refused"), FailureKind.INFRASTRUCTURE),
        # The database answered: a timeout or a constraint is not an outage.
        (_operational(_Sqlstate("55P03")), FailureKind.TRANSIENT),  # lock_timeout
        (_operational(_Sqlstate("57014")), FailureKind.TRANSIENT),  # statement_timeout
        (IntegrityError("INSERT", None, Exception("dup")), FailureKind.TRANSIENT),
        (RuntimeError("downstream 502"), FailureKind.TRANSIENT),
    ],
)
def test_classify(exc, kind):
    assert classify(exc) is kind


# --- the ladder ---------------------------------------------------------------------


def test_ladder_walks_every_tier_then_the_dlq():
    assert [LADDER.next_topic(n) for n in range(5)] == [
        "m.r5s", "m.r1m", "m.r10m", "m.dlq", "m.dlq",
    ]
    assert LADDER.delay_of("m.r1m") == 60
    with pytest.raises(ValueError):
        LADDER.next_topic(-1)


def test_default_ladder_matches_settings():
    from app.core.config import settings
    from app.messaging import topics

    assert topics.DEFAULT_LADDER.retries == settings.retry_topics
    assert topics.DEFAULT_LADDER.retry_delays == (5.0, 60.0, 600.0)
    assert topics.DEFAULT_LADDER.next_topic(0) == "payments.retry.5s"
    assert topics.DEFAULT_LADDER.next_topic(3) == topics.DLQ_TOPIC


# --- failure history headers ------------------------------------------------------------


def test_first_failure_sets_the_history_and_keeps_every_original_header():
    now = dt.datetime(2026, 11, 11, 9, 30, tzinfo=dt.timezone.utc)
    original = {"event_id": EVENT_ID, "traceparent": "00-abc-def-01", "schema_version": "1"}
    out = failure_headers(
        original, kind=FailureKind.TRANSIENT, exc=RuntimeError("boom\nline two"),
        source_topic="m", destination="m.r5s", now=now,
    )
    assert out == {
        **original,
        "attempt_count": "1",
        "last_error": "transient: RuntimeError: boom line two",
        "first_failed_at": "2026-11-11T09:30:00+00:00",
        "original_topic": "m",
        "failure_history": json.dumps(
            [{"attempt": 1, "topic": "m.r5s", "error": "transient: RuntimeError: boom line two",
              "at": "2026-11-11T09:30:00+00:00"}],
            separators=(",", ":"),
        ),
    }


def test_later_failures_keep_first_failed_at_and_original_topic():
    earlier = {
        "event_id": EVENT_ID,
        "attempt_count": "2",
        "first_failed_at": "2026-11-11T09:30:00+00:00",
        "original_topic": "m",
        "last_error": "transient: X: old",
    }
    out = failure_headers(
        earlier, kind=FailureKind.TRANSIENT, exc=RuntimeError("new"), source_topic="m.other",
        destination="m.r10m",
    )
    assert out["attempt_count"] == "3"
    assert out["first_failed_at"] == "2026-11-11T09:30:00+00:00"
    assert out["original_topic"] == "m"
    assert out["last_error"] == "transient: RuntimeError: new"


def test_last_error_is_bounded():
    out = failure_headers(
        {}, kind=FailureKind.PERMANENT, exc=EventError("x" * 5000), source_topic="m",
        destination="m.dlq",
    )
    assert len(out["last_error"]) == 1000
    [entry] = json.loads(out["failure_history"])
    assert len(entry["error"]) == 500


def test_each_forward_appends_one_history_entry():
    headers: dict[str, str] = {"event_id": EVENT_ID}
    for attempt, topic in enumerate(["m.r5s", "m.r1m", "m.r10m", "m.dlq"], start=1):
        headers = failure_headers(
            headers, kind=FailureKind.TRANSIENT, exc=RuntimeError(f"fail {attempt}"),
            source_topic="m", destination=topic,
        )
    history = json.loads(headers["failure_history"])
    assert [(e["attempt"], e["topic"], e["error"]) for e in history] == [
        (1, "m.r5s", "transient: RuntimeError: fail 1"),
        (2, "m.r1m", "transient: RuntimeError: fail 2"),
        (3, "m.r10m", "transient: RuntimeError: fail 3"),
        (4, "m.dlq", "transient: RuntimeError: fail 4"),
    ]
    assert len(headers["failure_history"]) < 1000  # a small header


@pytest.mark.parametrize("raw", ["not json", '{"a": 1}', "[1, 2]"])
def test_garbled_history_header_starts_a_fresh_history(raw):
    out = failure_headers(
        {"failure_history": raw, "attempt_count": "2"}, kind=FailureKind.TRANSIENT,
        exc=RuntimeError("x"), source_topic="m", destination="m.r10m",
    )
    assert [e["attempt"] for e in json.loads(out["failure_history"])] == [3]


@pytest.mark.parametrize("value, expected", [(None, 0), ("0", 0), ("3", 3), ("-2", 0), ("x", 0)])
def test_attempt_count_reads_garbage_as_zero(value, expected):
    headers = {} if value is None else {"attempt_count": value}
    assert attempt_count(headers) == expected


# --- dead letters -------------------------------------------------------------------------


def _letter(headers: dict[str, str], value: bytes | None, offset: int = 7):
    return dead_letter_from(
        [(k, v.encode()) for k, v in headers.items()], value,
        topic="m.dlq", partition=2, offset=offset, timestamp_ms=1_762_853_400_000,
    )


def test_dead_letter_carries_the_failure_history():
    body = json.dumps({"event_id": EVENT_ID, "amount_minor": 150000}).encode()
    letter = _letter(
        {
            "event_id": EVENT_ID,
            "attempt_count": "4",
            "first_failed_at": "2026-11-11T09:30:00+00:00",
            "original_topic": "m",
            "last_error": "transient: RuntimeError: boom",
        },
        body,
    )
    assert letter.event_id == EVENT_ID
    assert letter.attempts == 4
    assert letter.reason == "transient: RuntimeError: boom"
    assert letter.original_topic == "m"
    assert letter.first_failed_at == dt.datetime(2026, 11, 11, 9, 30, tzinfo=dt.timezone.utc)
    assert letter.payload_json == body.decode()
    assert letter.traceparent is None and letter.failure_history == []


def test_dead_letter_keeps_traceparent_and_history():
    history = [{"attempt": 1, "topic": "m.dlq", "error": "permanent: E: x", "at": "t"}]
    letter = _letter(
        {"event_id": EVENT_ID, "traceparent": "00-" + "a" * 32 + "-" + "b" * 16 + "-01",
         "failure_history": json.dumps(history)},
        b"{}",
    )
    assert letter.traceparent == "00-" + "a" * 32 + "-" + "b" * 16 + "-01"
    assert letter.failure_history == history


def test_dead_letter_without_headers_is_still_recorded():
    letter = _letter({}, b"\xff\xfe not json")
    assert letter.event_id == NIL_EVENT_ID
    assert letter.attempts == 0
    assert letter.reason.startswith("unknown")
    assert letter.original_topic == "unknown"
    assert letter.first_failed_at.year == 2025  # from the message timestamp
    assert body_from_payload_json(letter.payload_json) == b"\xff\xfe not json"


def test_dead_letter_falls_back_to_the_payload_event_id():
    letter = _letter({"event_id": "not-a-uuid"}, json.dumps({"event_id": EVENT_ID}).encode())
    assert letter.event_id == EVENT_ID


@pytest.mark.parametrize(
    "body",
    [b'{"amount_minor": 1000.5}', b"{truncated", b"", b'{"a": "\\u0000"}', b"\x00\x01"],
)
def test_payload_round_trips_exactly(body):
    stored = payload_json_for(body)
    json.loads(stored)  # always valid JSON, so the jsonb cast accepts it
    assert body_from_payload_json(stored) == body


def test_float_payload_is_stored_verbatim_not_wrapped():
    assert payload_json_for(b'{"amount_minor": 1000.5}') == '{"amount_minor": 1000.5}'
    assert RAW_BODY_KEY in payload_json_for(b"{truncated")
    assert base64.b64decode(json.loads(payload_json_for(b"{x"))[RAW_BODY_KEY]) == b"{x"


def test_dedup_id_collapses_copies_but_not_replays():
    first = dedup_id_for(EVENT_ID, None, "m.dlq|0|1")
    assert dedup_id_for(EVENT_ID, None, "m.dlq|3|9") == first  # a duplicate copy
    assert dedup_id_for(EVENT_ID, "42", "m.dlq|0|1") != first  # failed again after replay 42
    assert dedup_id_for(EVENT_ID, "42", "x") == dedup_id_for(EVENT_ID, "42", "y")
    # No real event_id: keyed by the message's own position.
    assert dedup_id_for(NIL_EVENT_ID, None, "a") != dedup_id_for(NIL_EVENT_ID, None, "b")


def test_replay_message_resets_attempts_and_keeps_the_event_id():
    from app.db.repositories.dlq import DlqRow

    now = dt.datetime.now(dt.timezone.utc)
    payload = {
        "event_id": EVENT_ID, "event_type": "payment.posted", "schema_version": 1,
        "from_account_id": "acc_alice", "amount_minor": 150000,
    }
    row = DlqRow(
        id=42, event_id=EVENT_ID, original_topic="m", payload_json=json.dumps(payload),
        reason="transient: X: y", attempts=4, first_failed_at=now, created_at=now,
        replayed_at=None,
    )
    key, value, headers = replay_message(row)
    assert key == "acc_alice"
    assert json.loads(value) == payload
    assert headers == {
        "event_id": EVENT_ID,
        "attempt_count": "0",
        "replay_of": "42",
        "event_type": "payment.posted",
        "schema_version": "1",
    }
