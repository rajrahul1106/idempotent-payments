"""Phase 7a gate: one payment is one trace across every process it touches.

Real Redpanda, real Postgres. The gate runs the relay and the consumer as
separate processes, each with its own service name, writing spans to a
file; the API runs in this process with spans in memory. All three must
land in one trace, linked parent to child.

The retry and DLQ tests run the pipeline in this process (every span here
is service "api"); what they check is the trace id and the parent chain.
"""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys
import threading
import time
import uuid

import pytest
from sqlalchemy import text

from app.core.telemetry import current_traceparent, trace_id_of, tracer
from app.db.session import read_session
from tests.messaging.conftest import (
    FAST_GROUP,
    as_message,
    dlq_rows,
    payment_event,
    processed_count,
    publish_raw,
    wait_for,
)
from tests.tracing import (
    in_trace,
    named,
    parent_id,
    read_spans,
    service,
    span_id,
    trace_id,
)

pytestmark = pytest.mark.messaging

ROOT = pathlib.Path(__file__).resolve().parents[2]

_RELAY_PROCESS = """
import sys
from tests.tracing import file_exporter
from app.core.telemetry import setup_tracing, shutdown_tracing
setup_tracing("relay", exporter=file_exporter(sys.argv[2]), synchronous=True)

from app.db.session import unit_of_work
from app.messaging import relay
from app.messaging.producer import KafkaProducer

producer = KafkaProducer(sys.argv[3])
with unit_of_work() as session:
    result = relay.publish_batch(session, producer, topic=sys.argv[1])
producer.close()
shutdown_tracing()
print(",".join(result.event_ids), flush=True)
"""

_CONSUMER_PROCESS = f"""
import json, sys
from tests.tracing import file_exporter
from app.core.telemetry import setup_tracing, shutdown_tracing
setup_tracing("consumer", exporter=file_exporter(sys.argv[3]), synchronous=True)

from app.messaging.consumer import PaymentEventConsumer
from app.messaging.topics import TopicLadder

ladder = TopicLadder(**{{k: tuple(v) if isinstance(v, list) else v
                         for k, v in json.loads(sys.argv[1]).items()}})
consumer = PaymentEventConsumer(ladder=ladder, group_id=sys.argv[2],
                                config_overrides={FAST_GROUP!r})
consumer.run(max_messages=1)
shutdown_tracing()
print(consumer.stats.applied, flush=True)
"""


def _run(script: str, *args: str) -> str:
    result = subprocess.run(
        [sys.executable, "-c", script, *args],
        cwd=ROOT, capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stderr[-3000:]
    return result.stdout.strip().splitlines()[-1]


# --- 1. GATE ----------------------------------------------------------------------------------


def test_gate_one_payment_is_one_trace_across_api_relay_and_consumer(
    kafka, fresh_ladder, group_id, client, owner_token, make_account, clear_outbox, spans, tmp_path
):
    source, destination = make_account(500_000), make_account(0)
    key = str(uuid.uuid4())
    token = owner_token(source)
    response = client.post(
        "/v1/payments",
        json={"from_account_id": source, "to_account_id": destination,
              "amount_minor": 25_050, "currency": "INR"},
        headers={"Authorization": f"Bearer {token}", "Idempotency-Key": key},
    )
    assert response.status_code == 201, response.text
    event_id = response.json()["event_id"]

    # Two more processes, each its own service, each writing its spans.
    relay_file, consumer_file = tmp_path / "relay.jsonl", tmp_path / "consumer.jsonl"
    published = _run(_RELAY_PROCESS, fresh_ladder.main, str(relay_file), kafka)
    assert published == event_id
    applied = _run(_CONSUMER_PROCESS, json.dumps(fresh_ladder.__dict__), group_id,
                   str(consumer_file))
    assert applied == "1" and processed_count(group_id) == 1

    api_spans = spans.dicts()
    [server] = [s for s in api_spans if s["kind"] == "SpanKind.SERVER"
                and s["attributes"].get("http.route") == "/v1/payments"]
    the_trace = trace_id(server)
    every = api_spans + read_spans(relay_file) + read_spans(consumer_file)
    trace = in_trace(every, the_trace)

    # One trace, three processes.
    assert {service(s) for s in trace} == {"api", "relay", "consumer"}
    # Everything the relay and consumer did for this event is in it: no
    # span of theirs started a trace of its own.
    for path in (relay_file, consumer_file):
        mine = [s for s in read_spans(path) if s["attributes"].get("event_id") == event_id]
        assert mine and all(trace_id(s) == the_trace for s in mine)

    # Linked parent to child across both process boundaries.
    [publish] = named(trace, "outbox publish")
    [process] = named(trace, "payment.posted process")
    assert (service(publish), service(process)) == ("relay", "consumer")
    assert parent_id(publish) == span_id(server)
    assert parent_id(process) == span_id(publish)
    assert process["attributes"]["outcome"] == "applied"
    for span in (publish, process):
        assert span["attributes"]["event_id"] == event_id
        assert span["attributes"]["account_id"] == source
    assert process["attributes"]["attempt_count"] == 0

    # The consumer's database work is inside its span.
    queries = [s for s in trace if service(s) == "consumer" and s["kind"] == "SpanKind.CLIENT"]
    assert queries and all(parent_id(q) == span_id(process) for q in queries)

    # Nothing secret anywhere in the trace.
    dumped = json.dumps(trace)
    assert key not in dumped and token not in dumped


# --- 2. through both retry tiers --------------------------------------------------------------


def _traced_message(event):
    """The message as the relay would send it, under a fresh root span."""
    with tracer("tests").start_as_current_span("test root"):
        traceparent = current_traceparent()
    key, value, headers = as_message(event)
    headers["traceparent"] = traceparent
    return (key, value, headers), trace_id_of(traceparent)


def test_fail_twice_then_succeed_is_one_trace_through_both_tiers(
    kafka, make_ladder, group_id, pipeline, spans
):
    ladder = make_ladder(delays=(0.5, 0.5, 0.5))
    event = payment_event("acc_traced_retry", "acc_dst", 4_200)
    message, the_trace = _traced_message(event)

    def side_effect(evt, attempt: int) -> None:
        if attempt < 3:
            raise RuntimeError(f"attempt {attempt} fails")

    publish_raw(kafka, ladder.main, [message])
    running = pipeline(ladder, group_id, side_effect=side_effect)
    assert wait_for(lambda: processed_count(group_id) == 1, timeout=60)
    running.close()

    trace = in_trace(spans.dicts(), the_trace)
    attempts = named(trace, "payment.posted process")
    forwards = named(trace, "retry forward")
    assert [s["attributes"]["attempt_count"] for s in attempts] == [0, 1, 2]
    assert [s["attributes"]["outcome"] for s in attempts] == ["retried", "retried", "applied"]
    assert [s["attributes"]["retry.topic"] for s in forwards] == list(ladder.retries[:2])
    # attempt 1 -> tier 0 -> attempt 2 -> tier 1 -> attempt 3, each a child of the last.
    chain = [attempts[0], forwards[0], attempts[1], forwards[1], attempts[2]]
    for parent, child in zip(chain, chain[1:]):
        assert parent_id(child) == span_id(parent), (parent["name"], child["name"])
    assert attempts[0]["status"]["status_code"] == "ERROR"
    assert named(trace, "dlq write") == []


# --- 3. DLQ: stored trace and history, and a replay that rejoins the trace ----------------------


def test_dead_letter_keeps_trace_and_history_and_a_replay_joins_the_trace(
    kafka, make_ladder, group_id, pipeline, client, operator_token, replay_to, spans
):
    ladder = make_ladder(delays=(0.3, 0.3, 0.3))
    replay_to(kafka, ladder.main)
    event = payment_event("acc_traced_dlq", "acc_dst", 9_900)
    message, the_trace = _traced_message(event)
    broken = threading.Event()
    broken.set()
    long_error = "downstream said: " + "x" * 2_000

    def side_effect(evt, attempt: int) -> None:
        if broken.is_set():
            raise RuntimeError(f"attempt {attempt}: {long_error}")

    publish_raw(kafka, ladder.main, [message])
    running = pipeline(ladder, group_id, side_effect=side_effect)
    assert wait_for(lambda: len(dlq_rows([event.event_id])) == 1, timeout=60)

    # Stored with its trace context and every attempt.
    with read_session() as session:
        row = session.execute(
            text("SELECT id, traceparent, failure_history FROM dlq_messages WHERE event_id = :e"),
            {"e": event.event_id},
        ).one()
    assert trace_id_of(row.traceparent) == the_trace
    history = row.failure_history
    assert [e["attempt"] for e in history] == [1, 2, 3, 4]
    assert [e["topic"] for e in history] == [*ladder.retries, ladder.dlq]
    assert all(e["error"].startswith(f"transient: RuntimeError: attempt {e['attempt']}: ")
               for e in history)
    assert all(len(e["error"]) == 500 for e in history)  # truncated, header stays small
    assert [e["at"] for e in history] == sorted(e["at"] for e in history)
    [last_attempt] = [s for s in named(in_trace(spans.dicts(), the_trace),
                                       "payment.posted process")
                      if s["attributes"]["attempt_count"] == 3]
    assert row.traceparent.split("-")[2] == span_id(last_attempt)  # the attempt that sent it

    # The API returns both.
    listed = client.get("/v1/dlq", params={"reason": "transient: RuntimeError: attempt 4"},
                        headers={"Authorization": f"Bearer {operator_token}"})
    [item] = [i for i in listed.json()["items"] if i["event_id"] == event.event_id]
    assert item["traceparent"] == row.traceparent and item["failure_history"] == history

    # A replay continues the same trace.
    broken.clear()
    replayed = client.post(f"/v1/dlq/{row.id}/replay",
                           headers={"Authorization": f"Bearer {operator_token}"})
    assert replayed.status_code == 200, replayed.text
    assert wait_for(lambda: processed_count(group_id) == 1, timeout=30)
    time.sleep(0.5)
    running.close()

    every = spans.dicts()
    trace = in_trace(every, the_trace)
    [replay] = named(trace, "dlq replay")
    assert parent_id(replay) == span_id(last_attempt)
    assert replay["attributes"]["dlq.message_id"] == row.id
    [applied] = [s for s in named(trace, "payment.posted process")
                 if s["attributes"].get("outcome") == "applied"]
    assert parent_id(applied) == span_id(replay)
    assert applied["attributes"]["attempt_count"] == 0
    assert len(named(trace, "dlq write")) == 1
    # The operator's HTTP request is its own trace, linked from the replay.
    [request] = [s for s in every if s["kind"] == "SpanKind.SERVER"
                 and s["attributes"].get("http.route") == "/v1/dlq/{message_id}/replay"]
    assert trace_id(request) != the_trace
    assert [link["context"]["span_id"].removeprefix("0x") for link in replay["links"]] == [
        span_id(request)
    ]
