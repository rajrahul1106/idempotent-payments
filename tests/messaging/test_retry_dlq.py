"""Phase 6 gate: a failing message never blocks the messages behind it, and
a message that can never succeed ends up in the DLQ with its history,
replayable by an operator.

Real Redpanda, real Postgres. Each test has its own ladder of topics (main,
three retry tiers, DLQ, all 6 partitions) and its own consumer groups. Tier
delays are overridden to seconds. Failures are injected only through the
consumer's side_effect constructor argument.
"""

from __future__ import annotations

import dataclasses
import json
import pathlib
import random
import signal
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest
from sqlalchemy.exc import OperationalError

from tests.messaging.conftest import (
    FAST_GROUP,
    RunningLoop,
    as_message,
    dlq_rows,
    message_count,
    payment_event,
    processed_count,
    projected_event_ids,
    publish_raw,
    wait_for,
)

pytestmark = pytest.mark.messaging

ROOT = pathlib.Path(__file__).resolve().parents[2]


class InjectedFailure(RuntimeError):
    """What a flaky downstream system raises. Classified TRANSIENT."""


def _injected_outage() -> OperationalError:
    """What SQLAlchemy raises when Postgres refuses the connection."""
    return OperationalError(
        "SELECT 1", None, psycopg.OperationalError("connection refused (injected)")
    )


# --- poison messages ---------------------------------------------------------------


def _permanent(kind: str, account: str) -> tuple[str, tuple[str, bytes, dict[str, str]]]:
    """A message no retry can fix, and the event_id the DLQ will record."""
    event = payment_event(account, "acc_dst", 1_000)
    key, value, headers = as_message(event)
    if kind == "unparseable":
        value = b'{"event_id": "' + event.event_id.encode() + b'", truncated'
    elif kind == "schema_version":
        payload = dict(event.payload, schema_version=2)
        value = json.dumps(payload).encode()
        headers["schema_version"] = "2"
    elif kind == "event_id_mismatch":
        payload = dict(event.payload, event_id=str(uuid.uuid4()))
        value = json.dumps(payload).encode()
    elif kind == "float_amount":
        value = value.replace(b'"amount_minor":1000', b'"amount_minor":1000.5')
        assert b"1000.5" in value
    else:  # pragma: no cover
        raise ValueError(kind)
    return event.event_id, (key, value, headers)


PERMANENT_KINDS = ("unparseable", "schema_version", "event_id_mismatch", "float_amount")


# --- 1. GATE: 50 poison messages in 5,000 good ones ---------------------------------


def test_gate_50_poison_messages_in_5000_good_ones(kafka, make_ladder, group_id, pipeline):
    ladder = make_ladder(delays=(2.0, 2.0, 2.0))
    accounts = [f"acc_gate_{i}" for i in range(50)]
    rng = random.Random(6)

    good = [payment_event(accounts[i % 50], "acc_dst", 1_000 + i) for i in range(5_000)]
    messages = [(e.event_id, e.partition_key, as_message(e)) for e in good]

    # 25 that fail on every attempt (TRANSIENT, through every tier) and 25
    # that are malformed (PERMANENT, straight to the DLQ). Each lands in the
    # first 80% of the stream, so good messages for its account follow it.
    transient = [payment_event(rng.choice(accounts), "acc_dst", 7_777) for _ in range(25)]
    permanent = [
        _permanent(PERMANENT_KINDS[i % len(PERMANENT_KINDS)], rng.choice(accounts))
        for i in range(25)
    ]
    poison = [(e.event_id, e.partition_key, as_message(e)) for e in transient] + [
        (event_id, message[0], message) for event_id, message in permanent
    ]
    for item in poison:
        messages.insert(rng.randint(0, int(len(messages) * 0.8)), item)
    assert len(messages) == 5_050

    transient_ids = {e.event_id for e in transient}
    permanent_ids = {event_id for event_id, _ in permanent}
    lock = threading.Lock()
    good_done: dict[str, float] = {}
    poison_attempts: dict[str, list[tuple[int, float]]] = {e: [] for e in transient_ids}

    def side_effect(event, attempt: int) -> None:
        now = time.monotonic()
        with lock:
            if event.event_id in transient_ids:
                poison_attempts[event.event_id].append((attempt, now))
                raise InjectedFailure(f"downstream rejected {event.event_id}")
            good_done.setdefault(event.event_id, now)

    publish_raw(kafka, ladder.main, [message for _, _, message in messages])
    running = pipeline(ladder, group_id, side_effect=side_effect)

    poison_ids = transient_ids | permanent_ids
    assert wait_for(
        lambda: processed_count(group_id) == 5_000 and len(dlq_rows(poison_ids)) == 50,
        timeout=300,
    ), (processed_count(group_id), len(dlq_rows(poison_ids)), running.main.stats)
    time.sleep(1.0)  # let any stray copy surface before counting
    running.close()

    # Every good message: exactly one effect.
    good_ids = [e.event_id for e in good]
    projected = projected_event_ids(good_ids)
    assert len(projected) == 5_000 and set(projected) == set(good_ids)
    assert processed_count(group_id) == 5_000
    assert running.main.stats.applied == 5_000

    # Every poison message: exactly one DLQ row, with its failure history.
    rows = {row["event_id"]: row for row in dlq_rows(poison_ids)}
    assert len(dlq_rows(poison_ids)) == 50 and set(rows) == poison_ids
    for event_id in transient_ids:
        row = rows[event_id]
        assert row["attempts"] == 4, row  # first delivery plus one per tier
        assert row["reason"].startswith("transient: InjectedFailure: downstream rejected"), row
        assert row["original_topic"] == ladder.main
        assert row["first_failed_at"] <= row["created_at"]
        assert [a for a, _ in poison_attempts[event_id]] == [1, 2, 3, 4]
    for event_id in permanent_ids:
        row = rows[event_id]
        assert row["attempts"] == 1, row  # never retried
        assert row["reason"].startswith("permanent: EventError: "), row
        assert row["original_topic"] == ladder.main
    assert running.main.stats.dead_lettered == 50
    assert running.main.stats.retried == 25 * 3
    assert [tier.stats.forwarded for tier in running.tiers] == [25, 25, 25]
    assert running.writer.stats.written == 50

    # Good messages behind a poison message did not wait for any tier delay:
    # the next good message for the poisoned account finished within half
    # the first tier's delay of the poison's first attempt. A consumer that
    # waited out the delay in place could not. (Measured: 1 to 16 ms.)
    order = [(event_id, account) for event_id, account, _ in messages]
    checked = 0
    for index, (event_id, account) in enumerate(order):
        if event_id not in transient_ids:
            continue
        follower = next(
            other for other, acct in order[index + 1 :] if acct == account and other in good_done
        )
        (_, first), (_, second) = poison_attempts[event_id][:2]
        gap = good_done[follower] - first
        assert 0 < gap < ladder.retry_delays[0] / 2, (event_id, follower, gap)
        assert second - first >= ladder.retry_delays[0]  # the tier did hold the poison
        checked += 1
    assert checked == 25


# --- 2. transient, succeeds on attempt 3 --------------------------------------------


def test_transient_failure_that_succeeds_on_attempt_3(kafka, make_ladder, group_id, pipeline):
    ladder = make_ladder(delays=(1.0, 1.0, 1.0))
    event = payment_event("acc_flaky", "acc_dst", 4_200)
    attempts: list[int] = []

    def side_effect(evt, attempt: int) -> None:
        attempts.append(attempt)
        if attempt < 3:
            raise InjectedFailure(f"attempt {attempt} times out")

    publish_raw(kafka, ladder.main, [as_message(event)])
    running = pipeline(ladder, group_id, side_effect=side_effect)
    assert wait_for(lambda: processed_count(group_id) == 1, timeout=60)
    time.sleep(2.5)  # longer than a tier delay: nothing else may arrive
    running.close()

    assert attempts == [1, 2, 3]
    tier_ids = [tier.stats.forwarded_event_ids for tier in running.tiers]
    assert tier_ids == [[event.event_id], [event.event_id], []]  # the first two tiers only
    assert projected_event_ids([event.event_id]) == [event.event_id]
    assert dlq_rows([event.event_id]) == []
    assert message_count(kafka, ladder.dlq) == 0
    assert running.main.stats.retried == 2 and running.main.stats.dead_lettered == 0


# --- 3. permanent, straight to the DLQ ---------------------------------------------


@pytest.mark.parametrize("kind", PERMANENT_KINDS)
def test_permanent_error_goes_straight_to_the_dlq(kafka, make_ladder, group_id, pipeline, kind):
    ladder = make_ladder(delays=(1.0, 1.0, 1.0))
    event_id, message = _permanent(kind, "acc_permanent")
    behind = payment_event("acc_permanent", "acc_dst", 500)
    publish_raw(kafka, ladder.main, [message, as_message(behind)])

    running = pipeline(ladder, group_id)
    assert wait_for(lambda: len(dlq_rows([event_id])) == 1 and processed_count(group_id) == 1)
    running.close()

    [row] = dlq_rows([event_id])
    assert row["attempts"] == 1
    assert row["reason"].startswith("permanent: EventError: "), row
    assert [message_count(kafka, topic) for topic in ladder.retries] == [0, 0, 0]
    assert [tier.stats.forwarded for tier in running.tiers] == [0, 0, 0]
    assert projected_event_ids([behind.event_id]) == [behind.event_id]
    if kind != "unparseable":
        # The body is stored verbatim and exact: a float amount stays 1000.5.
        stored = json.loads(row["payload"])
        assert stored["event_id"] in (event_id, json.loads(message[1])["event_id"])
    if kind == "float_amount":
        assert '"amount_minor": 1000.5' in row["payload"]


# --- 4. crash after the forward, before the offset commit --------------------------

# A main consumer in its own process. The target event fails on attempt 1
# ("recovers") or on every attempt ("always"). The process SIGKILLs itself
# right after its first forward is acknowledged, before the offset commit.
_CRASHING_CONSUMER = f"""
import json, os, signal, sys
from app.messaging.consumer import PaymentEventConsumer
from app.messaging.topics import TopicLadder

ladder_json, group, target, mode = sys.argv[1:5]
ladder = TopicLadder(**{{k: tuple(v) if isinstance(v, list) else v
                         for k, v in json.loads(ladder_json).items()}})

def side_effect(event, attempt):
    if event.event_id == target and (mode == "always" or attempt == 1):
        raise RuntimeError(f"injected failure on attempt {{attempt}}")

def after_forward(message, topic):
    print("crash-after-forward", topic, flush=True)
    os.kill(os.getpid(), signal.SIGKILL)

PaymentEventConsumer(
    ladder=ladder, group_id=group, config_overrides={FAST_GROUP!r},
    side_effect=side_effect, after_forward=after_forward,
).run()
"""


@pytest.mark.parametrize("mode", ["recovers", "always"])
def test_crash_between_forward_and_offset_commit(kafka, make_ladder, group_id, pipeline, mode):
    ladder = make_ladder(delays=(1.0, 1.0, 1.0))
    event = payment_event("acc_crash", "acc_dst", 3_000)
    publish_raw(kafka, ladder.main, [as_message(event)])

    victim = subprocess.Popen(
        [sys.executable, "-c", _CRASHING_CONSUMER, json.dumps(ladder.__dict__), group_id,
         event.event_id, mode],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    line = victim.stdout.readline().split()
    victim.wait(timeout=30)
    victim.stdout.close()
    assert line == ["crash-after-forward", ladder.retries[0]], line
    assert victim.returncode == -signal.SIGKILL
    # The forward was acknowledged; the original offset was not committed.
    assert message_count(kafka, ladder.retries[0]) == 1

    def side_effect(evt, attempt: int) -> None:
        if evt.event_id == event.event_id and (mode == "always" or attempt == 1):
            raise InjectedFailure(f"injected failure on attempt {attempt}")

    running = pipeline(ladder, group_id, side_effect=side_effect)
    if mode == "recovers":
        # Two copies come back from the first tier: one applies, one is skipped.
        assert wait_for(
            lambda: running.main.stats.applied == 1 and running.main.stats.skipped == 1,
            timeout=60,
        ), running.main.stats
    else:
        # Two copies run the whole ladder; both reach the DLQ topic.
        assert wait_for(
            lambda: running.writer.stats.written + running.writer.stats.skipped == 2,
            timeout=90,
        ), running.writer.stats
    time.sleep(1.5)
    running.close()

    effects = projected_event_ids([event.event_id])
    rows = dlq_rows([event.event_id])
    if mode == "recovers":
        assert effects == [event.event_id] and rows == []
        assert message_count(kafka, ladder.dlq) == 0
    else:
        assert effects == [] and len(rows) == 1
        assert message_count(kafka, ladder.dlq) == 2  # the duplicate, absorbed
        assert running.writer.stats.skipped == 1


# --- 5. operator replay ----------------------------------------------------------------


def test_replay_applies_once_and_is_operator_only(
    kafka, make_ladder, group_id, pipeline, client, operator_token, client_token, replay_to
):
    from app.services.dlq import DeadLetter, dedup_id_for, record_dead_letter

    ladder = make_ladder(delays=(0.5, 0.5, 0.5))
    replay_to(kafka, ladder.main)
    operator = {"Authorization": f"Bearer {operator_token}"}
    broken = threading.Event()
    broken.set()
    event = payment_event("acc_replay", "acc_dst", 9_900)

    def side_effect(evt, attempt: int) -> None:
        if broken.is_set() and evt.event_id == event.event_id:
            raise InjectedFailure("downstream is down")

    publish_raw(kafka, ladder.main, [as_message(event)])
    running = pipeline(ladder, group_id, side_effect=side_effect)
    assert wait_for(lambda: len(dlq_rows([event.event_id])) == 1, timeout=60)
    [row] = dlq_rows([event.event_id])
    assert row["attempts"] == 4 and projected_event_ids([event.event_id]) == []
    broken.clear()  # the downstream system is fixed

    # Operator only. A client gets 403 and nothing is published.
    denied = client.post(
        f"/v1/dlq/{row['id']}/replay", headers={"Authorization": f"Bearer {client_token}"}
    )
    assert denied.status_code == 403, denied.text
    assert client.get("/v1/dlq", headers={"Authorization": f"Bearer {client_token}"}).status_code == 403
    assert client.post("/v1/dlq/987654321/replay", headers=operator).status_code == 404

    replayed = client.post(f"/v1/dlq/{row['id']}/replay", headers=operator)
    assert replayed.status_code == 200, replayed.text
    assert replayed.json()["event_id"] == event.event_id
    assert replayed.json()["replayed_at"] is not None
    assert wait_for(lambda: processed_count(group_id) == 1, timeout=30)

    # A second replay is refused; the effect still happened once.
    again = client.post(f"/v1/dlq/{row['id']}/replay", headers=operator)
    assert again.status_code == 409, again.text
    assert again.json()["error"]["code"] == "dlq_already_replayed"

    # Two concurrent replays of another dead letter: harmless.
    other = payment_event("acc_replay", "acc_dst", 1_100)
    _, value, _ = as_message(other)
    record_dead_letter(
        DeadLetter(
            dedup_id=dedup_id_for(other.event_id, None, "unused"),
            event_id=other.event_id,
            original_topic=ladder.main,
            payload_json=value.decode(),
            reason="transient: InjectedFailure: test",
            attempts=4,
            first_failed_at=row["first_failed_at"],
        ),
        f"{group_id}-dlq",
    )
    [other_row] = dlq_rows([other.event_id])
    with ThreadPoolExecutor(max_workers=2) as pool:
        codes = sorted(
            pool.map(
                lambda _: client.post(f"/v1/dlq/{other_row['id']}/replay", headers=operator).status_code,
                range(2),
            )
        )
    assert codes == [200, 409], codes
    assert wait_for(lambda: processed_count(group_id) == 2, timeout=30)
    time.sleep(1.5)  # any duplicate publish has had time to arrive
    running.close()

    assert projected_event_ids([event.event_id]) == [event.event_id]
    assert projected_event_ids([other.event_id]) == [other.event_id]
    assert processed_count(group_id) == 2
    assert len(dlq_rows([event.event_id])) == 1  # the replay did not fail again


# --- 6. infrastructure error ----------------------------------------------------------


def test_database_outage_holds_messages_and_resumes(kafka, make_ladder, group_id, pipeline):
    ladder = make_ladder(delays=(0.5, 0.5, 0.5))
    events = [payment_event(f"acc_outage_{i % 2}", "acc_dst", 100 + i) for i in range(20)]
    database_down = threading.Event()
    database_down.set()

    def side_effect(evt, attempt: int) -> None:
        if database_down.is_set():
            raise _injected_outage()

    publish_raw(kafka, ladder.main, [as_message(e) for e in events])
    running = pipeline(ladder, group_id, side_effect=side_effect, db_backoff=(0.05, 0.3))
    assert wait_for(lambda: running.main.stats.held_for_database >= 20, timeout=60)
    time.sleep(1.5)  # longer than a tier delay

    # During the outage nothing moved: no retry topic, no DLQ, no offsets.
    assert processed_count(group_id) == 0
    assert [message_count(kafka, t) for t in (*ladder.retries, ladder.dlq)] == [0, 0, 0, 0]
    assert running.main.stats.handled == 0

    database_down.clear()
    assert wait_for(lambda: processed_count(group_id) == 20, timeout=60)
    running.close()

    assert running.main.stats.applied == 20
    assert running.main.stats.retried == 0 and running.main.stats.dead_lettered == 0
    assert [message_count(kafka, t) for t in (*ladder.retries, ladder.dlq)] == [0, 0, 0, 0]
    # Each held message was retried in place, so each account kept its order.
    for account in ("acc_outage_0", "acc_outage_1"):
        published = [e.event_id for e in events if e.partition_key == account]
        assert projected_event_ids(published) == published


# --- 7. waiting in a tier causes no rebalance -------------------------------------------


def test_waiting_in_a_tier_does_not_rebalance_the_group(kafka, make_ladder, group_id):
    from app.messaging.retry_tier import RetryTierConsumer

    delay = 9.0
    ladder = make_ladder(delays=(delay, 1.0, 1.0))
    # max.poll.interval.ms is 6 s, shorter than the 9 s wait. A tier that
    # blocked until the message was due would be thrown out of its group.
    strict = {**FAST_GROUP, "max.poll.interval.ms": 6000}
    tier = RunningLoop(
        RetryTierConsumer(
            ladder.retries[0], ladder=ladder, group_id=f"{group_id}-retry0", config_overrides=strict
        )
    )
    try:
        assert wait_for(lambda: tier.consumer.assignment == set(range(6)), timeout=30)
        event = payment_event("acc_waiting", "acc_dst", 600)
        key, value, headers = as_message(event)
        headers.update(attempt_count="1", last_error="transient: X: y", original_topic=ladder.main)
        sent_at = time.monotonic()
        publish_raw(kafka, ladder.retries[0], [(key, value, headers)])

        assert wait_for(lambda: tier.stats.forwarded == 1, timeout=delay + 20)
        waited = time.monotonic() - sent_at
        # Read before close: leaving the group on close is itself a revoke.
        rebalances = dataclasses.replace(tier.consumer.rebalances)
    finally:
        tier.close()

    assert waited >= delay - 0.5, waited
    assert tier.stats.waits >= 1
    assert (rebalances.assigned, rebalances.revoked, rebalances.lost) == (1, 0, 0), rebalances
    assert message_count(kafka, ladder.main) == 1
