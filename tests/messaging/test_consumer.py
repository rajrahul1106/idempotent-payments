"""Phase 5 gate: each event's side effect is applied exactly once, even when
Kafka redelivers and the consumer crashes.

Real Redpanda, real Postgres. Each test has its own topic (6 partitions) and
consumer group. "Applied once" is checked in both tables: processed_events,
the dedup record, and payment_events, the side effect.
"""

from __future__ import annotations

import json
import pathlib
import random
import signal
import subprocess
import sys
import uuid
from decimal import Decimal

import pytest
from sqlalchemy import text

from app.db.session import read_session
from tests.messaging.conftest import (
    FAST_GROUP,
    payment_event,
    processed_count,
    projected_event_ids,
    publish,
    wait_for,
)

pytestmark = pytest.mark.messaging

ROOT = pathlib.Path(__file__).resolve().parents[2]

# A consumer in its own process, so it can die for real. "slow" sleeps after
# each database commit so the parent can kill it mid-batch. "crash" kills
# itself after the Nth database commit, before that message's offset commit.
_CONSUMER_PROCESS = f"""
import json, os, signal, sys, time
from app.messaging.consumer import PaymentEventConsumer
from app.messaging.topics import TopicLadder

ladder_json, group, mode, arg = sys.argv[1:5]
ladder = TopicLadder(**{{k: tuple(v) if isinstance(v, list) else v
                         for k, v in json.loads(ladder_json).items()}})
seen = 0

def after_db_commit(event, applied):
    global seen
    seen += 1
    if mode == "slow":
        time.sleep(float(arg))
    elif mode == "crash" and seen == int(arg):
        print("crash-after-db-commit", event.event_id, flush=True)
        os.kill(os.getpid(), signal.SIGKILL)

PaymentEventConsumer(
    ladder=ladder, group_id=group, config_overrides={FAST_GROUP!r},
    after_db_commit=after_db_commit,
).run()
"""


def _consumer_process(ladder, group: str, mode: str, arg: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-c", _CONSUMER_PROCESS, json.dumps(ladder.__dict__), group, mode, arg],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )


def _spread(count: int, accounts: int) -> list:
    return [payment_event(f"acc_src_{i % accounts}", "acc_dst", 1_000 + i) for i in range(count)]


# --- 1. duplicates -------------------------------------------------------------


def test_1000_events_with_300_duplicates_apply_exactly_700(kafka, fresh_ladder, group_id, consumers):
    originals = _spread(700, accounts=40)
    rng = random.Random(7)
    messages = list(originals)
    for duplicate in rng.choices(originals, k=300):
        # An exact duplicate: same event, inserted somewhere after the original.
        position = rng.randint(messages.index(duplicate) + 1, len(messages))
        messages.insert(position, duplicate)
    assert len(messages) == 1000
    publish(kafka, fresh_ladder.main, messages)

    running = consumers(ladder=fresh_ladder, group_id=group_id)
    assert wait_for(lambda: running.stats.handled >= 1000), running.stats
    running.close()

    ids = [e.event_id for e in originals]
    assert running.stats.applied == 700
    assert running.stats.skipped == 300
    assert processed_count(group_id) == 700
    assert len(projected_event_ids(ids)) == 700


# --- 2. SIGKILL mid-batch --------------------------------------------------------


def test_consumer_killed_mid_batch_then_restarted_applies_every_event_once(
    kafka, fresh_ladder, group_id, consumers
):
    events = _spread(300, accounts=30)
    publish(kafka, fresh_ladder.main, events)

    victim = _consumer_process(fresh_ladder, group_id, "slow", "0.01")
    try:
        assert wait_for(lambda: processed_count(group_id) >= 60), "consumer made no progress"
    finally:
        victim.send_signal(signal.SIGKILL)
        victim.wait(timeout=10)
        victim.stdout.close()
    assert victim.returncode == -signal.SIGKILL
    done_before_kill = processed_count(group_id)
    assert done_before_kill < 300, "killed too late to be mid-batch"

    restarted = consumers(ladder=fresh_ladder, group_id=group_id)
    assert wait_for(lambda: processed_count(group_id) == 300, timeout=90)
    restarted.close()

    assert len(projected_event_ids([e.event_id for e in events])) == 300
    # The restart never applied anything the killed process had applied.
    assert restarted.stats.applied == 300 - done_before_kill


# --- 3. crash between the database commit and the offset commit ---------------------


def test_crash_after_db_commit_before_offset_commit_skips_the_redelivery(
    kafka, fresh_ladder, group_id, consumers
):
    events = _spread(100, accounts=10)
    publish(kafka, fresh_ladder.main, events)

    victim = _consumer_process(fresh_ladder, group_id, "crash", "40")
    line = victim.stdout.readline().split()
    victim.wait(timeout=30)
    victim.stdout.close()
    assert line[:1] == ["crash-after-db-commit"], line
    crashed_event = line[1]
    assert victim.returncode == -signal.SIGKILL

    # Its database transaction committed; its offset did not.
    with read_session() as session:
        recorded = session.execute(
            text(
                "SELECT count(*) FROM processed_events "
                "WHERE event_id = :e AND consumer_group = :g"
            ),
            {"e": crashed_event, "g": group_id},
        ).scalar_one()
    assert recorded == 1

    restarted = consumers(ladder=fresh_ladder, group_id=group_id)
    assert wait_for(
        lambda: processed_count(group_id) == 100
        and crashed_event in restarted.stats.skipped_event_ids,
        timeout=90,
    ), restarted.stats
    restarted.close()

    assert projected_event_ids([crashed_event]) == [crashed_event]  # one effect, not two
    assert len(projected_event_ids([e.event_id for e in events])) == 100


# --- 4. per-account order ----------------------------------------------------------


def test_200_events_for_one_account_are_processed_in_publish_order(
    kafka, fresh_ladder, group_id, consumers
):
    events = [payment_event("acc_ordered", "acc_dst", 100 + i) for i in range(200)]
    publish(kafka, fresh_ladder.main, events)

    running = consumers(ladder=fresh_ladder, group_id=group_id)
    assert wait_for(lambda: processed_count(group_id) == 200)
    running.close()

    published = [e.event_id for e in events]
    assert projected_event_ids(published) == published


# --- 5. two instances, one group -----------------------------------------------------


def test_two_consumers_in_one_group_apply_each_event_once(
    kafka, fresh_ladder, group_id, consumers
):
    first = consumers(ladder=fresh_ladder, group_id=group_id)
    second = consumers(ladder=fresh_ladder, group_id=group_id)
    assert wait_for(
        lambda: first.consumer.assignment
        and second.consumer.assignment
        and first.consumer.assignment | second.consumer.assignment == set(range(6)),
        timeout=60,
    ), (first.consumer.assignment, second.consumer.assignment)
    assert not first.consumer.assignment & second.consumer.assignment

    events = _spread(600, accounts=60)
    publish(kafka, fresh_ladder.main, events)

    assert wait_for(lambda: processed_count(group_id) == 600)
    first.close()
    second.close()

    assert len(projected_event_ids([e.event_id for e in events])) == 600
    assert first.stats.applied > 0 and second.stats.applied > 0
    assert first.stats.applied + second.stats.applied == 600


# --- 6. end to end ---------------------------------------------------------------------


def test_payment_flows_from_api_through_relay_and_consumer(
    kafka, fresh_ladder, group_id, consumers, client, owner_token, make_account, clear_outbox
):
    from app.db.session import unit_of_work
    from app.messaging import relay
    from app.messaging.producer import KafkaProducer

    source, destination = make_account(500_000), make_account(0)
    resp = client.post(
        "/v1/payments",
        json={
            "from_account_id": source,
            "to_account_id": destination,
            "amount_minor": 100_000,
            "currency": "INR",
        },
        headers={
            "Authorization": f"Bearer {owner_token(source)}",
            "Idempotency-Key": str(uuid.uuid4()),
        },
    )
    assert resp.status_code == 201, resp.text
    payment = resp.json()

    producer = KafkaProducer(bootstrap_servers=kafka)
    try:
        with unit_of_work() as session:
            published = relay.publish_batch(session, producer, topic=fresh_ladder.main)
    finally:
        producer.close()
    assert published.event_ids == [payment["event_id"]]

    running = consumers(ladder=fresh_ladder, group_id=group_id)
    assert wait_for(lambda: processed_count(group_id) == 1)
    running.close()

    with read_session() as session:
        row = session.execute(
            text(
                "SELECT transaction_id, from_account_id, to_account_id, amount, currency "
                "FROM payment_events WHERE event_id = :e"
            ),
            {"e": payment["event_id"]},
        ).one()
        duplicate_event_ids = session.execute(
            text(
                "SELECT count(*) FROM (SELECT 1 FROM processed_events "
                "GROUP BY event_id, consumer_group HAVING count(*) > 1) d"
            )
        ).scalar_one()
        ledger_sum = session.execute(text("SELECT COALESCE(SUM(amount), 0) FROM postings")).scalar_one()
        unpublished = session.execute(
            text("SELECT count(*) FROM outbox WHERE published_at IS NULL")
        ).scalar_one()
        negative_customers = session.execute(
            text(
                "SELECT count(*) FROM (SELECT a.id FROM accounts a "
                "JOIN postings p ON p.account_id = a.id WHERE a.account_type = 'customer' "
                "GROUP BY a.id HAVING SUM(p.amount) < 0) n"
            )
        ).scalar_one()

    assert tuple(row) == (
        payment["payment_id"],
        source,
        destination,
        Decimal("1000.0000"),
        "INR",
    )
    assert duplicate_event_ids == 0  # I1
    assert ledger_sum == Decimal("0")  # I2
    assert unpublished == 0  # I3
    assert negative_customers == 0  # I4
