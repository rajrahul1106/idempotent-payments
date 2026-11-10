"""Relay wake-up (§15): NOTIFY cuts publish delay, polling still guarantees.

The relay runs its real loop, advisory lock included, against the test
database, with a recording producer in place of the broker. The poll
interval is set long on purpose so the difference is unmistakable.
"""

from __future__ import annotations

import threading
import time
import uuid

import pytest

from app.messaging import relay
from tests.messaging.conftest import RecordingProducer, wait_for

pytestmark = pytest.mark.integration


@pytest.fixture
def running_relay(clear_outbox):
    started: list[tuple[threading.Thread, threading.Event]] = []

    def start(*, poll_interval: float, listen: bool) -> RecordingProducer:
        producer, stop = RecordingProducer(), threading.Event()
        thread = threading.Thread(
            target=relay.run_forever,
            kwargs={"producer": producer, "stop": stop, "lock_retry_seconds": 0.1,
                    "poll_interval": poll_interval, "listen": listen},
            daemon=True,
        )
        thread.start()
        started.append((thread, stop))
        # Let it take the lock, run its first (empty) tick and settle into
        # its idle wait, so each payment below arrives mid-wait.
        time.sleep(1.0)
        return producer

    yield start
    for thread, stop in started:
        stop.set()
    for thread, _ in started:
        thread.join(timeout=15)


def _pay(client, owner_token, source, destination) -> str:
    response = client.post(
        "/v1/payments",
        json={"from_account_id": source, "to_account_id": destination,
              "amount_minor": 100, "currency": "INR"},
        headers={"Authorization": f"Bearer {owner_token(source)}",
                 "Idempotency-Key": str(uuid.uuid4())},
    )
    assert response.status_code == 201, response.text
    return response.json()["event_id"]


def _delays(client, owner_token, make_account, producer, count: int, timeout: float) -> list[float]:
    source, destination = make_account(100_000), make_account(0)
    delays = []
    for _ in range(count):
        started = time.monotonic()
        event_id = _pay(client, owner_token, source, destination)
        assert wait_for(lambda: event_id in producer.event_ids, timeout=timeout, interval=0.01)
        delays.append(time.monotonic() - started)
        time.sleep(0.3)  # the relay goes back to its idle wait
    return delays


def test_notify_publishes_well_under_the_poll_interval(
    running_relay, client, owner_token, make_account
):
    producer = running_relay(poll_interval=5.0, listen=True)
    delays = _delays(client, owner_token, make_account, producer, count=5, timeout=10)
    # Polling alone would average 2.5 s here and could take 5 s.
    assert max(delays) < 1.0, delays


def test_without_notify_every_row_is_still_published_by_polling(
    running_relay, client, owner_token, make_account
):
    producer = running_relay(poll_interval=1.5, listen=False)
    delays = _delays(client, owner_token, make_account, producer, count=3, timeout=10)
    assert len(delays) == 3
    # Published by the poll, so it waited for one: the wake-up really is off.
    assert max(delays) > 0.5, delays


def test_a_missed_notification_never_strands_a_row(running_relay, make_outbox_rows):
    producer = running_relay(poll_interval=1.0, listen=True)
    # Rows committed with no NOTIFY at all: the relay was never woken for them.
    event_ids = make_outbox_rows(5)
    assert wait_for(lambda: producer.event_ids == event_ids, timeout=10), producer.event_ids


def test_a_rolled_back_payment_sends_no_notification():
    """NOTIFY is transactional: nothing is delivered for a rollback."""
    from app.db.repositories import outbox as outbox_repo
    from app.db.session import SessionFactory

    listener = relay.OutboxListener()
    stop = threading.Event()
    try:
        assert listener.wait(0.2, stop) is False  # connected, nothing pending
        session = SessionFactory()
        try:
            outbox_repo.notify(session, str(uuid.uuid4()))
            session.rollback()
        finally:
            session.close()
        assert listener.wait(0.5, stop) is False

        with SessionFactory() as session:
            outbox_repo.notify(session, str(uuid.uuid4()))
            session.commit()
        assert listener.wait(2.0, stop) is True
    finally:
        listener.close()
