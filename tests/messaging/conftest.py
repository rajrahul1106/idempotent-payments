"""Fixtures for the relay, consumer and retry/DLQ tests.

`RecordingProducer` stands in for a broker. This is deliberate and it is
not a mocked broker in the sense the blueprint rules out: what is under
test in Phase 4 is whether the relay loses a committed row, and that is a
question about PostgreSQL and the relay's transaction boundary, both of
which are real here. A double is also the only way to inject the failures
that matter, since a real broker will not refuse a message on command or
crash between acknowledgement and commit.

Redelivery semantics need a real broker, and those belong to the consumer
in Phase 5, where the tests carry the `messaging` marker and Kafka runs.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from app.messaging.producer import PublishError


class RecordingProducer:
    """Records every publish. Can be told to fail, and to fail only once.

    Satisfies the `Producer` protocol in app.messaging.producer.
    """

    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []
        self.closed = False
        self.flush_calls = 0
        #: Raise PublishError on the publish whose key matches this.
        self.fail_on_key: str | None = None
        #: Raise PublishError on the Nth publish (1-based). None disables.
        self.fail_on_nth: int | None = None
        #: When True, the configured failure fires once and then clears.
        self.fail_once = False
        self._attempts = 0

    # --- Producer protocol -------------------------------------------
    def publish(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._attempts += 1
        should_fail = (
            self.fail_on_key is not None and key == self.fail_on_key
        ) or (self.fail_on_nth is not None and self._attempts == self.fail_on_nth)

        if should_fail:
            if self.fail_once:
                self.fail_on_key = None
                self.fail_on_nth = None
            raise PublishError(f"injected failure on {topic} key={key}")

        self.messages.append(
            {
                "topic": topic,
                "key": key,
                "value": value,
                "headers": dict(headers or {}),
            }
        )

    def flush(self, timeout_seconds: float = 10.0) -> int:
        self.flush_calls += 1
        return 0

    def close(self) -> None:
        self.closed = True

    # --- test helpers -------------------------------------------------
    @property
    def event_ids(self) -> list[str]:
        """Published event ids, in publish order."""
        return [m["headers"]["event_id"] for m in self.messages]

    @property
    def keys(self) -> list[str]:
        return [m["key"] for m in self.messages]


class CrashingProducer(RecordingProducer):
    """Acknowledges a publish, then crashes the process mid-batch.

    Models the one window the relay cannot close: the broker has the
    message but the transaction has not committed. The next tick must
    republish it rather than lose it.
    """

    class Crash(RuntimeError):
        pass

    def __init__(self, crash_after: int) -> None:
        super().__init__()
        self.crash_after = crash_after
        self._published = 0

    def publish(self, topic: str, key: str, value: bytes, headers=None) -> None:
        super().publish(topic, key, value, headers)
        self._published += 1
        if self._published >= self.crash_after:
            raise CrashingProducer.Crash(
                f"crashed after acknowledging {self._published} message(s)"
            )


@pytest.fixture
def producer() -> RecordingProducer:
    return RecordingProducer()


@pytest.fixture
def clear_outbox(database):
    """Empty the outbox so a test's counts are its own.

    The relay claims every unpublished row regardless of who wrote it, so
    a test asserting "50 rows recovered" needs to start from zero. This
    only ever runs against the _test database (see tests/conftest.py).
    """
    from sqlalchemy import text

    from app.db.session import unit_of_work

    def _clear() -> None:
        with unit_of_work() as session:
            session.execute(text("DELETE FROM outbox"))

    _clear()
    yield _clear
    _clear()


@pytest.fixture
def make_outbox_rows(clear_outbox):
    """Insert and commit N outbox events. Returns their event ids in order."""
    from app.db.models.messaging import OutboxEvent
    from app.db.session import unit_of_work

    def _make(
        count: int,
        *,
        partition_key: str = "acc_relay_test",
        traceparent: str | None = None,
    ) -> list[str]:
        event_ids = [str(uuid.uuid4()) for _ in range(count)]
        with unit_of_work() as session:
            for index, event_id in enumerate(event_ids):
                session.add(
                    OutboxEvent(
                        event_id=event_id,
                        event_type="payment.posted",
                        schema_version=1,
                        partition_key=partition_key,
                        payload={
                            "event_id": event_id,
                            "sequence": index,
                            "amount_minor": 1000 + index,
                        },
                        traceparent=traceparent,
                    )
                )
        return event_ids

    return _make


# --- Phases 5 and 6: real Redpanda -------------------------------------------
#
# Every test gets its own topics (a main topic, its retry tiers and its DLQ)
# and its own consumer groups, named with this run's id, so no test can see
# another's messages and no failure can land on a topic `make up` consumes.
# Topics are deleted again at teardown.

import re  # noqa: E402
import threading  # noqa: E402
import time  # noqa: E402
from types import SimpleNamespace  # noqa: E402

RUN_ID = uuid.uuid4().hex[:8]

#: Short group timeouts, so a SIGKILLed consumer's partitions move in
#: seconds rather than the default 45.
FAST_GROUP = {"session.timeout.ms": 6000, "heartbeat.interval.ms": 1000}


def wait_for(condition, timeout: float = 60.0, interval: float = 0.1) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(interval)
    return bool(condition())


def _ladder_names(request) -> str:
    label = re.sub(r"[^A-Za-z0-9._-]", "-", request.node.name)[:50]
    return f"test.{RUN_ID}.{label}.{uuid.uuid4().hex[:6]}"


@pytest.fixture
def make_ladder(kafka, request):
    """make_ladder(delays) -> a TopicLadder of fresh 6-partition topics:
    main, one retry topic per delay, and a DLQ. Deleted at teardown."""
    from app.messaging.topic_setup import ensure_topics
    from app.messaging.topics import TopicLadder

    created: list[str] = []

    def _make(delays: tuple[float, ...] = (1.0, 1.0, 1.0)) -> TopicLadder:
        base = _ladder_names(request)
        ladder = TopicLadder(
            main=base,
            retries=tuple(f"{base}.retry{i}" for i in range(len(delays))),
            retry_delays=tuple(delays),
            dlq=f"{base}.dlq",
        )
        ensure_topics(list(ladder.all), partitions=6, bootstrap_servers=kafka)
        created.extend(ladder.all)
        return ladder

    yield _make
    if created:
        delete_topics(kafka, created)


@pytest.fixture
def fresh_ladder(make_ladder):
    return make_ladder()


def delete_topics(bootstrap: str, names: list[str]) -> None:
    """Delete test topics and wait for it. Warns instead of failing the test."""
    import warnings

    from confluent_kafka.admin import AdminClient

    # Keep the client referenced until the futures resolve; a client that is
    # garbage-collected first never finishes the request.
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for name, future in admin.delete_topics(names, operation_timeout=30).items():
        try:
            future.result(timeout=30)
        except Exception as exc:
            warnings.warn(f"could not delete test topic {name}: {exc}", stacklevel=2)


@pytest.fixture
def group_id() -> str:
    return f"test-{RUN_ID}-{uuid.uuid4().hex[:8]}"


def payment_event(
    from_account: str = "acc_src",
    to_account: str = "acc_dst",
    amount_minor: int = 1_000,
    *,
    traceparent: str | None = None,
) -> SimpleNamespace:
    """An outbox-shaped event, so the relay's own serialiser builds the message."""
    event_id = str(uuid.uuid4())
    return SimpleNamespace(
        event_id=event_id,
        event_type="payment.posted",
        schema_version=1,
        partition_key=from_account,
        traceparent=traceparent,
        payload={
            "event_id": event_id,
            "event_type": "payment.posted",
            "schema_version": 1,
            "transaction_id": f"pay_{uuid.uuid4().hex[:24]}",
            "from_account_id": from_account,
            "to_account_id": to_account,
            "amount_minor": amount_minor,
            "currency": "INR",
            "reference": None,
            "occurred_at": "2026-11-10T00:00:00+00:00",
        },
    )


def publish(bootstrap: str, topic: str, events) -> None:
    """Publish in order, keyed like the relay, and wait for every ack."""
    from confluent_kafka import Producer

    from app.messaging.relay import _serialise

    producer = Producer(
        {"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True}
    )
    for event in events:
        value, headers = _serialise(event)
        producer.produce(
            topic,
            key=event.partition_key.encode("utf-8"),
            value=value,
            headers=list(headers.items()),
        )
        producer.poll(0)
    assert producer.flush(30) == 0, "broker did not acknowledge every message"


class RunningLoop:
    """A real ConsumerLoop (main consumer, retry tier or DLQ writer) on a
    background thread."""

    def __init__(self, loop) -> None:
        self.consumer = loop
        self.stop = threading.Event()
        self.error: BaseException | None = None
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        try:
            self.consumer.run(self.stop)
        except BaseException as exc:  # surfaced by close()
            self.error = exc

    @property
    def stats(self):
        return self.consumer.stats

    def close(self) -> None:
        self.stop.set()
        self.thread.join(timeout=30)
        assert not self.thread.is_alive(), "consumer did not stop"
        assert self.error is None, f"consumer failed: {self.error!r}"


class RunningConsumer(RunningLoop):
    """A PaymentEventConsumer with short group timeouts."""

    def __init__(self, **kwargs) -> None:
        from app.messaging.consumer import PaymentEventConsumer

        kwargs.setdefault("config_overrides", FAST_GROUP)
        super().__init__(PaymentEventConsumer(**kwargs))


@pytest.fixture
def consumers():
    """Start consumers with start(**kwargs); all are stopped at teardown."""
    started: list[RunningLoop] = []

    def start(**kwargs) -> RunningConsumer:
        running = RunningConsumer(**kwargs)
        started.append(running)
        return running

    yield start
    _stop_all(started)


def _stop_all(started) -> None:
    for running in started:
        running.stop.set()
    for running in started:
        running.thread.join(timeout=30)


class Pipeline:
    """The whole Phase 6 path on one ladder: the main consumer, one consumer
    per retry tier, and the DLQ writer, each in its own group."""

    def __init__(self, ladder, group_id: str, **main_kwargs) -> None:
        from app.messaging.dlq_writer import DlqWriter
        from app.messaging.retry_tier import RetryTierConsumer

        self.ladder = ladder
        self.group_id = group_id
        self.writer_group = f"{group_id}-dlq"
        main_kwargs.setdefault("config_overrides", FAST_GROUP)
        self.main = RunningConsumer(ladder=ladder, group_id=group_id, **main_kwargs)
        self.tiers = [
            RunningLoop(
                RetryTierConsumer(
                    topic,
                    ladder=ladder,
                    group_id=f"{group_id}-retry{index}",
                    config_overrides=FAST_GROUP,
                )
            )
            for index, topic in enumerate(ladder.retries)
        ]
        self.writer = RunningLoop(
            DlqWriter(ladder=ladder, group_id=self.writer_group, config_overrides=FAST_GROUP)
        )

    @property
    def all(self) -> list[RunningLoop]:
        return [self.main, *self.tiers, self.writer]

    def close(self) -> None:
        for running in self.all:
            running.stop.set()
        for running in self.all:
            running.close()


@pytest.fixture
def pipeline():
    """pipeline(ladder, group_id, **main_consumer_kwargs). Stopped at teardown."""
    started: list[Pipeline] = []

    def start(ladder, group_id: str, **kwargs) -> Pipeline:
        running = Pipeline(ladder, group_id, **kwargs)
        started.append(running)
        return running

    yield start
    for running in started:
        _stop_all(running.all)


def publish_raw(bootstrap: str, topic: str, messages) -> None:
    """Publish (key, value, headers) tuples in order and wait for every ack."""
    from confluent_kafka import Producer

    producer = Producer(
        {"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True}
    )
    for key, value, headers in messages:
        producer.produce(
            topic,
            key=key.encode("utf-8") if isinstance(key, str) else key,
            value=value,
            headers=[(k, v.encode("utf-8")) for k, v in headers.items()],
        )
        producer.poll(0)
    assert producer.flush(30) == 0, "broker did not acknowledge every message"


def as_message(event) -> tuple[str, bytes, dict[str, str]]:
    """An outbox-shaped event as (key, value, headers), via the relay's serialiser."""
    from app.messaging.relay import _serialise

    value, headers = _serialise(event)
    return event.partition_key, value, headers


def message_count(bootstrap: str, topic: str) -> int:
    """Messages ever written to a fresh topic: the sum of high watermarks."""
    from confluent_kafka import Consumer, TopicPartition

    probe = Consumer({"bootstrap.servers": bootstrap, "group.id": f"probe-{uuid.uuid4().hex}"})
    try:
        partitions = probe.list_topics(topic, timeout=10).topics[topic].partitions
        return sum(
            probe.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1]
            for p in partitions
        )
    finally:
        probe.close()


def dlq_rows(event_ids) -> list[dict]:
    from sqlalchemy import text

    from app.db.session import read_session

    with read_session() as session:
        return [
            dict(row)
            for row in session.execute(
                text(
                    "SELECT id, event_id::text AS event_id, original_topic, reason, attempts, "
                    "first_failed_at, created_at, replayed_at, payload::text AS payload "
                    "FROM dlq_messages WHERE event_id = ANY(CAST(:ids AS uuid[])) ORDER BY id"
                ),
                {"ids": list(event_ids)},
            ).mappings()
        ]


def processed_count(group: str) -> int:
    from sqlalchemy import text

    from app.db.session import read_session

    with read_session() as session:
        return session.execute(
            text("SELECT count(*) FROM processed_events WHERE consumer_group = :g"),
            {"g": group},
        ).scalar_one()


def projected_event_ids(event_ids) -> list[str]:
    """payment_events rows for these events, in the order they were written."""
    from sqlalchemy import text

    from app.db.session import read_session

    with read_session() as session:
        return [
            str(row)
            for row in session.execute(
                text(
                    "SELECT event_id FROM payment_events "
                    "WHERE event_id = ANY(CAST(:ids AS uuid[])) ORDER BY id"
                ),
                {"ids": list(event_ids)},
            ).scalars()
        ]
