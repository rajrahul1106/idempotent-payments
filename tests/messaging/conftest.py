"""Fixtures for the relay tests.

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
def clear_outbox(db_available):
    """Empty the outbox so a test's counts are its own.

    The relay claims every unpublished row regardless of who wrote it, so
    a test asserting "50 rows recovered" needs to start from zero.
    """
    if not db_available:
        pytest.skip("PostgreSQL not reachable")
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
