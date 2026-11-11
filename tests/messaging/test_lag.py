"""Consumer lag from committed offsets, without the consumer (§11)."""

from __future__ import annotations

import pytest

from app.messaging.lag import LagReader
from tests.messaging.conftest import as_message, payment_event, publish_raw, wait_for

pytestmark = pytest.mark.messaging


def test_lag_is_read_from_the_broker_and_survives_the_consumer(
    kafka, fresh_ladder, group_id, consumers
):
    events = [payment_event(f"acc_lag_{i % 3}", "acc_dst", 100 + i) for i in range(30)]
    publish_raw(kafka, fresh_ladder.main, [as_message(e) for e in events])
    reader = LagReader(kafka)
    try:
        def total() -> int:
            return sum(p.lag for p in reader.read({group_id: fresh_ladder.main}))

        # No consumer has ever run: everything is lag, partition by partition.
        lags = reader.read({group_id: fresh_ladder.main})
        assert len(lags) == 6 and total() == 30

        running = consumers(ladder=fresh_ladder, group_id=group_id)
        assert wait_for(lambda: total() == 0, timeout=60)
        running.close()

        # The consumer is gone; lag still reads, and still rises.
        publish_raw(kafka, fresh_ladder.main, [as_message(payment_event()) for _ in range(4)])
        assert total() == 4
    finally:
        reader.close()
