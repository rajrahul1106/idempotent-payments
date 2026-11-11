"""What the API and the pipeline gauges expose, read the way Prometheus
reads them: from /metrics."""

from __future__ import annotations

import re
import uuid

import pytest
from prometheus_client.parser import text_string_to_metric_families
from sqlalchemy import text

from app.core import metrics
from app.db.session import engine

pytestmark = pytest.mark.integration

IDENTIFIER = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"  # a UUID
    r"|\b[0-9a-f]{32}\b"  # a trace id
    r"|\bacc_|\bpay_|Bearer"
)


def _scrape(client) -> dict[tuple, float]:
    response = client.get("/metrics")
    assert response.status_code == 200
    samples = {}
    for family in text_string_to_metric_families(response.text):
        for sample in family.samples:
            samples[(sample.name, tuple(sorted(sample.labels.items())))] = sample.value
    return samples


def _value(samples, name, **labels) -> float:
    return samples.get((name, tuple(sorted(labels.items()))), 0.0)


def _pay(client, token, source, destination, *, key=None, amount=100):
    return client.post(
        "/v1/payments",
        json={"from_account_id": source, "to_account_id": destination,
              "amount_minor": amount, "currency": "INR"},
        headers={"Authorization": f"Bearer {token}", "Idempotency-Key": key or str(uuid.uuid4())},
    )


def test_payment_outcomes_are_counted_with_bounded_labels(client, owner_token, make_account):
    source, destination = make_account(10_000), make_account(0)
    token = owner_token(source)
    before = _scrape(client)

    key = str(uuid.uuid4())
    assert _pay(client, token, source, destination, key=key).status_code == 201
    assert _pay(client, token, source, destination, key=key).status_code == 200
    assert _pay(client, token, source, destination, key=key, amount=101).status_code == 422
    assert _pay(client, token, source, destination, amount=50_000).status_code == 422
    after = _scrape(client)

    def delta(name, **labels):
        return _value(after, name, **labels) - _value(before, name, **labels)

    assert delta("payments_total", status="created", currency="INR") == 1
    assert delta("payments_total", status="replayed", currency="INR") == 1
    assert delta("payments_total", status="idempotency_key_reuse", currency="none") == 1
    assert delta("payments_total", status="insufficient_funds", currency="none") == 1
    assert delta("idempotency_replays_total", outcome="replayed") == 1
    assert delta("idempotency_replays_total", outcome="mismatch") == 1
    assert delta("payment_duration_seconds_count", endpoint="/v1/payments") == 4
    assert _value(after, "db_pool_capacity") > 0

    # The route template, never the raw path.
    client.get(f"/v1/accounts/{source}", headers={"Authorization": f"Bearer {token}"})
    endpoints = {dict(labels).get("endpoint") for (name, labels) in _scrape(client)
                 if name.startswith("payment_duration_seconds")}
    assert "/v1/accounts/{account_id}" in endpoints


def test_the_api_exposes_only_its_own_metrics(client):
    names = {name for (name, _) in _scrape(client)}
    assert "payments_total" in names and "db_pool_capacity" in names
    assert "process_heartbeat_timestamp_seconds" in names
    for foreign in ("outbox_unpublished_rows", "kafka_consumer_lag", "relay_lock_held",
                    "reconciliation_last_run_timestamp_seconds", "events_processed_total"):
        assert foreign not in names, foreign


def test_no_label_value_is_a_per_request_identifier(client, owner_token, make_account):
    source, destination = make_account(10_000), make_account(0)
    _pay(client, owner_token(source), source, destination)
    offenders = [(name, labels) for (name, labels) in _scrape(client)
                 if any(IDENTIFIER.search(str(value)) for _, value in labels)]
    assert offenders == []


def test_outbox_gauges_come_from_the_database(clear_outbox_rows):
    from app.services.reconciliation import PipelineGauges

    gauges = PipelineGauges()
    gauges._refresh_outbox()  # noqa: SLF001 - Kafka is not needed for this half
    assert metrics.OUTBOX_UNPUBLISHED._value.get() == 0  # noqa: SLF001
    assert metrics.OUTBOX_OLDEST_AGE._value.get() == 0  # noqa: SLF001

    with engine.begin() as conn:
        for minutes in (1, 3, 5):
            conn.execute(
                text("INSERT INTO outbox (event_id, event_type, partition_key, payload, created_at) "
                     "VALUES (:e, 'payment.posted', 'acc_gauge', '{}', now() - make_interval(mins => :m))"),
                {"e": str(uuid.uuid4()), "m": minutes},
            )
    gauges._refresh_outbox()  # noqa: SLF001
    assert metrics.OUTBOX_UNPUBLISHED._value.get() == 3  # noqa: SLF001
    assert 300 <= metrics.OUTBOX_OLDEST_AGE._value.get() < 310  # noqa: SLF001
    assert metrics.OUTBOX_RECENT_DELAY_P95._value.get() == 0  # noqa: SLF001 - none published

    # Published now, after waiting 1, 3 and 5 minutes: the p95 comes from the
    # database, whichever process did the publishing and whenever it was scraped.
    with engine.begin() as conn:
        conn.execute(text("UPDATE outbox SET published_at = now()"))
    gauges._refresh_outbox()  # noqa: SLF001
    assert 280 <= metrics.OUTBOX_RECENT_DELAY_P95._value.get() < 310  # noqa: SLF001


def test_relay_records_publish_delay(clear_outbox_rows):
    from app.messaging import relay
    from tests.messaging.conftest import RecordingProducer

    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO outbox (event_id, event_type, partition_key, payload, created_at) "
                 "VALUES (:e, 'payment.posted', 'acc_delay', '{}', now() - interval '90 seconds')"),
            {"e": str(uuid.uuid4())},
        )
    histogram = metrics.OUTBOX_PUBLISH_DELAY
    count_before = histogram._sum.get(), sum(b.get() for b in histogram._buckets)  # noqa: SLF001
    assert relay.run_once(RecordingProducer()).published == 1
    total = histogram._sum.get() - count_before[0]  # noqa: SLF001
    assert 90 <= total < 100


@pytest.fixture
def clear_outbox_rows(database):
    def _clear():
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM outbox"))
    _clear()
    yield
    _clear()
