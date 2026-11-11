"""Prometheus metrics, all defined here (blueprint §11).

Each process serves its own /metrics: the API on its HTTP port, every other
process on settings.metrics_port through start_metrics_server.

Two rules:

1. A metric must not die with the process it watches. Outbox depth, the
   oldest unpublished row's age, consumer lag and retry tier depth are
   computed by the reconciliation process, from Postgres and from Kafka's
   committed offsets, so they stay visible while the relay or a consumer
   is down.

2. Labels are low-cardinality and bounded: a status, a topic, a check name.
   Never an event_id, account_id, idempotency key, trace id or anything
   else that grows with traffic. tests/unit/test_metrics.py fails if one
   appears.
"""

from __future__ import annotations

import time

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    disable_created_metrics,
    start_http_server,
)
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import REGISTRY, Collector

# The *_created series double every counter for no alerting value.
disable_created_metrics()

#: Label names that must never appear on any metric. Checked by a test.
FORBIDDEN_LABELS = frozenset(
    {
        "event_id",
        "account_id",
        "idempotency_key",
        "trace_id",
        "span_id",
        "transaction_id",
        "payment_id",
        "user",
        "username",
        "key",
    }
)

LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)
DELAY_BUCKETS = (0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800)

# --- api ---------------------------------------------------------------------------

PAYMENTS = Counter(
    "payments",
    "POST /v1/payments outcomes. status: created, replayed, or the error code.",
    ["status", "currency"],
    registry=None,
)
REQUEST_DURATION = Histogram(
    "payment_duration_seconds",
    "HTTP request duration by route template.",
    ["endpoint"],
    buckets=LATENCY_BUCKETS,
    registry=None,
)
IDEMPOTENCY_REPLAYS = Counter(
    "idempotency_replays",
    "Requests that reused an idempotency key. outcome: replayed or mismatch.",
    ["outcome"],
    registry=None,
)

# --- relay -------------------------------------------------------------------------

OUTBOX_PUBLISH_DELAY = Histogram(
    "outbox_publish_delay_seconds",
    "Commit of the outbox row to broker acknowledgement.",
    buckets=DELAY_BUCKETS,
    registry=None,
)
RELAY_LOCK_HELD = Gauge("relay_lock_held", "1 while this relay holds the D1 advisory lock.", registry=None)

# --- consumers ---------------------------------------------------------------------

EVENTS_PROCESSED = Counter(
    "events_processed",
    "Messages handled, by the topic they were read from. outcome: applied, skipped, "
    "retried, dead_lettered, held, forwarded, written.",
    ["topic", "outcome"],
    registry=None,
)
DLQ_MESSAGES = Counter(
    "dlq_messages",
    "Dead letters recorded by the DLQ writer. reason: the failure kind "
    "(permanent, transient, unknown), never the error text.",
    ["reason"],
    registry=None,
)

# --- computed by the reconciliation process ----------------------------------------

OUTBOX_UNPUBLISHED = Gauge("outbox_unpublished_rows", "Outbox rows not yet published.", registry=None)
OUTBOX_OLDEST_AGE = Gauge(
    "outbox_oldest_unpublished_age_seconds",
    "Age of the oldest unpublished outbox row; 0 when there is none.",
    registry=None,
)
OUTBOX_RECENT_DELAY_P95 = Gauge(
    "outbox_publish_delay_recent_p95_seconds",
    "p95 of published_at - created_at over rows published in the last 5 minutes, from "
    "Postgres. Unlike the relay's histogram it does not miss a backlog the relay drained "
    "before its first scrape. 0 when nothing was published.",
    registry=None,
)
CONSUMER_LAG = Gauge(
    "kafka_consumer_lag",
    "High watermark minus committed offset, per partition and group.",
    ["topic", "partition", "group"],
    registry=None,
)
RETRY_TOPIC_DEPTH = Gauge(
    "retry_topic_depth", "Messages waiting in a retry tier (its group's total lag).", ["tier"],
    registry=None,
)
RECONCILIATION_STATUS = Gauge(
    "reconciliation_status", "Last result per check: 1 pass, 0.5 warn, 0 fail.", ["check_name"],
    registry=None,
)
RECONCILIATION_LAST_RUN = Gauge(
    "reconciliation_last_run_timestamp_seconds", "When the invariant checks last completed.",
    registry=None,
)
PIPELINE_GAUGES_LAST_SUCCESS = Gauge(
    "pipeline_gauges_last_success_timestamp_seconds",
    "When outbox and lag gauges were last refreshed. source: postgres or kafka.",
    ["source"],
    registry=None,
)

# --- every process -----------------------------------------------------------------

HEARTBEAT = Gauge(
    "process_heartbeat_timestamp_seconds",
    "Last time this process's main loop went round. The Compose healthcheck reads it.",
    registry=None,
)


# --- ownership: a process exposes only the metrics it is the source of -------------
#
# Every process imports this module, so a metric registered at import would
# be exposed by all of them, mostly as a meaningless 0: outbox_unpublished_rows
# from the API, reconciliation_last_run_timestamp_seconds from the relay.
# Metrics are created unregistered, and each process registers its own set.

OWNED = {
    "api": (PAYMENTS, REQUEST_DURATION, IDEMPOTENCY_REPLAYS),
    "relay": (OUTBOX_PUBLISH_DELAY, RELAY_LOCK_HELD),
    "consumer": (EVENTS_PROCESSED,),
    "retry-tier": (EVENTS_PROCESSED,),
    "dlq-writer": (EVENTS_PROCESSED, DLQ_MESSAGES),
    "reconciliation": (
        OUTBOX_UNPUBLISHED,
        OUTBOX_OLDEST_AGE,
        OUTBOX_RECENT_DELAY_P95,
        CONSUMER_LAG,
        RETRY_TOPIC_DEPTH,
        RECONCILIATION_STATUS,
        RECONCILIATION_LAST_RUN,
        PIPELINE_GAUGES_LAST_SUCCESS,
    ),
}
ALL_METRICS = (*{id(m): m for group in OWNED.values() for m in group}.values(), HEARTBEAT)

_registered: set[int] = set()


def register(role: str) -> None:
    """Expose this process's metrics: its role's, plus the heartbeat and the
    database pool. Idempotent."""
    for metric in (*OWNED[role], HEARTBEAT):
        if id(metric) not in _registered:
            REGISTRY.register(metric)
            _registered.add(id(metric))
    register_pool_metrics()


def _initialise_series() -> None:
    """Create the series the alerts watch, at zero, before anything happens.

    increase() and rate() need two samples. A counter whose series first
    appears already at 20 shows no increase at all, so a DLQ that receives
    its first 20 messages between two scrapes would never alert.
    """
    for reason in ("permanent", "transient", "unknown"):
        DLQ_MESSAGES.labels(reason=reason)
    for status in ("internal_error", "service_unavailable"):
        PAYMENTS.labels(status=status, currency="none")
    IDEMPOTENCY_REPLAYS.labels(outcome="mismatch")
    IDEMPOTENCY_REPLAYS.labels(outcome="replayed")


_initialise_series()


def heartbeat() -> None:
    HEARTBEAT.set(time.time())


class _PoolCollector(Collector):
    """db_pool_connections{state} and db_pool_capacity, read at scrape time."""

    def collect(self):
        from app.db.session import engine

        pool = engine.pool
        in_use = GaugeMetricFamily(
            "db_pool_connections", "This process's SQLAlchemy pool.", labels=["state"]
        )
        in_use.add_metric(["in_use"], pool.checkedout())
        in_use.add_metric(["idle"], pool.checkedin())
        yield in_use
        capacity = GaugeMetricFamily(
            "db_pool_capacity", "pool_size plus max_overflow: the most connections it opens."
        )
        capacity.add_metric([], pool.size() + pool._max_overflow)  # noqa: SLF001
        yield capacity


_pool_registered = False


def register_pool_metrics() -> None:
    global _pool_registered
    if not _pool_registered:
        REGISTRY.register(_PoolCollector())
        _pool_registered = True


def start_metrics_server(role: str, port: int | None = None) -> None:
    """Serve /metrics for a process that is not the API."""
    from app.core.config import settings

    register(role)
    heartbeat()
    start_http_server(port or settings.metrics_port)
