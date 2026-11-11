"""Metric definitions: bounded labels only, and the healthcheck."""

from __future__ import annotations

import http.server
import threading
import time

import pytest
from app.core import metrics
from app.core.healthcheck import check


def test_no_metric_is_labelled_with_a_per_request_identifier():
    offenders = [
        (metric._name, set(metric._labelnames) & metrics.FORBIDDEN_LABELS)  # noqa: SLF001
        for metric in metrics.ALL_METRICS
        if set(metric._labelnames) & metrics.FORBIDDEN_LABELS  # noqa: SLF001
    ]
    assert offenders == []


def test_the_forbidden_list_would_catch_one():
    from prometheus_client import CollectorRegistry, Counter

    bad = Counter("bad_metric", "x", ["event_id"], registry=CollectorRegistry())
    assert set(bad._labelnames) & metrics.FORBIDDEN_LABELS  # noqa: SLF001


def test_every_section_11_metric_is_owned_by_exactly_the_process_that_produces_it():
    owners = {}
    for role, owned in metrics.OWNED.items():
        for metric in owned:
            owners.setdefault(metric._name, set()).add(role)  # noqa: SLF001
    assert owners == {
        "payments": {"api"},
        "payment_duration_seconds": {"api"},
        "idempotency_replays": {"api"},
        "outbox_publish_delay_seconds": {"relay"},
        "relay_lock_held": {"relay"},
        "events_processed": {"consumer", "retry-tier", "dlq-writer"},
        "dlq_messages": {"dlq-writer"},
        # Computed by reconciliation so they outlive the relay and consumers.
        "outbox_unpublished_rows": {"reconciliation"},
        "outbox_oldest_unpublished_age_seconds": {"reconciliation"},
        "outbox_publish_delay_recent_p95_seconds": {"reconciliation"},
        "kafka_consumer_lag": {"reconciliation"},
        "retry_topic_depth": {"reconciliation"},
        "reconciliation_status": {"reconciliation"},
        "reconciliation_last_run_timestamp_seconds": {"reconciliation"},
        "pipeline_gauges_last_success_timestamp_seconds": {"reconciliation"},
    }


# --- healthcheck ---------------------------------------------------------------------


class _Metrics(http.server.BaseHTTPRequestHandler):
    body = b""

    def do_GET(self):  # noqa: N802
        self.send_response(200)
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, *args):
        pass


@pytest.fixture
def serve():
    servers = []

    def _serve(body: str) -> str:
        handler = type("H", (_Metrics,), {"body": body.encode()})
        server = http.server.HTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        return f"http://127.0.0.1:{server.server_port}/metrics"

    yield _serve
    for server in servers:
        server.shutdown()


def test_healthcheck_passes_on_a_fresh_heartbeat(serve):
    assert check(serve(f"process_heartbeat_timestamp_seconds {time.time()}\n"), 60) is None


def test_healthcheck_fails_on_a_stale_heartbeat(serve):
    url = serve(f"process_heartbeat_timestamp_seconds {time.time() - 300}\n")
    assert "old" in check(url, 60)


def test_healthcheck_fails_without_a_heartbeat_or_a_server(serve):
    assert check(serve("something_else 1\n"), 60) == "no heartbeat metric"
    assert check("http://127.0.0.1:1/metrics", 60) is not None
