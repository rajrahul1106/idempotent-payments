"""The four invariant checks, on demand and on a schedule (blueprint §9, §10).

    I1  no_duplicate_event_ids  an event_id whose postings span more than one
                                transaction: one event applied twice to the
                                ledger. (processed_events cannot hold a
                                duplicate, its primary key forbids it, so a
                                check there could never fail. This one can.)
    I2  ledger_balances_to_zero SUM(postings.amount) is exactly zero
    I3  outbox_fully_published  no outbox row unpublished for longer than
                                settings.reconciliation_outbox_warn_seconds
    I4  no_negative_balances    no customer account below zero

Outcomes follow §9: I1, I2 and I4 fail (a correctness break: stop the
consumers, then investigate), I3 warns (a backlog, not a broken invariant:
the row is still there and will be published).

All four run in one REPEATABLE READ, read-only transaction, so they see one
snapshot of the database. Each result is then written to
reconciliation_runs in its own short transaction.

The same process also keeps the pipeline gauges current (outbox depth and
age, consumer lag, retry tier depth; see PipelineGauges), because they must
stay visible while the relay or a consumer is down.

Run with: python -m app.services.reconciliation
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from sqlalchemy import text

from app.core import metrics
from app.core.config import settings
from app.core.money import format_amount
from app.core.telemetry import setup_tracing, shutdown_tracing, tracer
from app.db.session import engine, unit_of_work

logger = logging.getLogger(__name__)
_tracer = tracer(__name__)

PASS, WARN, FAIL = "pass", "warn", "fail"
#: reconciliation_status gauge values (§11: 1 pass, 0 fail).
STATUS_VALUE = {PASS: 1.0, WARN: 0.5, FAIL: 0.0}


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: str
    detail: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {"name": self.name, "status": self.status, **self.detail}


@dataclass(frozen=True)
class Report:
    checked_at: dt.datetime
    checks: list[CheckResult]

    @property
    def status(self) -> str:
        statuses = {c.status for c in self.checks}
        return FAIL if FAIL in statuses else WARN if WARN in statuses else PASS

    def as_dict(self) -> dict[str, Any]:
        return {
            "checked_at": self.checked_at.isoformat(),
            "checks": [c.as_dict() for c in self.checks],
        }


# --- the checks: each one query, each one able to fail ---------------------------

_DUPLICATE_EVENTS = text(
    "SELECT count(*) FROM (SELECT event_id FROM postings "
    "GROUP BY event_id HAVING count(DISTINCT transaction_id) > 1) d"
)
_LEDGER_SUM = text("SELECT COALESCE(SUM(amount), 0) FROM postings")
_STALE_OUTBOX = text(
    "SELECT count(*) FROM outbox WHERE published_at IS NULL "
    "AND created_at < now() - make_interval(secs => :seconds)"
)
_NEGATIVE_ACCOUNTS = text(
    "SELECT count(*) FROM (SELECT a.id FROM accounts a "
    "JOIN postings p ON p.account_id = a.id WHERE a.account_type = 'customer' "
    "GROUP BY a.id HAVING SUM(p.amount) < 0) n"
)


def _check_duplicates(conn) -> CheckResult:
    duplicates = conn.execute(_DUPLICATE_EVENTS).scalar_one()
    return CheckResult("no_duplicate_event_ids", FAIL if duplicates else PASS,
                       {"duplicates": duplicates})


def _check_ledger(conn) -> CheckResult:
    total: Decimal = conn.execute(_LEDGER_SUM).scalar_one()
    # format_amount: a string at scale 4. Never a float, even in a report.
    return CheckResult("ledger_balances_to_zero", PASS if total == 0 else FAIL,
                       {"value": format_amount(Decimal(total))})


def _check_outbox(conn) -> CheckResult:
    seconds = settings.reconciliation_outbox_warn_seconds
    stale = conn.execute(_STALE_OUTBOX, {"seconds": seconds}).scalar_one()
    return CheckResult("outbox_fully_published", WARN if stale else PASS,
                       {f"unpublished_older_than_{seconds}s": stale})


def _check_balances(conn) -> CheckResult:
    offending = conn.execute(_NEGATIVE_ACCOUNTS).scalar_one()
    return CheckResult("no_negative_balances", FAIL if offending else PASS,
                       {"offending_accounts": offending})


CHECKS = (_check_duplicates, _check_ledger, _check_outbox, _check_balances)


def run_checks(*, record: bool = True) -> Report:
    """Run I1 to I4 against one snapshot. With `record`, write the results."""
    with _tracer.start_as_current_span("reconciliation run") as span:
        with engine.connect() as conn:
            conn = conn.execution_options(isolation_level="REPEATABLE READ")
            with conn.begin():
                conn.execute(text("SET TRANSACTION READ ONLY"))
                checked_at = conn.execute(text("SELECT now()")).scalar_one()
                report = Report(checked_at=checked_at, checks=[check(conn) for check in CHECKS])
        span.set_attribute("reconciliation.status", report.status)
        if record:
            _record(report)
    for check in report.checks:
        metrics.RECONCILIATION_STATUS.labels(check_name=check.name).set(STATUS_VALUE[check.status])
    metrics.RECONCILIATION_LAST_RUN.set(time.time())
    level = {PASS: logging.INFO, WARN: logging.WARNING, FAIL: logging.ERROR}[report.status]
    logger.log(level, "reconciliation %s: %s", report.status, json.dumps(report.as_dict()["checks"]))
    return report


def _record(report: Report) -> None:
    with unit_of_work() as session:
        for check in report.checks:
            session.execute(
                text(
                    "INSERT INTO reconciliation_runs (check_name, status, detail, ran_at) "
                    "VALUES (:name, :status, CAST(:detail AS jsonb), :ran_at)"
                ),
                {
                    "name": check.name,
                    "status": check.status,
                    "detail": json.dumps(check.detail),
                    "ran_at": report.checked_at,
                },
            )


# --- pipeline gauges: metrics that must not die with what they watch -----------------


class PipelineGauges:
    """Outbox depth and age from Postgres; consumer lag and retry tier depth
    from Kafka's committed offsets. Each source fails on its own: a broker
    outage leaves the outbox gauges updating, and the other way round."""

    def __init__(self) -> None:
        self._lag_reader = None

    def refresh(self) -> None:
        self._refresh_outbox()
        self._refresh_lag()

    def _refresh_outbox(self) -> None:
        from app.db.repositories import outbox as outbox_repo
        from app.db.session import read_session

        try:
            with read_session() as session:
                backlog = outbox_repo.backlog_size(session)
                oldest = outbox_repo.oldest_unpublished_age_seconds(session)
                recent_p95 = outbox_repo.recent_publish_delay_p95(session)
        except Exception as exc:
            logger.warning("outbox gauges not refreshed: %s", exc)
            return
        metrics.OUTBOX_UNPUBLISHED.set(backlog)
        metrics.OUTBOX_OLDEST_AGE.set(oldest or 0)
        metrics.OUTBOX_RECENT_DELAY_P95.set(recent_p95 or 0)
        metrics.PIPELINE_GAUGES_LAST_SUCCESS.labels(source="postgres").set(time.time())

    def _refresh_lag(self) -> None:
        from app.messaging.lag import LagReader, watched_groups

        try:
            if self._lag_reader is None:
                self._lag_reader = LagReader()
            lags = self._lag_reader.read(watched_groups())
        except Exception as exc:
            logger.warning("consumer lag not refreshed: %s", exc)
            self.close()
            return
        depth: dict[str, int] = {}
        for lag in lags:
            metrics.CONSUMER_LAG.labels(
                topic=lag.topic, partition=str(lag.partition), group=lag.group
            ).set(lag.lag)
            if "retry" in lag.topic:
                tier = lag.topic.rsplit(".", 1)[-1]
                depth[tier] = depth.get(tier, 0) + lag.lag
        for tier, value in depth.items():
            metrics.RETRY_TOPIC_DEPTH.labels(tier=tier).set(value)
        metrics.PIPELINE_GAUGES_LAST_SUCCESS.labels(source="kafka").set(time.time())

    def close(self) -> None:
        if self._lag_reader is not None:
            self._lag_reader.close()
            self._lag_reader = None


# --- the scheduled job ---------------------------------------------------------------


def run_every(
    interval_seconds: float,
    stop: threading.Event,
    *,
    gauges: PipelineGauges | None = None,
    gauge_interval_seconds: float | None = None,
) -> None:
    """Run the checks now and then every interval, until stopped. With
    `gauges`, refresh them every gauge interval in between. A failed run is
    logged and the schedule continues: one unreachable database must not
    end it."""
    tick = min(interval_seconds, gauge_interval_seconds or interval_seconds)
    next_check = time.monotonic()
    while not stop.is_set():
        metrics.heartbeat()
        if time.monotonic() >= next_check:
            next_check = time.monotonic() + interval_seconds
            try:
                run_checks()
            except Exception:
                logger.exception("reconciliation run failed")
        if gauges is not None:
            gauges.refresh()
        stop.wait(max(0.0, min(tick, next_check - time.monotonic())))


def main() -> int:
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    stop = threading.Event()

    def _handle_signal(signum, _frame) -> None:
        logger.info("signal %s received, stopping", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    setup_tracing("reconciliation")
    metrics.start_metrics_server("reconciliation")
    logger.info(
        "reconciliation every %ds, pipeline gauges every %ds",
        settings.reconciliation_interval_seconds,
        settings.pipeline_gauges_interval_seconds,
    )
    gauges = PipelineGauges()
    try:
        run_every(
            settings.reconciliation_interval_seconds,
            stop,
            gauges=gauges,
            gauge_interval_seconds=settings.pipeline_gauges_interval_seconds,
        )
    finally:
        gauges.close()
        shutdown_tracing()
    return 0


if __name__ == "__main__":
    sys.exit(main())
