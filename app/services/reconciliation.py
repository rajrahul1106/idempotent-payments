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

Run every 5 minutes with: python -m app.services.reconciliation
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import signal
import sys
import threading
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from sqlalchemy import text

from app.core.config import settings
from app.core.money import format_amount
from app.core.telemetry import setup_tracing, shutdown_tracing, tracer
from app.db.session import engine, unit_of_work

logger = logging.getLogger(__name__)
_tracer = tracer(__name__)

PASS, WARN, FAIL = "pass", "warn", "fail"


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


# --- the scheduled job ---------------------------------------------------------------


def run_every(interval_seconds: float, stop: threading.Event) -> None:
    """Run now, then every interval, until stopped. A failed run is logged
    and the schedule continues: one unreachable database must not end it."""
    while not stop.is_set():
        try:
            run_checks()
        except Exception:
            logger.exception("reconciliation run failed")
        stop.wait(interval_seconds)


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
    logger.info("reconciliation every %ds", settings.reconciliation_interval_seconds)
    try:
        run_every(settings.reconciliation_interval_seconds, stop)
    finally:
        shutdown_tracing()
    return 0


if __name__ == "__main__":
    sys.exit(main())
