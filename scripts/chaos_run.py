"""Chaos suite (BLUEPRINT.md §10): real failures under load, checked against
a manifest of every request sent.

    python scripts/chaos_run.py --scenario all --events 20000      # make chaos
    python scripts/chaos_run.py --scenario db-restart --events 5000

Scenarios, one run each:
    consumer-kill   two consumers; SIGKILL one at a random point (a rebalance
                    during a busy run)
    broker-outage   stop Redpanda for 60 s mid-load, then start it
    db-restart      restart Postgres mid-load: the API may answer 503, never
                    500, and nothing may go to a retry topic or the DLQ (D5)
    relay-kill      SIGKILL the relay mid-load, then start it again

Load: CHAOS_EVENTS planned operations (default 20,000). 15% resend an earlier
idempotency key with the same body; 1% are malformed messages published
straight to payments.main; new payments spread over many accounts, about
10% of them on one hot account. The client retries a 503 with the same key,
honouring Retry-After, as a well-behaved client would.

A run PASSES only if, after the pipeline drains with no manual repair:
    every idempotency key answered 201 has exactly one transaction
    every payment's event_id has exactly one payment_events row
    the DLQ holds exactly the injected poison messages
    I1 to I4 pass (GET /v1/admin/reconciliation)
    no response was a 500
    nothing was sent to a retry topic
It is INVALID (resource), never FAILED, if the Docker VM's free memory fell
below 300 MB: an OOM is not a correctness result.

Output: docs/results/chaos-<scenario>-<events>-<stamp>.json (summary) and
docs/results/manifests/<same>.jsonl.gz (every request and its responses).
Local stack only.
"""

from __future__ import annotations

import argparse
import collections
import dataclasses
import datetime as dt
import gzip
import json
import os
import pathlib
import random
import re
import statistics
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import httpx
import psycopg

ROOT = pathlib.Path(__file__).resolve().parents[1]
RESULTS = ROOT / "docs" / "results"
API = "http://localhost:8000"
BOOTSTRAP = "localhost:19092"
DSN = "postgresql://payments:payments@localhost:5433/payments"
PROJECT = "idempotent-payments"
SCENARIOS = ("consumer-kill", "broker-outage", "db-restart", "relay-kill")
RETRY_TOPICS = ("payments.retry.5s", "payments.retry.1m", "payments.retry.10m")

MEMORY_FLOOR_MB = 300
MEMORY_SAMPLE_SECONDS = 5
DRAIN_TIMEOUT_SECONDS = 900
MAX_503_RETRIES = 60

FUND_MINOR = 10_000_000  # 100,000.00 INR per account
HOT_FUND_MINOR = 200_000_000


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def sh(*args: str, check: bool = True, timeout: float = 180) -> str:
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout, cwd=ROOT)
    if check and result.returncode != 0:
        raise RuntimeError(f"{' '.join(args)}: {result.stderr.strip()}")
    return result.stdout


def compose(*args: str, check: bool = True) -> str:
    return sh("docker", "compose", *args, check=check)


def token(subject: str, role: str, account_id: str | None = None) -> str:
    from app.core.security import issue_token

    return issue_token(subject, role, account_id=account_id)[0]


# --- the plan ---------------------------------------------------------------------------


@dataclasses.dataclass
class Op:
    index: int
    kind: str  # payment | resend | poison
    key: str | None = None
    source: str | None = None
    destination: str | None = None
    amount_minor: int | None = None
    of: int | None = None  # resend: index of the original
    event_id: str | None = None  # poison
    attempts: list = dataclasses.field(default_factory=list)  # [status, ms, unix]
    final: int | str | None = None
    payment_id: str | None = None
    response_event_id: str | None = None
    acknowledged: bool = False  # poison: broker acknowledged it


def make_plan(events: int, accounts: list[str], hot: str, rng: random.Random) -> list[Op]:
    plan: list[Op] = []
    originals: list[int] = []
    for i in range(events):
        r = rng.random()
        if r < 0.15 and len(originals) > 20:
            j = rng.choice(originals[-2000:])
            o = plan[j]
            plan.append(Op(i, "resend", key=o.key, source=o.source, destination=o.destination,
                           amount_minor=o.amount_minor, of=j))
        elif r < 0.16:
            plan.append(Op(i, "poison", event_id=str(uuid.uuid4())))
        else:
            if rng.random() < 0.10:  # the hot account, half the time paying, half paid
                other = rng.choice(accounts)
                source, destination = (hot, other) if rng.random() < 0.5 else (other, hot)
            else:
                source, destination = rng.sample(accounts, 2)
            plan.append(Op(i, "payment", key=str(uuid.uuid4()), source=source,
                           destination=destination, amount_minor=rng.randint(100, 10_000)))
            originals.append(i)
    return plan


# --- monitoring: memory guard and per-service peaks ----------------------------------------


class Monitor(threading.Thread):
    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.stop_event = threading.Event()
        self.abort = threading.Event()
        self.min_available_mb: float | None = None
        self.samples: list[tuple[float, float]] = []
        self.peak_mb: dict[str, float] = {}

    @staticmethod
    def _vm_available_mb() -> float | None:
        # /proc/meminfo is not namespaced: any container shows the VM's.
        for container in (f"{PROJECT}-prometheus-1", f"{PROJECT}-api-1"):
            try:
                out = sh("docker", "exec", container, "cat", "/proc/meminfo", timeout=10)
            except Exception:
                continue
            m = re.search(r"MemAvailable:\s+(\d+) kB", out)
            if m:
                return int(m.group(1)) / 1024
        return None

    @staticmethod
    def _mib(text: str) -> float:
        n = float(re.match(r"[\d.]+", text).group())
        return n * 1024 if text.endswith("GiB") else n / 1024 if text.endswith("KiB") else n

    def run(self) -> None:
        while not self.stop_event.is_set():
            available = self._vm_available_mb()
            if available is not None:
                self.samples.append((time.time(), available))
                self.min_available_mb = (available if self.min_available_mb is None
                                         else min(self.min_available_mb, available))
                if available < MEMORY_FLOOR_MB and not self.abort.is_set():
                    log(f"MEMORY GUARD: VM free memory {available:.0f} MB < {MEMORY_FLOOR_MB} MB; aborting")
                    self.abort.set()
            try:
                out = sh("docker", "stats", "--no-stream", "--format", "{{.Name}} {{.MemUsage}}",
                         timeout=20)
                for line in out.splitlines():
                    name, usage = line.split(" ", 1)
                    if not name.startswith(PROJECT):
                        continue
                    service = re.sub(rf"^{PROJECT}-|-\d+$", "", name)
                    if name.endswith(tuple(f"-{n}" for n in range(2, 9))):
                        service += name[name.rindex("-"):]  # consumer-2
                    self.peak_mb[service] = max(self.peak_mb.get(service, 0), self._mib(usage.split()[0]))
            except Exception:
                pass
            self.stop_event.wait(MEMORY_SAMPLE_SECONDS)


# --- database and broker probes ------------------------------------------------------------


def query(sql: str, params=None, *, retries: int = 60):
    for attempt in range(retries):
        try:
            with psycopg.connect(DSN, connect_timeout=5) as conn:
                return conn.execute(sql, params).fetchall()
        except psycopg.OperationalError:
            if attempt == retries - 1:
                raise
            time.sleep(2)


def topic_message_counts(topics) -> dict[str, int]:
    from confluent_kafka import Consumer, TopicPartition

    probe = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"chaos-probe-{uuid.uuid4().hex[:6]}"})
    try:
        meta = probe.list_topics(timeout=20)
        return {
            t: sum(probe.get_watermark_offsets(TopicPartition(t, p), timeout=20)[1]
                   for p in meta.topics[t].partitions)
            for t in topics
        }
    finally:
        probe.close()


def group_lag(groups=("payments-ledger", "payments-dlq-writer")) -> dict[str, int]:
    from app.messaging.lag import LagReader, watched_groups

    reader = LagReader(BOOTSTRAP)
    try:
        wanted = {g: t for g, t in watched_groups().items() if g in groups}
        totals: dict[str, int] = collections.Counter()
        for lag in reader.read(wanted):
            totals[lag.group] += lag.lag
        return dict(totals)
    finally:
        reader.close()


# --- setup -------------------------------------------------------------------------------------


def create_accounts(count: int, run_id: str) -> tuple[list[str], str]:
    def one(i: int, fund: int) -> str:
        op = {"Authorization": f"Bearer {token(f'chaos-op-{run_id}-{i}', 'operator')}"}
        with httpx.Client(base_url=API, timeout=30) as c:
            r = c.post("/v1/accounts", headers=op,
                       json={"holder_name": f"chaos {run_id} {i}", "currency": "INR"})
            r.raise_for_status()
            account = r.json()["id"]
            f = c.post("/v1/payments", headers={**op, "Idempotency-Key": str(uuid.uuid4())},
                       json={"from_account_id": "sys_settlement", "to_account_id": account,
                             "amount_minor": fund, "currency": "INR"})
            f.raise_for_status()
            return account

    with ThreadPoolExecutor(max_workers=8) as pool:
        accounts = list(pool.map(lambda i: one(i, FUND_MINOR), range(count)))
    return accounts, one(count, HOT_FUND_MINOR)


# --- traffic ---------------------------------------------------------------------------------


class Traffic:
    def __init__(self, plan: list[Op], concurrency: int, monitor: Monitor) -> None:
        self.plan = plan
        self.concurrency = concurrency
        self.monitor = monitor
        self.next_index = 0
        self.lock = threading.Lock()
        self.done = 0
        self.tokens: dict[tuple[str, int], str] = {}
        self.producer = None
        self.producer_lock = threading.Lock()  # never the plan lock: a broker outage
                                               # must not stall the payment workers

    def _token(self, account: str, slot: int) -> str:
        key = (account, slot % 8)  # several rate-limit buckets per account
        if key not in self.tokens:
            self.tokens[key] = token(f"chaos-{account}-{key[1]}", "client", account_id=account)
        return self.tokens[key]

    def _poison(self, op: Op) -> None:
        from confluent_kafka import Producer

        with self.producer_lock:
            if self.producer is None:
                self.producer = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all",
                                          "message.timeout.ms": 300_000})
        kind = op.index % 2
        value = (json.dumps({"event_id": op.event_id, "event_type": "payment.posted",
                             "schema_version": 99}).encode() if kind
                 else b'{"event_id": "' + op.event_id.encode() + b'", truncated')
        delivered = threading.Event()
        result = {}

        def on_delivery(err, _msg):
            result["err"] = err
            delivered.set()

        with self.producer_lock:
            self.producer.produce("payments.main", key=b"chaos-poison", value=value,
                                  headers=[("event_id", op.event_id.encode())],
                                  on_delivery=on_delivery)
        while not delivered.is_set():
            with self.producer_lock:
                self.producer.poll(0.1)
        op.acknowledged = result.get("err") is None
        op.final = "acknowledged" if op.acknowledged else f"not acknowledged: {result.get('err')}"

    def _payment(self, op: Op, client: httpx.Client, slot: int) -> None:
        body = {"from_account_id": op.source, "to_account_id": op.destination,
                "amount_minor": op.amount_minor, "currency": "INR"}
        headers = {"Authorization": f"Bearer {self._token(op.source, slot)}",
                   "Idempotency-Key": op.key}
        for _ in range(MAX_503_RETRIES + 1):
            started = time.monotonic()
            try:
                r = client.post("/v1/payments", json=body, headers=headers)
                status, payload = r.status_code, (r.json() if r.content else {})
                retry_after = float(r.headers.get("Retry-After", "1"))
            except httpx.HTTPError as exc:
                status, payload, retry_after = type(exc).__name__, {}, 1.0
            op.attempts.append([status, round((time.monotonic() - started) * 1000, 1),
                                round(time.time(), 3)])
            if status in (503, 409) or isinstance(status, str):
                # Busy account, database down (503), or the same key still in
                # flight (409, D2): retry with the SAME key after Retry-After.
                time.sleep(retry_after)
                continue
            op.final = status
            if status in (200, 201):
                op.payment_id = payload.get("payment_id")
                op.response_event_id = payload.get("event_id")
            return
        op.final = "gave_up"

    def worker(self, slot: int) -> None:
        client = httpx.Client(base_url=API, timeout=60)
        try:
            while not self.monitor.abort.is_set():
                with self.lock:
                    if self.next_index >= len(self.plan):
                        return
                    op = self.plan[self.next_index]
                    self.next_index += 1
                if op.kind == "poison":
                    self._poison(op)
                else:
                    self._payment(op, client, slot)
                with self.lock:
                    self.done += 1
        finally:
            client.close()

    def run(self) -> float:
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=self.concurrency) as pool:
            list(pool.map(self.worker, range(self.concurrency)))
        if self.producer is not None:
            self.producer.flush(60)
        return time.monotonic() - started


# --- faults ---------------------------------------------------------------------------------------


class Fault(threading.Thread):
    def __init__(self, scenario: str, traffic: Traffic, at_fraction: float) -> None:
        super().__init__(daemon=True)
        self.scenario = scenario
        self.traffic = traffic
        self.at = int(len(traffic.plan) * at_fraction)
        self.log: list[tuple[str, float]] = []
        self.error: str | None = None

    def mark(self, what: str) -> None:
        self.log.append((what, time.time()))
        log(f"FAULT {what}")

    def run(self) -> None:
        try:
            while self.traffic.done < self.at and not self.traffic.monitor.abort.is_set():
                time.sleep(0.2)
            if self.traffic.monitor.abort.is_set():
                return
            getattr(self, self.scenario.replace("-", "_"))()
        except Exception as exc:  # recorded, and the run fails
            self.error = repr(exc)
            log(f"FAULT ERROR {exc!r}")

    def consumer_kill(self) -> None:
        victims = sorted(sh("docker", "ps", "--filter", f"label=com.docker.compose.service=consumer",
                            "--filter", f"label=com.docker.compose.project={PROJECT}",
                            "--format", "{{.Names}}").split())
        victim = random.Random(self.at).choice(victims)
        self.mark(f"SIGKILL {victim} (of {len(victims)} consumers)")
        sh("docker", "kill", "--signal", "KILL", victim)
        self.mark("killed")

    def broker_outage(self) -> None:
        self.mark("stop redpanda")
        compose("stop", "redpanda")
        time.sleep(60)
        self.mark("start redpanda")
        compose("start", "redpanda")
        self.mark("restored")

    def db_restart(self) -> None:
        self.mark("restart postgres")
        compose("restart", "postgres")
        self.mark("restored")

    def relay_kill(self) -> None:
        self.mark("SIGKILL relay")
        sh("docker", "kill", "--signal", "KILL", f"{PROJECT}-relay-1")
        time.sleep(10)
        self.mark("start relay")
        compose("start", "relay")
        self.mark("restored")


# --- drain and verification ----------------------------------------------------------------------------


def wait_for_drain(event_ids: list[str], poison_ids: list[str], since: dt.datetime,
                   monitor: Monitor) -> tuple[bool, float]:
    started = time.monotonic()
    while time.monotonic() - started < DRAIN_TIMEOUT_SECONDS:
        if monitor.abort.is_set():
            return False, time.monotonic() - started
        try:
            unpublished = query("SELECT count(*) FROM outbox WHERE published_at IS NULL")[0][0]
            projected = query("SELECT count(*) FROM payment_events WHERE event_id = ANY(%s::uuid[])",
                              (event_ids,))[0][0]
            dead = query("SELECT count(DISTINCT event_id) FROM dlq_messages WHERE created_at >= %s "
                         "AND event_id = ANY(%s::uuid[])", (since, poison_ids))[0][0]
            lags = group_lag()
        except Exception as exc:
            log(f"drain probe: {exc!r}")
            time.sleep(5)
            continue
        if (unpublished == 0 and projected == len(event_ids) and dead == len(poison_ids)
                and all(v == 0 for v in lags.values())):
            return True, time.monotonic() - started
        time.sleep(3)
    return False, time.monotonic() - started


def recovery_seconds(fault: Fault, plan: list[Op]) -> float | None:
    """Seconds from the fault's restore action (or, for a consumer kill, the
    kill) until every payment committed before it had been applied end to
    end. 0 when nothing was left waiting. From database timestamps only."""
    restored = next((t for what, t in reversed(fault.log) if what in ("restored", "killed")), None)
    if restored is None:
        return None
    payment_ids = [o.payment_id for o in plan if o.payment_id]
    last = query(
        "SELECT max(pe.applied_at) FROM payment_events pe JOIN transactions t "
        "ON t.id = pe.transaction_id WHERE t.id = ANY(%s) AND t.created_at <= to_timestamp(%s)",
        (payment_ids, restored))[0][0]
    if last is None:
        return None
    return round(max(0.0, last.timestamp() - restored), 1)


def verify(plan, accounts, hot, poison_ids, since, retry_before, funded) -> dict:
    checks: dict[str, dict] = {}
    payments = [o for o in plan if o.kind in ("payment", "resend")]
    statuses = collections.Counter(a[0] for o in payments for a in o.attempts)
    checks["zero_500_responses"] = {"pass": statuses.get(500, 0) == 0, "500s": statuses.get(500, 0)}

    # 201 per key -> exactly one transaction, and every replay names it.
    created = {}
    for o in payments:
        if o.final in (200, 201) and o.payment_id:
            created.setdefault(o.key, set()).add(o.payment_id)
    keys_201 = {o.key for o in payments if any(a[0] == 201 for a in o.attempts)}
    split = {k: v for k, v in created.items() if len(v) != 1}
    rows = dict(query("SELECT key::text, transaction_id FROM idempotency_keys "
                      "WHERE key = ANY(%s::uuid[])", (list(keys_201),)))
    mismatched = [k for k in keys_201 if rows.get(k) not in created.get(k, set())]
    run_accounts = [*accounts, hot]
    tx_count = query("SELECT count(DISTINCT transaction_id) FROM postings WHERE account_id = ANY(%s)",
                     (run_accounts,))[0][0]
    checks["one_transaction_per_201_key"] = {
        "pass": not split and not mismatched and tx_count == funded + len(keys_201),
        "keys_answered_201": len(keys_201), "transactions_for_run_accounts": tx_count,
        "funding_transactions": funded, "keys_with_two_payment_ids": len(split),
        "keys_whose_stored_transaction_differs": len(mismatched),
    }

    event_ids = sorted({o.response_event_id for o in payments if o.response_event_id})
    per_event = query("SELECT count(*), count(DISTINCT event_id) FROM payment_events "
                      "WHERE event_id = ANY(%s::uuid[])", (event_ids,))[0]
    checks["one_payment_events_row_per_event"] = {
        "pass": per_event[0] == per_event[1] == len(event_ids),
        "events": len(event_ids), "rows": per_event[0]}

    dlq = query("SELECT event_id::text FROM dlq_messages WHERE created_at >= %s", (since,))
    dlq_ids = [r[0] for r in dlq]
    checks["dlq_is_exactly_the_poison"] = {
        "pass": sorted(dlq_ids) == sorted(poison_ids),
        "injected": len(poison_ids), "dlq_rows": len(dlq_ids),
        "unexpected": len(set(dlq_ids) - set(poison_ids)),
        "missing": len(set(poison_ids) - set(dlq_ids))}

    retry_after = topic_message_counts(RETRY_TOPICS)
    sent = {t: retry_after[t] - retry_before[t] for t in RETRY_TOPICS}
    checks["nothing_sent_to_retry_topics"] = {"pass": not any(sent.values()), "messages": sent}

    # Balances: the arithmetic of every distinct 201, per run account.
    expected = {a: Decimal(FUND_MINOR) / 100 for a in accounts}
    expected[hot] = Decimal(HOT_FUND_MINOR) / 100
    seen = set()
    for o in payments:
        if o.key in keys_201 and o.key not in seen:
            seen.add(o.key)
            expected[o.source] -= Decimal(o.amount_minor) / 100
            expected[o.destination] += Decimal(o.amount_minor) / 100
    actual = dict(query("SELECT account_id, sum(amount) FROM postings WHERE account_id = ANY(%s) "
                        "GROUP BY account_id", (run_accounts,)))
    off = {a: [str(expected[a]), str(actual.get(a))] for a in run_accounts
           if actual.get(a) != expected[a]}
    checks["balances_match_the_arithmetic"] = {"pass": not off, "accounts": len(run_accounts),
                                               "mismatched": off}

    op = {"Authorization": f"Bearer {token('chaos-verify', 'operator')}"}
    recon = httpx.get(f"{API}/v1/admin/reconciliation", headers=op, timeout=60).json()
    checks["invariants_I1_to_I4"] = {"pass": all(c["status"] == "pass" for c in recon["checks"]),
                                     "checks": recon["checks"]}
    return checks


# --- one run ----------------------------------------------------------------------------------------------


def percentile(values, q):
    if not values:
        return None
    values = sorted(values)
    return round(values[min(len(values) - 1, int(round(q * (len(values) - 1))))], 1)


def run_scenario(scenario: str, events: int, concurrency: int, seed: int, accounts_n: int) -> dict:
    run_id = f"{scenario}-{events}-{time.strftime('%Y%m%dT%H%M%S')}"
    log(f"=== {run_id} ===")
    rng = random.Random(seed)
    preflight(scenario)

    accounts, hot = create_accounts(accounts_n, run_id)
    funded = accounts_n + 1
    since = query("SELECT now()")[0][0]
    retry_before = topic_message_counts(RETRY_TOPICS)
    plan = make_plan(events, accounts, hot, rng)
    poison_ids = [o.event_id for o in plan if o.kind == "poison"]

    monitor = Monitor()
    monitor.start()
    traffic = Traffic(plan, concurrency, monitor)
    fault = Fault(scenario, traffic, at_fraction=rng.uniform(0.25, 0.6))
    fault.start()
    log(f"traffic: {events} ops, {accounts_n} accounts + hot, {len(poison_ids)} poison, "
        f"fault at op {fault.at}")
    traffic_seconds = traffic.run()
    fault.join(timeout=180)
    log(f"traffic done in {traffic_seconds:.0f}s; draining")

    acknowledged_poison = [o.event_id for o in plan if o.kind == "poison" and o.acknowledged]
    event_ids = sorted({o.response_event_id for o in plan if o.response_event_id})
    drained, drain_seconds = wait_for_drain(event_ids, acknowledged_poison, since, monitor)
    monitor.stop_event.set()
    monitor.join(timeout=30)
    if scenario == "consumer-kill":
        compose("up", "-d", "--no-deps", "--scale", "consumer=1", "consumer")

    payments = [o for o in plan if o.kind != "poison"]
    attempts = [a for o in payments for a in o.attempts]
    latencies = [a[1] for a in attempts if isinstance(a[0], int)]
    statuses = collections.Counter(str(a[0]) for a in attempts)
    finals = collections.Counter(str(o.final) for o in payments)
    db_503 = [a[2] for a in attempts if a[0] == 503]

    if monitor.abort.is_set():
        verdict, checks = "INVALID (resource)", {}
    else:
        checks = verify(plan, accounts, hot, acknowledged_poison, since, retry_before, funded)
        checks["drained_without_manual_repair"] = {"pass": drained,
                                                   "seconds_after_traffic": round(drain_seconds, 1)}
        checks["fault_executed"] = {"pass": fault.error is None and bool(fault.log),
                                    "error": fault.error}
        verdict = "PASS" if all(c["pass"] for c in checks.values()) else "FAILED"

    result = {
        "run": run_id, "scenario": scenario, "verdict": verdict, "seed": seed,
        "events_planned": events, "concurrency": concurrency,
        "machine": machine(),
        "load": {
            "payment_requests": sum(o.kind == "payment" for o in plan),
            "resends_same_key_same_body": sum(o.kind == "resend" for o in plan),
            "poison_published": len(acknowledged_poison),
            "hot_account_share": round(sum(hot in (o.source, o.destination) for o in payments)
                                       / max(1, len(payments)), 3),
            "accounts": accounts_n + 1,
        },
        "results": {
            "traffic_seconds": round(traffic_seconds, 1),
            "throughput_requests_per_s": round(len(attempts) / traffic_seconds, 1),
            "throughput_payments_created_per_s": round(
                sum(1 for o in payments if o.kind == "payment" and o.final == 201) / traffic_seconds, 1),
            "latency_ms": {"p50": percentile(latencies, 0.50), "p95": percentile(latencies, 0.95),
                           "p99": percentile(latencies, 0.99)},
            "http_attempt_statuses": dict(statuses),
            "final_statuses": dict(finals),
            "count_503": len(db_503),
            "window_503_seconds": round(max(db_503) - min(db_503), 1) if db_503 else 0,
            "recovery_seconds_after_fault": recovery_seconds(fault, plan),
            "drain_seconds_after_traffic": round(drain_seconds, 1),
            "fault_timeline": [[w, dt.datetime.fromtimestamp(t, dt.timezone.utc).isoformat()]
                               for w, t in fault.log],
            "vm_min_available_mb": round(monitor.min_available_mb or 0),
            "peak_memory_mb": {k: round(v) for k, v in sorted(monitor.peak_mb.items())},
        },
        "checks": checks,
    }
    write(run_id, result, plan)
    log(f"=== {run_id}: {verdict} ===")
    return result


def preflight(scenario: str) -> None:
    """A clean starting line: the whole stack up, nothing left in flight."""
    compose("up", "-d")
    if scenario == "consumer-kill":
        compose("up", "-d", "--no-deps", "--scale", "consumer=2", "consumer")
    deadline = time.monotonic() + 240
    while time.monotonic() < deadline:
        try:
            if (httpx.get(f"{API}/health", timeout=5).status_code == 200
                    and query("SELECT count(*) FROM outbox WHERE published_at IS NULL")[0][0] == 0
                    and all(v == 0 for v in group_lag().values())):
                break
        except Exception:
            pass
        time.sleep(3)
    else:
        raise RuntimeError("stack not ready or not drained before the run")
    time.sleep(15 if scenario == "consumer-kill" else 3)  # consumers settle into the group


def machine() -> dict:
    vm_total = None
    try:
        out = sh("docker", "info", "--format", "{{.MemTotal}} {{.NCPU}}")
        total, cpus = out.split()
        vm_total = {"docker_vm_memory_gb": round(int(total) / 2**30, 2), "docker_vm_cpus": int(cpus)}
    except Exception:
        pass
    return {
        "host": "MacBook Air, Apple M2, 8 GB",
        "host_cpu": sh("sysctl", "-n", "machdep.cpu.brand_string", check=False).strip(),
        "host_memory_gb": round(int(sh("sysctl", "-n", "hw.memsize", check=False) or 0) / 2**30),
        **(vm_total or {}),
    }


def write(run_id: str, result: dict, plan: list[Op]) -> None:
    (RESULTS / "manifests").mkdir(parents=True, exist_ok=True)
    manifest = RESULTS / "manifests" / f"chaos-{run_id}.jsonl.gz"
    with gzip.open(manifest, "wt") as out:
        for op in plan:
            out.write(json.dumps(dataclasses.asdict(op), separators=(",", ":")) + "\n")
    result["manifest"] = str(manifest.relative_to(ROOT))
    (RESULTS / f"chaos-{run_id}.json").write_text(json.dumps(result, indent=2, default=str) + "\n")


def table(results: list[dict]) -> str:
    rows = ["| Scenario | Events | Verdict | Req/s | p50 ms | p95 ms | p99 ms | 503s | 500s | "
            "Recovery s | Drain s | VM min free MB |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    for r in results:
        x = r["results"]
        rows.append(
            f"| {r['scenario']} | {r['events_planned']:,} | {r['verdict']} | "
            f"{x['throughput_requests_per_s']} | {x['latency_ms']['p50']} | {x['latency_ms']['p95']} | "
            f"{x['latency_ms']['p99']} | {x['count_503']} | {x['http_attempt_statuses'].get('500', 0)} | "
            f"{x['recovery_seconds_after_fault']} | {x['drain_seconds_after_traffic']} | "
            f"{x['vm_min_available_mb']} |")
    return "\n".join(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", default="all", choices=("all", *SCENARIOS))
    parser.add_argument("--events", type=int, default=int(os.environ.get("CHAOS_EVENTS", 20_000)))
    parser.add_argument("--concurrency", type=int, default=int(os.environ.get("CHAOS_CONCURRENCY", 16)))
    parser.add_argument("--accounts", type=int, default=200)
    parser.add_argument("--seed", type=int, default=int(os.environ.get("CHAOS_SEED", 8)))
    args = parser.parse_args()

    scenarios = SCENARIOS if args.scenario == "all" else (args.scenario,)
    results = []
    for i, scenario in enumerate(scenarios):
        results.append(run_scenario(scenario, args.events, args.concurrency, args.seed + i,
                                    args.accounts))
    print()
    print(table(results))
    for r in results:
        failed = [name for name, c in r["checks"].items() if not c["pass"]]
        if failed:
            print(f"{r['scenario']}: failed checks: {', '.join(failed)}")
    return 0 if all(r["verdict"] == "PASS" for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
