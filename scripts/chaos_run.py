"""Chaos suite (docs/ARCHITECTURE.md §10): real failures under load, checked against
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
    db-outage       stop Postgres for 60 s mid-load (compose stop, then
                    start): the API must answer 503 throughout the outage,
                    never 500, and nothing may go to a retry topic or the DLQ

Load: CHAOS_EVENTS planned operations (default 20,000). 15% resend an earlier
idempotency key with the same body; 1% are malformed messages published
straight to payments.main; new payments spread over many accounts, about
10% of them on one hot account. The client retries a 503 with the same key,
honouring Retry-After, as a well-behaved client would.

A run PASSES only if, after the pipeline drains with no manual repair:
    every idempotency key the client was paid for (201, or a 200 replay when
    the 201 was lost to the fault) has exactly one transaction
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
# Compose names the project after the checkout's directory (lowercased,
# other characters dropped) unless COMPOSE_PROJECT_NAME says otherwise.
PROJECT = os.environ.get("COMPOSE_PROJECT_NAME") or re.sub(
    r"[^a-z0-9_-]", "", pathlib.Path(__file__).resolve().parent.parent.name.lower()
)
# db-outage is last so the first four keep the seeds they had in Phase 8.
SCENARIOS = ("consumer-kill", "broker-outage", "db-restart", "relay-kill", "db-outage")
RETRY_TOPICS = ("payments.retry.5s", "payments.retry.1m", "payments.retry.10m")

MEMORY_FLOOR_MB = 300
MEMORY_SAMPLE_SECONDS = 5
DRAIN_TIMEOUT_SECONDS = 900
# A well-behaved client keeps retrying a 503 with the same key. 300 retries
# at Retry-After: 1 outlast a 60 s database outage with room to spare; 60
# did not (each 503 comes back in milliseconds).
MAX_503_RETRIES = 300
DB_OUTAGE_SECONDS = 60

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
        self.cpu_samples: dict[str, list[float]] = {}  # docker stats CPU %, 100 = one core

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
                out = sh("docker", "stats", "--no-stream", "--format",
                         "{{.Name}} {{.CPUPerc}} {{.MemUsage}}", timeout=20)
                for line in out.splitlines():
                    name, cpu, usage = line.split(" ", 2)
                    if not name.startswith(PROJECT):
                        continue
                    service = service_of(name)
                    self.peak_mb[service] = max(self.peak_mb.get(service, 0), self._mib(usage.split()[0]))
                    self.cpu_samples.setdefault(service, []).append(float(cpu.rstrip("%") or 0))
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


# --- CPU, timing and thermal probes ---------------------------------------------------------


def service_of(container: str) -> str:
    service = re.sub(rf"^{PROJECT}-|-\d+$", "", container)
    if container.endswith(tuple(f"-{n}" for n in range(2, 9))):
        service += container[container.rindex("-"):]  # consumer-2
    return service


def vm_cpu_seconds() -> tuple[float, float] | None:
    """(busy, total) CPU seconds of the whole Docker VM since it booted.
    /proc/stat is not namespaced, so any container shows the VM's."""
    try:
        line = sh("docker", "exec", f"{PROJECT}-prometheus-1", "head", "-1", "/proc/stat",
                  timeout=10).split()
    except Exception:
        return None
    ticks = [int(v) for v in line[1:9]]  # user nice system idle iowait irq softirq steal
    total = sum(ticks)
    return (total - ticks[3] - ticks[4]) / 100, total / 100


def container_cpu_seconds() -> dict[str, float]:
    """CPU seconds each container has used, from its own cgroup. A container
    that restarted starts again from 0."""
    names = sh("docker", "ps", "--filter", f"label=com.docker.compose.project={PROJECT}",
               "--format", "{{.Names}}").split()

    def one(name: str):
        try:
            out = sh("docker", "exec", name, "cat", "/sys/fs/cgroup/cpu.stat", timeout=10)
            return service_of(name), int(re.search(r"usage_usec (\d+)", out).group(1)) / 1e6
        except Exception:
            return service_of(name), None

    with ThreadPoolExecutor(max_workers=8) as pool:
        return {k: v for k, v in pool.map(one, names) if v is not None}


def cpu_delta(before: dict, after: dict) -> dict[str, float | None]:
    return {k: (round(after[k] - before[k], 1) if k in before and after[k] >= before[k] else None)
            for k in sorted(after)}


def cpu_benchmark() -> dict | None:
    try:
        return json.loads(sh(sys.executable, str(ROOT / "scripts" / "cpu_bench.py"), timeout=120))
    except Exception as exc:
        return {"error": repr(exc)}


def host_state() -> dict:
    batt = sh("pmset", "-g", "batt", check=False).splitlines()
    therm = [line.strip() for line in sh("pmset", "-g", "therm", check=False).splitlines()
             if line.strip()]
    low_power = re.search(r"lowpowermode\s+(\d)", sh("pmset", "-g", check=False))
    return {
        "power_source": batt[0].split("'")[1] if batt and "'" in batt[0] else None,
        "battery": batt[1].split("\t")[-1].strip() if len(batt) > 1 else None,
        "low_power_mode": (low_power.group(1) == "1") if low_power else None,
        "pmset_therm": therm,
        "load_average": sh("sysctl", "-n", "vm.loadavg", check=False).strip(),
    }


def utc(t: float) -> str:
    return dt.datetime.fromtimestamp(t, dt.timezone.utc).isoformat(timespec="seconds")


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
    def __init__(self, plan: list[Op], concurrency: int, monitor: Monitor, *,
                 buckets: int = 8, deadline: float | None = None,
                 subject_per_op: bool = False) -> None:
        self.plan = plan
        self.concurrency = concurrency
        self.monitor = monitor
        self.buckets = buckets  # rate-limit subjects per account
        self.deadline = deadline  # time.monotonic() after which no new op starts
        # The API rate-limits per token subject (100 a minute). By default a
        # worker slot keeps its subjects; per op spreads one account's
        # requests over all `buckets` subjects whatever the concurrency, so
        # a few clients do not trip the limit on the hot account.
        self.subject_per_op = subject_per_op
        self.next_index = 0
        self.lock = threading.Lock()
        self.done = 0
        self.tokens: dict[tuple[str, int], str] = {}
        self.producer = None
        self.producer_lock = threading.Lock()  # never the plan lock: a broker outage
                                               # must not stall the payment workers

    def _token(self, account: str, slot: int) -> str:
        key = (account, slot % self.buckets)  # several rate-limit buckets per account
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
        subject = op.index if self.subject_per_op else slot
        headers = {"Authorization": f"Bearer {self._token(op.source, subject)}",
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
                if self.deadline is not None and time.monotonic() >= self.deadline:
                    return
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
        self.containers_during_outage: list[str] = []

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
        victims = sorted(sh("docker", "ps", "--filter", "label=com.docker.compose.service=consumer",
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

    def db_outage(self) -> None:
        self.mark("stop postgres")
        compose("stop", "postgres")
        self.mark("stopped")
        self.containers_during_outage = sh(
            "docker", "compose", "ps", "--all", "--format", "{{.Service}} {{.State}}").split("\n")
        time.sleep(DB_OUTAGE_SECONDS)
        self.mark("start postgres")
        compose("start", "postgres")
        self.mark("started")
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            try:
                with psycopg.connect(DSN, connect_timeout=2) as conn:
                    conn.execute("SELECT 1")
                break
            except psycopg.OperationalError:
                time.sleep(0.2)
        self.mark("restored")  # Postgres accepts connections again

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
    # A key the client got a payment for: answered 201, or answered only by a
    # 200 replay because the 201 was lost to the fault (the commit landed,
    # the connection did not survive to carry the response). Each needs
    # exactly one transaction, and every transaction must belong to one.
    keys_paid = {o.key for o in payments if o.final in (200, 201) and o.payment_id}
    split = {k: v for k, v in created.items() if len(v) != 1}
    rows = dict(query("SELECT key::text, transaction_id FROM idempotency_keys "
                      "WHERE key = ANY(%s::uuid[])", (list(keys_paid),)))
    mismatched = [k for k in keys_paid if rows.get(k) not in created.get(k, set())]
    run_accounts = [*accounts, hot]
    tx_count = query("SELECT count(DISTINCT transaction_id) FROM postings WHERE account_id = ANY(%s)",
                     (run_accounts,))[0][0]
    checks["one_transaction_per_201_key"] = {
        "pass": not split and not mismatched and tx_count == funded + len(keys_paid),
        "keys_answered_201": len(keys_201),
        "keys_paid_but_only_answered_by_replay": len(keys_paid - keys_201),
        "transactions_for_run_accounts": tx_count,
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
        if o.key in keys_paid and o.key not in seen:
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


def outage_checks(fault: Fault, plan: list[Op]) -> dict:
    """db-outage only: every request made while Postgres was stopped got 503,
    and how long after Postgres came back the API created a payment again."""
    marks = dict(fault.log)
    stopped, starting, restored = marks.get("stopped"), marks.get("start postgres"), marks.get("restored")
    if None in (stopped, starting, restored):
        return {"api_503_throughout_outage": {"pass": False, "error": "outage marks missing",
                                              "marks": list(marks)}}
    attempts = [a for o in plan if o.kind != "poison" for a in o.attempts]
    began = lambda a: a[2] - a[1] / 1000  # noqa: E731
    inside = [a for a in attempts if began(a) >= stopped and a[2] <= starting]
    spanning = [a for a in attempts if began(a) < stopped <= a[2]]
    statuses = collections.Counter(str(a[0]) for a in inside)
    created_after = [a[2] for a in attempts if a[0] == 201 and began(a) >= restored]
    first_201 = min(created_after) if created_after else None
    return {
        "api_503_throughout_outage": {
            "pass": bool(inside) and set(statuses) == {"503"},
            "outage_seconds": round(starting - stopped, 1),
            "requests_during_outage": len(inside), "statuses_during_outage": dict(statuses),
            "in_flight_when_postgres_stopped": dict(collections.Counter(str(a[0]) for a in spanning)),
            "containers_during_outage": [c for c in fault.containers_during_outage if c],
        },
        "api_recovered_after_postgres_returned": {
            "pass": first_201 is not None,
            "postgres_start_to_accepting_s": round(restored - marks["started"], 1),
            "first_201_after_postgres_accepting_s": (round(first_201 - restored, 2)
                                                     if first_201 else None),
        },
    }


def run_scenario(scenario: str, events: int, concurrency: int, seed: int, accounts_n: int,
                 round_no: int | None = None) -> dict:
    run_id = f"{scenario}-{events}-{time.strftime('%Y%m%dT%H%M%S')}"
    log(f"=== {run_id} ===")
    started_at = time.time()
    rng = random.Random(seed)
    host_before = host_state()
    preflight(scenario)
    bench_before = cpu_benchmark()

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
    cpu_before, vm_before, client_before = container_cpu_seconds(), vm_cpu_seconds(), time.process_time()
    traffic_started = time.time()
    traffic_seconds = traffic.run()
    traffic_ended = time.time()
    cpu_after, vm_after, client_after = container_cpu_seconds(), vm_cpu_seconds(), time.process_time()
    fault.join(timeout=180)
    log(f"traffic done in {traffic_seconds:.0f}s; draining")

    acknowledged_poison = [o.event_id for o in plan if o.kind == "poison" and o.acknowledged]
    event_ids = sorted({o.response_event_id for o in plan if o.response_event_id})
    drained, drain_seconds = wait_for_drain(event_ids, acknowledged_poison, since, monitor)
    monitor.stop_event.set()
    monitor.join(timeout=30)
    if scenario == "consumer-kill":
        compose("up", "-d", "--no-deps", "--scale", "consumer=1", "consumer")
    bench_after = cpu_benchmark()  # the stack is idle again: drained
    host_after = host_state()
    ended_at = time.time()

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
        if scenario == "db-outage":
            checks.update(outage_checks(fault, plan))
        verdict = "PASS" if all(c["pass"] for c in checks.values()) else "FAILED"

    result = {
        "run": run_id, "scenario": scenario, "verdict": verdict, "seed": seed,
        "round": round_no, "events_planned": events, "concurrency": concurrency,
        "machine": machine(),
        "timing": {"started_at": utc(started_at), "traffic_started_at": utc(traffic_started),
                   "traffic_ended_at": utc(traffic_ended), "ended_at": utc(ended_at)},
        "cpu": {
            "container_cpu_seconds_during_traffic": cpu_delta(cpu_before, cpu_after),
            "vm_busy_cpu_seconds_during_traffic": (round(vm_after[0] - vm_before[0], 1)
                                                   if vm_before and vm_after else None),
            "vm_cpu_utilisation_during_traffic": (
                round((vm_after[0] - vm_before[0]) / (vm_after[1] - vm_before[1]), 3)
                if vm_before and vm_after else None),
            "vm_busy_cpu_seconds_per_1000_requests": (
                round((vm_after[0] - vm_before[0]) * 1000 / max(1, len(attempts)), 2)
                if vm_before and vm_after else None),
            "load_client_cpu_seconds": round(client_after - client_before, 1),
        },
        "thermal": {"cpu_benchmark_before": bench_before, "cpu_benchmark_after": bench_after,
                    "host_before": host_before, "host_after": host_after},
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
            "peak_cpu_percent": {k: round(max(v)) for k, v in sorted(monitor.cpu_samples.items())},
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
    parser.add_argument("--round", type=int, default=None,
                        help="label for consecutive full runs (stored in the JSON)")
    parser.add_argument("--stop-on-failure", action="store_true",
                        help="stop at the first run that is not PASS")
    args = parser.parse_args()

    scenarios = SCENARIOS if args.scenario == "all" else (args.scenario,)
    results = []
    for i, scenario in enumerate(scenarios):
        results.append(run_scenario(scenario, args.events, args.concurrency,
                                    args.seed + SCENARIOS.index(scenario), args.accounts, args.round))
        if args.stop_on_failure and results[-1]["verdict"] != "PASS":
            log(f"{scenario} is {results[-1]['verdict']}; stopping")
            break
    print()
    print(table(results))
    for r in results:
        failed = [name for name, c in r["checks"].items() if not c["pass"]]
        if failed:
            print(f"{r['scenario']}: failed checks: {', '.join(failed)}")
    return 0 if all(r["verdict"] == "PASS" for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
