"""Steady-state baseline: no faults, client concurrency stepped up.

    python scripts/baseline_run.py                         # make baseline
    python scripts/baseline_run.py --steps 4,8 --seconds 30

The same traffic mix as the chaos suite (scripts/chaos_run.py): 15% resend an
earlier idempotency key with the same body, 1% malformed messages published
straight to payments.main, about 10% of new payments on one hot account. One
consumer, as in normal operation. Each step runs for --seconds at a fixed
number of concurrent clients; between steps the pipeline drains.

Per step it records the client's view (requests per second, p50/p95/p99 of
every HTTP attempt, every status), and what each part of the system was
doing, so the step where it stops scaling can be explained, not guessed:

    CPU          seconds used by each container (its cgroup) and by the whole
                 Docker VM (/proc/stat); process CPU per job from Prometheus;
                 the load client's own CPU time
    DB pool      the API's checked-out connections (its /metrics, every 1 s)
    NOTIFY lock  backends waiting for the database-wide lock a NOTIFY takes
                 at commit (pg_locks, every 1 s; docs/ARCHITECTURE.md §15 lim. 11)
    row locks    backends waiting on another transaction (hot accounts)
    relay        payments created per second against outbox rows published
                 per second, outbox backlog and publish delay (§15 lim. 10)
    memory       the Docker VM's free memory (the chaos memory guard)

"Sustained throughput" is the highest requests per second of a step whose
p99 stayed under 500 ms with no error at all (any status but 200 or 201).

Output: docs/results/baseline-<stamp>.json. Local stack only. It changes
nothing in the stack; it only measures it.
"""

from __future__ import annotations

import argparse
import collections
import json
import random
import re
import statistics
import sys
import threading
import time

import httpx
import psycopg

from scripts.chaos_run import (
    API,
    DSN,
    RESULTS,
    Monitor,
    Traffic,
    container_cpu_seconds,
    cpu_benchmark,
    cpu_delta,
    create_accounts,
    group_lag,
    host_state,
    log,
    machine,
    make_plan,
    percentile,
    preflight,
    query,
    utc,
    vm_cpu_seconds,
)

PROM = "http://localhost:9090"
P99_LIMIT_MS = 500
SAMPLE_SECONDS = 1.0
CLIENT_BUCKETS = 64  # rate-limit subjects per account; the server's limit is per subject


# --- 1-second sampler: Postgres and the API's pool ---------------------------------------


class Sampler(threading.Thread):
    WAITS = """
        SELECT coalesce(wait_event_type, 'CPU'), coalesce(wait_event, 'running'), count(*)
        FROM pg_stat_activity
        WHERE datname = 'payments' AND state = 'active' AND backend_type = 'client backend'
          AND pid <> pg_backend_pid()
        GROUP BY 1, 2"""
    LOCKS = """
        SELECT
          count(*) FILTER (WHERE locktype = 'object' AND classid = 'pg_database'::regclass
                           AND objid = 0 AND NOT granted),
          count(*) FILTER (WHERE locktype IN ('transactionid', 'tuple') AND NOT granted)
        FROM pg_locks"""
    CONNECTIONS = """
        SELECT count(*) FILTER (WHERE state = 'active'), count(*)
        FROM pg_stat_activity
        WHERE datname = 'payments' AND backend_type = 'client backend'
          AND pid <> pg_backend_pid()"""

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.reset()

    def reset(self) -> None:
        with getattr(self, "lock", threading.Lock()):
            self.samples = 0
            self.waits: collections.Counter = collections.Counter()
            self.notify_waiting: list[int] = []
            self.row_lock_waiting: list[int] = []
            self.active: list[int] = []
            self.connections: list[int] = []
            self.pool_in_use: list[int] = []

    def run(self) -> None:
        conn = psycopg.connect(DSN, autocommit=True)
        http = httpx.Client(base_url=API, timeout=5)
        pool_re = re.compile(r'^db_pool_connections\{state="in_use"\} ([\d.]+)', re.M)
        try:
            while not self.stop_event.is_set():
                started = time.monotonic()
                try:
                    waits = conn.execute(self.WAITS).fetchall()
                    notify, row = conn.execute(self.LOCKS).fetchone()
                    active, total = conn.execute(self.CONNECTIONS).fetchone()
                    m = pool_re.search(http.get("/metrics").text)
                    with self.lock:
                        self.samples += 1
                        for kind, event, n in waits:
                            self.waits[f"{kind}:{event}"] += n
                        self.notify_waiting.append(notify)
                        self.row_lock_waiting.append(row)
                        self.active.append(active)
                        self.connections.append(total)
                        if m:
                            self.pool_in_use.append(int(float(m.group(1))))
                except Exception as exc:  # a missed sample is not a result
                    log(f"sampler: {exc!r}")
                self.stop_event.wait(max(0.0, SAMPLE_SECONDS - (time.monotonic() - started)))
        finally:
            conn.close()
            http.close()

    def summary(self) -> dict:
        with self.lock:
            n = max(1, self.samples)

            def stats(values):
                return {"mean": round(statistics.fmean(values), 2) if values else None,
                        "max": max(values) if values else None}

            return {
                "samples": self.samples,
                "active_backends": stats(self.active),
                "connections": stats(self.connections),
                "api_pool_in_use": stats(self.pool_in_use),
                "api_pool_at_capacity_share": (
                    round(sum(v >= 25 for v in self.pool_in_use) / len(self.pool_in_use), 3)
                    if self.pool_in_use else None),
                "notify_lock_waiting": stats(self.notify_waiting),
                "row_lock_waiting": stats(self.row_lock_waiting),
                # Mean number of active backends in each state per sample.
                "active_backends_by_wait": {k: round(v / n, 2)
                                            for k, v in self.waits.most_common(10)},
            }


# --- Prometheus ------------------------------------------------------------------------------


def prom(expr: str, at: float) -> list[tuple[dict, float]]:
    r = httpx.get(f"{PROM}/api/v1/query", params={"query": expr, "time": at}, timeout=20)
    r.raise_for_status()
    return [(x["metric"], float(x["value"][1])) for x in r.json()["data"]["result"]]


def prom_one(expr: str, at: float) -> float | None:
    try:
        values = prom(expr, at)
    except Exception:
        return None
    return round(values[0][1], 3) if values else None


def prometheus_evidence(start: float, end: float) -> dict:
    # Prometheus scrapes every 10 s; let the last scrape of the step land.
    time.sleep(12)
    w = max(30, int(end - start))
    at = end + 10
    cores = {}
    try:
        for metric, value in prom(f"increase(process_cpu_seconds_total[{w}s])", at):
            cores[metric.get("job")] = round(value / w, 3)
    except Exception:
        pass
    q = "histogram_quantile({q}, sum by (le) (increase(payment_duration_seconds_bucket" \
        "{{endpoint=\"/v1/payments\"}}[{w}s])))"
    return {
        "window_seconds": w,
        "process_cpu_cores_by_job": dict(sorted(cores.items())),
        "api_server_side_p50_s": prom_one(q.format(q=0.5, w=w), at),
        "api_server_side_p99_s": prom_one(q.format(q=0.99, w=w), at),
        "payments_created_per_s": prom_one(
            f'sum(increase(payments_total{{status="created"}}[{w}s])) / {w}', at),
        "relay_published_per_s": prom_one(f"increase(outbox_publish_delay_seconds_count[{w}s]) / {w}", at),
        "consumer_applied_per_s": prom_one(
            f'sum(increase(events_processed_total{{job="consumer",outcome="applied"}}[{w}s])) / {w}', at),
        "outbox_unpublished_max": prom_one(f"max_over_time(outbox_unpublished_rows[{w}s])", at),
        "outbox_publish_delay_recent_p95_max_s": prom_one(
            f"max_over_time(outbox_publish_delay_recent_p95_seconds[{w}s])", at),
        "consumer_lag_max": prom_one(
            f'max_over_time(sum(kafka_consumer_lag{{group="payments-ledger"}})[{w}s:10s])', at),
        "api_pool_in_use_max_scraped": prom_one(
            f'max_over_time(db_pool_connections{{job="api",state="in_use"}}[{w}s])', at),
    }


# --- one step --------------------------------------------------------------------------------


def wait_for_pipeline(timeout: float = 900) -> float:
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        try:
            if (query("SELECT count(*) FROM outbox WHERE published_at IS NULL")[0][0] == 0
                    and all(v == 0 for v in group_lag().values())):
                return round(time.monotonic() - started, 1)
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError("pipeline did not drain between steps")


def run_step(concurrency: int, seconds: float, accounts, hot, rng, sampler: Sampler,
             record: bool = True) -> dict:
    plan = make_plan(int(seconds * 800), accounts, hot, rng)  # more than any step can send
    monitor = Monitor()
    monitor.start()
    traffic = Traffic(plan, concurrency, monitor, buckets=CLIENT_BUCKETS,
                      deadline=time.monotonic() + seconds, subject_per_op=True)
    cpu_before, vm_before, client_before = container_cpu_seconds(), vm_cpu_seconds(), time.process_time()
    sampler.reset()
    started = time.time()
    traffic_seconds = traffic.run()
    ended = time.time()
    pg = sampler.summary()
    cpu_after, vm_after, client_after = container_cpu_seconds(), vm_cpu_seconds(), time.process_time()
    monitor.stop_event.set()
    monitor.join(timeout=30)
    try:
        backlog = query("SELECT count(*) FROM outbox WHERE published_at IS NULL")[0][0]
        lag = group_lag().get("payments-ledger", 0)
    except Exception:
        backlog = lag = None
    if not record:
        wait_for_pipeline()
        return {}
    evidence = prometheus_evidence(started, ended)
    drain = wait_for_pipeline()

    sent = [o for o in plan if o.attempts or o.final is not None]
    attempts = [a for o in sent if o.kind != "poison" for a in o.attempts]
    statuses = collections.Counter(str(a[0]) for a in attempts)
    latencies = [a[1] for a in attempts if isinstance(a[0], int)]
    errors = sum(n for s, n in statuses.items() if s not in ("200", "201"))
    poison = [o for o in sent if o.kind == "poison"]
    wall = traffic_seconds
    cpu = cpu_delta(cpu_before, cpu_after)
    p99 = percentile(latencies, 0.99)
    result = {
        "concurrency": concurrency,
        "started_at": utc(started), "ended_at": utc(ended),
        "seconds": round(wall, 1),
        "requests": len(attempts),
        "requests_per_s": round(len(attempts) / wall, 1),
        "payments_created_per_s": round(statuses.get("201", 0) / wall, 1),
        "latency_ms": {"p50": percentile(latencies, 0.50), "p95": percentile(latencies, 0.95),
                       "p99": p99, "max": round(max(latencies), 1) if latencies else None},
        "statuses": dict(sorted(statuses.items())),
        "errors": errors,
        "poison_published": sum(o.acknowledged for o in poison),
        "within_slo": bool(latencies) and p99 < P99_LIMIT_MS and errors == 0,
        "cpu": {
            "container_cores": {k: (round(v / wall, 2) if v is not None else None)
                                for k, v in cpu.items()},
            "vm_cpu_utilisation": (round((vm_after[0] - vm_before[0]) / (vm_after[1] - vm_before[1]), 3)
                                   if vm_before and vm_after else None),
            "vm_busy_cores": (round((vm_after[0] - vm_before[0]) / wall, 2)
                              if vm_before and vm_after else None),
            "load_client_cores": round((client_after - client_before) / wall, 2),
            "peak_cpu_percent_docker_stats": {k: round(max(v)) for k, v in
                                              sorted(monitor.cpu_samples.items())},
        },
        "postgres_and_pool": pg,
        "prometheus": evidence,
        "pipeline_at_step_end": {"outbox_unpublished": backlog, "consumer_lag": lag,
                                 "drain_seconds": drain},
        "vm_min_available_mb": round(monitor.min_available_mb or 0),
        "memory_guard_tripped": monitor.abort.is_set(),
    }
    log(f"step c={concurrency}: {result['requests_per_s']} req/s, p50/p95/p99 "
        f"{result['latency_ms']['p50']}/{result['latency_ms']['p95']}/{p99} ms, errors {errors}, "
        f"statuses {result['statuses']}, api cores {result['cpu']['container_cores'].get('api')}, "
        f"postgres cores {result['cpu']['container_cores'].get('postgres')}, "
        f"vm util {result['cpu']['vm_cpu_utilisation']}, pool max {pg['api_pool_in_use']['max']}, "
        f"notify waiting mean {pg['notify_lock_waiting']['mean']}, backlog {backlog}, lag {lag}")
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--steps", default="4,8,16,32,64")
    parser.add_argument("--seconds", type=float, default=120)
    parser.add_argument("--accounts", type=int, default=200)
    parser.add_argument("--seed", type=int, default=5)
    args = parser.parse_args()
    steps = [int(s) for s in args.steps.split(",")]

    run_id = f"baseline-{time.strftime('%Y%m%dT%H%M%S')}"
    log(f"=== {run_id}: steps {steps}, {args.seconds:.0f} s each ===")
    started_at = time.time()
    host_before = host_state()
    preflight("baseline")
    bench_before = cpu_benchmark()
    rng = random.Random(args.seed)
    accounts, hot = create_accounts(args.accounts, run_id)

    sampler = Sampler()
    sampler.start()
    log("warm-up: 20 s at 4 clients, not recorded")
    run_step(4, 20, accounts, hot, rng, sampler, record=False)

    results = []
    for concurrency in steps:
        results.append(run_step(concurrency, args.seconds, accounts, hot, rng, sampler))
        if results[-1]["memory_guard_tripped"]:
            log("memory guard tripped; stopping the baseline (INVALID, resource)")
            break
    sampler.stop_event.set()
    bench_after = cpu_benchmark()

    good = [r for r in results if r["within_slo"] and not r["memory_guard_tripped"]]
    best = max(good, key=lambda r: r["requests_per_s"]) if good else None
    summary = {
        "run": run_id,
        "kind": "steady-state baseline, no faults",
        "machine": machine(),
        "timing": {"started_at": utc(started_at), "ended_at": utc(time.time())},
        "load": {"mix": "15% same-key resends, 1% poison to payments.main, ~10% of new payments "
                        "on one hot account", "accounts": args.accounts + 1,
                 "seconds_per_step": args.seconds, "consumers": 1,
                 "rate_limit_subjects_per_account": CLIENT_BUCKETS,
                 "subjects": "rotated per request, so the client never meets the "
                             "100-a-minute per-subject limit",
                 "client_retries": "503 and 409 retried with the same key after Retry-After, "
                                   "each attempt counted"},
        "slo": {"p99_ms_below": P99_LIMIT_MS, "errors": 0},
        "sustained": ({"concurrency": best["concurrency"], "requests_per_s": best["requests_per_s"],
                       "payments_created_per_s": best["payments_created_per_s"],
                       "p99_ms": best["latency_ms"]["p99"]} if best else None),
        "steps": results,
        "thermal": {"cpu_benchmark_before": bench_before, "cpu_benchmark_after": bench_after,
                    "host_before": host_before, "host_after": host_state()},
    }
    path = RESULTS / f"{run_id}.json"
    path.write_text(json.dumps(summary, indent=2, default=str) + "\n")
    print()
    print("| Clients | Req/s | Created/s | p50 ms | p95 ms | p99 ms | Errors | Within SLO |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for r in results:
        x = r["latency_ms"]
        print(f"| {r['concurrency']} | {r['requests_per_s']} | {r['payments_created_per_s']} | "
              f"{x['p50']} | {x['p95']} | {x['p99']} | {r['errors']} | {r['within_slo']} |")
    print(f"\nsustained: {summary['sustained']}\nwrote {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
