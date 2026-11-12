"""Write docs/results/README.md from the run JSON files in docs/results/.

    python scripts/chaos_report.py

Every number in the report is read from a file in docs/results/: the chaos
runs (chaos-*.json, scripts/chaos_run.py) and the steady-state baseline
(baseline-*.json, scripts/baseline_run.py). The few sentences of reading
(what limited throughput, what the thermal comparison shows) are in
READINGS below, and quote those numbers rather than restate them.
"""

from __future__ import annotations

import collections
import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
RESULTS = ROOT / "docs" / "results"
ROUND_EVENTS = 20_000

# The author's reading of the measured numbers, written after the runs. Each
# one names the figures it rests on; those figures are in the tables below.
READINGS = {
    "limit": (
        "**The single API process.** Its container used 1.09 to 1.17 cores at every step, from 4 "
        "clients to 64, while the whole Docker VM used about 2.5 of its 8 cores, Postgres 0.3 to "
        "0.5 and the load client 0.2. The API is one uvicorn worker, one Python interpreter, and "
        "the interpreter's global lock lets it run about one core of Python at a time. It was "
        "saturated at the first step, so every client beyond four only waited. With more "
        "requests in flight, more of them waited on the hot account's row lock (row-lock waiters: mean 0.02 at 4 clients, 6.8 at 64; "
        "`Lock:tuple` and `Lock:transactionid` lead the wait events from 16 clients). Each waiting "
        "request held a pooled connection: the pool was at its 25-connection ceiling in 28% of "
        "samples at 32 clients and 60% at 64. The API also spent more CPU per request: 5.3 ms at "
        "4 clients, 7.5 ms at 64. The server-side p99 tracks the client's, so requests queued "
        "inside the API, not in front of it. The pool ceiling is a consequence, not the cause: "
        "throughput stopped rising at 4 clients, and the pool first filled at 32.\n\n"
        "The fall from 205 to 155 requests a second as clients were added cannot be put on "
        "concurrency alone. The steps ran in rising order over 11 minutes while the machine "
        "warmed: the eight-process CPU benchmark took 1.16 s before the baseline and 1.55 s after "
        "it. More CPU per request is what both more contention and a slower clock would cause "
        "(see the thermal check). The finding that does not depend on clock speed is that the "
        "API container sat at about 1.1 cores at every step while everything else had "
        "headroom.\n\n"
        "Not the limit at this load:\n\n"
        "- **The NOTIFY commit lock** (§15 limitation 11): mean waiters at most 0.07, with a "
        "maximum of 7 in a single sample.\n"
        "- **The relay's per-message acknowledgement** (limitation 10): it published as fast "
        "as payments were created, with never more than 3 rows unpublished and the publish "
        "delay p95 at most 0.39 s, using 0.3 of a core.\n"
        "- **Docker VM memory:** at least 1,264 MB free throughout.\n\n"
        "The API caps the load at about 205 requests a second, so neither the NOTIFY lock nor "
        "the relay was pushed to its own limit; this run does not say where those would bind."),
    "thermal": (
        "**Throughput did not fall round after round.** Each scenario compared with itself, "
        "rounds 1, 2 and 3 in requests a second:\n\n"
        "- consumer-kill: 181.6, 109.5, 116.8\n"
        "- db-restart: 133.7, 99.5, 123.0\n"
        "- broker-outage: 109.1, 102.4, 111.2\n"
        "- relay-kill: 116.8, 108.7, 127.1\n"
        "- db-outage: 102.5, 99.8, 104.8\n\n"
        "Round 3 was faster than round 2 in all five scenarios. Correctness passed in every run.\n\n"
        "What changed was how much CPU time the same work took, and that follows the machine's "
        "heat, not the code. No code changed between runs, and each scenario replays the same "
        "seeded plan, so its requests are identical from round to round (19,790 for "
        "consumer-kill, 20,738 for db-outage).\n\n"
        "- **Throughput fell where CPU per request rose.** The scenarios that lost the most "
        "throughput are the ones whose identical requests cost the most extra CPU. For "
        "consumer-kill the "
        "API container used 127 CPU seconds in round 1, then 212 and 197. The whole VM used "
        "14.3, then 25.4 and 24.1 CPU seconds per 1,000 requests. db-restart went 19.5, 29.0, "
        "22.6, with throughput 133.7, 99.5, 123.0. The API process is CPU-bound, so when each "
        "CPU second does less work it serves fewer requests a second. That is what a lower "
        "clock does.\n"
        "- **The fixed CPU benchmark slowed.** On eight processes it took 1.16 s on the cool "
        "machine before the baseline, and between 1.23 and 2.73 s after the chaos "
        "runs, with the stack drained: the same work, up to 2.4 times as long.\n"
        "- **What rules out the obvious alternatives.** `pmset` recorded AC power and Low Power "
        "Mode off before and after every run, and no thermal warning. The database grew by about "
        "20,000 payments per run, but that cannot explain the pattern: round 3 needed less CPU "
        "per request than round 2 in all five scenarios, on a larger database.\n\n"
        "So this is consistent with thermal throttling on a fanless laptop, not a code "
        "regression. It was not a steady decline either: the machine was hot within the first "
        "runs and then varied from run to run. Throughput figures from this machine are "
        "therefore a range, about 100 to 180 requests a second under the chaos mix, and "
        "205 at best when cool (the baseline's first step).\n\n"
        "**Not observed directly:** the clock speed itself. macOS reports it only to root "
        "(`powermetrics`), and these runs did not use root."),
}


def load(pattern: str) -> list[dict]:
    runs = [json.loads(p.read_text()) for p in sorted(RESULTS.glob(pattern))]
    runs.sort(key=lambda r: r["run"].rsplit("-", 1)[-1])
    return runs


def hhmm(iso: str | None) -> str:
    return iso[11:19] if iso else "-"


def fmt(v, spec: str = "") -> str:
    return "-" if v is None else format(v, spec)


def machine_section(machine: dict, host: dict | None) -> list[str]:
    lines = [
        "# Chaos and load results",
        "",
        "## Machine",
        "",
        f"- Host: {machine.get('host')}, fanless: under sustained load it lowers its CPU clock "
        "to manage heat",
        f"- Docker VM: {machine.get('docker_vm_memory_gb')} GB memory, "
        f"{machine.get('docker_vm_cpus')} CPUs, no other projects or Docker extensions running",
    ]
    if host:
        lines.append(f"- Power: {host.get('power_source')}, Low Power Mode "
                     f"{'on' if host.get('low_power_mode') else 'off'} (recorded by `pmset` at the "
                     "start of every run since Day 5a)")
    lines += [
        "- The whole stack runs in that VM: 13 containers, including Postgres, Redpanda "
        "(512 MB), Jaeger (512 MB cap), Prometheus and Grafana; one API process "
        "(uvicorn, one worker). The load generator runs on the host.",
        "- Every number below comes from this machine. 6 GB or more for Docker is advised "
        "where available (BLUEPRINT.md §15, limitation 16).",
        "",
    ]
    return lines


def judged_section() -> list[str]:
    return [
        "## How a run is judged",
        "",
        "`scripts/chaos_run.py` (`make chaos`) sends a seeded plan of operations: 15% resend "
        "an earlier idempotency key with the same body, 1% are malformed messages published "
        "straight to `payments.main`, about 10% of new payments involve one hot account. The "
        "client retries a 503 (busy account, database down) and a 409 (same key in flight) "
        "with the same key after `Retry-After`, as a well-behaved client would. After the "
        "fault and the load, the pipeline must drain on its own and the run is checked against "
        "the manifest of every request: one transaction per key the client was paid for (a 201, "
        "or a 200 replay when the 201 was lost to the fault), one `payment_events` row per "
        "event, the DLQ exactly the injected poison, nothing sent to a retry topic, balances "
        "equal to the arithmetic of those payments, I1 to I4 passing, and no 500 at all. A run "
        "is INVALID (resource), never FAILED, if the Docker VM's free memory fell below 300 MB.",
        "",
        "| Scenario | Fault |",
        "| --- | --- |",
        "| consumer-kill | two consumers; SIGKILL one at a random point |",
        "| broker-outage | `docker compose stop redpanda` for 60 s mid-load, then start |",
        "| db-restart | `docker compose restart postgres` mid-load (a blip of under a second) |",
        "| relay-kill | SIGKILL the relay, start it 10 s later |",
        "| db-outage | `docker compose stop postgres` for 60 s mid-load, then start; the API must "
        "answer 503 to every request made while it is down |",
        "",
        "Recovery is the time from the restore action (or the kill) until every payment "
        "committed before it had been applied end to end, from database timestamps. For "
        "db-outage the restore is the moment Postgres accepts connections again.",
        "",
    ]


def baseline_section(b: dict) -> list[str]:
    steps = b["steps"]
    load = b["load"]
    lines = [
        "## Steady-state baseline (no faults)",
        "",
        f"`scripts/baseline_run.py` (`make baseline`), run {b['timing']['started_at'][:10]} "
        f"{hhmm(b['timing']['started_at'])}-{hhmm(b['timing']['ended_at'])} UTC, first, on a cool "
        f"machine. The same mix as the chaos suite, {load['accounts']} accounts, one consumer, "
        f"{load['seconds_per_step']:.0f} s per step at a fixed number of concurrent clients, the "
        "pipeline drained between steps. Latency is per HTTP attempt as the client saw it. An "
        "error is any status but 200 or 201. The client rotates each account's requests over 64 "
        "token subjects, so the API's per-subject rate limit (100 a minute) is never what it "
        "measures. A first attempt was stopped after its first step for exactly that reason: "
        "with subjects tied to 4 worker slots, the hot account drew 12 × 429. No result file "
        "was kept from it.",
        "",
        "| Clients | Started (UTC) | Req/s | Created/s | p50 ms | p95 ms | p99 ms | Errors | "
        "Statuses | Within p99 < 500 ms, 0 errors |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for s in steps:
        x = s["latency_ms"]
        lines.append(
            f"| {s['concurrency']} | {hhmm(s['started_at'])} | {s['requests_per_s']} | "
            f"{s['payments_created_per_s']} | {x['p50']} | {x['p95']} | {x['p99']} | "
            f"{s['errors']} | {', '.join(f'{k}: {v:,}' for k, v in s['statuses'].items())} | "
            f"{'yes' if s['within_slo'] else 'no'} |")
    best = b.get("sustained")
    lines += [""]
    if best:
        lines.append(
            f"**Sustained throughput: {best['requests_per_s']} requests/s "
            f"({best['payments_created_per_s']} payments created/s) at {best['concurrency']} "
            f"clients, p99 {best['p99_ms']} ms, zero errors.** The highest step that met both "
            "conditions.")
    else:
        lines.append("**No step met p99 < 500 ms with zero errors.**")
    lines += [
        "",
        "### Where the time went, per step",
        "",
        "Cores are CPU seconds per second of the step: container figures from each "
        "container's cgroup, the VM figure from `/proc/stat` (8 CPUs), the client's from its own "
        "process. Pool, NOTIFY and row-lock figures are 1-second samples of the API's "
        "`db_pool_connections` and of `pg_locks` (mean / max). Relay and consumer rates, "
        "backlog and lag are from Prometheus over the step.",
        "",
        "| Clients | API cores | Postgres cores | Relay cores | VM busy cores | Client cores | "
        "API pool in use (of 25) | NOTIFY lock waiters | Row-lock waiters | Created/s vs relay "
        "published/s | Outbox max | Lag max | VM min free MB |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for s in steps:
        c, pg, p = s["cpu"], s["postgres_and_pool"], s["prometheus"]
        cores = c["container_cores"]
        lines.append(
            f"| {s['concurrency']} | {fmt(cores.get('api'))} | {fmt(cores.get('postgres'))} | "
            f"{fmt(cores.get('relay'))} | {fmt(c['vm_busy_cores'])} | {c['load_client_cores']} | "
            f"{pg['api_pool_in_use']['mean']} / {pg['api_pool_in_use']['max']} | "
            f"{pg['notify_lock_waiting']['mean']} / {pg['notify_lock_waiting']['max']} | "
            f"{pg['row_lock_waiting']['mean']} / {pg['row_lock_waiting']['max']} | "
            f"{fmt(p['payments_created_per_s'], '.0f')} vs {fmt(p['relay_published_per_s'], '.0f')} | "
            f"{fmt(p['outbox_unpublished_max'], '.0f')} | {fmt(p['consumer_lag_max'], '.0f')} | "
            f"{s['vm_min_available_mb']} |")
    lines += ["", "What active Postgres backends were waiting on (mean backends per sample):", "",
              "| Clients | Wait events |", "| --- | --- |"]
    for s in steps:
        waits = s["postgres_and_pool"]["active_backends_by_wait"]
        lines.append(f"| {s['concurrency']} | "
                     + ", ".join(f"{k} {v}" for k, v in list(waits.items())[:6]) + " |")
    lines += ["", "API latency inside the server (Prometheus `payment_duration_seconds`, "
              "bucketed) against the client's view:", "",
              "| Clients | Server p50 s | Server p99 s | Client p50 ms | Client p99 ms |",
              "| --- | --- | --- | --- | --- |"]
    for s in steps:
        p = s["prometheus"]
        lines.append(f"| {s['concurrency']} | {fmt(p['api_server_side_p50_s'])} | "
                     f"{fmt(p['api_server_side_p99_s'])} | {s['latency_ms']['p50']} | "
                     f"{s['latency_ms']['p99']} |")
    if READINGS["limit"]:
        lines += ["", "### What limited it", "", READINGS["limit"]]
    lines.append("")
    return lines


def rounds_section(rounds: dict[int, list[dict]]) -> list[str]:
    lines = [
        f"## Three consecutive full runs at {ROUND_EVENTS:,} operations",
        "",
        "`make chaos` (all five scenarios) three times in a row. A FAILED or INVALID run "
        "would have reset the count to zero; every attempt is listed under "
        "[Every run](#every-run).",
        "",
        "| Round | Scenario | Started (UTC) | Ended (UTC) | Verdict | Req/s | p50 ms | p95 ms | "
        "p99 ms | 503s | 500s | Recovery s | Drain s | VM min free MB |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for n in sorted(rounds):
        for r in rounds[n]:
            x = r["results"]
            t = r.get("timing", {})
            lines.append(
                f"| {n} | {r['scenario']} | {hhmm(t.get('started_at'))} | {hhmm(t.get('ended_at'))} | "
                f"{r['verdict']} | {x['throughput_requests_per_s']} | {x['latency_ms']['p50']} | "
                f"{x['latency_ms']['p95']} | {x['latency_ms']['p99']} | {x['count_503']} | "
                f"{x['http_attempt_statuses'].get('500', 0)} | {x['recovery_seconds_after_fault']} | "
                f"{x['drain_seconds_after_traffic']} | {x['vm_min_available_mb']} |")
    clean = 0
    for n in sorted(rounds):
        ok = len(rounds[n]) == 5 and all(r["verdict"] == "PASS" for r in rounds[n])
        clean = clean + 1 if ok else 0
    lines += ["", f"Consecutive clean full runs: **{clean}**.", ""]

    outages = [r for n in sorted(rounds) for r in rounds[n] if r["scenario"] == "db-outage"]
    if outages:
        lines += [
            "### The 60-second database outage",
            "",
            "| Round | Outage s | Requests while down | Their statuses | "
            "Postgres start to accepting s | First 201 after accepting s | Pipeline recovery s |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
        for r in outages:
            c = r["checks"]
            a, b = c.get("api_503_throughout_outage", {}), c.get("api_recovered_after_postgres_returned", {})
            lines.append(
                f"| {r['round']} | {a.get('outage_seconds')} | {fmt(a.get('requests_during_outage'), ',')} | "
                f"{a.get('statuses_during_outage')} | "
                f"{b.get('postgres_start_to_accepting_s')} | {b.get('first_201_after_postgres_accepting_s')} | "
                f"{r['results']['recovery_seconds_after_fault']} |")
        lines += ["", "Pipeline recovery 0.0 means every payment committed before the stop had "
                  "already been applied by the time Postgres accepted connections again: nothing "
                  "was left waiting.", "",
                  "Consumers sent nothing to a retry topic or the DLQ in any of them "
                  "(`nothing_sent_to_retry_topics` and `dlq_is_exactly_the_poison` in each run's "
                  "JSON).", ""]
    return lines


def thermal_section(rounds: dict[int, list[dict]], baseline: dict | None) -> list[str]:
    by = collections.defaultdict(dict)
    for n, runs in rounds.items():
        for r in runs:
            by[r["scenario"]][n] = r
    ns = sorted(rounds)
    lines = [
        "## Thermal check",
        "",
        "The same scenario, round after round. Throughput is requests per second of the "
        "run's traffic phase. VM CPU per 1,000 requests is busy CPU seconds of the whole Docker "
        "VM (`/proc/stat`) during traffic, divided by requests: the work done per request, "
        "which should not change if the code does not. The benchmark is `scripts/cpu_bench.py`, "
        "a fixed CPU loop timed on the host while the stack is idle, before and after each run: "
        "if the CPU slows itself to shed heat it takes longer.",
        "",
        "| Scenario | " + " | ".join(f"Req/s R{n}" for n in ns) + " | "
        + " | ".join(f"VM CPU s per 1k req R{n}" for n in ns) + " | "
        + " | ".join(f"API CPU s R{n}" for n in ns) + " |",
        "| --- |" + " --- |" * (3 * len(ns)),
    ]
    for scenario in ("consumer-kill", "broker-outage", "db-restart", "relay-kill", "db-outage"):
        if scenario not in by:
            continue
        row = by[scenario]
        lines.append(
            f"| {scenario} | "
            + " | ".join(str(row[n]["results"]["throughput_requests_per_s"]) if n in row else "-"
                         for n in ns) + " | "
            + " | ".join(fmt(row[n].get("cpu", {}).get("vm_busy_cpu_seconds_per_1000_requests"))
                         if n in row else "-" for n in ns) + " | "
            + " | ".join(fmt(row[n].get("cpu", {}).get("container_cpu_seconds_during_traffic", {})
                             .get("api")) if n in row else "-" for n in ns) + " |")
    lines += ["", "CPU benchmark (seconds; lower is faster), in run order:", "",
              "| Run | Started (UTC) | Before: 1 core | Before: 8 processes | After: 1 core | "
              "After: 8 processes |", "| --- | --- | --- | --- | --- | --- |"]
    if baseline:
        t = baseline["thermal"]
        bb, ba = t.get("cpu_benchmark_before") or {}, t.get("cpu_benchmark_after") or {}
        lines.append(f"| baseline | {hhmm(baseline['timing']['started_at'])} | "
                     f"{bb.get('single_thread_s')} | {bb.get('eight_processes_s')} | "
                     f"{ba.get('single_thread_s')} | {ba.get('eight_processes_s')} |")
    for n in ns:
        for r in rounds[n]:
            t = r.get("thermal", {})
            bb, ba = t.get("cpu_benchmark_before") or {}, t.get("cpu_benchmark_after") or {}
            lines.append(f"| R{n} {r['scenario']} | {hhmm(r['timing']['started_at'])} | "
                         f"{bb.get('single_thread_s')} | {bb.get('eight_processes_s')} | "
                         f"{ba.get('single_thread_s')} | {ba.get('eight_processes_s')} |")
    if READINGS["thermal"]:
        lines += ["", READINGS["thermal"]]
    lines.append("")
    return lines


def memory_and_load(rounds: dict[int, list[dict]]) -> list[str]:
    runs = [r for n in sorted(rounds) for r in rounds[n]]
    if not runs:
        return []
    scenarios = list(dict.fromkeys(r["scenario"] for r in runs))
    services = sorted({s for r in runs for s in r["results"]["peak_memory_mb"]})
    lines = ["## Peak memory per service, three rounds (MB, highest of the rounds)", "",
             "| Service | " + " | ".join(scenarios) + " |", "| --- |" + " --- |" * len(scenarios)]
    for s in services:
        cells = []
        for sc in scenarios:
            vals = [r["results"]["peak_memory_mb"].get(s) for r in runs if r["scenario"] == sc]
            vals = [v for v in vals if v is not None]
            cells.append(str(max(vals)) if vals else "-")
        lines.append(f"| {s} | " + " | ".join(cells) + " |")
    lines += ["", "## Load of the three rounds", "",
              "| Round | Scenario | Payment requests | Same-key resends | Poison | Hot-account share | "
              "Final statuses |", "| --- | --- | --- | --- | --- | --- | --- |"]
    for r in runs:
        load = r["load"]
        lines.append(
            f"| {r['round']} | {r['scenario']} | {load['payment_requests']:,} | "
            f"{load['resends_same_key_same_body']:,} | {load['poison_published']} | "
            f"{load['hot_account_share']:.1%} | {r['results']['final_statuses']} |")
    lines.append("")
    return lines


def every_run(runs: list[dict]) -> list[str]:
    lines = [
        "## Every run",
        "",
        "Every chaos run on this machine, in order, including the Phase 8 ramp, its INVALID "
        "run and its 50,000-operation runs.",
        "",
        "| Run | Round | Verdict | Req/s | p50 ms | p95 ms | p99 ms | 503s | 500s | Recovery s | "
        "Drain s | VM min free MB |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for r in runs:
        x = r["results"]
        lines.append(
            f"| [{r['run']}](chaos-{r['run']}.json) | {r.get('round') or '-'} | {r['verdict']} | "
            f"{x['throughput_requests_per_s']} | {x['latency_ms']['p50']} | "
            f"{x['latency_ms']['p95']} | {x['latency_ms']['p99']} | {x['count_503']} | "
            f"{x['http_attempt_statuses'].get('500', 0)} | {x['recovery_seconds_after_fault']} | "
            f"{x['drain_seconds_after_traffic']} | {x['vm_min_available_mb']} |")
    lines += [
        "",
        "The first run (relay-kill, 5,000) used an earlier definition of recovery that needed a "
        "payment created after the restore; it recorded none, hence `None`.",
        "",
        "Each chaos run also writes a manifest of every request and its responses to "
        "`docs/results/manifests/chaos-<run>.jsonl.gz` (gzipped JSON lines). Manifests are kept "
        "on the machine that ran them and are not committed; the summary JSON is what the repo "
        "keeps.",
        "",
    ]
    return lines


def main() -> None:
    runs = load("chaos-*.json")
    baselines = load("baseline-*.json")
    baseline = baselines[-1] if baselines else None
    rounds: dict[int, list[dict]] = collections.defaultdict(list)
    for r in runs:
        if r.get("round") and r["events_planned"] == ROUND_EVENTS:
            rounds[r["round"]].append(r)
    latest = runs[-1] if runs else (baseline or {})
    host = (latest.get("thermal") or {}).get("host_before")
    lines = machine_section(latest.get("machine", {}), host)
    lines += judged_section()
    if baseline:
        lines += baseline_section(baseline)
    if rounds:
        lines += rounds_section(rounds)
        lines += thermal_section(rounds, baseline)
        lines += memory_and_load(rounds)
    lines += every_run(runs)
    (RESULTS / "README.md").write_text("\n".join(lines))
    print(f"wrote {RESULTS / 'README.md'} from {len(runs)} chaos runs and "
          f"{len(baselines)} baseline run(s)")


if __name__ == "__main__":
    main()
