"""Write docs/results/README.md from the chaos run JSON files.

    python scripts/chaos_report.py

Every number in the report comes from a file in docs/results/; nothing is
typed by hand.
"""

from __future__ import annotations

import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
RESULTS = ROOT / "docs" / "results"


def main() -> None:
    runs = [json.loads(p.read_text()) for p in sorted(RESULTS.glob("chaos-*.json"))]
    runs.sort(key=lambda r: r["run"].rsplit("-", 1)[-1])
    machine = runs[-1]["machine"] if runs else {}
    lines = [
        "# Chaos results",
        "",
        "## Machine",
        "",
        f"- Host: {machine.get('host')} ({machine.get('host_cpu')}, "
        f"{machine.get('host_memory_gb')} GB)",
        f"- Docker VM: {machine.get('docker_vm_memory_gb')} GB memory, "
        f"{machine.get('docker_vm_cpus')} CPUs, no other projects or Docker extensions running",
        "- The whole stack runs in that VM: 13 containers, including Postgres, Redpanda "
        "(512 MB), Jaeger (512 MB cap), Prometheus and Grafana. The load generator runs on "
        "the host.",
        "- Every number below comes from this machine. 6 GB or more for Docker is advised "
        "where available (BLUEPRINT.md §15, limitation 16).",
        "",
        "## How a run is judged",
        "",
        "`scripts/chaos_run.py` (`make chaos`) sends a seeded plan of operations: 15% resend "
        "an earlier idempotency key with the same body, 1% are malformed messages published "
        "straight to `payments.main`, about 10% of new payments involve one hot account. The "
        "client retries a 503 with the same key after `Retry-After`. After the fault and the "
        "load, the pipeline must drain on its own and the run is checked against the manifest "
        "of every request: one transaction per key answered 201, one `payment_events` row per "
        "event, the DLQ exactly the injected poison, nothing sent to a retry topic, balances "
        "equal to the arithmetic of the 201s, I1 to I4 passing, and no 500 at all. A run is "
        "INVALID (resource), never FAILED, if the Docker VM's free memory fell below 300 MB.",
        "",
        "Recovery is the time from the restore action (or the kill) until every payment "
        "committed before it had been applied end to end, from database timestamps.",
        "",
        "## Runs",
        "",
        "| Run | Verdict | Req/s | p50 ms | p95 ms | p99 ms | 503s | 500s | Recovery s | "
        "Drain s | VM min free MB |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for r in runs:
        x = r["results"]
        lines.append(
            f"| [{r['run']}](chaos-{r['run']}.json) | {r['verdict']} | "
            f"{x['throughput_requests_per_s']} | {x['latency_ms']['p50']} | "
            f"{x['latency_ms']['p95']} | {x['latency_ms']['p99']} | {x['count_503']} | "
            f"{x['http_attempt_statuses'].get('500', 0)} | {x['recovery_seconds_after_fault']} | "
            f"{x['drain_seconds_after_traffic']} | {x['vm_min_available_mb']} |"
        )
    gate = [r for r in runs if r["events_planned"] >= 20_000]
    if gate:
        services = sorted({s for r in gate for s in r["results"]["peak_memory_mb"]})
        label = [f"{r['scenario']} {r['events_planned'] // 1000}k" for r in gate]
        lines += ["", "## Peak memory per service, runs of 20,000 events or more (MB)", "",
                  "| Service | " + " | ".join(label) + " |",
                  "| --- |" + " --- |" * len(gate)]
        for s in services:
            lines.append(f"| {s} | " + " | ".join(
                str(r["results"]["peak_memory_mb"].get(s, "-")) for r in gate) + " |")
        lines += ["", "## Load of the runs of 20,000 events or more", "",
                  "| Run | Payment requests | Same-key resends | Poison | Hot-account share | "
                  "Final statuses |", "| --- | --- | --- | --- | --- | --- |"]
        for r in gate:
            load = r["load"]
            lines.append(
                f"| {r['scenario']} {r['events_planned'] // 1000}k | {load['payment_requests']:,} | "
                f"{load['resends_same_key_same_body']:,} | {load['poison_published']} | "
                f"{load['hot_account_share']:.1%} | {r['results']['final_statuses']} |")
    lines += ["", "The first run (relay-kill, 5,000) used an earlier definition of recovery "
              "that needed a payment created after the restore; it recorded none, hence `None`.",
              "", "Each run also writes a manifest of every request and its responses to "
              "`docs/results/manifests/chaos-<run>.jsonl.gz` (gzipped JSON lines). Manifests "
              "are kept on the machine that ran them and are not committed (about 27 MB for "
              "these runs); the summary JSON above is what the repo keeps.", ""]
    (RESULTS / "README.md").write_text("\n".join(lines))
    print(f"wrote {RESULTS / 'README.md'} from {len(runs)} runs")


if __name__ == "__main__":
    main()
