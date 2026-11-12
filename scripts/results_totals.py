"""Totals across every committed result file, as quoted in README.md.

    python scripts/results_totals.py

Counting method:

- A chaos run is one docs/results/chaos-*.json file. Its verdict is the
  file's "verdict": PASS, FAILED, or INVALID (resource) when the Docker VM
  ran short of memory, which is not a correctness result.
- An operation is one entry of the run's seeded plan ("events_planned"): a
  payment with a new idempotency key, a resend of an earlier key with the
  same body, or a malformed (poison) message published straight to Kafka.
  The three counts are the run's "load" section and add up to the plan.
- A payment applied is a final 201: one new key, one transaction, checked
  against the run's manifest. Totals are given for PASS runs and for all.
- The baseline counts every HTTP request answered during its five recorded
  steps (the 20 s warm-up is not recorded) and the poison it published.

Reads only the committed JSON summaries; the manifests are not needed.
"""

from __future__ import annotations

import collections
import glob
import json
import pathlib

RESULTS = pathlib.Path(__file__).resolve().parent.parent / "docs" / "results"


def chaos_totals() -> dict:
    runs = [json.loads(pathlib.Path(f).read_text()) for f in sorted(glob.glob(str(RESULTS / "chaos-*.json")))]
    verdicts = collections.Counter(run["verdict"] for run in runs)
    sizes = collections.Counter(run["events_planned"] for run in runs)

    def add(selected) -> dict:
        total = collections.Counter()
        for run in selected:
            load, results = run["load"], run["results"]
            total["operations"] += run["events_planned"]
            total["new_key_payments"] += load["payment_requests"]
            total["same_key_resends"] += load["resends_same_key_same_body"]
            total["poison_messages"] += load["poison_published"]
            total["final_201"] += results["final_statuses"].get("201", 0)
            total["final_200_replays"] += results["final_statuses"].get("200", 0)
            total["http_attempts"] += sum(results["http_attempt_statuses"].values())
            total["http_503"] += results["http_attempt_statuses"].get("503", 0)
            total["http_500"] += results["http_attempt_statuses"].get("500", 0)
        return dict(total)

    return {
        "runs": len(runs),
        "verdicts": dict(verdicts),
        "runs_by_size": dict(sorted(sizes.items())),
        "pass_runs": add(r for r in runs if r["verdict"] == "PASS"),
        "all_runs": add(runs),
    }


def baseline_totals() -> list[dict]:
    out = []
    for f in sorted(glob.glob(str(RESULTS / "baseline-*.json"))):
        run = json.loads(pathlib.Path(f).read_text())
        out.append(
            {
                "run": run["run"],
                "steps": len(run["steps"]),
                "requests": sum(step["requests"] for step in run["steps"]),
                "errors": sum(step["errors"] for step in run["steps"]),
                "poison_published": sum(step["poison_published"] for step in run["steps"]),
                "sustained": run["sustained"],
            }
        )
    return out


if __name__ == "__main__":
    print(json.dumps({"chaos": chaos_totals(), "baseline": baseline_totals()}, indent=2))
