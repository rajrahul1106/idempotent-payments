"""Runbook drills against the local stack (RUNBOOK.md).

    python scripts/drill.py accounts 60 --fund 10000000 > accounts.json
    python scripts/drill.py pay accounts.json --count 12000 --concurrency 32
    python scripts/drill.py pay accounts.json --duration 300 --mismatch-every 20
    python scripts/drill.py watch alerts.jsonl          # records alert state changes
    python scripts/drill.py wait ConsumerLagHigh --timeout 900

Tokens are minted locally with the development JWT secret, one per drill
account, so the per-token rate limit (100 requests a minute) is not the
bottleneck. Local stack only: it refuses a non-local API URL.
"""

from __future__ import annotations

import argparse
import collections
import json
import statistics
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import httpx

API = "http://localhost:8000"
PROMETHEUS = "http://localhost:9090"


def _token(subject: str, role: str, account_id: str | None = None) -> str:
    from app.core.security import issue_token

    return issue_token(subject, role, account_id=account_id)[0]


def accounts(count: int, fund_minor: int) -> list[str]:
    """Create `count` customer accounts, each funded from sys_settlement."""
    created = []
    with httpx.Client(base_url=API, timeout=30) as client:
        for i in range(count):
            operator = {"Authorization": f"Bearer {_token(f'drill-operator-{i}', 'operator')}"}
            response = client.post("/v1/accounts", headers=operator,
                                   json={"holder_name": f"Drill {i}", "currency": "INR"})
            response.raise_for_status()
            account = response.json()["id"]
            funded = client.post(
                "/v1/payments",
                headers={**operator, "Idempotency-Key": str(uuid.uuid4())},
                json={"from_account_id": "sys_settlement", "to_account_id": account,
                      "amount_minor": fund_minor, "currency": "INR"},
            )
            funded.raise_for_status()
            created.append(account)
    return created


def pay(account_ids: list[str], *, count: int | None, duration: float | None, concurrency: int,
        to: str, mismatch_every: int, amount: int) -> dict:
    tokens = {a: _token(f"drill-{a}", "client", account_id=a) for a in account_ids}
    statuses: collections.Counter = collections.Counter()
    latencies: list[float] = []
    lock = threading.Lock()
    deadline = time.monotonic() + duration if duration else None
    sent = iter(range(count if count is not None else 10**9))
    sent_lock = threading.Lock()

    def worker(slot: int) -> None:
        client = httpx.Client(base_url=API, timeout=60)
        n = 0
        while True:
            with sent_lock:
                index = next(sent, None)
            if index is None or (deadline and time.monotonic() > deadline):
                break
            account = account_ids[index % len(account_ids)]
            # Spread credits over the drill accounts: every payment crediting
            # one account serialises on that row's lock (lock_timeout -> 500).
            destination = to or account_ids[(index + 1) % len(account_ids)]
            headers = {"Authorization": f"Bearer {tokens[account]}"}
            key = str(uuid.uuid4())
            body = {"from_account_id": account, "to_account_id": destination,
                    "amount_minor": amount, "currency": "INR"}
            started = time.monotonic()
            try:
                response = client.post("/v1/payments", json=body,
                                       headers={**headers, "Idempotency-Key": key})
                code = str(response.status_code)
                if mismatch_every and n % mismatch_every == 0:
                    # A client bug: the same key reused for a different body.
                    again = client.post("/v1/payments", json={**body, "amount_minor": amount + 1},
                                        headers={**headers, "Idempotency-Key": key})
                    with lock:
                        statuses[f"reuse-{again.status_code}"] += 1
            except httpx.HTTPError as exc:
                code = type(exc).__name__
            with lock:
                statuses[code] += 1
                latencies.append(time.monotonic() - started)
            n += 1
        client.close()

    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        list(pool.map(worker, range(concurrency)))
    elapsed = time.monotonic() - started
    latencies.sort()
    return {
        "requests": sum(v for k, v in statuses.items() if not k.startswith("reuse")),
        "statuses": dict(statuses),
        "seconds": round(elapsed, 1),
        "per_second": round(len(latencies) / elapsed, 1) if elapsed else 0,
        "p50_ms": round(statistics.median(latencies) * 1000) if latencies else None,
        "p99_ms": round(latencies[int(len(latencies) * 0.99) - 1] * 1000) if latencies else None,
    }


def _alerts() -> list[dict]:
    return httpx.get(f"{PROMETHEUS}/api/v1/alerts", timeout=10).json()["data"]["alerts"]


def _key(alert: dict) -> str:
    labels = {k: v for k, v in alert["labels"].items() if k not in ("alertname", "severity")}
    return alert["labels"]["alertname"] + (json.dumps(labels, sort_keys=True) if labels else "")


def watch(path: str, interval: float = 5.0) -> None:
    """Append every alert state change to `path`, one JSON line each."""
    known: dict[str, str] = {}
    with open(path, "a", buffering=1) as out:
        while True:
            now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            try:
                current = {_key(a): a["state"] for a in _alerts()}
            except Exception as exc:  # Prometheus restarting
                print(f"{now} watch error: {exc}", file=sys.stderr)
                time.sleep(interval)
                continue
            for key, state in current.items():
                if known.get(key) != state:
                    out.write(json.dumps({"at": now, "alert": key, "state": state}) + "\n")
            for key in set(known) - set(current):
                out.write(json.dumps({"at": now, "alert": key, "state": "resolved"}) + "\n")
            known = current
            time.sleep(interval)


def wait(alertname: str, state: str, timeout: float) -> int:
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        try:
            alerts = _alerts()
        except httpx.HTTPError as exc:  # Prometheus busy or restarting: ask again
            print(f"prometheus: {exc!r}, retrying", file=sys.stderr)
            time.sleep(5)
            continue
        matching = [a for a in alerts if a["labels"]["alertname"] == alertname
                    and a["state"] == state]
        if matching:
            for alert in matching:
                print(json.dumps({"alert": _key(alert), "state": alert["state"],
                                  "activeAt": alert["activeAt"], "value": alert["value"],
                                  "seen_at": time.strftime("%H:%M:%SZ", time.gmtime())}))
            return 0
        time.sleep(5)
    print(f"{alertname} not {state} after {timeout:.0f}s", file=sys.stderr)
    return 1


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    a = sub.add_parser("accounts")
    a.add_argument("count", type=int)
    a.add_argument("--fund", type=int, default=10_000_000)
    p = sub.add_parser("pay")
    p.add_argument("accounts_file")
    p.add_argument("--count", type=int)
    p.add_argument("--duration", type=float)
    p.add_argument("--concurrency", type=int, default=16)
    p.add_argument("--to", default=None, help="one destination for every payment")
    p.add_argument("--amount", type=int, default=100)
    p.add_argument("--mismatch-every", type=int, default=0)
    w = sub.add_parser("watch")
    w.add_argument("path")
    t = sub.add_parser("wait")
    t.add_argument("alertname")
    t.add_argument("--state", default="firing")
    t.add_argument("--timeout", type=float, default=600)
    args = parser.parse_args()

    if args.command == "accounts":
        print(json.dumps(accounts(args.count, args.fund)))
    elif args.command == "pay":
        ids = json.load(open(args.accounts_file))
        print(json.dumps(pay(ids, count=args.count, duration=args.duration,
                             concurrency=args.concurrency, to=args.to,
                             mismatch_every=args.mismatch_every, amount=args.amount)))
    elif args.command == "watch":
        watch(args.path)
    elif args.command == "wait":
        return wait(args.alertname, args.state, args.timeout)
    return 0


if __name__ == "__main__":
    sys.exit(main())
