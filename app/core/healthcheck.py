"""Container healthcheck: is this process's main loop still going round?

    python -m app.core.healthcheck                 # a worker: reads its /metrics
    python -m app.core.healthcheck --url http://localhost:8000/health

For a worker it reads process_heartbeat_timestamp_seconds from the local
/metrics and fails if the heartbeat is older than --max-age seconds. A
process that is up but wedged (a stuck poll loop, a hung connection) fails
this, where a port check would pass. For the API it requires a 200 from
/health. Standard library only, so it runs in the slim image.
"""

from __future__ import annotations

import argparse
import sys
import time
import urllib.request

HEARTBEAT = "process_heartbeat_timestamp_seconds"


def check(url: str, max_age: float) -> str | None:
    """None when healthy, otherwise why not."""
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            body = response.read().decode("utf-8")
            status = response.status
    except Exception as exc:
        return f"{url}: {exc}"
    if status != 200:
        return f"{url}: HTTP {status}"
    if not url.endswith("/metrics"):
        return None
    for line in body.splitlines():
        if line.startswith(HEARTBEAT + " "):
            age = time.time() - float(line.split()[1])
            return None if age <= max_age else f"heartbeat is {age:.0f}s old"
    return "no heartbeat metric"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:9100/metrics")
    parser.add_argument("--max-age", type=float, default=60.0)
    args = parser.parse_args(argv)
    problem = check(args.url, args.max_age)
    if problem:
        print(f"unhealthy: {problem}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
