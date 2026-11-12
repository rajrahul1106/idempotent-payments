"""A fixed CPU workload, timed, for the thermal check (docs/results/README.md).

    python scripts/cpu_bench.py
    {"single_thread_s": 0.29, "eight_processes_s": 1.41}

The same pure-Python integer loop every time: once on one core (best of
three), and on eight processes at once (every core of the M2). On a machine
that holds its clock speed the times stay put from run to run; if the CPU
slows itself down to shed heat they rise, with nothing else changed. Run it
while the stack is idle, so it measures the CPU and not contention.
"""

from __future__ import annotations

import json
import multiprocessing
import time

SINGLE_N = 5_000_000
EIGHT_N = 20_000_000


def work(n: int) -> float:
    started = time.perf_counter()
    x = 0
    for i in range(n):
        x += i * i
    return time.perf_counter() - started


def main() -> None:
    single = min(work(SINGLE_N) for _ in range(3))
    with multiprocessing.Pool(8) as pool:
        pool.map(work, [1_000] * 8)  # start the workers before the clock
        started = time.perf_counter()
        pool.map(work, [EIGHT_N] * 8)
        eight = time.perf_counter() - started
    print(json.dumps({"single_thread_s": round(single, 3), "eight_processes_s": round(eight, 3)}))


if __name__ == "__main__":
    main()
