# Chaos and load results

## Machine

- Host: MacBook Air, Apple M2, 8 GB, fanless: under sustained load it lowers its CPU clock to manage heat
- Docker VM: 3.83 GB memory, 8 CPUs, no other projects or Docker extensions running
- Power: AC Power, Low Power Mode off (recorded by `pmset` at the start of every run since Day 5a)
- The whole stack runs in that VM: 13 containers, including Postgres, Redpanda (512 MB), Jaeger (512 MB cap), Prometheus and Grafana; one API process (uvicorn, one worker). The load generator runs on the host.
- Every number below comes from this machine. 6 GB or more for Docker is advised where available (docs/ARCHITECTURE.md §15, limitation 16).

## How a run is judged

`scripts/chaos_run.py` (`make chaos`) sends a seeded plan of operations: 15% resend an earlier idempotency key with the same body, 1% are malformed messages published straight to `payments.main`, about 10% of new payments involve one hot account. The client retries a 503 (busy account, database down) and a 409 (same key in flight) with the same key after `Retry-After`, as a well-behaved client would. After the fault and the load, the pipeline must drain on its own and the run is checked against the manifest of every request: one transaction per key the client was paid for (a 201, or a 200 replay when the 201 was lost to the fault), one `payment_events` row per event, the DLQ exactly the injected poison, nothing sent to a retry topic, balances equal to the arithmetic of those payments, I1 to I4 passing, and no 500 at all. A run is INVALID (resource), never FAILED, if the Docker VM's free memory fell below 300 MB.

| Scenario | Fault |
| --- | --- |
| consumer-kill | two consumers; SIGKILL one at a random point |
| broker-outage | `docker compose stop redpanda` for 60 s mid-load, then start |
| db-restart | `docker compose restart postgres` mid-load (a blip of under a second) |
| relay-kill | SIGKILL the relay, start it 10 s later |
| db-outage | `docker compose stop postgres` for 60 s mid-load, then start; the API must answer 503 to every request made while it is down |

Recovery is the time from the restore action (or the kill) until every payment committed before it had been applied end to end, from database timestamps. For db-outage the restore is the moment Postgres accepts connections again.

## Steady-state baseline (no faults)

`scripts/baseline_run.py` (`make baseline`), run 2026-11-12 14:29:16-14:40:55 UTC, first, on a cool machine. The same mix as the chaos suite, 201 accounts, one consumer, 120 s per step at a fixed number of concurrent clients, the pipeline drained between steps. Latency is per HTTP attempt as the client saw it. An error is any status but 200 or 201. The client rotates each account's requests over 64 token subjects, so the API's per-subject rate limit (100 a minute) is never what it measures. A first attempt was stopped after its first step for exactly that reason: with subjects tied to 4 worker slots, the hot account drew 12 × 429. No result file was kept from it.

| Clients | Started (UTC) | Req/s | Created/s | p50 ms | p95 ms | p99 ms | Errors | Statuses | Within p99 < 500 ms, 0 errors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4 | 14:29:46 | 205.2 | 174.9 | 18.5 | 32.2 | 47.8 | 0 | 200: 3,624, 201: 20,937 | yes |
| 8 | 14:31:59 | 180.4 | 153.4 | 42.7 | 72.1 | 99.1 | 0 | 200: 3,233, 201: 18,341 | yes |
| 16 | 14:34:12 | 161.7 | 137.2 | 96.6 | 160.0 | 205.2 | 0 | 200: 2,925, 201: 16,420 | yes |
| 32 | 14:36:26 | 162.6 | 138.2 | 177.1 | 408.0 | 612.7 | 0 | 200: 2,915, 201: 16,551 | no |
| 64 | 14:38:39 | 155.4 | 132.5 | 377.9 | 726.3 | 937.1 | 0 | 200: 2,749, 201: 15,883 | no |

**Sustained throughput: 205.2 requests/s (174.9 payments created/s) at 4 clients, p99 47.8 ms, zero errors.** The highest step that met both conditions.

### Where the time went, per step

Cores are CPU seconds per second of the step: container figures from each container's cgroup, the VM figure from `/proc/stat` (8 CPUs), the client's from its own process. Pool, NOTIFY and row-lock figures are 1-second samples of the API's `db_pool_connections` and of `pg_locks` (mean / max). Relay and consumer rates, backlog and lag are from Prometheus over the step.

| Clients | API cores | Postgres cores | Relay cores | VM busy cores | Client cores | API pool in use (of 25) | NOTIFY lock waiters | Row-lock waiters | Created/s vs relay published/s | Outbox max | Lag max | VM min free MB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4 | 1.09 | 0.32 | 0.3 | 2.51 | 0.2 | 3.1 / 4 | 0.01 / 1 | 0.02 / 1 | 169 vs 162 | 3 | 2 | 1335 |
| 8 | 1.14 | 0.37 | 0.29 | 2.56 | 0.19 | 5.82 / 8 | 0.07 / 7 | 0.12 / 2 | 152 vs 147 | 1 | 2 | 1295 |
| 16 | 1.16 | 0.4 | 0.28 | 2.61 | 0.18 | 11.06 / 16 | 0.02 / 2 | 0.79 / 5 | 125 vs 133 | 2 | 4 | 1319 |
| 32 | 1.17 | 0.46 | 0.29 | 2.65 | 0.18 | 21.92 / 25 | 0.01 / 1 | 4.62 / 14 | 131 vs 125 | 2 | 3 | 1282 |
| 64 | 1.16 | 0.48 | 0.28 | 2.67 | 0.18 | 24.51 / 25 | 0.0 / 0 | 6.8 / 16 | 129 vs 125 | 2 | 3 | 1264 |

What active Postgres backends were waiting on (mean backends per sample):

| Clients | Wait events |
| --- | --- |
| 4 | Client:ClientRead 0.15, IO:WALSync 0.14, CPU:running 0.13, Lock:object 0.01, Lock:transactionid 0.01 |
| 8 | Lock:transactionid 0.14, IO:WALSync 0.13, CPU:running 0.12, Lock:object 0.07, Client:ClientRead 0.05, IO:DataFileRead 0.02 |
| 16 | Lock:transactionid 0.68, CPU:running 0.16, IO:WALSync 0.15, Lock:tuple 0.1, Client:ClientRead 0.03, LWLock:WALWrite 0.02 |
| 32 | Lock:transactionid 2.33, Lock:tuple 2.27, CPU:running 0.18, IO:WALSync 0.15, Client:ClientRead 0.08, Lock:object 0.01 |
| 64 | Lock:tuple 3.69, Lock:transactionid 3.13, CPU:running 0.21, Client:ClientRead 0.15, IO:WALSync 0.08, IO:DataFileRead 0.02 |

API latency inside the server (Prometheus `payment_duration_seconds`, bucketed) against the client's view:

| Clients | Server p50 s | Server p99 s | Client p50 ms | Client p99 ms |
| --- | --- | --- | --- | --- |
| 4 | 0.017 | 0.049 | 18.5 | 47.8 |
| 8 | 0.038 | 0.099 | 42.7 | 99.1 |
| 16 | 0.086 | 0.247 | 96.6 | 205.2 |
| 32 | 0.175 | 0.819 | 177.1 | 612.7 |
| 64 | 0.389 | 0.986 | 377.9 | 937.1 |

### What limited it

**The single API process.** Its container used 1.09 to 1.17 cores at every step, from 4 clients to 64, while the whole Docker VM used about 2.5 of its 8 cores, Postgres 0.3 to 0.5 and the load client 0.2. The API is one uvicorn worker, one Python interpreter, and the interpreter's global lock lets it run about one core of Python at a time. It was saturated at the first step, so every client beyond four only waited. With more requests in flight, more of them waited on the hot account's row lock (row-lock waiters: mean 0.02 at 4 clients, 6.8 at 64; `Lock:tuple` and `Lock:transactionid` lead the wait events from 16 clients). Each waiting request held a pooled connection: the pool was at its 25-connection ceiling in 28% of samples at 32 clients and 60% at 64. The API also spent more CPU per request: 5.3 ms at 4 clients, 7.5 ms at 64. The server-side p99 tracks the client's, so requests queued inside the API, not in front of it. The pool ceiling is a consequence, not the cause: throughput stopped rising at 4 clients, and the pool first filled at 32.

The fall from 205 to 155 requests a second as clients were added cannot be put on concurrency alone. The steps ran in rising order over 11 minutes while the machine warmed: the eight-process CPU benchmark took 1.16 s before the baseline and 1.55 s after it. More CPU per request is what both more contention and a slower clock would cause (see the thermal check). The finding that does not depend on clock speed is that the API container sat at about 1.1 cores at every step while everything else had headroom.

Not the limit at this load:

- **The NOTIFY commit lock** (§15 limitation 11): mean waiters at most 0.07, with a maximum of 7 in a single sample.
- **The relay's per-message acknowledgement** (limitation 10): it published as fast as payments were created, with never more than 3 rows unpublished and the publish delay p95 at most 0.39 s, using 0.3 of a core.
- **Docker VM memory:** at least 1,264 MB free throughout.

The API caps the load at about 205 requests a second, so neither the NOTIFY lock nor the relay was pushed to its own limit; this run does not say where those would bind.

## Three consecutive full runs at 20,000 operations

`make chaos` (all five scenarios) three times in a row. A FAILED or INVALID run would have reset the count to zero; every attempt is listed under [Every run](#every-run).

| Round | Scenario | Started (UTC) | Ended (UTC) | Verdict | Req/s | p50 ms | p95 ms | p99 ms | 503s | 500s | Recovery s | Drain s | VM min free MB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | consumer-kill | 14:43:18 | 14:45:34 | PASS | 181.6 | 85.2 | 144.3 | 189.0 | 0 | 0 | 52.0 | 0.3 | 1236 |
| 1 | broker-outage | 14:45:38 | 14:49:01 | PASS | 109.1 | 97.0 | 204.2 | 304.1 | 0 | 0 | 66.2 | 6.9 | 1203 |
| 1 | db-restart | 14:49:07 | 14:51:51 | PASS | 133.7 | 114.5 | 192.2 | 245.8 | 16 | 0 | 0.0 | 0.9 | 1209 |
| 1 | relay-kill | 14:51:57 | 14:55:02 | PASS | 116.8 | 128.6 | 237.6 | 318.6 | 0 | 0 | 5.7 | 0.4 | 1261 |
| 1 | db-outage | 14:55:07 | 14:58:45 | PASS | 102.5 | 105.7 | 195.7 | 265.6 | 960 | 0 | 0.0 | 0.3 | 1237 |
| 2 | consumer-kill | 14:58:51 | 15:02:19 | PASS | 109.5 | 136.6 | 258.7 | 361.1 | 0 | 0 | 50.7 | 0.6 | 1230 |
| 2 | broker-outage | 15:02:23 | 15:06:05 | PASS | 102.4 | 101.4 | 210.6 | 301.2 | 0 | 0 | 57.6 | 13.8 | 1239 |
| 2 | db-restart | 15:06:09 | 15:09:46 | PASS | 99.5 | 147.8 | 281.5 | 382.1 | 32 | 0 | 0.8 | 0.7 | 1251 |
| 2 | relay-kill | 15:09:51 | 15:13:09 | PASS | 108.7 | 137.4 | 265.2 | 380.1 | 0 | 0 | 6.5 | 0.8 | 1378 |
| 2 | db-outage | 15:13:14 | 15:16:57 | PASS | 99.8 | 104.2 | 217.3 | 303.0 | 960 | 0 | 0.0 | 0.7 | 1379 |
| 3 | consumer-kill | 15:17:03 | 15:20:19 | PASS | 116.8 | 122.7 | 261.9 | 372.1 | 0 | 0 | 48.1 | 0.4 | 1341 |
| 3 | broker-outage | 15:20:24 | 15:23:47 | PASS | 111.2 | 92.2 | 174.1 | 256.8 | 0 | 0 | 56.0 | 13.4 | 1398 |
| 3 | db-restart | 15:23:51 | 15:26:45 | PASS | 123.0 | 116.3 | 248.1 | 344.2 | 16 | 0 | 1.3 | 0.5 | 1345 |
| 3 | relay-kill | 15:26:52 | 15:29:44 | PASS | 127.1 | 115.8 | 228.3 | 324.6 | 0 | 0 | 5.7 | 0.4 | 1461 |
| 3 | db-outage | 15:29:49 | 15:33:20 | PASS | 104.8 | 99.4 | 192.0 | 284.6 | 960 | 0 | 0.0 | 0.8 | 1419 |

Consecutive clean full runs: **3**.

### The 60-second database outage

| Round | Outage s | Requests while down | Their statuses | Postgres start to accepting s | First 201 after accepting s | Pipeline recovery s |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 60.1 | 944 | {'503': 944} | 0.3 | 0.06 | 0.0 |
| 2 | 60.1 | 928 | {'503': 928} | 0.2 | 0.75 | 0.0 |
| 3 | 60.2 | 928 | {'503': 928} | 0.3 | 0.65 | 0.0 |

Pipeline recovery 0.0 means every payment committed before the stop had already been applied by the time Postgres accepted connections again: nothing was left waiting.

Consumers sent nothing to a retry topic or the DLQ in any of them (`nothing_sent_to_retry_topics` and `dlq_is_exactly_the_poison` in each run's JSON).

## Thermal check

The same scenario, round after round. Throughput is requests per second of the run's traffic phase. VM CPU per 1,000 requests is busy CPU seconds of the whole Docker VM (`/proc/stat`) during traffic, divided by requests: the work done per request, which should not change if the code does not. The benchmark is `scripts/cpu_bench.py`, a fixed CPU loop timed on the host while the stack is idle, before and after each run: if the CPU slows itself to shed heat it takes longer.

| Scenario | Req/s R1 | Req/s R2 | Req/s R3 | VM CPU s per 1k req R1 | VM CPU s per 1k req R2 | VM CPU s per 1k req R3 | API CPU s R1 | API CPU s R2 | API CPU s R3 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| consumer-kill | 181.6 | 109.5 | 116.8 | 14.29 | 25.39 | 24.06 | 127.1 | 212.3 | 196.9 |
| broker-outage | 109.1 | 102.4 | 111.2 | 20.3 | 20.59 | 17.98 | 170.8 | 169.3 | 148.7 |
| db-restart | 133.7 | 99.5 | 123.0 | 19.5 | 29.02 | 22.61 | 171.0 | 227.8 | 186.0 |
| relay-kill | 116.8 | 108.7 | 127.1 | 23.16 | 26.34 | 22.0 | 198.0 | 209.9 | 182.7 |
| db-outage | 102.5 | 99.8 | 104.8 | 19.72 | 20.48 | 19.34 | 169.2 | 172.8 | 162.6 |

CPU benchmark (seconds; lower is faster), in run order:

| Run | Started (UTC) | Before: 1 core | Before: 8 processes | After: 1 core | After: 8 processes |
| --- | --- | --- | --- | --- | --- |
| baseline | 14:29:16 | 0.171 | 1.157 | 0.171 | 1.545 |
| R1 consumer-kill | 14:43:18 | 0.184 | 1.285 | 0.172 | 1.309 |
| R1 broker-outage | 14:45:38 | 0.173 | 1.291 | 0.168 | 1.883 |
| R1 db-restart | 14:49:07 | 0.167 | 1.819 | 0.231 | 2.484 |
| R1 relay-kill | 14:51:57 | 0.175 | 1.886 | 0.208 | 2.471 |
| R1 db-outage | 14:55:07 | 0.175 | 1.773 | 0.179 | 1.793 |
| R2 consumer-kill | 14:58:51 | 0.167 | 1.291 | 0.209 | 2.276 |
| R2 broker-outage | 15:02:23 | 0.176 | 1.832 | 0.181 | 2.426 |
| R2 db-restart | 15:06:09 | 0.177 | 2.341 | 0.241 | 2.726 |
| R2 relay-kill | 15:09:51 | 0.173 | 1.959 | 0.213 | 2.26 |
| R2 db-outage | 15:13:14 | 0.171 | 1.827 | 0.173 | 1.331 |
| R3 consumer-kill | 15:17:03 | 0.174 | 1.208 | 0.214 | 2.04 |
| R3 broker-outage | 15:20:24 | 0.169 | 1.61 | 0.168 | 1.228 |
| R3 db-restart | 15:23:51 | 0.166 | 1.174 | 0.211 | 2.156 |
| R3 relay-kill | 15:26:52 | 0.17 | 1.998 | 0.17 | 1.683 |
| R3 db-outage | 15:29:49 | 0.171 | 1.851 | 0.17 | 1.236 |

**Throughput did not fall round after round.** Each scenario compared with itself, rounds 1, 2 and 3 in requests a second:

- consumer-kill: 181.6, 109.5, 116.8
- db-restart: 133.7, 99.5, 123.0
- broker-outage: 109.1, 102.4, 111.2
- relay-kill: 116.8, 108.7, 127.1
- db-outage: 102.5, 99.8, 104.8

Round 3 was faster than round 2 in all five scenarios. Correctness passed in every run.

What changed was how much CPU time the same work took, and that follows the machine's heat, not the code. No code changed between runs, and each scenario replays the same seeded plan, so its requests are identical from round to round (19,790 for consumer-kill, 20,738 for db-outage).

- **Throughput fell where CPU per request rose.** The scenarios that lost the most throughput are the ones whose identical requests cost the most extra CPU. For consumer-kill the API container used 127 CPU seconds in round 1, then 212 and 197. The whole VM used 14.3, then 25.4 and 24.1 CPU seconds per 1,000 requests. db-restart went 19.5, 29.0, 22.6, with throughput 133.7, 99.5, 123.0. The API process is CPU-bound, so when each CPU second does less work it serves fewer requests a second. That is what a lower clock does.
- **The fixed CPU benchmark slowed.** On eight processes it took 1.16 s on the cool machine before the baseline, and between 1.23 and 2.73 s after the chaos runs, with the stack drained: the same work, up to 2.4 times as long.
- **What rules out the obvious alternatives.** `pmset` recorded AC power and Low Power Mode off before and after every run, and no thermal warning. The database grew by about 20,000 payments per run, but that cannot explain the pattern: round 3 needed less CPU per request than round 2 in all five scenarios, on a larger database.

So this is consistent with thermal throttling on a fanless laptop, not a code regression. It was not a steady decline either: the machine was hot within the first runs and then varied from run to run. Throughput figures from this machine are therefore a range, about 100 to 180 requests a second under the chaos mix, and 205 at best when cool (the baseline's first step).

**Not observed directly:** the clock speed itself. macOS reports it only to root (`powermetrics`), and these runs did not use root.

## Peak memory per service, three rounds (MB, highest of the rounds)

| Service | consumer-kill | broker-outage | db-restart | relay-kill | db-outage |
| --- | --- | --- | --- | --- | --- |
| api | 125 | 116 | 113 | 121 | 111 |
| consumer | 82 | 101 | 77 | 84 | 86 |
| consumer-2 | 89 | - | - | - | - |
| dlq-writer | 67 | 68 | 65 | 71 | 71 |
| grafana | 196 | 126 | 100 | 106 | 97 |
| jaeger | 492 | 493 | 511 | 488 | 492 |
| postgres | 466 | 667 | 683 | 558 | 787 |
| prometheus | 124 | 87 | 82 | 82 | 84 |
| reconciliation | 88 | 93 | 85 | 83 | 76 |
| redpanda | 498 | 491 | 491 | 494 | 492 |
| relay | 84 | 78 | 82 | 86 | 84 |
| retry-10m | 67 | 80 | 66 | 52 | 59 |
| retry-1m | 66 | 66 | 63 | 44 | 48 |
| retry-5s | 65 | 78 | 57 | 44 | 48 |

## Load of the three rounds

| Round | Scenario | Payment requests | Same-key resends | Poison | Hot-account share | Final statuses |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | consumer-kill | 16,820 | 2,970 | 210 | 10.6% | {'201': 16820, '200': 2970} |
| 1 | broker-outage | 16,834 | 2,960 | 206 | 9.8% | {'201': 16834, '200': 2960} |
| 1 | db-restart | 16,845 | 2,933 | 222 | 10.3% | {'201': 16845, '200': 2933} |
| 1 | relay-kill | 16,783 | 2,996 | 221 | 9.9% | {'201': 16783, '200': 2996} |
| 1 | db-outage | 16,755 | 3,023 | 222 | 10.0% | {'201': 16755, '200': 3023} |
| 2 | consumer-kill | 16,820 | 2,970 | 210 | 10.6% | {'201': 16820, '200': 2970} |
| 2 | broker-outage | 16,834 | 2,960 | 206 | 9.8% | {'201': 16834, '200': 2960} |
| 2 | db-restart | 16,845 | 2,933 | 222 | 10.3% | {'201': 16845, '200': 2933} |
| 2 | relay-kill | 16,783 | 2,996 | 221 | 9.9% | {'201': 16783, '200': 2996} |
| 2 | db-outage | 16,755 | 3,023 | 222 | 10.0% | {'201': 16755, '200': 3023} |
| 3 | consumer-kill | 16,820 | 2,970 | 210 | 10.6% | {'201': 16820, '200': 2970} |
| 3 | broker-outage | 16,834 | 2,960 | 206 | 9.8% | {'201': 16834, '200': 2960} |
| 3 | db-restart | 16,845 | 2,933 | 222 | 10.3% | {'201': 16845, '200': 2933} |
| 3 | relay-kill | 16,783 | 2,996 | 221 | 9.9% | {'201': 16783, '200': 2996} |
| 3 | db-outage | 16,755 | 3,023 | 222 | 10.0% | {'201': 16755, '200': 3023} |

## Every run

Every chaos run on this machine, in order, including the Phase 8 ramp, its INVALID run and its 50,000-operation runs.

| Run | Round | Verdict | Req/s | p50 ms | p95 ms | p99 ms | 503s | 500s | Recovery s | Drain s | VM min free MB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [relay-kill-5000-20261112T022434](chaos-relay-kill-5000-20261112T022434.json) | - | PASS | 176.1 | 83.6 | 159.1 | 279.4 | 0 | 0 | None | 6.4 | 531 |
| [db-restart-5000-20261112T022540](chaos-db-restart-5000-20261112T022540.json) | - | PASS | 161.8 | 89.5 | 167.4 | 231.2 | 16 | 0 | 0.6 | 0.2 | 356 |
| [broker-outage-5000-20261112T022632](chaos-broker-outage-5000-20261112T022632.json) | - | INVALID (resource) | 59.0 | 75.6 | 166.4 | 254.2 | 0 | 0 | 0.0 | 31.9 | 285 |
| [broker-outage-5000-20261112T025959](chaos-broker-outage-5000-20261112T025959.json) | - | PASS | 60.7 | 68.3 | 144.4 | 289.9 | 0 | 0 | 47.9 | 44.4 | 1596 |
| [consumer-kill-5000-20261112T030213](chaos-consumer-kill-5000-20261112T030213.json) | - | PASS | 170.6 | 85.0 | 159.2 | 354.4 | 0 | 0 | 47.1 | 38.0 | 1424 |
| [consumer-kill-20000-20261112T030350](chaos-consumer-kill-20000-20261112T030350.json) | - | PASS | 176.3 | 84.9 | 157.0 | 239.5 | 0 | 0 | 49.6 | 0.2 | 1363 |
| [broker-outage-20000-20261112T030603](chaos-broker-outage-20000-20261112T030603.json) | - | PASS | 123.3 | 80.1 | 146.5 | 232.2 | 0 | 0 | 54.9 | 13.0 | 1308 |
| [db-restart-20000-20261112T030906](chaos-db-restart-20000-20261112T030906.json) | - | PASS | 159.6 | 92.4 | 173.4 | 286.2 | 16 | 0 | 0.0 | 0.2 | 1325 |
| [relay-kill-20000-20261112T031119](chaos-relay-kill-20000-20261112T031119.json) | - | PASS | 135.2 | 108.7 | 218.8 | 318.5 | 0 | 0 | 5.9 | 0.3 | 1257 |
| [consumer-kill-50000-20261112T031413](chaos-consumer-kill-50000-20261112T031413.json) | - | PASS | 156.5 | 93.8 | 186.7 | 288.4 | 0 | 0 | 48.3 | 0.4 | 1212 |
| [broker-outage-50000-20261112T031955](chaos-broker-outage-50000-20261112T031955.json) | - | PASS | 137.2 | 91.0 | 181.8 | 292.0 | 0 | 0 | 62.2 | 0.3 | 1257 |
| [db-restart-50000-20261112T032610](chaos-db-restart-50000-20261112T032610.json) | - | PASS | 160.4 | 91.5 | 177.8 | 292.4 | 16 | 0 | 0.4 | 0.6 | 1307 |
| [relay-kill-50000-20261112T033131](chaos-relay-kill-50000-20261112T033131.json) | - | PASS | 158.6 | 92.8 | 179.7 | 310.5 | 0 | 0 | 4.5 | 2.5 | 1299 |
| [db-outage-3000-20261112T201120](chaos-db-outage-3000-20261112T201120.json) | - | PASS | 49.9 | 77.8 | 144.5 | 205.3 | 960 | 0 | 0.0 | 0.1 | 1274 |
| [consumer-kill-20000-20261112T201318](chaos-consumer-kill-20000-20261112T201318.json) | 1 | PASS | 181.6 | 85.2 | 144.3 | 189.0 | 0 | 0 | 52.0 | 0.3 | 1236 |
| [broker-outage-20000-20261112T201538](chaos-broker-outage-20000-20261112T201538.json) | 1 | PASS | 109.1 | 97.0 | 204.2 | 304.1 | 0 | 0 | 66.2 | 6.9 | 1203 |
| [db-restart-20000-20261112T201907](chaos-db-restart-20000-20261112T201907.json) | 1 | PASS | 133.7 | 114.5 | 192.2 | 245.8 | 16 | 0 | 0.0 | 0.9 | 1209 |
| [relay-kill-20000-20261112T202157](chaos-relay-kill-20000-20261112T202157.json) | 1 | PASS | 116.8 | 128.6 | 237.6 | 318.6 | 0 | 0 | 5.7 | 0.4 | 1261 |
| [db-outage-20000-20261112T202507](chaos-db-outage-20000-20261112T202507.json) | 1 | PASS | 102.5 | 105.7 | 195.7 | 265.6 | 960 | 0 | 0.0 | 0.3 | 1237 |
| [consumer-kill-20000-20261112T202851](chaos-consumer-kill-20000-20261112T202851.json) | 2 | PASS | 109.5 | 136.6 | 258.7 | 361.1 | 0 | 0 | 50.7 | 0.6 | 1230 |
| [broker-outage-20000-20261112T203223](chaos-broker-outage-20000-20261112T203223.json) | 2 | PASS | 102.4 | 101.4 | 210.6 | 301.2 | 0 | 0 | 57.6 | 13.8 | 1239 |
| [db-restart-20000-20261112T203609](chaos-db-restart-20000-20261112T203609.json) | 2 | PASS | 99.5 | 147.8 | 281.5 | 382.1 | 32 | 0 | 0.8 | 0.7 | 1251 |
| [relay-kill-20000-20261112T203951](chaos-relay-kill-20000-20261112T203951.json) | 2 | PASS | 108.7 | 137.4 | 265.2 | 380.1 | 0 | 0 | 6.5 | 0.8 | 1378 |
| [db-outage-20000-20261112T204314](chaos-db-outage-20000-20261112T204314.json) | 2 | PASS | 99.8 | 104.2 | 217.3 | 303.0 | 960 | 0 | 0.0 | 0.7 | 1379 |
| [consumer-kill-20000-20261112T204703](chaos-consumer-kill-20000-20261112T204703.json) | 3 | PASS | 116.8 | 122.7 | 261.9 | 372.1 | 0 | 0 | 48.1 | 0.4 | 1341 |
| [broker-outage-20000-20261112T205024](chaos-broker-outage-20000-20261112T205024.json) | 3 | PASS | 111.2 | 92.2 | 174.1 | 256.8 | 0 | 0 | 56.0 | 13.4 | 1398 |
| [db-restart-20000-20261112T205351](chaos-db-restart-20000-20261112T205351.json) | 3 | PASS | 123.0 | 116.3 | 248.1 | 344.2 | 16 | 0 | 1.3 | 0.5 | 1345 |
| [relay-kill-20000-20261112T205652](chaos-relay-kill-20000-20261112T205652.json) | 3 | PASS | 127.1 | 115.8 | 228.3 | 324.6 | 0 | 0 | 5.7 | 0.4 | 1461 |
| [db-outage-20000-20261112T205949](chaos-db-outage-20000-20261112T205949.json) | 3 | PASS | 104.8 | 99.4 | 192.0 | 284.6 | 960 | 0 | 0.0 | 0.8 | 1419 |

The first run (relay-kill, 5,000) used an earlier definition of recovery that needed a payment created after the restore; it recorded none, hence `None`.

Each chaos run also writes a manifest of every request and its responses to `docs/results/manifests/chaos-<run>.jsonl.gz` (gzipped JSON lines). Manifests are kept on the machine that ran them and are not committed; the summary JSON is what the repo keeps.
