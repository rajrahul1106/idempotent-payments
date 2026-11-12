# Chaos results

## Machine

- Host: MacBook Air, Apple M2, 8 GB (Apple M2, 8 GB)
- Docker VM: 3.83 GB memory, 8 CPUs, no other projects or Docker extensions running
- The whole stack runs in that VM: 13 containers, including Postgres, Redpanda (512 MB), Jaeger (512 MB cap), Prometheus and Grafana. The load generator runs on the host.
- Every number below comes from this machine. 6 GB or more for Docker is advised where available (BLUEPRINT.md §15, limitation 16).

## How a run is judged

`scripts/chaos_run.py` (`make chaos`) sends a seeded plan of operations: 15% resend an earlier idempotency key with the same body, 1% are malformed messages published straight to `payments.main`, about 10% of new payments involve one hot account. The client retries a 503 with the same key after `Retry-After`. After the fault and the load, the pipeline must drain on its own and the run is checked against the manifest of every request: one transaction per key answered 201, one `payment_events` row per event, the DLQ exactly the injected poison, nothing sent to a retry topic, balances equal to the arithmetic of the 201s, I1 to I4 passing, and no 500 at all. A run is INVALID (resource), never FAILED, if the Docker VM's free memory fell below 300 MB.

Recovery is the time from the restore action (or the kill) until every payment committed before it had been applied end to end, from database timestamps.

## Runs

| Run | Verdict | Req/s | p50 ms | p95 ms | p99 ms | 503s | 500s | Recovery s | Drain s | VM min free MB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [relay-kill-5000-20261112T022434](chaos-relay-kill-5000-20261112T022434.json) | PASS | 176.1 | 83.6 | 159.1 | 279.4 | 0 | 0 | None | 6.4 | 531 |
| [db-restart-5000-20261112T022540](chaos-db-restart-5000-20261112T022540.json) | PASS | 161.8 | 89.5 | 167.4 | 231.2 | 16 | 0 | 0.6 | 0.2 | 356 |
| [broker-outage-5000-20261112T022632](chaos-broker-outage-5000-20261112T022632.json) | INVALID (resource) | 59.0 | 75.6 | 166.4 | 254.2 | 0 | 0 | 0.0 | 31.9 | 285 |
| [broker-outage-5000-20261112T025959](chaos-broker-outage-5000-20261112T025959.json) | PASS | 60.7 | 68.3 | 144.4 | 289.9 | 0 | 0 | 47.9 | 44.4 | 1596 |
| [consumer-kill-5000-20261112T030213](chaos-consumer-kill-5000-20261112T030213.json) | PASS | 170.6 | 85.0 | 159.2 | 354.4 | 0 | 0 | 47.1 | 38.0 | 1424 |
| [consumer-kill-20000-20261112T030350](chaos-consumer-kill-20000-20261112T030350.json) | PASS | 176.3 | 84.9 | 157.0 | 239.5 | 0 | 0 | 49.6 | 0.2 | 1363 |
| [broker-outage-20000-20261112T030603](chaos-broker-outage-20000-20261112T030603.json) | PASS | 123.3 | 80.1 | 146.5 | 232.2 | 0 | 0 | 54.9 | 13.0 | 1308 |
| [db-restart-20000-20261112T030906](chaos-db-restart-20000-20261112T030906.json) | PASS | 159.6 | 92.4 | 173.4 | 286.2 | 16 | 0 | 0.0 | 0.2 | 1325 |
| [relay-kill-20000-20261112T031119](chaos-relay-kill-20000-20261112T031119.json) | PASS | 135.2 | 108.7 | 218.8 | 318.5 | 0 | 0 | 5.9 | 0.3 | 1257 |
| [consumer-kill-50000-20261112T031413](chaos-consumer-kill-50000-20261112T031413.json) | PASS | 156.5 | 93.8 | 186.7 | 288.4 | 0 | 0 | 48.3 | 0.4 | 1212 |
| [broker-outage-50000-20261112T031955](chaos-broker-outage-50000-20261112T031955.json) | PASS | 137.2 | 91.0 | 181.8 | 292.0 | 0 | 0 | 62.2 | 0.3 | 1257 |
| [db-restart-50000-20261112T032610](chaos-db-restart-50000-20261112T032610.json) | PASS | 160.4 | 91.5 | 177.8 | 292.4 | 16 | 0 | 0.4 | 0.6 | 1307 |
| [relay-kill-50000-20261112T033131](chaos-relay-kill-50000-20261112T033131.json) | PASS | 158.6 | 92.8 | 179.7 | 310.5 | 0 | 0 | 4.5 | 2.5 | 1299 |

## Peak memory per service, runs of 20,000 events or more (MB)

| Service | consumer-kill 20k | broker-outage 20k | db-restart 20k | relay-kill 20k | consumer-kill 50k | broker-outage 50k | db-restart 50k | relay-kill 50k |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| api | 93 | 107 | 110 | 102 | 112 | 100 | 99 | 98 |
| consumer | 88 | 98 | 92 | 91 | 79 | 95 | 73 | 83 |
| consumer-2 | 70 | - | - | - | 77 | - | - | - |
| dlq-writer | 76 | 77 | 79 | 79 | 66 | 67 | 79 | 70 |
| grafana | 208 | 210 | 214 | 214 | 214 | 214 | 132 | 202 |
| jaeger | 406 | 469 | 512 | 512 | 512 | 512 | 422 | 431 |
| postgres | 256 | 294 | 297 | 216 | 362 | 311 | 541 | 625 |
| prometheus | 120 | 123 | 123 | 125 | 127 | 71 | 71 | 118 |
| reconciliation | 70 | 83 | 65 | 65 | 79 | 79 | 68 | 68 |
| redpanda | 250 | 264 | 270 | 280 | 306 | 332 | 357 | 420 |
| relay | 69 | 80 | 82 | 69 | 80 | 69 | 83 | 102 |
| retry-10m | 65 | 75 | 66 | 66 | 79 | 78 | 78 | 67 |
| retry-1m | 65 | 75 | 66 | 66 | 79 | 77 | 66 | 67 |
| retry-5s | 65 | 66 | 66 | 67 | 78 | 77 | 77 | 66 |

## Load of the runs of 20,000 events or more

| Run | Payment requests | Same-key resends | Poison | Hot-account share | Final statuses |
| --- | --- | --- | --- | --- | --- |
| consumer-kill 20k | 16,820 | 2,970 | 210 | 10.6% | {'201': 16820, '200': 2970} |
| broker-outage 20k | 16,834 | 2,960 | 206 | 9.8% | {'201': 16834, '200': 2960} |
| db-restart 20k | 16,845 | 2,933 | 222 | 10.3% | {'201': 16845, '200': 2933} |
| relay-kill 20k | 16,783 | 2,996 | 221 | 9.9% | {'201': 16783, '200': 2996} |
| consumer-kill 50k | 41,968 | 7,522 | 510 | 10.2% | {'201': 41968, '200': 7522} |
| broker-outage 50k | 42,037 | 7,467 | 496 | 10.1% | {'201': 42037, '200': 7467} |
| db-restart 50k | 42,100 | 7,378 | 522 | 10.0% | {'201': 42100, '200': 7378} |
| relay-kill 50k | 41,952 | 7,530 | 518 | 9.8% | {'201': 41952, '200': 7530} |

The first run (relay-kill, 5,000) used an earlier definition of recovery that needed a payment created after the restore; it recorded none, hence `None`.

Each run also writes a manifest of every request and its responses to `docs/results/manifests/chaos-<run>.jsonl.gz` (gzipped JSON lines). Manifests are kept on the machine that ran them and are not committed (about 27 MB for these runs); the summary JSON above is what the repo keeps.
