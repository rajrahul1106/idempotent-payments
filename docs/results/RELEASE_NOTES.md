# v1.0.0

The first release: a payments backend that applies every transfer to a double-entry ledger exactly once under client retries, broker redelivery and process crashes, with the measurements and failure-injection runs that support that claim.

The guarantee is **at-least-once delivery with exactly-once processing**: a message may be delivered more than once, and its effect is applied once.

## What is in it

- **Write path:** `POST /v1/payments` with a mandatory `Idempotency-Key`. One Postgres transaction commits the payment, two postings that must sum to zero (a deferred trigger refuses anything else), the key with its stored response, and an outbox event. The same key with the same body returns the stored response; with a different body, 422.
- **Pipeline:**
  - An outbox relay publishes to Kafka (Redpanda locally), woken by LISTEN/NOTIFY and polling every 200 ms as the guarantee.
  - A consumer applies each event under a `processed_events` record in the same transaction.
  - Retry topics at 5 s, 1 min and 10 min, and a dead-letter queue with operator replay.
  - A database outage holds messages instead of dead-lettering them.
- **Operations:**
  - Prometheus metrics and 15 alert rules, each with a `promtool` unit test.
  - Three Grafana dashboards.
  - OpenTelemetry tracing across every process.
  - A reconciliation process that checks the four invariants every five minutes.
  - A seven-scenario runbook, each procedure executed with its output recorded.
- **Docs:** the [README](https://github.com/rajrahul1106/idempotent-payments/blob/v1.0.0/README.md), [ARCHITECTURE.md](https://github.com/rajrahul1106/idempotent-payments/blob/v1.0.0/docs/ARCHITECTURE.md), and [API.md](https://github.com/rajrahul1106/idempotent-payments/blob/v1.0.0/docs/API.md) with a real request and response for every endpoint.

## Results

Measured on a MacBook Air (Apple M2, 8 GB, fanless), with the whole stack in a Docker VM with 3.83 GB and 8 CPUs. Throughput on this machine varies with its temperature, so it is given as a range.

- **Steady state, no faults:**
  - 205 requests/s (175 payments created/s) at p99 48 ms with zero errors, at 4 clients.
  - 155 requests/s at p99 937 ms at 64 clients.
  - The single API process, at about 1.1 cores, is the limit.
- **Chaos:** 29 runs: 28 PASS, 1 INVALID (resource), 0 FAILED. That is 603,000 operations across the passing runs.
  - Three consecutive clean rounds of all five scenarios (consumer kill, 60 s broker outage, database restart, relay kill, 60 s database outage) at 20,000 operations each.
  - 506,848 payments, each applied exactly once.
  - Zero HTTP 500s in 600,567 attempts.
- **60 s database outage:** every request made while it was down got 503. Nothing reached a retry topic or the DLQ, and the first payment after the restart succeeded within 0.75 s.
- **Hypothesis:** a state machine of 200 examples against the real API, database and broker. Both deliberate bugs in the mutation check were caught: the balance check outside the account lock, and the side effect applied without a new dedup record.

Full tables: [docs/results/README.md](https://github.com/rajrahul1106/idempotent-payments/blob/v1.0.0/docs/results/README.md).

## Changes since Day 5a

- **Polish:**
  - Login for an unknown username now costs the same argon2 work as a wrong password.
  - A router that fails to import stops the API at startup.
  - Request bodies are capped at 64 KB (413 `payload_too_large`).
  - Unused code and settings were removed.
- **Dependencies:** every dependency, transitive ones included, is pinned in `requirements.lock`, and the image and `make setup` install from it. Tests run with zero warnings, and any new warning fails them.
- **Build:** `.dockerignore` cuts the build context from 58 MB to 0.5 MB.
- **Fresh clones:** `docker-compose.yml` no longer fixes the project name, so a second checkout gets its own containers and volumes.
- **CI:** GitHub Actions runs the static checks, the unit tests and the PostgreSQL integration tests. The 26 Kafka tests run locally; the README explains why.

## Attached files

`chaos-<scenario>-<operations>-<timestamp>.jsonl.gz`, one for each of the 29 chaos runs, 55 MB in total. Each is a gzipped JSON-lines manifest of every request the run sent and every response it received, and it is what the run was judged against. The per-run summaries are in the repository under `docs/results/`, but the manifests are not, because of their size.

## Known limitations

- **One API worker.** The rate limiter is in memory, so the API runs as one worker, and that caps throughput.
- **`sys_settlement` is a hot account.** Every deposit and withdrawal serialises on it.
- **One Postgres, no replica.**
- **All numbers come from one laptop.**

The full list is in [ARCHITECTURE.md §15](https://github.com/rajrahul1106/idempotent-payments/blob/v1.0.0/docs/ARCHITECTURE.md#15-known-limitations).
