# Runbook

Seven failure scenarios (BLUEPRINT.md §11, plus the DLQ writer crash loop),
one general "process down" procedure and one client-bug appendix. Each gives
the symptom, the alert that fires, how to diagnose, how to recover, and
whether each step is safe to repeat.

Every scenario was **executed once on the local stack on 10 to 11 Nov 2026
(UTC)**. The output blocks below are pasted from those runs, cut only where
marked `...`. Anything not executed is marked **UNTESTED**. The ledger
scenario ran against a throwaway copy of the dev database, never the dev
database itself.

Where to look:

| What | Where |
| --- | --- |
| Alerts (no Alertmanager) | http://localhost:9090/alerts |
| Dashboards | http://localhost:3000, folder "Payments": Payments overview, Pipeline health, Correctness |
| Traces | http://localhost:16686 |
| Rules and their unit tests | `monitoring/prometheus/alerts.yml`, `alerts_test.yml` (`make rules`) |

Drill tooling, all local-stack only:

| Tool | Purpose |
| --- | --- |
| `scripts/drill.py accounts N` | create N funded customer accounts (operator API) |
| `scripts/drill.py pay accounts.json --count/--duration` | payment traffic, one minted client token per account |
| `scripts/drill.py wait <Alert>` | block until Prometheus shows the alert firing |
| `scripts/drill.py watch alerts.jsonl` | record every alert state change |
| `scripts/drills/inject_poison.py N` | N malformed events on payments.main |
| `scripts/drills/inject_retry_copies.py <tier> N` | N copies of already-applied events on a retry tier |
| `scripts/drills/inject_unwritable_dead_letter.py` | one dead letter the DLQ writer cannot store |
| `scripts/dlq_record.py <partition> <offset>` | **recovery tool**: record a dead letter by hand (scenario 7) |

The drill helpers ran from a scratch copy whose contents are identical to the
files in `scripts/drills/`.

## Alert index

| Alert | Fires when | Scenario |
| --- | --- | --- |
| DlqMessagesIncreasing | any increase of `dlq_messages_total` in 10 min | [1](#1-dlq-depth-is-growing) |
| ConsumerLagHigh | a group above 10,000 messages behind for 5 min (tiers excluded) | [2](#2-consumer-lag-rising) |
| RetryTierDepthHigh | a retry tier above 1,000 waiting for 1 min | [2](#2-consumer-lag-rising) |
| OutboxNotDraining | `outbox_unpublished_rows` above 100 for 2 min | [3](#3-outbox-not-draining) |
| OutboxPublishDelayHigh | publish delay p95 (from Postgres) above 60 s for 1 min | [3](#3-outbox-not-draining) |
| ReconciliationCheckFailed | any invariant check at 0 (fail), immediately | [4](#4-ledger-does-not-balance) |
| EventsSkipRateHigh | over 20% of consumed events skipped as duplicates, 2 min | [5](#5-duplicate-skip-rate-spike) |
| DbPoolSaturated | a process using over 80% of its DB pool for 1 min | [6](#6-database-connection-exhaustion) |
| PaymentSuccessRateLow | over 1% of payments failing server-side (5 min window), 1 min | [6](#6-database-connection-exhaustion) |
| PaymentLatencyP99High | POST /v1/payments p99 above 500 ms for 2 min | [6](#6-database-connection-exhaustion) |
| DlqWriterStuck | messages on payments.dlq not recorded for 2 min | [7](#7-dlq-writer-crash-looping) |
| ProcessCrashLooping | a process restarted more than 3 times in 10 min | [7](#7-dlq-writer-crash-looping) |
| ProcessDown | a scraped process down for 1 min | [Process down](#process-down) |
| KafkaUnreachable | the reconciliation job has not read Kafka for over 60 s, 1 min | [Process down](#process-down) |
| IdempotencyKeyMismatchRateHigh | over 1% of payments reuse a key with a different body, 2 min | [A](#a-idempotency-key-reuse-by-a-client) |

Outbox depth and age, the publish-delay p95, consumer lag and retry tier
depth are computed by the **reconciliation** job (from Postgres and from
Kafka's committed offsets), so they stay visible while the relay or a
consumer is down. If the reconciliation job itself is down, those alerts go
quiet and `ProcessDown{job="reconciliation"}` fires instead.

---

## Process down

**Symptom.** A target is down at http://localhost:9090/targets; the
"Process up" panel on Pipeline health shows DOWN.

**Alerts.** `ProcessDown` (any scraped process: api, relay, consumer,
retry-5s, retry-1m, retry-10m, dlq-writer, reconciliation). The broker is not
scraped: a dead Redpanda shows as `KafkaUnreachable`.

**Diagnosis.**

```bash
docker compose ps -a                         # state and exit code
docker compose logs --tail 50 <service>      # why it stopped
docker inspect --format '{{.State.ExitCode}} oom={{.State.OOMKilled}}' idempotent-payments-<service>-1
```

**Recovery.**

| Step | Safe to repeat |
| --- | --- |
| 1. Fix the cause from the logs (memory, config, a dependency down) | n/a |
| 2. `docker compose up -d <service>` | Yes. Every process is restart-safe: offsets and the outbox are durable, and the relay re-takes its lock |
| 3. Confirm `up == 1` and the alert resolves | Yes |

**Executed.** `ProcessDown` fired during scenarios 2, 3, 4 and 7 (relay,
consumer, retry-5s, dlq-writer, reconciliation). Recovery is the restart
step of each scenario.

**Executed, unplanned: Redpanda stopped during scenario 7.** The broker
stopped at about 23:58:14 with no log line and no exit record kept by Docker;
its first restart (triggered by `docker compose start dlq-writer`) failed to
bind its RPC port. Root cause not determined; the Docker VM has 4 GB and
Jaeger had been OOM-killed earlier in the session. No alert covered a dead
broker, so `KafkaUnreachable` was added and proven on this outage:

```
[00:02:55Z] $ curl -s localhost:9090/api/v1/query --data-urlencode 'query=time() - pipeline_gauges_last_success_timestamp_seconds' ...
seconds since last success postgres 12
seconds since last success kafka 281

[00:02:55Z] $ docker compose logs --since 3m reconciliation | grep 'consumer lag not refreshed' | tail -1
reconciliation-1  | 2026-11-11 00:02:53,812 WARNING __main__ consumer lag not refreshed: KafkaError{code=_TRANSPORT,val=-195,str="Failed to get metadata: Local: Broker transport failure"}

[00:02:56Z] $ python scripts/drill.py wait KafkaUnreachable --timeout 180
{"alert": "KafkaUnreachable{\"instance\": \"reconciliation:9100\", \"job\": \"reconciliation\", \"source\": \"kafka\"}", "state": "firing", "activeAt": "2026-11-11T00:03:01.751296719Z", "value": "3.465923013687134e+02", "seen_at": "00:04:01Z"}

[00:04:01Z] $ docker compose ps -a redpanda --format '{{.Service}}: {{.Status}}'
redpanda: Exited (1) 4 minutes ago

[00:04:02Z] $ docker compose up -d redpanda 2>&1 | tail -1; sleep 20; docker compose ps redpanda ...
 Container idempotent-payments-redpanda-1  Started
redpanda: Up 20 seconds (healthy)
```

Nothing was lost; the partitions came back with their offsets, and every
outbox event was projected afterwards (24,858 of 24,858).

---

## 1. DLQ depth is growing

**Symptom.** "Dead letters (10 min)" on Pipeline health is above zero;
`GET /v1/dlq?replayed=false` returns rows.

**Alert.** `DlqMessagesIncreasing`.

**Diagnosis.** Group by cause, with event ids masked, to separate one
systemic cause (one reason, many rows) from scattered bad data:

```bash
docker compose exec -T postgres psql -U payments -d payments -c "
  SELECT regexp_replace(reason, '[0-9a-f]{8}-[0-9a-f-]{27}', '<event>', 'g') AS reason, count(*)
  FROM dlq_messages WHERE replayed_at IS NULL GROUP BY 1 ORDER BY 2 DESC"
curl -s 'localhost:8000/v1/dlq?reason=permanent&replayed=false' -H "Authorization: Bearer $OPERATOR_TOKEN"
```

`reason` starts with `permanent:` (never retried: the message itself is
wrong) or `transient:` (every tier tried). Each row carries
`failure_history` (one entry per attempt) and `traceparent` (open it in
Jaeger).

**Recovery.**

| Step | Safe to repeat |
| --- | --- |
| 1. Fix the cause first: the producer or upstream data for `permanent`, the code or dependency for `transient` | n/a |
| 2. Replay: `POST /v1/dlq/{id}/replay` (operator) | Yes. The consumer dedups on `event_id`; a second replay of the same row returns 409 |
| 3. A replay that fails again is a new row with `replay_of` lineage, not a lost message: check the DLQ again | Yes |

**Executed** (23:45 UTC). 25 malformed events injected; the alert fired
15 s later.

```
[23:45:26Z] $ python scripts/drills/inject_poison.py 25
produced 25 poison messages to payments.main

[23:45:26Z] $ python scripts/drill.py wait DlqMessagesIncreasing --timeout 180
{"alert": "DlqMessagesIncreasing", "state": "firing", "activeAt": "2026-11-10T23:45:41.751296719Z", "value": "2.5355463089295853e+01", "seen_at": "23:45:42Z"}

[23:45:42Z] $ curl -s localhost:9090/api/v1/query --data-urlencode 'query=sum by (reason) (increase(dlq_messages_total[10m]))' ...
dlq increase 10m permanent 25
dlq increase 10m transient 0
dlq increase 10m unknown 0

[23:45:56Z] $ docker compose exec -T postgres psql ... "SELECT regexp_replace(reason, ...) AS reason, count(*) FROM dlq_messages WHERE replayed_at IS NULL GROUP BY 1 ORDER BY 2 DESC"
 permanent: EventError: event <event>: body is not JSON: Expecting property name enclosed in double quotes: line 1 column 54 (char 53) |    13
 permanent: EventError: event <event>: unknown schema_version 99                                                                       |    13
```

(13 + 13: the 25 drill messages plus the smoke-test message from Phase 6.)
Replaying a message whose cause was **not** fixed, to show step 1 matters:

```
[23:45:43Z] $ curl -s -X POST localhost:8000/v1/dlq/26/replay -H "Authorization: Bearer $OPERATOR_TOKEN" ...
replayed 26 at 2026-11-10T23:45:43.362339Z
 id | attempts |                            reason                            | replayed
----+----------+--------------------------------------------------------------+----------
 26 |        1 | permanent: EventError: event 967495ad-3d3c-40df-8dfe-389bca9 | t
 27 |        1 | permanent: EventError: event 967495ad-3d3c-40df-8dfe-389bca9 | f
```

Row 26 is marked replayed; the message failed again and came back as row
27. Nothing was applied, nothing was lost. Step 1 (fixing a real producer)
is **UNTESTED**: the drill messages are synthetic and have no producer to
fix.

---

## 2. Consumer lag rising

**Symptom.** "Consumer lag by group" climbing on Pipeline health.

**Alerts.** `ConsumerLagHigh` (main consumer or DLQ writer group);
`RetryTierDepthHigh` (a retry tier's waiting messages); usually
`ProcessDown` for the consumer in question.

**Diagnosis.**

```bash
docker compose ps consumer retry-5s retry-1m retry-10m
docker compose exec -T redpanda rpk group describe payments-ledger      # lag per partition
curl -s localhost:9090/api/v1/query --data-urlencode 'query=sum by (group) (kafka_consumer_lag)'
```

Even lag across partitions: a slow or stopped consumer, or a slow database.
Uneven lag: one hot account (one partition). `rpk group describe` shows no
lag for a group that has never committed; the `kafka_consumer_lag` gauge
counts those from the start of the partition and is the number to trust.

**Recovery.**

| Step | Safe to repeat |
| --- | --- |
| 1. Restart a stopped consumer: `docker compose up -d consumer` | Yes. It resumes from its committed offsets; anything redelivered is skipped by dedup |
| 2. Slow consumer: scale up to the partition count (6); beyond that, instances sit idle | Yes |
| 3. Confirm lag returns to 0 | Yes |

**Executed, main consumer** (23:34 UTC). Consumer stopped, 12,000 payments
taken.

```
[23:34:16Z] $ docker compose stop consumer
[23:34:17Z] $ python scripts/drill.py pay accounts.json --count 12000 --concurrency 32
{"requests": 12000, "statuses": {"201": 11934, "500": 66}, "seconds": 104.5, "per_second": 114.8, "p50_ms": 187, "p99_ms": 1746}

[23:36:23Z] $ docker compose exec -T redpanda rpk group describe payments-ledger
GROUP        payments-ledger
STATE        Empty
MEMBERS      0
TOTAL-LAG    11934
TOPIC          PARTITION  CURRENT-OFFSET  LOG-START-OFFSET  LOG-END-OFFSET  LAG
payments.main  0          417             0                 2803            2386
payments.main  1          225             0                 2017            1792
payments.main  2          151             0                 1344            1193
payments.main  3          375             0                 3363            2988
payments.main  4          300             0                 2683            2383
payments.main  5          155             0                 1347            1192

[23:36:23Z] $ python scripts/drill.py wait ProcessDown --timeout 120
{"alert": "ProcessDown{\"instance\": \"consumer:9100\", \"job\": \"consumer\"}", "state": "firing", "activeAt": "2026-11-10T23:34:24.281000865Z", "value": "0e+00", "seen_at": "23:36:23Z"}

[23:36:23Z] $ python scripts/drill.py wait ConsumerLagHigh --timeout 480
{"alert": "ConsumerLagHigh{\"group\": \"payments-ledger\", \"topic\": \"payments.main\"}", "state": "firing", "activeAt": "2026-11-10T23:36:01.751296719Z", "value": "1.1934e+04", "seen_at": "23:41:06Z"}

[23:41:33Z] $ docker compose start consumer
23:41:43Z payments-ledger lag 11934
23:41:53Z payments-ledger lag 4006
23:42:03Z payments-ledger lag 0

[23:42:34Z] $ docker compose exec -T postgres psql ... "SELECT (SELECT count(*) FROM outbox) AS outbox_events, (SELECT count(*) FROM payment_events) AS projected"
 outbox_events | projected
---------------+-----------
         13557 |     13556
```

The one missing projection was the Phase 5 preflight event `c803182a...`,
published on 10 Nov to a `payments.main` that was then deleted by hand; the
message went with the topic. Recovered by republishing from the outbox
(the relay's own path; dedup makes it safe to repeat):

```
$ docker compose exec -T postgres psql ... -c "UPDATE outbox SET published_at = NULL WHERE event_id = 'c803182a-e978-40b7-84db-32f003b7f0c1'"
UPDATE 1
 outbox_events | projected | recovered_amount
---------------+-----------+------------------
         13557 |     13557 |        1500.0000
```

The 66 HTTP 500s were lock timeouts: every payment in this drill credited
the same account, so all writers queued on one row lock (see Findings).

**Executed, retry tier** (23:43 UTC). `retry-5s` stopped while 1,100
failed messages (copies of already-applied events) arrived for it.

```
[23:43:15Z] $ python scripts/drills/inject_retry_copies.py payments.retry.5s 1100
produced 1100 messages to payments.retry.5s

[23:43:16Z] $ python scripts/drill.py wait RetryTierDepthHigh --timeout 240
{"alert": "RetryTierDepthHigh{\"instance\": \"reconciliation:9100\", \"job\": \"reconciliation\", \"tier\": \"5s\"}", "state": "firing", "activeAt": "2026-11-10T23:43:31.751296719Z", "value": "1.1e+03", "seen_at": "23:44:32Z"}

[23:44:32Z] $ curl -s localhost:9090/api/v1/query --data-urlencode 'query=retry_topic_depth' ...
retry_topic_depth 5s 1100
retry_topic_depth 1m 0
retry_topic_depth 10m 0

[23:44:32Z] $ docker compose start retry-5s
[23:44:34Z] $ sleep 30; ... retry_topic_depth{tier="5s"} ...; docker compose logs --since 40s consumer | grep -c 'skipped already-processed'
retry_topic_depth 5s 0
1100

[23:45:05Z] $ ... outbox_events | projected
         13557 |     13557
```

All 1,100 went back to the main topic and were skipped: no second effect.

---

## 3. Outbox not draining

**Symptom.** "Outbox unpublished rows" and "Oldest unpublished row age"
rising on Pipeline health; downstream sees nothing new.

**Alerts.** `OutboxNotDraining`, `ProcessDown{job="relay"}`, and once the
backlog is published, `OutboxPublishDelayHigh`.

**Diagnosis.**

```bash
docker compose ps relay
docker compose exec -T postgres psql -U payments -d payments -c "
  SELECT count(*) AS unpublished, round(extract(epoch FROM now() - min(created_at))) AS oldest_seconds
  FROM outbox WHERE published_at IS NULL"
docker compose exec -T postgres psql -U payments -d payments -c "
  SELECT pid, application_name, state FROM pg_stat_activity WHERE application_name LIKE 'payments-relay%'"
docker compose logs --tail 20 relay           # lock held? broker reachable?
```

No `payments-relay-lock` session means no relay holds the D1 lock. A
`payments-relay-lock` session with no publishing means the broker is the
problem (`KafkaUnreachable`).

**Recovery.**

| Step | Safe to repeat |
| --- | --- |
| 1. Restore the relay (`docker compose up -d relay`) or the broker | Yes. Exactly one relay publishes (D1); a second waits |
| 2. Do **not** republish rows by hand: committed rows are never lost and the relay finds them on its next poll | n/a |
| 3. Confirm the backlog reaches 0 | Yes |

**Executed** (23:19 UTC, and again at 23:31 for the delay alert).

```
[23:19:51Z] $ docker compose stop relay
[23:19:52Z] $ python scripts/drill.py pay accounts.json --count 600 --concurrency 16
{"requests": 600, "statuses": {"201": 600}, "seconds": 3.2, "per_second": 190.2, "p50_ms": 78, "p99_ms": 160}

[23:19:56Z] $ python scripts/drill.py wait ProcessDown --timeout 180
{"alert": "ProcessDown{\"instance\": \"relay:9100\", \"job\": \"relay\"}", "state": "firing", "activeAt": "2026-11-10T23:20:04.281000865Z", "value": "0e+00", "seen_at": "23:21:06Z"}

[23:21:07Z] $ python scripts/drill.py wait OutboxNotDraining --timeout 300
{"alert": "OutboxNotDraining{\"instance\": \"reconciliation:9100\", \"job\": \"reconciliation\"}", "state": "firing", "activeAt": "2026-11-10T23:20:11.751296719Z", "value": "6e+02", "seen_at": "23:22:12Z"}

[23:22:12Z] $ docker compose exec -T postgres psql ... "SELECT count(*) AS unpublished, ... AS oldest_seconds FROM outbox WHERE published_at IS NULL"
 unpublished | oldest_seconds
-------------+----------------
         600 |            140

[23:22:13Z] $ docker compose exec -T postgres psql ... "SELECT pid, application_name, state FROM pg_stat_activity WHERE application_name LIKE 'payments-relay%'"
 pid | application_name | state
-----+------------------+-------
(0 rows)

[23:22:13Z] $ docker compose logs --tail 3 relay
relay-1  | 2026-11-10 23:19:52,150 INFO __main__ signal 15 received, draining
relay-1  | 2026-11-10 23:19:52,221 INFO __main__ relay lock 506032705 released
relay-1  | 2026-11-10 23:19:52,249 INFO __main__ relay stopped

[23:24:13Z] $ docker compose start relay
[23:24:16Z] $ docker compose logs --since 20s relay | grep -E 'acquired|published' | head -4
relay-1  | 2026-11-10 23:24:17,042 INFO __main__ relay lock 506032705 acquired (backend pid 51363); this relay publishes
relay-1  | 2026-11-10 23:24:17,176 INFO __main__ relay published 100/100 event(s)
relay-1  | 2026-11-10 23:24:17,214 INFO __main__ relay published 100/100 event(s)
relay-1  | 2026-11-10 23:24:17,245 INFO __main__ relay published 100/100 event(s)

[23:24:31Z] $ ... "SELECT count(*) AS unpublished FROM outbox WHERE published_at IS NULL"
 unpublished
-------------
           0
```

The first run did not fire `OutboxPublishDelayHigh`: the restarted relay
drained all 600 rows in 0.2 s, before Prometheus first scraped it, so its
own histogram never showed them. The alert was moved to a p95 computed
from Postgres by the reconciliation job, and the drill repeated:

```
[23:31:07Z] $ docker compose stop relay
[23:31:08Z] $ python scripts/drill.py pay accounts.json --count 300 --concurrency 16
{"requests": 300, "statuses": {"201": 300}, "seconds": 2.0, "per_second": 148.3, "p50_ms": 81, "p99_ms": 384}
[23:31:11Z] $ sleep 90; ... "SELECT count(*) AS unpublished, ... AS oldest_seconds ..."
         300 |             92
[23:32:42Z] $ docker compose start relay
[23:32:44Z] $ python scripts/drill.py wait OutboxPublishDelayHigh --timeout 240
{"alert": "OutboxPublishDelayHigh{\"instance\": \"reconciliation:9100\", \"job\": \"reconciliation\"}", "state": "firing", "activeAt": "2026-11-10T23:33:01.751296719Z", "value": "9.53809321e+01", "seen_at": "23:34:05Z"}
[23:34:05Z] $ ... outbox_publish_delay_recent_p95_seconds
outbox_publish_delay_recent_p95_seconds 95.4
[23:34:05Z] $ ... "SELECT count(*) AS published_last_5m, round(max(...)) AS max_delay_s FROM outbox WHERE published_at > now() - interval '5 minutes'"
               300 |        95.4
```

---

## 4. Ledger does not balance

**Symptom.** "Invariant checks" on Correctness shows `ledger_balances_to_zero`
FAIL. This is a correctness break, not a performance problem.

**Alert.** `ReconciliationCheckFailed{check_name="ledger_balances_to_zero"}`
(also fires for `no_duplicate_event_ids` and `no_negative_balances`).

**Diagnosis.**

```bash
docker compose exec -T postgres psql -U payments -d payments -c "
  SELECT transaction_id, sum(amount) AS net, count(*) AS legs
  FROM postings GROUP BY transaction_id HAVING sum(amount) <> 0"
docker compose logs reconciliation | grep 'reconciliation fail' | tail -1
```

**Recovery.**

| Step | Safe to repeat |
| --- | --- |
| 1. Stop the consumers first (`docker compose stop consumer retry-5s retry-1m retry-10m`), so nothing builds on a broken ledger | Yes |
| 2. Find the offending transaction (query above) and how it got in (`session_replication_role` use, a manual write) | Yes |
| 3. Never edit or delete a posting. Append the correcting leg(s) to the same transaction, booked to a settlement or suspense account, reviewed by two people. The deferred trigger then checks a balanced transaction, so no trigger bypass is needed | **No**: each run appends another leg. Re-run the diagnosis query before repeating |
| 4. Re-run the checks (`GET /v1/admin/reconciliation`) and restart the consumers | Yes |

**Executed** (00:06 UTC) on `payments_drill`, a copy of the dev database.
The real reconciliation service was stopped and an identical one, pointed
at the copy, was run under the same network name so Prometheus scraped it.

```
[00:06:04Z] $ docker compose exec -T postgres sh -c 'createdb -U payments payments_drill && pg_dump -U payments payments | psql -q -U payments -d payments_drill' ...
copy: 49722 postings, sum 0.0000

[00:06:08Z] $ # the violation, on the COPY only
docker compose exec -T postgres psql -U payments -d payments_drill -c "BEGIN" -c "SET LOCAL session_replication_role = replica" -c "INSERT INTO transactions (id, amount, currency) VALUES ('tx_drill_imbalance', 12.3400, 'INR')" -c "INSERT INTO postings (...) VALUES ('tx_drill_imbalance', 'acc_carol', gen_random_uuid(), 12.3400, 'INR')" -c "COMMIT"
BEGIN
SET
INSERT 0 1
INSERT 0 1
COMMIT

[00:06:09Z] $ docker compose stop reconciliation
[00:09:46Z] $ docker run -d --name reconciliation-drill --network idempotent-payments_default --network-alias reconciliation -e DATABASE_URL=postgresql+psycopg://payments:payments@postgres:5432/payments_drill ... idempotent-payments-reconciliation python -m app.services.reconciliation
5400491d2078

[00:09:47Z] $ python scripts/drill.py wait ReconciliationCheckFailed --timeout 180
{"alert": "ReconciliationCheckFailed{\"check_name\": \"ledger_balances_to_zero\", \"instance\": \"reconciliation:9100\", \"job\": \"reconciliation\"}", "state": "firing", "activeAt": "2026-11-11T00:09:59.022626356Z", "value": "0e+00", "seen_at": "00:10:02Z"}

[00:10:02Z] $ ... reconciliation_status
reconciliation_status no_duplicate_event_ids 1
reconciliation_status ledger_balances_to_zero 0
reconciliation_status outbox_fully_published 1
reconciliation_status no_negative_balances 1

[00:09:18Z] $ docker compose exec -T postgres psql -U payments -d payments_drill -c "SELECT transaction_id, sum(amount) AS net, count(*) AS legs FROM postings GROUP BY transaction_id HAVING sum(amount) <> 0"
   transaction_id   |   net   | legs
--------------------+---------+------
 tx_drill_imbalance | 12.3400 |    1

[00:10:03Z] $ # Recovery, on the copy: append the missing leg to the same transaction, booked to the settlement account
docker compose exec -T postgres psql -U payments -d payments_drill -c "INSERT INTO postings (transaction_id, account_id, event_id, amount, currency) SELECT 'tx_drill_imbalance', 'sys_settlement', event_id, -12.3400, 'INR' FROM postings WHERE transaction_id = 'tx_drill_imbalance'"
INSERT 0 1

[00:10:03Z] $ ... "SELECT count(*) AS unbalanced_transactions, (SELECT sum(amount) FROM postings) AS ledger_sum FROM (...) u"
 unbalanced_transactions | ledger_sum
-------------------------+------------
                       0 |     0.0000

[00:10:03Z] $ docker restart reconciliation-drill; sleep 20; ... reconciliation_status
reconciliation_status ledger_balances_to_zero 1

[00:10:24Z] $ docker rm -f reconciliation-drill; docker compose start reconciliation; docker compose exec -T postgres dropdb -U payments payments_drill
dropped payments_drill
[00:10:27Z] $ ... "SELECT 'dev database ledger sum: ' || sum(amount) FROM postings"
dev database ledger sum: 0.0000
```

**UNTESTED:** step 1 (stopping the consumers). The drill broke a copy the
live consumers never touch, so stopping them would not have exercised
anything. A first attempt with `docker compose run` did not fire the alert:
that container is reachable only as `reconciliation-drill`, which Prometheus
does not scrape. Use `docker run --network-alias reconciliation` as above.

---

## 5. Duplicate skip rate spike

**Symptom.** "Duplicate skip rate" on Correctness above 20%.

**Alert.** `EventsSkipRateHigh`.

The dedup is working, so this is not urgent: no event is applied twice. It
means wasted work, usually a relay that republishes (a crash loop, or a
mark-published step that does not commit).

**Diagnosis.**

```bash
docker compose logs --since 10m relay | grep 'relay published'     # publishing without new payments?
docker compose exec -T postgres psql -U payments -d payments -c "
  SELECT count(*) AS republished_last_10m FROM outbox
  WHERE published_at > now() - interval '10 minutes' AND created_at < now() - interval '10 minutes'"
docker compose exec -T postgres psql -U payments -d payments -c "
  SELECT count(*) FROM (SELECT event_id FROM processed_events
  GROUP BY event_id, consumer_group HAVING count(*) > 1) d"          # must be 0
```

**Recovery.**

| Step | Safe to repeat |
| --- | --- |
| 1. Confirm duplicate effects are 0 (last query) | Yes |
| 2. Fix whatever makes the relay republish (relay restarts, a failing mark-published commit) | n/a |
| 3. Nothing to clean up: the alert resolves when the 5-minute window passes | Yes |

**Executed** (23:47 UTC). A relay that "forgot" it published 500 rows,
simulated by clearing `published_at`.

```
[23:47:45Z] $ docker compose exec -T postgres psql ... -c "UPDATE outbox SET published_at = NULL WHERE id IN (SELECT id FROM outbox ORDER BY id DESC LIMIT 500)"
UPDATE 500

[23:47:46Z] $ sleep 5; docker compose logs --since 10s relay | grep 'relay published' | head -6
relay-1  | 2026-11-10 23:47:46,755 INFO __main__ relay published 100/100 event(s)
relay-1  | 2026-11-10 23:47:46,797 INFO __main__ relay published 100/100 event(s)
relay-1  | 2026-11-10 23:47:46,840 INFO __main__ relay published 100/100 event(s)
relay-1  | 2026-11-10 23:47:46,873 INFO __main__ relay published 100/100 event(s)
relay-1  | 2026-11-10 23:47:46,900 INFO __main__ relay published 100/100 event(s)

[23:47:51Z] $ python scripts/drill.py wait EventsSkipRateHigh --timeout 300
{"alert": "EventsSkipRateHigh", "state": "firing", "activeAt": "2026-11-10T23:47:51.751296719Z", "value": "1e+00", "seen_at": "23:49:52Z"}

[23:49:52Z] $ ... sum by (outcome) (increase(events_processed_total{job="consumer"}[5m]))
consumer 5m applied 0
consumer 5m skipped 517
consumer 5m dead_lettered 1

[23:49:53Z] $ ... republished_last_10m
                  501

[23:49:53Z] $ ... outbox_events | projected | duplicate_effects
         13557 |     13557 |                 0
```

---

## 6. Database connection exhaustion

**Symptom.** Payments slow and failing; "Database pool in use" at 100% on
Payments overview.

**Alerts.** `DbPoolSaturated`, then `PaymentSuccessRateLow` and
`PaymentLatencyP99High`.

**Diagnosis.** Usually a lock held longer than expected. Find the blocker:

```bash
docker compose exec -T postgres psql -U payments -d payments -c "
  SELECT application_name, state, wait_event_type, count(*) FROM pg_stat_activity
  WHERE datname = 'payments' GROUP BY 1, 2, 3 ORDER BY 4 DESC"
docker compose exec -T postgres psql -U payments -d payments -c "
  SELECT blocker.pid, blocker.application_name, blocker.state,
         round(extract(epoch FROM now() - blocker.xact_start)) AS xact_seconds, count(*) AS blocked_sessions
  FROM pg_stat_activity blocked
  JOIN LATERAL unnest(pg_blocking_pids(blocked.pid)) AS b(pid) ON true
  JOIN pg_stat_activity blocker ON blocker.pid = b.pid GROUP BY 1, 2, 3, 4"
```

**Recovery.**

| Step | Safe to repeat |
| --- | --- |
| 1. Record the blocker's query before ending it (`SELECT pid, query_start, query FROM pg_stat_activity WHERE pid = ...`) | Yes |
| 2. `SELECT pg_terminate_backend(<pid>)`. Its transaction rolls back; the ledger cannot be left half-written | Yes: a second call on a gone pid returns false |
| 3. Confirm the pool drains and payments succeed | Yes |

**Executed** (23:50 UTC). A forgotten transaction held row locks on the 60
drill accounts while 30 clients kept paying.

```
[23:50:17Z] $ docker compose exec -T postgres psql ... -c "SET application_name = 'drill-blocker'" -c "BEGIN" -c "SELECT count(*) FROM (SELECT id FROM accounts WHERE id IN (...60 drill accounts...) FOR UPDATE) l" -c "SELECT pg_sleep(400)" &
[23:50:19Z] $ python scripts/drill.py pay accounts.json --duration 240 --concurrency 30 &

[23:50:19Z] $ python scripts/drill.py wait DbPoolSaturated --timeout 240
{"alert": "DbPoolSaturated{\"job\": \"api\"}", "state": "firing", "activeAt": "2026-11-10T23:50:22.079660426Z", "value": "1e+00", "seen_at": "23:51:24Z"}

[23:51:24Z] $ python scripts/drill.py wait PaymentSuccessRateLow --timeout 240
{"alert": "PaymentSuccessRateLow", "state": "firing", "activeAt": "2026-11-10T23:50:39.50391435Z", "value": "1e+00", "seen_at": "23:51:40Z"}

[23:51:40Z] $ ... db_pool_connections{job="api"} ...; ... db_pool_capacity{job="api"}
api db_pool_connections in_use 25
api db_pool_connections idle 0
api db_pool_capacity 25

[23:51:40Z] $ ... "SELECT application_name, state, wait_event_type, count(*) FROM pg_stat_activity ..."
   application_name    |             state             | wait_event_type | count
-----------------------+-------------------------------+-----------------+-------
                       | active                        | Lock            |    24
                       | idle                          | Client          |     4
                       | idle in transaction (aborted) | Client          |     1
 drill-blocker         | active                        | Timeout         |     1
 payments-relay-listen | idle                          | Client          |     1
 payments-relay-lock   | idle                          | Client          |     1
 psql                  | active                        |                 |     1

[23:51:42Z] $ ... blocker query ...
  pid  | application_name | state  | xact_seconds | blocked_sessions
-------+------------------+--------+--------------+------------------
 55126 | drill-blocker    | active |           84 |               20
 52345 |                  | active |            0 |                1
 ...

[23:51:42Z] $ python scripts/drill.py wait PaymentLatencyP99High --timeout 240
{"alert": "PaymentLatencyP99High", "state": "firing", "activeAt": "2026-11-10T23:50:39.50391435Z", "value": "9.848961937716261e+00", "seen_at": "23:52:43Z"}

[23:52:43Z] $ docker compose logs --since 60s api | grep -oE 'canceling statement due to lock timeout|QueuePool limit[^,]*' | sort | uniq -c
 558 canceling statement due to lock timeout

[23:52:56Z] $ ... "SELECT pid, query_start, left(query, 60) AS query FROM pg_stat_activity WHERE application_name = 'drill-blocker'"
  pid  |         query_start          |        query
-------+------------------------------+----------------------
 55126 | 2026-11-10 23:50:17.77818+00 | SELECT pg_sleep(400)

[23:52:56Z] $ ... "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'drill-blocker'"
 pg_terminate_backend
----------------------
 t

[23:52:57Z] $ sleep 20; ... db_pool_connections{job="api"} ...; ... increase(payments_total[20s]) ...
api db_pool_connections in_use 2
api db_pool_connections idle 19
payments last 20s created 2234

$ cat drill6_pay.json        # the whole 240 s of traffic
{"requests": 12356, "statuses": {"500": 1055, "201": 11301}, "seconds": 240.6, "per_second": 51.4, "p50_ms": 198, "p99_ms": 5987}
```

The application's own sessions have no `application_name`, so in
`pg_stat_activity` they are the blank rows; only the relay's two
connections are named (see Findings).

---

## 7. DLQ writer crash-looping

By design: the DLQ writer is the last stop, so on a row it cannot write for
any reason other than a database outage it logs at ERROR and exits with the
offset uncommitted rather than skip a dead letter. Compose restarts it and
it fails on the same message again. Before exiting it keeps /metrics up for
12 s, so each restart is scraped.

**Symptom.** "DLQ writer backlog" above 0 and "Restarts (10 min)" rising on
Pipeline health; `dlq_messages` stops growing.

**Alerts.** `DlqWriterStuck` (from committed offsets, so it fires while the
writer is dead) and `ProcessCrashLooping{job="dlq-writer"}`.

**Diagnosis.** The ERROR line names the message:

```bash
docker compose logs --since 5m dlq-writer | grep 'could not record' | tail -1
docker compose exec -T redpanda rpk topic consume payments.dlq -p <partition> -o <offset> -n 1 -f '%p@%o %h{%k=%v; }\n'
docker compose exec -T redpanda rpk group describe payments-dlq-writer
```

**Recovery.** The message must be recorded, not skipped.

| Step | Safe to repeat |
| --- | --- |
| 1. `docker compose stop dlq-writer`, so nothing races the next step | Yes |
| 2. Fix the code if the cause is a bug in the writer. Otherwise record this one message by hand: `python scripts/dlq_record.py <partition> <offset>`. It stores the message with the writer's own dedup key, minus the characters Postgres refuses | Yes: a second run finds it recorded and does nothing |
| 3. `docker compose start dlq-writer`. It reads the message, finds it already recorded, skips it and commits past it. No offset is moved by hand | Yes |
| 4. Confirm lag 0 and the row in `dlq_messages` | Yes |

**Executed** (23:55 UTC). A dead letter whose `failure_history` header holds
a `\u0000` escape: valid JSON, refused by `jsonb`.

```
[23:55:19Z] $ python scripts/drills/inject_unwritable_dead_letter.py
produced dead letter 85cd9cf7-74e6-4b34-9c9e-5948a467460a

[23:55:19Z] $ sleep 20; docker compose logs --since 25s dlq-writer | grep -E 'ERROR|exiting' | head -3
dlq-writer-1  | 2026-11-10 23:55:20,035 ERROR __main__ could not record dead letter payments.dlq[4]@1 (event 85cd9cf7-74e6-4b34-9c9e-5948a467460a): (psycopg.errors.UntranslatableCharacter) unsupported Unicode escape sequence
dlq-writer-1  | 2026-11-10 23:55:20,139 ERROR __main__ DLQ writer cannot continue
dlq-writer-1  | 2026-11-10 23:55:20,144 ERROR app.messaging.consumer_base exiting in 12s (kept up so this failure is scraped)

[23:55:40Z] $ python scripts/drill.py wait DlqWriterStuck --timeout 300
{"alert": "DlqWriterStuck", "state": "firing", "activeAt": "2026-11-10T23:55:41.751296719Z", "value": "1e+00", "seen_at": "23:57:46Z"}

[23:57:46Z] $ python scripts/drill.py wait ProcessCrashLooping --timeout 400
{"alert": "ProcessCrashLooping{\"instance\": \"dlq-writer:9100\", \"job\": \"dlq-writer\"}", "state": "firing", "activeAt": "2026-11-10T23:56:24.281000865Z", "value": "1e+01", "seen_at": "23:57:47Z"}

[23:57:47Z] $ docker inspect --format 'restarts={{.RestartCount}} status={{.State.Status}} started={{.State.StartedAt}}' idempotent-payments-dlq-writer-1
restarts=10 status=running started=2026-11-10T23:57:35.436785971Z

[23:57:47Z] $ ... changes(process_start_time_seconds{job="dlq-writer"}[10m]) ...; ... kafka_consumer_lag{group="payments-dlq-writer"} ...
dlq-writer restarts in 10m: 10
payments-dlq-writer lag: 1
```

Recovery was interrupted by the Redpanda outage (see
[Process down](#process-down)) and completed once the broker was back:

```
[00:04:52Z] $ docker compose stop dlq-writer
 Container idempotent-payments-dlq-writer-1  Stopped

[00:05:44Z] $ docker compose exec -T redpanda rpk topic consume payments.dlq -p 4 -o 1 -n 1 -f '%p@%o %h{%k=%v; }\n'
4@1 event_id=85cd9cf7-74e6-4b34-9c9e-5948a467460a; attempt_count=1; original_topic=payments.main; last_error=permanent: EventError: drill; failure_history=[{"attempt": 1, "topic": "payments.dlq", "error": "permanent: drill \u0000 byte", "at": "2026-11-10T23:55 ...

[00:04:52Z] $ python scripts/dlq_record.py 4 1
event 85cd9cf7-74e6-4b34-9c9e-5948a467460a: recorded

[00:04:53Z] $ python scripts/dlq_record.py 4 1   # safe to repeat
event 85cd9cf7-74e6-4b34-9c9e-5948a467460a: already recorded, nothing to do

[00:04:53Z] $ docker compose start dlq-writer
 Container idempotent-payments-dlq-writer-1  Started

[00:04:56Z] $ sleep 20; docker compose logs --since 20s dlq-writer | grep -E 'already recorded|ERROR'
dlq-writer-1  | 2026-11-11 00:04:57,239 INFO __main__ dead letter for event 85cd9cf7-74e6-4b34-9c9e-5948a467460a already recorded; skipped payments.dlq[4]@1

[00:05:16Z] $ docker compose exec -T redpanda rpk group describe payments-dlq-writer | grep TOTAL-LAG
TOTAL-LAG    0

[00:05:16Z] $ ... "SELECT id, left(reason, 75) AS reason, failure_history->0->>'error' AS history_error FROM dlq_messages WHERE event_id = '85cd9cf7-...'"
 id |                            reason                             |     history_error
----+---------------------------------------------------------------+------------------------
 28 | permanent: EventError: drill [NUL characters removed by hand] | permanent: drill  byte
```

**UNTESTED:** the "fix the code" branch of step 2.

---

## A. Idempotency key reuse by a client

**Symptom.** "Idempotency key reuse per second" shows `mismatch` on
Payments overview; a client sees 422 `idempotency_key_reuse`.

**Alert.** `IdempotencyKeyMismatchRateHigh`. A client is sending the same
`Idempotency-Key` with a different body: a client bug, and money-safe (the
second request is refused, not applied).

**Diagnosis.**

```bash
curl -s localhost:9090/api/v1/query --data-urlencode \
  'query=sum by (outcome) (rate(idempotency_replays_total[5m])) / scalar(sum(rate(payments_total[5m])))'
docker compose logs --since 10m api | grep '" 422'      # client address per request
```

Metrics carry no client identity by design (bounded labels only); the API
access log has the client address.

**Recovery.**

| Step | Safe to repeat |
| --- | --- |
| 1. Identify the client from the access log and have it generate one key per payment intent | n/a |
| 2. Nothing to repair: refused requests left nothing behind | n/a |

**Executed** (00:11 UTC). A client reusing 1 key in 20 with a different
amount.

```
[00:11:10Z] $ python scripts/drill.py pay accounts.json --duration 220 --concurrency 4 --mismatch-every 20 &
buggy client started: 1 in 20 keys reused with a different amount

[00:13:34Z] $ ... share of payments ...
share of payments mismatch 0.0475
share of payments replayed 0.0

[00:13:37Z] $ docker compose logs --since 2m api | grep -c '" 422'
402

[00:14:45Z] $ python scripts/drill.py wait IdempotencyKeyMismatchRateHigh --timeout 300
{"alert": "IdempotencyKeyMismatchRateHigh", "state": "firing", "activeAt": "2026-11-11T00:11:29.50391435Z", "value": "4.7758779916569676e-02", "seen_at": "00:14:47Z"}

$ cat drillA_pay.json
{"requests": 16186, "statuses": {"reuse-422": 812, "201": 16181, "500": 5}, "seconds": 220.0, "per_second": 73.6, "p50_ms": 27, "p99_ms": 313}
```

**UNTESTED:** step 1. There is no real client to contact; stopping the drill
traffic stood in for the fix.

---

## Evidence: every alert seen firing

From the Prometheus API, `ALERTS{alertstate="firing"}` over the whole drill
session, first sample per rule:

```
Prometheus query_range ALERTS{alertstate="firing"} 2026-11-10T23:15:00Z .. 2026-11-11T00:15:15Z, step 10s
alert                            first seen firing (UTC)  labels
PaymentSuccessRateLow            2026-11-10 23:51:40
PaymentLatencyP99High            2026-11-10 23:36:50
IdempotencyKeyMismatchRateHigh   2026-11-11 00:13:30
OutboxNotDraining                2026-11-10 23:22:20      {'job': 'reconciliation'}
OutboxPublishDelayHigh           2026-11-10 23:34:10      {'job': 'reconciliation'}
ConsumerLagHigh                  2026-11-10 23:41:10      {'group': 'payments-ledger'}
RetryTierDepthHigh               2026-11-10 23:44:40      {'job': 'reconciliation', 'tier': '5s'}
EventsSkipRateHigh               2026-11-10 23:50:00
DlqMessagesIncreasing            2026-11-10 23:45:50
DlqWriterStuck                   2026-11-10 23:57:50
KafkaUnreachable                 2026-11-11 00:04:10      {'job': 'reconciliation'}
DbPoolSaturated                  2026-11-10 23:51:30      {'job': 'api'}
ReconciliationCheckFailed        2026-11-11 00:10:00      {'check_name': 'ledger_balances_to_zero', 'job': 'reconciliation'}
ProcessDown                      2026-11-10 23:21:10      {'job': 'relay'}
ProcessCrashLooping              2026-11-10 23:56:30      {'job': 'dlq-writer'}
15 of 15 rules seen firing
ProcessDown job=consumer       firing from 23:35:30 (first run of samples)
ProcessDown job=dlq-writer     firing from 00:00:10 (first run of samples)
ProcessDown job=reconciliation firing from 00:07:30 (first run of samples)
ProcessDown job=relay          firing from 23:21:10 (first run of samples)
ProcessDown job=retry-5s       firing from 23:44:30 (first run of samples)
```

`PaymentLatencyP99High` first fired during scenario 2's 12,000-payment run
(the lock contention on one account), before scenario 6 fired it again on
purpose.

## Findings from the drills

1. **A hot account turned lock contention into HTTP 500.** Every payment in
   the first lag drill credited one account; 128 requests waited past
   `lock_timeout` (3 s) and got 500 `internal_error`. **Fixed after the
   drills (D21):** an account lock timeout now returns 503 `service_busy`
   with `Retry-After: 1`, and a retry with the same key is processed
   normally. `sys_settlement` is such an account for every deposit and
   withdrawal (BLUEPRINT.md §15).
2. **Jaeger was OOM-killed** during the 12,000-payment run (in-memory
   storage, no cap, no restart policy), and still restarted with a
   20,000-trace cap. **Fixed after the drills:** `MEMORY_MAX_TRACES=5000`,
   `GOMEMLIMIT=400MiB`, a 512 MB container limit and
   `restart: unless-stopped`. A rerun of the 12,000-payment load peaked at
   383 MiB with no restart. Payments were never affected: span export is
   off the request path.
3. **Redpanda stopped without a trace** at about 23:58 UTC. Root cause not
   determined. No alert covered it; `KafkaUnreachable` now does.
4. **A restarted process's burst is invisible to `rate()`.** The relay
   drained 600 rows before its first scrape. Alerts that matter after a
   restart are computed from durable state by the reconciliation job.
5. **The application's database sessions are anonymous** in
   `pg_stat_activity`. Setting `application_name` per process would make
   scenario 6's diagnosis one query shorter.
6. **The DLQ writer refuses a `\u0000` escape** in a failure-history header.
   Scenario 7 relies on it; a writer that cleaned it itself (as
   `scripts/dlq_record.py` does) would not crash-loop on it.
