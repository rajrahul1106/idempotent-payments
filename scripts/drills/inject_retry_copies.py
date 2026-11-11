"""Drill helper: put N copies of already-applied events on a retry tier, as
the main consumer would after a transient failure."""
import datetime as dt, json, subprocess, sys
from confluent_kafka import Producer
topic, count = sys.argv[1], int(sys.argv[2])
rows = subprocess.run(
    ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "payments", "-d", "payments",
     "-At", "-F", "\t", "-c",
     f"SELECT event_id, partition_key, payload::text FROM outbox ORDER BY id DESC LIMIT {count}"],
    capture_output=True, text=True, check=True).stdout.splitlines()
now = dt.datetime.now(dt.timezone.utc).isoformat()
producer = Producer({"bootstrap.servers": "localhost:19092", "acks": "all"})
for line in rows:
    event_id, key, payload = line.split("\t")
    headers = {"event_id": event_id, "event_type": "payment.posted", "schema_version": "1",
               "attempt_count": "1", "first_failed_at": now, "original_topic": "payments.main",
               "last_error": "transient: OperationalError: canceling statement due to lock timeout (drill)",
               "failure_history": json.dumps([{"attempt": 1, "topic": topic, "error": "transient: drill", "at": now}])}
    producer.produce(topic, key=key.encode(), value=payload.encode(),
                     headers=[(k, v.encode()) for k, v in headers.items()])
    producer.poll(0)
assert producer.flush(30) == 0
print(f"produced {len(rows)} messages to {topic}")
