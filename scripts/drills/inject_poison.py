"""Drill helper: N malformed payment events on payments.main (half an unknown
schema_version, half not JSON at all)."""
import json, sys, uuid
from confluent_kafka import Producer
count = int(sys.argv[1])
producer = Producer({"bootstrap.servers": "localhost:19092", "acks": "all"})
for i in range(count):
    event_id = str(uuid.uuid4())
    if i % 2:
        value = json.dumps({"event_id": event_id, "event_type": "payment.posted",
                            "schema_version": 99, "from_account_id": "acc_drill_poison"}).encode()
    else:
        value = b'{"event_id": "' + event_id.encode() + b'", truncated'
    producer.produce("payments.main", key=b"acc_drill_poison", value=value,
                     headers=[("event_id", event_id.encode()), ("schema_version", b"1")])
assert producer.flush(30) == 0
print(f"produced {count} poison messages to payments.main")
