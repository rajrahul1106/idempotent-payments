"""Drill helper: one dead letter the DLQ writer cannot store. Its
failure_history header holds a \\u0000 escape, which jsonb rejects."""
import json, uuid
from confluent_kafka import Producer
event_id = str(uuid.uuid4())
history = '[{"attempt": 1, "topic": "payments.dlq", "error": "permanent: drill \\u0000 byte", "at": "2026-11-10T23:55:00+00:00"}]'
json.loads(history)  # valid JSON; only jsonb refuses it
headers = {"event_id": event_id, "attempt_count": "1", "original_topic": "payments.main",
           "last_error": "permanent: EventError: drill", "failure_history": history}
producer = Producer({"bootstrap.servers": "localhost:19092", "acks": "all"})
producer.produce("payments.dlq", key=b"acc_drill_dlq", value=json.dumps({"event_id": event_id}).encode(),
                 headers=[(k, v.encode()) for k, v in headers.items()])
assert producer.flush(30) == 0
print("produced dead letter", event_id)
