"""Record one dead letter the DLQ writer cannot store (RUNBOOK.md scenario 7).

    python scripts/dlq_record.py <partition> <offset>

Reads that exact message from payments.dlq, removes NUL characters (which
Postgres text and jsonb refuse) from its text fields, and records it with
the DLQ writer's own dedup key. When the writer restarts it finds the
message already recorded, skips it and commits past it. Nothing is lost and
no offset is moved by hand. Safe to repeat: a second run records nothing.

Run it with the DLQ writer stopped, so the two cannot race.
"""

from __future__ import annotations

import dataclasses
import sys

from confluent_kafka import Consumer, TopicPartition

from app.core.config import settings
from app.messaging import topics
from app.messaging.dlq_writer import dead_letter_from
from app.services.dlq import record_dead_letter


def _clean(value):
    if isinstance(value, str):
        return value.replace("\x00", "")
    if isinstance(value, list):
        return [_clean(v) for v in value]
    if isinstance(value, dict):
        return {k: _clean(v) for k, v in value.items()}
    return value


def main() -> int:
    partition, offset = int(sys.argv[1]), int(sys.argv[2])
    consumer = Consumer({"bootstrap.servers": settings.kafka_bootstrap_servers,
                         "group.id": "dlq-record-by-hand", "enable.auto.commit": False})
    consumer.assign([TopicPartition(topics.DLQ_TOPIC, partition, offset)])
    message = None
    for _ in range(30):
        message = consumer.poll(1.0)
        if message is not None and message.error() is None:
            break
    consumer.close()
    if message is None or message.error() is not None or message.offset() != offset:
        print(f"could not read {topics.DLQ_TOPIC}[{partition}]@{offset}", file=sys.stderr)
        return 1
    letter = dead_letter_from(
        message.headers(), message.value(), topic=message.topic(),
        partition=partition, offset=offset, timestamp_ms=message.timestamp()[1],
    )
    cleaned = dataclasses.replace(
        letter,
        reason=_clean(letter.reason) + " [NUL characters removed by hand]",
        traceparent=_clean(letter.traceparent),
        failure_history=_clean(letter.failure_history),
        payload_json=_clean(letter.payload_json),
    )
    written = record_dead_letter(cleaned, settings.kafka_dlq_writer_group)
    print(f"event {letter.event_id}: {'recorded' if written else 'already recorded, nothing to do'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
