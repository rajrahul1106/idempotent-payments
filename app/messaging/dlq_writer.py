"""The DLQ writer: every message on payments.dlq becomes one dlq_messages row.

Per message:

    BEGIN
      INSERT processed_events (dedup key, the writer's own group)  -- conflict: skip
      INSERT dlq_messages (event_id, original_topic, payload, reason,
                           attempts, first_failed_at)
    COMMIT
    commit the Kafka offset

The failure history comes from the headers the main consumer set: attempts
from attempt_count, reason from last_error, plus first_failed_at and
original_topic. A missing header is filled from what the message itself
shows rather than refused, because this is the last stop: a message the
writer refused would be lost.

A database outage holds the message, like the main consumer (decision D5).
Any other error stops the writer with the offset uncommitted. It cannot
send a dead letter anywhere further, so it stops rather than skip one.

Run with: python -m app.messaging.dlq_writer
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import signal
import sys
import threading
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from app.core.config import settings
from app.messaging import topics
from app.messaging.consumer import decode_headers
from app.messaging.consumer_base import ConsumerLoop, ProcessingError
from app.messaging.failures import FailureKind, attempt_count, classify
from app.services.dlq import (
    NIL_EVENT_ID,
    DeadLetter,
    dedup_id_for,
    is_uuid,
    payload_json_for,
    record_dead_letter,
)

logger = logging.getLogger(__name__)


def _parse_time(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def dead_letter_from(
    raw_headers: list[tuple[str, bytes]] | None,
    value: bytes | None,
    *,
    topic: str,
    partition: int,
    offset: int,
    timestamp_ms: int | None,
) -> DeadLetter:
    """Build the row for one DLQ message. Never raises on bad input."""
    headers = decode_headers(raw_headers)
    payload_json = payload_json_for(value)

    event_id = headers.get(topics.HEADER_EVENT_ID)
    if not is_uuid(event_id):
        try:
            candidate = json.loads(payload_json, parse_float=Decimal).get("event_id")
        except AttributeError:
            candidate = None
        event_id = candidate if is_uuid(candidate) else NIL_EVENT_ID
    event_id = str(event_id).lower()

    stamped = (
        dt.datetime.fromtimestamp(timestamp_ms / 1000, dt.timezone.utc)
        if timestamp_ms
        else dt.datetime.now(dt.timezone.utc)
    )
    return DeadLetter(
        dedup_id=dedup_id_for(
            event_id, headers.get(topics.HEADER_REPLAY_OF), f"{topic}|{partition}|{offset}"
        ),
        event_id=event_id,
        original_topic=headers.get(topics.HEADER_ORIGINAL_TOPIC) or "unknown",
        payload_json=payload_json,
        reason=headers.get(topics.HEADER_LAST_ERROR) or "unknown: no last_error header",
        attempts=attempt_count(headers),
        first_failed_at=_parse_time(headers.get(topics.HEADER_FIRST_FAILED_AT)) or stamped,
    )


@dataclass
class WriterStats:
    written: int = 0
    skipped: int = 0
    held_for_database: int = 0


class DlqWriter(ConsumerLoop):
    client_id = "payments-dlq-writer"

    def __init__(
        self,
        *,
        ladder: topics.TopicLadder | None = None,
        group_id: str | None = None,
        bootstrap_servers: str | None = None,
        config_overrides: dict[str, Any] | None = None,
        db_backoff: tuple[float, float] | None = None,
    ) -> None:
        self.ladder = ladder or topics.DEFAULT_LADDER
        super().__init__(
            topic=self.ladder.dlq,
            group_id=group_id or settings.kafka_dlq_writer_group,
            bootstrap_servers=bootstrap_servers,
            config_overrides=config_overrides,
            db_backoff=db_backoff,
        )
        self.stats = WriterStats()

    def _on_close(self) -> None:
        logger.info(
            "DLQ writer stopped: written=%d skipped=%d", self.stats.written, self.stats.skipped
        )

    def _handle(self, message) -> bool:
        _kind, millis = message.timestamp()
        letter = dead_letter_from(
            message.headers(),
            message.value(),
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            timestamp_ms=millis if millis and millis > 0 else None,
        )
        try:
            written = record_dead_letter(letter, self.group_id)
        except Exception as exc:
            if classify(exc) is FailureKind.INFRASTRUCTURE:
                self.stats.held_for_database += 1
                self._hold_for_database(message, exc)
                return False
            logger.error(
                "could not record dead letter %s (event %s): %s. Offset not committed; stopping.",
                self.where(message),
                letter.event_id,
                exc,
            )
            raise ProcessingError(f"could not record {self.where(message)}") from exc

        self._database_ok(message)
        self._commit(message)
        if written:
            self.stats.written += 1
            logger.error(
                "dead letter recorded: event %s from %s after %d attempt(s): %s",
                letter.event_id,
                letter.original_topic,
                letter.attempts,
                letter.reason,
            )
        else:
            self.stats.skipped += 1
            logger.info(
                "dead letter for event %s already recorded; skipped %s",
                letter.event_id,
                self.where(message),
            )
        return True


def main() -> int:
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    stop = threading.Event()

    def _handle_signal(signum, _frame) -> None:
        logger.info("signal %s received, stopping", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    try:
        DlqWriter().run(stop)
    except ProcessingError:
        logger.exception("DLQ writer cannot continue")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
