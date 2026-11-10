"""GET /v1/dlq and POST /v1/dlq/{message_id}/replay. Operator only."""

from __future__ import annotations

import json
import threading
from decimal import Decimal

from fastapi import APIRouter, Depends, Query

from app.api.deps import Principal, require_role
from app.db.repositories.dlq import DlqRow
from app.messaging import topics
from app.messaging.producer import KafkaProducer, Producer
from app.schemas.dlq import DlqMessageOut, DlqPage
from app.services.dlq import list_dead_letters, replay_dead_letter

router = APIRouter(tags=["dlq"])

_producer: Producer | None = None
_producer_lock = threading.Lock()


def replay_target() -> tuple[Producer, str]:
    """Where a replay is published. Tests override this dependency."""
    global _producer
    with _producer_lock:
        if _producer is None:
            _producer = KafkaProducer(client_id="payments-api-replay")
    return _producer, topics.MAIN_TOPIC


def close_replay_producer() -> None:
    """Flush and close the replay producer. Called at API shutdown."""
    global _producer
    with _producer_lock:
        producer, _producer = _producer, None
    if producer is not None:
        producer.close()


def _out(row: DlqRow) -> DlqMessageOut:
    return DlqMessageOut(
        id=row.id,
        event_id=row.event_id,
        original_topic=row.original_topic,
        reason=row.reason,
        attempts=row.attempts,
        first_failed_at=row.first_failed_at,
        created_at=row.created_at,
        replayed_at=row.replayed_at,
        # Decimal, never float. Pydantic writes a Decimal as a JSON string.
        payload=json.loads(row.payload_json, parse_float=Decimal),
    )


@router.get("/dlq", response_model=DlqPage)
def list_dlq(
    reason: str | None = Query(
        None, max_length=200, description="Prefix of reason, e.g. 'permanent' or 'transient'"
    ),
    replayed: bool | None = Query(None, description="true: replayed only; false: not replayed"),
    after_id: int | None = Query(None, ge=0),
    limit: int = Query(50, ge=1, le=200),
    _: Principal = Depends(require_role("operator")),
) -> DlqPage:
    rows = list_dead_letters(reason=reason, replayed=replayed, after_id=after_id, limit=limit)
    return DlqPage(
        items=[_out(row) for row in rows],
        next_after_id=rows[-1].id if len(rows) == limit else None,
    )


@router.post("/dlq/{message_id}/replay", response_model=DlqMessageOut)
def replay(
    message_id: int,
    _: Principal = Depends(require_role("operator")),
    target: tuple[Producer, str] = Depends(replay_target),
) -> DlqMessageOut:
    producer, topic = target
    return _out(replay_dead_letter(message_id, producer, topic))
