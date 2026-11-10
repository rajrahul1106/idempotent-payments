"""What the consumer accepts as a payment.posted event, before any I/O."""

from __future__ import annotations

import json

import pytest

from app.messaging.consumer import EventError, parse_payment_posted

EVENT_ID = "c803182a-e978-40b7-84db-32f003b7f0c1"
TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


def _payload(**overrides) -> dict:
    payload = {
        "event_id": EVENT_ID,
        "event_type": "payment.posted",
        "schema_version": 1,
        "transaction_id": "pay_7c32ec703f1447b680ae11a8",
        "from_account_id": "acc_alice",
        "to_account_id": "acc_bob",
        "amount_minor": 150000,
        "currency": "INR",
    }
    payload.update(overrides)
    return payload


def _headers(**overrides) -> list[tuple[str, bytes]]:
    headers = {"event_id": EVENT_ID, "schema_version": "1", "traceparent": TRACEPARENT}
    headers.update(overrides)
    return [(k, v.encode()) for k, v in headers.items() if v is not None]


def _parse(payload=None, headers=None, raw: bytes | None = None):
    body = raw if raw is not None else json.dumps(payload or _payload()).encode()
    return parse_payment_posted(headers if headers is not None else _headers(), body)


def test_valid_event_is_parsed_and_traceparent_passes_through_untouched():
    event = _parse()
    assert event.event_id == EVENT_ID
    assert event.amount_minor == 150000
    assert type(event.amount_minor) is int
    assert event.traceparent == TRACEPARENT


def test_header_event_id_must_match_the_payload():
    with pytest.raises(EventError, match="does not match"):
        _parse(_payload(event_id="00000000-0000-4000-8000-000000000000"))


def test_missing_event_id_header_is_an_error():
    with pytest.raises(EventError, match="no event_id header"):
        _parse(headers=_headers(event_id=None))


@pytest.mark.parametrize("version", [2, 0, "1", 1.0, True, None])
def test_unknown_schema_version_is_an_error(version):
    with pytest.raises(EventError, match="schema_version"):
        _parse(_payload(schema_version=version))


def test_header_and_payload_schema_versions_must_agree():
    with pytest.raises(EventError, match="schema_version"):
        _parse(headers=_headers(schema_version="2"))


@pytest.mark.parametrize("amount", [1500.0, 1500.5, "150000", True, None])
def test_amount_must_be_an_integer_and_never_a_float(amount):
    with pytest.raises(EventError, match="amount_minor"):
        _parse(_payload(amount_minor=amount))


def test_float_in_the_raw_json_never_becomes_a_python_float():
    raw = json.dumps(_payload()).replace("150000", "1500.00").encode()
    with pytest.raises(EventError, match="amount_minor"):
        _parse(raw=raw)


@pytest.mark.parametrize("raw", [b"not json", b"[1, 2]", b"\xff\xfe"])
def test_malformed_body_is_an_error(raw):
    with pytest.raises(EventError):
        _parse(raw=raw)


def test_unknown_event_type_is_an_error():
    with pytest.raises(EventError, match="event_type"):
        _parse(_payload(event_type="payment.reversed"))
