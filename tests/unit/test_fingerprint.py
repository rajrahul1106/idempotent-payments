"""docs/ARCHITECTURE.md §5.5: the idempotency fingerprint and its canonical JSON.

SHA-256 over the canonical form of the request body: sorted keys, no
whitespace, UTF-8. Each test pins one property of that form, so a change
to it fails here instead of silently turning a client's honest retry into
a 422 idempotency_key_reuse.
"""

from __future__ import annotations

import hashlib
import json

from hypothesis import given, strategies as st

from app.services.idempotency import fingerprint
from app.services.payment import Actor, PaymentCommand


def _sha256(canonical: str) -> str:
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# --- the canonical form -----------------------------------------------------


def test_is_sha256_of_sorted_compact_json():
    body = {"to_account_id": "acc_b", "amount_minor": 200000, "from_account_id": "acc_a"}
    canonical = '{"amount_minor":200000,"from_account_id":"acc_a","to_account_id":"acc_b"}'
    assert fingerprint(body) == _sha256(canonical)


def test_nested_keys_are_sorted_too():
    assert fingerprint({"metadata": {"z": 1, "a": 2}}) == _sha256('{"metadata":{"a":2,"z":1}}')


def test_non_ascii_is_encoded_as_utf8_not_escaped():
    body = {"reference": "₹ 2,000 · café"}
    assert fingerprint(body) == _sha256('{"reference":"₹ 2,000 · café"}')
    # The \u-escaped form is a different byte string and must not match.
    escaped = json.dumps(body, sort_keys=True, separators=(",", ":"))
    assert fingerprint(body) != _sha256(escaped)


def test_key_order_and_whitespace_of_the_raw_request_do_not_matter():
    compact = json.loads('{"amount_minor":100,"currency":"INR"}')
    spaced = json.loads('{\n  "currency" : "INR",\n\t"amount_minor": 100\n}')
    assert fingerprint(compact) == fingerprint(spaced)


def test_any_changed_value_changes_the_fingerprint():
    base = {"amount_minor": 100, "currency": "INR"}
    assert fingerprint({**base, "amount_minor": 101}) != fingerprint(base)
    assert fingerprint({**base, "currency": "USD"}) != fingerprint(base)


def test_output_is_64_lowercase_hex_characters():
    digest = fingerprint({"a": 1})
    assert len(digest) == 64
    assert set(digest) <= set("0123456789abcdef")


@given(
    body=st.dictionaries(
        st.text(min_size=1, max_size=8),
        st.one_of(st.integers(), st.text(max_size=8), st.none(), st.booleans()),
        max_size=8,
    ),
    rnd=st.randoms(),
)
def test_insertion_order_never_matters(body, rnd):
    items = list(body.items())
    rnd.shuffle(items)
    assert fingerprint(dict(items)) == fingerprint(body)


# --- what the payment fingerprint covers -------------------------------------


def _command(**overrides) -> PaymentCommand:
    fields = {
        "idempotency_key": "8f14e45f-ea8a-4c2b-9f3a-1b2c3d4e5f60",
        "from_account_id": "acc_a",
        "to_account_id": "acc_b",
        "amount_minor": 200000,
        "currency": "INR",
        "actor": Actor(role="client", account_id="acc_a"),
    }
    fields.update(overrides)
    return PaymentCommand(**fields)


def test_payment_body_has_one_exact_canonical_form():
    canonical = (
        '{"amount_minor":200000,"currency":"INR","from_account_id":"acc_a",'
        '"metadata":{},"reference":null,"to_account_id":"acc_b"}'
    )
    assert fingerprint(_command().body_for_fingerprint()) == _sha256(canonical)


def test_covers_the_body_but_not_the_key_the_trace_or_the_caller():
    base = fingerprint(_command().body_for_fingerprint())
    others = [
        _command(idempotency_key="00000000-0000-4000-8000-000000000000"),
        _command(traceparent="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        _command(actor=Actor(role="operator")),
    ]
    for command in others:
        assert fingerprint(command.body_for_fingerprint()) == base


def test_omitted_and_empty_optional_fields_fingerprint_the_same():
    """Canonicalisation happens after validation, so a client that omits
    reference and metadata matches one that sends null and {}."""
    omitted = fingerprint(_command().body_for_fingerprint())
    explicit = fingerprint(_command(reference=None, metadata={}).body_for_fingerprint())
    assert omitted == explicit
