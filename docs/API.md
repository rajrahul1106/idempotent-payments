# API

Every example below was run against the local stack (`make up`) on 12 Nov 2026 (the demo page endpoints on 14 Nov 2026), and the response under it is the one it returned, unedited except for line breaks in JSON. Only the status line and a few headers are shown (`Content-Type`, `X-Request-Id`, `Idempotency-Replayed`, `Retry-After`). The interactive OpenAPI page is at http://localhost:8000/docs once the stack is up.

## Conventions

- **Base URL:** `http://localhost:8000`. Every business endpoint is under `/v1`.
- **Authentication:** a bearer JWT from `POST /v1/auth/token`, valid 15 minutes. Three roles: `viewer` reads, `client` pays from the one account its token names, `operator` does everything, including debiting system accounts. The seeded users (`scripts/seed.py`) are `operator`, `alice` (a client owning `acc_alice`) and `auditor` (a viewer); their passwords are development-only.
- **Money:** `amount_minor` is an integer number of minor units (paise for INR). A float is refused, never rounded. Balances and postings come back as decimal strings with four places.
- **Idempotency:** `POST /v1/payments` requires an `Idempotency-Key` header holding a UUID. The same key with the same body returns the stored response with `200` and `Idempotency-Replayed: true`; the same key with a different body is `422 idempotency_key_reuse`.
- **Errors:** one shape everywhere: `{"error": {"code", "message", "request_id", "trace_id"}}`. `trace_id` is the request's OpenTelemetry trace, so it can be looked up in Jaeger.
- **Request id:** send `X-Request-Id` to choose one; otherwise the API makes one. It is returned as a header and in any error body.
- **Limits:** request bodies are capped at 64 KB (`413 payload_too_large`); `metadata` at 2 KB; 100 requests a minute per token subject (`429 rate_limited` with `Retry-After`).

## Endpoints

| Method | Path | Who | Purpose |
| --- | --- | --- | --- |
| POST | `/v1/auth/token` | anyone | Exchange a username and password for a token |
| POST | `/v1/accounts` | operator | Create an account |
| GET | `/v1/accounts` | any signed-in role | The accounts the caller may see, with `can_debit` (`limit` 1 to 500, default 100) |
| GET | `/v1/accounts/{account_id}` | viewer, operator; a client for its own account | Account and its derived balance |
| GET | `/v1/accounts/{account_id}/postings` | as above | Posting history, newest first (`limit` 1 to 200, `offset`) |
| POST | `/v1/payments` | client (own source account), operator | Move money; idempotent by key |
| GET | `/v1/payments` | viewer, operator; a client for its own account | Recent payments, newest first (`account_id`, `limit` 1 to 100, default 20) |
| GET | `/v1/payments/{payment_id}` | viewer, operator; a client that pays or receives it | One payment and its postings |
| GET | `/v1/payments/{payment_id}/journey` | as above | Every recorded stage of one payment |
| GET | `/demo` | anyone | The demo page (static HTML; it signs in and calls the endpoints above) |
| GET | `/v1/dlq` | operator | Dead letters, oldest first (`limit`, `after_id`, `reason` prefix, `replayed`) |
| POST | `/v1/dlq/{message_id}/replay` | operator | Republish one dead letter to the main topic |
| GET | `/v1/admin/reconciliation` | operator | Run the four invariant checks now |
| GET | `/health`, `/ready`, `/metrics` | anyone | Liveness, readiness, Prometheus scrape |

## Status codes for `POST /v1/payments`

| Code | Error code | When |
| --- | --- | --- |
| 201 | | Created and posted |
| 200 | | Replay of a completed request (same key, same body) |
| 400 | `invalid_request` | Malformed JSON, a missing or extra field, a float amount, a missing or non-UUID key |
| 401 | `unauthenticated` | No token, or an invalid or expired one |
| 403 | `forbidden` | The role may not do this, or the client does not own the source account |
| 404 | `account_not_found` | Either account does not exist |
| 409 | `request_in_progress` | The same key is still being processed after a 3 s wait; retry with the same key |
| 413 | `payload_too_large` | Body over 64 KB |
| 422 | `idempotency_key_reuse` | Same key, different body |
| 422 | `insufficient_funds`, `currency_mismatch`, `same_account_transfer`, `account_not_active` | The payment is refused |
| 429 | `rate_limited` | Over 100 requests a minute for this token subject |
| 500 | `internal_error` | A bug; logged at ERROR. None occurred across the chaos runs |
| 503 | `service_unavailable` | The database is unreachable; retry with the same key |
| 503 | `service_busy` | Waited over 3 s for an account's row lock; `Retry-After: 1`, retry with the same key |

## Setup for the examples

```bash
API=http://localhost:8000
token() {
  curl -s -X POST $API/v1/auth/token -H "Content-Type: application/json" \
    -d "{\"username\": \"$1\", \"password\": \"$2\"}" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])'
}
OPERATOR=$(token operator operator-dev-password)
ALICE=$(token alice alice-dev-password)
```

`ACCOUNT`, `KEY`, `PAYMENT` and `MESSAGE` below are set from earlier responses (`KEY=$(uuidgen)`).

## Operations

#### GET /health

```bash
curl -si $API/health
```

```http
HTTP/1.1 200 OK
content-type: application/json
x-request-id: req_1fd5ae2df7914f7b

{
  "status": "ok",
  "service": "payments-api",
  "version": "0.1.0"
}
```

#### GET /ready

503 with `"database": false` when Postgres is unreachable.

```bash
curl -si $API/ready
```

```http
HTTP/1.1 200 OK
content-type: application/json
x-request-id: req_76b9fbb0b7a141ce

{
  "status": "ok",
  "checks": {
    "database": true
  }
}
```

#### GET /metrics

Prometheus text format. Each process serves its own; this is the API's payments counter after the examples on this page. Labels are bounded: a status and a currency, never an id.

```bash
curl -s $API/metrics | grep '^payments_total' | sort
```

```
payments_total{currency="INR",status="created"} 4.0
payments_total{currency="INR",status="replayed"} 2.0
payments_total{currency="none",status="forbidden"} 2.0
payments_total{currency="none",status="idempotency_key_reuse"} 2.0
payments_total{currency="none",status="insufficient_funds"} 2.0
payments_total{currency="none",status="internal_error"} 0.0
payments_total{currency="none",status="invalid_request"} 2.0
payments_total{currency="none",status="service_unavailable"} 0.0
```

## Authentication

#### POST /v1/auth/token

```bash
curl -si -X POST $API/v1/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"username": "operator", "password": "operator-dev-password"}'
```

```http
HTTP/1.1 200 OK
content-type: application/json
x-request-id: req_6eebfe089ecd4465

{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJvcGVyYXRvciIsInJvbGUiOiJvcGVyYXRvciIsImlhdCI6MTc5NDUwMzczOSwiZXhwIjoxNzk0NTA0NjM5fQ.2CYc_u1HLT1VgJip9Tm2lqnM9QZCf6bJnIbg9f_mzVg",
  "token_type": "bearer",
  "expires_in": 900,
  "role": "operator"
}
```

#### POST /v1/auth/token, a client

A client's token also carries the `account_id` it owns.

```bash
curl -si -X POST $API/v1/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"username": "alice", "password": "alice-dev-password"}'
```

```http
HTTP/1.1 200 OK
content-type: application/json
x-request-id: req_e14501cd46bd4199

{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGljZSIsInJvbGUiOiJjbGllbnQiLCJpYXQiOjE3OTQ1MDM3MzksImV4cCI6MTc5NDUwNDYzOSwiYWNjb3VudF9pZCI6ImFjY19hbGljZSJ9.KDb6VplF99lR-2M4ONZkBsVU8OZ1NIUNYgbxGgMd69c",
  "token_type": "bearer",
  "expires_in": 900,
  "role": "client"
}
```

#### Wrong password: 401

An unknown username gets the same answer and takes as long: it is checked against a real argon2 hash.

```bash
curl -si -X POST $API/v1/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"username": "alice", "password": "wrong"}'
```

```http
HTTP/1.1 401 Unauthorized
content-type: application/json
x-request-id: req_431677cbb03c4fef

{
  "error": {
    "code": "unauthenticated",
    "message": "Invalid credentials",
    "request_id": "req_431677cbb03c4fef",
    "trace_id": "931ff04b5b829032fc0c5835a92907a8"
  }
}
```

## Accounts

#### POST /v1/accounts

```bash
curl -si -X POST $API/v1/accounts \
  -H "Authorization: Bearer $OPERATOR" \
  -H 'Content-Type: application/json' \
  -d '{"holder_name": "Dev Patel", "currency": "INR"}'
```

```http
HTTP/1.1 201 Created
content-type: application/json
x-request-id: req_7a02567a9f704c2d

{
  "id": "acc_3da18cab015b421b8589",
  "holder_name": "Dev Patel",
  "currency": "INR",
  "account_type": "customer",
  "status": "active",
  "balance": "0.0000",
  "balance_minor": 0,
  "created_at": "2026-11-12T17:15:39.478402Z"
}
```

#### GET /v1/accounts/{account_id}

The balance is `SUM(postings)`, computed on every read, never stored. Here: 500.00 funded plus 2,000.00 paid in by the payment below.

```bash
curl -si $API/v1/accounts/$ACCOUNT -H "Authorization: Bearer $OPERATOR"
```

```http
HTTP/1.1 200 OK
content-type: application/json
x-request-id: req_e08b009293b344d3

{
  "id": "acc_3da18cab015b421b8589",
  "holder_name": "Dev Patel",
  "currency": "INR",
  "account_type": "customer",
  "status": "active",
  "balance": "2500.0000",
  "balance_minor": 250000,
  "created_at": "2026-11-12T17:15:39.478402Z"
}
```

#### GET /v1/accounts/{account_id}/postings

```bash
curl -si "$API/v1/accounts/$ACCOUNT/postings?limit=10" -H "Authorization: Bearer $OPERATOR"
```

```http
HTTP/1.1 200 OK
content-type: application/json
x-request-id: req_ae277827dab644c3

[
  {
    "id": 1390543,
    "transaction_id": "pay_17eb485399be49898e156293",
    "event_id": "159ead48-a854-44b0-8d3c-7a2106f935bb",
    "amount": "2000.0000",
    "currency": "INR",
    "created_at": "2026-11-12T17:15:39.552734Z"
  },
  {
    "id": 1390541,
    "transaction_id": "pay_faa7a3a817174be2b09aad87",
    "event_id": "db82bf4f-e6bb-4547-b4b6-1b172d4c304c",
    "amount": "500.0000",
    "currency": "INR",
    "created_at": "2026-11-12T17:15:39.512589Z"
  }
]
```

#### No token: 401

```bash
curl -si $API/v1/accounts/acc_alice
```

```http
HTTP/1.1 401 Unauthorized
content-type: application/json
x-request-id: req_01d233871ebd451e

{
  "error": {
    "code": "unauthenticated",
    "message": "Missing bearer token",
    "request_id": "req_01d233871ebd451e",
    "trace_id": "7da5a26d605d8e283200ce66e39c2dd1"
  }
}
```

## Payments

#### POST /v1/payments: a deposit

Money entering the ledger comes from the system account `sys_settlement`, so every transaction still sums to zero. Only an operator may debit a system account.

```bash
curl -si -X POST $API/v1/payments \
  -H "Authorization: Bearer $OPERATOR" \
  -H "Idempotency-Key: $(uuidgen)" \
  -H 'Content-Type: application/json' \
  -d "{\"from_account_id\": \"sys_settlement\", \"to_account_id\": \"$ACCOUNT\", \"amount_minor\": 50000, \"currency\": \"INR\", \"reference\": \"opening-deposit\"}"
```

```http
HTTP/1.1 201 Created
content-type: application/json
x-request-id: req_cba9b8ab2d534855

{
  "payment_id": "pay_faa7a3a817174be2b09aad87",
  "status": "posted",
  "from_account_id": "sys_settlement",
  "to_account_id": "acc_3da18cab015b421b8589",
  "amount_minor": 50000,
  "currency": "INR",
  "event_id": "db82bf4f-e6bb-4547-b4b6-1b172d4c304c",
  "postings": [
    {
      "account_id": "sys_settlement",
      "amount": "-500.0000"
    },
    {
      "account_id": "acc_3da18cab015b421b8589",
      "amount": "500.0000"
    }
  ],
  "created_at": "2026-11-12T17:15:39.518062+00:00"
}
```

#### POST /v1/payments

```bash
curl -si -X POST $API/v1/payments \
  -H "Authorization: Bearer $ALICE" \
  -H "Idempotency-Key: $KEY" \
  -H 'Content-Type: application/json' \
  -d "{\"from_account_id\": \"acc_alice\", \"to_account_id\": \"$ACCOUNT\", \"amount_minor\": 200000, \"currency\": \"INR\", \"reference\": \"INV-2026-0412\", \"metadata\": {\"channel\": \"upi\"}}"
```

```http
HTTP/1.1 201 Created
content-type: application/json
x-request-id: req_6424280cff5544a7

{
  "payment_id": "pay_17eb485399be49898e156293",
  "status": "posted",
  "from_account_id": "acc_alice",
  "to_account_id": "acc_3da18cab015b421b8589",
  "amount_minor": 200000,
  "currency": "INR",
  "event_id": "159ead48-a854-44b0-8d3c-7a2106f935bb",
  "postings": [
    {
      "account_id": "acc_alice",
      "amount": "-2000.0000"
    },
    {
      "account_id": "acc_3da18cab015b421b8589",
      "amount": "2000.0000"
    }
  ],
  "created_at": "2026-11-12T17:15:39.555886+00:00"
}
```

#### Retry with the same key and body: 200

The stored response, not a second payment: the same `payment_id` and `event_id`. It is stored as JSONB, so its keys come back in a different order; the values are identical.

```bash
curl -si -X POST $API/v1/payments \
  -H "Authorization: Bearer $ALICE" \
  -H "Idempotency-Key: $KEY" \
  -H 'Content-Type: application/json' \
  -d "{\"from_account_id\": \"acc_alice\", \"to_account_id\": \"$ACCOUNT\", \"amount_minor\": 200000, \"currency\": \"INR\", \"reference\": \"INV-2026-0412\", \"metadata\": {\"channel\": \"upi\"}}"
```

```http
HTTP/1.1 200 OK
idempotency-replayed: true
content-type: application/json
x-request-id: req_a177c345cb874957

{
  "status": "posted",
  "currency": "INR",
  "event_id": "159ead48-a854-44b0-8d3c-7a2106f935bb",
  "postings": [
    {
      "amount": "-2000.0000",
      "account_id": "acc_alice"
    },
    {
      "amount": "2000.0000",
      "account_id": "acc_3da18cab015b421b8589"
    }
  ],
  "created_at": "2026-11-12T17:15:39.555886+00:00",
  "payment_id": "pay_17eb485399be49898e156293",
  "amount_minor": 200000,
  "to_account_id": "acc_3da18cab015b421b8589",
  "from_account_id": "acc_alice"
}
```

#### Same key, different body: 422

```bash
curl -si -X POST $API/v1/payments \
  -H "Authorization: Bearer $ALICE" \
  -H "Idempotency-Key: $KEY" \
  -H 'Content-Type: application/json' \
  -d "{\"from_account_id\": \"acc_alice\", \"to_account_id\": \"$ACCOUNT\", \"amount_minor\": 200001, \"currency\": \"INR\", \"reference\": \"INV-2026-0412\", \"metadata\": {\"channel\": \"upi\"}}"
```

```http
HTTP/1.1 422 Unprocessable Content
content-type: application/json
x-request-id: req_6acc939b659348b8

{
  "error": {
    "code": "idempotency_key_reuse",
    "message": "This idempotency key was already used with a different request body",
    "request_id": "req_6acc939b659348b8",
    "trace_id": "433a86d391b1e7fe6e4c0ea871eb965d"
  }
}
```

#### No idempotency key: 400

```bash
curl -si -X POST $API/v1/payments \
  -H "Authorization: Bearer $ALICE" \
  -H 'Content-Type: application/json' \
  -d "{\"from_account_id\": \"acc_alice\", \"to_account_id\": \"$ACCOUNT\", \"amount_minor\": 100, \"currency\": \"INR\"}"
```

```http
HTTP/1.1 400 Bad Request
content-type: application/json
x-request-id: req_836b869922c94aa6

{
  "error": {
    "code": "invalid_request",
    "message": "Idempotency-Key header is required",
    "request_id": "req_836b869922c94aa6",
    "trace_id": "adb94d8eacc716e110eec4846832b5ee"
  }
}
```

#### A float amount is rejected, not rounded: 400

```bash
curl -si -X POST $API/v1/payments \
  -H "Authorization: Bearer $ALICE" \
  -H "Idempotency-Key: $(uuidgen)" \
  -H 'Content-Type: application/json' \
  -d "{\"from_account_id\": \"acc_alice\", \"to_account_id\": \"$ACCOUNT\", \"amount_minor\": 100.5, \"currency\": \"INR\"}"
```

```http
HTTP/1.1 400 Bad Request
content-type: application/json
x-request-id: req_7a9601c87dbc48fa

{
  "error": {
    "code": "invalid_request",
    "message": "amount_minor: Input should be a valid integer",
    "request_id": "req_7a9601c87dbc48fa",
    "trace_id": "45557537e1cebc93ed3683b4f1e61f77"
  }
}
```

#### Insufficient funds: 422

```bash
curl -si -X POST $API/v1/payments \
  -H "Authorization: Bearer $ALICE" \
  -H "Idempotency-Key: $(uuidgen)" \
  -H 'Content-Type: application/json' \
  -d "{\"from_account_id\": \"acc_alice\", \"to_account_id\": \"$ACCOUNT\", \"amount_minor\": 900000000, \"currency\": \"INR\"}"
```

```http
HTTP/1.1 422 Unprocessable Content
content-type: application/json
x-request-id: req_e088f640cda7424d

{
  "error": {
    "code": "insufficient_funds",
    "message": "Account acc_alice has balance 4123.0500, requested 9000000.0000",
    "request_id": "req_e088f640cda7424d",
    "trace_id": "0b28f130b02a05b8c5ea290d0aaa83d9"
  }
}
```

#### A client may not debit someone else's account: 403

```bash
curl -si -X POST $API/v1/payments \
  -H "Authorization: Bearer $ALICE" \
  -H "Idempotency-Key: $(uuidgen)" \
  -H 'Content-Type: application/json' \
  -d "{\"from_account_id\": \"acc_bob\", \"to_account_id\": \"acc_alice\", \"amount_minor\": 100, \"currency\": \"INR\"}"
```

```http
HTTP/1.1 403 Forbidden
content-type: application/json
x-request-id: req_ef142542d1a94f3b

{
  "error": {
    "code": "forbidden",
    "message": "Role 'client' may not debit account acc_bob",
    "request_id": "req_ef142542d1a94f3b",
    "trace_id": "dc6061c75d14e0d22625fbe4f531ec9a"
  }
}
```

#### A body over 64 KB: 413

Refused before authentication or parsing; a chunked body is refused as soon as it passes the cap.

```bash
head -c 70000 /dev/zero | tr '\0' 'x' | curl -si -X POST $API/v1/payments \
  -H 'Content-Type: application/json' --data-binary @-
```

```http
HTTP/1.1 413 Content Too Large
content-type: application/json
x-request-id: req_6e7b18355ea149f5

{
  "error": {
    "code": "payload_too_large",
    "message": "Request body exceeds 65536 bytes",
    "request_id": "req_6e7b18355ea149f5",
    "trace_id": "c99be8e89370871e5fbc39e905252a27"
  }
}
```

#### GET /v1/payments/{payment_id}

```bash
curl -si $API/v1/payments/$PAYMENT -H "Authorization: Bearer $ALICE"
```

```http
HTTP/1.1 200 OK
content-type: application/json
x-request-id: req_4e5451185c3a4a07

{
  "payment_id": "pay_17eb485399be49898e156293",
  "status": "posted",
  "amount": "2000.0000",
  "currency": "INR",
  "reference": "INV-2026-0412",
  "postings": [
    {
      "account_id": "acc_alice",
      "amount": "-2000.0000",
      "event_id": "159ead48-a854-44b0-8d3c-7a2106f935bb"
    },
    {
      "account_id": "acc_3da18cab015b421b8589",
      "amount": "2000.0000",
      "event_id": "159ead48-a854-44b0-8d3c-7a2106f935bb"
    }
  ],
  "created_at": "2026-11-12T17:15:39.552734+00:00"
}
```

## Dead letters

#### GET /v1/dlq

```bash
curl -si "$API/v1/dlq?replayed=false&limit=1" -H "Authorization: Bearer $OPERATOR"
```

```http
HTTP/1.1 200 OK
content-type: application/json
x-request-id: req_98516466905b449f

{
  "items": [
    {
      "id": 2,
      "event_id": "0143a3e3-3106-4ec4-8703-de42987c2e19",
      "original_topic": "payments.main",
      "reason": "permanent: EventError: event 0143a3e3-3106-4ec4-8703-de42987c2e19: body is not JSON: Expecting property name enclosed in double quotes: line 1 column 54 (char 53)",
      "attempts": 1,
      "first_failed_at": "2026-11-10T23:45:26.955149Z",
      "created_at": "2026-11-10T23:45:27.285107Z",
      "replayed_at": null,
      "traceparent": "00-0cb03f908aa592064d24eebf1a48c7a3-fa18016aeb4df677-03",
      "failure_history": [
        {
          "at": "2026-11-10T23:45:26.955149+00:00",
          "error": "permanent: EventError: event 0143a3e3-3106-4ec4-8703-de42987c2e19: body is not JSON: Expecting property name enclosed in double quotes: line 1 column 54 (char 53)",
          "topic": "payments.dlq",
          "attempt": 1
        }
      ],
      "payload": {
        "undecodable_body_base64": "eyJldmVudF9pZCI6ICIwMTQzYTNlMy0zMTA2LTRlYzQtODcwMy1kZTQyOTg3YzJlMTkiLCB0cnVuY2F0ZWQ="
      }
    }
  ],
  "next_after_id": 2
}
```

#### POST /v1/dlq/{message_id}/replay

Republished to `payments.main` with its original `event_id`, so replaying a message whose effect already applied is harmless: the consumer skips it. This one is a malformed test message and will fail again, which records a new dead letter.

```bash
curl -si -X POST $API/v1/dlq/$MESSAGE/replay -H "Authorization: Bearer $OPERATOR"
```

```http
HTTP/1.1 200 OK
content-type: application/json
x-request-id: req_d88abec1ec984551

{
  "id": 2,
  "event_id": "0143a3e3-3106-4ec4-8703-de42987c2e19",
  "original_topic": "payments.main",
  "reason": "permanent: EventError: event 0143a3e3-3106-4ec4-8703-de42987c2e19: body is not JSON: Expecting property name enclosed in double quotes: line 1 column 54 (char 53)",
  "attempts": 1,
  "first_failed_at": "2026-11-10T23:45:26.955149Z",
  "created_at": "2026-11-10T23:45:27.285107Z",
  "replayed_at": "2026-11-12T17:15:39.869668Z",
  "traceparent": "00-0cb03f908aa592064d24eebf1a48c7a3-fa18016aeb4df677-03",
  "failure_history": [
    {
      "at": "2026-11-10T23:45:26.955149+00:00",
      "error": "permanent: EventError: event 0143a3e3-3106-4ec4-8703-de42987c2e19: body is not JSON: Expecting property name enclosed in double quotes: line 1 column 54 (char 53)",
      "topic": "payments.dlq",
      "attempt": 1
    }
  ],
  "payload": {
    "undecodable_body_base64": "eyJldmVudF9pZCI6ICIwMTQzYTNlMy0zMTA2LTRlYzQtODcwMy1kZTQyOTg3YzJlMTkiLCB0cnVuY2F0ZWQ="
  }
}
```

#### Replay it again: 409

```bash
curl -si -X POST $API/v1/dlq/$MESSAGE/replay -H "Authorization: Bearer $OPERATOR"
```

```http
HTTP/1.1 409 Conflict
content-type: application/json
x-request-id: req_48db2b19a2f84034

{
  "error": {
    "code": "dlq_already_replayed",
    "message": "DLQ message 2 was already replayed at 2026-11-12T17:15:39.869668+00:00",
    "request_id": "req_48db2b19a2f84034",
    "trace_id": "df525dfa19a991c21cced83b2ed9d9c6"
  }
}
```

## Reconciliation

#### GET /v1/admin/reconciliation

The four invariants (README, "The four invariants") over the whole database. The reconciliation process also runs them every five minutes and exports the result as `reconciliation_status`.

```bash
curl -si $API/v1/admin/reconciliation -H "Authorization: Bearer $OPERATOR"
```

```http
HTTP/1.1 200 OK
content-type: application/json
x-request-id: req_9853e220604542f3

{
  "checked_at": "2026-11-12T17:15:39.916247+00:00",
  "checks": [
    {
      "name": "no_duplicate_event_ids",
      "status": "pass",
      "duplicates": 0
    },
    {
      "name": "ledger_balances_to_zero",
      "status": "pass",
      "value": "0.0000"
    },
    {
      "name": "outbox_fully_published",
      "status": "pass",
      "unpublished_older_than_60s": 0
    },
    {
      "name": "no_negative_balances",
      "status": "pass",
      "offending_accounts": 0
    }
  ]
}
```

## Demo page endpoints

Read-only endpoints added for the demo page at http://localhost:8000/demo. None of them writes, and none changes who may move money: `POST /v1/payments` still enforces decision D4 itself. `PAYMENT` below is the newest payment from `GET /v1/payments`.

#### GET /v1/accounts, as a client

Who sees what (decisions D4 and D26, docs/ARCHITECTURE.md §16). A client sees its own account in full, plus every customer account by id and holder name only, so it can choose whom to pay. Fields it may not see are `null`, never zero. It sees no system accounts. `can_debit` comes from the same function the payment service enforces. Accounts are listed oldest first (`limit` 1 to 500, default 100), so the seeded accounts come first; a client's own account is always included.

```bash
curl -si "$API/v1/accounts?limit=4" -H "Authorization: Bearer $ALICE"
```

```http
HTTP/1.1 200 OK
content-type: application/json
x-request-id: req_029c022b63fc4ffd

[
  {
    "id": "acc_alice",
    "holder_name": "Alice Sharma",
    "currency": "INR",
    "account_type": "customer",
    "balance": "3423.0500",
    "balance_minor": 342305,
    "can_debit": true
  },
  {
    "id": "acc_bob",
    "holder_name": "Bob Mehta",
    "currency": null,
    "account_type": null,
    "balance": null,
    "balance_minor": null,
    "can_debit": false
  },
  {
    "id": "acc_carol",
    "holder_name": "Carol Nair",
    "currency": null,
    "account_type": null,
    "balance": null,
    "balance_minor": null,
    "can_debit": false
  },
  {
    "id": "acc_d241c71b7d4c43458178",
    "holder_name": "Drill 0",
    "currency": null,
    "account_type": null,
    "balance": null,
    "balance_minor": null,
    "can_debit": false
  }
]
```

#### GET /v1/accounts, as an operator

viewer and operator see every account, system accounts included, with balances. An operator may debit any of them (`can_debit: true`); a viewer none.

```bash
curl -si "$API/v1/accounts?limit=5" -H "Authorization: Bearer $OPERATOR"
```

```http
HTTP/1.1 200 OK
content-type: application/json
x-request-id: req_1a94487c170e4491

[
  {
    "id": "acc_alice",
    "holder_name": "Alice Sharma",
    "currency": "INR",
    "account_type": "customer",
    "balance": "3423.0500",
    "balance_minor": 342305,
    "can_debit": true
  },
  {
    "id": "acc_bob",
    "holder_name": "Bob Mehta",
    "currency": "INR",
    "account_type": "customer",
    "balance": "25760.4500",
    "balance_minor": 2576045,
    "can_debit": true
  },
  {
    "id": "acc_carol",
    "holder_name": "Carol Nair",
    "currency": "INR",
    "account_type": "customer",
    "balance": "10250.5000",
    "balance_minor": 1025050,
    "can_debit": true
  },
  {
    "id": "sys_fees",
    "holder_name": "Fee Income",
    "currency": "INR",
    "account_type": "system",
    "balance": "0.0000",
    "balance_minor": 0,
    "can_debit": true
  },
  {
    "id": "sys_settlement",
    "holder_name": "Bank Settlement",
    "currency": "INR",
    "account_type": "system",
    "balance": "-719731000.0000",
    "balance_minor": -71973100000,
    "can_debit": true
  }
]
```

#### GET /v1/payments

Newest first. A client sees only payments with a posting on its own account; viewer and operator see all, or one account's with `account_id`. `amount` is a decimal string with four places, `amount_minor` an integer.

```bash
curl -si "$API/v1/payments?limit=2" -H "Authorization: Bearer $ALICE"
```

```http
HTTP/1.1 200 OK
content-type: application/json
x-request-id: req_130ea7fe9bb6406a

[
  {
    "payment_id": "pay_df0865b38a094dbeac3ab6b4",
    "status": "posted",
    "amount": "100.0000",
    "amount_minor": 10000,
    "currency": "INR",
    "from_account_id": "acc_alice",
    "to_account_id": "acc_bob",
    "created_at": "2026-11-13T09:15:42.658300+00:00"
  },
  {
    "payment_id": "pay_30525a6d2e314d929ee53f91",
    "status": "posted",
    "amount": "100.0000",
    "amount_minor": 10000,
    "currency": "INR",
    "from_account_id": "acc_alice",
    "to_account_id": "acc_bob",
    "created_at": "2026-11-13T09:14:24.093630+00:00"
  }
]
```

#### A client asking for another account's payments: 403

```bash
curl -si "$API/v1/payments?account_id=acc_bob" -H "Authorization: Bearer $ALICE"
```

```http
HTTP/1.1 403 Forbidden
content-type: application/json
x-request-id: req_02946823a66d49ef

{
  "error": {
    "code": "forbidden",
    "message": "Clients may list only payments on their own account",
    "request_id": "req_02946823a66d49ef",
    "trace_id": "b4e90352f61715577062ea964b359058"
  }
}
```

#### GET /v1/payments/{payment_id}/journey

Everything recorded about one payment, read in one REPEATABLE READ snapshot. Every field comes from a database row. Same access as `GET /v1/payments/{payment_id}`; an unknown id is `404 payment_not_found`.

| Stage | Done when this row exists |
| --- | --- |
| `received` | the payment's `idempotency_keys` row (`created_at`) |
| `recorded` | the `transactions` row (`created_at`) |
| `queued` | the `outbox` row (`created_at`) |
| `sent` | `outbox.published_at` is set |
| `processed` | a `processed_events` row for the main consumer group |

The first three are written in one database transaction, so they carry the same time. A stage that is not done has `at: null` and a `note` with the time elapsed since the money moved; nothing is said about why. `overall` is `failed` when a dead letter for the event has not been replayed, else `completed` once processed, else `in_progress`. `ledger_sum` is the sum of the postings and is always `0.0000`. A payment made outside the API (a seed or test fixture) has no `received` stage.

```bash
curl -si "$API/v1/payments/$PAYMENT/journey" -H "Authorization: Bearer $ALICE"
```

```http
HTTP/1.1 200 OK
content-type: application/json
x-request-id: req_6af180231648407d

{
  "payment": {
    "id": "pay_df0865b38a094dbeac3ab6b4",
    "status": "posted",
    "amount": "100.0000",
    "amount_minor": 10000,
    "currency": "INR",
    "reference": null,
    "created_at": "2026-11-13T09:15:42.658300+00:00"
  },
  "parties": {
    "sender": {
      "id": "acc_alice",
      "holder_name": "Alice Sharma",
      "account_type": "customer"
    },
    "receiver": {
      "id": "acc_bob",
      "holder_name": "Bob Mehta",
      "account_type": "customer"
    }
  },
  "ledger_entries": [
    {
      "account_id": "acc_alice",
      "holder_name": "Alice Sharma",
      "amount": "-100.0000",
      "direction": "money out"
    },
    {
      "account_id": "acc_bob",
      "holder_name": "Bob Mehta",
      "amount": "100.0000",
      "direction": "money in"
    }
  ],
  "ledger_sum": "0.0000",
  "event_id": "a8902696-4934-44e8-ba0f-69f0907ea20b",
  "stages": [
    {
      "key": "received",
      "label": "Request received",
      "done": true,
      "at": "2026-11-13T09:15:42.658300+00:00",
      "note": null
    },
    {
      "key": "recorded",
      "label": "Money moved in the ledger",
      "done": true,
      "at": "2026-11-13T09:15:42.658300+00:00",
      "note": null
    },
    {
      "key": "queued",
      "label": "Queued for delivery",
      "done": true,
      "at": "2026-11-13T09:15:42.658300+00:00",
      "note": null
    },
    {
      "key": "sent",
      "label": "Delivered to processor",
      "done": true,
      "at": "2026-11-13T09:15:42.684245+00:00",
      "note": null
    },
    {
      "key": "processed",
      "label": "Processed",
      "done": true,
      "at": "2026-11-13T09:15:42.692439+00:00",
      "note": null
    }
  ],
  "dead_letters": [],
  "overall": "completed",
  "summary": "100.00 INR moved and every step is complete.",
  "checked_at": "2026-11-14T17:35:55.993668+00:00"
}
```
