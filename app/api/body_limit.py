"""Request body cap: 64 KB (docs/ARCHITECTURE.md §7 and §9).

The largest legitimate body is a payment with 2 KB of metadata. Anything
over the cap is refused with 413 before a route, a validator or the
database sees it. A declared Content-Length over the cap is refused without
reading the body; a chunked body is counted as it arrives and refused as
soon as it passes the cap, so no more than the cap is ever held in memory.
"""

from __future__ import annotations

import json
import uuid

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.core.telemetry import current_trace_id

MAX_BODY_BYTES = 64 * 1024


class BodySizeLimit:
    def __init__(self, app: ASGIApp, max_bytes: int = MAX_BODY_BYTES) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = dict(scope["headers"])
        declared = headers.get(b"content-length")
        if declared is not None:
            try:
                too_large = int(declared) > self.max_bytes
            except ValueError:
                too_large = False  # the server rejects a malformed length itself
            if too_large:
                await self._refuse(headers, send)
                return

        # Read the body here, counting, then hand the app what was read.
        chunks: list[Message] = []
        size = 0
        while True:
            message = await receive()
            if message["type"] != "http.request":
                chunks.append(message)  # http.disconnect
                break
            size += len(message.get("body", b""))
            if size > self.max_bytes:
                await self._refuse(headers, send)
                return
            chunks.append(message)
            if not message.get("more_body", False):
                break

        async def replay() -> Message:
            if chunks:
                return chunks.pop(0)
            return await receive()

        await self.app(scope, replay, send)

    async def _refuse(self, headers: dict[bytes, bytes], send: Send) -> None:
        request_id = headers.get(b"x-request-id", b"").decode("latin-1") or (
            f"req_{uuid.uuid4().hex[:16]}"
        )
        body = json.dumps(
            {
                "error": {
                    "code": "payload_too_large",
                    "message": f"Request body exceeds {self.max_bytes} bytes",
                    "request_id": request_id,
                    "trace_id": current_trace_id(),
                }
            }
        ).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 413,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode()),
                    (b"x-request-id", request_id.encode("latin-1")),
                    (b"connection", b"close"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})
