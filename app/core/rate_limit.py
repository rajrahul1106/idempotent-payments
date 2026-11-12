"""In-memory token bucket.

Scope limitation, stated plainly: this is per-process. Two API containers
means two independent buckets. Fixing that needs shared state, which the
design defers rather than pulling Redis in for one feature
(docs/ARCHITECTURE.md §15, limitation 2).
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field

from app.core.config import settings


@dataclass
class _Bucket:
    tokens: float
    last_refill: float


@dataclass
class TokenBucketLimiter:
    capacity: int = field(default_factory=lambda: settings.rate_limit_per_minute)
    refill_per_second: float = field(
        default_factory=lambda: settings.rate_limit_per_minute / 60.0
    )
    _buckets: dict[str, _Bucket] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def allow(self, subject: str) -> bool:
        now = time.monotonic()
        with self._lock:
            bucket = self._buckets.get(subject)
            if bucket is None:
                self._buckets[subject] = _Bucket(
                    tokens=self.capacity - 1, last_refill=now
                )
                return True

            elapsed = now - bucket.last_refill
            bucket.tokens = min(
                self.capacity, bucket.tokens + elapsed * self.refill_per_second
            )
            bucket.last_refill = now

            if bucket.tokens < 1:
                return False
            bucket.tokens -= 1
            return True

    def reset(self) -> None:
        with self._lock:
            self._buckets.clear()


limiter = TokenBucketLimiter()
