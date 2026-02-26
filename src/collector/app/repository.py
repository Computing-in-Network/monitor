from __future__ import annotations

import time


class IdempotentStore:
    def __init__(self, ttl_seconds: int):
        self._ttl = ttl_seconds
        self._seen: dict[str, float] = {}

    def is_duplicate(self, key: str) -> bool:
        now = time.time()
        self._cleanup(now)
        if key in self._seen:
            return True
        self._seen[key] = now
        return False

    def _cleanup(self, now: float) -> None:
        expiry = now - self._ttl
        remove = [k for k, t in self._seen.items() if t < expiry]
        for k in remove:
            self._seen.pop(k, None)

