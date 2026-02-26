from __future__ import annotations

import time
from collections import defaultdict
from threading import Lock


class ProducerRateLimiter:
    def __init__(self, rpm: int):
        self._rpm = max(0, int(rpm))
        self._window_sec = 60.0
        self._lock = Lock()
        self._buckets: dict[str, list[float]] = defaultdict(list)

    def enabled(self) -> bool:
        return self._rpm > 0

    def check(self, producer: str) -> tuple[bool, int]:
        if not self.enabled():
            return (True, 0)
        now = time.time()
        with self._lock:
            bucket = self._buckets[producer]
            cutoff = now - self._window_sec
            # Keep only points in current window.
            i = 0
            while i < len(bucket) and bucket[i] < cutoff:
                i += 1
            if i:
                del bucket[:i]
            if len(bucket) >= self._rpm:
                retry_after = max(1, int(self._window_sec - (now - bucket[0])))
                return (False, retry_after)
            bucket.append(now)
            return (True, 0)
