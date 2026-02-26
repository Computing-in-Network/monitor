from __future__ import annotations

from collections import Counter
from threading import Lock


class OutcomeStats:
    def __init__(self) -> None:
        self._lock = Lock()
        self._counter: Counter[str] = Counter()

    def record(self, code: str) -> None:
        with self._lock:
            self._counter[code] += 1

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            counts = dict(self._counter)

        total = sum(counts.values())
        ok = counts.get("OK", 0)
        duplicate = counts.get("DUPLICATE", 0)
        success_total = ok + duplicate
        fail_total = total - success_total
        success_rate = round(success_total / total, 4) if total else 0.0
        fail_rate = round(fail_total / total, 4) if total else 0.0

        return {
            "total": total,
            "success_total": success_total,
            "fail_total": fail_total,
            "success_rate": success_rate,
            "fail_rate": fail_rate,
            "by_code": counts,
        }
