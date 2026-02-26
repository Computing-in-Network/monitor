from __future__ import annotations

from collections import Counter
from threading import Lock


class OutcomeStats:
    def __init__(self) -> None:
        self._lock = Lock()
        self._counter: Counter[str] = Counter()
        self._dup_by_event_type: Counter[str] = Counter()
        self._dup_by_producer: Counter[str] = Counter()

    def record(self, code: str, event_type: str | None = None, producer: str | None = None) -> None:
        with self._lock:
            self._counter[code] += 1
            if code == "DUPLICATE":
                if event_type:
                    self._dup_by_event_type[event_type] += 1
                if producer:
                    self._dup_by_producer[producer] += 1

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            counts = dict(self._counter)
            dup_event = dict(self._dup_by_event_type)
            dup_producer = dict(self._dup_by_producer)

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
            "duplicate_by_event_type": dup_event,
            "duplicate_by_producer": dup_producer,
        }
