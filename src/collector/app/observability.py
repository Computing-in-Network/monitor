from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from statistics import mean
from threading import Lock
from typing import Any


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


@dataclass
class _SLOBucket:
    total: int = 0
    ok: int = 0
    error: int = 0
    latencies_ms: list[float] | None = None

    def __post_init__(self) -> None:
        if self.latencies_ms is None:
            self.latencies_ms = []


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    idx = int(max(0, min(len(xs) - 1, round((len(xs) - 1) * q))))
    return float(xs[idx])


class ApiSLOTracker:
    def __init__(self, max_samples_per_api: int = 2000) -> None:
        self._lock = Lock()
        self._max_samples = max(100, int(max_samples_per_api))
        self._buckets: dict[str, _SLOBucket] = {}

    def record(self, api: str, ok: bool, latency_ms: float) -> None:
        k = str(api or "").strip() or "unknown"
        v = max(0.0, float(latency_ms))
        with self._lock:
            bucket = self._buckets.setdefault(k, _SLOBucket())
            bucket.total += 1
            if ok:
                bucket.ok += 1
            else:
                bucket.error += 1
            bucket.latencies_ms.append(v)
            if len(bucket.latencies_ms) > self._max_samples:
                del bucket.latencies_ms[: len(bucket.latencies_ms) - self._max_samples]

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            data = {
                key: _SLOBucket(
                    total=value.total,
                    ok=value.ok,
                    error=value.error,
                    latencies_ms=list(value.latencies_ms or []),
                )
                for key, value in self._buckets.items()
            }

        by_api: dict[str, dict[str, Any]] = {}
        for key, value in data.items():
            total = value.total
            ok = value.ok
            error = value.error
            latencies = value.latencies_ms or []
            by_api[key] = {
                "total": total,
                "ok": ok,
                "error": error,
                "availability": round(ok / total, 4) if total else 1.0,
                "error_rate": round(error / total, 4) if total else 0.0,
                "latency_ms_avg": round(mean(latencies), 3) if latencies else 0.0,
                "latency_ms_p95": round(_percentile(latencies, 0.95), 3) if latencies else 0.0,
            }

        return {"by_api": by_api}
