from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any
from uuid import uuid4


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class FailedEvent:
    id: str
    created_at: str
    trace_id: str
    subject: str
    payload: dict[str, Any]
    error_code: str
    error_message: str
    status: str = "pending"
    replay_count: int = 0
    last_replay_at: str | None = None
    last_replay_error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "created_at": self.created_at,
            "trace_id": self.trace_id,
            "subject": self.subject,
            "payload": self.payload,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "status": self.status,
            "replay_count": self.replay_count,
            "last_replay_at": self.last_replay_at,
            "last_replay_error": self.last_replay_error,
        }


class FailedEventStore:
    def __init__(self, max_items: int = 2000, audit_file_path: str = "/tmp/collector_failed_events.jsonl") -> None:
        self._max_items = max(100, max_items)
        self._audit_path = Path(audit_file_path) if audit_file_path else None
        self._lock = Lock()
        self._order: deque[str] = deque()
        self._events: dict[str, FailedEvent] = {}

    def _append_audit(self, record: dict[str, Any]) -> None:
        if not self._audit_path:
            return
        self._audit_path.parent.mkdir(parents=True, exist_ok=True)
        with self._audit_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def _trim_if_needed(self) -> None:
        while len(self._order) > self._max_items:
            old_id = self._order.popleft()
            self._events.pop(old_id, None)

    def record(
        self,
        trace_id: str,
        subject: str,
        payload: dict[str, Any],
        error_code: str,
        error_message: str,
    ) -> dict[str, Any]:
        event = FailedEvent(
            id=uuid4().hex,
            created_at=_now_iso(),
            trace_id=trace_id,
            subject=subject,
            payload=payload,
            error_code=error_code,
            error_message=error_message,
        )
        data = event.as_dict()
        with self._lock:
            self._events[event.id] = event
            self._order.append(event.id)
            self._trim_if_needed()
        self._append_audit({"action": "record", "event": data})
        return data

    def list_events(self, limit: int = 100, status: str | None = None) -> list[dict[str, Any]]:
        n = max(1, min(1000, int(limit)))
        with self._lock:
            ids = list(self._order)
            out: list[dict[str, Any]] = []
            for event_id in reversed(ids):
                event = self._events.get(event_id)
                if not event:
                    continue
                if status and event.status != status:
                    continue
                out.append(event.as_dict())
                if len(out) >= n:
                    break
        return out

    def pending_event_ids(self, limit: int = 50) -> list[str]:
        n = max(1, min(500, int(limit)))
        with self._lock:
            ids = [
                event_id
                for event_id in self._order
                if self._events.get(event_id) and self._events[event_id].status in {"pending", "failed"}
            ]
        return ids[:n]

    def get_event(self, event_id: str) -> dict[str, Any] | None:
        with self._lock:
            event = self._events.get(event_id)
            return event.as_dict() if event else None

    def mark_replay(self, event_id: str, success: bool, replay_error: str | None = None) -> dict[str, Any] | None:
        with self._lock:
            event = self._events.get(event_id)
            if not event:
                return None
            event.replay_count += 1
            event.last_replay_at = _now_iso()
            if success:
                event.status = "replayed"
                event.last_replay_error = None
            else:
                event.status = "failed"
                event.last_replay_error = replay_error or "unknown replay error"
            data = event.as_dict()
        self._append_audit({"action": "replay", "event": data})
        return data

    def summary(self) -> dict[str, Any]:
        with self._lock:
            total = len(self._events)
            by_status = {"pending": 0, "failed": 0, "replayed": 0}
            for event in self._events.values():
                if event.status in by_status:
                    by_status[event.status] += 1
                else:
                    by_status[event.status] = by_status.get(event.status, 0) + 1
        return {"total": total, "by_status": by_status}
