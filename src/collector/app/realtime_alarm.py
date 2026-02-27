from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import Any
from uuid import uuid4


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class AlarmState:
    fingerprint: str
    severity: str


class RealtimeAlarmEngine:
    """Generate alarm upsert/recover events directly from metric ingest."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._active: dict[str, AlarmState] = {}

    def evaluate_metric(self, event_type: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
        if event_type == "node_metric":
            return self._evaluate_node(payload)
        if event_type == "link_metric":
            return self._evaluate_link(payload)
        return []

    def _evaluate_node(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        node_uid = str(payload.get("node_uid") or payload.get("node_id") or "").strip()
        if not node_uid:
            return []

        cpu = _to_float(payload.get("cpu_ratio"))
        mem = _to_float(payload.get("mem_ratio"))
        status = str(payload.get("status") or "UP").strip().upper()

        severity = "info"
        evidence: list[str] = []
        if status in {"DOWN"}:
            severity = "critical"
            evidence.append(f"status={status}")
        elif status in {"DEGRADED"}:
            severity = "warning"
            evidence.append(f"status={status}")

        if cpu >= 0.93:
            severity = "critical"
            evidence.append(f"cpu_ratio={cpu:.3f}>=0.93")
        elif cpu >= 0.85 and severity != "critical":
            severity = "warning"
            evidence.append(f"cpu_ratio={cpu:.3f}>=0.85")

        if mem >= 0.93:
            severity = "critical"
            evidence.append(f"mem_ratio={mem:.3f}>=0.93")
        elif mem >= 0.85 and severity != "critical":
            severity = "warning"
            evidence.append(f"mem_ratio={mem:.3f}>=0.85")

        alarm_id = f"AUTO-RT-NODE-{node_uid}"
        return self._emit(
            should_alarm=bool(evidence),
            alarm_id=alarm_id,
            scope_type="node",
            scope_id=node_uid,
            severity=severity,
            evidence=evidence,
            topology_epoch=str(payload.get("topology_epoch") or ""),
            title=f"节点实时告警 {node_uid}",
        )

    def _evaluate_link(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        link_uid = str(payload.get("link_uid") or payload.get("link_id") or "").strip()
        if not link_uid:
            return []

        state = str(payload.get("state") or "UP").strip().upper()
        loss = _to_float(payload.get("loss_rate"))
        rtt = _to_float(payload.get("rtt_ms"))
        jitter = _to_float(payload.get("jitter_ms"))

        severity = "info"
        evidence: list[str] = []
        if state == "DOWN":
            severity = "critical"
            evidence.append(f"state={state}")
        elif state == "DEGRADED":
            severity = "warning"
            evidence.append(f"state={state}")

        if loss >= 0.06:
            severity = "critical"
            evidence.append(f"loss_rate={loss:.4f}>=0.06")
        elif loss >= 0.03 and severity != "critical":
            severity = "warning"
            evidence.append(f"loss_rate={loss:.4f}>=0.03")

        if rtt >= 280:
            severity = "critical"
            evidence.append(f"rtt_ms={rtt:.2f}>=280")
        elif rtt >= 180 and severity != "critical":
            severity = "warning"
            evidence.append(f"rtt_ms={rtt:.2f}>=180")

        if jitter >= 35 and severity == "info":
            severity = "warning"
            evidence.append(f"jitter_ms={jitter:.2f}>=35")

        alarm_id = f"AUTO-RT-LINK-{link_uid}"
        return self._emit(
            should_alarm=bool(evidence),
            alarm_id=alarm_id,
            scope_type="link",
            scope_id=link_uid,
            severity=severity,
            evidence=evidence,
            topology_epoch=str(payload.get("topology_epoch") or ""),
            title=f"链路实时告警 {link_uid}",
        )

    def _emit(
        self,
        *,
        should_alarm: bool,
        alarm_id: str,
        scope_type: str,
        scope_id: str,
        severity: str,
        evidence: list[str],
        topology_epoch: str,
        title: str,
    ) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        fingerprint = "|".join([severity] + sorted(evidence))
        with self._lock:
            prev = self._active.get(alarm_id)
            if should_alarm:
                if prev is not None and prev.fingerprint == fingerprint and prev.severity == severity:
                    return []
                self._active[alarm_id] = AlarmState(fingerprint=fingerprint, severity=severity)
                events.append(
                    {
                        "schema_version": "monitor.v1",
                        "message_id": f"auto-rt-{uuid4().hex[:16]}",
                        "timestamp": _now_iso(),
                        "topology_epoch": topology_epoch,
                        "alarm_id": alarm_id,
                        "severity": severity,
                        "scope_type": scope_type,
                        "scope_id": scope_id,
                        "scope_uid": scope_id,
                        "title": title,
                        "detail": "; ".join(evidence),
                        "fingerprint": fingerprint,
                        "lifecycle_state": "active",
                        "source": "realtime-threshold",
                        "evidence": evidence,
                    }
                )
                return events

            if prev is None:
                return []
            self._active.pop(alarm_id, None)
            events.append(
                {
                    "schema_version": "monitor.v1",
                    "message_id": f"auto-rt-{uuid4().hex[:16]}",
                    "timestamp": _now_iso(),
                    "topology_epoch": topology_epoch,
                    "alarm_id": alarm_id,
                    "severity": prev.severity,
                    "scope_type": scope_type,
                    "scope_id": scope_id,
                    "scope_uid": scope_id,
                    "title": f"{title}恢复",
                    "detail": "metric recovered below threshold",
                    "fingerprint": prev.fingerprint,
                    "lifecycle_state": "recovered",
                    "source": "realtime-threshold",
                }
            )
            return events
