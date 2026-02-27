from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_link_uid(a: Any, b: Any) -> str:
    x = str(a or "").strip()
    y = str(b or "").strip()
    if not x or not y:
        return ""
    return "<->".join(sorted([x, y]))


@dataclass
class FaultState:
    fault_id: str
    fault_type: str
    target: dict[str, Any]
    created_at: str
    last_seen_at: str = field(default_factory=_now_iso)


class FaultInjectionBridge:
    def __init__(self) -> None:
        self._lock = Lock()
        self._active: dict[str, FaultState] = {}

    def active_faults(self) -> list[dict[str, Any]]:
        with self._lock:
            return [
                {
                    "fault_id": x.fault_id,
                    "fault_type": x.fault_type,
                    "target": dict(x.target),
                    "created_at": x.created_at,
                    "last_seen_at": x.last_seen_at,
                }
                for x in self._active.values()
            ]

    def map_control_ack(self, ack: dict[str, Any], topology_epoch: str | None = None) -> dict[str, Any]:
        action = str(ack.get("action") or "").strip()
        ok = bool(ack.get("ok", False))
        deduplicated = bool(ack.get("deduplicated", False))
        fault = ack.get("fault") if isinstance(ack.get("fault"), dict) else {}
        faults = ack.get("faults") if isinstance(ack.get("faults"), list) else []
        request_id = str(ack.get("request_id") or "")
        epoch = str(topology_epoch) if topology_epoch not in (None, "") else "default"

        if not ok:
            return {
                "status": "ignored",
                "reason": "ack_not_ok",
                "action": action,
                "request_id": request_id,
                "error": str(ack.get("error") or ""),
                "active_faults": self.active_faults(),
            }

        alarms_upsert: list[dict[str, Any]] = []
        alarms_recover: list[dict[str, Any]] = []

        with self._lock:
            if action in {"inject_node_fault", "inject_link_fault"}:
                mapped = self._map_fault_to_alarm(fault=fault, topology_epoch=epoch)
                if mapped is not None:
                    fid = str(fault.get("fault_id") or "")
                    if fid:
                        existing = self._active.get(fid)
                        if existing is None:
                            self._active[fid] = FaultState(
                                fault_id=fid,
                                fault_type=str(fault.get("fault_type") or ""),
                                target=dict(fault.get("target") or {}),
                                created_at=str(fault.get("created_at") or _now_iso()),
                                last_seen_at=_now_iso(),
                            )
                            alarms_upsert.append(mapped)
                        else:
                            existing.last_seen_at = _now_iso()
                            if not deduplicated:
                                alarms_upsert.append(mapped)

            elif action == "clear_fault":
                fault_id = str(ack.get("fault_id") or fault.get("fault_id") or "")
                state = self._active.pop(fault_id, None) if fault_id else None
                if state is not None:
                    alarms_recover.append(self._recover_alarm_event(state=state, topology_epoch=epoch))

            elif action == "clear_all_faults":
                for state in self._active.values():
                    alarms_recover.append(self._recover_alarm_event(state=state, topology_epoch=epoch))
                self._active.clear()

            elif action == "list_faults":
                incoming: dict[str, FaultState] = {}
                for item in faults:
                    if not isinstance(item, dict):
                        continue
                    fid = str(item.get("fault_id") or "")
                    if not fid:
                        continue
                    incoming[fid] = FaultState(
                        fault_id=fid,
                        fault_type=str(item.get("fault_type") or ""),
                        target=dict(item.get("target") or {}),
                        created_at=str(item.get("created_at") or _now_iso()),
                        last_seen_at=_now_iso(),
                    )

                previous_ids = set(self._active.keys())
                incoming_ids = set(incoming.keys())
                recovered_ids = sorted(previous_ids - incoming_ids)
                created_ids = sorted(incoming_ids - previous_ids)

                for rid in recovered_ids:
                    state = self._active.get(rid)
                    if state is not None:
                        alarms_recover.append(self._recover_alarm_event(state=state, topology_epoch=epoch))
                self._active = incoming

                for cid in created_ids:
                    mapped = self._map_fault_to_alarm(fault={
                        "fault_id": incoming[cid].fault_id,
                        "fault_type": incoming[cid].fault_type,
                        "target": incoming[cid].target,
                        "created_at": incoming[cid].created_at,
                    }, topology_epoch=epoch)
                    if mapped is not None:
                        alarms_upsert.append(mapped)

        return {
            "status": "ok",
            "action": action,
            "request_id": request_id,
            "deduplicated": deduplicated,
            "alarms_upsert": alarms_upsert,
            "alarms_recover": alarms_recover,
            "active_faults": self.active_faults(),
        }

    def _map_fault_to_alarm(self, fault: dict[str, Any], topology_epoch: str) -> dict[str, Any] | None:
        fault_id = str(fault.get("fault_id") or "").strip()
        fault_type = str(fault.get("fault_type") or "").strip().upper()
        target = fault.get("target") if isinstance(fault.get("target"), dict) else {}
        created_at = str(fault.get("created_at") or _now_iso())
        if not fault_id or not fault_type:
            return None

        if fault_type == "DAMAGED":
            node_id = str(target.get("node_id") or "").strip()
            if not node_id:
                return None
            return {
                "schema_version": "monitor.v1",
                "message_id": f"fi-{fault_id}-open",
                "timestamp": _now_iso(),
                "topology_epoch": topology_epoch,
                "alarm_id": f"FI-{fault_id}",
                "severity": "critical",
                "scope_type": "node",
                "scope_id": node_id,
                "scope_uid": node_id,
                "source": "dynamic-topo",
                "cause": fault_type,
                "lifecycle_state": "open",
                "created_at": created_at,
            }

        if fault_type == "INTERRUPTED":
            a = str(target.get("a") or "").strip()
            b = str(target.get("b") or "").strip()
            uid = normalize_link_uid(a, b)
            if not uid:
                return None
            return {
                "schema_version": "monitor.v1",
                "message_id": f"fi-{fault_id}-open",
                "timestamp": _now_iso(),
                "topology_epoch": topology_epoch,
                "alarm_id": f"FI-{fault_id}",
                "severity": "critical",
                "scope_type": "link",
                "scope_id": uid,
                "scope_uid": uid,
                "source": "dynamic-topo",
                "cause": fault_type,
                "lifecycle_state": "open",
                "created_at": created_at,
            }
        return None

    def _recover_alarm_event(self, state: FaultState, topology_epoch: str) -> dict[str, Any]:
        scope_type = "node" if state.fault_type.upper() == "DAMAGED" else "link"
        if scope_type == "node":
            scope_uid = str(state.target.get("node_id") or "").strip()
        else:
            scope_uid = normalize_link_uid(state.target.get("a"), state.target.get("b"))
        return {
            "schema_version": "monitor.v1",
            "message_id": f"fi-{state.fault_id}-recover",
            "timestamp": _now_iso(),
            "topology_epoch": topology_epoch,
            "alarm_id": f"FI-{state.fault_id}",
            "severity": "info",
            "scope_type": scope_type,
            "scope_id": scope_uid,
            "scope_uid": scope_uid,
            "source": "dynamic-topo",
            "cause": state.fault_type,
            "lifecycle_state": "recovered",
            "created_at": state.created_at,
            "recovered_at": _now_iso(),
        }
