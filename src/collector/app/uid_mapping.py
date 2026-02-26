from __future__ import annotations

from dataclasses import dataclass, field
from threading import Lock
from typing import Any


def _epoch_of(value: Any) -> str:
    if value in (None, ""):
        return "default"
    return str(value)


@dataclass
class EpochMap:
    node_uids: set[str] = field(default_factory=set)
    link_uids: set[str] = field(default_factory=set)


class UidMappingService:
    def __init__(self) -> None:
        self._lock = Lock()
        self._epochs: dict[str, EpochMap] = {}
        self._mapping_fail_counts: dict[str, int] = {}
        self._alarm_total = 0
        self._alarm_scope_uid = 0

    def _get_epoch(self, epoch: str) -> EpochMap:
        rec = self._epochs.get(epoch)
        if rec is None:
            rec = EpochMap()
            self._epochs[epoch] = rec
        return rec

    def upsert_node(self, topology_epoch: Any, node_uid: str) -> None:
        uid = str(node_uid or "").strip()
        if not uid:
            return
        epoch = _epoch_of(topology_epoch)
        with self._lock:
            self._get_epoch(epoch).node_uids.add(uid)

    def upsert_link(self, topology_epoch: Any, link_uid: str) -> None:
        uid = str(link_uid or "").strip()
        if not uid:
            return
        epoch = _epoch_of(topology_epoch)
        with self._lock:
            self._get_epoch(epoch).link_uids.add(uid)

    def validate_alarm(self, payload: dict[str, Any]) -> tuple[str, str] | None:
        scope_type = str(payload.get("scope_type") or "").strip().lower()
        scope_uid = str(payload.get("scope_uid") or "").strip()
        epoch = _epoch_of(payload.get("topology_epoch"))
        with self._lock:
            self._alarm_total += 1
            if scope_uid:
                self._alarm_scope_uid += 1
            rec = self._epochs.get(epoch)
            if rec is None:
                self._mapping_fail_counts["EPOCH_MAPPING_NOT_FOUND"] = (
                    self._mapping_fail_counts.get("EPOCH_MAPPING_NOT_FOUND", 0) + 1
                )
                return ("EPOCH_MAPPING_NOT_FOUND", "当前 topology_epoch 无可用映射")
            if scope_type == "node":
                if not scope_uid or scope_uid not in rec.node_uids:
                    self._mapping_fail_counts["UNKNOWN_NODE_UID"] = self._mapping_fail_counts.get("UNKNOWN_NODE_UID", 0) + 1
                    return ("UNKNOWN_NODE_UID", f"未找到 node_uid 映射: {scope_uid or '<empty>'}")
            if scope_type == "link":
                if not scope_uid or scope_uid not in rec.link_uids:
                    self._mapping_fail_counts["UNKNOWN_LINK_UID"] = self._mapping_fail_counts.get("UNKNOWN_LINK_UID", 0) + 1
                    return ("UNKNOWN_LINK_UID", f"未找到 link_uid 映射: {scope_uid or '<empty>'}")
        return None

    def stats(self) -> dict[str, Any]:
        with self._lock:
            total = self._alarm_total
            covered = self._alarm_scope_uid
            coverage = round(covered / total, 4) if total else 1.0
            return {
                "epoch_count": len(self._epochs),
                "mapping_fail_counts": dict(self._mapping_fail_counts),
                "alarm_total": total,
                "alarm_scope_uid_total": covered,
                "alarm_scope_uid_coverage": coverage,
            }
