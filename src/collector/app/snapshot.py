from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import format_datetime
from threading import Lock
from typing import Any


_SEVERITY_ORDER = {"critical": 0, "warning": 1, "info": 2}


@dataclass
class EpochSnapshot:
    nodes: dict[str, dict[str, Any]] = field(default_factory=dict)
    links: dict[str, dict[str, Any]] = field(default_factory=dict)
    alarms: dict[str, dict[str, Any]] = field(default_factory=dict)
    docker_name_to_uid: dict[str, str] = field(default_factory=dict)
    snapshot_version: int = 0
    updated_at: datetime | None = None


class MonitorSnapshotStore:
    def __init__(self) -> None:
        self._lock = Lock()
        self._epochs: dict[str, EpochSnapshot] = {}
        self._latest_epoch: str | None = None

    def _epoch_key(self, payload: dict[str, Any]) -> str:
        topology_epoch = payload.get("topology_epoch")
        if topology_epoch is None or topology_epoch == "":
            return "default"
        return str(topology_epoch)

    def _get_or_create_epoch(self, epoch: str) -> EpochSnapshot:
        snap = self._epochs.get(epoch)
        if snap is None:
            snap = EpochSnapshot()
            self._epochs[epoch] = snap
        return snap

    def has_epoch(self, topology_epoch: Any) -> bool:
        epoch = "default" if topology_epoch in (None, "") else str(topology_epoch)
        with self._lock:
            return epoch in self._epochs

    def has_node_uid(self, topology_epoch: Any, node_uid: str) -> bool:
        epoch = "default" if topology_epoch in (None, "") else str(topology_epoch)
        with self._lock:
            snap = self._epochs.get(epoch)
            return bool(snap and node_uid in snap.nodes)

    def has_link_uid(self, topology_epoch: Any, link_uid: str) -> bool:
        epoch = "default" if topology_epoch in (None, "") else str(topology_epoch)
        with self._lock:
            snap = self._epochs.get(epoch)
            return bool(snap and link_uid in snap.links)

    @staticmethod
    def _etag(epoch: str, snapshot_version: int) -> str:
        return f'W/"{epoch}-{snapshot_version}"'

    def apply(self, event_type: str, payload: dict[str, Any]) -> None:
        epoch = self._epoch_key(payload)
        with self._lock:
            snap = self._get_or_create_epoch(epoch)
            self._latest_epoch = epoch

            if event_type == "node_metric":
                node_uid = str(payload.get("node_uid") or payload.get("node_id") or "")
                if node_uid:
                    snap.nodes[node_uid] = dict(payload)
                    docker_name = str(payload.get("docker_name") or "")
                    if docker_name:
                        snap.docker_name_to_uid[docker_name] = node_uid
                    snap.snapshot_version += 1
                    snap.updated_at = datetime.now(timezone.utc)
                return

            if event_type == "link_metric":
                link_uid = str(payload.get("link_uid") or payload.get("link_id") or "")
                if link_uid:
                    snap.links[link_uid] = dict(payload)
                    snap.snapshot_version += 1
                    snap.updated_at = datetime.now(timezone.utc)
                return

            if event_type == "alarm":
                alarm_id = str(payload.get("alarm_id") or "")
                if alarm_id:
                    snap.alarms[alarm_id] = dict(payload)
                    snap.snapshot_version += 1
                    snap.updated_at = datetime.now(timezone.utc)

    def snapshot(self, topology_epoch: str | None = None) -> dict[str, Any]:
        with self._lock:
            epoch = str(topology_epoch) if topology_epoch else (self._latest_epoch or "default")
            snap = self._epochs.get(epoch) or EpochSnapshot()
            alarms = list(snap.alarms.values())
            alarms.sort(
                key=lambda item: (
                    _SEVERITY_ORDER.get(str(item.get("severity", "info")).lower(), 9),
                    str(item.get("timestamp", "")),
                )
            )
            alarm_summary = {
                "total": len(alarms),
                "critical": sum(1 for a in alarms if str(a.get("severity", "")).lower() == "critical"),
                "warning": sum(1 for a in alarms if str(a.get("severity", "")).lower() == "warning"),
                "info": sum(1 for a in alarms if str(a.get("severity", "")).lower() == "info"),
            }
            updated_at_iso = snap.updated_at.isoformat() if snap.updated_at else None
            last_modified = format_datetime(snap.updated_at, usegmt=True) if snap.updated_at else None
            return {
                "monitor": {
                    "nodes": dict(snap.nodes),
                    "links": dict(snap.links),
                    "alarms": alarms,
                    "alarm_summary": alarm_summary,
                    "snapshot_version": snap.snapshot_version,
                    "topology_epoch": epoch,
                    "available_epochs": sorted(self._epochs.keys()),
                    "updated_at": updated_at_iso,
                    "etag": self._etag(epoch, snap.snapshot_version),
                    "last_modified": last_modified,
                }
            }
