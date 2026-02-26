from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .storage import TimescaleWriter


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class DiscoverRequest:
    topology_epoch: str | None
    scope_type: str = "network"  # network | node | link
    scope_id: str = "all"
    window_sec: int = 300
    strategies: list[str] | None = None
    include_evidence_points: bool = False


class AutoAlarmDiscoverer:
    def discover(
        self,
        req: DiscoverRequest,
        snapshot_payload: dict[str, Any],
        ts_writer: TimescaleWriter | None = None,
    ) -> dict[str, Any]:
        strategies = set(req.strategies or ["threshold", "baseline"])
        monitor = snapshot_payload.get("monitor", {}) if isinstance(snapshot_payload, dict) else {}
        nodes = monitor.get("nodes", {}) if isinstance(monitor.get("nodes"), dict) else {}
        links = monitor.get("links", {}) if isinstance(monitor.get("links"), dict) else {}

        scoped_nodes, scoped_links = self._scope(req.scope_type, req.scope_id, nodes, links)
        alarms: list[dict[str, Any]] = []
        alarms.extend(self._discover_node_alarms(scoped_nodes, use_baseline=("baseline" in strategies), ts_writer=ts_writer))
        alarms.extend(self._discover_link_alarms(scoped_links, use_baseline=("baseline" in strategies), ts_writer=ts_writer))
        alarms.sort(key=lambda x: (-float(x.get("confidence", 0.0)), str(x.get("scope_id", ""))))

        node_count = sum(1 for x in alarms if x.get("scope_type") == "node")
        link_count = sum(1 for x in alarms if x.get("scope_type") == "link")
        return {
            "status": "ok",
            "generated_at": _now(),
            "scope_type": req.scope_type,
            "scope_id": req.scope_id,
            "topology_epoch": req.topology_epoch,
            "strategies": sorted(strategies),
            "detected_alarms": alarms,
            "summary": {
                "total": len(alarms),
                "node": node_count,
                "link": link_count,
                "scanned_nodes": len(scoped_nodes),
                "scanned_links": len(scoped_links),
            },
        }

    def _scope(
        self,
        scope_type: str,
        scope_id: str,
        nodes: dict[str, Any],
        links: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        mode = str(scope_type or "network").strip().lower()
        sid = str(scope_id or "all").strip()
        node_values = [x for x in nodes.values() if isinstance(x, dict)]
        link_values = [x for x in links.values() if isinstance(x, dict)]

        if mode == "network":
            return node_values, link_values

        if mode == "node":
            selected_nodes = [x for x in node_values if str(x.get("node_uid") or x.get("node_id") or "") == sid]
            selected_links = []
            for lk in link_values:
                src = str(lk.get("src_node_uid") or lk.get("src_node_id") or "")
                dst = str(lk.get("dst_node_uid") or lk.get("dst_node_id") or "")
                if sid in {src, dst}:
                    selected_links.append(lk)
            return selected_nodes, selected_links

        if mode == "link":
            selected_links = []
            for lk in link_values:
                uid = str(lk.get("link_uid") or "")
                lid = str(lk.get("link_id") or "")
                if sid in {uid, lid}:
                    selected_links.append(lk)
            related_nodes: list[dict[str, Any]] = []
            if selected_links:
                uids: set[str] = set()
                for lk in selected_links:
                    src = str(lk.get("src_node_uid") or lk.get("src_node_id") or "")
                    dst = str(lk.get("dst_node_uid") or lk.get("dst_node_id") or "")
                    if src:
                        uids.add(src)
                    if dst:
                        uids.add(dst)
                for n in node_values:
                    nid = str(n.get("node_uid") or n.get("node_id") or "")
                    if nid in uids:
                        related_nodes.append(n)
            return related_nodes, selected_links

        return node_values, link_values

    def _discover_node_alarms(
        self,
        nodes: list[dict[str, Any]],
        use_baseline: bool,
        ts_writer: TimescaleWriter | None,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for n in nodes:
            nid = str(n.get("node_uid") or n.get("node_id") or "").strip()
            if not nid:
                continue
            evidence: list[str] = []
            severity = "warning"
            confidence = 0.55
            cpu = _to_float(n.get("cpu_ratio"))
            mem = _to_float(n.get("mem_ratio"))
            status = str(n.get("status") or "UP").upper()

            if status not in {"UP", "NORMAL"}:
                evidence.append(f"node_status={status}")
                confidence = max(confidence, 0.75)
            if cpu >= 0.92:
                evidence.append(f"cpu_ratio={cpu:.3f}>=0.92")
                severity = "critical"
                confidence = max(confidence, 0.9)
            elif cpu >= 0.82:
                evidence.append(f"cpu_ratio={cpu:.3f}>=0.82")
                confidence = max(confidence, 0.75)
            if mem >= 0.92:
                evidence.append(f"mem_ratio={mem:.3f}>=0.92")
                severity = "critical"
                confidence = max(confidence, 0.9)
            elif mem >= 0.82:
                evidence.append(f"mem_ratio={mem:.3f}>=0.82")
                confidence = max(confidence, 0.75)

            if use_baseline and ts_writer is not None and ts_writer.is_ready():
                base_cpu = _baseline(ts_writer, "node_metric", "cpu_ratio", nid)
                if base_cpu is not None and cpu > 0 and cpu > base_cpu * 1.8:
                    evidence.append(f"cpu_ratio_spike={cpu:.3f}>{base_cpu:.3f}*1.8")
                    confidence = max(confidence, 0.85)
                base_mem = _baseline(ts_writer, "node_metric", "mem_ratio", nid)
                if base_mem is not None and mem > 0 and mem > base_mem * 1.8:
                    evidence.append(f"mem_ratio_spike={mem:.3f}>{base_mem:.3f}*1.8")
                    confidence = max(confidence, 0.85)

            if evidence:
                out.append(
                    {
                        "alarm_id": f"AUTO-NODE-{nid}",
                        "scope_type": "node",
                        "scope_id": nid,
                        "severity": severity,
                        "confidence": round(min(confidence, 0.99), 3),
                        "evidence": evidence,
                    }
                )
        return out

    def _discover_link_alarms(
        self,
        links: list[dict[str, Any]],
        use_baseline: bool,
        ts_writer: TimescaleWriter | None,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for lk in links:
            uid = str(lk.get("link_uid") or lk.get("link_id") or "").strip()
            if not uid:
                continue
            lid = str(lk.get("link_id") or uid)
            evidence: list[str] = []
            severity = "warning"
            confidence = 0.55
            state = str(lk.get("state") or "UP").upper()
            loss = _to_float(lk.get("loss_rate"))
            rtt = _to_float(lk.get("rtt_ms"))
            jitter = _to_float(lk.get("jitter_ms"))

            if state not in {"UP", "NORMAL"}:
                evidence.append(f"link_state={state}")
                confidence = max(confidence, 0.8)
                if state in {"DOWN", "DISCONNECTED"}:
                    severity = "critical"
                    confidence = max(confidence, 0.95)
            if loss >= 0.06:
                evidence.append(f"loss_rate={loss:.4f}>=0.06")
                severity = "critical"
                confidence = max(confidence, 0.92)
            elif loss >= 0.03:
                evidence.append(f"loss_rate={loss:.4f}>=0.03")
                confidence = max(confidence, 0.78)
            if rtt >= 280:
                evidence.append(f"rtt_ms={rtt:.2f}>=280")
                severity = "critical"
                confidence = max(confidence, 0.88)
            elif rtt >= 180:
                evidence.append(f"rtt_ms={rtt:.2f}>=180")
                confidence = max(confidence, 0.74)
            if jitter >= 35:
                evidence.append(f"jitter_ms={jitter:.2f}>=35")
                confidence = max(confidence, 0.72)

            if use_baseline and ts_writer is not None and ts_writer.is_ready():
                base_rtt = _baseline(ts_writer, "link_metric", "rtt_ms", lid)
                if base_rtt is not None and rtt > 0 and rtt > base_rtt * 1.8:
                    evidence.append(f"rtt_spike={rtt:.2f}>{base_rtt:.2f}*1.8")
                    confidence = max(confidence, 0.84)
                base_loss = _baseline(ts_writer, "link_metric", "loss_rate", lid)
                if base_loss is not None and loss > 0 and loss > max(base_loss * 2.0, 0.02):
                    evidence.append(f"loss_spike={loss:.4f}>{base_loss:.4f}*2.0")
                    confidence = max(confidence, 0.84)

            if evidence:
                out.append(
                    {
                        "alarm_id": f"AUTO-LINK-{uid}",
                        "scope_type": "link",
                        "scope_id": uid,
                        "severity": severity,
                        "confidence": round(min(confidence, 0.99), 3),
                        "evidence": evidence,
                    }
                )
        return out


def _baseline(ts_writer: TimescaleWriter, event_type: str, metric: str, entity_id: str) -> float | None:
    try:
        points = ts_writer.read_metric_series(
            event_type=event_type,
            metric=metric,
            entity_id=entity_id,
            limit=24,
            topology_epoch=None,
        )
    except Exception:  # noqa: BLE001
        return None
    values = [float(x.get("value", 0.0)) for x in points if isinstance(x, dict)]
    if len(values) < 4:
        return None
    return sum(values) / len(values)


def _to_float(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0
