from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Any
from uuid import uuid4

from .alarm_discovery import AutoAlarmDiscoverer, DiscoverRequest
from .fault_spread import AnalyzeRequest, SpreadAnalyzer
from .fault_task_impact import TaskImpactService, TaskImpactRequest
from .storage import TimescaleWriter


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class SimulationSession:
    simulation_id: str
    scenario_type: str
    topology_epoch: str | None
    params: dict[str, Any]
    steps_total: int
    current_step: int = 0
    status: str = "created"  # created | running | completed
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)
    timeline: list[dict[str, Any]] = field(default_factory=list)


class SimulationManager:
    def __init__(self) -> None:
        self._lock = Lock()
        self._sessions: dict[str, SimulationSession] = {}

    def create(self, scenario_type: str, topology_epoch: str | None, params: dict[str, Any], steps_total: int) -> SimulationSession:
        sid = f"sim-{uuid4().hex[:12]}"
        session = SimulationSession(
            simulation_id=sid,
            scenario_type=scenario_type,
            topology_epoch=topology_epoch,
            params=dict(params),
            steps_total=max(1, int(steps_total)),
        )
        with self._lock:
            self._sessions[sid] = session
        return session

    def get(self, simulation_id: str) -> SimulationSession | None:
        with self._lock:
            return self._sessions.get(simulation_id)

    def step(
        self,
        simulation_id: str,
        *,
        snapshot_payload: dict[str, Any],
        alarm_discoverer: AutoAlarmDiscoverer,
        spread_analyzer: SpreadAnalyzer,
        task_impact: TaskImpactService,
        ts_writer: TimescaleWriter | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            session = self._sessions.get(simulation_id)
            if session is None:
                raise KeyError(simulation_id)
            if session.current_step >= session.steps_total:
                session.status = "completed"
                return self._session_view(session)
            session.current_step += 1
            session.status = "running" if session.current_step < session.steps_total else "completed"
            session.updated_at = _now()
            step_no = session.current_step

        monitor = deepcopy(snapshot_payload.get("monitor", {}))
        nodes = monitor.get("nodes") if isinstance(monitor.get("nodes"), dict) else {}
        links = monitor.get("links") if isinstance(monitor.get("links"), dict) else {}
        self._inject_faults(
            scenario_type=session.scenario_type,
            params=session.params,
            step_no=step_no,
            steps_total=session.steps_total,
            nodes=nodes,
            links=links,
        )

        fake_snapshot = {"monitor": {"nodes": nodes, "links": links, "alarms": [], "topology_epoch": session.topology_epoch}}
        discovered = alarm_discoverer.discover(
            DiscoverRequest(
                topology_epoch=session.topology_epoch,
                scope_type="network",
                scope_id="all",
                strategies=["threshold", "baseline"],
            ),
            snapshot_payload=fake_snapshot,
            ts_writer=ts_writer,
        )
        alarms = [x for x in (discovered.get("detected_alarms") or []) if isinstance(x, dict)]
        seeds_node = [str(x.get("scope_id")) for x in alarms if str(x.get("scope_type")) == "node" and x.get("scope_id")]
        seeds_link = [str(x.get("scope_id")) for x in alarms if str(x.get("scope_type")) == "link" and x.get("scope_id")]
        if not seeds_node and not seeds_link and session.scenario_type == "link_down":
            lid = str(session.params.get("link_id") or "")
            if lid:
                seeds_link.append(lid)

        if not seeds_node and seeds_link:
            for lk in links.values():
                if not isinstance(lk, dict):
                    continue
                uid = str(lk.get("link_uid") or lk.get("link_id") or "")
                if uid not in seeds_link:
                    continue
                src = str(lk.get("src_node_uid") or lk.get("src_node_id") or "")
                dst = str(lk.get("dst_node_uid") or lk.get("dst_node_id") or "")
                if src and src not in seeds_node:
                    seeds_node.append(src)
                if dst and dst not in seeds_node:
                    seeds_node.append(dst)

        spread = spread_analyzer.analyze(
            AnalyzeRequest(
                alarm_nodes=seeds_node,
                links=[x for x in links.values() if isinstance(x, dict)],
                max_depth=4,
                mode="cascade",
                cascade_threshold=0.6,
            )
        )
        link_metrics = {}
        for uid, lk in links.items():
            if not isinstance(lk, dict):
                continue
            lid = str(lk.get("link_uid") or uid)
            link_metrics[lid] = {"state": lk.get("state"), "rtt_ms": lk.get("rtt_ms"), "loss_rate": lk.get("loss_rate")}
        tasks = self._build_tasks(link_metrics, max_tasks=20)
        impact = task_impact.evaluate(
            TaskImpactRequest(
                tasks=tasks,
                link_metrics=link_metrics,
                fault_spread=spread,
                rtt_warn_ms=180.0,
                loss_warn_rate=0.03,
            )
        )

        frame = {
            "step": step_no,
            "status": session.status,
            "detected_alarm_total": len(alarms),
            "impacted_nodes": len(spread.get("impacted_nodes") or []),
            "impacted_links": len(spread.get("impacted_links") or []),
            "disconnected_tasks": sum(1 for x in (impact.get("tasks") or []) if isinstance(x, dict) and x.get("status") == "disconnected"),
            "summary": self._risk_summary(alarms, spread, impact),
        }
        with self._lock:
            session.timeline.append(frame)
            session.updated_at = _now()
            return self._session_view(session)

    def _session_view(self, session: SimulationSession) -> dict[str, Any]:
        return {
            "simulation_id": session.simulation_id,
            "scenario_type": session.scenario_type,
            "topology_epoch": session.topology_epoch,
            "params": session.params,
            "steps_total": session.steps_total,
            "current_step": session.current_step,
            "status": session.status,
            "created_at": session.created_at,
            "updated_at": session.updated_at,
            "timeline": list(session.timeline),
        }

    def _inject_faults(
        self,
        *,
        scenario_type: str,
        params: dict[str, Any],
        step_no: int,
        steps_total: int,
        nodes: dict[str, Any],
        links: dict[str, Any],
    ) -> None:
        severity = max(0.2, min(1.0, step_no / max(1, steps_total)))
        if scenario_type == "link_down":
            target = str(params.get("link_id") or params.get("scope_id") or "").strip()
            for uid, lk in links.items():
                if not isinstance(lk, dict):
                    continue
                lid = str(lk.get("link_uid") or lk.get("link_id") or uid)
                if lid != target:
                    continue
                lk["state"] = "DOWN" if severity > 0.4 else "DEGRADED"
                lk["loss_rate"] = round(max(float(lk.get("loss_rate") or 0.0), 0.05 * severity), 4)
                lk["rtt_ms"] = round(max(float(lk.get("rtt_ms") or 0.0), 180 + 260 * severity), 2)
                lk["jitter_ms"] = round(max(float(lk.get("jitter_ms") or 0.0), 12 + 20 * severity), 2)
            return

        if scenario_type == "node_hotspot":
            target = str(params.get("node_id") or params.get("scope_id") or "").strip()
            n = nodes.get(target)
            if isinstance(n, dict):
                n["cpu_ratio"] = round(max(float(n.get("cpu_ratio") or 0.0), 0.85 + 0.12 * severity), 3)
                n["mem_ratio"] = round(max(float(n.get("mem_ratio") or 0.0), 0.75 + 0.2 * severity), 3)
                n["status"] = "DEGRADED" if severity < 0.8 else "UP"
            for lk in links.values():
                if not isinstance(lk, dict):
                    continue
                src = str(lk.get("src_node_uid") or lk.get("src_node_id") or "")
                dst = str(lk.get("dst_node_uid") or lk.get("dst_node_id") or "")
                if target not in {src, dst}:
                    continue
                lk["state"] = "DEGRADED"
                lk["rtt_ms"] = round(max(float(lk.get("rtt_ms") or 0.0), 140 + 120 * severity), 2)
                lk["loss_rate"] = round(max(float(lk.get("loss_rate") or 0.0), 0.02 + 0.04 * severity), 4)
            return

        # regional_blackout: affect nodes with common prefix.
        prefix = str(params.get("node_prefix") or "SAT-").strip()
        blackout_nodes = []
        for nid, n in nodes.items():
            if not isinstance(n, dict):
                continue
            if str(nid).startswith(prefix):
                n["status"] = "DOWN" if severity > 0.4 else "DEGRADED"
                n["cpu_ratio"] = round(max(float(n.get("cpu_ratio") or 0.0), 0.9), 3)
                blackout_nodes.append(str(nid))
        for lk in links.values():
            if not isinstance(lk, dict):
                continue
            src = str(lk.get("src_node_uid") or lk.get("src_node_id") or "")
            dst = str(lk.get("dst_node_uid") or lk.get("dst_node_id") or "")
            if src in blackout_nodes or dst in blackout_nodes:
                lk["state"] = "DOWN" if severity > 0.5 else "DEGRADED"
                lk["loss_rate"] = round(max(float(lk.get("loss_rate") or 0.0), 0.06 * severity), 4)
                lk["rtt_ms"] = round(max(float(lk.get("rtt_ms") or 0.0), 220 + 180 * severity), 2)

    def _build_tasks(self, link_metrics: dict[str, dict[str, Any]], max_tasks: int = 20) -> list[dict[str, Any]]:
        scored: list[tuple[float, str]] = []
        for uid, m in link_metrics.items():
            state = str(m.get("state") or "").upper()
            rtt = _f(m.get("rtt_ms"))
            loss = _f(m.get("loss_rate"))
            score = 0.0
            if state in {"DOWN", "DISCONNECTED"}:
                score += 100
            elif state == "DEGRADED":
                score += 40
            score += min(80.0, rtt / 4.0)
            score += min(60.0, loss * 1200.0)
            scored.append((score, uid))
        scored.sort(key=lambda x: (-x[0], x[1]))
        tasks: list[dict[str, Any]] = []
        for idx, (_, uid) in enumerate(scored[:max(1, max_tasks)], start=1):
            tasks.append(
                {
                    "task_id": f"sim-task-{idx:03d}",
                    "name": f"推演任务-{idx:03d}",
                    "criticality": 0.9 if idx <= 5 else 0.75,
                    "links": [uid],
                }
            )
        return tasks

    def _risk_summary(self, alarms: list[dict[str, Any]], spread: dict[str, Any], impact: dict[str, Any]) -> dict[str, Any]:
        task_rows = [x for x in (impact.get("tasks") or []) if isinstance(x, dict)]
        disconnected = sum(1 for x in task_rows if x.get("status") == "disconnected")
        degraded = sum(1 for x in task_rows if x.get("status") == "degraded")
        risk = "normal"
        if disconnected > 0 or len(alarms) >= 20:
            risk = "critical"
        elif degraded > 0 or len(alarms) >= 5:
            risk = "warning"
        return {
            "risk_level": risk,
            "alarm_total": len(alarms),
            "impacted_nodes": len(spread.get("impacted_nodes") or []),
            "impacted_links": len(spread.get("impacted_links") or []),
            "task_total": len(task_rows),
            "task_disconnected": disconnected,
        }


def _f(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0
