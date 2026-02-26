from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm_link_uid(src: str, dst: str) -> str:
    a = str(src or "").strip()
    b = str(dst or "").strip()
    if not a or not b:
        return ""
    return "<->".join(sorted([a, b]))


def _task_links(task: dict[str, Any]) -> list[str]:
    links = task.get("links")
    if isinstance(links, list):
        return [str(x) for x in links if str(x).strip()]
    paths = task.get("paths")
    if not isinstance(paths, list):
        return []
    merged: list[str] = []
    for path in paths:
        if not isinstance(path, list) or len(path) < 2:
            continue
        for idx in range(len(path) - 1):
            uid = _norm_link_uid(str(path[idx]), str(path[idx + 1]))
            if uid:
                merged.append(uid)
    seen: set[str] = set()
    out: list[str] = []
    for uid in merged:
        if uid in seen:
            continue
        seen.add(uid)
        out.append(uid)
    return out


def _impacted_links_from_spread(fault_spread: dict[str, Any] | None) -> set[str]:
    subgraph = (fault_spread or {}).get("subgraph", {})
    edges = subgraph.get("edges", [])
    out: set[str] = set()
    if not isinstance(edges, list):
        return out
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        uid = _norm_link_uid(str(edge.get("src") or ""), str(edge.get("dst") or ""))
        if uid:
            out.add(uid)
    return out


@dataclass
class TaskImpactRequest:
    tasks: list[dict[str, Any]]
    link_metrics: dict[str, dict[str, Any]]
    fault_spread: dict[str, Any] | None = None
    rtt_warn_ms: float = 180.0
    loss_warn_rate: float = 0.03


class TaskImpactService:
    def evaluate(self, req: TaskImpactRequest) -> dict[str, Any]:
        impacted_links = _impacted_links_from_spread(req.fault_spread)
        task_results: list[dict[str, Any]] = []
        work_orders: list[dict[str, Any]] = []
        for task in req.tasks:
            result = self._evaluate_task(task, req.link_metrics, impacted_links, req.rtt_warn_ms, req.loss_warn_rate)
            task_results.append(result)
            if result["status"] != "normal":
                work_orders.append(
                    {
                        "work_order_id": f"wo-{result['task_id'] or 'unknown'}",
                        "task_id": result["task_id"],
                        "status": "open",
                        "priority_score": result["priority_score"],
                        "suggestion": result["suggestion"],
                        "created_at": _now(),
                    }
                )
        task_results.sort(key=lambda item: (-float(item.get("priority_score", 0.0)), str(item.get("task_id", ""))))
        work_orders.sort(key=lambda item: -float(item.get("priority_score", 0.0)))
        return {
            "evaluated_at": _now(),
            "impacted_link_count": len(impacted_links),
            "tasks": task_results,
            "work_orders": work_orders,
        }

    def _evaluate_task(
        self,
        task: dict[str, Any],
        link_metrics: dict[str, dict[str, Any]],
        impacted_links: set[str],
        rtt_warn_ms: float,
        loss_warn_rate: float,
    ) -> dict[str, Any]:
        task_id = str(task.get("task_id") or "")
        links = _task_links(task)
        total_links = len(links)
        impacted_count = 0
        down_count = 0
        latency_abnormal_count = 0
        hit_links: list[str] = []

        for uid in links:
            metrics = link_metrics.get(uid, {})
            if uid in impacted_links:
                impacted_count += 1
                hit_links.append(uid)
            state = str(metrics.get("state") or "").upper()
            if state == "DOWN":
                down_count += 1
            try:
                rtt_ms = float(metrics.get("rtt_ms", 0.0))
            except (TypeError, ValueError):
                rtt_ms = 0.0
            try:
                loss_rate = float(metrics.get("loss_rate", 0.0))
            except (TypeError, ValueError):
                loss_rate = 0.0
            if rtt_ms >= rtt_warn_ms or loss_rate >= loss_warn_rate:
                latency_abnormal_count += 1

        impacted_ratio = (impacted_count / total_links) if total_links > 0 else 0.0
        down_ratio = (down_count / total_links) if total_links > 0 else 0.0
        if total_links == 0:
            status = "normal"
        elif down_ratio >= 0.5 or impacted_ratio >= 0.8:
            status = "disconnected"
        elif impacted_count > 0:
            status = "degraded"
        elif latency_abnormal_count > 0:
            status = "latency_anomaly"
        else:
            status = "normal"

        criticality = float(task.get("criticality", 0.5))
        status_weight = {"normal": 0.0, "latency_anomaly": 0.4, "degraded": 0.7, "disconnected": 1.0}[status]
        priority_score = round(100 * (0.5 * criticality + 0.35 * impacted_ratio + 0.15 * status_weight), 2)
        suggestion = (
            "优先恢复断链与主备切换"
            if status == "disconnected"
            else "提升链路质量并限流保护"
            if status == "degraded"
            else "检查时延/丢包并回退高风险路径"
            if status == "latency_anomaly"
            else "维持观测"
        )
        return {
            "task_id": task_id,
            "name": task.get("name"),
            "status": status,
            "priority_score": priority_score,
            "impacted_links": hit_links,
            "metrics": {
                "total_links": total_links,
                "impacted_links": impacted_count,
                "down_links": down_count,
                "latency_abnormal_links": latency_abnormal_count,
                "impacted_ratio": round(impacted_ratio, 4),
            },
            "suggestion": suggestion,
            "alert_item": {
                "task_id": task_id,
                "status": status,
                "ackable": status != "disconnected",
                "title": f"任务影响评估: {task.get('name') or task_id}",
                "detail": f"impacted={impacted_count}/{total_links}, down={down_count}, latency_abnormal={latency_abnormal_count}",
            },
        }
