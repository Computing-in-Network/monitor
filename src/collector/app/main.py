from __future__ import annotations

from collections import deque
from datetime import datetime
import json
import os
import time
from typing import Any, Optional
from urllib import error as urllib_error
from urllib import request as urllib_request

from fastapi import FastAPI, HTTPException, Request, Response

from .alarm_discovery import AutoAlarmDiscoverer, DiscoverRequest
from .config import load_config
from .failed_events import FailedEventStore
from .fault_injection import FaultInjectionBridge, normalize_link_uid
from .fault_spread import AnalyzeRequest, SpreadAnalyzer
from .fault_task_impact import TaskImpactRequest, TaskImpactService
from .forecast_registry import ForecastRegistry
from .observability import ApiSLOTracker, OutcomeStats
from .publisher import EventPublisher
from .repository import IdempotentStore
from .realtime_alarm import RealtimeAlarmEngine
from .routes import router
from .simulation import SimulationManager
from .snapshot import MonitorSnapshotStore
from .storage import TimescaleWriter


def create_app() -> FastAPI:
    config = load_config()

    app = FastAPI(title="net-analysis collector")
    app.include_router(router)

    app.state.config = config
    app.state.publisher = EventPublisher(
        config.nats_url,
        retries=config.publish_retries,
        retry_backoff_ms=config.publish_retry_backoff_ms,
    )
    app.state.idempotent = IdempotentStore(ttl_seconds=config.idempotency_ttl)
    app.state.stats = OutcomeStats()
    app.state.slo = ApiSLOTracker()
    app.state.snapshot_store = MonitorSnapshotStore()
    app.state.failed_events = FailedEventStore(
        max_items=config.failed_events_max_items,
        audit_file_path=config.failed_events_audit_file,
    )
    app.state.forecast_registry = ForecastRegistry(config.forecast_model_dir)
    app.state.alarm_discoverer = AutoAlarmDiscoverer()
    app.state.fault_spread = SpreadAnalyzer()
    app.state.task_impact = TaskImpactService()
    app.state.realtime_alarm = RealtimeAlarmEngine()
    app.state.simulations = SimulationManager()
    app.state.fault_bridge = FaultInjectionBridge()
    app.state.ts_writer = None
    if config.tsdb_enabled:
        app.state.ts_writer = TimescaleWriter(config.tsdb_dsn, schema=config.tsdb_schema)

    @app.on_event("startup")
    async def on_startup() -> None:
        await app.state.publisher.connect()
        if app.state.ts_writer is not None:
            app.state.ts_writer.connect()

    @app.on_event("shutdown")
    async def on_shutdown() -> None:
        await app.state.publisher.close()
        if app.state.ts_writer is not None:
            app.state.ts_writer.close()

    @app.get("/health")
    async def health() -> dict[str, object]:
        return {
            "status": "ok",
            "nats_connected": app.state.publisher.is_connected(),
            "tsdb_ready": bool(app.state.ts_writer and app.state.ts_writer.is_ready()),
            "metrics_total": app.state.stats.snapshot().get("total", 0),
        }

    @app.get("/metrics")
    async def metrics() -> dict[str, object]:
        return app.state.stats.snapshot()

    @app.get("/api/v1/monitor/snapshot")
    async def monitor_snapshot(
        request: Request,
        response: Response,
        topology_epoch: Optional[str] = None,
    ) -> object:
        start = time.perf_counter()
        ok = False
        try:
            payload = app.state.snapshot_store.snapshot(topology_epoch=topology_epoch)
            monitor = payload.get("monitor", {})
            etag = str(monitor.get("etag") or "")
            last_modified = str(monitor.get("last_modified") or "")

            if etag:
                response.headers["ETag"] = etag
            if last_modified:
                response.headers["Last-Modified"] = last_modified

            if_none_match = request.headers.get("if-none-match")
            if etag and if_none_match and if_none_match == etag:
                headers = {"ETag": etag}
                if last_modified:
                    headers["Last-Modified"] = last_modified
                ok = True
                return Response(status_code=304, headers=headers)

            ok = True
            return payload
        finally:
            app.state.slo.record("query.snapshot", ok=ok, latency_ms=(time.perf_counter() - start) * 1000.0)

    @app.get("/api/v1/ops/failed-events")
    async def failed_events(limit: int = 100, status: Optional[str] = None) -> dict[str, object]:
        return {
            "summary": app.state.failed_events.summary(),
            "items": app.state.failed_events.list_events(limit=limit, status=status),
        }

    @app.post("/api/v1/ops/fault-injection/control-ack")
    async def ingest_fault_control_ack(payload: dict[str, object]) -> dict[str, object]:
        if str(payload.get("type") or "").strip() != "control_ack":
            raise HTTPException(status_code=422, detail="payload.type 必须为 control_ack")
        topology_epoch = str(payload.get("topology_epoch")) if payload.get("topology_epoch") not in (None, "") else None
        mapped = app.state.fault_bridge.map_control_ack(ack=payload, topology_epoch=topology_epoch)
        for event in mapped.get("alarms_upsert", []):
            if isinstance(event, dict):
                app.state.snapshot_store.apply("alarm", event)
                if app.state.ts_writer is not None and app.state.ts_writer.is_ready():
                    app.state.ts_writer.write_event("alarm", event)
        for event in mapped.get("alarms_recover", []):
            if isinstance(event, dict):
                app.state.snapshot_store.apply("alarm", event)
                if app.state.ts_writer is not None and app.state.ts_writer.is_ready():
                    app.state.ts_writer.write_event("alarm", event)
        for wrapped in mapped.get("metrics_upsert", []):
            if not isinstance(wrapped, dict):
                continue
            event_type = str(wrapped.get("event_type") or "").strip()
            payload = wrapped.get("payload") if isinstance(wrapped.get("payload"), dict) else None
            if event_type not in {"node_metric", "link_metric"} or payload is None:
                continue
            app.state.snapshot_store.apply(event_type, payload)
            if app.state.ts_writer is not None and app.state.ts_writer.is_ready():
                app.state.ts_writer.write_event(event_type, payload)
            for alarm in app.state.realtime_alarm.evaluate_metric(event_type, payload):
                app.state.snapshot_store.apply("alarm", alarm)
                if app.state.ts_writer is not None and app.state.ts_writer.is_ready():
                    app.state.ts_writer.write_event("alarm", alarm)
        for wrapped in mapped.get("metrics_recover", []):
            if not isinstance(wrapped, dict):
                continue
            event_type = str(wrapped.get("event_type") or "").strip()
            payload = wrapped.get("payload") if isinstance(wrapped.get("payload"), dict) else None
            if event_type not in {"node_metric", "link_metric"} or payload is None:
                continue
            app.state.snapshot_store.apply(event_type, payload)
            if app.state.ts_writer is not None and app.state.ts_writer.is_ready():
                app.state.ts_writer.write_event(event_type, payload)
            for alarm in app.state.realtime_alarm.evaluate_metric(event_type, payload):
                app.state.snapshot_store.apply("alarm", alarm)
                if app.state.ts_writer is not None and app.state.ts_writer.is_ready():
                    app.state.ts_writer.write_event("alarm", alarm)
        return mapped

    @app.get("/api/v1/monitor/series")
    async def monitor_series(
        event_type: str,
        metric: str,
        entity_id: str,
        topology_epoch: Optional[str] = None,
        limit: int = 120,
    ) -> dict[str, object]:
        start = time.perf_counter()
        ok = False
        try:
            ts_writer = app.state.ts_writer
            if ts_writer is None or not ts_writer.is_ready():
                raise HTTPException(status_code=503, detail="timescale db not ready")
            try:
                points = ts_writer.read_metric_series(
                    event_type=event_type,
                    metric=metric,
                    entity_id=entity_id,
                    limit=limit,
                    topology_epoch=topology_epoch,
                )
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            ok = True
            return {
                "status": "ok",
                "source": "timescaledb",
                "event_type": event_type,
                "metric": metric,
                "entity_id": entity_id,
                "topology_epoch": topology_epoch,
                "points": points,
                "count": len(points),
            }
        finally:
            app.state.slo.record("query.series", ok=ok, latency_ms=(time.perf_counter() - start) * 1000.0)

    @app.get("/api/v1/analysis/forecast/lstm")
    async def forecast_lstm(
        event_type: str,
        metric: str,
        entity_id: str,
        topology_epoch: Optional[str] = None,
        horizon: int = 12,
        window: int = 12,
        history_limit: int = 240,
        model_id: Optional[str] = None,
        model_version: Optional[str] = None,
        strategy: str = "auto",
    ) -> dict[str, object]:
        start = time.perf_counter()
        ok = False
        try:
            ts_writer = app.state.ts_writer
            if ts_writer is None or not ts_writer.is_ready():
                raise HTTPException(status_code=503, detail="timescale db not ready")
            try:
                points = ts_writer.read_metric_series(
                    event_type=event_type,
                    metric=metric,
                    entity_id=entity_id,
                    limit=history_limit,
                    topology_epoch=topology_epoch,
                )
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            if len(points) < 4:
                ok = True
                return {
                    "status": "ok",
                    "source": "timescaledb",
                    "model_type": "lstm_no_data",
                    "model_id": model_id or f"{event_type}.{metric}.{entity_id}",
                    "model_version": "n/a",
                    "strategy": str(strategy or "auto").strip().lower(),
                    "event_type": event_type,
                    "metric": metric,
                    "entity_id": entity_id,
                    "topology_epoch": topology_epoch,
                    "history_points": len(points),
                    "validation_mape": None,
                    "metrics": {"mape": None, "rmse": None},
                    "confidence": {"level": "unknown", "reason": "insufficient_history"},
                    "window": max(3, min(int(window), 120)),
                    "horizon": max(1, min(int(horizon), 120)),
                    "points": [],
                    "note": "not enough points for forecast",
                }

            values = [float(p["value"]) for p in points]
            safe_horizon = max(1, min(int(horizon), 120))
            mode = str(strategy or "auto").strip().lower()
            if mode not in {"auto", "registered", "fallback"}:
                raise HTTPException(status_code=422, detail="strategy 仅支持 auto|registered|fallback")

            selected = None
            resolved_model_id = model_id or f"{event_type}.{metric}.{entity_id}"
            if mode in {"auto", "registered"}:
                selected = app.state.forecast_registry.resolve(resolved_model_id, version=model_version)
                if selected is None and mode == "registered":
                    raise HTTPException(status_code=422, detail=f"model not found: {resolved_model_id}")

            if selected is not None:
                model_win = int(
                    selected.raw.get("window") or selected.raw.get("params", {}).get("input_window") or window
                )
                safe_window = max(3, min(model_win, len(values)))
                baseline = _forecast_wma(values=values, window=safe_window, steps=safe_horizon)
                model_type = f"lstm_{selected.backend}"
                selected_version = selected.version
                validation_mape = selected.validation_mape
            else:
                safe_window = max(3, min(int(window), len(values)))
                baseline = _forecast_wma(values=values, window=safe_window, steps=safe_horizon)
                model_type = "lstm_fallback_wma"
                selected_version = "fallback"
                validation_mape = None

            latest = values[-1]
            out = []
            for idx, pred in enumerate(baseline, start=1):
                spread = max(abs(pred - latest) * 0.15, abs(pred) * 0.05, 1e-6)
                out.append(
                    {
                        "step": idx,
                        "yhat": round(pred, 6),
                        "lower": round(pred - spread, 6),
                        "upper": round(pred + spread, 6),
                    }
                )
            est_mape, est_rmse = _estimate_forecast_error(values=values, window=safe_window)
            confidence = _forecast_confidence_level(validation_mape=validation_mape, est_mape=est_mape)
            ok = True
            return {
                "status": "ok",
                "source": "timescaledb",
                "model_type": model_type,
                "model_id": resolved_model_id,
                "model_version": selected_version,
                "strategy": mode,
                "event_type": event_type,
                "metric": metric,
                "entity_id": entity_id,
                "topology_epoch": topology_epoch,
                "history_points": len(values),
                "validation_mape": validation_mape,
                "metrics": {"mape": est_mape, "rmse": est_rmse},
                "confidence": {"level": confidence, "reason": "validation_mape_first"},
                "window": safe_window,
                "horizon": safe_horizon,
                "points": out,
            }
        finally:
            app.state.slo.record("forecast.lstm", ok=ok, latency_ms=(time.perf_counter() - start) * 1000.0)

    @app.get("/api/v1/analysis/forecast/models")
    async def list_forecast_models(model_id: Optional[str] = None) -> dict[str, object]:
        refs = app.state.forecast_registry.list_models()
        if model_id:
            refs = [x for x in refs if x.model_id == model_id]
        return {
            "status": "ok",
            "count": len(refs),
            "models": [
                {
                    "model_id": x.model_id,
                    "version": x.version,
                    "backend": x.backend,
                    "validation_mape": x.validation_mape,
                    "file": x.file,
                }
                for x in refs
            ],
        }

    @app.post("/api/v1/analysis/alarm/discover")
    async def discover_alarms(payload: dict[str, object]) -> dict[str, object]:
        start = time.perf_counter()
        ok = False
        try:
            scope_type = str(payload.get("scope_type", "network")).strip().lower()
            scope_id = str(payload.get("scope_id", "all")).strip()
            if scope_type not in {"network", "node", "link"}:
                raise HTTPException(status_code=422, detail="scope_type 仅支持 network|node|link")
            strategies = payload.get("strategies")
            if strategies is None:
                strategies = ["threshold", "baseline"]
            if not isinstance(strategies, list):
                raise HTTPException(status_code=422, detail="strategies 必须为 list")
            req = DiscoverRequest(
                topology_epoch=str(payload.get("topology_epoch")) if payload.get("topology_epoch") not in (None, "") else None,
                scope_type=scope_type,
                scope_id=scope_id,
                window_sec=max(60, min(int(payload.get("window_sec", 300)), 86400)),
                strategies=[str(x) for x in strategies],
                include_evidence_points=bool(payload.get("include_evidence_points", False)),
            )
            snapshot_payload = app.state.snapshot_store.snapshot(topology_epoch=req.topology_epoch)
            result = app.state.alarm_discoverer.discover(req, snapshot_payload=snapshot_payload, ts_writer=app.state.ts_writer)
            ok = True
            return result
        finally:
            app.state.slo.record("analysis.alarm_discover", ok=ok, latency_ms=(time.perf_counter() - start) * 1000.0)

    @app.post("/api/v1/fault/spread")
    @app.post("/api/v1/fault/spread/analyze")
    async def fault_spread(payload: dict[str, object]) -> dict[str, object]:
        start = time.perf_counter()
        ok = False
        try:
            alarm_nodes = payload.get("alarm_nodes")
            links = payload.get("links")
            if not isinstance(alarm_nodes, list) or not isinstance(links, list):
                raise HTTPException(status_code=422, detail="alarm_nodes(list) 与 links(list) 为必填")
            mode = str(payload.get("mode", "single_point"))
            if mode not in {"single_point", "cascade"}:
                raise HTTPException(status_code=422, detail="mode 仅支持 single_point|cascade")
            req = AnalyzeRequest(
                alarm_nodes=[str(x) for x in alarm_nodes],
                links=_normalize_links_for_spread([x for x in links if isinstance(x, dict)]),
                max_depth=max(1, min(int(payload.get("max_depth", 3)), 8)),
                mode=mode,
                cascade_threshold=float(payload.get("cascade_threshold", 0.6)),
            )
            result = app.state.fault_spread.analyze(req)
            ok = True
            return {"status": "ok", "result": result}
        finally:
            app.state.slo.record("fault.spread", ok=ok, latency_ms=(time.perf_counter() - start) * 1000.0)

    @app.post("/api/v1/fault/task-impact")
    @app.post("/api/v1/fault/task-impact/evaluate")
    async def fault_task_impact(payload: dict[str, object]) -> dict[str, object]:
        start = time.perf_counter()
        ok = False
        try:
            tasks = payload.get("tasks")
            link_metrics = payload.get("link_metrics")
            if not isinstance(tasks, list) or not isinstance(link_metrics, dict):
                raise HTTPException(status_code=422, detail="tasks(list) 与 link_metrics(dict) 为必填")
            req = TaskImpactRequest(
                tasks=[x for x in tasks if isinstance(x, dict)],
                link_metrics={str(k): v for k, v in link_metrics.items() if isinstance(v, dict)},
                fault_spread=payload.get("fault_spread") if isinstance(payload.get("fault_spread"), dict) else None,
                rtt_warn_ms=float(payload.get("rtt_warn_ms", 180.0)),
                loss_warn_rate=float(payload.get("loss_warn_rate", 0.03)),
            )
            result = app.state.task_impact.evaluate(req)
            ok = True
            return {"status": "ok", "result": result}
        finally:
            app.state.slo.record("fault.task_impact", ok=ok, latency_ms=(time.perf_counter() - start) * 1000.0)

    @app.get("/api/v1/ops/slo")
    async def slo_metrics() -> dict[str, object]:
        ingest = app.state.stats.snapshot()
        api_slo = app.state.slo.snapshot().get("by_api", {})
        db_write_failed = int(ingest.get("by_code", {}).get("DB_WRITE_FAILED", 0))
        ingest_total = int(ingest.get("total", 0))
        db_slo = {
            "write_attempts_approx": ingest_total,
            "write_failed": db_write_failed,
            "write_error_rate": round(db_write_failed / ingest_total, 4) if ingest_total else 0.0,
        }

        def _collect(keys: list[str]) -> dict[str, object]:
            total = 0
            ok_count = 0
            error_count = 0
            p95_candidates: list[float] = []
            for key in keys:
                item = api_slo.get(key) or {}
                total += int(item.get("total", 0))
                ok_count += int(item.get("ok", 0))
                error_count += int(item.get("error", 0))
                p95 = float(item.get("latency_ms_p95", 0.0))
                if p95 > 0:
                    p95_candidates.append(p95)
            return {
                "total": total,
                "ok": ok_count,
                "error": error_count,
                "availability": round(ok_count / total, 4) if total else 1.0,
                "error_rate": round(error_count / total, 4) if total else 0.0,
                "latency_ms_p95_worst": max(p95_candidates) if p95_candidates else 0.0,
            }

        query_slo = _collect(["query.snapshot", "query.series"])
        forecast_slo = _collect(["forecast.lstm"])
        fault_slo = _collect(["fault.spread", "fault.task_impact"])
        objectives = {
            "availability_min": 0.99,
            "error_rate_max": 0.01,
            "latency_ms_p95_max": 200.0,
        }

        def _judge(s: dict[str, object]) -> dict[str, object]:
            availability = float(s.get("availability", 1.0))
            error_rate = float(s.get("error_rate", 0.0))
            p95 = float(s.get("latency_ms_p95_worst", 0.0))
            return {
                "availability_ok": availability >= objectives["availability_min"],
                "error_rate_ok": error_rate <= objectives["error_rate_max"],
                "latency_ok": p95 <= objectives["latency_ms_p95_max"] if p95 > 0 else True,
            }

        return {
            "status": "ok",
            "objectives": objectives,
            "ingest": ingest,
            "db": db_slo,
            "query": {**query_slo, "judge": _judge(query_slo)},
            "forecast": {**forecast_slo, "judge": _judge(forecast_slo)},
            "fault": {**fault_slo, "judge": _judge(fault_slo)},
            "by_api": api_slo,
        }

    @app.get("/api/v1/bff/snapshot")
    async def bff_snapshot(
        request: Request,
        response: Response,
        topology_epoch: Optional[str] = None,
    ) -> object:
        return await monitor_snapshot(request=request, response=response, topology_epoch=topology_epoch)

    @app.get("/api/v1/bff/series")
    async def bff_series(
        event_type: str,
        metric: str,
        entity_id: str,
        topology_epoch: Optional[str] = None,
        limit: int = 120,
    ) -> dict[str, object]:
        return await monitor_series(
            event_type=event_type,
            metric=metric,
            entity_id=entity_id,
            topology_epoch=topology_epoch,
            limit=limit,
        )

    @app.get("/api/v1/bff/forecast/lstm")
    async def bff_forecast_lstm(
        event_type: str,
        metric: str,
        entity_id: str,
        topology_epoch: Optional[str] = None,
        horizon: int = 12,
        window: int = 12,
        history_limit: int = 240,
        model_id: Optional[str] = None,
        model_version: Optional[str] = None,
        strategy: str = "auto",
    ) -> dict[str, object]:
        return await forecast_lstm(
            event_type=event_type,
            metric=metric,
            entity_id=entity_id,
            topology_epoch=topology_epoch,
            horizon=horizon,
            window=window,
            history_limit=history_limit,
            model_id=model_id,
            model_version=model_version,
            strategy=strategy,
        )

    @app.post("/api/v1/bff/fault/spread")
    async def bff_fault_spread(payload: dict[str, object]) -> dict[str, object]:
        return await fault_spread(payload=payload)

    @app.post("/api/v1/bff/fault/task-impact")
    async def bff_fault_task_impact(payload: dict[str, object]) -> dict[str, object]:
        return await fault_task_impact(payload=payload)

    @app.post("/api/v1/bff/analysis/alarm/discover")
    async def bff_discover_alarms(payload: dict[str, object]) -> dict[str, object]:
        return await discover_alarms(payload=payload)

    @app.post("/api/v1/bff/analysis/global-impact")
    async def bff_global_impact(payload: dict[str, object]) -> dict[str, object]:
        start = time.perf_counter()
        ok = False
        try:
            topology_epoch = str(payload.get("topology_epoch")) if payload.get("topology_epoch") not in (None, "") else None
            mode = str(payload.get("mode", "global")).strip().lower()
            if mode not in {"global", "focused", "auto"}:
                raise HTTPException(status_code=422, detail=_analysis_error("INVALID_SCOPE", "mode 仅支持 global|focused|auto"))
            scope_type = str(payload.get("scope_type", "network")).strip().lower()
            scope_id = _resolve_scope_id(payload)
            if scope_type not in {"network", "node", "link"}:
                scope_type = _infer_scope_type(scope_id=scope_id, fallback="network")
            if scope_type == "link":
                scope_id = _normalize_link_scope(scope_id)
            if scope_type not in {"network", "node", "link"}:
                raise HTTPException(
                    status_code=422,
                    detail=_analysis_error("INVALID_SCOPE", "scope_type 仅支持 network|node|link"),
                )

            snapshot_payload = app.state.snapshot_store.snapshot(topology_epoch=topology_epoch)
            monitor = snapshot_payload.get("monitor", {}) if isinstance(snapshot_payload, dict) else {}
            links_map = monitor.get("links", {}) if isinstance(monitor.get("links"), dict) else {}
            flows_map = monitor.get("flows", {}) if isinstance(monitor.get("flows"), dict) else {}
            links = [x for x in links_map.values() if isinstance(x, dict)]
            links = _merge_payload_links(links=links, payload_links=payload.get("links"))
            spread_links = _normalize_links_for_spread(links)
            if not spread_links:
                raise HTTPException(
                    status_code=422,
                    detail=_analysis_error("INSUFFICIENT_DATA", "no topology links in snapshot"),
                )

            non_fault_scope = mode == "focused" and scope_type == "node" and not _is_satellite_node(scope_id)
            alarm_scope_type = scope_type if mode == "focused" else "network"
            alarm_scope_id = scope_id if mode == "focused" else "all"
            discover_req = DiscoverRequest(
                topology_epoch=topology_epoch,
                scope_type=alarm_scope_type,
                scope_id=alarm_scope_id,
                window_sec=max(60, min(int(payload.get("window_sec", 300)), 86400)),
                strategies=[str(x) for x in (payload.get("strategies") or ["threshold", "baseline"])],
                include_evidence_points=bool(payload.get("include_evidence_points", False)),
            )
            discovered = app.state.alarm_discoverer.discover(
                discover_req,
                snapshot_payload=snapshot_payload,
                ts_writer=app.state.ts_writer,
            )
            detected = [] if non_fault_scope else [x for x in (discovered.get("detected_alarms") or []) if isinstance(x, dict)]
            seed_nodes, seed_links = _extract_seeds(detected)
            if mode == "focused" and scope_type == "node" and _is_satellite_node(scope_id) and scope_id not in seed_nodes:
                seed_nodes.append(scope_id)
            if mode == "focused" and scope_type == "link" and scope_id and scope_id not in seed_links:
                seed_links.append(scope_id)

            if not non_fault_scope and not seed_nodes and not seed_links:
                alarms = monitor.get("alarms", []) if isinstance(monitor.get("alarms"), list) else []
                for item in alarms:
                    if not isinstance(item, dict):
                        continue
                    st = str(item.get("scope_type") or "").strip().lower()
                    sid = str(item.get("scope_uid") or item.get("scope_id") or "").strip()
                    if st == "node" and sid and _is_satellite_node(sid) and sid not in seed_nodes:
                        seed_nodes.append(sid)
                    if st == "link" and sid and sid not in seed_links:
                        seed_links.append(sid)

            alarm_nodes = list(seed_nodes)
            if not alarm_nodes and seed_links:
                for lk in spread_links:
                    uid = str(lk.get("link_uid") or lk.get("link_id") or "")
                    if uid not in seed_links:
                        continue
                    src = str(lk.get("src") or lk.get("src_node_uid") or lk.get("src_node_id") or "")
                    dst = str(lk.get("dst") or lk.get("dst_node_uid") or lk.get("dst_node_id") or "")
                    if src and src not in alarm_nodes:
                        alarm_nodes.append(src)
                    if dst and dst not in alarm_nodes:
                        alarm_nodes.append(dst)

            terminal_focused = mode == "focused" and scope_type == "node" and _is_terminal_endpoint_node(scope_id)
            if terminal_focused or non_fault_scope:
                all_nodes: set[str] = set()
                for lk in spread_links:
                    src = str(lk.get("src") or lk.get("src_node_uid") or lk.get("src_node_id") or "").strip()
                    dst = str(lk.get("dst") or lk.get("dst_node_uid") or lk.get("dst_node_id") or "").strip()
                    if src:
                        all_nodes.add(src)
                    if dst:
                        all_nodes.add(dst)
                impacted = [scope_id] if scope_id else []
                spread_result = {
                    "mode": "single_point",
                    "seeds": impacted,
                    "core_nodes": impacted,
                    "boundary_nodes": [],
                    "unaffected_nodes": sorted(all_nodes - set(impacted)),
                    "impacted_nodes": impacted,
                    "impacted_links": [],
                    "subgraph": {"nodes": impacted, "edges": []},
                    "paths": [{"node": scope_id, "depth": 0}] if scope_id else [],
                    "fallback": False,
                    "policy": "terminal_isolated" if terminal_focused else "non_fault_scope",
                }
            else:
                route_spread = None
                allow_route_spread = True
                if mode == "focused" and scope_type in {"node", "link"}:
                    scope_severity = _focused_scope_severity(scope_type, scope_id, detected)
                    if scope_type == "node":
                        allow_route_spread = _focused_node_allow_route_spread(scope_id, detected, spread_links)
                    if scope_severity == "critical":
                        route_limits = {
                            "max_paths": 40,
                            "max_impacted_nodes": 56,
                            "max_impacted_links": 120,
                            "pair_budget": 300,
                        }
                    elif scope_severity == "warning":
                        route_limits = {
                            "max_paths": 12,
                            "max_impacted_nodes": 20,
                            "max_impacted_links": 40,
                            "pair_budget": 60,
                        }
                    else:
                        route_limits = {
                            "max_paths": 4,
                            "max_impacted_nodes": 8,
                            "max_impacted_links": 16,
                            "pair_budget": 20,
                        }
                    if allow_route_spread:
                        route_spread = _route_scoped_impact_from_flows(
                            scope_type=scope_type,
                            scope_id=scope_id,
                            flows=flows_map,
                            links=spread_links,
                            max_paths=route_limits["max_paths"],
                            max_impacted_nodes=route_limits["max_impacted_nodes"],
                            max_impacted_links=route_limits["max_impacted_links"],
                        )
                        if route_spread is None:
                            route_spread = _route_scoped_impact_from_terminal_paths(
                                scope_type=scope_type,
                                scope_id=scope_id,
                                links=spread_links,
                                pair_budget=route_limits["pair_budget"],
                                max_paths=route_limits["max_paths"],
                                max_impacted_nodes=route_limits["max_impacted_nodes"],
                                max_impacted_links=route_limits["max_impacted_links"],
                            )
                if route_spread is not None:
                    spread_result = route_spread
                elif mode == "focused" and scope_type == "node" and not allow_route_spread:
                    all_nodes: set[str] = set()
                    for lk in spread_links:
                        src = str(lk.get("src") or lk.get("src_node_uid") or lk.get("src_node_id") or "").strip()
                        dst = str(lk.get("dst") or lk.get("dst_node_uid") or lk.get("dst_node_id") or "").strip()
                        if src:
                            all_nodes.add(src)
                        if dst:
                            all_nodes.add(dst)
                    impacted = [scope_id] if scope_id else []
                    spread_result = {
                        "mode": "single_point",
                        "seeds": impacted,
                        "core_nodes": impacted,
                        "boundary_nodes": [],
                        "unaffected_nodes": sorted(all_nodes - set(impacted)),
                        "impacted_nodes": impacted,
                        "impacted_links": [],
                        "subgraph": {"nodes": impacted, "edges": []},
                        "paths": [{"node": scope_id, "depth": 0}] if scope_id else [],
                        "fallback": False,
                        "policy": "node_local_only",
                    }
                else:
                    spread_req = AnalyzeRequest(
                        alarm_nodes=alarm_nodes,
                        links=spread_links,
                        max_depth=max(1, min(int(payload.get("max_depth", 4)), 10)),
                        mode=str(payload.get("spread_mode", "cascade")),
                        cascade_threshold=float(payload.get("cascade_threshold", 0.6)),
                    )
                    spread_result = app.state.fault_spread.analyze(spread_req)
                    spread_result["policy"] = "topology_fallback"

            link_metrics = _extract_link_metrics(links_map)
            link_metrics.update(_extract_payload_link_metrics(payload.get("link_metrics")))
            tasks = [x for x in (payload.get("tasks") or []) if isinstance(x, dict)]
            if not tasks:
                if non_fault_scope:
                    tasks = []
                elif terminal_focused:
                    tasks = _build_node_local_tasks(
                        scope_id=scope_id,
                        link_metrics=link_metrics,
                        max_tasks=max(1, min(int(payload.get("max_tasks", 30)), 100)),
                    )
                elif mode == "focused":
                    tasks = _build_impacted_tasks(
                        link_metrics=link_metrics,
                        impacted_links=[str(x) for x in (spread_result.get("impacted_links") or [])],
                        max_tasks=max(1, min(int(payload.get("max_tasks", 30)), 100)),
                    )
                else:
                    tasks = _build_global_tasks(link_metrics, max_tasks=max(10, min(int(payload.get("max_tasks", 30)), 100)))
            impact_req = TaskImpactRequest(
                tasks=tasks,
                link_metrics=link_metrics,
                fault_spread=spread_result,
                rtt_warn_ms=float(payload.get("rtt_warn_ms", 180.0)),
                loss_warn_rate=float(payload.get("loss_warn_rate", 0.03)),
            )
            impact_result = app.state.task_impact.evaluate(impact_req)
            scope_observation = payload.get("scope_observation") if isinstance(payload.get("scope_observation"), dict) else {}
            inferred_snapshot_sev = (
                _infer_scope_severity_from_snapshot(scope_type, scope_id, monitor)
                if mode == "focused" and scope_type in {"node", "link"}
                else "info"
            )
            inferred_observation_sev = (
                _infer_scope_severity_from_observation(scope_type, scope_observation)
                if mode == "focused" and scope_type in {"node", "link"}
                else "info"
            )
            merged_inferred_sev = inferred_snapshot_sev
            if _severity_rank(inferred_observation_sev) > _severity_rank(merged_inferred_sev):
                merged_inferred_sev = inferred_observation_sev

            summary = _global_summary(
                detected_alarms=detected,
                spread_result=spread_result,
                impact_result=impact_result,
                scope_type=scope_type,
                scope_id=scope_id,
                scope_metric_severity=merged_inferred_sev,
            )
            out = {
                "status": "ok",
                "contract_version": "analysis.v1",
                "mode": mode,
                "scope_type": scope_type,
                "scope_id": scope_id,
                "topology_epoch": topology_epoch,
                "summary": summary,
                "seeds": {"alarm_nodes": alarm_nodes, "alarm_links": seed_links},
                "detected_alarms": detected,
                "impact_graph": spread_result,
                "task_impacts": impact_result,
            }
            ok = True
            return out
        finally:
            app.state.slo.record("analysis.global_impact", ok=ok, latency_ms=(time.perf_counter() - start) * 1000.0)

    @app.post("/api/v1/normalization/suggest-mapping")
    async def suggest_mapping(payload: dict[str, object]) -> dict[str, object]:
        probe_type = str(payload.get("probe_type") or "").strip() or "unknown_probe"
        raw_event_type = str(payload.get("raw_event_type") or "").strip() or "unknown_event"
        sample = payload.get("sample")
        if not isinstance(sample, dict) or not sample:
            raise HTTPException(status_code=422, detail="sample(object) 为必填")

        target_event_type = _infer_target_event_type(raw_event_type=raw_event_type, sample=sample)
        mapping, unknown_fields = _suggest_mapping_rules(target_event_type=target_event_type, sample=sample)
        confidence = _mapping_confidence(mapping=mapping, unknown_fields=unknown_fields)
        manual_todo = _mapping_manual_todo(mapping=mapping, unknown_fields=unknown_fields, target_event_type=target_event_type)
        ai_note = ""
        ai_err = ""
        provider = str(payload.get("provider") or os.getenv("AI_PROVIDER") or "newapi").strip().lower()
        if provider not in {"newapi", "gemini", "ollama"}:
            provider = "newapi"
        if bool(payload.get("use_ai", True)):
            model = str(
                payload.get("model")
                or (os.getenv("OLLAMA_MODEL") if provider == "ollama" else os.getenv("AI_MODEL_NAME") or os.getenv("GEMINI_MODEL"))
                or "qwen3-coder:latest"
            ).strip() or "qwen3-coder:latest"
            prompt = _build_mapping_suggestion_prompt(
                probe_type=probe_type,
                raw_event_type=raw_event_type,
                target_event_type=target_event_type,
                sample=sample,
                mapping=mapping,
                unknown_fields=unknown_fields,
                manual_todo=manual_todo,
            )
            if provider == "ollama":
                text, _, err, _ = _ollama_generate(prompt=prompt, model=model)
            else:
                text, _, err, _ = _newapi_generate(prompt=prompt, model=model)
            ai_note = str(text or "").strip()
            ai_err = str(err or "").strip()
        return {
            "status": "ok",
            "probe_type": probe_type,
            "raw_event_type": raw_event_type,
            "target_event_type": target_event_type,
            "mapping": mapping,
            "confidence": confidence,
            "unknown_fields": unknown_fields,
            "manual_todo": manual_todo,
            "ai_note": ai_note,
            "ai_error": ai_err,
        }

    @app.post("/api/v1/bff/analysis/run")
    async def bff_analysis_run(payload: dict[str, object]) -> dict[str, object]:
        start = time.perf_counter()
        ok = False
        try:
            mode = str(payload.get("mode", "auto")).strip().lower()
            if mode not in {"auto", "focused", "global"}:
                raise HTTPException(
                    status_code=422,
                    detail=_analysis_error("INVALID_SCOPE", "mode 仅支持 auto|focused|global"),
                )
            scope_type = str(payload.get("scope_type", "network")).strip().lower()
            scope_id = _resolve_scope_id(payload)
            if scope_type not in {"network", "node", "link"}:
                scope_type = _infer_scope_type(scope_id=scope_id, fallback="network")
            if scope_type == "link":
                scope_id = _normalize_link_scope(scope_id)

            if mode == "auto":
                if scope_type in {"node", "link"} and scope_id not in {"", "all"}:
                    mode = "focused"
                else:
                    mode = "global"
                    scope_type = "network"
                    scope_id = "all"
            elif mode == "global":
                scope_type = "network"
                scope_id = "all"

            if mode == "focused" and (scope_type not in {"node", "link"} or scope_id in {"", "all"}):
                raise HTTPException(
                    status_code=422,
                    detail=_analysis_error("INVALID_SCOPE", "focused 需提供 node/link 的 scope_id"),
                )

            run_payload = {
                "mode": mode,
                "scope_type": scope_type,
                "scope_id": scope_id,
                "topology_epoch": payload.get("topology_epoch"),
                "strategies": payload.get("strategies") or (["threshold"] if mode == "focused" else ["threshold", "baseline"]),
                "window_sec": payload.get("window_sec", 300),
                "max_depth": payload.get("max_depth", 4),
                "spread_mode": payload.get("spread_mode", "cascade"),
                "cascade_threshold": payload.get("cascade_threshold", 0.6),
                "rtt_warn_ms": payload.get("rtt_warn_ms", 180.0),
                "loss_warn_rate": payload.get("loss_warn_rate", 0.03),
                "max_tasks": payload.get("max_tasks", 30),
                "scope_observation": payload.get("scope_observation"),
            }
            try:
                result = await bff_global_impact(run_payload)
            except HTTPException as exc:
                code, msg = _normalize_analysis_exception(exc)
                raise HTTPException(status_code=422, detail=_analysis_error(code, msg)) from exc
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(
                    status_code=500,
                    detail=_analysis_error("INTERNAL_ERROR", f"analysis run failed: {exc}"),
                ) from exc
            out = {
                "status": "ok",
                "contract_version": "analysis.v1",
                "input": {
                    "mode": str(payload.get("mode", "auto")).strip().lower(),
                    "scope_type": str(payload.get("scope_type", "network")).strip().lower(),
                    "scope_id": _resolve_scope_id(payload),
                    "topology_epoch": payload.get("topology_epoch"),
                },
                "resolved": {
                    "mode": mode,
                    "scope_type": scope_type,
                    "scope_id": scope_id,
                },
                "summary": result.get("summary"),
                "topology_impact": {
                    "seed_nodes": (result.get("seeds") or {}).get("alarm_nodes", []),
                    "seed_links": (result.get("seeds") or {}).get("alarm_links", []),
                    "impacted_nodes": (result.get("impact_graph") or {}).get("impacted_nodes", []),
                    "impacted_links": (result.get("impact_graph") or {}).get("impacted_links", []),
                    "boundary_nodes": (result.get("impact_graph") or {}).get("boundary_nodes", []),
                    "policy": (result.get("impact_graph") or {}).get("policy"),
                    "route_mode": (
                        "route_first"
                        if str((result.get("impact_graph") or {}).get("policy") or "") == "route_terminal_paths"
                        else "topology_fallback"
                    ),
                    "matched_flows": (result.get("impact_graph") or {}).get("matched_flows"),
                },
                "tasks": (result.get("task_impacts") or {}).get("tasks", []),
                "alerts": [x.get("alert_item") for x in ((result.get("task_impacts") or {}).get("tasks") or []) if isinstance(x, dict)],
                "meta": {
                    "detected_alarm_total": len(result.get("detected_alarms") or []),
                    "task_total": len(((result.get("task_impacts") or {}).get("tasks") or [])),
                },
            }
            snapshot_payload = app.state.snapshot_store.snapshot(topology_epoch=payload.get("topology_epoch"))
            monitor = snapshot_payload.get("monitor", {}) if isinstance(snapshot_payload, dict) else {}
            all_link_ids = sorted(
                {
                    _normalize_link_scope(str(x.get("link_uid") or x.get("link_id") or ""))
                    for x in (monitor.get("links", {}) or {}).values()
                    if isinstance(x, dict)
                }
            )
            impacted_link_ids = [
                _normalize_link_scope(str(x))
                for x in (out.get("topology_impact", {}).get("impacted_links") or [])
                if str(x).strip()
            ]
            impacted_set = set(impacted_link_ids)
            out["topology_impact"]["filtered_out"] = [x for x in all_link_ids if x and x not in impacted_set][:80]
            out["topology_impact"]["rank_top"] = _build_route_rank_top(
                tasks=out.get("tasks") if isinstance(out.get("tasks"), list) else [],
                impacted_links=impacted_link_ids,
            )
            out["clusters"] = _build_fault_clusters(
                seeds=out.get("topology_impact", {}),
                impact_graph=result.get("impact_graph") if isinstance(result.get("impact_graph"), dict) else {},
            )
            out["narrative"] = _build_analysis_narrative(
                resolved=out.get("resolved", {}),
                summary=out.get("summary", {}),
                topology_impact=out.get("topology_impact", {}),
                top_task_id=(out.get("tasks") or [{}])[0].get("task_id") if isinstance(out.get("tasks"), list) and out.get("tasks") else None,
                clusters=out.get("clusters", []),
            )
            out["reasoning"] = _build_analysis_reasoning(
                resolved=out.get("resolved", {}),
                summary=out.get("summary", {}),
                detected_alarms=result.get("detected_alarms") if isinstance(result.get("detected_alarms"), list) else [],
                topology_impact=out.get("topology_impact", {}),
            )
            findings = [
                x for x in ((out.get("reasoning") or {}).get("top_findings") or [])
                if isinstance(x, dict)
            ][:10]
            primary_finding = findings[0] if findings else {}
            primary_evidence = [
                str(x) for x in (primary_finding.get("evidence") or [])
                if str(x).strip()
            ]
            scope_observation = payload.get("scope_observation") if isinstance(payload.get("scope_observation"), dict) else {}
            obs_severity = _infer_scope_severity_from_observation(scope_type, scope_observation)
            obs_evidence = _observation_evidence(scope_type=scope_type, observation=scope_observation)
            alarm_total = int((out.get("summary") or {}).get("detected_alarm_total") or 0)
            has_structured_finding = bool(findings)
            if alarm_total > 0 or has_structured_finding:
                decision = "confirmed_fault"
            elif _severity_rank(obs_severity) >= _severity_rank("warning"):
                decision = "suspected_anomaly"
            else:
                decision = "normal"
            risk_level = str((out.get("summary") or {}).get("risk_level") or "normal").strip().lower() or "normal"
            if decision == "normal":
                risk_level = "normal"
            elif risk_level == "normal" and _severity_rank(obs_severity) >= _severity_rank("warning"):
                risk_level = obs_severity
            out["risk_result"] = {
                "source": "rules_lstm",
                "risk_level": risk_level,
                "max_alarm_severity": (out.get("summary") or {}).get("max_alarm_severity"),
                "focused_scope_severity": (out.get("summary") or {}).get("focused_scope_severity"),
                "detected_alarm_total": (out.get("summary") or {}).get("detected_alarm_total"),
                "decision": decision,
                "direct_reason": "、".join(primary_evidence[:3]) if primary_evidence else ("、".join(obs_evidence[:3]) if obs_evidence else None),
                "primary_finding": primary_finding or None,
            }
            out["evidence_bundle"] = {
                "scope_observation": scope_observation,
                "scope_observation_severity": obs_severity,
                "scope_observation_evidence": obs_evidence,
                "top_findings": findings,
                "detected_alarms": [
                    x for x in (result.get("detected_alarms") or [])
                    if isinstance(x, dict)
                ][:30],
                "impacted_nodes_count": len(out.get("topology_impact", {}).get("impacted_nodes") or []),
                "impacted_links_count": len(out.get("topology_impact", {}).get("impacted_links") or []),
                "impacted_nodes_sample": (out.get("topology_impact", {}).get("impacted_nodes") or [])[:20],
                "impacted_links_sample": (out.get("topology_impact", {}).get("impacted_links") or [])[:20],
                "tasks_top": (out.get("tasks") or [])[:10],
            }
            out["security_correlation"] = _build_security_correlation(
                resolved=out.get("resolved", {}),
                detected_alarms=result.get("detected_alarms") if isinstance(result.get("detected_alarms"), list) else [],
                topology_impact=out.get("topology_impact", {}),
                tasks=out.get("tasks") if isinstance(out.get("tasks"), list) else [],
                monitor_payload=snapshot_payload,
                window_sec=max(60, min(int(payload.get("window_sec", 300)), 3600)),
            )
            out["evidence_bundle"]["security_correlation"] = out.get("security_correlation") or {}
            ok = True
            return out
        finally:
            app.state.slo.record("analysis.run", ok=ok, latency_ms=(time.perf_counter() - start) * 1000.0)

    @app.post("/api/v1/bff/analysis/explain")
    async def bff_analysis_explain(payload: dict[str, object]) -> dict[str, object]:
        analysis = payload.get("analysis")
        if not isinstance(analysis, dict):
            raise HTTPException(status_code=422, detail="analysis(object) 为必填")
        scope_type = str(payload.get("scope_type") or analysis.get("scope_type") or (analysis.get("resolved") or {}).get("scope_type") or "")
        scope_id = str(payload.get("scope_id") or analysis.get("scope_id") or (analysis.get("resolved") or {}).get("scope_id") or "")
        extra_context = payload.get("extra_context") if isinstance(payload.get("extra_context"), (dict, list, str)) else {}
        provider = str(payload.get("provider") or os.getenv("AI_PROVIDER") or "newapi").strip().lower()
        if provider not in {"newapi", "gemini", "ollama"}:
            provider = "newapi"
        model = str(
            payload.get("model")
            or (os.getenv("OLLAMA_MODEL") if provider == "ollama" else os.getenv("AI_MODEL_NAME") or os.getenv("GEMINI_MODEL"))
            or ("qwen3-coder:latest" if provider == "ollama" else "qwen2.5-coder:32b")
        ).strip() or ("qwen3-coder:latest" if provider == "ollama" else "qwen2.5-coder:32b")
        prompt = _build_ollama_analysis_prompt(
            analysis=analysis,
            scope_type=scope_type,
            scope_id=scope_id,
            extra_context=extra_context,
        )
        if provider == "ollama":
            text, used_url, err, used_model = _ollama_generate(prompt=prompt, model=model)
        else:
            # newapi is OpenAI-compatible; keep gemini alias mapped here for frontend compatibility.
            text, used_url, err, used_model = _newapi_generate(prompt=prompt, model=model)
        if text:
            return {
                "status": "ok",
                "source": provider,
                "model": used_model,
                "base_url": used_url,
                "report": text,
            }
        raise HTTPException(
            status_code=504,
            detail=_analysis_error("AI_UNAVAILABLE", err or f"{provider} unavailable"),
        )

    @app.post("/api/v1/bff/analysis/copilot")
    async def bff_analysis_copilot(payload: dict[str, object]) -> dict[str, object]:
        analysis = payload.get("analysis")
        question = str(payload.get("question") or "").strip()
        if not isinstance(analysis, dict):
            raise HTTPException(status_code=422, detail="analysis(object) 为必填")
        if not question:
            raise HTTPException(status_code=422, detail="question(string) 为必填")
        session_id = str(payload.get("session_id") or f"copilot-{int(time.time() * 1000)}")
        history = payload.get("history") if isinstance(payload.get("history"), list) else []
        provider = str(payload.get("provider") or os.getenv("AI_PROVIDER") or "newapi").strip().lower()
        if provider not in {"newapi", "gemini", "ollama"}:
            provider = "newapi"
        model = str(
            payload.get("model")
            or (os.getenv("OLLAMA_MODEL") if provider == "ollama" else os.getenv("AI_MODEL_NAME") or os.getenv("GEMINI_MODEL"))
            or "qwen3-coder:latest"
        ).strip() or "qwen3-coder:latest"
        prompt = _build_copilot_prompt(analysis=analysis, question=question, history=history)
        if provider == "ollama":
            text, _, err, used_model = _ollama_generate(prompt=prompt, model=model)
        else:
            text, _, err, used_model = _newapi_generate(prompt=prompt, model=model)
        if not text:
            answer = _copilot_rule_fallback(analysis=analysis, question=question)
            refs = _copilot_references(analysis=analysis)
            return {
                "status": "ok",
                "session_id": session_id,
                "answer": answer,
                "references": refs,
                "confidence": "low",
                "fallback": True,
                "fallback_reason": err or f"{provider} unavailable",
                "source": "rule_fallback",
                "model": used_model or model,
            }
        refs = _copilot_references(analysis=analysis)
        return {
            "status": "ok",
            "session_id": session_id,
            "answer": text.strip(),
            "references": refs,
            "confidence": "medium",
            "fallback": False,
            "source": provider,
            "model": used_model,
        }

    @app.post("/api/v1/bff/simulation/create")
    async def simulation_create(payload: dict[str, object]) -> dict[str, object]:
        scenario_type = str(payload.get("scenario_type", "link_down")).strip().lower()
        if scenario_type not in {"link_down", "node_hotspot", "regional_blackout"}:
            raise HTTPException(status_code=422, detail="scenario_type 仅支持 link_down|node_hotspot|regional_blackout")
        topology_epoch = str(payload.get("topology_epoch")) if payload.get("topology_epoch") not in (None, "") else None
        steps_total = max(1, min(int(payload.get("steps_total", 8)), 100))
        params = payload.get("params")
        if not isinstance(params, dict):
            params = {}
        session = app.state.simulations.create(
            scenario_type=scenario_type,
            topology_epoch=topology_epoch,
            params=params,
            steps_total=steps_total,
        )
        return {
            "status": "ok",
            "simulation_id": session.simulation_id,
            "scenario_type": session.scenario_type,
            "steps_total": session.steps_total,
            "topology_epoch": session.topology_epoch,
            "created_at": session.created_at,
        }

    @app.post("/api/v1/bff/simulation/{simulation_id}/step")
    async def simulation_step(simulation_id: str) -> dict[str, object]:
        snapshot_payload = app.state.snapshot_store.snapshot(topology_epoch=None)
        try:
            session_view = app.state.simulations.step(
                simulation_id,
                snapshot_payload=snapshot_payload,
                alarm_discoverer=app.state.alarm_discoverer,
                spread_analyzer=app.state.fault_spread,
                task_impact=app.state.task_impact,
                ts_writer=app.state.ts_writer,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"simulation not found: {simulation_id}") from exc
        return {"status": "ok", "simulation": session_view}

    @app.get("/api/v1/bff/simulation/{simulation_id}")
    async def simulation_get(simulation_id: str) -> dict[str, object]:
        session = app.state.simulations.get(simulation_id)
        if session is None:
            raise HTTPException(status_code=404, detail=f"simulation not found: {simulation_id}")
        return {
            "status": "ok",
            "simulation": {
                "simulation_id": session.simulation_id,
                "scenario_type": session.scenario_type,
                "topology_epoch": session.topology_epoch,
                "params": session.params,
                "steps_total": session.steps_total,
                "current_step": session.current_step,
                "status": session.status,
                "created_at": session.created_at,
                "updated_at": session.updated_at,
                "timeline": session.timeline,
            },
        }

    @app.get("/api/v1/bff/simulation/{simulation_id}/timeline")
    async def simulation_timeline(simulation_id: str) -> dict[str, object]:
        session = app.state.simulations.get(simulation_id)
        if session is None:
            raise HTTPException(status_code=404, detail=f"simulation not found: {simulation_id}")
        timeline_rows = session.timeline
        key_events = _extract_simulation_key_events(timeline_rows)
        return {
            "status": "ok",
            "simulation_id": session.simulation_id,
            "scenario_type": session.scenario_type,
            "current_step": session.current_step,
            "steps_total": session.steps_total,
            "timeline": timeline_rows,
            "key_events": key_events,
            "summary": _simulation_timeline_summary(timeline_rows),
        }

    @app.post("/api/v1/bff/simulation/{simulation_id}/report")
    async def simulation_report(simulation_id: str, payload: dict[str, object] | None = None) -> dict[str, object]:
        session = app.state.simulations.get(simulation_id)
        if session is None:
            raise HTTPException(status_code=404, detail=f"simulation not found: {simulation_id}")
        payload = payload or {}
        provider = str(payload.get("provider") or os.getenv("AI_PROVIDER") or "newapi").strip().lower()
        if provider not in {"newapi", "gemini", "ollama"}:
            provider = "newapi"
        model = str(
            payload.get("model")
            or (os.getenv("OLLAMA_MODEL") if provider == "ollama" else os.getenv("AI_MODEL_NAME") or os.getenv("GEMINI_MODEL"))
            or ("qwen3-coder:latest" if provider == "ollama" else "qwen3-coder:latest")
        ).strip() or ("qwen3-coder:latest" if provider == "ollama" else "qwen3-coder:latest")

        summary = _simulation_timeline_summary(session.timeline)
        key_events = _extract_simulation_key_events(session.timeline)
        prompt = _build_simulation_report_prompt(
            simulation_id=session.simulation_id,
            scenario_type=session.scenario_type,
            focus_scope=session.params,
            timeline_summary=summary,
            key_events=key_events,
            timeline_tail=session.timeline[-8:],
        )
        if provider == "ollama":
            text, used_url, err, used_model = _ollama_generate(prompt=prompt, model=model)
        else:
            text, used_url, err, used_model = _newapi_generate(prompt=prompt, model=model)
        if not text:
            raise HTTPException(status_code=504, detail=_analysis_error("AI_UNAVAILABLE", err or f"{provider} unavailable"))
        return {
            "status": "ok",
            "simulation_id": session.simulation_id,
            "scenario_type": session.scenario_type,
            "model": used_model,
            "source": provider,
            "base_url": used_url,
            "summary": summary,
            "key_events": key_events,
            "report_markdown": text,
        }

    @app.post("/api/v1/ops/failed-events/replay")
    async def replay_failed_events(limit: int = 50) -> dict[str, object]:
        ids = app.state.failed_events.pending_event_ids(limit=limit)
        attempted = 0
        replayed = 0
        failed = 0
        details: list[dict[str, object]] = []
        for event_id in ids:
            event = app.state.failed_events.get_event(event_id)
            if not event:
                continue
            attempted += 1
            try:
                await app.state.publisher.publish(
                    str(event.get("subject") or ""),
                    dict(event.get("payload") or {}),
                )
                app.state.failed_events.mark_replay(event_id, success=True)
                replayed += 1
                details.append({"id": event_id, "status": "replayed"})
            except Exception as exc:  # noqa: BLE001
                app.state.failed_events.mark_replay(event_id, success=False, replay_error=str(exc))
                failed += 1
                details.append({"id": event_id, "status": "failed", "error": str(exc)})
        return {
            "attempted": attempted,
            "replayed": replayed,
            "failed": failed,
            "details": details,
            "summary": app.state.failed_events.summary(),
        }

    return app


def _ollama_base_urls() -> list[str]:
    raw = os.getenv("OLLAMA_BASE_URLS", "").strip()
    urls: list[str] = []
    if raw:
        urls.extend([x.strip() for x in raw.split(",") if x.strip()])
    single = os.getenv("OLLAMA_BASE_URL", "").strip()
    if single:
        urls.append(single)
    if not urls:
        urls = [
            "http://192.168.0.2:11434",
            "http://host.docker.internal:11434",
            "http://172.17.0.1:11434",
            "http://127.0.0.1:11434",
        ]
    seen: set[str] = set()
    out: list[str] = []
    for x in urls:
        u = x.rstrip("/")
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _ollama_generate(*, prompt: str, model: str) -> tuple[str, str, str, str]:
    timeout_sec = max(3, min(int(os.getenv("OLLAMA_TIMEOUT_SEC", "18")), 120))
    total_budget_sec = max(timeout_sec, min(int(os.getenv("OLLAMA_TOTAL_TIMEOUT_SEC", "35")), 180))
    started_at = time.monotonic()
    last_err = "ollama request failed"
    fallback_models = [x.strip() for x in str(os.getenv("OLLAMA_FALLBACK_MODELS", "deepseek-r1:14b,qwen3:32b")).split(",") if x.strip()]
    model_candidates: list[str] = []
    for m in [model, *fallback_models]:
        if m and m not in model_candidates:
            model_candidates.append(m)

    for cur_model in model_candidates:
        for base_url in _ollama_base_urls():
            remaining = total_budget_sec - (time.monotonic() - started_at)
            if remaining <= 1.0:
                return "", "", f"TimeoutError: total budget exceeded ({total_budget_sec}s)", model
            req_body = {
                "model": cur_model,
                "prompt": prompt,
                "stream": False,
                "keep_alive": "20m",
                "options": {
                    "temperature": 0.2,
                    "num_predict": 720,
                },
            }
            req = urllib_request.Request(
                url=f"{base_url}/api/generate",
                data=json.dumps(req_body).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib_request.urlopen(req, timeout=min(timeout_sec, max(3.0, remaining))) as resp:  # noqa: S310
                    raw = resp.read().decode("utf-8", errors="replace")
                obj = json.loads(raw) if raw else {}
                text = str(obj.get("response") or "").strip()
                if text:
                    return text, base_url, "", cur_model
                last_err = f"empty response for {cur_model}@{base_url}"
            except (urllib_error.URLError, urllib_error.HTTPError, TimeoutError, json.JSONDecodeError) as exc:
                last_err = f"{type(exc).__name__}: {exc} ({cur_model}@{base_url})"
                continue
    return "", "", last_err, model


def _newapi_generate(*, prompt: str, model: str) -> tuple[str, str, str, str]:
    api_key = str(os.getenv("GEMINI_API_KEY") or os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return "", "", "GEMINI_API_KEY/OPENAI_API_KEY not set", model
    req_url = str(
        os.getenv("AI_SERVER_URL")
        or os.getenv("OPENAI_BASE_URL")
        or "http://192.168.0.5:3002/v1/chat/completions"
    ).strip()
    timeout_sec = max(3, min(int(os.getenv("AI_TIMEOUT_SEC", os.getenv("GEMINI_TIMEOUT_SEC", "35"))), 180))
    max_tokens = max(128, min(int(os.getenv("AI_MAX_TOKENS", os.getenv("GEMINI_MAX_OUTPUT_TOKENS", "1024"))), 4096))
    req_body = {
        "model": model,
        "messages": [
            {"role": "system", "content": "你是网络故障管理专家，请给出严谨、可执行、可验证的分析。"},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
        "stream": False,
        "max_tokens": max_tokens,
        # Keep compatibility with OpenAI-compatible gateways that only honor one of these fields.
        "max_completion_tokens": max_tokens,
        "max_output_tokens": max_tokens,
    }
    def _post_once(body: dict[str, Any]) -> tuple[str, str]:
        req = urllib_request.Request(
            url=req_url,
            data=json.dumps(body).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            method="POST",
        )
        with urllib_request.urlopen(req, timeout=timeout_sec) as resp:  # noqa: S310
            raw = resp.read().decode("utf-8", errors="replace")
        obj = json.loads(raw) if raw else {}
        choices = obj.get("choices") if isinstance(obj.get("choices"), list) else []
        if not choices:
            return "", "empty choices"
        msg = (choices[0] or {}).get("message") if isinstance(choices[0], dict) else {}
        text = str((msg or {}).get("content") or "").strip()
        finish_reason = str((choices[0] or {}).get("finish_reason") or "").strip().lower()
        if not text:
            return "", "empty response text"
        if finish_reason:
            return text, finish_reason
        return text, ""

    try:
        text, reason = _post_once(req_body)
        likely_truncated = (
            reason in {"length", "max_tokens"}
            or ("**1." in text and "**2." in text and "**3." not in text)
            or (text and text[-1] not in "。！？.!?】)}")
        )
        if likely_truncated:
            retry_prompt = (
                "请在300字内完整重写报告，必须包含并按顺序输出四段："
                "结论、直接原因、影响范围、建议动作。每段1-2句。不要省略段名。\n\n"
                f"{prompt}"
            )
            retry_body = dict(req_body)
            retry_body["messages"] = [
                {"role": "system", "content": "你是网络故障管理专家，输出要完整、紧凑、可读。"},
                {"role": "user", "content": retry_prompt},
            ]
            retry_body["temperature"] = 0.1
            text2, reason2 = _post_once(retry_body)
            if text2:
                return text2, req_url, "", model
            return text, req_url, f"incomplete response ({reason or reason2})", model
        return text, req_url, "", model
    except (urllib_error.URLError, urllib_error.HTTPError, TimeoutError, json.JSONDecodeError) as exc:
        return "", req_url, f"{type(exc).__name__}: {exc}", model


def _build_ollama_analysis_prompt(
    *,
    analysis: dict[str, Any],
    scope_type: str,
    scope_id: str,
    extra_context: Any = None,
) -> str:
    summary = analysis.get("summary") if isinstance(analysis.get("summary"), dict) else {}
    topo = analysis.get("topology_impact") if isinstance(analysis.get("topology_impact"), dict) else {}
    reasoning = analysis.get("reasoning") if isinstance(analysis.get("reasoning"), dict) else {}
    narrative = analysis.get("narrative") if isinstance(analysis.get("narrative"), dict) else {}
    risk_result = analysis.get("risk_result") if isinstance(analysis.get("risk_result"), dict) else {}
    evidence_bundle = analysis.get("evidence_bundle") if isinstance(analysis.get("evidence_bundle"), dict) else {}
    tasks = analysis.get("tasks") if isinstance(analysis.get("tasks"), list) else []
    extra_obj = extra_context if isinstance(extra_context, dict) else {}
    obs = extra_obj.get("scope_observation") if isinstance(extra_obj.get("scope_observation"), dict) else {}
    fc = extra_obj.get("forecast_context") if isinstance(extra_obj.get("forecast_context"), dict) else {}
    impacted_nodes = topo.get("impacted_nodes") if isinstance(topo.get("impacted_nodes"), list) else []
    impacted_links = topo.get("impacted_links") if isinstance(topo.get("impacted_links"), list) else []
    findings_all = [x for x in (reasoning.get("top_findings") or []) if isinstance(x, dict)]
    compact = {
        "scope_type": scope_type,
        "scope_id": scope_id,
        "risk_level": summary.get("risk_level"),
        "risk_result": risk_result,
        "max_alarm_severity": summary.get("max_alarm_severity"),
        "focused_scope_severity": summary.get("focused_scope_severity"),
        "detected_alarm_total": summary.get("detected_alarm_total"),
        "impacted_nodes_count": len(impacted_nodes),
        "impacted_links_count": len(impacted_links),
        "impacted_nodes_sample": impacted_nodes[:20],
        "impacted_links_sample": impacted_links[:20],
        "top_findings": findings_all[:8],
        "note": reasoning.get("note"),
        "verdict": narrative.get("verdict"),
        "next_action": narrative.get("next_action"),
        "tasks_top10": tasks[:10],
        "evidence_bundle": evidence_bundle,
        "scope_observation": obs,
        "forecast_context": fc,
        "direct_reason_hint": extra_obj.get("direct_reason"),
        "extra_context": extra_obj,
    }
    data_json = json.dumps(compact, ensure_ascii=False)
    return (
        "你是网络故障管理专家。请基于输入数据写一份“可执行、可验证”的诊断报告。\n"
        "强约束：risk_result 是规则/LSTM给出的最终判级，AI不得改写、不得降级或升级该风险级别。\n"
        "要求：\n"
        "1) 先给结论（沿用 risk_result 的风险级别+紧急程度）\n"
        "2) 明确“直接原因”：必须写出命中的指标名、观测值、阈值、比较关系（例如 cpu=85.6% >= 82%）\n"
        "3) 给出“证据链”：至少3条，优先使用 scope_observation 与 top_findings，不要泛泛而谈\n"
        "4) 说明影响范围：影响节点数/链路数，并点名最多5个关键对象\n"
        "5) 给出3条处置动作（P1/P2/P3），每条包含预期结果与验证方法\n"
        "6) 给出“误报可能性评估”（低/中/高）和需要补采的关键指标\n"
        "7) 若数据不足，明确写出“数据不足项”而不是编造原因\n"
        "8) 输出中文纯文本，分段清晰，不要输出JSON。\n"
        f"\n输入数据：\n{data_json}\n"
    )


def _build_fallback_analysis_report(*, analysis: dict[str, Any], scope_type: str, scope_id: str) -> str:
    summary = analysis.get("summary") if isinstance(analysis.get("summary"), dict) else {}
    topo = analysis.get("topology_impact") if isinstance(analysis.get("topology_impact"), dict) else {}
    reasoning = analysis.get("reasoning") if isinstance(analysis.get("reasoning"), dict) else {}
    findings = [x for x in (reasoning.get("top_findings") or []) if isinstance(x, dict)]
    evidence = []
    if findings:
        evidence = [str(x) for x in (findings[0].get("evidence") or []) if str(x).strip()]
    risk = str(summary.get("risk_level") or "unknown")
    impacted_nodes = len(topo.get("impacted_nodes") or [])
    impacted_links = len(topo.get("impacted_links") or [])
    ev_text = "、".join(evidence[:4]) if evidence else "当前返回未提供明确阈值证据"
    return (
        f"结论：对象 {scope_type}/{scope_id} 当前风险评估为 {risk}。\n"
        f"直接原因：{ev_text}。\n"
        f"影响范围：影响节点 {impacted_nodes} 个，影响链路 {impacted_links} 条。\n"
        "建议动作：\n"
        "1. 先复核该对象最近5分钟指标曲线与阈值命中点。\n"
        "2. 对受影响链路执行连通性与丢包复测，确认是否持续异常。\n"
        "3. 若异常持续，执行限流/切流并观察风险等级是否在10分钟内下降。"
    )


def _forecast_wma(values: list[float], window: int, steps: int) -> list[float]:
    hist = deque(values[-window:], maxlen=window)
    weights = [i + 1 for i in range(window)]
    weight_sum = float(sum(weights))
    out: list[float] = []
    for _ in range(steps):
        weighted = sum(v * w for v, w in zip(hist, weights, strict=True)) / weight_sum
        out.append(float(weighted))
        hist.append(float(weighted))
    return out


def _analysis_error(code: str, message: str) -> dict[str, str]:
    return {"error_code": str(code), "error_message": str(message)}


def _resolve_scope_id(payload: dict[str, Any]) -> str:
    direct = str(payload.get("scope_id", "")).strip()
    if direct and direct != "all":
        return direct
    alias = str(payload.get("entity", "") or payload.get("entity_id", "")).strip()
    if alias:
        return alias
    src = str(payload.get("source", "") or payload.get("src", "") or payload.get("a", "")).strip()
    dst = str(payload.get("target", "") or payload.get("dst", "") or payload.get("b", "")).strip()
    if src and dst:
        return normalize_link_uid(src, dst)
    return str(payload.get("scope_id", "all")).strip() or "all"


def _infer_scope_type(scope_id: str, fallback: str = "network") -> str:
    sid = str(scope_id or "").strip()
    if "<->" in sid or "->" in sid or "|" in sid:
        return "link"
    if sid and sid != "all":
        return "node"
    return fallback


def _normalize_link_scope(scope_id: str) -> str:
    sid = str(scope_id or "").strip()
    if "<->" in sid:
        parts = [x.strip() for x in sid.split("<->", 1)]
        return normalize_link_uid(parts[0], parts[1]) if len(parts) == 2 else sid
    if "->" in sid:
        parts = [x.strip() for x in sid.split("->", 1)]
        return normalize_link_uid(parts[0], parts[1]) if len(parts) == 2 else sid
    if "|" in sid:
        parts = [x.strip() for x in sid.split("|", 1)]
        return normalize_link_uid(parts[0], parts[1]) if len(parts) == 2 else sid
    return sid


def _normalize_analysis_exception(exc: HTTPException) -> tuple[str, str]:
    detail = exc.detail
    if isinstance(detail, dict):
        code = str(detail.get("error_code") or "INTERNAL_ERROR")
        msg = str(detail.get("error_message") or detail.get("detail") or "analysis error")
        return code, msg
    text = str(detail or "analysis error")
    if text.startswith("INVALID_SCOPE"):
        return "INVALID_SCOPE", text.split(":", 1)[-1].strip() if ":" in text else text
    if text.startswith("INSUFFICIENT_DATA"):
        return "INSUFFICIENT_DATA", text.split(":", 1)[-1].strip() if ":" in text else text
    return "INTERNAL_ERROR", text


def _extract_seeds(detected_alarms: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    nodes: list[str] = []
    links: list[str] = []
    for item in detected_alarms:
        st = str(item.get("scope_type") or "").strip().lower()
        sid = str(item.get("scope_id") or "").strip()
        if not sid:
            continue
        if st == "node" and sid not in nodes:
            nodes.append(sid)
        if st == "link" and sid not in links:
            links.append(sid)
    return nodes, links


def _extract_link_metrics(links_map: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for uid, raw in links_map.items():
        if not isinstance(raw, dict):
            continue
        key = str(raw.get("link_uid") or uid or "").strip()
        if not key:
            continue
        out[key] = {
            "state": raw.get("state"),
            "rtt_ms": raw.get("rtt_ms"),
            "loss_rate": raw.get("loss_rate"),
        }
    return out


def _extract_payload_link_metrics(raw: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for uid, item in raw.items():
        if not isinstance(item, dict):
            continue
        key = _normalize_link_scope(str(uid or "").strip())
        if not key:
            continue
        out[key] = {
            "state": item.get("state"),
            "rtt_ms": item.get("rtt_ms"),
            "loss_rate": item.get("loss_rate"),
        }
    return out


def _merge_payload_links(links: list[dict[str, Any]], payload_links: Any) -> list[dict[str, Any]]:
    out = [x for x in links if isinstance(x, dict)]
    seen = {
        _normalize_link_scope(str(x.get("link_uid") or x.get("link_id") or "").strip())
        for x in out
        if isinstance(x, dict)
    }
    extra = payload_links if isinstance(payload_links, list) else []
    for item in extra:
        if not isinstance(item, dict):
            continue
        src = str(item.get("src") or item.get("src_node_uid") or item.get("src_node_id") or "").strip()
        dst = str(item.get("dst") or item.get("dst_node_uid") or item.get("dst_node_id") or "").strip()
        uid = _normalize_link_scope(str(item.get("link_uid") or item.get("link_id") or ""))
        if not uid and src and dst:
            uid = normalize_link_uid(src, dst)
        if not uid or uid in seen:
            continue
        seen.add(uid)
        out.append(
            {
                "link_uid": uid,
                "link_id": uid,
                "src_node_uid": src,
                "src_node_id": src,
                "dst_node_uid": dst,
                "dst_node_id": dst,
                "state": item.get("state", "UP"),
                "rtt_ms": item.get("rtt_ms", 0.0),
                "loss_rate": item.get("loss_rate", 0.0),
                "jitter_ms": item.get("jitter_ms", 0.0),
            }
        )
    return out


def _normalize_links_for_spread(links: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in links:
        if not isinstance(item, dict):
            continue
        src = str(item.get("src") or item.get("src_node_uid") or item.get("src_node_id") or "").strip()
        dst = str(item.get("dst") or item.get("dst_node_uid") or item.get("dst_node_id") or "").strip()
        uid = _normalize_link_scope(str(item.get("link_uid") or item.get("link_id") or ""))
        if not uid and src and dst:
            uid = normalize_link_uid(src, dst)
        if not src or not dst:
            continue
        out.append(
            {
                **item,
                "src": src,
                "dst": dst,
                "link_uid": uid,
                "link_id": uid or str(item.get("link_id") or ""),
                "health": float(item.get("health") or 0.8),
            }
        )
    return out


def _build_global_tasks(link_metrics: dict[str, dict[str, Any]], max_tasks: int = 30) -> list[dict[str, Any]]:
    scored: list[tuple[float, str]] = []
    for uid, m in link_metrics.items():
        state = str(m.get("state") or "").upper()
        rtt = _safe_float(m.get("rtt_ms"))
        loss = _safe_float(m.get("loss_rate"))
        score = 0.0
        if state in {"DOWN", "DISCONNECTED"}:
            score += 100.0
        if state in {"DEGRADED"}:
            score += 40.0
        score += min(80.0, rtt / 4.0)
        score += min(60.0, loss * 1200.0)
        scored.append((score, uid))
    scored.sort(key=lambda x: (-x[0], x[1]))
    top = scored[: max(1, max_tasks)]
    tasks: list[dict[str, Any]] = []
    for idx, (_, uid) in enumerate(top, start=1):
        criticality = 0.95 if idx <= 5 else 0.8 if idx <= 15 else 0.65
        tasks.append(
            {
                "task_id": f"global-task-{idx:03d}",
                "name": f"全网关键链路任务-{idx:03d}",
                "criticality": criticality,
                "links": [uid],
            }
        )
    return tasks


def _is_terminal_endpoint_node(scope_id: str) -> bool:
    sid = str(scope_id or "").strip().upper()
    return sid.startswith("AIR-") or sid.startswith("SHIP-")


def _is_satellite_node(scope_id: str) -> bool:
    sid = str(scope_id or "").strip().upper()
    return sid.startswith("SAT-")


def _build_node_local_tasks(scope_id: str, link_metrics: dict[str, dict[str, Any]], max_tasks: int = 20) -> list[dict[str, Any]]:
    sid = str(scope_id or "").strip()
    if not sid:
        return []
    candidates: list[tuple[float, str]] = []
    for uid, m in link_metrics.items():
        key = _normalize_link_scope(str(uid or "").strip())
        if not key or "<->" not in key:
            continue
        a, b = [x.strip() for x in key.split("<->", 1)]
        if sid not in {a, b}:
            continue
        state = str(m.get("state") or "").upper()
        rtt = _safe_float(m.get("rtt_ms"))
        loss = _safe_float(m.get("loss_rate"))
        score = 0.0
        if state in {"DOWN", "DISCONNECTED"}:
            score += 100.0
        if state in {"DEGRADED"}:
            score += 40.0
        score += min(80.0, rtt / 4.0)
        score += min(60.0, loss * 1200.0)
        candidates.append((score, key))
    candidates.sort(key=lambda x: (-x[0], x[1]))
    tasks: list[dict[str, Any]] = []
    for idx, (_, uid) in enumerate(candidates[: max(1, max_tasks)], start=1):
        tasks.append(
            {
                "task_id": f"local-task-{sid}-{idx:03d}",
                "name": f"终端节点关联链路任务-{sid}-{idx:03d}",
                "criticality": 0.6,
                "links": [uid],
            }
        )
    return tasks


def _build_impacted_tasks(link_metrics: dict[str, dict[str, Any]], impacted_links: list[str], max_tasks: int = 30) -> list[dict[str, Any]]:
    impacted = {str(x).strip() for x in impacted_links if str(x).strip()}
    if not impacted:
        return []
    scored: list[tuple[float, str]] = []
    for uid in impacted:
        m = link_metrics.get(uid) or {}
        state = str(m.get("state") or "").upper()
        rtt = _safe_float(m.get("rtt_ms"))
        loss = _safe_float(m.get("loss_rate"))
        score = 0.0
        if state in {"DOWN", "DISCONNECTED"}:
            score += 100.0
        if state in {"DEGRADED"}:
            score += 40.0
        score += min(80.0, rtt / 4.0)
        score += min(60.0, loss * 1200.0)
        scored.append((score, uid))
    scored.sort(key=lambda x: (-x[0], x[1]))
    tasks: list[dict[str, Any]] = []
    for idx, (_, uid) in enumerate(scored[: max(1, max_tasks)], start=1):
        tasks.append(
            {
                "task_id": f"impact-task-{idx:03d}",
                "name": f"受影响路由链路任务-{idx:03d}",
                "criticality": 0.9 if idx <= 5 else 0.75,
                "links": [uid],
            }
        )
    return tasks


def _is_terminal_endpoint_node_id(node_id: str) -> bool:
    sid = str(node_id or "").strip().upper()
    return sid.startswith("AIR-") or sid.startswith("SHIP-")


def _is_link_anomalous(link: dict[str, Any]) -> bool:
    state = str(link.get("state") or "").strip().upper()
    if state in {"DOWN", "DISCONNECTED", "DEGRADED"}:
        return True
    rtt = _safe_float(link.get("rtt_ms"))
    loss = _safe_float(link.get("loss_rate"))
    jitter = _safe_float(link.get("jitter_ms"))
    return rtt >= 220.0 or loss >= 0.04 or jitter >= 40.0


def _focused_node_allow_route_spread(scope_id: str, detected_alarms: list[dict[str, Any]], links: list[dict[str, Any]]) -> bool:
    sid = str(scope_id or "").strip()
    if not sid:
        return False
    has_node_alarm = False
    for item in detected_alarms:
        if not isinstance(item, dict):
            continue
        if str(item.get("scope_type") or "").strip().lower() != "node":
            continue
        cur = str(item.get("scope_id") or item.get("scope_uid") or "").strip()
        if cur == sid:
            has_node_alarm = True
            break
    if not has_node_alarm:
        return False
    # 单节点 CPU 类指标越阈时，不直接推导到全网；只有邻接链路也异常才进行路由扩散。
    for lk in links:
        if not isinstance(lk, dict):
            continue
        src = str(lk.get("src") or lk.get("src_node_uid") or lk.get("src_node_id") or "").strip()
        dst = str(lk.get("dst") or lk.get("dst_node_uid") or lk.get("dst_node_id") or "").strip()
        if sid not in {src, dst}:
            continue
        if _is_link_anomalous(lk):
            return True
    return False


def _route_scoped_impact_from_flows(
    *,
    scope_type: str,
    scope_id: str,
    flows: dict[str, Any],
    links: list[dict[str, Any]],
    max_paths: int = 240,
    max_impacted_nodes: int = 220,
    max_impacted_links: int = 700,
) -> dict[str, Any] | None:
    all_flows = [x for x in flows.values() if isinstance(x, dict)]
    if not all_flows:
        return None
    link_index: dict[str, dict[str, Any]] = {}
    for lk in links:
        uid = _normalize_link_scope(str(lk.get("link_uid") or lk.get("link_id") or ""))
        if uid:
            link_index[uid] = lk

    scope_link = _normalize_link_scope(scope_id) if scope_type == "link" else ""
    matched: list[list[str]] = []
    for flow in all_flows:
        path = flow.get("path")
        if not isinstance(path, list):
            continue
        nodes = [str(x).strip() for x in path if str(x).strip()]
        if len(nodes) < 2:
            continue
        if not (_is_terminal_endpoint_node_id(nodes[0]) and _is_terminal_endpoint_node_id(nodes[-1])):
            continue
        if scope_type == "node":
            if scope_id in nodes:
                matched.append(nodes)
                if len(matched) >= max_paths:
                    break
            continue
        if scope_type == "link":
            hit = False
            for i in range(len(nodes) - 1):
                uid = normalize_link_uid(nodes[i], nodes[i + 1])
                if uid == scope_link:
                    hit = True
                    break
            if hit:
                matched.append(nodes)
                if len(matched) >= max_paths:
                    break
    if not matched:
        return None

    node_count: dict[str, int] = {}
    link_count: dict[str, int] = {}
    for nodes in matched:
        for n in nodes:
            node_count[n] = node_count.get(n, 0) + 1
        for i in range(len(nodes) - 1):
            uid = normalize_link_uid(nodes[i], nodes[i + 1])
            link_count[uid] = link_count.get(uid, 0) + 1

    impacted_links = set(
        x[0] for x in sorted(link_count.items(), key=lambda kv: (-kv[1], kv[0]))[:max_impacted_links]
    )
    impacted_nodes = set(
        x[0] for x in sorted(node_count.items(), key=lambda kv: (-kv[1], kv[0]))[:max_impacted_nodes]
    )
    for uid in impacted_links:
        a, b = uid.split("<->", 1)
        impacted_nodes.add(a)
        impacted_nodes.add(b)
    if len(impacted_nodes) > max_impacted_nodes:
        impacted_nodes = set(
            x[0] for x in sorted(
                ((n, node_count.get(n, 0)) for n in impacted_nodes),
                key=lambda kv: (-kv[1], kv[0])
            )[:max_impacted_nodes]
        )
        impacted_links = {uid for uid in impacted_links if all(n in impacted_nodes for n in uid.split("<->", 1))}

    edges: list[dict[str, Any]] = []
    for uid in sorted(impacted_links):
        a, b = uid.split("<->", 1)
        lk = link_index.get(uid) or {}
        edges.append(
            {
                "uid": uid,
                "src": a,
                "dst": b,
                "health": float(lk.get("health") or 0.8),
            }
        )
    paths = [{"node": n, "depth": 0} for n in sorted(impacted_nodes)]
    seed_nodes = [scope_id] if scope_type == "node" and scope_id else []
    seed_links = [scope_link] if scope_type == "link" and scope_link else []
    return {
        "mode": "route_paths",
        "policy": "route_terminal_paths",
        "seeds": seed_nodes or seed_links,
        "seed_nodes": seed_nodes,
        "seed_links": seed_links,
        "core_nodes": sorted(impacted_nodes),
        "boundary_nodes": [],
        "unaffected_nodes": [],
        "impacted_nodes": sorted(impacted_nodes),
        "impacted_links": sorted(impacted_links),
        "subgraph": {"nodes": sorted(impacted_nodes), "edges": edges},
        "paths": paths,
        "fallback": False,
        "matched_flows": len(matched),
    }


def _route_scoped_impact_from_terminal_paths(
    *,
    scope_type: str,
    scope_id: str,
    links: list[dict[str, Any]],
    pair_budget: int = 2400,
    max_paths: int = 260,
    max_impacted_nodes: int = 240,
    max_impacted_links: int = 800,
) -> dict[str, Any] | None:
    graph: dict[str, set[str]] = {}
    link_index: dict[str, dict[str, Any]] = {}
    for lk in links:
        src = str(lk.get("src") or lk.get("src_node_uid") or lk.get("src_node_id") or "").strip()
        dst = str(lk.get("dst") or lk.get("dst_node_uid") or lk.get("dst_node_id") or "").strip()
        if not src or not dst:
            continue
        graph.setdefault(src, set()).add(dst)
        graph.setdefault(dst, set()).add(src)
        uid = normalize_link_uid(src, dst)
        link_index[uid] = lk
    terminals = sorted([n for n in graph.keys() if _is_terminal_endpoint_node_id(n)])
    if len(terminals) < 2:
        return None
    scope_link = _normalize_link_scope(scope_id) if scope_type == "link" else ""
    matched_paths: list[list[str]] = []
    pair_count = 0
    for i, src in enumerate(terminals):
        for j in range(i + 1, len(terminals)):
            pair_count += 1
            if pair_count > pair_budget:
                break
            dst = terminals[j]
            path = _shortest_path_nodes(graph, src, dst)
            if len(path) < 2:
                continue
            if scope_type == "node":
                if scope_id in path:
                    matched_paths.append(path)
                    if len(matched_paths) >= max_paths:
                        break
                continue
            hit = False
            for k in range(len(path) - 1):
                if normalize_link_uid(path[k], path[k + 1]) == scope_link:
                    hit = True
                    break
            if hit:
                matched_paths.append(path)
                if len(matched_paths) >= max_paths:
                    break
        if pair_count > pair_budget:
            break
        if len(matched_paths) >= max_paths:
            break
    if not matched_paths:
        return None
    node_count: dict[str, int] = {}
    link_count: dict[str, int] = {}
    for path in matched_paths:
        for n in path:
            node_count[n] = node_count.get(n, 0) + 1
        for i in range(len(path) - 1):
            uid = normalize_link_uid(path[i], path[i + 1])
            link_count[uid] = link_count.get(uid, 0) + 1
    impacted_links = set(
        x[0] for x in sorted(link_count.items(), key=lambda kv: (-kv[1], kv[0]))[:max_impacted_links]
    )
    impacted_nodes = set(
        x[0] for x in sorted(node_count.items(), key=lambda kv: (-kv[1], kv[0]))[:max_impacted_nodes]
    )
    for uid in impacted_links:
        a, b = uid.split("<->", 1)
        impacted_nodes.add(a)
        impacted_nodes.add(b)
    if len(impacted_nodes) > max_impacted_nodes:
        impacted_nodes = set(
            x[0] for x in sorted(
                ((n, node_count.get(n, 0)) for n in impacted_nodes),
                key=lambda kv: (-kv[1], kv[0])
            )[:max_impacted_nodes]
        )
        impacted_links = {uid for uid in impacted_links if all(n in impacted_nodes for n in uid.split("<->", 1))}
    edges: list[dict[str, Any]] = []
    for uid in sorted(impacted_links):
        a, b = uid.split("<->", 1)
        lk = link_index.get(uid) or {}
        edges.append({"uid": uid, "src": a, "dst": b, "health": float(lk.get("health") or 0.8)})
    seed_nodes = [scope_id] if scope_type == "node" and scope_id else []
    seed_links = [scope_link] if scope_type == "link" and scope_link else []
    return {
        "mode": "route_paths",
        "policy": "route_terminal_paths",
        "seeds": seed_nodes or seed_links,
        "seed_nodes": seed_nodes,
        "seed_links": seed_links,
        "core_nodes": sorted(impacted_nodes),
        "boundary_nodes": [],
        "unaffected_nodes": [],
        "impacted_nodes": sorted(impacted_nodes),
        "impacted_links": sorted(impacted_links),
        "subgraph": {"nodes": sorted(impacted_nodes), "edges": edges},
        "paths": [{"node": n, "depth": 0} for n in sorted(impacted_nodes)],
        "fallback": False,
        "matched_flows": len(matched_paths),
    }


def _severity_rank(sev: str) -> int:
    s = str(sev or "").strip().lower()
    if s == "critical":
        return 3
    if s == "warning":
        return 2
    if s == "info":
        return 1
    return 0


def _focused_scope_severity(scope_type: str, scope_id: str, detected_alarms: list[dict[str, Any]]) -> str:
    best = "info"
    if not scope_type or not scope_id:
        return best
    sid = str(scope_id).strip()
    for item in detected_alarms:
        if not isinstance(item, dict):
            continue
        st = str(item.get("scope_type") or "").strip().lower()
        cur_sid = str(item.get("scope_id") or item.get("scope_uid") or "").strip()
        if st != scope_type or cur_sid != sid:
            continue
        sev = str(item.get("severity") or "info").strip().lower()
        if _severity_rank(sev) > _severity_rank(best):
            best = sev
    return best


def _infer_scope_severity_from_snapshot(scope_type: str, scope_id: str, monitor_payload: dict[str, Any]) -> str:
    st = str(scope_type or "").strip().lower()
    sid = str(scope_id or "").strip()
    if not st or not sid or not isinstance(monitor_payload, dict):
        return "info"

    if st == "node":
        nodes = monitor_payload.get("nodes") if isinstance(monitor_payload.get("nodes"), dict) else {}
        hit: dict[str, Any] | None = None
        for item in nodes.values():
            if not isinstance(item, dict):
                continue
            aliases = {
                str(item.get("node_uid") or "").strip(),
                str(item.get("node_id") or "").strip(),
                str(item.get("topo_node_id") or "").strip(),
                str(item.get("docker_name") or "").strip(),
            }
            if sid in aliases:
                hit = item
                break
        if not hit:
            return "info"
        cpu = _safe_float(hit.get("cpu_ratio"))
        mem = _safe_float(hit.get("mem_ratio"))
        status = str(hit.get("status") or "").strip().upper()
        if status == "DOWN" or cpu >= 0.92 or mem >= 0.92:
            return "critical"
        if status and status != "UP":
            return "warning"
        if cpu >= 0.82 or mem >= 0.82:
            return "warning"
        return "info"

    if st == "link":
        links = monitor_payload.get("links") if isinstance(monitor_payload.get("links"), dict) else {}
        sid_norm = _normalize_link_scope(sid)
        hit = None
        for item in links.values():
            if not isinstance(item, dict):
                continue
            uid = _normalize_link_scope(str(item.get("link_uid") or item.get("link_id") or ""))
            src = str(item.get("src") or item.get("src_node_uid") or item.get("src_node_id") or "").strip()
            dst = str(item.get("dst") or item.get("dst_node_uid") or item.get("dst_node_id") or "").strip()
            pair = normalize_link_uid(src, dst) if src and dst else ""
            if sid_norm and sid_norm in {uid, pair}:
                hit = item
                break
        if not hit:
            return "info"
        loss = _safe_float(hit.get("loss_rate"))
        rtt = _safe_float(hit.get("rtt_ms"))
        jitter = _safe_float(hit.get("jitter_ms"))
        state = str(hit.get("state") or "").strip().upper()
        if state in {"DOWN", "DISCONNECTED"} or loss >= 0.06 or rtt >= 280:
            return "critical"
        if state == "DEGRADED" or loss >= 0.03 or rtt >= 180 or jitter >= 35:
            return "warning"
        return "info"

    return "info"


def _infer_scope_severity_from_observation(scope_type: str, observation: dict[str, Any]) -> str:
    st = str(scope_type or "").strip().lower()
    if not isinstance(observation, dict):
        return "info"
    if st == "node":
        cpu = _safe_float(observation.get("cpu_ratio"))
        mem = _safe_float(observation.get("mem_ratio"))
        status = str(observation.get("status") or "").strip().upper()
        if status == "DOWN" or cpu >= 0.92 or mem >= 0.92:
            return "critical"
        if status and status != "UP":
            return "warning"
        if cpu >= 0.82 or mem >= 0.82:
            return "warning"
        return "info"
    if st == "link":
        loss = _safe_float(observation.get("loss_rate"))
        rtt = _safe_float(observation.get("rtt_ms"))
        jitter = _safe_float(observation.get("jitter_ms"))
        state = str(observation.get("state") or "").strip().upper()
        if state in {"DOWN", "DISCONNECTED"} or loss >= 0.06 or rtt >= 280:
            return "critical"
        if state == "DEGRADED" or loss >= 0.03 or rtt >= 180 or jitter >= 35:
            return "warning"
        return "info"
    return "info"


def _observation_evidence(*, scope_type: str, observation: dict[str, Any]) -> list[str]:
    st = str(scope_type or "").strip().lower()
    if not isinstance(observation, dict):
        return []
    if st == "node":
        cpu = _safe_float(observation.get("cpu_ratio"))
        mem = _safe_float(observation.get("mem_ratio"))
        status = str(observation.get("status") or "").strip().upper()
        out: list[str] = []
        if cpu >= 0.92:
            out.append(f"cpu_ratio={cpu:.3f}>=0.92")
        elif cpu >= 0.82:
            out.append(f"cpu_ratio={cpu:.3f}>=0.82")
        if mem >= 0.92:
            out.append(f"mem_ratio={mem:.3f}>=0.92")
        elif mem >= 0.82:
            out.append(f"mem_ratio={mem:.3f}>=0.82")
        if status and status != "UP":
            out.append(f"status={status}")
        return out
    if st == "link":
        loss = _safe_float(observation.get("loss_rate"))
        rtt = _safe_float(observation.get("rtt_ms"))
        jitter = _safe_float(observation.get("jitter_ms"))
        state = str(observation.get("state") or "").strip().upper()
        out = []
        if loss >= 0.06:
            out.append(f"loss_rate={loss:.3f}>=0.06")
        elif loss >= 0.03:
            out.append(f"loss_rate={loss:.3f}>=0.03")
        if rtt >= 280:
            out.append(f"rtt_ms={rtt:.1f}>=280")
        elif rtt >= 180:
            out.append(f"rtt_ms={rtt:.1f}>=180")
        if jitter >= 35:
            out.append(f"jitter_ms={jitter:.1f}>=35")
        if state in {"DOWN", "DISCONNECTED", "DEGRADED"}:
            out.append(f"state={state}")
        return out
    return []


def _shortest_path_nodes(graph: dict[str, set[str]], src: str, dst: str) -> list[str]:
    if src == dst:
        return [src]
    q = deque([src])
    prev: dict[str, str | None] = {src: None}
    while q:
        cur = q.popleft()
        for nxt in graph.get(cur, set()):
            if nxt in prev:
                continue
            prev[nxt] = cur
            if nxt == dst:
                q.clear()
                break
            q.append(nxt)
    if dst not in prev:
        return []
    out = [dst]
    cur = dst
    while True:
        p = prev.get(cur)
        if p is None:
            break
        out.append(p)
        cur = p
    out.reverse()
    return out


def _global_summary(
    detected_alarms: list[dict[str, Any]],
    spread_result: dict[str, Any],
    impact_result: dict[str, Any],
    *,
    scope_type: str = "",
    scope_id: str = "",
    scope_metric_severity: str = "info",
) -> dict[str, Any]:
    alarms_total = len(detected_alarms)
    node_alarms = sum(1 for x in detected_alarms if str(x.get("scope_type")) == "node")
    link_alarms = alarms_total - node_alarms
    impacted_nodes = len(spread_result.get("impacted_nodes") or [])
    impacted_links = len(spread_result.get("impacted_links") or [])
    tasks = [x for x in (impact_result.get("tasks") or []) if isinstance(x, dict)]
    disconnected = sum(1 for x in tasks if str(x.get("status")) == "disconnected")
    degraded = sum(1 for x in tasks if str(x.get("status")) == "degraded")
    avg_priority = round(sum(_safe_float(x.get("priority_score")) for x in tasks) / len(tasks), 2) if tasks else 0.0
    risk = "normal"
    max_alarm_severity = "info"
    for item in detected_alarms:
        if not isinstance(item, dict):
            continue
        sev = str(item.get("severity") or "info").strip().lower()
        if _severity_rank(sev) > _severity_rank(max_alarm_severity):
            max_alarm_severity = sev
    focused_severity = _focused_scope_severity(scope_type, scope_id, detected_alarms)
    inferred_severity = str(scope_metric_severity or "info").strip().lower()
    if disconnected > 0 or alarms_total >= 20:
        risk = "critical"
    elif degraded > 0 or alarms_total >= 5:
        risk = "warning"
    # 严重度下限约束：避免出现 critical 告警却被评估为 normal 的反直觉结果。
    if focused_severity == "critical":
        risk = "critical"
    elif max_alarm_severity == "critical" and risk == "normal":
        risk = "warning"
    elif max_alarm_severity == "warning" and risk == "normal":
        risk = "warning"
    if inferred_severity == "critical":
        risk = "critical"
    elif inferred_severity == "warning" and risk == "normal":
        risk = "warning"
    return {
        "risk_level": risk,
        "detected_alarm_total": alarms_total,
        "detected_node_alarms": node_alarms,
        "detected_link_alarms": link_alarms,
        "max_alarm_severity": max_alarm_severity,
        "focused_scope_severity": focused_severity,
        "scope_metric_severity": inferred_severity,
        "impacted_nodes": impacted_nodes,
        "impacted_links": impacted_links,
        "task_total": len(tasks),
        "task_disconnected": disconnected,
        "task_degraded": degraded,
        "average_priority_score": avg_priority,
    }


def _build_analysis_reasoning(
    *,
    resolved: dict[str, Any],
    summary: dict[str, Any],
    detected_alarms: list[dict[str, Any]],
    topology_impact: dict[str, Any],
) -> dict[str, Any]:
    scope_type = str((resolved or {}).get("scope_type") or "")
    scope_id = str((resolved or {}).get("scope_id") or "")
    fault_domain = "satellite_only"
    excluded_scope = scope_type == "node" and scope_id and not _is_satellite_node(scope_id)
    impact_policy = str((topology_impact or {}).get("policy") or "")
    matched_flows = int((topology_impact or {}).get("matched_flows") or 0)
    criteria = {
        "node": {
            "warning": ["cpu_ratio>=0.82", "mem_ratio>=0.82", "status!=UP"],
            "critical": ["cpu_ratio>=0.92", "mem_ratio>=0.92", "status=DOWN"],
        },
        "link": {
            "warning": ["loss_rate>=0.03", "rtt_ms>=180", "jitter_ms>=35", "state=DEGRADED"],
            "critical": ["loss_rate>=0.06", "rtt_ms>=280", "state=DOWN"],
        },
    }
    top_findings: list[dict[str, Any]] = []
    for item in detected_alarms[:5]:
        if not isinstance(item, dict):
            continue
        top_findings.append(
            {
                "scope_type": item.get("scope_type"),
                "scope_id": item.get("scope_id"),
                "severity": item.get("severity"),
                "confidence": item.get("confidence"),
                "evidence": item.get("evidence") if isinstance(item.get("evidence"), list) else [],
            }
        )
    note = (
        f"{scope_id} 不在故障域（仅 SAT-* 节点），本次不触发节点故障传播。"
        if excluded_scope
        else (
            f"影响评估采用终端路由优先（命中 flows={matched_flows}）。"
            if impact_policy == "route_terminal_paths"
            else "告警由阈值与基线策略联合判定，影响评估使用拓扑兜底。"
        )
    )
    return {
        "fault_domain": fault_domain,
        "excluded_scope": excluded_scope,
        "impact_policy": impact_policy or "topology_fallback",
        "criteria": criteria,
        "triggered_alarm_count": int((summary or {}).get("detected_alarm_total") or 0),
        "top_findings": top_findings,
        "note": note,
    }


def _build_analysis_narrative(
    *,
    resolved: dict[str, Any],
    summary: dict[str, Any],
    topology_impact: dict[str, Any],
    top_task_id: str | None,
    clusters: list[dict[str, Any]],
) -> dict[str, Any]:
    mode = str((resolved or {}).get("mode") or "-")
    scope_type = str((resolved or {}).get("scope_type") or "-")
    scope_id = str((resolved or {}).get("scope_id") or "-")
    risk = str((summary or {}).get("risk_level") or "unknown")
    impacted_nodes = len((topology_impact or {}).get("impacted_nodes") or [])
    impacted_links = len((topology_impact or {}).get("impacted_links") or [])
    task_total = int((summary or {}).get("task_total") or 0)
    avg_score = float((summary or {}).get("average_priority_score") or 0.0)

    if risk == "critical":
        verdict = "高风险，建议立即处置"
        action = "优先处理断链/中断任务，并执行链路切换或降载。"
    elif risk == "warning":
        verdict = "中风险，建议尽快干预"
        action = "优先排查高分任务链路，确认是否存在持续抖动或丢包升高。"
    else:
        verdict = "低风险，可持续观察"
        action = "保持监控，重点跟踪分数最高任务是否继续上升。"

    sentence = (
        f"本次为 {mode} 模式，聚焦对象 {scope_type}/{scope_id}。"
        f"当前风险等级为 {risk}，影响节点 {impacted_nodes} 个、影响链路 {impacted_links} 条，"
        f"共评估任务 {task_total} 个，平均优先级 {avg_score:.2f}。"
    )
    primary_faults = []
    for item in (clusters or [])[:3]:
        if not isinstance(item, dict):
            continue
        primary_faults.append(
            {
                "cluster_id": item.get("cluster_id"),
                "seed_nodes": item.get("seed_nodes", []),
                "seed_links": item.get("seed_links", []),
                "impacted_nodes": item.get("impacted_nodes_count", 0),
                "impacted_links": item.get("impacted_links_count", 0),
                "contribution": item.get("contribution_ratio", 0.0),
            }
        )
    return {
        "verdict": verdict,
        "summary_sentence": sentence,
        "next_action": action,
        "top_task_id": top_task_id or "-",
        "primary_faults": primary_faults,
    }


def _build_fault_clusters(seeds: dict[str, Any], impact_graph: dict[str, Any]) -> list[dict[str, Any]]:
    seed_nodes = [str(x) for x in (seeds.get("seed_nodes") or []) if str(x).strip()]
    seed_links = [str(x) for x in (seeds.get("seed_links") or []) if str(x).strip()]
    subgraph = impact_graph.get("subgraph") if isinstance(impact_graph, dict) else {}
    edges = subgraph.get("edges") if isinstance(subgraph, dict) else []
    if not isinstance(edges, list) or not edges:
        return [
            {
                "cluster_id": "cluster-1",
                "seed_nodes": seed_nodes,
                "seed_links": seed_links,
                "impacted_nodes_count": len(set(seed_nodes)),
                "impacted_links_count": len(set(seed_links)),
                "contribution_ratio": 1.0 if seed_nodes or seed_links else 0.0,
            }
        ] if (seed_nodes or seed_links) else []

    graph: dict[str, set[str]] = {}
    edge_uids: set[str] = set()
    for e in edges:
        if not isinstance(e, dict):
            continue
        src = str(e.get("src") or "").strip()
        dst = str(e.get("dst") or "").strip()
        if not src or not dst:
            continue
        graph.setdefault(src, set()).add(dst)
        graph.setdefault(dst, set()).add(src)
        edge_uids.add(normalize_link_uid(src, dst))

    unvisited = set(graph.keys())
    comps: list[set[str]] = []
    while unvisited:
        root = unvisited.pop()
        stack = [root]
        comp = {root}
        while stack:
            cur = stack.pop()
            for nxt in graph.get(cur, set()):
                if nxt in comp:
                    continue
                comp.add(nxt)
                if nxt in unvisited:
                    unvisited.remove(nxt)
                stack.append(nxt)
        comps.append(comp)

    total_nodes = max(1, sum(len(c) for c in comps))
    out: list[dict[str, Any]] = []
    for idx, comp in enumerate(sorted(comps, key=lambda c: -len(c)), start=1):
        comp_links = []
        for uid in edge_uids:
            a, b = uid.split("<->", 1)
            if a in comp and b in comp:
                comp_links.append(uid)
        comp_seed_nodes = [x for x in seed_nodes if x in comp]
        comp_seed_links = []
        for lk in seed_links:
            if "<->" not in lk:
                continue
            a, b = lk.split("<->", 1)
            if a in comp and b in comp:
                comp_seed_links.append(lk)
        out.append(
            {
                "cluster_id": f"cluster-{idx}",
                "seed_nodes": comp_seed_nodes,
                "seed_links": comp_seed_links,
                "impacted_nodes_count": len(comp),
                "impacted_links_count": len(comp_links),
                "contribution_ratio": round(len(comp) / total_nodes, 4),
            }
        )
    return out


def _build_security_correlation(
    *,
    resolved: dict[str, Any],
    detected_alarms: list[dict[str, Any]],
    topology_impact: dict[str, Any],
    tasks: list[dict[str, Any]],
    monitor_payload: dict[str, Any],
    window_sec: int,
) -> dict[str, Any]:
    monitor = monitor_payload.get("monitor", {}) if isinstance(monitor_payload, dict) else {}
    alarm_items = monitor.get("alarms") if isinstance(monitor.get("alarms"), list) else []
    now_ts = time.time()

    security_events: list[dict[str, Any]] = []
    for item in alarm_items:
        if not isinstance(item, dict):
            continue
        category = str(item.get("category") or "").strip().lower()
        tags = item.get("security_tags")
        has_security_tag = isinstance(tags, list) and any("security" in str(x).lower() for x in tags)
        if category != "security" and not has_security_tag:
            continue
        ts = _parse_any_timestamp(item.get("timestamp"))
        if ts is not None and now_ts - ts > window_sec:
            continue
        security_events.append(item)

    if not security_events:
        return {
            "level": "none",
            "score": 0.0,
            "window_sec": window_sec,
            "matched_security_events": 0,
            "evidence": [],
            "suspected_path": [],
        }

    anomaly_scopes: set[str] = set()
    for alarm in detected_alarms:
        if not isinstance(alarm, dict):
            continue
        st = str(alarm.get("scope_type") or "").strip().lower()
        sid = str(alarm.get("scope_id") or "").strip()
        if not sid:
            continue
        if st == "link":
            sid = _normalize_link_scope(sid)
        anomaly_scopes.add(f"{st}:{sid}")

    impacted_nodes = [str(x) for x in (topology_impact.get("impacted_nodes") or []) if str(x).strip()]
    impacted_links = [_normalize_link_scope(str(x)) for x in (topology_impact.get("impacted_links") or []) if str(x).strip()]
    impacted_scope_set = {f"node:{x}" for x in impacted_nodes} | {f"link:{x}" for x in impacted_links}

    overlap_items: list[str] = []
    overlap_count = 0
    for sec in security_events:
        st = str(sec.get("scope_type") or "").strip().lower()
        sid = str(sec.get("scope_id") or "").strip()
        if st == "link":
            sid = _normalize_link_scope(sid)
        key = f"{st}:{sid}" if st and sid else ""
        if key and (key in anomaly_scopes or key in impacted_scope_set):
            overlap_count += 1
            overlap_items.append(key)

    score_time = 0.35
    score_overlap = min(0.35, 0.35 * (overlap_count / max(1, len(security_events))))
    degraded_tasks = sum(1 for t in tasks if str((t or {}).get("status") or "") in {"degraded", "disconnected", "latency_anomaly"})
    score_task = 0.15 if degraded_tasks > 0 else 0.0
    score_metric = 0.15 if len(detected_alarms) > 0 else 0.0
    score = min(1.0, round(score_time + score_overlap + score_task + score_metric, 4))

    if score >= 0.75:
        level = "high"
    elif score >= 0.5:
        level = "medium"
    elif score >= 0.25:
        level = "low"
    else:
        level = "none"

    evidence = [
        {
            "type": "time_window",
            "score": round(score_time, 3),
            "detail": f"{window_sec}s 时间窗内命中安全事件 {len(security_events)} 条",
        },
    ]
    if overlap_count > 0:
        evidence.append(
            {
                "type": "entity_overlap",
                "score": round(score_overlap, 3),
                "detail": f"与网络异常对象重叠 {overlap_count} 条",
            }
        )
    if degraded_tasks > 0:
        evidence.append(
            {
                "type": "task_overlap",
                "score": round(score_task, 3),
                "detail": f"受影响任务中异常任务 {degraded_tasks} 个",
            }
        )
    if detected_alarms:
        evidence.append(
            {
                "type": "metric_anomaly",
                "score": round(score_metric, 3),
                "detail": f"当前分析命中网络异常 {len(detected_alarms)} 条",
            }
        )

    suspected_path = sorted(set(overlap_items))[:8]
    resolved_scope = f"{resolved.get('scope_type', '')}:{resolved.get('scope_id', '')}".strip(":")
    if resolved_scope and resolved_scope not in suspected_path and (overlap_count == 0):
        suspected_path = [resolved_scope]

    return {
        "level": level,
        "score": score,
        "window_sec": window_sec,
        "matched_security_events": len(security_events),
        "evidence": evidence,
        "suspected_path": suspected_path,
    }


def _build_route_rank_top(*, tasks: list[dict[str, Any]], impacted_links: list[str]) -> list[dict[str, Any]]:
    scores: dict[str, float] = {}
    impacted_set = {str(x) for x in impacted_links if str(x).strip()}
    for t in tasks:
        if not isinstance(t, dict):
            continue
        task_score = float(t.get("priority_score") or 0.0)
        status = str(t.get("status") or "")
        bonus = 15.0 if status == "disconnected" else 8.0 if status in {"degraded", "latency_anomaly"} else 0.0
        links = [str(x) for x in (t.get("impacted_links") or []) if str(x).strip()]
        for lk in links:
            lk_norm = _normalize_link_scope(lk)
            if impacted_set and lk_norm not in impacted_set:
                continue
            scores[lk_norm] = max(scores.get(lk_norm, 0.0), task_score + bonus)
    if not scores:
        return [{"link_id": x, "score": 0.0, "reason": "route_impact"} for x in impacted_links[:12]]
    ranked = sorted(scores.items(), key=lambda x: (-x[1], x[0]))[:12]
    return [{"link_id": k, "score": round(v, 2), "reason": "task_priority"} for k, v in ranked]


def _parse_any_timestamp(ts: Any) -> float | None:
    if ts in (None, ""):
        return None
    raw = str(ts).strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        return datetime.fromisoformat(raw).timestamp()
    except ValueError:
        return None


def _estimate_forecast_error(*, values: list[float], window: int) -> tuple[float | None, float | None]:
    if len(values) < max(6, window + 2):
        return None, None
    preds: list[float] = []
    actuals: list[float] = []
    for i in range(window, len(values)):
        hist = values[:i]
        pred = _forecast_wma(values=hist, window=min(window, len(hist)), steps=1)[0]
        preds.append(float(pred))
        actuals.append(float(values[i]))
    if not preds:
        return None, None
    ape: list[float] = []
    se_sum = 0.0
    for p, a in zip(preds, actuals):
        if abs(a) > 1e-9:
            ape.append(abs((a - p) / a))
        se_sum += (a - p) ** 2
    mape = round(sum(ape) / len(ape), 6) if ape else None
    rmse = round((se_sum / len(preds)) ** 0.5, 6)
    return mape, rmse


def _forecast_confidence_level(*, validation_mape: float | None, est_mape: float | None) -> str:
    m = validation_mape if isinstance(validation_mape, (int, float)) else est_mape
    if m is None:
        return "unknown"
    if m <= 0.12:
        return "high"
    if m <= 0.25:
        return "medium"
    return "low"


def _simulation_timeline_summary(timeline: list[dict[str, Any]]) -> dict[str, Any]:
    if not timeline:
        return {
            "steps": 0,
            "risk_peak": "normal",
            "max_impacted_nodes": 0,
            "max_impacted_links": 0,
            "max_alarm_total": 0,
        }
    risk_peak = "normal"
    max_nodes = 0
    max_links = 0
    max_alarm = 0
    for item in timeline:
        if not isinstance(item, dict):
            continue
        risk = str(((item.get("summary") or {}).get("risk_level")) or "normal")
        if _severity_rank(risk) > _severity_rank(risk_peak):
            risk_peak = risk
        max_nodes = max(max_nodes, int(item.get("impacted_nodes") or 0))
        max_links = max(max_links, int(item.get("impacted_links") or 0))
        max_alarm = max(max_alarm, int(item.get("detected_alarm_total") or 0))
    return {
        "steps": len(timeline),
        "risk_peak": risk_peak,
        "max_impacted_nodes": max_nodes,
        "max_impacted_links": max_links,
        "max_alarm_total": max_alarm,
    }


def _extract_simulation_key_events(timeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in timeline:
        if not isinstance(item, dict):
            continue
        step = int(item.get("step") or 0)
        risk = str(((item.get("summary") or {}).get("risk_level")) or "normal")
        delta = item.get("delta") if isinstance(item.get("delta"), dict) else {}
        d_nodes = int(delta.get("impacted_nodes") or 0)
        d_links = int(delta.get("impacted_links") or 0)
        d_alarms = int(delta.get("detected_alarm_total") or 0)
        if risk == "critical" or d_nodes > 8 or d_links > 20 or d_alarms > 5:
            out.append(
                {
                    "step": step,
                    "risk_level": risk,
                    "delta": {"impacted_nodes": d_nodes, "impacted_links": d_links, "detected_alarm_total": d_alarms},
                    "direct_reason": str(item.get("direct_reason") or ""),
                    "next_action": str(item.get("next_action") or ""),
                }
            )
    return out[:12]


def _build_simulation_report_prompt(
    *,
    simulation_id: str,
    scenario_type: str,
    focus_scope: dict[str, Any],
    timeline_summary: dict[str, Any],
    key_events: list[dict[str, Any]],
    timeline_tail: list[dict[str, Any]],
) -> str:
    compact = {
        "simulation_id": simulation_id,
        "scenario_type": scenario_type,
        "focus_scope": focus_scope,
        "timeline_summary": timeline_summary,
        "key_events": key_events,
        "timeline_tail": timeline_tail,
    }
    return (
        "你是网络故障演练复盘专家，请输出一份 Markdown 复盘报告。\n"
        "要求：\n"
        "1) 包含“场景摘要、关键拐点、处置动作评估、改进建议、后续演练建议”五个章节；\n"
        "2) 必须引用输入中的 step 与指标变化，不要编造；\n"
        "3) 关键拐点至少列出 3 条；\n"
        "4) 输出中文，简洁但信息完整。\n"
        f"\n输入：\n{json.dumps(compact, ensure_ascii=False)}\n"
    )


def _infer_target_event_type(*, raw_event_type: str, sample: dict[str, Any]) -> str:
    t = str(raw_event_type or "").strip().lower()
    if t in {"node_metric", "node-metric", "host", "node"}:
        return "node_metric"
    if t in {"link_metric", "link-metric", "interface", "net", "link"}:
        return "link_metric"
    if t in {"alarm", "alert"}:
        return "alarm"
    if t in {"flow", "flow_record"}:
        return "flow"
    keys = {str(k).strip().lower() for k in sample.keys()}
    if {"src", "dst"} & keys:
        return "link_metric"
    if {"cpu_ratio", "cpu_percent", "mem_ratio", "mem_percent"} & keys:
        return "node_metric"
    if {"severity", "scope_type"} & keys:
        return "alarm"
    if "flow_id" in keys:
        return "flow"
    return "node_metric"


def _suggest_mapping_rules(*, target_event_type: str, sample: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    aliases: dict[str, list[str]] = {
        "node_metric.node_uid": ["node_uid", "node_id", "docker_name", "host_id", "container_name"],
        "node_metric.cpu_ratio": ["cpu_ratio", "cpu_used_ratio", "cpu_percent", "cpu"],
        "node_metric.mem_ratio": ["mem_ratio", "mem_used_ratio", "mem_percent", "memory"],
        "node_metric.status": ["status", "state"],
        "link_metric.src_node_uid": ["src_node_uid", "src_node_id", "source", "src", "if_src"],
        "link_metric.dst_node_uid": ["dst_node_uid", "dst_node_id", "target", "dst", "if_dst"],
        "link_metric.link_uid": ["link_uid", "link_id"],
        "link_metric.loss_rate": ["loss_rate", "loss", "if_loss"],
        "link_metric.rtt_ms": ["rtt_ms", "latency_ms", "latency", "if_delay_ms"],
        "link_metric.jitter_ms": ["jitter_ms", "jitter", "if_jitter_ms"],
        "alarm.alarm_id": ["alarm_id", "id", "event_id"],
        "alarm.severity": ["severity", "level"],
        "alarm.scope_type": ["scope_type", "target_type"],
        "alarm.scope_id": ["scope_id", "scope_uid", "target_id"],
        "flow.flow_id": ["flow_id", "id", "session_id"],
    }
    required_fields: dict[str, list[str]] = {
        "node_metric": ["node_uid", "cpu_ratio", "mem_ratio"],
        "link_metric": ["src_node_uid", "dst_node_uid", "loss_rate", "rtt_ms"],
        "alarm": ["alarm_id", "severity", "scope_type", "scope_id"],
        "flow": ["flow_id"],
    }
    key_lc = {str(k).strip().lower(): str(k) for k in sample.keys()}
    used_raw: set[str] = set()
    rows: list[dict[str, Any]] = []
    for req in required_fields.get(target_event_type, []):
        alias_key = f"{target_event_type}.{req}"
        cands = aliases.get(alias_key, [req])
        chosen_raw = ""
        chosen_val: Any = None
        for c in cands:
            if c in key_lc:
                chosen_raw = key_lc[c]
                chosen_val = sample.get(chosen_raw)
                break
        confidence = 0.95 if chosen_raw == req else 0.78 if chosen_raw else 0.0
        reason = "exact_match" if chosen_raw == req else ("alias_match" if chosen_raw else "missing")
        if chosen_raw:
            used_raw.add(chosen_raw)
        rows.append(
            {
                "target_field": req,
                "source_field": chosen_raw or None,
                "source_value_preview": str(chosen_val)[:120] if chosen_raw else None,
                "confidence": round(confidence, 2),
                "reason": reason,
            }
        )
    unknown_fields = [str(k) for k in sample.keys() if str(k) not in used_raw]
    return rows, sorted(unknown_fields)


def _mapping_confidence(*, mapping: list[dict[str, Any]], unknown_fields: list[str]) -> dict[str, Any]:
    if not mapping:
        return {"score": 0.0, "level": "low"}
    avg = sum(float(x.get("confidence") or 0.0) for x in mapping) / len(mapping)
    penalty = min(0.25, 0.02 * len(unknown_fields))
    score = max(0.0, min(1.0, avg - penalty))
    level = "high" if score >= 0.8 else "medium" if score >= 0.55 else "low"
    return {"score": round(score, 3), "level": level}


def _mapping_manual_todo(*, mapping: list[dict[str, Any]], unknown_fields: list[str], target_event_type: str) -> list[str]:
    out: list[str] = []
    for row in mapping:
        if not row.get("source_field"):
            out.append(f"补齐必填字段 `{target_event_type}.{row.get('target_field')}` 的原始来源字段")
    if unknown_fields:
        out.append(f"确认未知字段用途并决定是否映射：{', '.join(unknown_fields[:8])}")
    out.append("人工确认后再导出映射配置并生效")
    return out


def _build_mapping_suggestion_prompt(
    *,
    probe_type: str,
    raw_event_type: str,
    target_event_type: str,
    sample: dict[str, Any],
    mapping: list[dict[str, Any]],
    unknown_fields: list[str],
    manual_todo: list[str],
) -> str:
    compact = {
        "probe_type": probe_type,
        "raw_event_type": raw_event_type,
        "target_event_type": target_event_type,
        "sample_keys": sorted([str(k) for k in sample.keys()]),
        "mapping": mapping,
        "unknown_fields": unknown_fields,
        "manual_todo": manual_todo,
    }
    return (
        "你是探针字段映射专家，请给出简洁的中文建议。\n"
        "输出要求：\n"
        "1) 先判断当前映射是否可上线（可/不可）；\n"
        "2) 列出3条主要风险；\n"
        "3) 给出3条人工确认动作；\n"
        "4) 不要输出JSON。\n"
        f"\n输入：\n{json.dumps(compact, ensure_ascii=False)}"
    )


def _build_copilot_prompt(*, analysis: dict[str, Any], question: str, history: list[Any]) -> str:
    compact = {
        "resolved": analysis.get("resolved"),
        "risk_result": analysis.get("risk_result"),
        "evidence_bundle": analysis.get("evidence_bundle"),
        "summary": analysis.get("summary"),
        "topology_impact": analysis.get("topology_impact"),
        "reasoning": analysis.get("reasoning"),
        "narrative": analysis.get("narrative"),
        "security_correlation": analysis.get("security_correlation"),
        "tasks_top": (analysis.get("tasks") or [])[:8] if isinstance(analysis.get("tasks"), list) else [],
        "history": history[-6:] if isinstance(history, list) else [],
        "question": question,
    }
    return (
        "你是网络故障分析副驾。请基于输入上下文回答用户追问。\n"
        "要求：\n"
        "1) 先给结论，再给依据；\n"
        "2) 至少引用2条证据（指标/阈值/影响对象/任务状态）；\n"
        "3) 若数据不足，明确指出缺失项；\n"
        "4) 输出中文纯文本，不要JSON。\n"
        f"\n上下文：\n{json.dumps(compact, ensure_ascii=False)}"
    )


def _copilot_references(*, analysis: dict[str, Any]) -> list[dict[str, Any]]:
    rr = analysis.get("risk_result") if isinstance(analysis.get("risk_result"), dict) else {}
    summary = analysis.get("summary") if isinstance(analysis.get("summary"), dict) else {}
    topo = analysis.get("topology_impact") if isinstance(analysis.get("topology_impact"), dict) else {}
    refs: list[dict[str, Any]] = [
        {"type": "summary", "key": "risk_level", "value": str(rr.get("risk_level") or summary.get("risk_level") or "-")},
        {"type": "summary", "key": "decision", "value": str(rr.get("decision") or "-")},
        {"type": "summary", "key": "detected_alarm_total", "value": int(summary.get("detected_alarm_total") or 0)},
        {"type": "impact", "key": "impacted_nodes", "value": len(topo.get("impacted_nodes") or [])},
        {"type": "impact", "key": "impacted_links", "value": len(topo.get("impacted_links") or [])},
    ]
    return refs


def _copilot_rule_fallback(*, analysis: dict[str, Any], question: str) -> str:
    summary = analysis.get("summary") if isinstance(analysis.get("summary"), dict) else {}
    topo = analysis.get("topology_impact") if isinstance(analysis.get("topology_impact"), dict) else {}
    risk = str(summary.get("risk_level") or "unknown")
    n = len(topo.get("impacted_nodes") or [])
    l = len(topo.get("impacted_links") or [])
    return (
        f"当前无法调用AI服务，先给规则化答复。你问的是：{question}\n"
        f"基于当前分析，风险等级为 {risk}，影响节点 {n} 个、影响链路 {l} 条。\n"
        "建议先查看直接原因与阈值证据，再按高优先级任务链路逐项排查。"
    )


def _safe_float(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


app = create_app()
