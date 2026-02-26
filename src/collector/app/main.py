from __future__ import annotations

from collections import deque
import time
from typing import Optional

from fastapi import FastAPI, HTTPException, Request, Response

from .config import load_config
from .failed_events import FailedEventStore
from .fault_spread import AnalyzeRequest, SpreadAnalyzer
from .fault_task_impact import TaskImpactRequest, TaskImpactService
from .forecast_registry import ForecastRegistry
from .observability import ApiSLOTracker, OutcomeStats
from .publisher import EventPublisher
from .repository import IdempotentStore
from .routes import router
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
    app.state.fault_spread = SpreadAnalyzer()
    app.state.task_impact = TaskImpactService()
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
                raise HTTPException(status_code=422, detail="not enough points for forecast")

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
                links=[x for x in links if isinstance(x, dict)],
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


app = create_app()
