from __future__ import annotations

from collections import deque
from typing import Optional

from fastapi import FastAPI, HTTPException, Request, Response

from .config import load_config
from .failed_events import FailedEventStore
from .observability import OutcomeStats
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
    app.state.snapshot_store = MonitorSnapshotStore()
    app.state.failed_events = FailedEventStore(
        max_items=config.failed_events_max_items,
        audit_file_path=config.failed_events_audit_file,
    )
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
            return Response(status_code=304, headers=headers)

        return payload

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

    @app.get("/api/v1/analysis/forecast/lstm")
    async def forecast_lstm(
        event_type: str,
        metric: str,
        entity_id: str,
        topology_epoch: Optional[str] = None,
        horizon: int = 12,
        window: int = 12,
        history_limit: int = 240,
    ) -> dict[str, object]:
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

        # Keep this endpoint deterministic and lightweight in collector:
        # weighted moving average serves as LSTM fallback when model runtime is not attached.
        values = [float(p["value"]) for p in points]
        safe_window = max(3, min(int(window), len(values)))
        safe_horizon = max(1, min(int(horizon), 120))
        baseline = _forecast_wma(values=values, window=safe_window, steps=safe_horizon)
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
        return {
            "status": "ok",
            "source": "timescaledb",
            "model_type": "lstm_fallback_wma",
            "event_type": event_type,
            "metric": metric,
            "entity_id": entity_id,
            "topology_epoch": topology_epoch,
            "history_points": len(values),
            "window": safe_window,
            "horizon": safe_horizon,
            "points": out,
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
