from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from .config import CollectorConfig
from .contract import ContractError, validate_payload
from .failed_events import FailedEventStore
from .middleware.auth import validate_api_token
from .events import build_subject, normalize_event_type
from .mapping import normalize_payload, validate_alarm_mapping
from .observability import OutcomeStats
from .publisher import EventPublisher, PublishUnavailableError
from .repository import IdempotentStore
from .snapshot import MonitorSnapshotStore
from .storage import TimescaleWriter, TimescaleWriteError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ingest", tags=["ingest"])

ALLOWED_KINDS = {"node_metric", "link_metric", "flow", "alarm", "node-metric", "link-metric"}


def _normalize_kind(kind: str) -> str:
    return normalize_event_type(kind)


def _get_config(request: Request) -> CollectorConfig:
    return request.app.state.config


def _get_publisher(request: Request) -> EventPublisher:
    return request.app.state.publisher


def _get_dedup(request: Request) -> IdempotentStore:
    return request.app.state.idempotent


def _get_stats(request: Request) -> OutcomeStats:
    return request.app.state.stats


def _get_snapshot_store(request: Request) -> MonitorSnapshotStore:
    return request.app.state.snapshot_store


def _get_failed_events_store(request: Request) -> FailedEventStore:
    return request.app.state.failed_events


def _get_ts_writer(request: Request) -> TimescaleWriter | None:
    return request.app.state.ts_writer


def _build_trace_id(request: Request) -> str:
    return request.headers.get("x-trace-id") or request.headers.get("x-request-id") or uuid4().hex


def _audit(
    request: Request,
    trace_id: str,
    result_code: str,
    event_type: str,
    message_id: str,
) -> None:
    logger.info(
        "ingest_audit trace_id=%s result_code=%s event_type=%s message_id=%s remote_addr=%s",
        trace_id,
        result_code,
        event_type,
        message_id,
        request.client.host if request.client else "",
    )


def _error_response(
    status_code: int,
    error_code: str,
    error_message: str,
    trace_id: str,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "status": "error",
            "error_code": error_code,
            "error_message": error_message,
            "trace_id": trace_id,
        },
    )


@router.post("/{kind}")
async def ingest(
    kind: str,
    request: Request,
    payload: dict[str, Any],
    x_api_token: str | None = Header(default=None),
    config: CollectorConfig = Depends(_get_config),
    publisher: EventPublisher = Depends(_get_publisher),
    dedup: IdempotentStore = Depends(_get_dedup),
    stats: OutcomeStats = Depends(_get_stats),
    snapshot_store: MonitorSnapshotStore = Depends(_get_snapshot_store),
    failed_events: FailedEventStore = Depends(_get_failed_events_store),
    ts_writer: TimescaleWriter | None = Depends(_get_ts_writer),
):
    trace_id = _build_trace_id(request)
    event_type = _normalize_kind(kind)
    message_id = str(payload.get("message_id") or "")

    if kind not in ALLOWED_KINDS:
        stats.record("INVALID_KIND")
        _audit(request, trace_id, "INVALID_KIND", event_type, message_id)
        return _error_response(
            status_code=400,
            error_code="INVALID_KIND",
            error_message=f"不支持的事件类型: {kind}",
            trace_id=trace_id,
        )

    headers = request.headers
    try:
        validate_api_token(headers, config.api_token)
    except HTTPException as exc:
        stats.record("UNAUTHORIZED")
        _audit(request, trace_id, "UNAUTHORIZED", event_type, message_id)
        return _error_response(
            status_code=exc.status_code,
            error_code="UNAUTHORIZED",
            error_message="鉴权失败",
            trace_id=trace_id,
        )

    normalize_payload(event_type, payload)
    try:
        validate_payload(event_type, payload)
    except ContractError as e:
        stats.record(e.code)
        _audit(request, trace_id, e.code, event_type, message_id)
        return _error_response(
            status_code=422,
            error_code=e.code,
            error_message=e.message,
            trace_id=trace_id,
        )

    mapping_error = validate_alarm_mapping(event_type, payload, snapshot_store)
    if mapping_error is not None:
        error_code, error_message = mapping_error
        stats.record(error_code)
        _audit(request, trace_id, error_code, event_type, message_id)
        return _error_response(
            status_code=422,
            error_code=error_code,
            error_message=error_message,
            trace_id=trace_id,
        )

    if dedup.is_duplicate(message_id):
        stats.record("DUPLICATE")
        _audit(request, trace_id, "DUPLICATE", event_type, message_id)
        return {
            "status": "duplicate",
            "event_type": event_type,
            "message_id": message_id,
            "trace_id": trace_id,
        }

    if "timestamp" not in payload:
        payload["timestamp"] = datetime.now(timezone.utc).isoformat()

    topic = build_subject(config.environment, config.topic_prefix, event_type)
    event_message = {
        "event_type": event_type,
        "payload": payload,
        "received_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "token": "masked" if x_api_token else "unknown",
            "remote_addr": request.client.host if request.client else "",
        },
    }
    try:
        await publisher.publish(topic, event_message)
    except PublishUnavailableError:
        stats.record("NATS_UNAVAILABLE")
        _audit(request, trace_id, "NATS_UNAVAILABLE", event_type, message_id)
        failed_events.record(
            trace_id=trace_id,
            subject=topic,
            payload=event_message,
            error_code="NATS_UNAVAILABLE",
            error_message="事件总线不可用，投递失败",
        )
        return _error_response(
            status_code=503,
            error_code="NATS_UNAVAILABLE",
            error_message="事件总线不可用，投递失败",
            trace_id=trace_id,
        )

    snapshot_store.apply(event_type, payload)
    if ts_writer is not None:
        try:
            await asyncio.to_thread(ts_writer.write_event, event_type, payload)
        except TimescaleWriteError as e:
            stats.record("DB_WRITE_FAILED")
            _audit(request, trace_id, "DB_WRITE_FAILED", event_type, message_id)
            failed_events.record(
                trace_id=trace_id,
                subject="timescale.write",
                payload={"event_type": event_type, "payload": payload},
                error_code="DB_WRITE_FAILED",
                error_message=str(e),
            )
    stats.record("OK")
    _audit(request, trace_id, "OK", event_type, message_id)
    return {
        "status": "ok",
        "event_type": event_type,
        "message_id": message_id,
        "trace_id": trace_id,
    }
