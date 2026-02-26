from __future__ import annotations

from typing import Any

SCHEMA_VERSION = "monitor.v1"

ALLOWED_NODE_STATUS = {"UP", "DOWN", "DEGRADED"}
ALLOWED_LINK_STATE = {"UP", "DOWN", "DEGRADED"}
ALLOWED_SEVERITY = {"critical", "warning", "info"}
ALLOWED_SCOPE_TYPE = {"node", "link", "path"}


class ContractError(ValueError):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


def _require(payload: dict[str, Any], key: str) -> None:
    if key not in payload or payload.get(key) in (None, ""):
        raise ContractError("INVALID_PAYLOAD", f"缺少必填字段: {key}")


def _require_any(payload: dict[str, Any], keys: tuple[str, ...], alias: str) -> None:
    for key in keys:
        if key in payload and payload.get(key) not in (None, ""):
            return
    raise ContractError("INVALID_PAYLOAD", f"缺少必填字段: {alias}")


def _check_ratio(payload: dict[str, Any], key: str) -> None:
    value = payload.get(key)
    if value is None:
        return
    if not isinstance(value, (int, float)) or value < 0 or value > 1:
        raise ContractError("INVALID_PAYLOAD", f"{key} 需为 0~1 数值")


def validate_common(payload: dict[str, Any]) -> None:
    _require(payload, "schema_version")
    _require(payload, "message_id")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ContractError("INVALID_PAYLOAD", "schema_version 必须为 monitor.v1")


def validate_payload(event_type: str, payload: dict[str, Any]) -> None:
    validate_common(payload)

    if event_type == "node_metric":
        _require_any(payload, ("node_uid", "node_id", "docker_name"), "node_uid|node_id|docker_name")
        _check_ratio(payload, "cpu_ratio")
        _check_ratio(payload, "mem_ratio")
        status = payload.get("status")
        if status is not None and status not in ALLOWED_NODE_STATUS:
            raise ContractError("INVALID_PAYLOAD", "status 仅支持 UP|DOWN|DEGRADED")
        return

    if event_type == "link_metric":
        _require_any(payload, ("link_uid", "link_id"), "link_uid|link_id")
        _require_any(payload, ("src_node_uid", "src_node_id"), "src_node_uid|src_node_id")
        _require_any(payload, ("dst_node_uid", "dst_node_id"), "dst_node_uid|dst_node_id")
        state = payload.get("state")
        if state is not None and state not in ALLOWED_LINK_STATE:
            raise ContractError("INVALID_PAYLOAD", "state 仅支持 UP|DOWN|DEGRADED")
        _check_ratio(payload, "loss_rate")
        return

    if event_type == "flow":
        _require(payload, "flow_id")
        return

    if event_type == "alarm":
        _require(payload, "alarm_id")
        severity = payload.get("severity")
        scope_type = payload.get("scope_type")
        if severity is not None and severity not in ALLOWED_SEVERITY:
            raise ContractError("INVALID_PAYLOAD", "severity 仅支持 critical|warning|info")
        if scope_type is not None and scope_type not in ALLOWED_SCOPE_TYPE:
            raise ContractError("INVALID_PAYLOAD", "scope_type 仅支持 node|link|path")
