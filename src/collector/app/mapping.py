from __future__ import annotations

from typing import Any


def _norm_link_uid(a: str, b: str) -> str:
    x = str(a or "").strip()
    y = str(b or "").strip()
    if not x or not y:
        return ""
    return "<->".join(sorted([x, y]))


def normalize_payload(event_type: str, payload: dict[str, Any]) -> None:
    if event_type == "node_metric":
        node_uid = str(payload.get("node_uid") or payload.get("node_id") or payload.get("docker_name") or "").strip()
        if node_uid:
            payload["node_uid"] = node_uid
        if "docker_name" not in payload or not payload.get("docker_name"):
            payload["docker_name"] = node_uid
        if "topo_node_id" not in payload or not payload.get("topo_node_id"):
            payload["topo_node_id"] = str(payload.get("node_id") or node_uid)
        return

    if event_type == "link_metric":
        src_uid = str(payload.get("src_node_uid") or payload.get("src_node_id") or "").strip()
        dst_uid = str(payload.get("dst_node_uid") or payload.get("dst_node_id") or "").strip()
        if src_uid:
            payload["src_node_uid"] = src_uid
            if "src_node_id" not in payload or not payload.get("src_node_id"):
                payload["src_node_id"] = src_uid
        if dst_uid:
            payload["dst_node_uid"] = dst_uid
            if "dst_node_id" not in payload or not payload.get("dst_node_id"):
                payload["dst_node_id"] = dst_uid
        link_uid = str(payload.get("link_uid") or "").strip() or _norm_link_uid(src_uid, dst_uid)
        if link_uid:
            payload["link_uid"] = link_uid
        if "link_id" not in payload or not payload.get("link_id"):
            payload["link_id"] = link_uid
        return

    if event_type == "alarm":
        scope_type = str(payload.get("scope_type") or "").strip().lower()
        scope_uid = str(payload.get("scope_uid") or "").strip()
        scope_id = str(payload.get("scope_id") or "").strip()
        if not scope_uid and scope_type == "node":
            scope_uid = str(payload.get("node_uid") or scope_id).strip()
        elif not scope_uid and scope_type == "link":
            if "<->" in scope_id:
                scope_uid = scope_id
            elif "->" in scope_id:
                p = [x.strip() for x in scope_id.split("->", 1)]
                scope_uid = _norm_link_uid(p[0], p[1])
            else:
                scope_uid = str(payload.get("link_uid") or scope_id).strip()
        elif not scope_uid and scope_type == "path":
            scope_uid = scope_id
        if scope_uid:
            payload["scope_uid"] = scope_uid
        if "scope_id" not in payload and scope_uid:
            payload["scope_id"] = scope_uid


def validate_alarm_mapping(event_type: str, payload: dict[str, Any], snapshot_store: Any) -> tuple[str, str] | None:
    if event_type != "alarm":
        return None
    scope_type = str(payload.get("scope_type") or "").strip().lower()
    scope_uid = str(payload.get("scope_uid") or "").strip()
    epoch = payload.get("topology_epoch")
    if not snapshot_store.has_epoch(epoch):
        return ("EPOCH_MAPPING_NOT_FOUND", "当前 topology_epoch 无可用映射")
    if scope_type == "node":
        if not scope_uid or not snapshot_store.has_node_uid(epoch, scope_uid):
            return ("UNKNOWN_NODE_UID", f"未找到 node_uid 映射: {scope_uid or '<empty>'}")
    elif scope_type == "link":
        if not scope_uid or not snapshot_store.has_link_uid(epoch, scope_uid):
            return ("UNKNOWN_LINK_UID", f"未找到 link_uid 映射: {scope_uid or '<empty>'}")
    return None
