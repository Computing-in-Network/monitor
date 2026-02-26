from __future__ import annotations

from dataclasses import dataclass


RAW_TOPICS = {
    "node_metric": "node_metric",
    "link_metric": "link_metric",
    "flow": "flow",
    "alarm": "alarm",
}

ANALYSIS_TOPICS = {
    "link_health": "link_health",
    "capacity": "capacity",
    "forecast": "forecast",
    "path": "path",
    "security": "security",
    "fault": "fault",
    "alert": "alert",
}


@dataclass(frozen=True)
class TopicSpec:
    event_type: str
    category: str = "raw"


def normalize_event_type(raw_type: str) -> str:
    """
    将上报路径中的别名转换为事件类型。
    """
    key = (raw_type or "").strip().lower().replace("-", "_")
    if key in RAW_TOPICS:
        return key
    return key


def build_subject(environment: str, category: str, event_type: str) -> str:
    return f"{environment}.{category}.{normalize_event_type(event_type)}"
