from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class CollectorConfig:
    environment: str
    nats_url: str
    api_token: str
    allowed_tokens: Optional[list[str]]
    idempotency_ttl: int
    topic_prefix: str
    publish_retries: int
    publish_retry_backoff_ms: int
    failed_events_max_items: int
    failed_events_audit_file: str
    tsdb_enabled: bool
    tsdb_dsn: str
    tsdb_schema: str


def load_config(path: str = "/app/config.example.yaml") -> CollectorConfig:
    raw = {}
    cfg_path = Path(path)
    if cfg_path.exists():
        with cfg_path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

    return CollectorConfig(
        environment=os.environ.get("NET_ENV", raw.get("environment", "dev")),
        nats_url=os.environ.get("NATS_URL", raw.get("nats_url", "nats://nats:4222")),
        api_token=os.environ.get("COLLECTOR_API_TOKEN", raw.get("api_token", "")),
        allowed_tokens=(raw.get("allowed_tokens") or None),
        idempotency_ttl=int(os.environ.get("IDEMPOTENCY_TTL", raw.get("idempotency_ttl", 300))),
        topic_prefix=os.environ.get("TOPIC_PREFIX", raw.get("topic_prefix", "raw")),
        publish_retries=int(os.environ.get("PUBLISH_RETRIES", raw.get("publish_retries", 2))),
        publish_retry_backoff_ms=int(
            os.environ.get("PUBLISH_RETRY_BACKOFF_MS", raw.get("publish_retry_backoff_ms", 200))
        ),
        failed_events_max_items=int(
            os.environ.get("FAILED_EVENTS_MAX_ITEMS", raw.get("failed_events_max_items", 2000))
        ),
        failed_events_audit_file=os.environ.get(
            "FAILED_EVENTS_AUDIT_FILE",
            raw.get("failed_events_audit_file", "/tmp/collector_failed_events.jsonl"),
        ),
        tsdb_enabled=str(os.environ.get("TSDB_ENABLED", raw.get("tsdb_enabled", "false"))).lower() in {"1", "true", "yes", "on"},
        tsdb_dsn=os.environ.get(
            "TSDB_DSN",
            raw.get("tsdb_dsn", "postgresql://monitor:monitor123@monitor_timescaledb:5432/monitor"),
        ),
        tsdb_schema=os.environ.get("TSDB_SCHEMA", raw.get("tsdb_schema", "monitor_ts")),
    )
