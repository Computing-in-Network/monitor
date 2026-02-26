#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import httpx

os.environ.setdefault("TSDB_ENABLED", "false")
os.environ.setdefault("NATS_URL", "nats://127.0.0.1:4222")
os.environ.setdefault("FORECAST_MODEL_DIR", "/tmp/forecast_models/lstm")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.main import create_app


def seed_snapshot(app) -> None:
    app.state.snapshot_store.apply(
        "node_metric",
        {
            "schema_version": "monitor.v1",
            "message_id": "g-node-1",
            "timestamp": "2026-02-26T11:00:00Z",
            "topology_epoch": "1708848000",
            "node_uid": "SAT-POLAR-001",
            "node_id": "SAT-POLAR-001",
            "cpu_ratio": 0.94,
            "mem_ratio": 0.72,
            "status": "UP",
        },
    )
    app.state.snapshot_store.apply(
        "node_metric",
        {
            "schema_version": "monitor.v1",
            "message_id": "g-node-2",
            "timestamp": "2026-02-26T11:00:00Z",
            "topology_epoch": "1708848000",
            "node_uid": "SAT-POLAR-002",
            "node_id": "SAT-POLAR-002",
            "cpu_ratio": 0.36,
            "mem_ratio": 0.42,
            "status": "UP",
        },
    )
    app.state.snapshot_store.apply(
        "node_metric",
        {
            "schema_version": "monitor.v1",
            "message_id": "g-node-3",
            "timestamp": "2026-02-26T11:00:00Z",
            "topology_epoch": "1708848000",
            "node_uid": "SAT-POLAR-003",
            "node_id": "SAT-POLAR-003",
            "cpu_ratio": 0.40,
            "mem_ratio": 0.44,
            "status": "UP",
        },
    )
    app.state.snapshot_store.apply(
        "link_metric",
        {
            "schema_version": "monitor.v1",
            "message_id": "g-link-1",
            "timestamp": "2026-02-26T11:00:00Z",
            "topology_epoch": "1708848000",
            "link_uid": "SAT-POLAR-001<->SAT-POLAR-002",
            "link_id": "SAT-POLAR-001-SAT-POLAR-002",
            "src_node_uid": "SAT-POLAR-001",
            "dst_node_uid": "SAT-POLAR-002",
            "src_node_id": "SAT-POLAR-001",
            "dst_node_id": "SAT-POLAR-002",
            "state": "UP",
            "loss_rate": 0.054,
            "rtt_ms": 240,
            "jitter_ms": 15,
        },
    )
    app.state.snapshot_store.apply(
        "link_metric",
        {
            "schema_version": "monitor.v1",
            "message_id": "g-link-2",
            "timestamp": "2026-02-26T11:00:00Z",
            "topology_epoch": "1708848000",
            "link_uid": "SAT-POLAR-002<->SAT-POLAR-003",
            "link_id": "SAT-POLAR-002-SAT-POLAR-003",
            "src_node_uid": "SAT-POLAR-002",
            "dst_node_uid": "SAT-POLAR-003",
            "src_node_id": "SAT-POLAR-002",
            "dst_node_id": "SAT-POLAR-003",
            "state": "DEGRADED",
            "loss_rate": 0.021,
            "rtt_ms": 165,
            "jitter_ms": 18,
        },
    )


async def main() -> None:
    app = create_app()
    seed_snapshot(app)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://collector") as client:
        r1 = await client.post(
            "/api/v1/bff/analysis/global-impact",
            json={
                "mode": "global",
                "scope_type": "network",
                "scope_id": "all",
                "topology_epoch": "1708848000",
            },
        )
        assert r1.status_code == 200, r1.text
        body1 = r1.json()
        assert body1.get("status") == "ok"
        assert isinstance(body1.get("detected_alarms"), list)
        assert isinstance(body1.get("impact_graph"), dict)
        assert isinstance(body1.get("task_impacts"), dict)
        assert body1.get("summary", {}).get("detected_alarm_total", 0) >= 1

        r2 = await client.post(
            "/api/v1/bff/analysis/global-impact",
            json={
                "mode": "focused",
                "scope_type": "link",
                "scope_id": "SAT-POLAR-001<->SAT-POLAR-002",
                "topology_epoch": "1708848000",
            },
        )
        assert r2.status_code == 200, r2.text
        body2 = r2.json()
        assert body2.get("scope_type") == "link"
        assert body2.get("scope_id") == "SAT-POLAR-001<->SAT-POLAR-002"

    print("global_impact_api_test_ok")


if __name__ == "__main__":
    asyncio.run(main())
