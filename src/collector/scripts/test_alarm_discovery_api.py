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
            "message_id": "seed-node-1",
            "timestamp": "2026-02-26T10:00:00Z",
            "topology_epoch": "1708848000",
            "node_uid": "SAT-POLAR-001",
            "node_id": "SAT-POLAR-001",
            "cpu_ratio": 0.93,
            "mem_ratio": 0.87,
            "status": "UP",
        },
    )
    app.state.snapshot_store.apply(
        "node_metric",
        {
            "schema_version": "monitor.v1",
            "message_id": "seed-node-2",
            "timestamp": "2026-02-26T10:00:00Z",
            "topology_epoch": "1708848000",
            "node_uid": "SAT-POLAR-002",
            "node_id": "SAT-POLAR-002",
            "cpu_ratio": 0.35,
            "mem_ratio": 0.41,
            "status": "UP",
        },
    )
    app.state.snapshot_store.apply(
        "link_metric",
        {
            "schema_version": "monitor.v1",
            "message_id": "seed-link-1",
            "timestamp": "2026-02-26T10:00:00Z",
            "topology_epoch": "1708848000",
            "link_uid": "SAT-POLAR-001<->SAT-POLAR-002",
            "link_id": "SAT-POLAR-001-SAT-POLAR-002",
            "src_node_uid": "SAT-POLAR-001",
            "dst_node_uid": "SAT-POLAR-002",
            "src_node_id": "SAT-POLAR-001",
            "dst_node_id": "SAT-POLAR-002",
            "state": "UP",
            "loss_rate": 0.041,
            "rtt_ms": 245,
            "jitter_ms": 11,
        },
    )


async def main() -> None:
    app = create_app()
    seed_snapshot(app)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://collector") as client:
        r1 = await client.post(
            "/api/v1/analysis/alarm/discover",
            json={
                "topology_epoch": "1708848000",
                "scope_type": "network",
                "scope_id": "all",
                "strategies": ["threshold"],
            },
        )
        assert r1.status_code == 200, r1.text
        body1 = r1.json()
        assert body1.get("summary", {}).get("total", 0) >= 2
        assert isinstance(body1.get("detected_alarms"), list)

        r2 = await client.post(
            "/api/v1/analysis/alarm/discover",
            json={
                "topology_epoch": "1708848000",
                "scope_type": "link",
                "scope_id": "SAT-POLAR-001<->SAT-POLAR-002",
                "strategies": ["threshold"],
            },
        )
        assert r2.status_code == 200, r2.text
        body2 = r2.json()
        assert body2.get("summary", {}).get("scanned_links", 0) >= 1

        r3 = await client.post(
            "/api/v1/bff/analysis/alarm/discover",
            json={
                "topology_epoch": "1708848000",
                "scope_type": "node",
                "scope_id": "SAT-POLAR-001",
                "strategies": ["threshold"],
            },
        )
        assert r3.status_code == 200, r3.text
        body3 = r3.json()
        assert body3.get("scope_type") == "node"

    print("alarm_discovery_api_test_ok")


if __name__ == "__main__":
    asyncio.run(main())
