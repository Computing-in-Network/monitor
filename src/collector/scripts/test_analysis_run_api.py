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
    nodes = [
        ("SAT-POLAR-001", 0.92, 0.81),
        ("SAT-POLAR-002", 0.44, 0.46),
        ("SAT-POLAR-003", 0.38, 0.41),
    ]
    for idx, (nid, cpu, mem) in enumerate(nodes, start=1):
        app.state.snapshot_store.apply(
            "node_metric",
            {
                "schema_version": "monitor.v1",
                "message_id": f"r-node-{idx}",
                "timestamp": "2026-02-26T12:00:00Z",
                "topology_epoch": "1708848000",
                "node_uid": nid,
                "node_id": nid,
                "cpu_ratio": cpu,
                "mem_ratio": mem,
                "status": "UP",
            },
        )
    app.state.snapshot_store.apply(
        "link_metric",
        {
            "schema_version": "monitor.v1",
            "message_id": "r-link-1",
            "timestamp": "2026-02-26T12:00:00Z",
            "topology_epoch": "1708848000",
            "link_uid": "SAT-POLAR-001<->SAT-POLAR-002",
            "link_id": "SAT-POLAR-001-SAT-POLAR-002",
            "src_node_uid": "SAT-POLAR-001",
            "dst_node_uid": "SAT-POLAR-002",
            "src_node_id": "SAT-POLAR-001",
            "dst_node_id": "SAT-POLAR-002",
            "state": "UP",
            "loss_rate": 0.041,
            "rtt_ms": 220,
            "jitter_ms": 14,
        },
    )
    app.state.snapshot_store.apply(
        "link_metric",
        {
            "schema_version": "monitor.v1",
            "message_id": "r-link-2",
            "timestamp": "2026-02-26T12:00:00Z",
            "topology_epoch": "1708848000",
            "link_uid": "SAT-POLAR-002<->SAT-POLAR-003",
            "link_id": "SAT-POLAR-002-SAT-POLAR-003",
            "src_node_uid": "SAT-POLAR-002",
            "dst_node_uid": "SAT-POLAR-003",
            "src_node_id": "SAT-POLAR-002",
            "dst_node_id": "SAT-POLAR-003",
            "state": "DEGRADED",
            "loss_rate": 0.021,
            "rtt_ms": 170,
            "jitter_ms": 10,
        },
    )


async def main() -> None:
    app = create_app()
    seed_snapshot(app)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://collector") as client:
        # global
        r1 = await client.post(
            "/api/v1/bff/analysis/run",
            json={
                "mode": "global",
                "scope_type": "network",
                "scope_id": "all",
                "topology_epoch": "1708848000",
            },
        )
        assert r1.status_code == 200, r1.text
        b1 = r1.json()
        assert b1.get("resolved", {}).get("mode") == "global"
        assert isinstance(b1.get("tasks"), list)

        # focused-link
        r2 = await client.post(
            "/api/v1/bff/analysis/run",
            json={
                "mode": "focused",
                "scope_type": "link",
                "scope_id": "SAT-POLAR-001<->SAT-POLAR-002",
                "topology_epoch": "1708848000",
            },
        )
        assert r2.status_code == 200, r2.text
        b2 = r2.json()
        assert b2.get("resolved", {}).get("scope_type") == "link"

        # auto
        r3 = await client.post(
            "/api/v1/bff/analysis/run",
            json={
                "mode": "auto",
                "scope_type": "link",
                "scope_id": "SAT-POLAR-001<->SAT-POLAR-002",
                "topology_epoch": "1708848000",
            },
        )
        assert r3.status_code == 200, r3.text
        b3 = r3.json()
        assert b3.get("resolved", {}).get("mode") == "focused"
        assert "summary" in b3 and "topology_impact" in b3

    print("analysis_run_api_test_ok")


if __name__ == "__main__":
    asyncio.run(main())
