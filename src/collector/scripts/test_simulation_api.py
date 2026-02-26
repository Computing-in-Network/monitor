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
    for nid, cpu, mem in [
        ("SAT-INCL-001", 0.32, 0.41),
        ("SAT-INCL-011", 0.35, 0.45),
        ("SAT-INCL-091", 0.30, 0.39),
    ]:
        app.state.snapshot_store.apply(
            "node_metric",
            {
                "schema_version": "monitor.v1",
                "message_id": f"s-node-{nid}",
                "timestamp": "2026-02-26T13:00:00Z",
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
            "message_id": "s-link-1",
            "timestamp": "2026-02-26T13:00:00Z",
            "topology_epoch": "1708848000",
            "link_uid": "SAT-INCL-001<->SAT-INCL-011",
            "link_id": "SAT-INCL-001-SAT-INCL-011",
            "src_node_uid": "SAT-INCL-001",
            "dst_node_uid": "SAT-INCL-011",
            "src_node_id": "SAT-INCL-001",
            "dst_node_id": "SAT-INCL-011",
            "state": "UP",
            "loss_rate": 0.004,
            "rtt_ms": 48,
            "jitter_ms": 5,
        },
    )
    app.state.snapshot_store.apply(
        "link_metric",
        {
            "schema_version": "monitor.v1",
            "message_id": "s-link-2",
            "timestamp": "2026-02-26T13:00:00Z",
            "topology_epoch": "1708848000",
            "link_uid": "SAT-INCL-011<->SAT-INCL-091",
            "link_id": "SAT-INCL-011-SAT-INCL-091",
            "src_node_uid": "SAT-INCL-011",
            "dst_node_uid": "SAT-INCL-091",
            "src_node_id": "SAT-INCL-011",
            "dst_node_id": "SAT-INCL-091",
            "state": "UP",
            "loss_rate": 0.003,
            "rtt_ms": 44,
            "jitter_ms": 4,
        },
    )


async def main() -> None:
    app = create_app()
    seed_snapshot(app)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://collector") as client:
        c = await client.post(
            "/api/v1/bff/simulation/create",
            json={
                "scenario_type": "link_down",
                "topology_epoch": "1708848000",
                "steps_total": 3,
                "params": {"link_id": "SAT-INCL-001<->SAT-INCL-011"},
            },
        )
        assert c.status_code == 200, c.text
        simulation_id = c.json().get("simulation_id")
        assert simulation_id

        s1 = await client.post(f"/api/v1/bff/simulation/{simulation_id}/step")
        assert s1.status_code == 200, s1.text
        assert s1.json().get("simulation", {}).get("current_step") == 1

        s2 = await client.post(f"/api/v1/bff/simulation/{simulation_id}/step")
        assert s2.status_code == 200, s2.text
        s3 = await client.post(f"/api/v1/bff/simulation/{simulation_id}/step")
        assert s3.status_code == 200, s3.text
        sim = s3.json().get("simulation", {})
        assert sim.get("status") == "completed"
        assert len(sim.get("timeline", [])) == 3

        g = await client.get(f"/api/v1/bff/simulation/{simulation_id}")
        assert g.status_code == 200, g.text
        t = await client.get(f"/api/v1/bff/simulation/{simulation_id}/timeline")
        assert t.status_code == 200, t.text
        assert len(t.json().get("timeline", [])) == 3

    print("simulation_api_test_ok")


if __name__ == "__main__":
    asyncio.run(main())
