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
    epoch = "1708848000"
    for idx, nid in enumerate(["SAT-POLAR-001", "SAT-POLAR-002", "SAT-POLAR-003"], start=1):
        app.state.snapshot_store.apply(
            "node_metric",
            {
                "schema_version": "monitor.v1",
                "message_id": f"fi-node-{idx}",
                "timestamp": "2026-02-26T12:00:00Z",
                "topology_epoch": epoch,
                "node_uid": nid,
                "node_id": nid,
                "cpu_ratio": 0.31,
                "mem_ratio": 0.42,
                "status": "UP",
            },
        )
    app.state.snapshot_store.apply(
        "link_metric",
        {
            "schema_version": "monitor.v1",
            "message_id": "fi-link-1",
            "timestamp": "2026-02-26T12:00:00Z",
            "topology_epoch": epoch,
            "link_uid": "SAT-POLAR-001<->SAT-POLAR-002",
            "link_id": "SAT-POLAR-001<->SAT-POLAR-002",
            "src_node_uid": "SAT-POLAR-001",
            "dst_node_uid": "SAT-POLAR-002",
            "src_node_id": "SAT-POLAR-001",
            "dst_node_id": "SAT-POLAR-002",
            "state": "UP",
            "loss_rate": 0.005,
            "rtt_ms": 42,
            "jitter_ms": 3,
        },
    )
    app.state.snapshot_store.apply(
        "link_metric",
        {
            "schema_version": "monitor.v1",
            "message_id": "fi-link-2",
            "timestamp": "2026-02-26T12:00:00Z",
            "topology_epoch": epoch,
            "link_uid": "SAT-POLAR-002<->SAT-POLAR-003",
            "link_id": "SAT-POLAR-002<->SAT-POLAR-003",
            "src_node_uid": "SAT-POLAR-002",
            "dst_node_uid": "SAT-POLAR-003",
            "src_node_id": "SAT-POLAR-002",
            "dst_node_id": "SAT-POLAR-003",
            "state": "UP",
            "loss_rate": 0.004,
            "rtt_ms": 39,
            "jitter_ms": 2,
        },
    )


async def run() -> None:
    app = create_app()
    seed_snapshot(app)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://collector") as client:
        base = await client.post(
            "/api/v1/bff/analysis/run",
            json={"mode": "global", "scope_type": "network", "scope_id": "all", "topology_epoch": "1708848000"},
        )
        assert base.status_code == 200, base.text
        base_impacted = base.json().get("topology_impact", {}).get("impacted_nodes", [])
        assert len(base_impacted) == 0

        inject = await client.post(
            "/api/v1/ops/fault-injection/control-ack",
            json={
                "type": "control_ack",
                "ok": True,
                "action": "inject_node_fault",
                "request_id": "req-1",
                "topology_epoch": "1708848000",
                "fault": {
                    "fault_id": "fault-001",
                    "fault_type": "DAMAGED",
                    "target": {"node_id": "SAT-POLAR-001"},
                    "created_at": "2026-02-26T12:00:10Z",
                },
            },
        )
        assert inject.status_code == 200, inject.text
        body_inject = inject.json()
        assert len(body_inject.get("alarms_upsert", [])) == 1

        snap1 = await client.get("/api/v1/bff/snapshot?topology_epoch=1708848000")
        assert snap1.status_code == 200, snap1.text
        alarms1 = snap1.json().get("monitor", {}).get("alarms", [])
        assert isinstance(alarms1, list)
        assert len(alarms1) >= 1
        assert any(str(a.get("alarm_id")) == "FI-fault-001" for a in alarms1 if isinstance(a, dict))
        node_after_inject = (
            snap1.json()
            .get("monitor", {})
            .get("nodes", {})
            .get("SAT-POLAR-001", {})
        )
        assert str(node_after_inject.get("status")) == "DOWN"

        after = await client.post(
            "/api/v1/bff/analysis/run",
            json={"mode": "global", "scope_type": "network", "scope_id": "all", "topology_epoch": "1708848000"},
        )
        assert after.status_code == 200, after.text
        impacted_nodes = after.json().get("topology_impact", {}).get("impacted_nodes", [])
        assert "SAT-POLAR-001" in impacted_nodes

        clear = await client.post(
            "/api/v1/ops/fault-injection/control-ack",
            json={
                "type": "control_ack",
                "ok": True,
                "action": "clear_fault",
                "request_id": "req-2",
                "fault_id": "fault-001",
                "topology_epoch": "1708848000",
            },
        )
        assert clear.status_code == 200, clear.text
        body_clear = clear.json()
        assert len(body_clear.get("alarms_recover", [])) == 1

        snap2 = await client.get("/api/v1/bff/snapshot?topology_epoch=1708848000")
        assert snap2.status_code == 200, snap2.text
        assert snap2.json().get("monitor", {}).get("alarm_summary", {}).get("total") == 0
        node_after_clear = (
            snap2.json()
            .get("monitor", {})
            .get("nodes", {})
            .get("SAT-POLAR-001", {})
        )
        assert str(node_after_clear.get("status")) == "UP"

        after_clear = await client.post(
            "/api/v1/bff/analysis/run",
            json={"mode": "global", "scope_type": "network", "scope_id": "all", "topology_epoch": "1708848000"},
        )
        assert after_clear.status_code == 200, after_clear.text
        impacted_after_clear = after_clear.json().get("topology_impact", {}).get("impacted_nodes", [])
        assert len(impacted_after_clear) == 0

        # Clear fallback path: ack doesn't echo fault_id, only returns current faults[].
        inject2 = await client.post(
            "/api/v1/ops/fault-injection/control-ack",
            json={
                "type": "control_ack",
                "ok": True,
                "action": "inject_link_fault",
                "request_id": "req-3",
                "topology_epoch": "1708848000",
                "fault": {
                    "fault_id": "fault-002",
                    "fault_type": "INTERRUPTED",
                    "target": {"a": "SAT-POLAR-001", "b": "SAT-POLAR-002"},
                    "created_at": "2026-02-26T12:01:00Z",
                },
            },
        )
        assert inject2.status_code == 200, inject2.text
        clear2 = await client.post(
            "/api/v1/ops/fault-injection/control-ack",
            json={
                "type": "control_ack",
                "ok": True,
                "action": "clear_fault",
                "request_id": "req-4",
                "topology_epoch": "1708848000",
                "faults": [],
            },
        )
        assert clear2.status_code == 200, clear2.text
        assert len(clear2.json().get("alarms_recover", [])) >= 1

    print("fault_injection_loop_test_ok")


if __name__ == "__main__":
    asyncio.run(run())
