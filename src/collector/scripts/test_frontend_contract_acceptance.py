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
        ("SAT-INCL-001", 0.93, 0.72),
        ("SAT-INCL-011", 0.42, 0.45),
        ("SAT-INCL-091", 0.36, 0.41),
    ]:
        app.state.snapshot_store.apply(
            "node_metric",
            {
                "schema_version": "monitor.v1",
                "message_id": f"fc-node-{nid}",
                "timestamp": "2026-02-26T14:00:00Z",
                "topology_epoch": "1708848000",
                "node_uid": nid,
                "node_id": nid,
                "cpu_ratio": cpu,
                "mem_ratio": mem,
                "status": "UP",
            },
        )

    for uid, lid, src, dst, loss, rtt in [
        ("SAT-INCL-001<->SAT-INCL-011", "SAT-INCL-001-SAT-INCL-011", "SAT-INCL-001", "SAT-INCL-011", 0.045, 225),
        ("SAT-INCL-011<->SAT-INCL-091", "SAT-INCL-011-SAT-INCL-091", "SAT-INCL-011", "SAT-INCL-091", 0.014, 120),
    ]:
        app.state.snapshot_store.apply(
            "link_metric",
            {
                "schema_version": "monitor.v1",
                "message_id": f"fc-link-{lid}",
                "timestamp": "2026-02-26T14:00:00Z",
                "topology_epoch": "1708848000",
                "link_uid": uid,
                "link_id": lid,
                "src_node_uid": src,
                "dst_node_uid": dst,
                "src_node_id": src,
                "dst_node_id": dst,
                "state": "UP",
                "loss_rate": loss,
                "rtt_ms": rtt,
                "jitter_ms": 8,
            },
        )


def assert_contract(body: dict) -> None:
    for key in ["status", "contract_version", "summary", "topology_impact", "tasks", "alerts", "meta"]:
        assert key in body, f"missing key: {key}"
    assert body.get("contract_version") == "analysis.v1"


async def main() -> None:
    app = create_app()
    seed_snapshot(app)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://collector") as client:
        # focused node
        n = await client.post(
            "/api/v1/bff/analysis/run",
            json={
                "mode": "focused",
                "scope_type": "node",
                "scope_id": "SAT-INCL-001",
                "topology_epoch": "1708848000",
            },
        )
        assert n.status_code == 200, n.text
        assert_contract(n.json())

        # focused link
        l = await client.post(
            "/api/v1/bff/analysis/run",
            json={
                "mode": "focused",
                "scope_type": "link",
                "scope_id": "SAT-INCL-001<->SAT-INCL-011",
                "topology_epoch": "1708848000",
            },
        )
        assert l.status_code == 200, l.text
        assert_contract(l.json())

        # global
        g = await client.post(
            "/api/v1/bff/analysis/run",
            json={
                "mode": "global",
                "scope_type": "network",
                "scope_id": "all",
                "topology_epoch": "1708848000",
            },
        )
        assert g.status_code == 200, g.text
        assert_contract(g.json())

        # invalid scope -> INVALID_SCOPE
        bad = await client.post(
            "/api/v1/bff/analysis/run",
            json={
                "mode": "focused",
                "scope_type": "network",
                "scope_id": "all",
                "topology_epoch": "1708848000",
            },
        )
        assert bad.status_code == 422, bad.text
        detail = bad.json().get("detail", {})
        assert isinstance(detail, dict)
        assert detail.get("error_code") == "INVALID_SCOPE"

    print("frontend_contract_acceptance_ok")


if __name__ == "__main__":
    asyncio.run(main())
