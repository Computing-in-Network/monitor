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


class DummyTSWriter:
    def is_ready(self) -> bool:
        return True

    def read_metric_series(self, event_type: str, metric: str, entity_id: str, limit: int, topology_epoch: str | None = None):
        return [
            {"ts": "2026-02-26T12:00:00Z", "value": 10.0},
            {"ts": "2026-02-26T12:01:00Z", "value": 11.0},
            {"ts": "2026-02-26T12:02:00Z", "value": 10.5},
            {"ts": "2026-02-26T12:03:00Z", "value": 12.0},
            {"ts": "2026-02-26T12:04:00Z", "value": 11.8},
        ][: max(1, int(limit))]


async def main() -> None:
    app = create_app()
    app.state.ts_writer = DummyTSWriter()

    spread_payload = {
        "alarm_nodes": ["SAT-INCL-001"],
        "links": [
            {"src": "SAT-INCL-001", "dst": "SAT-INCL-011", "health": 0.40},
            {"src": "SAT-INCL-011", "dst": "GW-EDGE-001", "health": 0.55},
        ],
        "mode": "cascade",
        "max_depth": 3,
    }

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://collector") as client:
        r1 = await client.get("/api/v1/bff/snapshot")
        assert r1.status_code == 200, r1.text
        etag = r1.headers.get("etag")
        assert etag, "missing ETag"

        r2 = await client.get("/api/v1/bff/snapshot", headers={"if-none-match": etag})
        assert r2.status_code == 304, r2.text

        rs = await client.get(
            "/api/v1/bff/series",
            params={
                "event_type": "link_metric",
                "metric": "rtt_ms",
                "entity_id": "SAT-INCL-001<->SAT-INCL-011",
                "limit": 5,
            },
        )
        assert rs.status_code == 200, rs.text
        assert isinstance(rs.json().get("points"), list)

        rf = await client.get(
            "/api/v1/bff/forecast/lstm",
            params={
                "event_type": "link_metric",
                "metric": "rtt_ms",
                "entity_id": "SAT-INCL-001<->SAT-INCL-011",
                "strategy": "fallback",
                "horizon": 3,
                "window": 4,
            },
        )
        assert rf.status_code == 200, rf.text
        assert len(rf.json().get("points", [])) == 3

        rspread = await client.post("/api/v1/bff/fault/spread", json=spread_payload)
        assert rspread.status_code == 200, rspread.text
        spread_result = rspread.json().get("result", {})

        rimpact = await client.post(
            "/api/v1/bff/fault/task-impact",
            json={
                "tasks": [
                    {
                        "task_id": "task-recon-001",
                        "name": "态势侦察",
                        "criticality": 0.9,
                        "links": ["SAT-INCL-001<->SAT-INCL-011", "SAT-INCL-011<->GW-EDGE-001"],
                    }
                ],
                "link_metrics": {
                    "SAT-INCL-001<->SAT-INCL-011": {"state": "DOWN", "rtt_ms": 350, "loss_rate": 0.2},
                    "SAT-INCL-011<->GW-EDGE-001": {"state": "UP", "rtt_ms": 210, "loss_rate": 0.04},
                },
                "fault_spread": spread_result,
            },
        )
        assert rimpact.status_code == 200, rimpact.text
        assert isinstance(rimpact.json().get("result", {}).get("tasks"), list)

    print("bff_api_test_ok")


if __name__ == "__main__":
    asyncio.run(main())
