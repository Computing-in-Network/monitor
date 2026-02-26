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
    }

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://collector") as client:
        await client.get("/api/v1/bff/snapshot")
        await client.get(
            "/api/v1/bff/series",
            params={"event_type": "link_metric", "metric": "rtt_ms", "entity_id": "SAT-INCL-001<->SAT-INCL-011"},
        )
        await client.get(
            "/api/v1/bff/forecast/lstm",
            params={
                "event_type": "link_metric",
                "metric": "rtt_ms",
                "entity_id": "SAT-INCL-001<->SAT-INCL-011",
                "strategy": "fallback",
            },
        )
        spread = await client.post("/api/v1/bff/fault/spread", json=spread_payload)
        spread_result = spread.json().get("result", {})
        await client.post(
            "/api/v1/bff/fault/task-impact",
            json={
                "tasks": [{"task_id": "task-1", "links": ["SAT-INCL-001<->SAT-INCL-011"]}],
                "link_metrics": {"SAT-INCL-001<->SAT-INCL-011": {"state": "DOWN", "rtt_ms": 333, "loss_rate": 0.1}},
                "fault_spread": spread_result,
            },
        )

        slo = await client.get("/api/v1/ops/slo")
        assert slo.status_code == 200, slo.text
        payload = slo.json()
        for key in ["ingest", "db", "query", "forecast", "fault", "by_api", "objectives"]:
            assert key in payload, f"missing key: {key}"
        assert payload["query"]["total"] >= 1
        assert payload["forecast"]["total"] >= 1
        assert payload["fault"]["total"] >= 2

    print("slo_api_test_ok")


if __name__ == "__main__":
    asyncio.run(main())
