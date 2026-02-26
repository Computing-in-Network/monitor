#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import os
import statistics
import sys
import time
from pathlib import Path

import httpx

# Avoid startup dependencies (NATS/TSDB) during API-level tests.
os.environ.setdefault("TSDB_ENABLED", "false")
os.environ.setdefault("NATS_URL", "nats://127.0.0.1:4222")
os.environ.setdefault("FORECAST_MODEL_DIR", "/tmp/forecast_models/lstm")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.main import create_app


SPREAD_PAYLOAD = {
    "alarm_nodes": ["SAT-INCL-001"],
    "links": [
        {"src": "SAT-INCL-001", "dst": "SAT-INCL-011", "health": 0.40},
        {"src": "SAT-INCL-011", "dst": "GW-EDGE-001", "health": 0.55},
        {"src": "GW-EDGE-001", "dst": "DC-CORE-001", "health": 0.91},
    ],
    "mode": "cascade",
    "max_depth": 3,
    "cascade_threshold": 0.6,
}


async def main() -> None:
    app = create_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://collector") as client:
        invalid = await client.post("/api/v1/fault/spread", json={"foo": "bar"})
        assert invalid.status_code == 422, f"unexpected invalid status: {invalid.status_code}"

        spread_resp = await client.post("/api/v1/fault/spread", json=SPREAD_PAYLOAD)
        assert spread_resp.status_code == 200, spread_resp.text
        spread_result = spread_resp.json()["result"]
        assert isinstance(spread_result.get("impacted_nodes"), list)
        assert isinstance(spread_result.get("impacted_links"), list)

        task_payload = {
            "tasks": [
                {
                    "task_id": "task-recon-001",
                    "name": "态势侦察",
                    "criticality": 0.9,
                    "links": ["SAT-INCL-001<->SAT-INCL-011", "SAT-INCL-011<->GW-EDGE-001"],
                },
                {
                    "task_id": "task-c2-001",
                    "name": "指挥链路",
                    "criticality": 0.7,
                    "links": ["GW-EDGE-001<->DC-CORE-001"],
                },
            ],
            "link_metrics": {
                "SAT-INCL-001<->SAT-INCL-011": {"state": "DOWN", "rtt_ms": 350, "loss_rate": 0.2},
                "SAT-INCL-011<->GW-EDGE-001": {"state": "UP", "rtt_ms": 210, "loss_rate": 0.04},
                "GW-EDGE-001<->DC-CORE-001": {"state": "UP", "rtt_ms": 80, "loss_rate": 0.002},
            },
            "fault_spread": spread_result,
            "rtt_warn_ms": 180,
            "loss_warn_rate": 0.03,
        }
        task_resp = await client.post("/api/v1/fault/task-impact", json=task_payload)
        assert task_resp.status_code == 200, task_resp.text
        task_result = task_resp.json()["result"]
        assert len(task_result.get("tasks", [])) == 2

        samples: list[float] = []
        for _ in range(200):
            t0 = time.perf_counter()
            r = await client.post("/api/v1/fault/spread", json=SPREAD_PAYLOAD)
            dt = (time.perf_counter() - t0) * 1000.0
            assert r.status_code == 200, r.text
            samples.append(dt)

    samples_sorted = sorted(samples)
    p95 = samples_sorted[int(0.95 * (len(samples_sorted) - 1))]
    avg = statistics.mean(samples)
    print(f"fault_api_test_ok: avg_ms={avg:.3f}, p95_ms={p95:.3f}, n={len(samples)}")


if __name__ == "__main__":
    asyncio.run(main())
