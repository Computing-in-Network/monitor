from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.realtime_alarm import RealtimeAlarmEngine


def main() -> None:
    engine = RealtimeAlarmEngine()

    node_alarm = engine.evaluate_metric(
        "node_metric",
        {
            "node_uid": "SAT-INCL-001",
            "topology_epoch": "1708848000",
            "status": "UP",
            "cpu_ratio": 0.96,
            "mem_ratio": 0.52,
        },
    )
    assert len(node_alarm) == 1
    assert node_alarm[0]["lifecycle_state"] == "active"

    node_recover = engine.evaluate_metric(
        "node_metric",
        {
            "node_uid": "SAT-INCL-001",
            "topology_epoch": "1708848000",
            "status": "UP",
            "cpu_ratio": 0.31,
            "mem_ratio": 0.44,
        },
    )
    assert len(node_recover) == 1
    assert node_recover[0]["lifecycle_state"] == "recovered"

    link_alarm = engine.evaluate_metric(
        "link_metric",
        {
            "link_uid": "SAT-POLAR-001<->SAT-POLAR-002",
            "topology_epoch": "1708848000",
            "state": "UP",
            "loss_rate": 0.08,
            "rtt_ms": 120,
            "jitter_ms": 2,
        },
    )
    assert len(link_alarm) == 1
    assert link_alarm[0]["scope_type"] == "link"
    assert link_alarm[0]["lifecycle_state"] == "active"

    link_recover = engine.evaluate_metric(
        "link_metric",
        {
            "link_uid": "SAT-POLAR-001<->SAT-POLAR-002",
            "topology_epoch": "1708848000",
            "state": "UP",
            "loss_rate": 0.0,
            "rtt_ms": 20,
            "jitter_ms": 1,
        },
    )
    assert len(link_recover) == 1
    assert link_recover[0]["lifecycle_state"] == "recovered"

    print("realtime_alarm_engine_test_ok")


if __name__ == "__main__":
    main()
