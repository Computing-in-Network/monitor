from __future__ import annotations

import argparse
import asyncio
import json
import random
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import websockets


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _link_uid(a: str, b: str) -> str:
    x, y = sorted([str(a), str(b)])
    return f"{x}<->{y}"


@dataclass
class ReporterConfig:
    topo_ws_url: str
    collector_url: str
    api_token: str
    topology_epoch: str
    report_interval_s: float
    timeout_s: float
    max_concurrency: int
    once: bool
    seed: int


class MetricState:
    def __init__(self, seed: int) -> None:
        self._rng = random.Random(seed)
        self._node: dict[str, dict[str, float]] = {}
        self._link: dict[str, dict[str, float]] = {}

    def node_metric(self, node_id: str, degree: int) -> dict[str, float]:
        item = self._node.get(node_id)
        if item is None:
            item = {
                "cpu_ratio": self._rng.uniform(0.20, 0.65),
                "mem_ratio": self._rng.uniform(0.25, 0.70),
                "tx_bps": self._rng.uniform(2e6, 20e6),
                "rx_bps": self._rng.uniform(2e6, 20e6),
                "conn_count": float(max(1, degree)),
            }
            self._node[node_id] = item

        load_factor = 1.0 + min(1.5, degree / 8.0)
        item["cpu_ratio"] = _clamp(item["cpu_ratio"] + self._rng.uniform(-0.03, 0.05), 0.05, 0.98)
        item["mem_ratio"] = _clamp(item["mem_ratio"] + self._rng.uniform(-0.02, 0.03), 0.08, 0.99)
        item["tx_bps"] = _clamp(item["tx_bps"] * self._rng.uniform(0.88, 1.12) * load_factor, 3e5, 3e8)
        item["rx_bps"] = _clamp(item["rx_bps"] * self._rng.uniform(0.88, 1.12) * load_factor, 3e5, 3e8)
        item["conn_count"] = float(max(1, degree))

        return {
            "cpu_ratio": round(item["cpu_ratio"], 3),
            "mem_ratio": round(item["mem_ratio"], 3),
            "tx_bps": int(item["tx_bps"]),
            "rx_bps": int(item["rx_bps"]),
            "conn_count": int(item["conn_count"]),
        }

    def link_metric(self, a: str, b: str) -> dict[str, float]:
        key = _link_uid(a, b)
        item = self._link.get(key)
        if item is None:
            item = {
                "rtt_ms": self._rng.uniform(12, 75),
                "jitter_ms": self._rng.uniform(0.8, 7.0),
                "loss_rate": self._rng.uniform(0.0003, 0.012),
                "snr_db": self._rng.uniform(10, 28),
                "ber": self._rng.uniform(1e-8, 8e-6),
            }
            self._link[key] = item

        item["rtt_ms"] = _clamp(item["rtt_ms"] + self._rng.uniform(-2.0, 2.0), 5.0, 300.0)
        item["jitter_ms"] = _clamp(item["jitter_ms"] + self._rng.uniform(-0.8, 0.8), 0.1, 80.0)
        item["loss_rate"] = _clamp(item["loss_rate"] + self._rng.uniform(-0.0015, 0.0015), 0.0, 0.25)
        item["snr_db"] = _clamp(item["snr_db"] + self._rng.uniform(-1.2, 1.2), 0.0, 50.0)
        item["ber"] = _clamp(item["ber"] * self._rng.uniform(0.7, 1.3), 1e-9, 1e-2)

        return {
            "rtt_ms": round(item["rtt_ms"], 2),
            "jitter_ms": round(item["jitter_ms"], 2),
            "loss_rate": round(item["loss_rate"], 4),
            "snr_db": round(item["snr_db"], 2),
            "ber": round(item["ber"], 8),
        }


def _parse_args() -> ReporterConfig:
    parser = argparse.ArgumentParser(description="Continuously report metrics from topology WS to collector")
    parser.add_argument("--topo-ws-url", default="ws://127.0.0.1:8765")
    parser.add_argument("--collector-url", default="http://127.0.0.1:9010")
    parser.add_argument("--api-token", default="change-me")
    parser.add_argument("--topology-epoch", default="default")
    parser.add_argument("--report-interval-s", type=float, default=2.0)
    parser.add_argument("--timeout-s", type=float, default=5.0)
    parser.add_argument("--max-concurrency", type=int, default=64)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--once", action="store_true", help="Report only once then exit")
    args = parser.parse_args()

    return ReporterConfig(
        topo_ws_url=str(args.topo_ws_url).strip(),
        collector_url=str(args.collector_url).rstrip("/"),
        api_token=str(args.api_token),
        topology_epoch=str(args.topology_epoch),
        report_interval_s=max(0.5, float(args.report_interval_s)),
        timeout_s=max(1.0, float(args.timeout_s)),
        max_concurrency=max(1, int(args.max_concurrency)),
        once=bool(args.once),
        seed=int(args.seed),
    )


async def _post_json(url: str, token: str, payload: dict[str, Any], timeout_s: float) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    def _send() -> None:
        request = urllib.request.Request(
            url=url,
            data=body,
            method="POST",
            headers={
                "content-type": "application/json",
                "x-api-token": token,
            },
        )
        with urllib.request.urlopen(request, timeout=timeout_s) as resp:
            if int(resp.status) >= 300:
                raise RuntimeError(f"HTTP {resp.status}")

    await asyncio.to_thread(_send)


async def _publish_frame(frame: dict[str, Any], config: ReporterConfig, state: MetricState, tick_id: int) -> tuple[int, int]:
    nodes = frame.get("nodes") or []
    links = frame.get("links") or []
    if not isinstance(nodes, list) or not isinstance(links, list):
        return 0, 0

    degree: dict[str, int] = {}
    for link in links:
        if not isinstance(link, dict):
            continue
        a = str(link.get("a") or "")
        b = str(link.get("b") or "")
        if not a or not b:
            continue
        degree[a] = degree.get(a, 0) + 1
        degree[b] = degree.get(b, 0) + 1

    node_url = f"{config.collector_url}/api/v1/ingest/node_metric"
    link_url = f"{config.collector_url}/api/v1/ingest/link_metric"
    now_iso = _now_iso()
    semaphore = asyncio.Semaphore(config.max_concurrency)

    ok_node = 0
    ok_link = 0

    async def _wrap(url: str, payload: dict[str, Any], is_node: bool) -> None:
        nonlocal ok_node, ok_link
        async with semaphore:
            try:
                await _post_json(url, config.api_token, payload, config.timeout_s)
                if is_node:
                    ok_node += 1
                else:
                    ok_link += 1
            except (urllib.error.URLError, TimeoutError, RuntimeError, OSError):
                return

    tasks: list[asyncio.Task[None]] = []
    for i, node in enumerate(nodes, start=1):
        if not isinstance(node, dict):
            continue
        node_id = str(node.get("id") or "")
        if not node_id:
            continue
        metric = state.node_metric(node_id=node_id, degree=degree.get(node_id, 0))
        payload = {
            "schema_version": "monitor.v1",
            "message_id": f"auto-node-{tick_id}-{i}",
            "node_id": node_id,
            "cpu_ratio": metric["cpu_ratio"],
            "mem_ratio": metric["mem_ratio"],
            "tx_bps": metric["tx_bps"],
            "rx_bps": metric["rx_bps"],
            "conn_count": metric["conn_count"],
            "status": "UP",
            "topology_epoch": config.topology_epoch,
            "timestamp": now_iso,
        }
        tasks.append(asyncio.create_task(_wrap(node_url, payload, True)))

    for i, link in enumerate(links, start=1):
        if not isinstance(link, dict):
            continue
        a = str(link.get("a") or "")
        b = str(link.get("b") or "")
        if not a or not b:
            continue
        metric = state.link_metric(a, b)
        payload = {
            "schema_version": "monitor.v1",
            "message_id": f"auto-link-{tick_id}-{i}",
            "link_id": f"{a}-{b}",
            "src_node_id": a,
            "dst_node_id": b,
            "state": "UP",
            "loss_rate": metric["loss_rate"],
            "rtt_ms": metric["rtt_ms"],
            "jitter_ms": metric["jitter_ms"],
            "snr_db": metric["snr_db"],
            "ber": metric["ber"],
            "topology_epoch": config.topology_epoch,
            "timestamp": now_iso,
        }
        tasks.append(asyncio.create_task(_wrap(link_url, payload, False)))

    if tasks:
        await asyncio.gather(*tasks)
    return ok_node, ok_link


async def run(config: ReporterConfig) -> None:
    state = MetricState(seed=config.seed)
    reconnect_s = 1.0
    tick_id = int(time.time() * 1000)

    while True:
        try:
            async with websockets.connect(config.topo_ws_url, max_size=10_000_000) as ws:
                print(f"[reporter] connected to topology ws: {config.topo_ws_url}", flush=True)
                reconnect_s = 1.0
                latest_frame: dict[str, Any] | None = None
                receiver_done = asyncio.Event()

                async def _receiver() -> None:
                    nonlocal latest_frame
                    async for raw in ws:
                        payload = json.loads(raw)
                        if not isinstance(payload, dict):
                            continue
                        if payload.get("type") == "control_ack":
                            continue
                        latest_frame = payload
                    receiver_done.set()

                recv_task = asyncio.create_task(_receiver())
                try:
                    if config.once:
                        while latest_frame is None and not receiver_done.is_set():
                            await asyncio.sleep(0.05)
                        if latest_frame is not None:
                            tick_id += 1
                            ok_node, ok_link = await _publish_frame(latest_frame, config, state, tick_id=tick_id)
                            print(
                                f"[reporter] tick={tick_id} nodes_ok={ok_node} links_ok={ok_link} "
                                f"epoch={config.topology_epoch}",
                                flush=True,
                            )
                        return

                    next_emit = time.monotonic()
                    while not receiver_done.is_set():
                        if latest_frame is None:
                            await asyncio.sleep(0.05)
                            continue

                        now = time.monotonic()
                        if now < next_emit:
                            await asyncio.sleep(min(0.2, next_emit - now))
                            continue

                        tick_id += 1
                        ok_node, ok_link = await _publish_frame(latest_frame, config, state, tick_id=tick_id)
                        print(
                            f"[reporter] tick={tick_id} nodes_ok={ok_node} links_ok={ok_link} "
                            f"epoch={config.topology_epoch}",
                            flush=True,
                        )
                        next_emit = time.monotonic() + config.report_interval_s
                finally:
                    recv_task.cancel()
                    await asyncio.gather(recv_task, return_exceptions=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"[reporter] connection error: {exc}; reconnect in {reconnect_s:.1f}s", flush=True)
            await asyncio.sleep(reconnect_s)
            reconnect_s = min(10.0, reconnect_s * 1.8)


def main() -> None:
    config = _parse_args()
    print(
        "[reporter] starting "
        f"topo_ws={config.topo_ws_url} collector={config.collector_url} "
        f"interval={config.report_interval_s}s epoch={config.topology_epoch} once={config.once}",
        flush=True,
    )
    asyncio.run(run(config))


if __name__ == "__main__":
    main()
