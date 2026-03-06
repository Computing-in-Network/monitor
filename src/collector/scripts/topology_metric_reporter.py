from __future__ import annotations

import argparse
import asyncio
import csv
import json
import random
import re
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import websockets

try:
    import docker
except Exception:  # pragma: no cover
    docker = None


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
    metric_source: str
    node_mapping_csv: str
    docker_timeout_s: float
    docker_workers: int


class SyntheticMetricState:
    def __init__(self, seed: int) -> None:
        self._rng = random.Random(seed)
        self._node: dict[str, dict[str, float]] = {}
        self._link: dict[str, dict[str, float]] = {}

    async def refresh(self, nodes: list[dict[str, Any]], degree: dict[str, int]) -> None:
        _ = nodes
        _ = degree

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
            "metric_source": "synthetic",
            "status": "UP",
            "docker_name": node_id,
            "cpu_usage_cores": None,
            "cpu_limit_cores": None,
            "mem_usage_bytes": None,
            "mem_limit_bytes": None,
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


@dataclass(frozen=True)
class NodeContainerRef:
    node_id: str
    docker_name: str
    container_ref: str


def _load_node_mapping(node_mapping_csv: str) -> dict[str, dict[str, str]]:
    csv_path = str(node_mapping_csv or "").strip()
    if not csv_path:
        return {}
    path = Path(csv_path)
    if not path.exists():
        raise ValueError(f"node mapping csv not found: {csv_path}")
    with path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        rows = [dict(row) for row in reader]
    if not rows:
        raise ValueError(f"node mapping csv is empty: {csv_path}")
    if "node_id" not in rows[0]:
        raise ValueError("node mapping csv missing required column: node_id")
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        node_id = str(row.get("node_id") or "").strip()
        if not node_id:
            continue
        out[node_id] = {
            "container_name": str(row.get("container_name") or "").strip(),
            "container_id": str(row.get("container_id") or "").strip(),
        }
    return out


def _count_cpuset(cpuset_cpus: str) -> int:
    total = 0
    raw = str(cpuset_cpus or "").strip()
    if not raw:
        return 0
    for chunk in raw.split(","):
        part = chunk.strip()
        if not part:
            continue
        if "-" in part:
            try:
                lo_text, hi_text = part.split("-", 1)
                lo = int(lo_text.strip())
                hi = int(hi_text.strip())
            except ValueError:
                continue
            if hi >= lo:
                total += hi - lo + 1
            continue
        try:
            _ = int(part)
            total += 1
        except ValueError:
            continue
    return total


def _infer_erv300_container_name(node_id: str) -> str:
    node = str(node_id or "").strip().upper()
    if not node:
        return ""
    m = re.fullmatch(r"SAT-POLAR-(\d{3})", node)
    if m:
        idx = int(m.group(1))
        if 1 <= idx <= 100:
            return f"erv300_r_{idx}"
    m = re.fullmatch(r"SAT-INCL-(\d{3})", node)
    if m:
        offset = int(m.group(1))
        if 1 <= offset <= 100:
            return f"erv300_r_{100 + offset}"
    m = re.fullmatch(r"AIR-(\d{3})", node)
    if m:
        offset = int(m.group(1))
        if 1 <= offset <= 50:
            return f"erv300_r_{200 + offset}"
    m = re.fullmatch(r"SHIP-(\d{3})", node)
    if m:
        offset = int(m.group(1))
        if 1 <= offset <= 50:
            return f"erv300_r_{250 + offset}"
    return ""


class DockerMetricState:
    def __init__(
        self,
        *,
        node_mapping: dict[str, dict[str, str]],
        timeout_s: float,
        workers: int,
        seed: int,
    ) -> None:
        if docker is None:  # pragma: no cover
            raise RuntimeError("docker python package is required for docker metric source")
        self._fallback = SyntheticMetricState(seed=seed)
        self._mapping = node_mapping
        self._timeout_s = max(1.0, float(timeout_s))
        self._workers = max(1, int(workers))
        self._samples: dict[str, dict[str, Any]] = {}
        self._inspect_cache: dict[str, dict[str, Any]] = {}
        self._last_net_totals: dict[str, tuple[float, int, int]] = {}
        self._client = docker.from_env(timeout=self._timeout_s)  # type: ignore[union-attr]
        self._api = self._client.api
        self._api.ping()

    async def refresh(self, nodes: list[dict[str, Any]], degree: dict[str, int]) -> None:
        node_ids = [str(node.get("id") or "") for node in nodes if isinstance(node, dict)]
        node_ids = [node_id for node_id in node_ids if node_id]
        if not node_ids:
            self._samples = {}
            return

        docker_metrics = await asyncio.to_thread(self._collect_samples_for_nodes, node_ids)
        samples: dict[str, dict[str, Any]] = {}
        for node_id in node_ids:
            metric = docker_metrics.get(node_id)
            if metric is None:
                fallback = self._fallback.node_metric(node_id=node_id, degree=degree.get(node_id, 0))
                fallback["metric_source"] = "synthetic_fallback"
                fallback["status"] = "DOWN"
                mapping = self._resolve_container_ref(node_id)
                fallback["docker_name"] = mapping.docker_name
                samples[node_id] = fallback
                continue

            metric["conn_count"] = int(max(1, degree.get(node_id, 0)))
            samples[node_id] = metric
        self._samples = samples

    def node_metric(self, node_id: str, degree: int) -> dict[str, Any]:
        metric = self._samples.get(node_id)
        if metric is not None:
            return metric
        fallback = self._fallback.node_metric(node_id=node_id, degree=degree)
        fallback["metric_source"] = "synthetic_fallback"
        fallback["status"] = "DOWN"
        mapping = self._resolve_container_ref(node_id)
        fallback["docker_name"] = mapping.docker_name
        return fallback

    def link_metric(self, a: str, b: str) -> dict[str, float]:
        return self._fallback.link_metric(a, b)

    def _resolve_container_ref(self, node_id: str) -> NodeContainerRef:
        row = self._mapping.get(node_id) or {}
        inferred_name = _infer_erv300_container_name(node_id)
        container_name = str(row.get("container_name") or "").strip() or inferred_name or node_id
        container_id = str(row.get("container_id") or "").strip()
        ref = container_id or container_name
        return NodeContainerRef(node_id=node_id, docker_name=container_name, container_ref=ref)

    def _collect_samples_for_nodes(self, node_ids: list[str]) -> dict[str, dict[str, Any]]:
        refs = [self._resolve_container_ref(node_id) for node_id in node_ids]
        unique_refs = sorted({ref.container_ref for ref in refs if ref.container_ref})
        by_ref: dict[str, dict[str, Any] | None] = {}
        if unique_refs:
            workers = max(1, min(self._workers, len(unique_refs)))
            with ThreadPoolExecutor(max_workers=workers) as pool:
                future_to_ref = {pool.submit(self._sample_container, ref): ref for ref in unique_refs}
                for future in as_completed(future_to_ref):
                    ref = future_to_ref[future]
                    try:
                        by_ref[ref] = future.result()
                    except Exception:
                        by_ref[ref] = None

        out: dict[str, dict[str, Any]] = {}
        for ref in refs:
            sample = by_ref.get(ref.container_ref)
            if sample is None:
                continue
            payload = dict(sample)
            payload["docker_name"] = ref.docker_name or str(sample.get("docker_name") or ref.container_ref)
            out[ref.node_id] = payload
        return out

    def _sample_container(self, ref: str) -> dict[str, Any] | None:
        inspect_data = self._inspect_container(ref)
        if inspect_data is None:
            return None

        raw_stats = self._fetch_stats(ref)
        if raw_stats is None:
            return None

        cpu_stats = raw_stats.get("cpu_stats") or {}
        precpu_stats = raw_stats.get("precpu_stats") or {}
        cpu_usage_stats = cpu_stats.get("cpu_usage") or {}
        precpu_usage_stats = precpu_stats.get("cpu_usage") or {}
        total_usage = float(cpu_usage_stats.get("total_usage") or 0.0)
        pre_total_usage = float(precpu_usage_stats.get("total_usage") or 0.0)
        system_usage = float(cpu_stats.get("system_cpu_usage") or 0.0)
        pre_system_usage = float(precpu_stats.get("system_cpu_usage") or 0.0)
        online_cpus = int(cpu_stats.get("online_cpus") or len(cpu_usage_stats.get("percpu_usage") or []) or 1)
        cpu_delta = max(0.0, total_usage - pre_total_usage)
        system_delta = max(0.0, system_usage - pre_system_usage)
        cpu_usage_cores = 0.0
        if cpu_delta > 0.0 and system_delta > 0.0 and online_cpus > 0:
            cpu_usage_cores = (cpu_delta / system_delta) * float(online_cpus)

        cpu_limit_cores = self._resolve_cpu_limit_cores(inspect_data, online_cpus=online_cpus)
        cpu_ratio = _clamp(cpu_usage_cores / max(cpu_limit_cores, 1e-9), 0.0, 1.0)

        memory_stats = raw_stats.get("memory_stats") or {}
        mem_usage_bytes = int(memory_stats.get("usage") or 0)
        mem_limit_bytes = int(memory_stats.get("limit") or 0)
        mem_ratio = _clamp((float(mem_usage_bytes) / float(mem_limit_bytes)) if mem_limit_bytes > 0 else 0.0, 0.0, 1.0)

        net_rx_total = 0
        net_tx_total = 0
        for item in (raw_stats.get("networks") or {}).values():
            if not isinstance(item, dict):
                continue
            net_rx_total += int(item.get("rx_bytes") or 0)
            net_tx_total += int(item.get("tx_bytes") or 0)

        container_id = str(inspect_data.get("Id") or ref)
        now = time.monotonic()
        rx_bps = 0
        tx_bps = 0
        prev = self._last_net_totals.get(container_id)
        if prev is not None:
            prev_ts, prev_rx, prev_tx = prev
            elapsed = now - prev_ts
            if elapsed > 1e-6:
                rx_bps = int(max(0, net_rx_total - prev_rx) / elapsed)
                tx_bps = int(max(0, net_tx_total - prev_tx) / elapsed)
        self._last_net_totals[container_id] = (now, net_rx_total, net_tx_total)

        docker_name = str(inspect_data.get("Name") or "").lstrip("/") or ref
        return {
            "cpu_ratio": round(cpu_ratio, 4),
            "mem_ratio": round(mem_ratio, 4),
            "tx_bps": int(tx_bps),
            "rx_bps": int(rx_bps),
            "conn_count": 0,
            "status": "UP",
            "docker_name": docker_name,
            "metric_source": "docker",
            "cpu_usage_cores": round(cpu_usage_cores, 6),
            "cpu_limit_cores": round(cpu_limit_cores, 6),
            "mem_usage_bytes": mem_usage_bytes,
            "mem_limit_bytes": mem_limit_bytes,
        }

    def _inspect_container(self, ref: str) -> dict[str, Any] | None:
        cached = self._inspect_cache.get(ref)
        if cached is not None:
            return cached
        try:
            inspect_data = self._api.inspect_container(ref)
        except Exception:
            return None
        if not isinstance(inspect_data, dict):
            return None
        self._inspect_cache[ref] = inspect_data
        container_id = str(inspect_data.get("Id") or "").strip()
        container_name = str(inspect_data.get("Name") or "").strip().lstrip("/")
        if container_id:
            self._inspect_cache[container_id] = inspect_data
        if container_name:
            self._inspect_cache[container_name] = inspect_data
        return inspect_data

    def _fetch_stats(self, ref: str) -> dict[str, Any] | None:
        try:
            raw = self._api.stats(ref, stream=False)
        except TypeError:
            raw = self._api.stats(ref, stream=False, decode=True)
        except Exception:
            return None
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, (bytes, bytearray)):
            raw_text = raw.decode("utf-8", errors="ignore")
        elif isinstance(raw, str):
            raw_text = raw
        else:
            return None
        raw_text = raw_text.strip()
        if not raw_text:
            return None
        try:
            data = json.loads(raw_text)
        except json.JSONDecodeError:
            return None
        return data if isinstance(data, dict) else None

    @staticmethod
    def _resolve_cpu_limit_cores(inspect_data: dict[str, Any], online_cpus: int) -> float:
        host_config = inspect_data.get("HostConfig") or {}
        if not isinstance(host_config, dict):
            return float(max(1, online_cpus))

        nano_cpus = int(host_config.get("NanoCpus") or 0)
        if nano_cpus > 0:
            return max(1e-9, float(nano_cpus) / 1e9)

        cpu_quota = int(host_config.get("CpuQuota") or 0)
        cpu_period = int(host_config.get("CpuPeriod") or 0)
        if cpu_quota > 0 and cpu_period > 0:
            return max(1e-9, float(cpu_quota) / float(cpu_period))

        cpuset = str(host_config.get("CpusetCpus") or "").strip()
        cpuset_count = _count_cpuset(cpuset)
        if cpuset_count > 0:
            return float(cpuset_count)

        return float(max(1, online_cpus))


MetricState = SyntheticMetricState | DockerMetricState


def _build_metric_state(config: ReporterConfig) -> MetricState:
    if config.metric_source != "docker":
        return SyntheticMetricState(seed=config.seed)

    try:
        node_mapping = _load_node_mapping(config.node_mapping_csv)
    except Exception as exc:
        print(f"[reporter] failed to load node mapping csv: {exc}; fallback to synthetic", flush=True)
        return SyntheticMetricState(seed=config.seed)

    try:
        state = DockerMetricState(
            node_mapping=node_mapping,
            timeout_s=config.docker_timeout_s,
            workers=config.docker_workers,
            seed=config.seed,
        )
        print(
            f"[reporter] docker metric source enabled mapping_nodes={len(node_mapping)} "
            f"timeout_s={config.docker_timeout_s:.1f}",
            flush=True,
        )
        return state
    except Exception as exc:
        print(f"[reporter] docker metric source unavailable: {exc}; fallback to synthetic", flush=True)
        return SyntheticMetricState(seed=config.seed)


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
    parser.add_argument("--metric-source", choices=("synthetic", "docker"), default="synthetic")
    parser.add_argument("--node-mapping-csv", default="", help="CSV with node_id->container_name/container_id mapping")
    parser.add_argument("--docker-timeout-s", type=float, default=5.0)
    parser.add_argument("--docker-workers", type=int, default=16)
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
        metric_source=str(args.metric_source).strip().lower(),
        node_mapping_csv=str(args.node_mapping_csv or "").strip(),
        docker_timeout_s=max(1.0, float(args.docker_timeout_s)),
        docker_workers=max(1, int(args.docker_workers)),
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

    await state.refresh(nodes, degree)

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
            "status": str(metric.get("status") or "UP"),
            "docker_name": str(metric.get("docker_name") or node_id),
            "metric_source": str(metric.get("metric_source") or config.metric_source),
            "topology_epoch": config.topology_epoch,
            "timestamp": now_iso,
        }
        for key in ("cpu_usage_cores", "cpu_limit_cores", "mem_usage_bytes", "mem_limit_bytes"):
            value = metric.get(key)
            if value is not None:
                payload[key] = value
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
    state = _build_metric_state(config)
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
        f"interval={config.report_interval_s}s epoch={config.topology_epoch} once={config.once} "
        f"metric_source={config.metric_source}",
        flush=True,
    )
    asyncio.run(run(config))


if __name__ == "__main__":
    main()
