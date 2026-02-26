from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any


def _build_graph(links: list[dict[str, Any]]) -> dict[str, set[str]]:
    graph: dict[str, set[str]] = defaultdict(set)
    for link in links:
        src = str(link.get("src") or link.get("src_node") or "").strip()
        dst = str(link.get("dst") or link.get("dst_node") or "").strip()
        if not src or not dst:
            continue
        graph[src].add(dst)
        graph[dst].add(src)
    return graph


def _bfs_layers(graph: dict[str, set[str]], seeds: list[str], max_depth: int) -> dict[str, int]:
    depth_map: dict[str, int] = {}
    queue: deque[tuple[str, int]] = deque()
    for seed in seeds:
        if seed in graph:
            depth_map[seed] = 0
            queue.append((seed, 0))
    while queue:
        node, depth = queue.popleft()
        if depth >= max_depth:
            continue
        for nxt in graph.get(node, set()):
            if nxt in depth_map:
                continue
            depth_map[nxt] = depth + 1
            queue.append((nxt, depth + 1))
    return depth_map


def _norm_link_uid(src: str, dst: str) -> str:
    a = str(src or "").strip()
    b = str(dst or "").strip()
    if not a or not b:
        return ""
    return "<->".join(sorted([a, b]))


def _impacted_edges(graph: dict[str, set[str]], impacted_nodes: set[str]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for src in impacted_nodes:
        for dst in graph.get(src, set()):
            if dst not in impacted_nodes:
                continue
            key = tuple(sorted([src, dst]))
            if key in seen:
                continue
            seen.add(key)
            out.append({"src": key[0], "dst": key[1], "uid": _norm_link_uid(key[0], key[1])})
    return out


@dataclass
class AnalyzeRequest:
    alarm_nodes: list[str]
    links: list[dict[str, Any]]
    max_depth: int = 3
    mode: str = "single_point"
    cascade_threshold: float = 0.6


class SpreadAnalyzer:
    def analyze(self, req: AnalyzeRequest) -> dict[str, Any]:
        graph = _build_graph(req.links)
        seeds = [str(x) for x in req.alarm_nodes if str(x).strip()]
        if not seeds:
            return {
                "mode": "single_point",
                "seeds": [],
                "core_nodes": [],
                "boundary_nodes": [],
                "unaffected_nodes": sorted(graph.keys()),
                "impacted_nodes": [],
                "impacted_links": [],
                "subgraph": {"nodes": [], "edges": []},
                "paths": [],
                "fallback": True,
            }

        depth_map = _bfs_layers(graph, seeds, max_depth=req.max_depth)
        impacted_nodes = set(depth_map.keys())
        if req.mode == "cascade":
            extended = set(impacted_nodes)
            for link in req.links:
                src = str(link.get("src") or link.get("src_node") or "")
                dst = str(link.get("dst") or link.get("dst_node") or "")
                health = float(link.get("health", 1.0))
                if health < req.cascade_threshold and (src in impacted_nodes or dst in impacted_nodes):
                    extended.add(src)
                    extended.add(dst)
            impacted_nodes = extended

        core_nodes = {n for n, depth in depth_map.items() if depth <= 1}
        boundary_nodes = {n for n, depth in depth_map.items() if depth == req.max_depth}
        all_nodes = set(graph.keys())
        edges = _impacted_edges(graph, impacted_nodes)
        paths = [{"node": node, "depth": depth_map.get(node, req.max_depth + 1)} for node in sorted(impacted_nodes)]
        return {
            "mode": req.mode,
            "seeds": seeds,
            "core_nodes": sorted(core_nodes),
            "boundary_nodes": sorted(boundary_nodes),
            "unaffected_nodes": sorted(all_nodes - impacted_nodes),
            "impacted_nodes": sorted(impacted_nodes),
            "impacted_links": [str(edge.get("uid")) for edge in edges if edge.get("uid")],
            "subgraph": {"nodes": sorted(impacted_nodes), "edges": edges},
            "paths": paths,
            "fallback": False,
        }
