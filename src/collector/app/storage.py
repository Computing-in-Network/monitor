from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg2


class TimescaleWriteError(RuntimeError):
    pass


@dataclass
class TimescaleWriter:
    dsn: str
    schema: str = "monitor_ts"

    def __post_init__(self) -> None:
        self._conn = None

    def connect(self) -> None:
        self._conn = psycopg2.connect(self.dsn)
        self._conn.autocommit = True

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def is_ready(self) -> bool:
        return self._conn is not None and not self._conn.closed

    def write_event(self, event_type: str, payload: dict[str, Any]) -> None:
        if not self.is_ready():
            raise TimescaleWriteError("timescale connection is not ready")
        if event_type == "node_metric":
            self._insert_node_metric(payload)
            return
        if event_type == "link_metric":
            self._insert_link_metric(payload)
            return
        if event_type == "flow":
            self._insert_flow(payload)
            return
        if event_type == "alarm":
            self._insert_alarm(payload)
            return
        raise TimescaleWriteError(f"unsupported event_type: {event_type}")

    def _ts(self, payload: dict[str, Any]) -> datetime:
        raw = payload.get("timestamp")
        if isinstance(raw, str) and raw:
            try:
                return datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                pass
        return datetime.now(timezone.utc)

    def _insert_node_metric(self, payload: dict[str, Any]) -> None:
        sql = f"""
        INSERT INTO {self.schema}.node_metrics(
          ts, message_id, topology_epoch, node_id, cpu_ratio, mem_ratio, conn_count, tx_bps, rx_bps, status, payload
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
        ON CONFLICT (ts, node_id, message_id) DO NOTHING;
        """
        self._execute(
            sql,
            (
                self._ts(payload),
                payload.get("message_id"),
                payload.get("topology_epoch"),
                payload.get("node_id"),
                payload.get("cpu_ratio"),
                payload.get("mem_ratio"),
                payload.get("conn_count"),
                payload.get("tx_bps"),
                payload.get("rx_bps"),
                payload.get("status"),
                json.dumps(payload, ensure_ascii=False),
            ),
        )

    def _insert_link_metric(self, payload: dict[str, Any]) -> None:
        sql = f"""
        INSERT INTO {self.schema}.link_metrics(
          ts, message_id, topology_epoch, link_id, src_node_id, dst_node_id, state,
          loss_rate, rtt_ms, jitter_ms, ber, snr_db, payload
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
        ON CONFLICT (ts, link_id, message_id) DO NOTHING;
        """
        self._execute(
            sql,
            (
                self._ts(payload),
                payload.get("message_id"),
                payload.get("topology_epoch"),
                payload.get("link_id"),
                payload.get("src_node_id"),
                payload.get("dst_node_id"),
                payload.get("state"),
                payload.get("loss_rate"),
                payload.get("rtt_ms"),
                payload.get("jitter_ms"),
                payload.get("ber"),
                payload.get("snr_db"),
                json.dumps(payload, ensure_ascii=False),
            ),
        )

    def _insert_flow(self, payload: dict[str, Any]) -> None:
        sql = f"""
        INSERT INTO {self.schema}.flows(
          ts, message_id, topology_epoch, flow_id, src_node_id, dst_node_id,
          bps, pkt_rate, priority, path, payload
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb)
        ON CONFLICT (ts, flow_id, message_id) DO NOTHING;
        """
        self._execute(
            sql,
            (
                self._ts(payload),
                payload.get("message_id"),
                payload.get("topology_epoch"),
                payload.get("flow_id"),
                payload.get("src_node_id"),
                payload.get("dst_node_id"),
                payload.get("bps"),
                payload.get("pkt_rate"),
                payload.get("priority"),
                json.dumps(payload.get("path"), ensure_ascii=False),
                json.dumps(payload, ensure_ascii=False),
            ),
        )

    def _insert_alarm(self, payload: dict[str, Any]) -> None:
        sql = f"""
        INSERT INTO {self.schema}.alarms(
          ts, message_id, topology_epoch, alarm_id, severity, scope_type, scope_id,
          title, detail, fingerprint, payload
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
        ON CONFLICT (ts, alarm_id, message_id) DO NOTHING;
        """
        self._execute(
            sql,
            (
                self._ts(payload),
                payload.get("message_id"),
                payload.get("topology_epoch"),
                payload.get("alarm_id"),
                payload.get("severity"),
                payload.get("scope_type"),
                payload.get("scope_id"),
                payload.get("title"),
                payload.get("detail"),
                payload.get("fingerprint"),
                json.dumps(payload, ensure_ascii=False),
            ),
        )

    def _execute(self, sql: str, params: tuple[Any, ...]) -> None:
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql, params)
        except Exception as exc:  # noqa: BLE001
            raise TimescaleWriteError(str(exc)) from exc

    def read_metric_series(
        self,
        *,
        event_type: str,
        metric: str,
        entity_id: str,
        limit: int = 120,
        topology_epoch: str | None = None,
    ) -> list[dict[str, Any]]:
        if not self.is_ready():
            raise TimescaleWriteError("timescale connection is not ready")

        table, id_column, metric_column = self._resolve_series_mapping(event_type=event_type, metric=metric)
        safe_limit = max(1, min(int(limit), 2000))

        sql = f"""
        SELECT ts, {metric_column}
        FROM {self.schema}.{table}
        WHERE {id_column} = %s
        """
        params: list[Any] = [entity_id]
        if topology_epoch not in (None, ""):
            sql += " AND topology_epoch = %s"
            params.append(topology_epoch)
        sql += " ORDER BY ts DESC LIMIT %s"
        params.append(safe_limit)

        rows = self._query(sql, tuple(params))
        points = [{"ts": row[0].isoformat(), "value": float(row[1])} for row in rows if row[1] is not None]
        points.reverse()
        return points

    @staticmethod
    def _resolve_series_mapping(*, event_type: str, metric: str) -> tuple[str, str, str]:
        mapping = {
            "node_metric": {
                "table": "node_metrics",
                "id": "node_id",
                "metrics": {"cpu_ratio", "mem_ratio", "tx_bps", "rx_bps", "conn_count"},
            },
            "link_metric": {
                "table": "link_metrics",
                "id": "link_id",
                "metrics": {"loss_rate", "rtt_ms", "jitter_ms", "snr_db", "ber"},
            },
            "flow": {
                "table": "flows",
                "id": "flow_id",
                "metrics": {"bps", "pkt_rate"},
            },
        }
        info = mapping.get(event_type)
        if not info:
            raise TimescaleWriteError(f"unsupported event_type for query: {event_type}")
        if metric not in info["metrics"]:
            raise TimescaleWriteError(f"unsupported metric={metric} for event_type={event_type}")
        return str(info["table"]), str(info["id"]), metric

    def _query(self, sql: str, params: Sequence[Any]) -> list[tuple[Any, ...]]:
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql, params)
                return list(cur.fetchall())
        except Exception as exc:  # noqa: BLE001
            raise TimescaleWriteError(str(exc)) from exc
