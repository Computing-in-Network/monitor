from __future__ import annotations

import asyncio
import json
from threading import Lock
from typing import Any

from nats.aio.client import Client as NATS


class PublishUnavailableError(RuntimeError):
    pass


class EventPublisher:
    def __init__(self, nats_url: str, retries: int = 2, retry_backoff_ms: int = 200):
        self._url = nats_url
        self._conn = NATS()
        self._retries = max(0, retries)
        self._retry_backoff_ms = max(1, retry_backoff_ms)
        self._lock = Lock()
        self._publish_ok = 0
        self._publish_fail = 0
        self._publish_retry = 0

    async def connect(self) -> None:
        await self._conn.connect(servers=[self._url])

    async def close(self) -> None:
        if self._conn.is_connected:
            await self._conn.close()

    def is_connected(self) -> bool:
        return bool(self._conn.is_connected)

    async def publish(self, subject: str, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        if not self._conn.is_connected:
            with self._lock:
                self._publish_fail += 1
            raise PublishUnavailableError("nats connection is not ready")
        attempts = self._retries + 1
        for attempt in range(attempts):
            try:
                await self._conn.publish(subject=subject, payload=data)
                with self._lock:
                    self._publish_ok += 1
                return
            except Exception as exc:
                if attempt >= attempts - 1:
                    with self._lock:
                        self._publish_fail += 1
                    raise PublishUnavailableError("failed to publish event") from exc
                with self._lock:
                    self._publish_retry += 1
                await asyncio.sleep(self._retry_backoff_ms / 1000.0)

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "publish_ok_total": self._publish_ok,
                "publish_fail_total": self._publish_fail,
                "publish_retry_total": self._publish_retry,
            }
