from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

from aiohttp import web

from src.config import HEALTH_STATE_PATH

logger = logging.getLogger(__name__)


class HealthServer:
    def __init__(self, port: int, get_status) -> None:
        self.port = port
        self.get_status = get_status
        self._runner: web.AppRunner | None = None
        self._start_time = time.time()

    async def start(self) -> None:
        app = web.Application()
        app.router.add_get("/health", self._health)
        app.router.add_get("/status", self._status)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", self.port)
        await site.start()
        logger.info("Health server listening on :%d", self.port)

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()

    async def _health(self, _request: web.Request) -> web.Response:
        status = self.get_status()
        healthy = status.get("authenticated", False)
        code = 200 if healthy else 503
        return web.json_response(
            {"status": "ok" if healthy else "degraded", "uptime_seconds": int(time.time() - self._start_time)},
            status=code,
        )

    async def _status(self, _request: web.Request) -> web.Response:
        status = self.get_status()
        self._persist(status)
        return web.json_response(status)

    def _persist(self, status: dict[str, Any]) -> None:
        HEALTH_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(HEALTH_STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(status, f, indent=2)
