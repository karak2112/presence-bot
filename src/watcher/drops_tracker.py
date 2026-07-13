from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

import aiohttp

if TYPE_CHECKING:
    from src.watcher.gql import TwitchGQL

logger = logging.getLogger(__name__)


@dataclass
class DropsTracker:
    gql: "TwitchGQL"
    interval_seconds: int = 180
    _stop: asyncio.Event = field(default_factory=asyncio.Event)
    _last_progress_key: str = ""
    stats: dict[str, Any] = field(
        default_factory=lambda: {
            "last_poll_at": 0.0,
            "last_error": "",
            "campaigns_in_progress": [],
            "ready_to_claim_count": 0,
        }
    )

    async def run(
        self,
        session: aiohttp.ClientSession,
        get_slots: Callable[[], list[Any]],
    ) -> None:
        while not self._stop.is_set():
            if get_slots():
                try:
                    await self.poll_inventory(session)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Drops tracker error")
            await asyncio.sleep(self.interval_seconds)

    def stop(self) -> None:
        self._stop.set()

    def _log_drop_progress(self, campaigns: list[dict[str, Any]]) -> None:
        if not campaigns:
            self._last_progress_key = ""
            return

        progress_key = "|".join(
            f"{c['drop']}:{c['minutes_watched']}/{c['minutes_required']}"
            f":{'claimed' if c['is_claimed'] else 'open'}"
            for c in campaigns
        )
        if progress_key == self._last_progress_key:
            return
        self._last_progress_key = progress_key

        for campaign in campaigns:
            status = "claimed" if campaign["is_claimed"] else "in progress"
            if campaign["ready_to_claim"]:
                status = "ready to claim"
            logger.info(
                "Drop %s / %s: %d/%d min (%s)",
                campaign["campaign"],
                campaign["drop"],
                campaign["minutes_watched"],
                campaign["minutes_required"],
                status,
            )

    async def poll_inventory(self, session: aiohttp.ClientSession) -> None:
        self.stats["last_poll_at"] = time.time()
        if not self.gql.drops_gql_available:
            if not self.stats["last_error"]:
                self.stats["last_error"] = "web auth required"
            return

        inventory = await self.gql.get_inventory(session)
        if inventory is None:
            self.stats["last_error"] = "inventory unavailable"
            self.stats["campaigns_in_progress"] = []
            self.stats["ready_to_claim_count"] = 0
            return

        campaigns = self.gql.summarize_drop_progress(inventory)
        self.stats["campaigns_in_progress"] = campaigns
        self.stats["ready_to_claim_count"] = sum(
            1 for campaign in campaigns if campaign["ready_to_claim"]
        )
        self.stats["last_error"] = ""
        self._log_drop_progress(campaigns)
