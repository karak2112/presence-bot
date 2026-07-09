from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

import aiohttp

from src.config import DROPS_CLAIMED_PATH

if TYPE_CHECKING:
    from src.watcher.gql import TwitchGQL

logger = logging.getLogger(__name__)

ClaimableDrop = tuple[str, str, str]  # campaign_name, drop_name, drop_instance_id


@dataclass
class DropsClaimer:
    gql: "TwitchGQL"
    interval_seconds: int = 180
    state_path: Path = DROPS_CLAIMED_PATH
    claimed_instance_ids: set[str] = field(default_factory=set)
    _stop: asyncio.Event = field(default_factory=asyncio.Event)
    stats: dict[str, Any] = field(
        default_factory=lambda: {
            "last_poll_at": 0.0,
            "last_claim_at": 0.0,
            "total_claimed": 0,
            "last_error": "",
        }
    )

    def load(self) -> None:
        if not self.state_path.exists():
            return
        with open(self.state_path, encoding="utf-8") as f:
            data = json.load(f)
        self.claimed_instance_ids = set(data.get("claimed_instance_ids", []))
        self.stats["total_claimed"] = data.get("total_claimed", len(self.claimed_instance_ids))

    def save(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "claimed_instance_ids": sorted(self.claimed_instance_ids),
                    "total_claimed": self.stats["total_claimed"],
                    "updated_at": time.time(),
                },
                f,
                indent=2,
            )

    async def run(
        self,
        session: aiohttp.ClientSession,
        get_slots: Callable[[], list[Any]],
    ) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self.interval_seconds)
            if not get_slots():
                continue
            try:
                await self.poll_and_claim(session)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Drops claim loop error")

    def stop(self) -> None:
        self._stop.set()

    async def poll_and_claim(self, session: aiohttp.ClientSession) -> None:
        self.stats["last_poll_at"] = time.time()
        if not self.gql.drops_gql_available:
            return

        claimable = await self.gql.find_claimable_drops(session)
        if claimable is None:
            self.stats["last_error"] = "inventory unavailable"
            return

        self.stats["last_error"] = ""
        for campaign_name, drop_name, drop_instance_id in claimable:
            if drop_instance_id in self.claimed_instance_ids:
                continue
            claimed = await self.gql.claim_drop(session, drop_instance_id)
            if claimed:
                self.claimed_instance_ids.add(drop_instance_id)
                self.stats["total_claimed"] += 1
                self.stats["last_claim_at"] = time.time()
                self.save()
                logger.info(
                    "Claimed drop: %s (%s)",
                    drop_name,
                    campaign_name,
                )
            await asyncio.sleep(2)
