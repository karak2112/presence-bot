from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
from dataclasses import dataclass, field

import aiohttp

from src.config import TWITCH_USER_AGENT
from src.scheduler.slots import WatchSlot
from src.watcher.gql import TwitchGQL

logger = logging.getLogger(__name__)


@dataclass
class WatchSession:
    login: str
    channel_id: str
    broadcast_id: str
    game_name: str = ""
    game_id: str = ""
    spade_url: str = ""
    minutes_sent: int = 0
    last_sent_at: float = 0.0
    last_status: int = 0
    failures: int = 0


@dataclass
class WatcherStats:
    sessions: dict[str, WatchSession] = field(default_factory=dict)
    total_minutes: int = 0
    conflict_detected: bool = False
    last_watchdog_at: float = 0.0


class MinuteWatchedWatcher:
    def __init__(self, gql: TwitchGQL, user_id: str) -> None:
        self.gql = gql
        self.user_id = user_id
        self.stats = WatcherStats()
        self._spade_url: str | None = None
        self._stop = asyncio.Event()

    async def _ensure_spade_url(
        self, session: aiohttp.ClientSession, channel_login: str | None = None
    ) -> str | None:
        if self._spade_url:
            return self._spade_url
        try:
            self._spade_url = await self.gql.get_spade_url(session, channel_login)
            return self._spade_url
        except Exception:
            logger.exception("Failed to fetch spade URL")
            return None

    async def run(self, session: aiohttp.ClientSession, get_slots) -> None:
        while not self._stop.is_set():
            try:
                slots: list[WatchSlot] = get_slots()
                if not slots:
                    await asyncio.sleep(30)
                    continue

                if not self._spade_url:
                    channel = slots[0].login if slots else None
                    if not await self._ensure_spade_url(session, channel):
                        await asyncio.sleep(30)
                        continue

                interval = 60.0 / max(len(slots), 1)
                for slot in slots:
                    try:
                        await self._send_minute(session, slot)
                    except Exception:
                        logger.exception("minute-watched loop error for %s", slot.login)
                    await asyncio.sleep(interval)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Watcher loop error, retrying in 30s")
                await asyncio.sleep(30)

    def stop(self) -> None:
        self._stop.set()

    async def refresh_sessions(self, session: aiohttp.ClientSession, slots: list[WatchSlot]) -> None:
        active_logins = {s.login for s in slots}
        for login in list(self.stats.sessions.keys()):
            if login not in active_logins:
                del self.stats.sessions[login]

        for slot in slots:
            watch_session = self.stats.sessions.get(slot.login)
            if watch_session and watch_session.broadcast_id == slot.stream_id:
                continue
            metadata = await self.gql.get_stream_metadata(session, slot.login)
            if not metadata:
                logger.debug("No stream metadata for %s (may have gone offline)", slot.login)
                continue
            self.stats.sessions[slot.login] = WatchSession(
                login=slot.login,
                channel_id=metadata["channel_id"],
                broadcast_id=metadata["broadcast_id"],
                game_name=metadata.get("game_name", slot.game_name),
                game_id=metadata.get("game_id", ""),
                spade_url=self._spade_url or "",
            )
            logger.info(
                "Watch session ready for %s (broadcast %s)",
                slot.login,
                metadata["broadcast_id"],
            )

    async def _send_minute(self, session: aiohttp.ClientSession, slot: WatchSlot) -> None:
        watch_session = self.stats.sessions.get(slot.login)
        if not watch_session or watch_session.broadcast_id != slot.stream_id:
            await self.refresh_sessions(session, [slot])
            watch_session = self.stats.sessions.get(slot.login)
            if not watch_session:
                return

        payload = self._build_payload(watch_session)
        spade_url = watch_session.spade_url or self._spade_url
        if not spade_url:
            return

        try:
            async with session.post(
                spade_url,
                data=payload,
                headers={"User-Agent": TWITCH_USER_AGENT},
            ) as resp:
                watch_session.last_status = resp.status
                watch_session.last_sent_at = time.time()
                if resp.status == 204:
                    watch_session.minutes_sent += 1
                    watch_session.failures = 0
                    self.stats.total_minutes += 1
                    logger.debug(
                        "minute-watched OK for %s (total %d)",
                        slot.login,
                        watch_session.minutes_sent,
                    )
                else:
                    watch_session.failures += 1
                    body = await resp.text()
                    logger.warning(
                        "minute-watched %s returned %s: %s",
                        slot.login,
                        resp.status,
                        body[:200],
                    )
                    if watch_session.failures >= 3:
                        self.stats.conflict_detected = True
                        logger.warning(
                            "Possible watch conflict for %s — another device may be "
                            "using your 2-stream limit",
                            slot.login,
                        )
        except Exception:
            watch_session.failures += 1
            logger.exception("minute-watched request failed for %s", slot.login)

    def _build_payload(self, watch_session: WatchSession) -> dict[str, str]:
        event_properties: dict[str, str | int] = {
            "channel_id": int(watch_session.channel_id),
            "broadcast_id": watch_session.broadcast_id,
            "player": "site",
            "user_id": int(self.user_id),
        }
        if watch_session.game_name:
            event_properties["game"] = watch_session.game_name
        if watch_session.game_id:
            event_properties["game_id"] = watch_session.game_id

        events = [{"event": "minute-watched", "properties": event_properties}]
        encoded = base64.b64encode(
            json.dumps(events, separators=(",", ":")).encode("utf-8")
        ).decode("utf-8")
        return {"data": encoded}

    async def watchdog(self, session: aiohttp.ClientSession, get_slots) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(600)
            slots = get_slots()
            self.stats.last_watchdog_at = time.time()
            if not slots:
                logger.debug("Watchdog: no live watch slots, skipping refresh")
                continue

            previous = self._spade_url
            try:
                refreshed = await self.gql.get_spade_url(session, slots[0].login)
                self._spade_url = refreshed
                logger.info("Watchdog: refreshed spade URL")
            except Exception:
                self._spade_url = previous
                logger.warning(
                    "Watchdog: spade URL refresh failed; keeping cached URL",
                    exc_info=True,
                )

            await self.refresh_sessions(session, slots)
            logger.info("Watchdog: refreshed %d watch sessions", len(slots))
