from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiohttp
from websockets.asyncio.client import connect

from src.config import POINTS_STATE_PATH

if TYPE_CHECKING:
    from src.auth.twitch_auth import TwitchAuth
    from src.watcher.gql import TwitchGQL

logger = logging.getLogger(__name__)

PUBSUB_URL = "wss://pubsub-edge.twitch.tv/v1"

# Twitch watch streak bonus amounts → streak level
STREAK_POINTS_TO_LEVEL = {300: 2, 350: 3, 400: 4, 450: 5}


@dataclass
class ChannelPointsState:
    balance: int = 0
    watch_streak: int = 0
    last_broadcast_id: str = ""
    updated_at: float = 0.0


@dataclass
class PointsTracker:
    auth: "TwitchAuth"
    gql: "TwitchGQL"
    login_to_channel_id: dict[str, str]
    channel_id_to_login: dict[str, str]
    interval_seconds: int = 900
    state_path: Path = POINTS_STATE_PATH
    states: dict[str, ChannelPointsState] = field(default_factory=dict)
    _stop: asyncio.Event = field(default_factory=asyncio.Event)
    _logged_broadcasts: set[str] = field(default_factory=set)

    def load(self) -> None:
        if not self.state_path.exists():
            return
        with open(self.state_path, encoding="utf-8") as f:
            raw = json.load(f)
        for login, data in raw.items():
            self.states[login.lower()] = ChannelPointsState(
                balance=data.get("balance", 0),
                watch_streak=data.get("watch_streak", 0),
                last_broadcast_id=data.get("last_broadcast_id", ""),
                updated_at=data.get("updated_at", 0),
            )

    def save(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            login: {
                "balance": state.balance,
                "watch_streak": state.watch_streak,
                "last_broadcast_id": state.last_broadcast_id,
                "updated_at": state.updated_at,
            }
            for login, state in self.states.items()
        }
        with open(self.state_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

    def _state(self, login: str) -> ChannelPointsState:
        login = login.lower()
        if login not in self.states:
            self.states[login] = ChannelPointsState()
        return self.states[login]

    def _format_streak(self, login: str) -> str:
        state = self._state(login)
        if state.watch_streak > 0:
            return f"{state.watch_streak} (from last earned bonus)"
        return (
            "unknown — Twitch does not expose current streak via API; "
            "updates when streak bonus is earned (~10 min in)"
        )

    def _balance_source(self) -> str:
        if self.gql.channel_points_gql_available:
            return ""
        return " (PubSub)"

    async def fetch_balance(self, session: aiohttp.ClientSession, login: str) -> int | None:
        if not self.gql.channel_points_gql_available:
            state = self._state(login)
            return state.balance if state.balance > 0 else None

        community_points = await self.gql.get_channel_points_context(session, login)
        if not community_points:
            return None

        balance = community_points.get("balance")
        if balance is None:
            return None

        state = self._state(login)
        state.balance = balance
        state.updated_at = time.time()
        self.save()
        return balance

    async def log_stream_start(
        self, session: aiohttp.ClientSession, login: str, broadcast_id: str
    ) -> None:
        key = f"{login.lower()}:{broadcast_id}"
        if key in self._logged_broadcasts:
            return

        state = self._state(login)
        if state.last_broadcast_id == broadcast_id:
            self._logged_broadcasts.add(key)
            return

        self._logged_broadcasts.add(key)
        balance = await self.fetch_balance(session, login)
        state.last_broadcast_id = broadcast_id
        self.save()

        if balance is not None:
            balance_text = str(balance)
        elif state.balance > 0:
            balance_text = f"{state.balance}{self._balance_source()}"
        else:
            balance_text = "unknown (awaiting PubSub points event)"

        logger.info(
            "Stream start %s: %s channel points, watch streak %s",
            login,
            balance_text,
            self._format_streak(login),
        )

    def _update_from_pubsub(self, message_data: dict[str, Any]) -> None:
        channel_id = str(message_data.get("channel_id", ""))
        login = self.channel_id_to_login.get(channel_id)
        if not login:
            return

        state = self._state(login)
        balance = message_data.get("balance", {}).get("balance")
        if balance is not None:
            state.balance = balance

        point_gain = message_data.get("point_gain", {})
        reason = point_gain.get("reason_code", "")
        if reason == "WATCH_STREAK":
            gained = point_gain.get("total_points", 0)
            if gained >= 450:
                state.watch_streak = max(state.watch_streak, 5)
            else:
                state.watch_streak = STREAK_POINTS_TO_LEVEL.get(
                    gained, state.watch_streak + 1
                )
            logger.info(
                "Watch streak earned for %s: level %d (+%d points, balance %d)",
                login,
                state.watch_streak,
                gained,
                state.balance,
            )
        elif reason:
            logger.info(
                "Points earned for %s: +%s (%s), balance %d",
                login,
                point_gain.get("total_points", 0),
                reason,
                state.balance,
            )

        state.updated_at = time.time()
        self.save()

    async def periodic_log(self, session: aiohttp.ClientSession, get_slots) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self.interval_seconds)
            slots = get_slots()
            if not slots:
                continue
            for slot in slots:
                state = self._state(slot.login)
                balance = await self.fetch_balance(session, slot.login)
                source = self._balance_source()
                if balance is not None and balance > 0:
                    logger.info(
                        "Points %s: %d balance%s, watch streak %s",
                        slot.login,
                        balance,
                        source,
                        self._format_streak(slot.login),
                    )
                elif state.balance > 0:
                    logger.info(
                        "Points %s: %d balance%s, watch streak %s",
                        slot.login,
                        state.balance,
                        source,
                        self._format_streak(slot.login),
                    )

    async def run_pubsub(self) -> None:
        user_id = self.auth.user_id
        topic = f"community-points-user-v1.{user_id}"

        while not self._stop.is_set():
            try:
                token = self.auth.access_token
                async with connect(PUBSUB_URL, ping_interval=None) as ws:
                    await ws.send(
                        json.dumps(
                            {
                                "type": "LISTEN",
                                "data": {"topics": [topic], "auth_token": token},
                            }
                        )
                    )
                    logger.info("PubSub listening for channel points events")

                    last_ping = time.time()
                    async for raw in ws:
                        if time.time() - last_ping >= 30:
                            await ws.send(json.dumps({"type": "PING"}))
                            last_ping = time.time()

                        message = json.loads(raw)
                        if message.get("type") == "MESSAGE":
                            data = json.loads(message["data"]["message"])
                            if data.get("type") == "points-earned":
                                self._update_from_pubsub(data.get("data", {}))
                        elif message.get("type") == "RECONNECT":
                            break
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("PubSub points connection error, reconnecting in 10s")
                await asyncio.sleep(10)

    def stop(self) -> None:
        self._stop.set()
