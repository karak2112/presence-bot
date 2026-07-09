from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

import aiohttp
from websockets.asyncio.client import connect

logger = logging.getLogger(__name__)

EVENTSUB_WS_URL = "wss://eventsub.wss.twitch.tv/ws"
HELIX_STREAMS_URL = "https://api.twitch.tv/helix/streams"

EventCallback = Callable[[str, dict[str, Any]], Awaitable[None]]


@dataclass
class LiveStream:
    login: str
    user_id: str
    stream_id: str
    title: str = ""
    game_name: str = ""
    viewer_count: int = 0
    display_name: str = ""


@dataclass
class StreamState:
    live: dict[str, LiveStream] = field(default_factory=dict)
    _callbacks: list[EventCallback] = field(default_factory=list)

    def on_event(self, callback: EventCallback) -> None:
        self._callbacks.append(callback)

    async def emit(self, event_type: str, payload: dict[str, Any]) -> None:
        for cb in self._callbacks:
            try:
                await cb(event_type, payload)
            except Exception:
                logger.exception("Event callback failed for %s", event_type)


class EventSubListener:
    # Twitch allows 10 subscription points per user for WebSocket transport.
    # stream.online + stream.offline = 2 per streamer → max 5 with both events.
    # For more streamers, use stream.online only for the top 10; poller covers offline.
    # Raids are detected via IRC USERNOTICE instead (see irc_manager.py).
    SUBSCRIPTION_VERSIONS = {
        "stream.online": "1",
        "stream.offline": "1",
    }
    CONDITION_FIELDS = {
        "stream.online": "broadcaster_user_id",
        "stream.offline": "broadcaster_user_id",
    }
    SUBSCRIBE_DELAY_SECONDS = 0.15

    def __init__(
        self,
        client_id: str,
        auth,
        broadcaster_ids: dict[str, str],
        login_by_id: dict[str, str],
        state: StreamState,
        subscription_plan: list[tuple[str, str, str]],
    ) -> None:
        self.client_id = client_id
        self.auth = auth
        self.broadcaster_ids = broadcaster_ids
        self.login_by_id = login_by_id
        self.state = state
        self.subscription_plan = subscription_plan
        self._session_id: str | None = None
        self._stop = asyncio.Event()

    async def run(self, session: aiohttp.ClientSession) -> None:
        while not self._stop.is_set():
            try:
                await self._connect_and_listen(session)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("EventSub connection error, reconnecting in 5s")
                await asyncio.sleep(5)

    def stop(self) -> None:
        self._stop.set()

    async def _connect_and_listen(self, session: aiohttp.ClientSession) -> None:
        async with connect(EVENTSUB_WS_URL, ping_interval=20, ping_timeout=20) as ws:
            logger.info("Connected to EventSub WebSocket")
            async for raw in ws:
                message = json.loads(raw)
                msg_type = message.get("metadata", {}).get("message_type")

                if msg_type == "session_welcome":
                    self._session_id = message["payload"]["session"]["id"]
                    await self._subscribe_all(session)
                elif msg_type == "session_reconnect":
                    reconnect_url = message["payload"]["session"]["reconnect_url"]
                    logger.info("EventSub reconnect requested: %s", reconnect_url)
                    return
                elif msg_type == "session_keepalive":
                    continue
                elif msg_type == "notification":
                    await self._handle_notification(message)
                elif msg_type == "revocation":
                    logger.warning("EventSub subscription revoked: %s", message)

    async def _subscribe_all(self, session: aiohttp.ClientSession) -> None:
        if not self._session_id or not self.subscription_plan:
            return

        online = [login for event, login, _ in self.subscription_plan if event == "stream.online"]
        offline = [login for event, login, _ in self.subscription_plan if event == "stream.offline"]
        if offline:
            logger.info(
                "EventSub: subscribing stream.online + stream.offline for %s",
                ", ".join(online),
            )
        else:
            poll_only = [
                login
                for login in self.broadcaster_ids
                if login not in online
            ]
            logger.info(
                "EventSub: subscribing stream.online for %s",
                ", ".join(online),
            )
            if poll_only:
                logger.info(
                    "EventSub: %d streamer(s) on poll-only (go-live + offline via Helix): %s",
                    len(poll_only),
                    ", ".join(poll_only),
                )

        success_count = 0
        for attempt in range(2):
            if attempt > 0 and self.auth.tokens:
                self.auth.tokens.expires_at = 0
            tokens = await self.auth.ensure_valid(session)
            token = tokens.access_token
            success_count = 0
            had_auth_error = False
            for event_type, login, user_id in self.subscription_plan:
                condition_field = self.CONDITION_FIELDS[event_type]
                version = self.SUBSCRIPTION_VERSIONS[event_type]
                condition = {condition_field: user_id}
                result = await self._create_subscription(
                    session, token, event_type, version, condition, login
                )
                await asyncio.sleep(self.SUBSCRIBE_DELAY_SECONDS)
                if result is True:
                    success_count += 1
                elif result is None:
                    had_auth_error = True
                elif result == "budget_exhausted":
                    logger.warning(
                        "EventSub: Twitch rejected subscription %s/%s (budget exceeded)",
                        event_type,
                        login,
                    )
                    break
            if success_count == len(self.subscription_plan) or success_count > 0:
                break
            if had_auth_error:
                logger.warning(
                    "EventSub subscriptions failed (auth), retrying with refreshed user token"
                )
            else:
                logger.warning("EventSub subscriptions failed, retrying")

        if success_count == 0:
            raise RuntimeError(
                "EventSub: no subscriptions created — re-run auth: "
                "docker compose run --rm bot python -m src.main auth"
            )

        if success_count < len(self.subscription_plan):
            logger.warning(
                "EventSub: %d/%d planned subscriptions active",
                success_count,
                len(self.subscription_plan),
            )
        else:
            logger.info(
                "EventSub: %d/%d subscriptions active",
                success_count,
                len(self.subscription_plan),
            )

    async def _create_subscription(
        self,
        session: aiohttp.ClientSession,
        access_token: str,
        event_type: str,
        version: str,
        condition: dict[str, str],
        login: str,
    ) -> bool | None | str:
        """Return True on success, False on other failure, None on auth failure, 'budget_exhausted' on 429 cost."""
        body = {
            "type": event_type,
            "version": version,
            "condition": condition,
            "transport": {
                "method": "websocket",
                "session_id": self._session_id,
            },
        }
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Client-Id": self.client_id,
            "Content-Type": "application/json",
        }
        async with session.post(
            "https://api.twitch.tv/helix/eventsub/subscriptions",
            headers=headers,
            json=body,
        ) as resp:
            data = await resp.json()
            if resp.status in (401, 403):
                logger.error(
                    "EventSub auth failed for %s/%s — user token invalid or expired",
                    event_type,
                    login,
                )
                return None
            if resp.status == 429:
                message = str(data.get("message", ""))
                if "total cost exceeded" in message.lower():
                    return "budget_exhausted"
            if resp.status not in (202, 409):
                logger.error(
                    "Failed to subscribe %s for %s: %s",
                    event_type,
                    login,
                    data,
                )
                return False
            logger.info("Subscribed to %s for %s", event_type, login)
            return True

    async def _handle_notification(self, message: dict[str, Any]) -> None:
        sub_type = message.get("metadata", {}).get("subscription_type", "")
        event = message.get("payload", {}).get("event", {})

        if sub_type == "stream.online":
            user_id = event.get("broadcaster_user_id", "")
            login = self.login_by_id.get(user_id, event.get("broadcaster_user_login", ""))
            stream = LiveStream(
                login=login.lower(),
                user_id=user_id,
                stream_id=event.get("id", ""),
                title="",
                game_name="",
            )
            self.state.live[login.lower()] = stream
            await self.state.emit(
                "stream.online",
                {"stream": stream, "event": event},
            )
        elif sub_type == "stream.offline":
            user_id = event.get("broadcaster_user_id", "")
            login = self.login_by_id.get(user_id, event.get("broadcaster_user_login", ""))
            self.state.live.pop(login.lower(), None)
            await self.state.emit(
                "stream.offline",
                {"login": login.lower(), "event": event},
            )
        elif sub_type == "channel.raid":
            from_id = event.get("from_broadcaster_user_id", "")
            to_login = event.get("to_broadcaster_user_login", "").lower()
            from_login = self.login_by_id.get(from_id, event.get("from_broadcaster_user_login", ""))
            await self.state.emit(
                "channel.raid",
                {
                    "from_login": from_login.lower(),
                    "to_login": to_login,
                    "viewers": event.get("viewers", 0),
                    "event": event,
                },
            )


class StreamPoller:
    def __init__(
        self,
        client_id: str,
        auth,
        broadcaster_ids: dict[str, str],
        login_by_id: dict[str, str],
        state: StreamState,
        interval: int = 60,
    ) -> None:
        self.client_id = client_id
        self.auth = auth
        self.broadcaster_ids = broadcaster_ids
        self.login_by_id = login_by_id
        self.state = state
        self.interval = interval
        self._stop = asyncio.Event()

    async def run(self, session: aiohttp.ClientSession) -> None:
        while not self._stop.is_set():
            try:
                await self._poll(session)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Stream poll failed")
            await asyncio.sleep(self.interval)

    def stop(self) -> None:
        self._stop.set()

    async def _poll(self, session: aiohttp.ClientSession) -> None:
        tokens = await self.auth.ensure_valid(session)
        headers = {
            "Authorization": f"Bearer {tokens.access_token}",
            "Client-Id": self.client_id,
        }
        user_ids = list(self.broadcaster_ids.values())
        if not user_ids:
            return

        params: list[tuple[str, str]] = [("user_id", uid) for uid in user_ids]
        async with session.get(HELIX_STREAMS_URL, headers=headers, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()

        seen: set[str] = set()
        for item in data.get("data", []):
            user_id = item["user_id"]
            login = self.login_by_id.get(user_id, item["user_login"]).lower()
            seen.add(login)
            existing = self.state.live.get(login)
            if not existing:
                stream = LiveStream(
                    login=login,
                    user_id=user_id,
                    stream_id=item["id"],
                    title=item.get("title", ""),
                    game_name=item.get("game_name", ""),
                    viewer_count=item.get("viewer_count", 0),
                )
                self.state.live[login] = stream
                await self.state.emit("stream.online", {"stream": stream, "event": item})
            else:
                existing.stream_id = item["id"]
                existing.title = item.get("title", "")
                existing.game_name = item.get("game_name", "")
                existing.viewer_count = item.get("viewer_count", 0)

        for login in list(self.state.live.keys()):
            if login not in seen:
                self.state.live.pop(login, None)
                await self.state.emit("stream.offline", {"login": login, "event": {}})
