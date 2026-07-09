from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from typing import Any

import aiohttp

from src.auth.twitch_auth import TwitchAuth
from src.browser.playwright_worker import BrowserSafetyNet
from src.chat.irc_manager import TwitchIRCManager
from src.config import AppConfig, StreamerConfig, load_config, setup_logging
from src.detection.eventsub_ws import EventSubListener, LiveStream, StreamPoller, StreamState
from src.health.server import HealthServer
from src.notify.email_notifier import EmailNotifier
from src.scheduler.slots import WatchScheduler
from src.watcher.gql import TwitchGQL
from src.watcher.minute_watched import MinuteWatchedWatcher
from src.watcher.points_tracker import PointsTracker

logger = logging.getLogger(__name__)


class TwitchPresenceBot:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.auth = TwitchAuth(config.twitch_client_id, config.twitch_client_secret)
        self.stream_state = StreamState()
        self.scheduler = WatchScheduler(config, {})
        self.notifier = EmailNotifier(
            config.smtp_host,
            config.smtp_port,
            config.smtp_user,
            config.smtp_pass,
            config.notify_email,
        )
        self.irc: TwitchIRCManager | None = None
        self.watcher: MinuteWatchedWatcher | None = None
        self.points_tracker: PointsTracker | None = None
        self.browser: BrowserSafetyNet | None = None
        self._streamer_map: dict[str, StreamerConfig] = {}
        self._login_by_id: dict[str, str] = {}
        self._broadcaster_ids: dict[str, str] = {}
        self._started_at = time.time()
        self._tasks: list[asyncio.Task] = []

    async def setup(self, session: aiohttp.ClientSession) -> None:
        if not self.auth.load():
            raise RuntimeError("Not authenticated. Run: python -m src.main auth")

        await self.auth.ensure_valid(session)
        await self._resolve_streamers(session)

        self.scheduler = WatchScheduler(self.config, self._streamer_map)
        self.stream_state.on_event(self._on_stream_event)

        gql = TwitchGQL(self.auth)
        self.watcher = MinuteWatchedWatcher(gql, self.auth.user_id)
        self.points_tracker = PointsTracker(
            self.auth,
            gql,
            self._broadcaster_ids,
            self._login_by_id,
            interval_seconds=self.config.points_log_interval_seconds,
        )
        self.points_tracker.load()
        self.irc = TwitchIRCManager(self.auth.login, self.auth.access_token)
        self.irc.on_raid(self._on_irc_raid)

        if self.config.browser_enabled:
            self.browser = BrowserSafetyNet(
                self.auth.access_token,
                refresh_interval=self.config.watchdog_interval_seconds,
            )
            started = await self.browser.start()
            if not started:
                logger.warning(
                    "Browser safety net unavailable; API watch + IRC still active"
                )

    async def _resolve_streamers(self, session: aiohttp.ClientSession) -> None:
        tokens = await self.auth.ensure_valid(session)
        headers = {
            "Authorization": f"Bearer {tokens.access_token}",
            "Client-Id": self.config.twitch_client_id,
        }
        logins = [s.login for s in self.config.streamers]
        params: list[tuple[str, str]] = [("login", login) for login in logins]

        async with session.get(
            "https://api.twitch.tv/helix/users",
            headers=headers,
            params=params,
        ) as resp:
            resp.raise_for_status()
            users = (await resp.json()).get("data", [])

        found = {u["login"].lower(): u for u in users}
        for streamer in self.config.streamers:
            user = found.get(streamer.login)
            if not user:
                logger.warning("Streamer not found on Twitch: %s", streamer.login)
                continue
            streamer.user_id = user["id"]
            streamer.display_name = user.get("display_name", streamer.login)
            self._streamer_map[streamer.login] = streamer
            self._broadcaster_ids[streamer.login] = user["id"]
            self._login_by_id[user["id"]] = streamer.login

        if not self._streamer_map:
            raise RuntimeError("No valid streamers configured in config/streamers.yaml")

    async def _on_stream_event(self, event_type: str, payload: dict[str, Any]) -> None:
        if event_type == "stream.online":
            stream: LiveStream = payload["stream"]
            streamer = self._streamer_map.get(stream.login)
            if streamer:
                stream.display_name = streamer.display_name or stream.login
            self.scheduler.recompute(self.stream_state.live)
            await self.notifier.notify_go_live(stream)
            await self._sync_presence()
        elif event_type == "stream.offline":
            self.scheduler.recompute(self.stream_state.live)
            await self._sync_presence()
        elif event_type == "channel.raid":
            from_login = payload["from_login"]
            to_login = payload["to_login"]
            self.scheduler.handle_raid(from_login, to_login, self.stream_state.live)
            if self.browser:
                await self.browser.handle_raid(to_login)
            await self._sync_presence()
            logger.info("Handled raid %s -> %s", from_login, to_login)

    async def _on_irc_raid(self, from_login: str, to_login: str, viewers: int) -> None:
        await self._on_stream_event(
            "channel.raid",
            {
                "from_login": from_login,
                "to_login": to_login,
                "viewers": viewers,
                "event": {"source": "irc"},
            },
        )

    async def _sync_presence(self) -> None:
        try:
            tokens = await self.auth.ensure_valid(self._session)
            if self.browser:
                self.browser.access_token = tokens.access_token
            if self.irc and self.irc.oauth_token != tokens.access_token:
                self.irc.update_token(tokens.access_token)

            live_logins = set(self.stream_state.live.keys())
            if self.irc:
                self.irc.set_channels(live_logins)
            if self.watcher:
                await self.watcher.refresh_sessions(
                    self._session,
                    self.scheduler.state.active_slots,
                )
            if self.points_tracker and self.watcher:
                for login, watch_session in self.watcher.stats.sessions.items():
                    await self.points_tracker.log_stream_start(
                        self._session, login, watch_session.broadcast_id
                    )
        except Exception:
            logger.exception("Failed to sync presence state")

    def get_slots(self):
        return self.scheduler.state.active_slots

    def get_browser_login(self):
        return self.scheduler.state.browser_login

    def get_status(self) -> dict[str, Any]:
        return {
            "authenticated": self.auth.tokens is not None,
            "uptime_seconds": int(time.time() - self._started_at),
            "live_streamers": list(self.stream_state.live.keys()),
            "active_watch_slots": [
                {"login": s.login, "priority": s.priority_score}
                for s in self.scheduler.state.active_slots
            ],
            "browser_login": self.scheduler.state.browser_login,
            "browser_ready": self.browser.ready if self.browser else False,
            "browser_enabled": self.browser._enabled if self.browser else False,
            "irc_joined": sorted(self.irc._joined) if self.irc else [],
            "total_minutes_watched": self.watcher.stats.total_minutes if self.watcher else 0,
            "watch_conflict_detected": (
                self.watcher.stats.conflict_detected if self.watcher else False
            ),
            "last_watchdog_at": (
                self.watcher.stats.last_watchdog_at if self.watcher else 0
            ),
            "channel_points": (
                {
                    login: {
                        "balance": state.balance,
                        "watch_streak": state.watch_streak,
                    }
                    for login, state in self.points_tracker.states.items()
                }
                if self.points_tracker
                else {}
            ),
        }

    async def run(self) -> None:
        async with aiohttp.ClientSession() as session:
            self._session = session
            await self.setup(session)

            eventsub = EventSubListener(
                self.config.twitch_client_id,
                self.auth,
                self._broadcaster_ids,
                self._login_by_id,
                self.stream_state,
            )
            poller = StreamPoller(
                self.config.twitch_client_id,
                self.auth,
                self._broadcaster_ids,
                self._login_by_id,
                self.stream_state,
                interval=self.config.poll_interval_seconds,
            )
            health = HealthServer(self.config.health_port, self.get_status)

            await health.start()
            self.scheduler.recompute(self.stream_state.live)

            self._tasks = [
                asyncio.create_task(eventsub.run(session), name="eventsub"),
                asyncio.create_task(poller.run(session), name="poller"),
                asyncio.create_task(self._token_refresh_loop(session), name="token-refresh"),
                asyncio.create_task(self.irc.run(), name="irc"),
                asyncio.create_task(
                    self.watcher.run(session, self.get_slots), name="watcher"
                ),
                asyncio.create_task(
                    self.watcher.watchdog(session, self.get_slots), name="watchdog"
                ),
                asyncio.create_task(self._irc_sync_loop(), name="irc-sync"),
                asyncio.create_task(
                    self.points_tracker.run_pubsub(), name="pubsub-points"
                ),
                asyncio.create_task(
                    self.points_tracker.periodic_log(session, self.get_slots),
                    name="points-log",
                ),
            ]

            if self.browser:
                self._tasks.append(
                    asyncio.create_task(
                        self.browser.run(self.get_browser_login), name="browser"
                    )
                )

            logger.info(
                "Bot running — monitoring %d streamers",
                len(self._streamer_map),
            )

            try:
                await asyncio.gather(*self._tasks)
            except asyncio.CancelledError:
                pass
            finally:
                eventsub.stop()
                poller.stop()
                if self.watcher:
                    self.watcher.stop()
                if self.points_tracker:
                    self.points_tracker.stop()
                if self.irc:
                    self.irc.stop()
                if self.browser:
                    await self.browser.stop()
                await health.stop()

    async def _token_refresh_loop(self, session: aiohttp.ClientSession) -> None:
        while True:
            await asyncio.sleep(300)
            try:
                tokens = await self.auth.ensure_valid(session)
                if self.browser:
                    self.browser.access_token = tokens.access_token
                if self.irc:
                    self.irc.update_token(tokens.access_token)
            except Exception:
                logger.exception("Token refresh failed")

    async def _irc_sync_loop(self) -> None:
        last_conflict = False
        while True:
            await asyncio.sleep(30)
            if self.irc:
                self.irc.set_channels(set(self.stream_state.live.keys()))
                await self.irc.sync_channels()
            if self.watcher and self.watcher.stats.conflict_detected and not last_conflict:
                last_conflict = True
                await self.notifier.notify_alert(
                    "Twitch bot: watch conflict detected",
                    "Minute-watched requests are failing. You may be watching "
                    "Twitch on another device, which conflicts with the 2-stream limit.",
                )
            elif self.watcher and not self.watcher.stats.conflict_detected:
                last_conflict = False


async def cmd_auth(config: AppConfig) -> None:
    if not config.twitch_client_id or not config.twitch_client_secret:
        print("Set TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET in .env first.")
        sys.exit(1)
    auth = TwitchAuth(config.twitch_client_id, config.twitch_client_secret)
    async with aiohttp.ClientSession() as session:
        await auth.device_login(session)
    print(f"Authenticated as {auth.login}. Tokens saved.")


async def cmd_test_email(config: AppConfig) -> None:
    notifier = EmailNotifier(
        config.smtp_host,
        config.smtp_port,
        config.smtp_user,
        config.smtp_pass,
        config.notify_email,
    )
    ok = await notifier.send_test_email()
    if ok:
        print(f"Test email sent to {config.notify_email}")
    else:
        print("Failed to send test email. Check logs and .env SMTP settings.")
        sys.exit(1)


async def cmd_run(config: AppConfig) -> None:
    bot = TwitchPresenceBot(config)
    await bot.run()


def main() -> None:
    parser = argparse.ArgumentParser(description="Twitch Watch-Streak & Presence Bot")
    parser.add_argument("command", choices=["auth", "run", "test-email"], help="auth, run, or test-email")
    parser.add_argument(
        "--config", default="config/streamers.yaml", help="Path to streamers config"
    )
    args = parser.parse_args()

    config = load_config(args.config)
    setup_logging(config.log_level, file_log=config.file_log_enabled)

    if args.command == "auth":
        asyncio.run(cmd_auth(config))
    elif args.command == "test-email":
        asyncio.run(cmd_test_email(config))
    else:
        asyncio.run(cmd_run(config))


if __name__ == "__main__":
    main()
