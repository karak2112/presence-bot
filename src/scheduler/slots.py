from __future__ import annotations

import logging
from dataclasses import dataclass, field

from src.config import AppConfig, StreamerConfig
from src.detection.eventsub_ws import LiveStream

logger = logging.getLogger(__name__)

MAX_SLOTS = 2
MAX_EVENTSUB_BUDGET = 10


@dataclass
class WatchSlot:
    login: str
    user_id: str
    stream_id: str
    priority_score: int
    game_name: str = ""


@dataclass
class SchedulerState:
    active_slots: list[WatchSlot] = field(default_factory=list)
    browser_login: str | None = None

    def active_logins(self) -> set[str]:
        return {s.login for s in self.active_slots}

    def top_priority_login(self) -> str | None:
        if not self.active_slots:
            return None
        return max(self.active_slots, key=lambda s: s.priority_score).login


class WatchScheduler:
    def __init__(self, config: AppConfig, streamer_map: dict[str, StreamerConfig]) -> None:
        self.config = config
        self.streamer_map = streamer_map
        self.state = SchedulerState()

    def _priority_score(self, streamer: StreamerConfig) -> int:
        base = self.config.priority_weights.get(streamer.priority, 10)
        if streamer.watch_streak:
            base += 20
        if streamer.subscribed:
            base += self.config.priority_weights.get("subscribed", 50)
        return base

    def recompute(self, live_streams: dict[str, LiveStream]) -> SchedulerState:
        candidates: list[WatchSlot] = []
        for login, stream in live_streams.items():
            streamer = self.streamer_map.get(login)
            if not streamer:
                continue
            score = self._priority_score(streamer)
            candidates.append(
                WatchSlot(
                    login=login,
                    user_id=stream.user_id,
                    stream_id=stream.stream_id,
                    priority_score=score,
                    game_name=stream.game_name,
                )
            )

        candidates.sort(key=lambda s: s.priority_score, reverse=True)
        self.state.active_slots = candidates[:MAX_SLOTS]
        self.state.browser_login = self.state.top_priority_login()

        if self.state.active_slots:
            logins = ", ".join(
                f"{s.login}({s.priority_score})" for s in self.state.active_slots
            )
            logger.info("Watch slots: %s", logins)
        else:
            logger.debug("No active watch slots")

        return self.state

    def priority_ranked_logins(self) -> list[str]:
        """All configured streamers sorted by priority score (highest first)."""
        ranked = [
            (login, self._priority_score(streamer))
            for login, streamer in self.streamer_map.items()
        ]
        ranked.sort(key=lambda item: item[1], reverse=True)
        return [login for login, _ in ranked]

    @staticmethod
    def build_eventsub_plan(
        broadcaster_ids: dict[str, str], ranked_logins: list[str]
    ) -> list[tuple[str, str, str]]:
        """Return (event_type, login, user_id) tuples that fit the EventSub budget."""
        ordered = [login for login in ranked_logins if login in broadcaster_ids]
        for login in broadcaster_ids:
            if login not in ordered:
                ordered.append(login)

        plan: list[tuple[str, str, str]] = []
        if len(ordered) * 2 <= MAX_EVENTSUB_BUDGET:
            for login in ordered:
                user_id = broadcaster_ids[login]
                plan.append(("stream.online", login, user_id))
                plan.append(("stream.offline", login, user_id))
            return plan

        # More than 5 streamers: instant go-live for top N; offline via poller for all.
        for login in ordered[:MAX_EVENTSUB_BUDGET]:
            plan.append(("stream.online", login, broadcaster_ids[login]))
        return plan

    def handle_raid(self, from_login: str, to_login: str, live_streams: dict[str, LiveStream]) -> SchedulerState:
        from_login = from_login.lower()
        to_login = to_login.lower()

        if to_login not in self.streamer_map:
            logger.info(
                "Raid not followed (target not monitored): %s -> %s",
                from_login,
                to_login,
            )
            self.recompute(live_streams)
            return self.state

        for slot in self.state.active_slots:
            if slot.login == from_login:
                slot.login = to_login
                if to_login in live_streams:
                    target = live_streams[to_login]
                    slot.user_id = target.user_id
                    slot.stream_id = target.stream_id
                    slot.game_name = target.game_name
                logger.info("Raid: switched slot from %s to %s", from_login, to_login)
                break

        self.state.browser_login = self.state.top_priority_login()
        return self.state
