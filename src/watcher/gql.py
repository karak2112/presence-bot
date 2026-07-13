from __future__ import annotations

import logging
import re
from typing import Any, TYPE_CHECKING

import aiohttp

from src.config import TWITCH_USER_AGENT, TWITCH_GQL_CLIENT_ID, load_device_id
from src.auth.web_auth import WebAuth

if TYPE_CHECKING:
    from src.auth.twitch_auth import TwitchAuth

logger = logging.getLogger(__name__)

DEFAULT_SPADE_URL = "https://spade.twitch.tv/track"
GQL_URL = "https://gql.twitch.tv/gql"
CHANNEL_POINTS_CONTEXT_HASH = (
    "1530a003a7d374b0380b79db0be0534f30ff46e61cffa2bc0e2468a909fbc024"
)
INVENTORY_HASH = "d86775d0ef16a63a33ad52e80eaff963b2d5b72fada7c991504a57496e1d8e4b"

SETTINGS_PATTERNS = [
    re.compile(r'src="(https://[\w.]+/config/settings\.[0-9a-f]{32}\.js)"', re.I),
    re.compile(r"(https://assets\.twitch\.tv/config/settings\.[0-9a-f]+\.js)", re.I),
    re.compile(r"(https://static\.twitchcdn\.net/config/settings.*?\.js)", re.I),
]
SPADE_PATTERN = re.compile(r'"(?:beacon|spade)_?url": ?"(https://[^"]+)"', re.I)

class TwitchGQL:
    """Twitch API client — Helix for stream metadata, page scrape for spade URL."""

    HELIX_STREAMS_URL = "https://api.twitch.tv/helix/streams"

    def __init__(self, auth: "TwitchAuth", device_id: str | None = None) -> None:
        self.auth = auth
        self.device_id = device_id or load_device_id()
        self._web_auth = WebAuth.load()
        self._channel_points_gql_disabled = False
        self._channel_points_gql_notice_logged = False
        self._drops_gql_disabled = False
        self._drops_gql_notice_logged = False
        if self._web_auth:
            logger.info(
                "GQL will use web session token (data/web_auth.json or TWITCH_WEB_AUTH_TOKEN)"
            )

    @property
    def uses_web_auth(self) -> bool:
        return self._web_auth is not None

    @property
    def channel_points_gql_available(self) -> bool:
        return self.uses_web_auth and not self._channel_points_gql_disabled

    @property
    def drops_gql_available(self) -> bool:
        return self.uses_web_auth and not self._drops_gql_disabled

    @property
    def access_token(self) -> str:
        return self.auth.access_token

    @access_token.setter
    def access_token(self, value: str) -> None:
        if self.auth.tokens:
            self.auth.tokens.access_token = value

    def _helix_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.auth.access_token}",
            "Client-Id": self.auth.client_id,
        }

    def _gql_headers(self) -> dict[str, str]:
        from src.config import TWITCH_CLIENT_VERSION

        if self._web_auth:
            return {
                "Authorization": f"OAuth {self._web_auth.auth_token}",
                "Client-Id": TWITCH_GQL_CLIENT_ID,
                "Client-Version": TWITCH_CLIENT_VERSION,
                "User-Agent": TWITCH_USER_AGENT,
                "X-Device-Id": self.device_id,
                "Content-Type": "application/json",
            }

        return {
            "Authorization": f"OAuth {self.auth.access_token}",
            "Client-Id": self.auth.client_id,
            "Client-Version": TWITCH_CLIENT_VERSION,
            "User-Agent": TWITCH_USER_AGENT,
            "X-Device-Id": self.device_id,
            "Content-Type": "application/json",
        }

    def _disable_gql(self, disable_key: str | None, *, web_token_rejected: bool) -> None:
        if disable_key == "drops":
            self._drops_gql_disabled = True
            if self._drops_gql_notice_logged:
                return
            self._drops_gql_notice_logged = True
            if web_token_rejected:
                logger.warning(
                    "Drops GQL rejected the web session token. "
                    "Re-copy auth-token from your browser into data/web_auth.json."
                )
            else:
                logger.info(
                    "Drops GQL requires a web session token. "
                    "Add data/web_auth.json (see data/web_auth.json.example)."
                )
        elif disable_key == "channel_points":
            self._channel_points_gql_disabled = True
            if self._channel_points_gql_notice_logged:
                return
            self._channel_points_gql_notice_logged = True
            if web_token_rejected:
                logger.warning(
                    "Channel points GQL rejected the web session token. "
                    "Balances will use PubSub until you refresh data/web_auth.json."
                )
            else:
                logger.info(
                    "Channel points GQL requires a web session token for balance fetch. "
                    "Balances will be tracked via PubSub instead."
                )

    async def get_channel_points_context(
        self, session: aiohttp.ClientSession, login: str
    ) -> dict[str, Any] | None:
        if not self.uses_web_auth or self._channel_points_gql_disabled:
            return None

        data = await self.post_gql_persisted(
            session,
            "ChannelPointsContext",
            CHANNEL_POINTS_CONTEXT_HASH,
            {"channelLogin": login},
            disable_key="channel_points",
        )
        if not data:
            return None
        community = data.get("data", {}).get("community")
        if not community:
            return None
        channel = community.get("channel")
        if not channel:
            return None
        return channel.get("self", {}).get("communityPoints")

    @staticmethod
    def summarize_drop_progress(inventory: dict[str, Any]) -> list[dict[str, Any]]:
        summary: list[dict[str, Any]] = []
        for campaign in inventory.get("dropCampaignsInProgress") or []:
            campaign_name = campaign.get("name", "Unknown campaign")
            for drop_dict in campaign.get("timeBasedDrops") or []:
                progress = drop_dict.get("self") or {}
                required = drop_dict.get("requiredMinutesWatched", 0)
                watched = progress.get("currentMinutesWatched", 0)
                summary.append(
                    {
                        "campaign": campaign_name,
                        "drop": drop_dict.get("name", "Unknown drop"),
                        "minutes_watched": watched,
                        "minutes_required": required,
                        "is_claimed": progress.get("isClaimed", False),
                        "ready_to_claim": bool(progress.get("dropInstanceID"))
                        and not progress.get("isClaimed"),
                    }
                )
        return summary

    async def post_gql_persisted(
        self,
        session: aiohttp.ClientSession,
        operation_name: str,
        sha256_hash: str,
        variables: dict[str, Any] | None = None,
        *,
        disable_key: str | None = None,
    ) -> dict[str, Any] | None:
        if not self.uses_web_auth:
            if disable_key:
                self._disable_gql(disable_key, web_token_rejected=False)
            return None
        if disable_key == "drops" and self._drops_gql_disabled:
            return None
        if disable_key == "channel_points" and self._channel_points_gql_disabled:
            return None

        body: dict[str, Any] = {
            "operationName": operation_name,
            "extensions": {
                "persistedQuery": {"version": 1, "sha256Hash": sha256_hash},
            },
        }
        if variables is not None:
            body["variables"] = variables

        headers = self._gql_headers()
        async with session.post(GQL_URL, headers=headers, json=body) as resp:
            text = await resp.text()
            if resp.status == 401:
                self._disable_gql(disable_key, web_token_rejected=self.uses_web_auth)
                return None
            if resp.status != 200:
                logger.warning(
                    "GQL %s failed: HTTP %s %s",
                    operation_name,
                    resp.status,
                    text[:200],
                )
                return None
            else:
                data = await resp.json()

        if data.get("errors"):
            logger.warning("GQL %s errors: %s", operation_name, data["errors"])
            return None
        return data

    async def get_inventory(self, session: aiohttp.ClientSession) -> dict[str, Any] | None:
        data = await self.post_gql_persisted(
            session,
            "Inventory",
            INVENTORY_HASH,
            {"fetchRewardCampaigns": True},
            disable_key="drops",
        )
        if not data:
            return None
        return data.get("data", {}).get("currentUser", {}).get("inventory")

    async def get_stream_metadata(
        self, session: aiohttp.ClientSession, login: str
    ) -> dict[str, Any] | None:
        # Helix works with our app client_id + user token. Twitch GQL rejects that combo.
        for attempt in range(2):
            async with session.get(
                self.HELIX_STREAMS_URL,
                headers=self._helix_headers(),
                params={"user_login": login},
            ) as resp:
                if resp.status == 401 and attempt == 0:
                    logger.warning("Helix token rejected for %s, refreshing", login)
                    if self.auth.tokens:
                        self.auth.tokens.expires_at = 0
                    await self.auth.ensure_valid(session)
                    continue
                if resp.status != 200:
                    logger.warning(
                        "Helix streams request failed for %s: HTTP %s",
                        login,
                        resp.status,
                    )
                    return None
                streams = (await resp.json()).get("data", [])
                if not streams:
                    return None
                stream = streams[0]
                return {
                    "channel_id": stream["user_id"],
                    "login": stream["user_login"].lower(),
                    "display_name": stream.get("user_name", login),
                    "broadcast_id": stream["id"],
                    "game_name": stream.get("game_name", ""),
                    "game_id": stream.get("game_id", ""),
                }
        logger.warning("Helix metadata unavailable for %s after token refresh", login)
        return None

    async def get_spade_url(
        self, session: aiohttp.ClientSession, channel_login: str | None = None
    ) -> str:
        headers = {
            "User-Agent": TWITCH_USER_AGENT,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml",
        }
        pages = []
        if channel_login:
            pages.append(f"https://www.twitch.tv/{channel_login}")
        pages.append("https://www.twitch.tv")

        last_error: Exception | None = None
        for page_url in pages:
            try:
                spade_url = await self._extract_spade_from_page(session, page_url, headers)
                logger.info("Resolved spade URL from %s", page_url)
                return spade_url
            except Exception as exc:
                last_error = exc
                logger.debug("Spade extraction failed for %s: %s", page_url, exc)

        logger.warning(
            "Could not scrape spade URL (%s); using default %s",
            last_error,
            DEFAULT_SPADE_URL,
        )
        return DEFAULT_SPADE_URL

    async def _extract_spade_from_page(
        self,
        session: aiohttp.ClientSession,
        page_url: str,
        headers: dict[str, str],
    ) -> str:
        async with session.get(page_url, headers=headers) as resp:
            resp.raise_for_status()
            html = await resp.text()

        match = SPADE_PATTERN.search(html)
        if match:
            return match.group(1)

        settings_url = self._find_settings_url(html)
        if not settings_url:
            raise RuntimeError(f"Could not find Twitch settings URL in {page_url}")

        async with session.get(settings_url, headers=headers) as resp:
            resp.raise_for_status()
            settings_text = await resp.text()

        match = SPADE_PATTERN.search(settings_text)
        if not match:
            raise RuntimeError(f"Could not find spade/beacon URL in settings from {page_url}")
        return match.group(1)

    @staticmethod
    def _find_settings_url(html: str) -> str | None:
        for pattern in SETTINGS_PATTERNS:
            match = pattern.search(html)
            if match:
                return match.group(1)
        return None
