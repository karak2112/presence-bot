from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import asdict, dataclass
from typing import Any

import aiohttp

from src.config import AUTH_DIR, TOKEN_PATH

logger = logging.getLogger(__name__)

DEVICE_CODE_URL = "https://id.twitch.tv/oauth2/device"
TOKEN_URL = "https://id.twitch.tv/oauth2/token"
VALIDATE_URL = "https://id.twitch.tv/oauth2/validate"

SCOPES = [
    "user:read:email",
    "chat:read",
    "chat:edit",
]


@dataclass
class TwitchTokens:
    access_token: str
    refresh_token: str
    expires_at: float
    user_id: str = ""
    login: str = ""
    display_name: str = ""

    @property
    def expired(self) -> bool:
        return time.time() >= self.expires_at - 120

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TwitchTokens:
        return cls(
            access_token=data["access_token"],
            refresh_token=data["refresh_token"],
            expires_at=data.get("expires_at", 0),
            user_id=data.get("user_id", ""),
            login=data.get("login", ""),
            display_name=data.get("display_name", ""),
        )


class TwitchAuth:
    def __init__(self, client_id: str, client_secret: str) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.tokens: TwitchTokens | None = None
        self._app_token: str | None = None
        self._app_token_expires_at: float = 0.0
        AUTH_DIR.mkdir(parents=True, exist_ok=True)

    def load(self) -> bool:
        if not TOKEN_PATH.exists():
            return False
        with open(TOKEN_PATH, encoding="utf-8") as f:
            self.tokens = TwitchTokens.from_dict(json.load(f))
        return True

    def save(self) -> None:
        if not self.tokens:
            return
        with open(TOKEN_PATH, "w", encoding="utf-8") as f:
            json.dump(self.tokens.to_dict(), f, indent=2)

    async def device_login(self, session: aiohttp.ClientSession) -> None:
        params = {
            "client_id": self.client_id,
            "scopes": " ".join(SCOPES),
        }
        async with session.post(DEVICE_CODE_URL, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()

        print("\n=== Twitch Device Login ===")
        print(f"Visit: {data['verification_uri']}")
        print(f"Enter code: {data['user_code']}")
        print("Waiting for authorization...\n")

        interval = data.get("interval", 5)
        deadline = time.time() + data["expires_in"]

        while time.time() < deadline:
            await asyncio.sleep(interval)
            token_data = await self._poll_device_token(session, data["device_code"])
            if token_data:
                await self._set_tokens(session, token_data)
                self.save()
                logger.info("Authenticated as %s", self.tokens.login if self.tokens else "?")
                return

        raise TimeoutError("Device login timed out")

    async def _poll_device_token(
        self, session: aiohttp.ClientSession, device_code: str
    ) -> dict[str, Any] | None:
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "device_code": device_code,
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
        }
        async with session.post(TOKEN_URL, data=payload) as resp:
            data = await resp.json()
            if resp.status == 200:
                return data
            if data.get("message") == "authorization_pending":
                return None
            if data.get("message") == "slow_down":
                await asyncio.sleep(5)
                return None
            raise RuntimeError(f"Device token error: {data}")

    async def _set_tokens(
        self, session: aiohttp.ClientSession, token_data: dict[str, Any]
    ) -> None:
        expires_at = time.time() + token_data.get("expires_in", 0)
        self.tokens = TwitchTokens(
            access_token=token_data["access_token"],
            refresh_token=token_data.get("refresh_token", ""),
            expires_at=expires_at,
        )
        await self._populate_user_info(session)

    async def _populate_user_info(self, session: aiohttp.ClientSession) -> None:
        if not self.tokens:
            return
        headers = {"Authorization": f"OAuth {self.tokens.access_token}"}
        async with session.get(VALIDATE_URL, headers=headers) as resp:
            resp.raise_for_status()
            data = await resp.json()
        self.tokens.user_id = data["user_id"]
        self.tokens.login = data["login"]

        helix_headers = {
            "Authorization": f"Bearer {self.tokens.access_token}",
            "Client-Id": self.client_id,
        }
        async with session.get(
            "https://api.twitch.tv/helix/users",
            headers=helix_headers,
            params={"id": self.tokens.user_id},
        ) as resp:
            resp.raise_for_status()
            users = (await resp.json()).get("data", [])
            if users:
                self.tokens.display_name = users[0].get("display_name", self.tokens.login)

    async def ensure_valid(self, session: aiohttp.ClientSession) -> TwitchTokens:
        if not self.tokens:
            raise RuntimeError("Not authenticated. Run: python -m src.main auth")

        if not self.tokens.expired and await self._token_is_valid(session):
            return self.tokens

        return await self._refresh_user_token(session)

    async def _token_is_valid(self, session: aiohttp.ClientSession) -> bool:
        if not self.tokens:
            return False
        headers = {"Authorization": f"OAuth {self.tokens.access_token}"}
        async with session.get(VALIDATE_URL, headers=headers) as resp:
            return resp.status == 200

    async def _refresh_user_token(self, session: aiohttp.ClientSession) -> TwitchTokens:
        if not self.tokens:
            raise RuntimeError("Not authenticated. Run: python -m src.main auth")

        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
            "refresh_token": self.tokens.refresh_token,
        }
        async with session.post(TOKEN_URL, data=payload) as resp:
            data = await resp.json()
            if resp.status != 200:
                raise RuntimeError(
                    f"Token refresh failed ({resp.status}): {data}. "
                    "Re-run: docker compose run --rm bot python -m src.main auth"
                )

        self.tokens.access_token = data["access_token"]
        if data.get("refresh_token"):
            self.tokens.refresh_token = data["refresh_token"]
        self.tokens.expires_at = time.time() + data.get("expires_in", 0)
        await self._populate_user_info(session)
        self.save()
        logger.info("Refreshed access token for %s", self.tokens.login)
        return self.tokens

    async def app_access_token(self, session: aiohttp.ClientSession) -> str:
        if self._app_token and time.time() < self._app_token_expires_at - 120:
            return self._app_token

        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }
        async with session.post(TOKEN_URL, data=payload) as resp:
            data = await resp.json()
            if resp.status != 200:
                raise RuntimeError(f"App token request failed ({resp.status}): {data}")

        self._app_token = data["access_token"]
        self._app_token_expires_at = time.time() + data.get("expires_in", 3600)
        return self._app_token

    @property
    def access_token(self) -> str:
        if not self.tokens:
            raise RuntimeError("Not authenticated")
        return self.tokens.access_token

    @property
    def user_id(self) -> str:
        if not self.tokens:
            raise RuntimeError("Not authenticated")
        return self.tokens.user_id

    @property
    def login(self) -> str:
        if not self.tokens:
            raise RuntimeError("Not authenticated")
        return self.tokens.login
