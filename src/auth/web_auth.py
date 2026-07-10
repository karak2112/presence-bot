from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path

from src.config import DATA_DIR, WEB_AUTH_PATH

logger = logging.getLogger(__name__)


@dataclass
class WebAuth:
    auth_token: str

    @classmethod
    def load(cls) -> WebAuth | None:
        env_token = os.getenv("TWITCH_WEB_AUTH_TOKEN", "").strip()
        if env_token:
            return cls(_normalize_token(env_token))

        if not WEB_AUTH_PATH.exists():
            return None

        with open(WEB_AUTH_PATH, encoding="utf-8") as f:
            raw = json.load(f)

        token = _normalize_token(str(raw.get("auth_token", "")).strip())
        if not token:
            return None
        return cls(token)

    def save(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(WEB_AUTH_PATH, "w", encoding="utf-8") as f:
            json.dump({"auth_token": self.auth_token}, f, indent=2)


def _normalize_token(token: str) -> str:
    return token.removeprefix("oauth:").strip()
