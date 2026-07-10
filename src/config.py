from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

load_dotenv()

TWITCH_GQL_CLIENT_ID = "kimne78kx3ncx6brgo4mv6wki5h1ko"
TWITCH_CLIENT_VERSION = "ef928475-9403-42f2-8a34-55784bd08e16"
TWITCH_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
AUTH_DIR = DATA_DIR / "auth"
TOKEN_PATH = AUTH_DIR / "tokens.json"
DEDUP_PATH = DATA_DIR / "email_dedup.json"
HEALTH_STATE_PATH = DATA_DIR / "health_state.json"
POINTS_STATE_PATH = DATA_DIR / "points_state.json"
BOT_LOG_PATH = DATA_DIR / "bot.log"
DEVICE_ID_PATH = DATA_DIR / "device_id.txt"
DROPS_CLAIMED_PATH = DATA_DIR / "drops_claimed.json"
WEB_AUTH_PATH = DATA_DIR / "web_auth.json"


@dataclass
class StreamerConfig:
    login: str
    priority: str = "order"
    watch_streak: bool = True
    subscribed: bool = False
    user_id: str | None = None
    display_name: str | None = None


@dataclass
class AppConfig:
    streamers: list[StreamerConfig]
    priority_weights: dict[str, int] = field(
        default_factory=lambda: {"streak": 100, "subscribed": 50, "order": 10}
    )
    twitch_client_id: str = ""
    twitch_client_secret: str = ""
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_pass: str = ""
    notify_email: str = ""
    poll_interval_seconds: int = 60
    watchdog_interval_seconds: int = 600
    health_port: int = 8080
    browser_enabled: bool = True
    log_level: str = "INFO"
    points_log_interval_seconds: int = 900
    file_log_enabled: bool = True
    drops_enabled: bool = True
    drops_poll_interval_seconds: int = 180


def load_config(config_path: str = "config/streamers.yaml") -> AppConfig:
    with open(config_path, encoding="utf-8") as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}

    streamers = [
        StreamerConfig(
            login=s["login"].lower(),
            priority=s.get("priority", "order").lower(),
            watch_streak=s.get("watch_streak", True),
            subscribed=s.get("subscribed", False),
        )
        for s in raw.get("streamers", [])
    ]

    return AppConfig(
        streamers=streamers,
        priority_weights=raw.get(
            "priority_weights",
            {"streak": 100, "subscribed": 50, "order": 10},
        ),
        twitch_client_id=os.getenv("TWITCH_CLIENT_ID", ""),
        twitch_client_secret=os.getenv("TWITCH_CLIENT_SECRET", ""),
        smtp_host=os.getenv("SMTP_HOST", ""),
        smtp_port=int(os.getenv("SMTP_PORT", "587")),
        smtp_user=os.getenv("SMTP_USER", ""),
        smtp_pass=os.getenv("SMTP_PASS", ""),
        notify_email=os.getenv("NOTIFY_EMAIL", ""),
        poll_interval_seconds=int(os.getenv("POLL_INTERVAL_SECONDS", "60")),
        watchdog_interval_seconds=int(os.getenv("WATCHDOG_INTERVAL_SECONDS", "600")),
        health_port=int(os.getenv("HEALTH_PORT", "8080")),
        browser_enabled=os.getenv("BROWSER_ENABLED", "true").lower() == "true",
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        points_log_interval_seconds=int(os.getenv("POINTS_LOG_INTERVAL_SECONDS", "900")),
        file_log_enabled=os.getenv("FILE_LOG_ENABLED", "true").lower() == "true",
        drops_enabled=os.getenv("DROPS_ENABLED", "true").lower() == "true",
        drops_poll_interval_seconds=int(os.getenv("DROPS_POLL_INTERVAL_SECONDS", "180")),
    )


def load_device_id() -> str:
    import uuid

    if DEVICE_ID_PATH.exists():
        device_id = DEVICE_ID_PATH.read_text(encoding="utf-8").strip()
        if device_id:
            return device_id

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    device_id = str(uuid.uuid4()).replace("-", "")[:32]
    DEVICE_ID_PATH.write_text(device_id, encoding="utf-8")
    return device_id


def setup_logging(level: str = "INFO", file_log: bool = True) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if file_log:
        handlers.append(logging.FileHandler(BOT_LOG_PATH, encoding="utf-8"))

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=handlers,
        force=True,
    )
