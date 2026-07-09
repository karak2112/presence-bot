from __future__ import annotations

import json
import logging
from email.message import EmailMessage
from pathlib import Path

import aiosmtplib

from src.config import DEDUP_PATH
from src.detection.eventsub_ws import LiveStream

logger = logging.getLogger(__name__)


class EmailNotifier:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        notify_to: str,
        dedup_path: Path = DEDUP_PATH,
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.notify_to = notify_to
        self.dedup_path = dedup_path
        self._sent: set[str] = set()
        self._load_dedup()

    @property
    def enabled(self) -> bool:
        return bool(self.host and self.user and self.password and self.notify_to)

    def _load_dedup(self) -> None:
        if self.dedup_path.exists():
            with open(self.dedup_path, encoding="utf-8") as f:
                self._sent = set(json.load(f))

    def _save_dedup(self) -> None:
        self.dedup_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.dedup_path, "w", encoding="utf-8") as f:
            json.dump(sorted(self._sent), f)

    def _dedup_key(self, stream: LiveStream) -> str:
        return f"{stream.user_id}:{stream.stream_id}"

    async def notify_go_live(self, stream: LiveStream) -> None:
        if not self.enabled:
            logger.debug("Email notifications disabled (missing SMTP config)")
            return

        key = self._dedup_key(stream)
        if key in self._sent:
            logger.debug("Skipping duplicate email for %s", stream.login)
            return

        subject = f"🔴 {stream.display_name or stream.login} is live on Twitch!"
        body = (
            f"{stream.display_name or stream.login} just went live.\n\n"
            f"Title: {stream.title or '(unknown)'}\n"
            f"Game: {stream.game_name or '(unknown)'}\n"
            f"Viewers: {stream.viewer_count}\n\n"
            f"Watch: https://twitch.tv/{stream.login}\n"
        )

        try:
            await self._send(subject, body)
            self._sent.add(key)
            self._save_dedup()
            logger.info("Sent go-live email for %s", stream.login)
        except Exception:
            logger.exception("Failed to send email for %s", stream.login)

    async def notify_alert(self, subject: str, body: str) -> None:
        if not self.enabled:
            return
        await self._send(subject, body)

    async def send_test_email(self) -> bool:
        if not self.enabled:
            logger.error(
                "Email not configured. Set SMTP_HOST, SMTP_USER, SMTP_PASS, NOTIFY_EMAIL in .env"
            )
            return False

        subject = "Twitch Presence Bot — test email"
        body = (
            "This is a test email from your Twitch Presence Bot.\n\n"
            "If you received this, SMTP notifications are working correctly.\n"
        )
        try:
            await self._send(subject, body)
            logger.info("Test email sent to %s", self.notify_to)
            return True
        except Exception:
            logger.exception("Failed to send test email")
            return False

    async def _send(self, subject: str, body: str) -> None:
        message = EmailMessage()
        message["From"] = self.user
        message["To"] = self.notify_to
        message["Subject"] = subject
        message.set_content(body)
        await aiosmtplib.send(
            message,
            hostname=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
            start_tls=True,
        )
