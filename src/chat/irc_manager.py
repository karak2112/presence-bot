from __future__ import annotations

import asyncio
import logging
import re
from typing import Awaitable, Callable, Set

logger = logging.getLogger(__name__)

IRC_HOST = "irc.chat.twitch.tv"
IRC_PORT = 6667
READ_TIMEOUT_SECONDS = 300

# Normal Twitch IRC churn — don't spam ERROR + traceback for these.
_TRANSIENT_IRC_ERRORS = (
    asyncio.TimeoutError,
    ConnectionResetError,
    ConnectionError,
    BrokenPipeError,
    OSError,
)

RaidCallback = Callable[[str, str, int], Awaitable[None]]


class TwitchIRCManager:
    def __init__(self, login: str, oauth_token: str) -> None:
        self.login = login.lower()
        self.oauth_token = oauth_token
        self._joined: Set[str] = set()
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._stop = asyncio.Event()
        self._desired_channels: Set[str] = set()
        self._raid_callbacks: list[RaidCallback] = []

    def on_raid(self, callback: RaidCallback) -> None:
        self._raid_callbacks.append(callback)

    def update_token(self, oauth_token: str) -> None:
        self.oauth_token = oauth_token
        if self._writer and not self._writer.is_closing():
            try:
                self._writer.close()
            except Exception:
                pass

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                await self._connect()
                await self._listen_loop()
            except asyncio.CancelledError:
                raise
            except RuntimeError as exc:
                logger.error("IRC login failed: %s", exc)
                await self._disconnect()
                await asyncio.sleep(30)
            except _TRANSIENT_IRC_ERRORS as exc:
                logger.warning("IRC disconnected (%s), reconnecting in 5s", exc.__class__.__name__)
                logger.debug("IRC disconnect details", exc_info=True)
                await self._disconnect()
                await asyncio.sleep(5)
            except Exception:
                logger.exception("IRC unexpected error, reconnecting in 5s")
                await self._disconnect()
                await asyncio.sleep(5)

    def stop(self) -> None:
        self._stop.set()

    def set_channels(self, logins: set[str]) -> None:
        channels = {login.lower() for login in logins}
        self._desired_channels = channels

    async def sync_channels(self) -> None:
        to_join = self._desired_channels - self._joined
        to_part = self._joined - self._desired_channels
        for channel in to_join:
            await self._join(channel)
        for channel in to_part:
            await self._part(channel)

    async def _connect(self) -> None:
        self._reader, self._writer = await asyncio.open_connection(IRC_HOST, IRC_PORT)
        token = self.oauth_token.replace("oauth:", "")
        await self._send(f"PASS oauth:{token}")
        await self._send(f"NICK {self.login}")
        await self._send("CAP REQ :twitch.tv/membership twitch.tv/tags twitch.tv/commands")
        await self._send("CAP END")
        logger.info("IRC connected as %s", self.login)

        for channel in self._desired_channels:
            await self._join(channel)

        await self.sync_channels()

    async def _listen_loop(self) -> None:
        assert self._reader is not None
        while not self._stop.is_set():
            try:
                line = await asyncio.wait_for(
                    self._reader.readline(), timeout=READ_TIMEOUT_SECONDS
                )
            except asyncio.TimeoutError:
                if not await self._send_ping():
                    logger.warning("IRC keepalive failed, reconnecting")
                    break
                continue

            if not line:
                logger.warning("IRC connection closed by server, reconnecting")
                break

            text = line.decode("utf-8", errors="replace").strip()
            if text.startswith("PING"):
                await self._send("PONG :tmi.twitch.tv")
            elif " RECONNECT" in text:
                logger.info("IRC server requested reconnect")
                break
            elif " LOGIN_UNSUCCESSFUL" in text or "Login unsuccessful" in text:
                raise RuntimeError(f"IRC login failed: {text}")
            else:
                await self._handle_line(text)

    async def _send_ping(self) -> bool:
        try:
            await self._send("PING :tmi.twitch.tv")
            return True
        except _TRANSIENT_IRC_ERRORS:
            return False

    @staticmethod
    def _parse_tags(line: str) -> tuple[dict[str, str], str]:
        if not line.startswith("@"):
            return {}, line
        try:
            tag_end = line.index(" ")
        except ValueError:
            return {}, line
        tags: dict[str, str] = {}
        for piece in line[1:tag_end].split(";"):
            if "=" in piece:
                key, value = piece.split("=", 1)
                tags[key] = value
        return tags, line[tag_end + 1 :]

    async def _handle_line(self, text: str) -> None:
        tags, rest = self._parse_tags(text)
        if tags.get("msg-id") != "raid" or "USERNOTICE" not in rest:
            return

        channel_match = re.search(r"USERNOTICE (#\S+)", rest)
        if not channel_match:
            return

        from_login = channel_match.group(1).lstrip("#").lower()
        to_login = tags.get("msg-param-login", "").lower()
        if not to_login:
            return

        viewers = int(tags.get("msg-param-viewerCount", "0") or "0")
        logger.info("IRC raid detected: %s -> %s (%d viewers)", from_login, to_login, viewers)
        for callback in self._raid_callbacks:
            try:
                await callback(from_login, to_login, viewers)
            except Exception:
                logger.exception("IRC raid callback failed")

    async def _join(self, login: str) -> None:
        channel = login.lower()
        if channel in self._joined:
            return
        await self._send(f"JOIN #{channel}")
        self._joined.add(channel)
        logger.info("IRC joined #%s", channel)

    async def _part(self, login: str) -> None:
        channel = login.lower()
        if channel not in self._joined:
            return
        await self._send(f"PART #{channel}")
        self._joined.discard(channel)
        logger.info("IRC parted #%s", channel)

    async def _send(self, message: str) -> None:
        if not self._writer:
            raise ConnectionResetError("IRC writer unavailable")
        self._writer.write(f"{message}\r\n".encode("utf-8"))
        await self._writer.drain()

    async def _disconnect(self) -> None:
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
        self._reader = None
        self._writer = None
        self._joined.clear()
