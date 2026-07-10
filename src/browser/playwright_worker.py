from __future__ import annotations

import asyncio
import logging
import time

logger = logging.getLogger(__name__)


class BrowserSafetyNet:
    """Headless Playwright browser for the top-priority watch slot."""

    def __init__(self, access_token: str, refresh_interval: int = 600) -> None:
        self.access_token = access_token
        self.refresh_interval = refresh_interval
        self._current_login: str | None = None
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None
        self._last_refresh: float = 0.0
        self._stop = asyncio.Event()
        self._enabled = True
        self._ready = False

    @property
    def current_login(self) -> str | None:
        return self._current_login

    @property
    def ready(self) -> bool:
        return self._ready

    def disable(self) -> None:
        self._enabled = False

    async def start(self) -> bool:
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.warning("Playwright not installed; browser safety net disabled")
            self._enabled = False
            return False

        try:
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(
                headless=True,
                args=[
                    "--mute-audio",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                ],
            )
            self._ready = True
            logger.info("Browser safety net started")
            return True
        except Exception:
            logger.exception(
                "Browser safety net failed to start; continuing without browser"
            )
            await self._cleanup()
            self._enabled = False
            return False

    async def _cleanup(self) -> None:
        if self._browser:
            try:
                await self._browser.close()
            except Exception:
                pass
            self._browser = None
        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception:
                pass
            self._playwright = None

    async def stop(self) -> None:
        self._stop.set()
        await self._close_page()
        await self._cleanup()
        self._ready = False

    async def run(self, get_browser_login) -> None:
        if not self._enabled:
            return
        while not self._stop.is_set():
            login = get_browser_login()
            if login and login != self._current_login:
                await self.navigate(login)
            elif login and time.time() - self._last_refresh >= self.refresh_interval:
                await self.refresh()
            elif not login and self._current_login:
                await self._close_page()
                self._current_login = None
            await asyncio.sleep(15)

    async def navigate(self, login: str) -> None:
        if not self._enabled or not self._browser:
            return
        await self._close_page()
        self._context = await self._browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
        )
        await self._context.add_cookies([
            {
                "name": "auth-token",
                "value": self.access_token,
                "domain": ".twitch.tv",
                "path": "/",
                "httpOnly": False,
                "secure": True,
                "sameSite": "None",
            }
        ])
        self._page = await self._context.new_page()
        url = f"https://www.twitch.tv/{login}"
        try:
            await self._page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await self._page.wait_for_timeout(5000)
            self._current_login = login
            self._last_refresh = time.time()
            logger.info("Browser watching %s", login)
        except Exception:
            logger.exception("Browser failed to load %s", login)
            await self._close_page()

    async def refresh(self) -> None:
        if not self._page or not self._current_login:
            return
        try:
            await self._page.reload(wait_until="domcontentloaded", timeout=60000)
            self._last_refresh = time.time()
            logger.info("Browser refreshed %s", self._current_login)
        except Exception:
            logger.exception("Browser refresh failed for %s", self._current_login)
            await self.navigate(self._current_login)

    async def _close_page(self) -> None:
        if self._page:
            try:
                await self._page.close()
            except Exception:
                pass
            self._page = None
        if self._context:
            try:
                await self._context.close()
            except Exception:
                pass
            self._context = None
