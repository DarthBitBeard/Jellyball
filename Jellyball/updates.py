"""Opt-in update check against GitHub Releases."""

import asyncio
import logging
import os
import random
import re
import time
from typing import Dict, Tuple

import httpx

from config import _log_failure, LOGGER
from db import get_setting_async
from version import __version__


UPDATE_CHECK_URL = os.getenv(
    "UPDATE_CHECK_URL", "https://api.github.com/repos/DarthBitBeard/Jellyball/releases/latest"
)
UPDATE_CHECK_INTERVAL = 12 * 3600.0
_UPDATE_STATE: Dict[str, object] = {"latest": "", "url": "", "checked_at": 0.0}


def _version_tuple(value: str) -> Tuple[int, ...]:
    numbers = re.findall(r"\d+", str(value or "").split("-", 1)[0])
    return tuple(int(n) for n in numbers[:3]) or (0,)


def _update_available() -> bool:
    latest = str(_UPDATE_STATE.get("latest") or "")
    return bool(latest) and _version_tuple(latest) > _version_tuple(__version__)


async def check_for_update() -> None:
    """One request to GitHub Releases (opt-in: Settings > Update check)."""
    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            response = await client.get(
                UPDATE_CHECK_URL,
                headers={"Accept": "application/vnd.github+json", "User-Agent": f"Jellyball/{__version__}"},
            )
        if response.status_code != 200:
            return
        release = response.json()
        tag = str(release.get("tag_name") or "").lstrip("vV")
        html_url = str(release.get("html_url") or "")
        if tag and not release.get("draft") and not release.get("prerelease"):
            _UPDATE_STATE.update({
                "latest": tag,
                "url": html_url if html_url.startswith("https://github.com/") else "",
                "checked_at": time.time(),
            })
            if _update_available():
                LOGGER.info("Jellyball %s is available (running %s)", tag, __version__)
    except Exception as exc:
        _log_failure("check for updates", exc, logging.INFO)


async def update_check_loop() -> None:
    await asyncio.sleep(random.uniform(30.0, 120.0))
    while True:
        if await get_setting_async("update_check_enabled", "0") == "1":
            await check_for_update()
        await asyncio.sleep(UPDATE_CHECK_INTERVAL)
