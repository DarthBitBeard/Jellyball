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
# The URL above is env-overridable (tests, mirrors); this one is not. When the
# two differ, the reported tag must match on both before we believe it, so a
# DNS or config spoof of the check URL alone cannot plant a fake "update".
CANONICAL_UPDATE_CHECK_URL = "https://api.github.com/repos/DarthBitBeard/Jellyball/releases/latest"
RELEASE_TAG_URL_PREFIX = "https://github.com/DarthBitBeard/Jellyball/releases/tag/"
UPDATE_CHECK_INTERVAL = 12 * 3600.0
_UPDATE_STATE: Dict[str, object] = {"latest": "", "url": "", "checked_at": 0.0}


def _version_tuple(value: str) -> Tuple[int, ...]:
    numbers = re.findall(r"\d+", str(value or "").split("-", 1)[0])
    return tuple(int(n) for n in numbers[:3]) or (0,)


def _update_available() -> bool:
    latest = str(_UPDATE_STATE.get("latest") or "")
    return bool(latest) and _version_tuple(latest) > _version_tuple(__version__)


async def _fetch_release_tag(client: httpx.AsyncClient, url: str) -> Tuple[str, str, bool]:
    """Return (tag, html_url, is_final) for a GitHub Releases 'latest' response.

    Raises on transport errors; returns ("", "", False) for non-200, drafts,
    prereleases, or unparseable bodies so callers fail closed.
    """
    response = await client.get(
        url,
        headers={"Accept": "application/vnd.github+json", "User-Agent": f"Jellyball/{__version__}"},
    )
    if response.status_code != 200:
        return "", "", False
    release = response.json()
    if release.get("draft") or release.get("prerelease"):
        return "", "", False
    tag = str(release.get("tag_name") or "").lstrip("vV")
    if not tag or not re.search(r"\d", tag):
        return "", "", False
    html_url = str(release.get("html_url") or "")
    return tag, html_url, True


async def check_for_update() -> None:
    """Check GitHub Releases for a newer version (opt-in: Settings > Update check).

    Display-only: the result feeds the dashboard banner, nothing is downloaded
    or installed. When UPDATE_CHECK_URL is overridden, the reported tag is
    cross-checked against the canonical API response and ignored on mismatch.
    """
    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            tag, html_url, is_final = await _fetch_release_tag(client, UPDATE_CHECK_URL)
            if not is_final:
                return
            if UPDATE_CHECK_URL != CANONICAL_UPDATE_CHECK_URL:
                canonical_tag, _, canonical_final = await _fetch_release_tag(
                    client, CANONICAL_UPDATE_CHECK_URL
                )
                if not canonical_final or canonical_tag != tag:
                    LOGGER.warning(
                        "Update check: tag %r from %s does not match canonical %r; ignoring",
                        tag, UPDATE_CHECK_URL, canonical_tag,
                    )
                    return
            _UPDATE_STATE.update({
                "latest": tag,
                "url": html_url if html_url.startswith(RELEASE_TAG_URL_PREFIX) else "",
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
