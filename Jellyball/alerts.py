"""Notifications and Jellyfin integration: Discord/Telegram alerts with
webhook retries, and the (debounced, signature-gated) Jellyfin guide refresh.
"""

import asyncio
import hashlib
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import httpx

from config import _log_failure, _validate_upstream_url, LOGGER
from state import _spawn_background_task, is_multiview, stream_state
from db import get_setting_async
import catalog
from jellyfin_client import RefreshResult, fetch_recording_channels, refresh_guide
from network_safety import bounded_float
from version import __version__


async def get_notification_config() -> dict:
    return {
        "discord_webhook_url": await get_setting_async("discord_webhook_url", os.getenv("DISCORD_WEBHOOK_URL", "")),
        "telegram_bot_token": await get_setting_async("telegram_bot_token", os.getenv("TELEGRAM_BOT_TOKEN", "")),
        "telegram_chat_id": await get_setting_async("telegram_chat_id", os.getenv("TELEGRAM_CHAT_ID", ""))
    }

# Backoff between webhook delivery attempts: one initial attempt plus up to
# this many retries, for transient failures only (network errors, 5xx, 429).
_ALERT_RETRY_DELAYS = (1.0, 3.0)
_ALERT_MAX_RETRY_AFTER_SECONDS = 10.0


def _parse_retry_after_seconds(value: Optional[str]) -> Optional[float]:
    """Parse a `Retry-After` header's numeric-seconds form. The HTTP-date form
    is rare for webhook 429s and isn't worth the parsing surface here; when
    the header is absent or unparseable the caller falls back to its own
    backoff schedule."""
    if not value:
        return None
    try:
        return max(0.0, float(value.strip()))
    except (TypeError, ValueError):
        return None


async def _post_webhook_with_retries(client: httpx.AsyncClient, provider: str, url: str, **kwargs) -> None:
    """POST a webhook payload with up to len(_ALERT_RETRY_DELAYS) retries.

    Retries transient failures — network/timeout errors, 5xx, and 429 (honouring
    a `Retry-After` header, capped at _ALERT_MAX_RETRY_AFTER_SECONDS). Any other
    4xx is treated as non-retryable. Reuses the caller's single AsyncClient
    across every attempt. The final failure is logged once at WARNING with only
    the provider name — never the webhook URL or any token embedded in it.
    """
    attempts = len(_ALERT_RETRY_DELAYS) + 1
    for attempt in range(attempts):
        is_last_attempt = attempt == attempts - 1
        try:
            response = await client.post(url, **kwargs)
        except httpx.HTTPError:
            if is_last_attempt:
                LOGGER.warning("Alert webhook delivery failed after retries provider=%s", provider)
                return
            await asyncio.sleep(_ALERT_RETRY_DELAYS[attempt])
            continue

        if response.status_code < 400:
            return

        retryable = response.status_code == 429 or response.status_code >= 500
        if not retryable or is_last_attempt:
            LOGGER.warning(
                "Alert webhook delivery failed after retries provider=%s status=%s",
                provider, response.status_code,
            )
            return

        if response.status_code == 429:
            retry_after = _parse_retry_after_seconds(response.headers.get("Retry-After"))
            delay = _ALERT_RETRY_DELAYS[attempt] if retry_after is None else retry_after
            delay = min(delay, _ALERT_MAX_RETRY_AFTER_SECONDS)
        else:
            delay = _ALERT_RETRY_DELAYS[attempt]
        await asyncio.sleep(delay)


async def send_alert(title: str, message: str, level: str = "warning"):
    config = await get_notification_config()
    discord_url = config["discord_webhook_url"]
    if discord_url and not _validate_upstream_url(discord_url):
        LOGGER.warning("Ignoring invalid Discord webhook URL")
        discord_url = ""
    tg_token = config["telegram_bot_token"]
    tg_chat_id = config["telegram_chat_id"]

    color_map = {"info": 3066993, "warning": 16753920, "danger": 14431526, "success": 3647337}
    headers = {"User-Agent": f"Jellyball/{__version__}"}
    tasks = []

    if discord_url:
        embed = {"title": title, "description": message, "color": color_map.get(level, 16753920), "timestamp": datetime.now(timezone.utc).isoformat()}
        async def _send_discord():
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    await _post_webhook_with_retries(client, "Discord", discord_url, json={"embeds": [embed]}, headers=headers)
            except Exception as exc:
                _log_failure("send Discord alert", exc)
        tasks.append(_send_discord())

    if tg_token and tg_chat_id:
        tg_url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
        tg_text = f"*{title}*\n{message}"
        async def _send_telegram():
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    await _post_webhook_with_retries(client, "Telegram", tg_url, json={"chat_id": tg_chat_id, "text": tg_text, "parse_mode": "Markdown"}, headers=headers)
            except Exception as exc:
                _log_failure("send Telegram alert", exc)
        tasks.append(_send_telegram())

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

async def get_jellyfin_config() -> dict:
    return {
        "jellyfin_url": (await get_setting_async("jellyfin_url", os.getenv("JELLYFIN_URL", "http://localhost:8096"))).rstrip("/"),
        "jellyfin_api_key": await get_setting_async("jellyfin_api_key", os.getenv("JELLYFIN_API_KEY", "")),
        "jellyfin_task_id": await get_setting_async("jellyfin_task_id", os.getenv("JELLYFIN_TASK_ID", ""))
    }

async def refresh_jellyfin_guide() -> RefreshResult:
    """Trigger Jellyfin's Refresh Guide task. The result says which step failed
    (unreachable / API key rejected / no such task); see jellyfin_client."""
    return await refresh_guide(await get_jellyfin_config())


async def trigger_jellyfin_refresh() -> bool:
    return (await refresh_jellyfin_guide()).ok


JELLYFIN_AUTO_REFRESH_MIN_INTERVAL = bounded_float(
    os.getenv("JELLYFIN_AUTO_REFRESH_MIN_INTERVAL", "600"), 600.0, 60.0, 86400.0
)
_JELLYFIN_REFRESH_STATE: Dict[str, Any] = {"signature": None, "last_run": 0.0, "pending": None, "listed": None}
# A channel that just became listed reaches Jellyfin this soon (a game is
# about to start), however recently the guide was last refreshed.
NEW_CHANNEL_REFRESH_MIN_INTERVAL = 60.0


def _listed_channels() -> frozenset:
    return frozenset(team_id for team_id, data in list(stream_state.items())
                     if catalog._channel_listed(data, team_id=team_id))


def _guide_signature() -> str:
    """Hash of everything Jellyfin's guide shows for our channels. Scrapes only
    trigger a guide refresh when this changes - previously every successful
    5-minute rescrape of every channel queued a full Jellyfin guide refresh."""
    parts = [f"show_offseason={catalog.SHOW_OFFSEASON_CHANNELS}", f"listing={catalog.LISTING_MODE}/{catalog.LISTING_LEAD_MINUTES}"]
    for team_id, data in sorted(stream_state.items()):
        parts.append("\0".join(str(value) for value in (
            team_id,
            data.get("name", ""),
            data.get("logo_url", ""),
            data.get("start_time", ""),
            data.get("stop_time", ""),
            data.get("schedule_status", ""),
            data.get("tvg_id", ""),
            data.get("group_title", ""),
            bool(data.get("candidates")) or is_multiview(data),
            # Changes with the clock too (game day starts, a game ends), which
            # guide_listing_watch_loop picks up.
            catalog._channel_listed(data, team_id=team_id),
        )))
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()


async def _debounced_jellyfin_refresh(delay: float) -> None:
    try:
        await asyncio.sleep(delay)
        # Running now: never cancelled to be brought forward.
        _JELLYFIN_REFRESH_STATE["pending_fast"] = True
        signature = _guide_signature()
        if signature == _JELLYFIN_REFRESH_STATE["signature"]:
            return
        _JELLYFIN_REFRESH_STATE["signature"] = signature
        _JELLYFIN_REFRESH_STATE["last_run"] = time.monotonic()
        _JELLYFIN_REFRESH_STATE["listed"] = _listed_channels()
        await trigger_jellyfin_refresh()
    finally:
        # A cancelled (brought-forward) refresh must not clear its replacement.
        if _JELLYFIN_REFRESH_STATE["pending"] is asyncio.current_task():
            _JELLYFIN_REFRESH_STATE["pending"] = None


def request_jellyfin_guide_refresh_if_changed() -> None:
    """Refresh Jellyfin's guide for automatic (scrape/schedule-driven) changes only
    when the guide contents actually changed, at most once per
    JELLYFIN_AUTO_REFRESH_MIN_INTERVAL. User-initiated changes still call
    trigger_jellyfin_refresh() directly for an immediate refresh."""
    if _guide_signature() == _JELLYFIN_REFRESH_STATE["signature"]:
        return
    previous = _JELLYFIN_REFRESH_STATE["listed"]
    channel_appeared = previous is not None and bool(_listed_channels() - previous)
    pending = _JELLYFIN_REFRESH_STATE["pending"]
    if _JELLYFIN_REFRESH_STATE["last_run"] == 0.0:
        # First automatic refresh after startup: wait a minute so the initial
        # burst of channel scrapes lands in one refresh instead of the first one.
        delay = 60.0
    else:
        elapsed = time.monotonic() - _JELLYFIN_REFRESH_STATE["last_run"]
        interval = NEW_CHANNEL_REFRESH_MIN_INTERVAL if channel_appeared else JELLYFIN_AUTO_REFRESH_MIN_INTERVAL
        delay = max(0.0, interval - elapsed)
    if pending is not None:
        if not channel_appeared or _JELLYFIN_REFRESH_STATE.get("pending_fast"):
            return
        # A slow refresh is queued but a channel just appeared: bring it forward.
        pending.cancel()
    _JELLYFIN_REFRESH_STATE["pending_fast"] = channel_appeared
    _JELLYFIN_REFRESH_STATE["pending"] = _spawn_background_task(
        _debounced_jellyfin_refresh(delay), "debounced Jellyfin guide refresh"
    )


GUIDE_LISTING_CHECK_INTERVAL = 60.0
RECORDING_CHECK_INTERVAL = 300.0


async def refresh_recording_channels() -> None:
    """Remember which channels have a Jellyfin recording set up (they stay
    listed). Unchanged when Jellyfin can't be reached."""
    channels = await fetch_recording_channels(await get_jellyfin_config())
    if channels is not None:
        catalog.RECORDING_TVG_IDS = frozenset(channels)


async def guide_listing_watch_loop() -> None:
    """Which channels Jellyfin should list changes without a scrape (a game is
    about to start, a game day begins, a game ends, a recording is set up);
    refresh the guide when it does."""
    last_recording_check = float("-inf")
    while True:
        await asyncio.sleep(GUIDE_LISTING_CHECK_INTERVAL)
        try:
            if catalog.LISTING_MODE == catalog.LISTING_ALL:
                continue
            if time.monotonic() - last_recording_check >= RECORDING_CHECK_INTERVAL:
                last_recording_check = time.monotonic()
                await refresh_recording_channels()
            check_guide_listing()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_failure("guide listing check", exc)


def check_guide_listing() -> None:
    if catalog.LISTING_MODE != catalog.LISTING_ALL:
        request_jellyfin_guide_refresh_if_changed()
