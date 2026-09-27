# First: config resolves DATA_DIR, loads .env and starts logging before anything else.
import config  # noqa: F401

import asyncio
import json
import logging
import math
import os
import random
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
import webbrowser
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Set, Tuple

import httpx
from fastapi import Depends, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from playwright.async_api import async_playwright

from config import (
    _clean_label,
    _find_available_port,
    _log_failure,
    _LOG_LISTENER,
    _m3u_attribute,
    _m3u_title,
    _PLAYWRIGHT_PATH_SET_BY_APP,
    _positive_env_number,
    _public_base_url,
    _resource_path,
    _safe_team_id,
    _upstream_media_headers,
    _validate_upstream_url,
    _xml_attr,
    _xml_text,
    BUNDLE_DIR,
    DATA_DIR,
    LOG_FILE,
    LOGGER,
)
import state
from state import (
    _cancel_background_tasks,
    _media_client,
    _spawn_background_task,
    PLACEHOLDER_SESSION_ID,
    stream_state,
)
from db import (
    _bulk_set_favorite_sync,
    _export_teams_sync,
    _fetch_expired_scheduled_teams,
    _load_dashboard_metrics_async,
    _METRIC_WRITER,
    _performance_stats_sync,
    _playback_stats_sync,
    _record_stream_test_sync,
    _schedule_team_disable_sync,
    _toggle_favorite_sync,
    close_all_db_connections,
    delete_multiview_channel_async,
    delete_team_async,
    get_setting,
    get_setting_async,
    init_db,
    load_multiview_channels,
    load_teams,
    log_metric_event_async,
    prune_database_logs,
    save_multiview_channel_async,
    save_team_async,
    set_setting_async,
    update_team_meta_async,
)
import security
from security import _configure_dashboard_auth, CsrfOriginMiddleware, verify_dashboard_auth
import scrapers
from scrapers import (
    _provider_url_setting_key,
    _set_provider_url_override,
    ACTIVE_PROVIDERS,
    HtmlAggregatorScraper,
    master_scrape,
)
import catalog
from catalog import (
    _channel_group_title,
    _channel_is_always_live,
    _channel_is_off_season,
    _channel_listed,
    _channel_logo_url,
    _channel_tvg_id,
    _season_resume_label,
    _special_channel_for,
    _sport_labeled_name,
    fetch_espn_team_schedule,
    get_catalog_entries,
    is_in_season,
    is_stream_window_active,
    parse_team_schedule,
    resolve_espn_logo,
    SCRAPE_REFRESH_SECONDS,
    STREAM_LEAD_TIME,
    STREAM_TRAIL_TIME,
    xmltv_ts,
)
from alerts import (
    get_jellyfin_config,
    get_notification_config,
    request_jellyfin_guide_refresh_if_changed,
    send_alert,
    trigger_jellyfin_refresh,
)
from updates import _update_available, _UPDATE_STATE, check_for_update, update_check_loop
import legacy_proxy
from legacy_proxy import (
    _fetch_upstream_body,
    _hls_response,
    _legacy_proxy_stream,
    _STARTUP_BUFFER_TASKS,
    CHUNK_CACHE,
    HLS_MEDIA_TYPE,
    PREFETCH_CONCURRENCY,
)
from hls_session import FetchResult, SessionConfig, SessionHooks, SessionRegistry, SourceSpec
from network_safety import bounded_float, bounded_int
from sports_matcher import get_team_search_terms
from stream_extractor import DEFAULT_USER_AGENT, verify_stream_live
from version import __version__


async def enforce_scheduled_disables_once() -> None:
    """Remove teams whose scheduled auto-disable date has passed."""
    try:
        expired = await asyncio.to_thread(_fetch_expired_scheduled_teams)
    except Exception as exc:
        _log_failure("query scheduled team disables", exc)
        return
    if not expired:
        return
    for team_id, name in expired:
        try:
            await _remove_channel(team_id)
            LOGGER.info("Auto-disabled scheduled team=%s name=%s", team_id, name)
        except Exception as exc:
            _log_failure(f"auto-disable team={team_id}", exc)
    _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after scheduled disable")


async def enforce_scheduled_disables():
    while True:
        try:
            await enforce_scheduled_disables_once()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_failure("scheduled disable enforcement", exc)
        await asyncio.sleep(3600)


def _maybe_record_playback_event(team_id: str) -> None:
    """Fire-and-forget, de-duplicated so Jellyfin's frequent manifest re-polling
    during one viewing session doesn't produce dozens of rows for it."""
    now = time.monotonic()
    if now - _LAST_PLAYBACK_EVENT.get(team_id, 0.0) < PLAYBACK_EVENT_DEDUPE_SECONDS:
        return
    _LAST_PLAYBACK_EVENT[team_id] = now
    _spawn_background_task(
        _METRIC_WRITER.enqueue(("playback_event", (team_id,), {})),
        f"record playback event team={team_id}",
    )


_SCRAPE_IN_FLIGHT: Set[str] = set()

_TEAM_SCRAPE_TASKS: Dict[str, asyncio.Task] = {}
_TEAM_SCRAPE_WAKE_EVENTS: Dict[str, asyncio.Event] = {}
_TEAM_STATE_LOCKS: Dict[str, asyncio.Lock] = {}


def _team_state_lock(team_id: str) -> asyncio.Lock:
    lock = _TEAM_STATE_LOCKS.get(team_id)
    if lock is None:
        lock = asyncio.Lock()
        _TEAM_STATE_LOCKS[team_id] = lock
    return lock


def _start_team_scrape_loop(team_id: str, initial_delay: float = 0.0) -> asyncio.Task:
    existing = _TEAM_SCRAPE_TASKS.get(team_id)
    if existing and not existing.done():
        return existing

    _TEAM_SCRAPE_WAKE_EVENTS.setdefault(team_id, asyncio.Event())
    task = asyncio.create_task(team_scrape_loop(team_id, initial_delay), name=f"scrape team={team_id}")
    _TEAM_SCRAPE_TASKS[team_id] = task

    def _scrape_finished(done: asyncio.Task) -> None:
        if _TEAM_SCRAPE_TASKS.get(team_id) is done:
            _TEAM_SCRAPE_TASKS.pop(team_id, None)
        if done.cancelled():
            return
        try:
            exception = done.exception()
        except asyncio.CancelledError:
            return
        if exception:
            _log_failure(f"team scrape loop team={team_id}", exception, logging.ERROR)

    task.add_done_callback(_scrape_finished)
    return task


async def _stop_team_scrape_loop(team_id: str) -> None:
    wake_event = _TEAM_SCRAPE_WAKE_EVENTS.pop(team_id, None)
    if wake_event:
        wake_event.set()
    task = _TEAM_SCRAPE_TASKS.pop(team_id, None)
    if task and not task.done():
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    _TEAM_STATE_LOCKS.pop(team_id, None)


ACTIVE_HEALTH_INTERVAL = _positive_env_number("ACTIVE_HEALTH_INTERVAL", 3.0)
STANDBY_HEALTH_INTERVAL = _positive_env_number("STANDBY_HEALTH_INTERVAL", 45.0)
STANDBY_HEALTH_CONCURRENCY = bounded_int(os.getenv("STANDBY_HEALTH_CONCURRENCY", "4"), 4, 1, 32)
ACTIVE_HEALTH_CONCURRENCY = bounded_int(os.getenv("ACTIVE_HEALTH_CONCURRENCY", "8"), 8, 1, 64)
EMERGENCY_SCRAPE_COOLDOWN = _positive_env_number("EMERGENCY_SCRAPE_COOLDOWN", 60.0)

# Jellyfin re-polls a channel's manifest every few seconds during live playback, so
# recording a playback_events row on every proxy_stream() call would count one viewing
# session as dozens of rows. De-dupe to at most one row per team per this window.
PLAYBACK_EVENT_DEDUPE_SECONDS = 300.0
_LAST_PLAYBACK_EVENT: Dict[str, float] = {}


def _scrape_lifecycle_defaults() -> dict:
    return {
        "scrape_in_progress": False,
        "last_scrape_started": 0.0,
        "last_scrape_completed": 0.0,
        "scrape_result": "pending",
        "scrape_error": "",
    }


def _mark_scrape_started(data: dict) -> None:
    data["scrape_in_progress"] = True
    data["last_scrape_started"] = time.time()
    data["scrape_error"] = ""
    data["scrape_result"] = "running"


def _mark_scrape_finished(data: dict, result: str, error: str = "") -> None:
    data["scrape_in_progress"] = False
    data["last_scrape_completed"] = time.time()
    data["scrape_result"] = result
    data["scrape_error"] = error


_CANDIDATE_HEALTH_FIELDS = (
    "last_health_check", "last_health_ok", "consecutive_failures", "session_compatible", "incompatible_at",
    "probe_state", "has_audio", "codec_signature",
)


_TOKEN_PATH_SEGMENT_RE = re.compile(r"^[A-Za-z0-9_\-.~=]{24,}$")


def _is_token_path_segment(segment: str) -> bool:
    """Long mixed letters+digits segments (or signed 'exp=...~hmac=...' ones) are
    rotating tokens, not stream identity. Channel names/ids are short."""
    if "=" in segment and ("exp" in segment.lower() or "hmac" in segment.lower() or "token" in segment.lower()):
        return True
    return bool(
        _TOKEN_PATH_SEGMENT_RE.match(segment)
        and any(c.isdigit() for c in segment)
        and any(c.isalpha() for c in segment)
    )


def candidate_source_key(candidate: dict) -> Tuple[str, str, str]:
    """Identity of a stream source ignoring its query string and token-like path
    segments: aggregator URLs carry rotating tokens, so the same CDN stream gets
    a new URL on every rescrape (a new key lost its health history and made a
    token refresh look like a source switch)."""
    parts = urllib.parse.urlsplit(str(candidate.get("url") or ""))
    path = "/".join("*" if _is_token_path_segment(seg) else seg for seg in parts.path.split("/"))
    return (str(candidate.get("provider") or "").lower(), parts.netloc.lower(), path)


def _merge_stream_candidates(
    previous: List[dict],
    active_index: int,
    fresh: List[dict],
    keep_active: bool,
) -> Tuple[List[dict], int]:
    """Merge a rescrape's candidates into the current list without disturbing playback.

    Previously every rescrape (every 5 minutes, forever, for 24/7 channels) replaced
    the list and reset active_index to 0 - an unsignaled mid-playback source swap
    even when the current stream was perfectly healthy. Now a healthy active
    candidate stays first (and stays the object the channel session is playing;
    if the rescrape found the same source with a fresh token, only its URL is
    refreshed), fresh candidates become standbys, and known standbys keep their
    health history. Returns (candidates, active_index).
    """
    previous_by_key = {candidate_source_key(c): c for c in previous if c.get("url")}
    merged: List[dict] = []
    seen: Set[Tuple[str, str, str]] = set()

    if keep_active and 0 <= active_index < len(previous):
        active = previous[active_index]
        if active.get("url"):
            active_key = candidate_source_key(active)
            for candidate in fresh:
                if candidate.get("url") and candidate_source_key(candidate) == active_key:
                    for field in ("url", "referer", "origin"):
                        if candidate.get(field):
                            active[field] = candidate[field]
                    break
            merged.append(active)
            seen.add(active_key)

    for candidate in fresh:
        if not candidate.get("url"):
            continue
        key = candidate_source_key(candidate)
        if key in seen:
            continue
        old = previous_by_key.get(key)
        if old is not None:
            for field in _CANDIDATE_HEALTH_FIELDS:
                if field in old and field not in candidate:
                    candidate[field] = old[field]
        merged.append(candidate)
        seen.add(key)

    merged = merged[:scrapers.MAX_STREAM_CANDIDATES]
    kept_active = (
        keep_active and bool(merged) and 0 <= active_index < len(previous)
        and merged[0] is previous[active_index]
    )
    if kept_active:
        return merged, 0
    # Nothing to keep: start at the best-health source, not blindly at [0]
    # (which may have failed seconds ago).
    return merged, _best_candidate_index(merged)


async def trigger_scrape(team_id: str, force: bool = False):
    if team_id in _SCRAPE_IN_FLIGHT:
        LOGGER.debug("Skipping duplicate scrape team=%s", team_id)
        return
    data = stream_state.get(team_id)
    if not data:
        return
    _SCRAPE_IN_FLIGHT.add(team_id)
    _mark_scrape_started(data)
    try:
        await _trigger_scrape(team_id, force=force)
    except asyncio.CancelledError:
        _mark_scrape_finished(data, "cancelled")
        raise
    except Exception as exc:
        _mark_scrape_finished(data, "failed", type(exc).__name__)
        raise
    else:
        _mark_scrape_finished(data, "healthy" if data.get("candidates") else "empty")
    finally:
        _SCRAPE_IN_FLIGHT.discard(team_id)


def _resolve_schedule_status(
    start_utc: Optional[datetime],
    stop_utc: Optional[datetime],
    *,
    in_season: bool,
    always_live: bool,
    schedule_ok: bool,
    previous_start: str,
    previous_stop: str,
) -> Tuple[str, str, str]:
    """Pure decision logic extracted from _trigger_scrape: which stream-window state
    wins when a freshly-found event, an off-season league, a confirmed no-event
    response, and a failed/stale lookup can all be true in different combinations.
    Kept standalone (no stream_state, no I/O) so this branching — previously only
    ever exercised end-to-end via a live ESPN fetch — can be unit tested directly."""
    if start_utc and stop_utc:
        return xmltv_ts(start_utc), xmltv_ts(stop_utc), "scheduled"
    if not in_season and not always_live:
        return "", "", "off_season"
    if schedule_ok and not always_live:
        # A successful empty response is authoritative. Do not retain a past
        # event, because it can make a current event appear out of window.
        return "", "", "no_event"
    return previous_start, previous_stop, ("always_live" if always_live else "lookup_failed")


async def _trigger_scrape_unlocked(team_id: str, force: bool = False):
    if team_id not in stream_state:
        return
    query = stream_state[team_id]["query"]
    team_name = stream_state[team_id]["name"]
    category = stream_state[team_id].get("category", "")
    source_id = stream_state[team_id].get("source_id", "")
    always_live = bool(stream_state[team_id].get("always_live"))
    in_season = is_in_season(category)
    logo_url = resolve_espn_logo(team_name, category, source_id)
    previous_start = stream_state[team_id].get("start_time", "")
    previous_stop = stream_state[team_id].get("stop_time", "")
    if always_live:
        start_utc, stop_utc = None, None
        schedule_ok = True
    elif not in_season:
        # Off-season: skip the ESPN schedule lookup entirely rather than
        # polling a league that has no games to find.
        start_utc, stop_utc = None, None
        schedule_ok = True
    else:
        start_utc, stop_utc, schedule_ok = await fetch_espn_team_schedule(team_name, query, category, source_id)

    current = stream_state.get(team_id)
    if current is None:
        return

    start_str, stop_str, schedule_status = _resolve_schedule_status(
        start_utc, stop_utc,
        in_season=in_season, always_live=always_live, schedule_ok=schedule_ok,
        previous_start=previous_start, previous_stop=previous_stop,
    )
    current["schedule_status"] = schedule_status

    current["logo_url"] = logo_url or current.get("logo_url", "")
    current["start_time"] = start_str
    current["stop_time"] = stop_str
    await update_team_meta_async(team_id, current["logo_url"], start_str, stop_str)
    current = stream_state.get(team_id)
    if current is None:
        return

    if _stream_window_should_close(team_id, current):
        current["candidates"] = []
        current["active_index"] = 0
        current["is_healthy"] = False
        current["exhausted"] = False
        return

    if (
        not force
        and current.get("candidates")
        and current.get("is_healthy")
        and not current.get("exhausted")
        and time.time() - current.get("last_candidate_refresh", 0.0) < HEALTHY_RESCRAPE_SECONDS
    ):
        # A healthy channel doesn't need its providers re-searched every 5 minutes
        # (that was constant Playwright load for every 24/7 channel); refresh the
        # standby list at HEALTHY_RESCRAPE_SECONDS, or sooner once it degrades.
        return

    previous_candidates = current.get("candidates", [])
    new_candidates = await master_scrape(
        query,
        team_name=team_name,
        team_id=team_id,
        search_terms=current.get("search_terms") or None,
        always_live=always_live,
    )
    current = stream_state.get(team_id)
    if current is None:
        return
    was_healthy = current.get("is_healthy", False)
    if new_candidates:
        merged, merged_index = _merge_stream_candidates(
            current.get("candidates", []),
            current.get("active_index", 0),
            new_candidates,
            keep_active=bool(was_healthy),
        )
        current["candidates"] = merged
        current["active_index"] = merged_index
        current["exhausted"] = False
        current["last_candidate_refresh"] = time.time()
        is_healthy = True
        current["is_healthy"] = True
        SESSIONS.poke(team_id)
    elif previous_candidates:
        LOGGER.warning(
            "Keeping existing stream candidates after empty refresh team=%s count=%d",
            team_id,
            len(previous_candidates),
        )
        is_healthy = current.get("is_healthy", False)
    else:
        current["candidates"] = []
        current["active_index"] = 0
        is_healthy = False
        current["is_healthy"] = False

    if not current.get("logo_url"):
        for c in new_candidates:
            if c.get("logo_url"):
                current["logo_url"] = c["logo_url"]
                break

    if is_healthy and not was_healthy and len(new_candidates) > 0:
        _spawn_background_task(
            send_alert("✅ Stream Available", f"Stream active for **{team_name}**.", "success"),
            f"send stream-available alert team={team_id}",
        )

    if new_candidates:
        request_jellyfin_guide_refresh_if_changed()


async def _trigger_scrape(team_id: str, force: bool = False):
    """Serialize all scrape-driven updates for one channel."""
    async with _team_state_lock(team_id):
        await _trigger_scrape_unlocked(team_id, force=force)


async def team_scrape_loop(team_id: str, initial_delay: float = 0.0):
    if initial_delay > 0:
        # Spread startup scrapes out instead of launching every channel's provider
        # searches (and Playwright pages) in the same instant.
        await asyncio.sleep(initial_delay)
    while team_id in stream_state:
        scrape_failed = False
        try:
            await trigger_scrape(team_id)
        except Exception as exc:
            scrape_failed = True
            _log_failure(f"scheduled scrape team={team_id}", exc, logging.ERROR)

        data = stream_state.get(team_id)
        if not data:
            return
        start, stop = parse_team_schedule(data)
        delay = SCRAPE_REFRESH_SECONDS
        if data.get("schedule_status") == "off_season":
            # No point polling ESPN every few minutes for a sport that won't
            # have games for months; a daily check-in is plenty, and a
            # manual rescrape or the season starting still wakes it early.
            delay = 86400
        elif start and stop:
            now = datetime.now(timezone.utc)
            window_start = start - STREAM_LEAD_TIME
            if now < window_start:
                delay = min(SCRAPE_REFRESH_SECONDS, max(30, int((window_start - now).total_seconds())))
            elif now > stop + STREAM_TRAIL_TIME:
                delay = min(SCRAPE_REFRESH_SECONDS, 900)
        if scrape_failed:
            delay = min(delay, 60)
        # +/-10% jitter keeps channels that started together from re-scraping in
        # lockstep (a pool/CPU/Chromium spike every SCRAPE_REFRESH_SECONDS).
        delay = max(15.0, delay * random.uniform(0.9, 1.1))
        wake_event = _TEAM_SCRAPE_WAKE_EVENTS.get(team_id)
        if wake_event is None:
            return
        try:
            await asyncio.wait_for(wake_event.wait(), timeout=delay)
            wake_event.clear()
        except asyncio.TimeoutError:
            pass


IDLE_HEALTH_INTERVAL = _positive_env_number("IDLE_HEALTH_INTERVAL", 30.0)
HEALTH_FAILURE_THRESHOLD = bounded_int(os.getenv("HEALTH_FAILURE_THRESHOLD", "2"), 2, 1, 10)
HEALTH_PROBE_TIMEOUT = _positive_env_number("HEALTH_PROBE_TIMEOUT", 12.0)
STANDBY_PROBES_PER_CHANNEL = bounded_int(os.getenv("STANDBY_PROBES_PER_CHANNEL", "5"), 5, 1, 50)
UNWATCHED_STANDBY_INTERVAL = _positive_env_number("UNWATCHED_STANDBY_INTERVAL", 300.0)
HEALTHY_RESCRAPE_SECONDS = _positive_env_number("HEALTHY_RESCRAPE_SECONDS", 1800.0)
STARTUP_SCRAPE_SPREAD_SECONDS = bounded_float(os.getenv("STARTUP_SCRAPE_SPREAD_SECONDS", "45"), 45.0, 0.0, 600.0)
# While every candidate is marked failed, keep probing them all (watched or
# not) and recover on the first that plays again.
EXHAUSTED_PROBE_INTERVAL = _positive_env_number("EXHAUSTED_PROBE_INTERVAL", 20.0)
# Outside the scheduled window a channel is only closed after it has not been
# flowing for this long (one missed sample used to end overtime games).
WINDOW_CLOSE_GRACE_SECONDS = _positive_env_number("WINDOW_CLOSE_GRACE_SECONDS", 180.0)
# A channel's only source stalls: retry it this many times before No Signal.
SELF_RETRY_LIMIT = bounded_int(os.getenv("SELF_RETRY_LIMIT", "2"), 2, 0, 10)
SELF_RETRY_WINDOW = _positive_env_number("SELF_RETRY_WINDOW", 120.0)
# Session-incompatible sources (fMP4, separate audio) get another chance later.
INCOMPATIBLE_RETRY_SECONDS = _positive_env_number("INCOMPATIBLE_RETRY_SECONDS", 3600.0)
FAILOVER_ALERT_COOLDOWN = _positive_env_number("FAILOVER_ALERT_COOLDOWN", 300.0)
TOKEN_REFRESH_COOLDOWN = _positive_env_number("TOKEN_REFRESH_COOLDOWN", 120.0)
# Failover candidate tiers: known-good within this long; failed longer ago than
# FAILED_RETRY_AFTER is worth another try.
CANDIDATE_GOOD_FOR_SECONDS = _positive_env_number("CANDIDATE_GOOD_FOR_SECONDS", 180.0)
CANDIDATE_FAILED_RETRY_AFTER = _positive_env_number("CANDIDATE_FAILED_RETRY_AFTER", 60.0)
_ACTIVE_PROBE_SEMAPHORE: Optional[asyncio.Semaphore] = None


def _candidate_tier(candidate: dict, now: float) -> int:
    """0 known-good recently, 1 unknown, 2 failed a while ago, 3 failed moments ago."""
    ok = candidate.get("last_health_ok")
    checked = float(candidate.get("last_health_check") or 0.0)
    if ok is True and now - checked < CANDIDATE_GOOD_FOR_SECONDS:
        return 0
    if ok is None or ok is True:
        return 1
    if now - checked > CANDIDATE_FAILED_RETRY_AFTER:
        return 2
    return 3


def _candidate_session_compatible(candidate: dict, now: float) -> bool:
    if candidate.get("session_compatible", True) is not False:
        return True
    return now - float(candidate.get("incompatible_at") or 0.0) > INCOMPATIBLE_RETRY_SECONDS


def _best_candidate_index(candidates: List[dict]) -> int:
    """Where a (re)built candidate list should start: the best-health usable
    source, list order breaking ties (the list is already ranked by quality)."""
    if not candidates:
        return 0
    now = time.time()
    ranked = sorted(
        range(len(candidates)),
        key=lambda i: (
            0 if _candidate_session_compatible(candidates[i], now) else 1,
            _candidate_tier(candidates[i], now),
            i,
        ),
    )
    return ranked[0]


def _pick_next_candidate(
    candidates: List[dict],
    current_index: int,
    prefer_signature: Optional[tuple] = None,
) -> Optional[int]:
    """Choose the failover target: the freshest known-good standby first, then
    never-checked ones, then ones that failed a while ago - wrapping around the
    list instead of giving up at the end (the old code only ever tried
    active_index + 1). Candidates the channel session found unplayable
    (fMP4 / separate audio) are skipped, and a same-codec source is preferred
    so Jellyfin's decoder doesn't have to switch codecs mid-stream."""
    count = len(candidates)
    if count < 2:
        return None
    now = time.time()
    order = [(current_index + step) % count for step in range(1, count)]

    def rank(index: int) -> Tuple[int, int, int]:
        candidate = candidates[index]
        tier = _candidate_tier(candidate, now)  # 3 = failed moments ago; not worth switching to
        signature = candidate.get("codec_signature")
        codec_penalty = 0 if prefer_signature is None or signature in (None, prefer_signature) else 1
        return tier, codec_penalty, order.index(index)

    eligible = [
        index for index in order
        if _candidate_session_compatible(candidates[index], now) and rank(index)[0] < 3
    ]
    return min(eligible, key=rank) if eligible else None


async def request_failover(team_id: str, source_key: tuple, reason: str, incompatible: bool = False) -> bool:
    """Single failover entry point for channel sessions (real playback) and
    health probes (unwatched channels). A no-op if the active candidate already
    changed, so concurrent reporters can't double-advance."""
    data = stream_state.get(team_id)
    if not data or data.get("type") == "multiview" or _is_placeholder_key(source_key):
        return False
    candidates = data.get("candidates") or []
    if not candidates:
        return False
    active_index = data.get("active_index", 0)
    if active_index >= len(candidates) or candidate_source_key(candidates[active_index]) != tuple(source_key):
        return False

    active = candidates[active_index]
    active["last_health_ok"] = False
    active["last_health_check"] = time.time()
    if incompatible:
        active["session_compatible"] = False
        active["incompatible_at"] = time.time()
    team_name = data.get("name", team_id)
    if reason == "playlist forbidden":
        _request_token_refresh(team_id, data)
    next_index = _pick_next_candidate(candidates, active_index, active.get("codec_signature"))
    if next_index is None:
        if not incompatible and _allow_self_retry(data):
            # The only (usable) source stalled. A stall is often brief, so give
            # it another stale window instead of going straight to No Signal.
            LOGGER.info("Retrying the same source team=%s reason=%s", team_id, reason)
            return False
        data["is_healthy"] = False
        data["exhausted"] = True
        data["exhausted_since"] = time.monotonic()
        SESSIONS.poke(team_id)
        _handle_candidates_exhausted(team_id, data, team_name)
        return False

    data["active_index"] = next_index
    data["exhausted"] = False
    data["self_retries"] = 0
    # The new source starts with a clean failure count (a stale count left
    # from an earlier stint made its first failed probe fail over at once).
    candidates[next_index]["consecutive_failures"] = 0
    new_provider = candidates[next_index].get("provider", "Unknown")
    SESSIONS.poke(team_id)
    LOGGER.info(
        "Failover team=%s from=%s to=%s reason=%s",
        team_id, active.get("provider", "Unknown"), new_provider, reason,
    )
    await log_metric_event_async(team_id, new_provider, "failover", reason)
    data["failover_count"] = int(data.get("failover_count", 0)) + 1
    data["last_failover"] = {"at": time.time(), "reason": reason, "to": new_provider}
    _send_failover_alert(team_id, data, team_name, new_provider)
    return True


def _allow_self_retry(data: dict) -> bool:
    now = time.monotonic()
    if now - float(data.get("self_retry_window_start") or 0.0) > SELF_RETRY_WINDOW:
        data["self_retry_window_start"] = now
        data["self_retries"] = 0
    if int(data.get("self_retries", 0)) >= SELF_RETRY_LIMIT:
        return False
    data["self_retries"] = int(data.get("self_retries", 0)) + 1
    return True


def _send_failover_alert(team_id: str, data: dict, team_name: str, new_provider: str) -> None:
    """At most one failover alert per channel per FAILOVER_ALERT_COOLDOWN; a
    flapping channel used to send one every few seconds (and got the webhook
    rate-limited right before the more important 'exhausted' alert)."""
    now = time.monotonic()
    last = data.get("last_failover_alert")
    if last is not None and now - last < FAILOVER_ALERT_COOLDOWN:
        data["suppressed_failover_alerts"] = int(data.get("suppressed_failover_alerts", 0)) + 1
        return
    suppressed = int(data.get("suppressed_failover_alerts", 0))
    data["last_failover_alert"] = now
    data["suppressed_failover_alerts"] = 0
    extra = f" ({suppressed} more failover(s) since the last alert)" if suppressed else ""
    _spawn_background_task(
        send_alert("⚠️ Stream Failover", f"Failed over to {new_provider} for **{team_name}**.{extra}", "warning"),
        f"send failover alert team={team_id}",
    )


def _request_token_refresh(team_id: str, data: dict) -> None:
    """The active playlist answered 401/403: its token probably expired, and
    standbys from the same scrape carry tokens just as old. Rescrape now (the
    merge keeps health history) rather than waiting for the next cycle."""
    now = time.monotonic()
    if now - float(data.get("last_token_refresh") or 0.0) < TOKEN_REFRESH_COOLDOWN:
        return
    data["last_token_refresh"] = now
    LOGGER.info("Refreshing stream tokens team=%s", team_id)
    _spawn_background_task(_trigger_scrape(team_id, force=True), f"token refresh rescrape team={team_id}")


async def _probe_exhausted_candidates(team_id: str, data: dict) -> None:
    """Recover an exhausted channel as soon as any known source plays again,
    instead of waiting for a rescrape to return candidates."""
    try:
        candidates = list(data.get("candidates") or [])
        if not candidates:
            return
        semaphore = asyncio.Semaphore(STANDBY_HEALTH_CONCURRENCY)
        await asyncio.gather(
            *(_probe_standby_candidate(candidate, semaphore) for candidate in candidates),
            return_exceptions=True,
        )
        current = stream_state.get(team_id)
        if current is not data or not data.get("exhausted") or data.get("candidates") is None:
            return
        now = time.time()
        for index, candidate in enumerate(data["candidates"]):
            if candidate.get("last_health_ok") is True and _candidate_session_compatible(candidate, now):
                data["active_index"] = index
                data["exhausted"] = False
                data["is_healthy"] = True
                data["self_retries"] = 0
                candidate["consecutive_failures"] = 0
                SESSIONS.poke(team_id)
                LOGGER.info(
                    "Recovered exhausted channel team=%s provider=%s",
                    team_id, candidate.get("provider", "Unknown"),
                )
                return
    finally:
        data["exhausted_probe_in_flight"] = False


def _handle_candidates_exhausted(team_id: str, data: dict, team_name: str) -> None:
    current_time = time.monotonic()
    if current_time - data.get("last_exhausted_alert", 0.0) >= EMERGENCY_SCRAPE_COOLDOWN:
        data["last_exhausted_alert"] = current_time
        _spawn_background_task(
            send_alert("🚨 All Stream Candidates Exhausted", f"All candidates for **{team_name}** failed.", "danger"),
            f"send exhausted-stream alert team={team_id}",
        )
    if current_time - data.get("last_emergency_scrape", 0.0) >= EMERGENCY_SCRAPE_COOLDOWN:
        data["last_emergency_scrape"] = current_time
        _spawn_background_task(emergency_rescrape(team_id), f"emergency rescrape team={team_id}")


async def emergency_rescrape(team_id: str) -> None:
    data = stream_state.get(team_id)
    if data is None:
        return
    if team_id in _SCRAPE_IN_FLIGHT:
        LOGGER.debug("Skipping emergency scrape already in progress team=%s", team_id)
        return
    _SCRAPE_IN_FLIGHT.add(team_id)
    _mark_scrape_started(data)

    def _install_partial(streams: List[dict]) -> None:
        # First playable provider result while still off the air: use it now.
        current = stream_state.get(team_id)
        if current is None or not current.get("exhausted"):
            return
        merged, merged_index = _merge_stream_candidates(
            current.get("candidates", []), 0, streams, keep_active=False
        )
        current["candidates"] = merged
        current["active_index"] = merged_index
        current["exhausted"] = False
        current["is_healthy"] = True
        SESSIONS.poke(team_id)
        LOGGER.info("Emergency rescrape found streams early team=%s count=%d", team_id, len(streams))

    try:
        candidates = await master_scrape(
            data["query"],
            team_name=data.get("name", team_id),
            team_id=team_id,
            search_terms=data.get("search_terms") or None,
            always_live=bool(data.get("always_live")),
            on_partial=_install_partial,
        )
        current = stream_state.get(team_id)
        if current is not None:
            if candidates:
                # Every known candidate just failed, so nothing is kept active, but
                # standbys found again keep their health history. If an early
                # partial result is already playing, keep playing it.
                playing_early = not current.get("exhausted") and bool(current.get("candidates"))
                merged, merged_index = _merge_stream_candidates(
                    current.get("candidates", []), current.get("active_index", 0) if playing_early else 0,
                    candidates, keep_active=playing_early,
                )
                current["candidates"] = merged
                current["active_index"] = merged_index
                current["is_healthy"] = True
                current["exhausted"] = False
                current["last_candidate_refresh"] = time.time()
                SESSIONS.poke(team_id)
            else:
                LOGGER.warning(
                    "Keeping existing stream candidates after empty emergency refresh team=%s count=%d",
                    team_id,
                    len(current.get("candidates", [])),
                )
        _mark_scrape_finished(data, "healthy" if candidates else "empty")
    except asyncio.CancelledError:
        _mark_scrape_finished(data, "cancelled")
        raise
    except Exception as exc:
        _mark_scrape_finished(data, "failed", type(exc).__name__)
        _log_failure(f"emergency rescrape team={team_id}", exc, logging.ERROR)
    finally:
        _SCRAPE_IN_FLIGHT.discard(team_id)


async def check_stream_health(url: str, referer: str, origin: str = "", probe_state: Optional[dict] = None) -> bool:
    owns_client = state.SHARED_HTTP_CLIENT is None
    client = state.SHARED_HTTP_CLIENT or httpx.AsyncClient(timeout=5.0, follow_redirects=True, http2=True)
    try:
        # Origin must be forwarded: streams captured with one (Playwright-intercepted
        # providers) play through the proxy, which sends it, but failed every probe
        # without it - causing endless false failovers.
        # probe_state (kept on the candidate) lets the probe notice a playlist
        # that stopped advancing between probes, not just one that is reachable.
        return await verify_stream_live(client, url, referer, origin=origin, probe_state=probe_state)
    except Exception as exc:
        _log_failure("stream health check", exc)
        return False
    finally:
        if owns_client:
            await client.aclose()


def _session_on_placeholder(session) -> bool:
    source = getattr(session, "source", None)
    return source is not None and bool(source.key) and source.key[0] == "placeholder"


def _session_playing_real_source(session) -> bool:
    """Flowing with real content: the No Signal placeholder also produces
    segments, but a channel showing it is not healthy."""
    return session is not None and session.is_flowing() and not _session_on_placeholder(session)


def _session_flowing(team_id: str) -> bool:
    session = SESSIONS.peek(team_id)
    return session is not None and session.is_watched() and _session_playing_real_source(session)


def _stream_window_should_close(team_id: str, data: dict) -> bool:
    """Outside the scheduled window, but keep a game that runs long (overtime,
    rain delay) while people are watching it and it is still flowing. Closing
    needs WINDOW_CLOSE_GRACE_SECONDS without real playback, not one sample."""
    if is_stream_window_active(data) or _session_flowing(team_id):
        data.pop("window_close_pending_since", None)
        return False
    now = time.monotonic()
    pending_since = data.setdefault("window_close_pending_since", now)
    return now - pending_since >= WINDOW_CLOSE_GRACE_SECONDS


async def _probe_active_candidate(team_id: str, data: dict) -> None:
    global _ACTIVE_PROBE_SEMAPHORE
    if _ACTIVE_PROBE_SEMAPHORE is None:
        _ACTIVE_PROBE_SEMAPHORE = asyncio.Semaphore(ACTIVE_HEALTH_CONCURRENCY)
    candidates = data.get("candidates") or []
    active_index = data.get("active_index", 0)
    if active_index >= len(candidates):
        return
    candidate = candidates[active_index]
    try:
        async with _ACTIVE_PROBE_SEMAPHORE:
            try:
                is_alive = await asyncio.wait_for(
                    check_stream_health(
                        candidate["url"], candidate.get("referer", ""), candidate.get("origin", ""),
                        probe_state=candidate.setdefault("probe_state", {}),
                    ),
                    timeout=HEALTH_PROBE_TIMEOUT,
                )
            except asyncio.TimeoutError:
                is_alive = False
        # The channel may have failed over (or started being watched) meanwhile.
        if data.get("candidates") is not candidates or data.get("active_index", 0) != active_index:
            return
        candidate["last_health_check"] = time.time()
        if is_alive:
            candidate["last_health_ok"] = True
            candidate["consecutive_failures"] = 0
            data["is_healthy"] = True
            return
        candidate["consecutive_failures"] = candidate.get("consecutive_failures", 0) + 1
        if candidate["consecutive_failures"] < HEALTH_FAILURE_THRESHOLD:
            # One failed probe is often a transient blip; re-check soon before acting.
            data["next_active_probe"] = time.monotonic() + max(ACTIVE_HEALTH_INTERVAL * 2, 5.0)
            return
        candidate["last_health_ok"] = False
        data["is_healthy"] = False
        await request_failover(team_id, candidate_source_key(candidate), "health probe failed")
    finally:
        data["probe_in_flight"] = False


async def _probe_standby_candidate(candidate: dict, semaphore: asyncio.Semaphore) -> None:
    async with semaphore:
        try:
            is_alive = await asyncio.wait_for(
                check_stream_health(
                    candidate["url"], candidate.get("referer", ""), candidate.get("origin", ""),
                    probe_state=candidate.setdefault("probe_state", {}),
                ),
                timeout=HEALTH_PROBE_TIMEOUT,
            )
        except asyncio.TimeoutError:
            is_alive = False
    candidate["last_health_check"] = time.time()
    candidate["last_health_ok"] = is_alive
    if is_alive:
        candidate["consecutive_failures"] = 0


async def standby_health_loop() -> None:
    """Keep standby health fresh so failover picks a working source first. Runs
    on its own so a slow sweep never delays active-stream failure detection.
    Watched channels' standbys are probed every STANDBY_HEALTH_INTERVAL, others
    every UNWATCHED_STANDBY_INTERVAL, at most STANDBY_PROBES_PER_CHANNEL each."""
    last_unwatched_sweep = 0.0
    while True:
        await asyncio.sleep(STANDBY_HEALTH_INTERVAL)
        try:
            now = time.monotonic()
            include_unwatched = now - last_unwatched_sweep >= UNWATCHED_STANDBY_INTERVAL
            if include_unwatched:
                last_unwatched_sweep = now
            semaphore = asyncio.Semaphore(STANDBY_HEALTH_CONCURRENCY)
            probes = []
            for team_id, data in list(stream_state.items()):
                if data.get("type") == "multiview" or not data.get("candidates"):
                    continue
                if not include_unwatched and not SESSIONS.is_watched(team_id):
                    continue
                active_index = data.get("active_index", 0)
                standbys = [c for i, c in enumerate(data["candidates"]) if i != active_index]
                for candidate in standbys[:STANDBY_PROBES_PER_CHANNEL]:
                    probes.append(_probe_standby_candidate(candidate, semaphore))
            if probes:
                await asyncio.gather(*probes, return_exceptions=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_failure("standby health sweep", exc, logging.ERROR)


def _failover_monitor_tick() -> None:
    now = time.monotonic()
    for team_id, data in list(stream_state.items()):
        if data.get("type") == "multiview":
            entry = _MULTIVIEW_PROCESSES.get(team_id)
            data["is_healthy"] = bool(entry and entry.get("ready") and not entry.get("exited"))
            continue
        if _stream_window_should_close(team_id, data):
            if data.get("candidates"):
                data["candidates"] = []
                data["active_index"] = 0
                data["is_healthy"] = False
                data["exhausted"] = False
            continue
        candidates = data.get("candidates")
        if not candidates:
            continue
        if data.get("active_index", 0) >= len(candidates):
            data["active_index"] = 0

        if data.get("exhausted"):
            data["is_healthy"] = False
            if not data.get("exhausted_probe_in_flight") and now >= data.get("next_exhausted_probe", 0.0):
                data["exhausted_probe_in_flight"] = True
                data["next_exhausted_probe"] = now + EXHAUSTED_PROBE_INTERVAL
                _spawn_background_task(_probe_exhausted_candidates(team_id, data), f"exhausted probe team={team_id}")
            continue

        session = SESSIONS.peek(team_id)
        if session is not None and session.is_watched():
            # Real playback is the health signal for watched channels: the
            # session reports stale playlists / failing segments itself, so no
            # synthetic probes (which used to fail over on a single blip).
            data["is_healthy"] = _session_playing_real_source(session)
            continue

        if data.get("probe_in_flight") or now < data.get("next_active_probe", 0.0):
            continue
        data["probe_in_flight"] = True
        data["next_active_probe"] = now + IDLE_HEALTH_INTERVAL
        _spawn_background_task(_probe_active_candidate(team_id, data), f"health probe team={team_id}")


async def failover_monitor():
    standby_task = asyncio.create_task(standby_health_loop(), name="standby health")
    try:
        while True:
            try:
                _failover_monitor_tick()
            except Exception as exc:
                _log_failure("failover monitor iteration", exc, logging.ERROR)
            await asyncio.sleep(ACTIVE_HEALTH_INTERVAL)
    finally:
        standby_task.cancel()
        await asyncio.gather(standby_task, return_exceptions=True)


def _install_loop_exception_filter() -> None:
    """Windows' Proactor event loop reports every client that drops its TCP
    connection abruptly (Jellyfin stopping a stream, ffmpeg reconnecting) as an
    unhandled ConnectionResetError traceback from _call_connection_lost. On a
    24/7 server that is pure log noise; keep every other loop error."""
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()

    def handler(event_loop, context):
        exc = context.get("exception")
        if isinstance(exc, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
            return
        if previous is not None:
            previous(event_loop, context)
        else:
            event_loop.default_exception_handler(context)

    loop.set_exception_handler(handler)


@asynccontextmanager
async def lifespan(app: FastAPI):

    _install_loop_exception_filter()
    init_db()
    _METRIC_WRITER.start()
    # Before wiping the run dirs: their pid files identify ffmpeg left running
    # by a crashed previous instance (and those would keep the files locked).
    _kill_orphaned_ffmpeg()
    shutil.rmtree(MULTIVIEW_OUTPUT_ROOT, ignore_errors=True)
    MULTIVIEW_OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    shutil.rmtree(PLACEHOLDER_OUTPUT_DIR, ignore_errors=True)
    # Advanced settings saved from the dashboard override the env defaults.
    _load_tunable_overrides()
    catalog.SHOW_OFFSEASON_CHANNELS = get_setting("show_offseason_channels", "0") == "1"
    _spawn_background_task(update_check_loop(), "update check")
    stream_state.clear()
    _TEAM_SCRAPE_TASKS.clear()
    _SCRAPE_IN_FLIGHT.clear()
    _TEAM_STATE_LOCKS.clear()
    _STARTUP_BUFFER_TASKS.clear()
    legacy_proxy._PREFETCH_SEMAPHORE = asyncio.Semaphore(PREFETCH_CONCURRENCY)
    try:
        scrapers.PLAYWRIGHT_CLIENT = await async_playwright().start()
        scrapers.SHARED_BROWSER = await scrapers.PLAYWRIGHT_CLIENT.chromium.launch(headless=True)
    except Exception as exc:
        # HTTP scraping remains available when Chromium is not installed or cannot start.
        _log_failure("start Playwright; using HTTP extraction only", exc)
        if scrapers.PLAYWRIGHT_CLIENT:
            await scrapers.PLAYWRIGHT_CLIENT.stop()
        scrapers.PLAYWRIGHT_CLIENT = None
        scrapers.SHARED_BROWSER = None
    
    state.SHARED_HTTP_CLIENT = httpx.AsyncClient(
        limits=httpx.Limits(max_keepalive_connections=50, max_connections=150),
        timeout=12.0,
        follow_redirects=True,
        http2=True
    )
    # Separate pool for playback (playlists + segments): scrape bursts used to
    # exhaust the shared pool and make segment fetches hit PoolTimeout. Long
    # keepalive because segment polls are 2-6s apart (the 5s default expired
    # between polls, paying a new TCP+TLS handshake on most requests). Redirects
    # are followed manually so every hop is SSRF-validated.
    state.MEDIA_HTTP_CLIENT = httpx.AsyncClient(
        limits=httpx.Limits(max_keepalive_connections=64, max_connections=200, keepalive_expiry=60.0),
        timeout=httpx.Timeout(connect=5.0, read=10.0, write=10.0, pool=3.0),
        follow_redirects=False,
        http2=True,
    )
    SESSIONS.sessions.clear()
    await _check_ffmpeg_available()
    for (
        team_id,
        name,
        query,
        logo_url,
        start_time,
        stop_time,
        category,
        source_id,
        content_type,
        encoded_search_terms,
        always_live,
        catalog_key,
        auto_disable_after,
    ) in load_teams():
        try:
            stored_search_terms = json.loads(encoded_search_terms or "[]")
        except (TypeError, ValueError, json.JSONDecodeError):
            stored_search_terms = []
        if not isinstance(stored_search_terms, list):
            stored_search_terms = []
        search_terms = [str(term).strip() for term in stored_search_terms if str(term).strip()]
        if not search_terms:
            search_terms = get_team_search_terms(name, query, team_id)
        display_name = _sport_labeled_name(name, category or "")
        special_channel = _special_channel_for({"catalog_key": catalog_key})
        stream_state[team_id] = {
            "name": display_name, "query": query, "candidates": [], "active_index": 0, "is_healthy": False,
            "logo_url": logo_url or resolve_espn_logo(name, category, source_id),
            "start_time": start_time or "",
            "stop_time": stop_time or "",
            "category": category or "custom",
            "source_id": source_id or "",
            "content_type": content_type or "team",
            "search_terms": search_terms,
            "always_live": bool(always_live) or bool(special_channel),
            "catalog_key": catalog_key or "",
            "tvg_id": special_channel.tvg_id if special_channel else "",
            "group_title": special_channel.group_title if special_channel else "",
            "auto_disable_after": auto_disable_after or "",
            **_scrape_lifecycle_defaults(),
        }
        _start_team_scrape_loop(team_id, initial_delay=random.uniform(0.0, STARTUP_SCRAPE_SPREAD_SECONDS))

    for (
        channel_id,
        mv_name,
        layout,
        encoded_member_ids,
        active_audio_team_id,
        tvg_id,
        group_title,
        logo_url,
    ) in load_multiview_channels():
        try:
            member_team_ids = json.loads(encoded_member_ids or "[]")
        except (TypeError, ValueError, json.JSONDecodeError):
            member_team_ids = []
        member_team_ids = [str(t) for t in member_team_ids if str(t).strip()]
        if len(member_team_ids) not in (2, 4):
            LOGGER.warning("Skipping multiview channel with invalid member count: %s", channel_id)
            continue
        missing = [t for t in member_team_ids if t not in stream_state]
        if missing:
            # Keep the Multi-View: removed members render as a "No Signal" pane.
            LOGGER.warning("Multi-View %s references removed channels %s; showing No Signal for them", channel_id, missing)
        stream_state[channel_id] = {
            "name": mv_name, "query": "", "type": "multiview",
            "candidates": [{"synthetic": True}],  # non-empty sentinel only to satisfy generic health-dot/404 checks; not a real stream candidate
            "active_index": 0, "is_healthy": False,
            "logo_url": logo_url, "start_time": "", "stop_time": "",
            "category": "multiview", "source_id": "", "content_type": "multiview",
            "search_terms": [], "always_live": True, "catalog_key": "",
            "tvg_id": tvg_id, "group_title": group_title or "Multi-View",
            "layout": layout, "member_team_ids": member_team_ids,
            "active_audio_team_id": active_audio_team_id if active_audio_team_id in member_team_ids else member_team_ids[0],
            **_scrape_lifecycle_defaults(),
        }

    monitor_task = asyncio.create_task(failover_monitor(), name="failover monitor")
    prune_task = asyncio.create_task(prune_database_logs(), name="database log pruning")
    schedule_task = asyncio.create_task(enforce_scheduled_disables(), name="scheduled disable enforcement")
    multiview_idle_task = asyncio.create_task(multiview_idle_monitor(), name="multiview idle monitor")
    multiview_watchdog_task = asyncio.create_task(multiview_watchdog(), name="multiview watchdog")
    # Tracked background task: _cancel_background_tasks() stops it at shutdown.
    _spawn_background_task(multiview_output_sweeper(), "multiview output sweep")
    yield

    monitor_task.cancel()
    prune_task.cancel()
    schedule_task.cancel()
    multiview_idle_task.cancel()
    multiview_watchdog_task.cancel()
    scrape_tasks = list(_TEAM_SCRAPE_TASKS.values())
    for t in scrape_tasks:
        t.cancel()
    # Wait for tasks to safely wind down before closing shared clients.
    await asyncio.gather(
        monitor_task, prune_task, schedule_task, multiview_idle_task, multiview_watchdog_task, *scrape_tasks,
        return_exceptions=True,
    )
    _TEAM_SCRAPE_TASKS.clear()
    _TEAM_SCRAPE_WAKE_EVENTS.clear()
    _TEAM_STATE_LOCKS.clear()
    await SESSIONS.close_all()
    for channel_id in list(_MULTIVIEW_PROCESSES):
        await _stop_multiview_process(channel_id)
    await _stop_placeholder_process()
    # Ensure tracked prefetchers and other background work are stopped too.
    await _cancel_background_tasks()
    await _METRIC_WRITER.stop()
    close_all_db_connections()
    _STARTUP_BUFFER_TASKS.clear()
    shutil.rmtree(MULTIVIEW_OUTPUT_ROOT, ignore_errors=True)
    shutil.rmtree(PLACEHOLDER_OUTPUT_DIR, ignore_errors=True)
        
    if scrapers.SHARED_BROWSER:
        try:
            await scrapers.SHARED_BROWSER.close()
        except Exception as exc:
            _log_failure("close Playwright browser", exc)
    if scrapers.PLAYWRIGHT_CLIENT:
        try:
            await scrapers.PLAYWRIGHT_CLIENT.stop()
        except Exception as exc:
            _log_failure("stop Playwright", exc)
    if state.SHARED_HTTP_CLIENT:
        try:
            await state.SHARED_HTTP_CLIENT.aclose()
        except Exception as exc:
            _log_failure("close shared HTTP client", exc)
        state.SHARED_HTTP_CLIENT = None
    if state.MEDIA_HTTP_CLIENT:
        try:
            await state.MEDIA_HTTP_CLIENT.aclose()
        except Exception as exc:
            _log_failure("close media HTTP client", exc)
        state.MEDIA_HTTP_CLIENT = None
    scrapers.SHARED_BROWSER = None
    scrapers.PLAYWRIGHT_CLIENT = None
    legacy_proxy._PREFETCH_SEMAPHORE = None
    stream_state.clear()

def _is_expected_slow_request(scope) -> bool:
    """A channel playlist's first request waits for the session to start (up to
    STREAM_STARTUP_TIMEOUT); that's normal, not worth a warning each time."""
    path = scope.get("path") or ""
    return path.endswith(".m3u8") and (path.startswith("/stream/") or path.startswith("/multiview/"))


class RequestDiagnosticsMiddleware:
    """Pure ASGI middleware: logs 5xx and slow responses, turns unhandled errors
    into a 500. Replaces @app.middleware("http") (BaseHTTPMiddleware), which
    pushed every streamed body chunk through an extra memory stream and task
    hop - measurable overhead on the segment relay path."""

    def __init__(self, asgi_app) -> None:
        self.app = asgi_app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        started = time.perf_counter()
        status_holder = {"status": None}

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                status_holder["status"] = message["status"]
                elapsed_ms = (time.perf_counter() - started) * 1000
                if message["status"] >= 500:
                    LOGGER.warning(
                        "HTTP failure method=%s path=%s status=%d duration_ms=%.0f",
                        scope.get("method"), scope.get("path"), message["status"], elapsed_ms,
                    )
                elif elapsed_ms >= 5000 and not _is_expected_slow_request(scope):
                    LOGGER.warning(
                        "Slow request method=%s path=%s status=%d duration_ms=%.0f",
                        scope.get("method"), scope.get("path"), message["status"], elapsed_ms,
                    )
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception as exc:
            if status_holder["status"] is not None:
                # Response already started (e.g. an upstream segment died
                # mid-body): re-raise so the server aborts the connection and
                # the client retries, instead of seeing a clean truncated body.
                raise
            elapsed_ms = (time.perf_counter() - started) * 1000
            _log_failure(f"HTTP {scope.get('method')} {scope.get('path')} ({elapsed_ms:.0f}ms)", exc, logging.ERROR)
            response = JSONResponse(status_code=500, content={"detail": "Internal server error"})
            await response(scope, receive, send)


app = FastAPI(title="Jellyfin Sports Proxy - Titan Engine", version=__version__, lifespan=lifespan)
app.add_middleware(CsrfOriginMiddleware)
app.add_middleware(RequestDiagnosticsMiddleware)

# Dashboard templates/static assets. Resolved with _resource_path so both the
# source tree and the PyInstaller bundle (BUNDLE_DIR/_MEIPASS) find them; see
# jellyball.spec, which adds both directories to `datas`.
TEMPLATES = Jinja2Templates(directory=str(_resource_path("templates")))
app.mount("/static", StaticFiles(directory=str(_resource_path("static"))), name="static")

# Route registration order: routes_stream registers /stream/{team_id}.m3u8 and
# /stream/{team_id}/seg/... before the catch-all /stream/{team_id}.
app.include_router(legacy_proxy.router)


@app.get("/healthz")
async def healthz():
    """Unauthenticated liveness probe (Docker healthcheck, installer, tray)."""
    return {"status": "ok", "app": "jellyball", "version": __version__}


# --- MULTI-VIEW (FFmpeg grid compositing) ---

def _default_ffmpeg_path() -> str:
    """Prefer a bundled ffmpeg binary in the packaged executable; otherwise
    fall back to a plain "ffmpeg" lookup on PATH (dev runs, Docker, or a
    build that didn't have one available to bundle)."""
    if getattr(sys, "frozen", False):
        bundled_name = "ffmpeg.exe" if sys.platform == "win32" else "ffmpeg"
        bundled_path = BUNDLE_DIR / "ffmpeg_bin" / bundled_name
        if bundled_path.is_file():
            return str(bundled_path)
    return "ffmpeg"


FFMPEG_PATH = os.getenv("FFMPEG_PATH") or _default_ffmpeg_path()


def _jellyfin_ffmpeg_path() -> Optional[str]:
    """Jellyfin's own ffmpeg build, when Jellyball runs on the Jellyfin server."""
    candidates: List[Path] = []
    if sys.platform == "win32":
        for root in (os.getenv("ProgramW6432"), os.getenv("ProgramFiles"), r"C:\Program Files"):
            if root:
                candidates.append(Path(root) / "Jellyfin" / "Server" / "ffmpeg.exe")
    else:
        candidates += [Path("/usr/lib/jellyfin-ffmpeg/ffmpeg"), Path("/usr/share/jellyfin-ffmpeg/ffmpeg")]
    for candidate in candidates:
        if candidate.is_file():
            return str(candidate)
    return None


# Multi-View's encoder must match the GPU driver. The bundled ffmpeg is a very
# recent build (its NVENC needs NVIDIA driver 610+); on a Jellyfin server the
# Jellyfin ffmpeg is built for whatever driver Jellyfin's own hardware
# transcoding already uses, so prefer it unless a path is configured.
MULTIVIEW_FFMPEG_PATH = (
    os.getenv("MULTIVIEW_FFMPEG_PATH")
    or (None if os.getenv("FFMPEG_PATH") else _jellyfin_ffmpeg_path())
    or FFMPEG_PATH
)
MULTIVIEW_FFMPEG_VERSION_INFO = ""
MULTIVIEW_HWACCEL = os.getenv("MULTIVIEW_HWACCEL", "nvenc").strip().lower()
MULTIVIEW_BITRATE = os.getenv("MULTIVIEW_BITRATE", "6M")
MULTIVIEW_SEGMENT_SECONDS = bounded_int(os.getenv("MULTIVIEW_SEGMENT_SECONDS", "4"), 4, 1, 15)
MULTIVIEW_IDLE_TIMEOUT_SECONDS = bounded_float(os.getenv("MULTIVIEW_IDLE_TIMEOUT_SECONDS", "180"), 180.0, 30.0, 3600.0)
MULTIVIEW_IDLE_CHECK_INTERVAL = bounded_float(os.getenv("MULTIVIEW_IDLE_CHECK_INTERVAL", "30"), 30.0, 5.0, 300.0)
MULTIVIEW_STARTUP_TIMEOUT_SECONDS = bounded_float(os.getenv("MULTIVIEW_STARTUP_TIMEOUT_SECONDS", "30"), 30.0, 5.0, 120.0)
# Each running Multi-View channel is its own ffmpeg transcode (GPU encoder session +
# CPU/network for every member stream it composites); an unbounded number of them can
# exhaust NVENC/QSV session limits or the host's CPU. Cap concurrent transcodes.
MAX_CONCURRENT_MULTIVIEW = bounded_int(os.getenv("MAX_CONCURRENT_MULTIVIEW", "3"), 3, 1, 32)
MULTIVIEW_OUTPUT_ROOT = DATA_DIR / "multiview"


def _env_choice(name: str, default: str, allowed) -> str:
    """A string setting limited to known values: an unknown ffmpeg preset or
    tune would make every single Multi-View spawn fail."""
    value = (os.getenv(name) or "").strip().lower()
    if not value:
        return default
    if value not in allowed:
        LOGGER.warning("Ignoring %s=%r (expected one of %s); using %r", name, value, ", ".join(sorted(allowed)), default)
        return default
    return value


# Encoder tuning. The GOP is FPS x segment length and keyframes are forced on
# the segment grid (see _multiview_video_encoder_args), so any combination
# keeps every per-audio output cutting at the same instants.
MULTIVIEW_FPS = bounded_int(os.getenv("MULTIVIEW_FPS", "30"), 30, 10, 60)
MULTIVIEW_HLS_LIST_SIZE = bounded_int(os.getenv("MULTIVIEW_HLS_LIST_SIZE", "8"), 8, 3, 30)
MULTIVIEW_NVENC_PRESET = _env_choice(
    "MULTIVIEW_NVENC_PRESET", "p4",
    {"p1", "p2", "p3", "p4", "p5", "p6", "p7", "default", "slow", "medium", "fast", "hp", "hq", "bd", "ll", "llhq", "llhp"},
)
MULTIVIEW_NVENC_TUNE = _env_choice("MULTIVIEW_NVENC_TUNE", "ll", {"hq", "ll", "ull", "lossless"})
# After a GPU encoder failure new runs are software-encoded for this long,
# then the GPU is tried again (the failure is often transient: every NVENC
# session taken by Jellyfin's own transcodes, a driver update, ...).
NVENC_FALLBACK_SECONDS = bounded_float(os.getenv("NVENC_FALLBACK_SECONDS", "600"), 600.0, 30.0, 86400.0)
# Spawn-failure backoff (and the step/cap of the restart backoff below).
MULTIVIEW_BACKOFF_BASE_SECONDS = bounded_float(os.getenv("MULTIVIEW_BACKOFF_BASE_SECONDS", "15"), 15.0, 1.0, 600.0)
MULTIVIEW_BACKOFF_MAX_SECONDS = max(
    MULTIVIEW_BACKOFF_BASE_SECONDS,
    bounded_float(os.getenv("MULTIVIEW_BACKOFF_MAX_SECONDS", "300"), 300.0, 1.0, 3600.0),
)
# Watchdog/view-failure restarts: the first MULTIVIEW_RESTART_BURST inside a
# rolling window are immediate; each further one waits exponentially longer.
MULTIVIEW_RESTART_WINDOW_SECONDS = bounded_float(os.getenv("MULTIVIEW_RESTART_WINDOW_SECONDS", "600"), 600.0, 60.0, 86400.0)
MULTIVIEW_RESTART_BURST = bounded_int(os.getenv("MULTIVIEW_RESTART_BURST", "2"), 2, 0, 20)
# A refused start (concurrency cap reached / ffmpeg unavailable) is retried
# after a short delay that doubles per consecutive refusal, up to a minute.
MULTIVIEW_REFUSAL_BACKOFF_SECONDS = bounded_float(os.getenv("MULTIVIEW_REFUSAL_BACKOFF_SECONDS", "5"), 5.0, 1.0, 300.0)
MULTIVIEW_REFUSAL_BACKOFF_MAX_SECONDS = max(60.0, MULTIVIEW_REFUSAL_BACKOFF_SECONDS)
# How often leftover run directories (~100MB of segments each) are swept.
MULTIVIEW_SWEEP_INTERVAL = bounded_float(os.getenv("MULTIVIEW_SWEEP_INTERVAL", "600"), 600.0, 60.0, 86400.0)

MULTIVIEW_LAYOUTS = {
    "side_by_side_2": {"count": 2, "pane_w": 960, "pane_h": 1080, "xstack": "0_0|w0_0"},
    "grid_2x2": {"count": 4, "pane_w": 960, "pane_h": 540, "xstack": "0_0|w0_0|0_h0|w0_h0"},
}

FFMPEG_AVAILABLE = False
FFMPEG_VERSION_INFO = ""

_MULTIVIEW_PROCESSES: Dict[str, dict] = {}
# Tracks recent spawn failures per channel so a client that keeps retrying a
# permanently-broken Multi-View (e.g. one member currently has no live
# candidates, so ffmpeg's -i for it 404s and the whole process aborts) can't
# force a new ffmpeg spawn attempt on every single request. Real players
# (Jellyfin's tuner among them) retry a failed live-TV stream on their own
# schedule regardless of what we return, so this backoff is the only thing
# standing between "one dead input" and continuous CPU/GPU churn.
_MULTIVIEW_FAILURES: Dict[str, dict] = {}
# Short "don't start before" holds on top of the failure backoff, for refused
# starts and backed-off restarts: {"until": monotonic, "kind": str, "reason": str}.
_MULTIVIEW_HOLDS: Dict[str, dict] = {}
# Consecutive refused starts per channel (sizes the refusal hold).
_MULTIVIEW_REFUSALS: Dict[str, int] = {}
# Rolling restart history: {"times": deque of monotonic, "last_alert": Optional[float]}.
# Unlike _MULTIVIEW_FAILURES it survives successful spawns, so a run that
# starts fine and then stalls every minute still backs off.
_MULTIVIEW_RESTARTS: Dict[str, dict] = {}
# Multi-Views an admin stopped (monotonic time of the Stop). While set, nothing
# auto-starts the grid and its viewers get the "No Signal" placeholder.
# Cleared by an explicit play: the next NEW viewer session (a playlist or
# segment request for this Multi-View while none of its view sessions is
# running, i.e. a fresh tune) or POST /multiview/{id}/start. A viewer who was
# already watching when Stop was pressed keeps getting No Signal until their
# session idles out (SESSION_IDLE_SECONDS after they stop pulling), so the
# player that happened to be open can't silently undo the Stop.
_MULTIVIEW_MANUAL_STOPS: Dict[str, float] = {}
# Channels holding a concurrency slot while their spawn warms members and waits
# for first segments. Reserved before the (up to 20s) warm-up, so simultaneous
# starts can't all pass MAX_CONCURRENT_MULTIVIEW; released when the spawn ends
# (a successful run keeps the slot through its _MULTIVIEW_PROCESSES entry).
_MULTIVIEW_SLOT_RESERVATIONS: Set[str] = set()
# Run directories created by a spawn but not registered yet (the sweep skips them).
_MULTIVIEW_PENDING_RUN_DIRS: Set[Path] = set()
_MULTIVIEW_RUN_COUNTER = 0
# At most one start task per channel; this (not a lock) serializes spawns.
_MULTIVIEW_START_TASKS: Dict[str, asyncio.Task] = {}
_MULTIVIEW_LAST_VIEWER: Dict[str, float] = {}


def _multiview_backoff_seconds(failure_count: int) -> float:
    return min(MULTIVIEW_BACKOFF_MAX_SECONDS, MULTIVIEW_BACKOFF_BASE_SECONDS * (2 ** max(0, failure_count - 1)))


def _multiview_hold_remaining(channel_id: str) -> float:
    hold = _MULTIVIEW_HOLDS.get(channel_id)
    if not hold:
        return 0.0
    remaining = hold["until"] - time.monotonic()
    if remaining <= 0:
        _MULTIVIEW_HOLDS.pop(channel_id, None)
        return 0.0
    return remaining


def _multiview_cooldown_remaining(channel_id: str) -> float:
    """Seconds before this Multi-View may be started again (failure backoff,
    refusal hold or restart backoff, whichever ends last)."""
    record = _MULTIVIEW_FAILURES.get(channel_id)
    failure_remaining = 0.0
    if record:
        elapsed = time.monotonic() - record["last_failure"]
        failure_remaining = max(0.0, _multiview_backoff_seconds(record["count"]) - elapsed)
    return max(failure_remaining, _multiview_hold_remaining(channel_id))


def _set_multiview_hold(channel_id: str, seconds: float, kind: str, reason: str) -> None:
    until = time.monotonic() + seconds
    current = _MULTIVIEW_HOLDS.get(channel_id)
    if current is None or current["until"] < until:
        _MULTIVIEW_HOLDS[channel_id] = {"until": until, "kind": kind, "reason": reason}


def _record_multiview_failure(channel_id: str, error: str) -> None:
    record = _MULTIVIEW_FAILURES.setdefault(channel_id, {"count": 0, "last_failure": 0.0, "last_error": ""})
    record["count"] += 1
    record["last_failure"] = time.monotonic()
    record["last_error"] = error


def _clear_multiview_failure(channel_id: str) -> None:
    _MULTIVIEW_FAILURES.pop(channel_id, None)


def _record_multiview_refusal(channel_id: str, reason: str) -> float:
    """A start refused before anything was launched. Viewers get the
    placeholder meanwhile; nothing retries the spawn until the hold expires,
    so this logs once per hold instead of on every 0.5s session poll."""
    count = _MULTIVIEW_REFUSALS.get(channel_id, 0) + 1
    _MULTIVIEW_REFUSALS[channel_id] = count
    delay = min(MULTIVIEW_REFUSAL_BACKOFF_MAX_SECONDS, MULTIVIEW_REFUSAL_BACKOFF_SECONDS * (2 ** (count - 1)))
    _set_multiview_hold(channel_id, delay, "refused", reason)
    LOGGER.warning(
        "Refusing to start multiview channel=%s: %s; next attempt in %.0fs (viewers see No Signal)",
        channel_id, reason, delay,
    )
    return delay


def _multiview_restart_delay(restart_count: int) -> float:
    """Hold before the Nth restart inside the rolling window may start."""
    excess = restart_count - MULTIVIEW_RESTART_BURST
    if excess <= 0:
        return 0.0
    return min(MULTIVIEW_BACKOFF_MAX_SECONDS, MULTIVIEW_BACKOFF_BASE_SECONDS * (2 ** (excess - 1)))


def _recent_multiview_restarts(channel_id: str, now: Optional[float] = None) -> int:
    record = _MULTIVIEW_RESTARTS.get(channel_id)
    if not record:
        return 0
    now = time.monotonic() if now is None else now
    times = record["times"]
    while times and now - times[0] > MULTIVIEW_RESTART_WINDOW_SECONDS:
        times.popleft()
    return len(times)


def _note_multiview_restart(channel_id: str, reason: str) -> Tuple[int, float]:
    """Count a restart in the rolling window; returns (restarts in window, hold
    before the next start). Sends one alert when the backoff kicks in, then at
    most one per window while it stays engaged."""
    now = time.monotonic()
    record = _MULTIVIEW_RESTARTS.setdefault(channel_id, {"times": deque(), "last_alert": None})
    _recent_multiview_restarts(channel_id, now)
    record["times"].append(now)
    count = len(record["times"])
    delay = _multiview_restart_delay(count)
    if delay > 0:
        _set_multiview_hold(channel_id, delay, "restart", reason)
        last_alert = record["last_alert"]
        if last_alert is None or now - last_alert >= MULTIVIEW_RESTART_WINDOW_SECONDS:
            record["last_alert"] = now
            name = (stream_state.get(channel_id) or {}).get("name", channel_id)
            _spawn_background_task(
                send_alert(
                    "⚠️ Multi-View Unstable",
                    f"**{name}** restarted {count} times in {MULTIVIEW_RESTART_WINDOW_SECONDS / 60:.0f} min "
                    f"(last: {reason}). Restarts now back off; next start in {delay:.0f}s.",
                    "warning",
                ),
                f"send multiview restart alert channel={channel_id}",
            )
    return count, delay


def _forget_multiview_channel(channel_id: str) -> None:
    """Drop every per-channel Multi-View bookkeeping entry (channel removed)."""
    for mapping in (
        _MULTIVIEW_FAILURES, _MULTIVIEW_HOLDS, _MULTIVIEW_REFUSALS, _MULTIVIEW_RESTARTS,
        _MULTIVIEW_MANUAL_STOPS, _MULTIVIEW_LAST_VIEWER, _MULTIVIEW_START_TASKS,
    ):
        mapping.pop(channel_id, None)
    _MULTIVIEW_SLOT_RESERVATIONS.discard(channel_id)


def _multiview_error_from_log(log_lines) -> str:
    """Pull the most useful line out of ffmpeg's log for a human-readable failure reason."""
    lines = list(log_lines)
    for line in reversed(lines):
        if "Error opening input" in line or "error while opening" in line.lower():
            return line.strip()
    for line in reversed(lines):
        if line.strip():
            return line.strip()
    return "ffmpeg exited unexpectedly"


async def _probe_ffmpeg_version(path: str, timeout: float = 5.0) -> Tuple[Optional[int], str]:
    """Run `<path> -version`; returns (exit code, first output line). A binary
    that hangs (e.g. on an unreachable network path) is killed on timeout
    instead of being left running for the life of the service."""
    process = await asyncio.create_subprocess_exec(
        path, "-version",
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        creationflags=_child_process_creationflags(),
    )
    try:
        stdout, _ = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        if process.returncode is None:
            try:
                process.kill()
            except (ProcessLookupError, OSError):
                pass
            try:
                await asyncio.wait_for(process.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
        raise
    first_line = stdout.decode(errors="replace").splitlines()[0] if stdout else ""
    return process.returncode, first_line


async def _check_ffmpeg_available() -> None:
    """Probe FFMPEG_PATH once at startup so failures surface as a clear log/dashboard warning."""
    global FFMPEG_AVAILABLE, FFMPEG_VERSION_INFO
    try:
        returncode, FFMPEG_VERSION_INFO = await _probe_ffmpeg_version(FFMPEG_PATH)
        FFMPEG_AVAILABLE = returncode == 0
    except (FileNotFoundError, OSError, asyncio.TimeoutError) as exc:
        FFMPEG_AVAILABLE = False
        FFMPEG_VERSION_INFO = ""
        _log_failure("locate ffmpeg for multiview", exc, logging.WARNING)
    if not FFMPEG_AVAILABLE:
        LOGGER.warning("ffmpeg not found at FFMPEG_PATH=%r; Multi-View channels unavailable", FFMPEG_PATH)
    await _check_multiview_ffmpeg()


async def _check_multiview_ffmpeg() -> None:
    global MULTIVIEW_FFMPEG_PATH, MULTIVIEW_FFMPEG_VERSION_INFO
    if MULTIVIEW_FFMPEG_PATH != FFMPEG_PATH:
        try:
            returncode, first_line = await _probe_ffmpeg_version(MULTIVIEW_FFMPEG_PATH)
            if returncode == 0 and first_line:
                MULTIVIEW_FFMPEG_VERSION_INFO = first_line
            else:
                raise OSError(f"exit code {returncode}")
        except (FileNotFoundError, OSError, asyncio.TimeoutError) as exc:
            _log_failure(f"probe Multi-View ffmpeg {MULTIVIEW_FFMPEG_PATH!r}; using {FFMPEG_PATH!r}", exc)
            MULTIVIEW_FFMPEG_PATH = FFMPEG_PATH
    if MULTIVIEW_FFMPEG_PATH == FFMPEG_PATH:
        MULTIVIEW_FFMPEG_VERSION_INFO = FFMPEG_VERSION_INFO
    if MULTIVIEW_FFMPEG_VERSION_INFO:
        LOGGER.info("Multi-View ffmpeg: %s (%s)", MULTIVIEW_FFMPEG_VERSION_INFO, MULTIVIEW_FFMPEG_PATH)


def _multiview_bufsize(bitrate: str) -> str:
    """Double a ffmpeg bitrate string (e.g. "6M" -> "12M", "6000k" -> "12000k") for -bufsize."""
    match = re.match(r"^(\d+(?:\.\d+)?)([A-Za-z]*)$", bitrate.strip())
    if not match:
        return bitrate
    value, unit = match.groups()
    doubled = float(value) * 2
    doubled_str = str(int(doubled)) if doubled == int(doubled) else str(doubled)
    return f"{doubled_str}{unit}"


def _multiview_video_encoder_args(encoder: Optional[str] = None) -> List[str]:
    """Encoder + rate-control flags. Kept per-backend because several of these
    (e.g. -forced-idr, -sc_threshold) are private AVOptions that only exist on
    some encoders and make ffmpeg fail outright with "Unrecognized option" on
    others (notably h264_qsv), so they must not be shared unconditionally.

    Keyframes are forced on the segment grid (fps is pinned to MULTIVIEW_FPS in
    the filter graph), so every HLS output of the tee cuts at the same instants
    and segments line up across the per-audio outputs."""
    encoder = (encoder or MULTIVIEW_HWACCEL).lower()
    gop = MULTIVIEW_FPS * MULTIVIEW_SEGMENT_SECONDS
    common_rate_args = [
        "-b:v", MULTIVIEW_BITRATE,
        "-maxrate", MULTIVIEW_BITRATE,
        "-bufsize", _multiview_bufsize(MULTIVIEW_BITRATE),
        "-g", str(gop),
        "-keyint_min", str(gop),
        "-bf", "0",
        "-pix_fmt", "yuv420p",
        "-force_key_frames", f"expr:gte(t,n_forced*{MULTIVIEW_SEGMENT_SECONDS})",
    ]
    if encoder == "nvenc":
        return [
            "-c:v", "h264_nvenc", "-preset", MULTIVIEW_NVENC_PRESET, "-tune", MULTIVIEW_NVENC_TUNE, "-rc", "cbr",
            *common_rate_args,
            "-forced-idr", "1",
        ]
    if encoder == "qsv":
        return [
            "-c:v", "h264_qsv", "-preset", "veryfast", "-look_ahead", "0",
            *common_rate_args,
        ]
    return [
        "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
        *common_rate_args,
        "-sc_threshold", "0",
    ]


def _build_xstack_filter(layout: str, member_count: int) -> str:
    """Return the video part of -filter_complex: scale/pad each input and stack
    them into one grid, pinned to MULTIVIEW_FPS (xstack otherwise emits a frame
    whenever *any* input has one, so mixed 30/60fps members made the output rate
    irregular and broke the GOP-to-segment alignment)."""
    spec = MULTIVIEW_LAYOUTS[layout]
    pane_w, pane_h = spec["pane_w"], spec["pane_h"]
    parts = []
    labels = []
    for idx in range(member_count):
        label = f"v{idx}"
        labels.append(label)
        parts.append(
            f"[{idx}:v]scale={pane_w}:{pane_h}:force_original_aspect_ratio=decrease,"
            f"pad={pane_w}:{pane_h}:(ow-iw)/2:(oh-ih)/2,setsar=1[{label}]"
        )
    stack_inputs = "".join(f"[{label}]" for label in labels)
    parts.append(f"{stack_inputs}xstack=inputs={member_count}:layout={spec['xstack']},fps={MULTIVIEW_FPS}[vout]")
    return ";".join(parts)


def _build_multiview_audio_filter(audio_presence: List[bool]) -> str:
    """Silent stand-in tracks [aN] for members without audio (so every
    per-audio output always has a track). Members that do have audio are
    stream-copied, not filtered: decoding + re-encoding the members' live audio
    made ffmpeg's scheduler throttle every input (measured ~0.27x real time
    with two live inputs, vs 1.2x when the audio is copied)."""
    return ";".join(
        f"anullsrc=r=48000:cl=stereo[a{idx}]"
        for idx, has_audio in enumerate(audio_presence)
        if not has_audio
    )


def _multiview_tee_outputs(member_count: int) -> str:
    """One muxed HLS output per member audio, all fed by the same encoded video.

    Escaping is exact on purpose: inside the tee spec, the select value must be
    written as select=\\'v:0,a:N\\' (a literal backslash before each quote in the
    argv element) or ffmpeg rejects it. onfail=ignore keeps one broken output
    from killing the others (the watchdog restarts the run if one stalls). No
    temp_file flag: on Windows its rename-over fails whenever we have the
    playlist open for reading, which would drop that output."""
    slaves = []
    for idx in range(member_count):
        options = ":".join([
            "f=hls",
            f"select=\\'v:0,a:{idx}\\'",
            "onfail=ignore",
            f"hls_time={MULTIVIEW_SEGMENT_SECONDS}",
            f"hls_list_size={MULTIVIEW_HLS_LIST_SIZE}",
            "hls_flags=delete_segments+independent_segments",
            "hls_segment_type=mpegts",
            f"hls_segment_filename=a{idx}/seg_%06d.ts",
        ])
        slaves.append(f"[{options}]a{idx}/index.m3u8")
    return "|".join(slaves)


def _internal_base_url() -> str:
    """Base URL ffmpeg uses to read our own member sessions."""
    host = (os.getenv("JELLYBALL_HOST") or "127.0.0.1").strip()
    if host in ("", "0.0.0.0", "::", "[::]", "localhost"):
        host = "127.0.0.1"
    return f"http://{host}:{config.PORT}"


def _multiview_input_url(team_id: str) -> str:
    if team_id not in stream_state:
        # A member that was deleted shows the "No Signal" pane instead of
        # breaking the whole Multi-View.
        team_id = PLACEHOLDER_SESSION_ID
    return f"{_internal_base_url()}/stream/{team_id}.m3u8"


def _build_multiview_ffmpeg_args(
    channel_id: str,
    data: dict,
    out_dir: Path,
    audio_presence: Optional[List[Optional[bool]]] = None,
    encoder: Optional[str] = None,
    *,
    hw_decode: Optional[bool] = None,
    input_ids: Optional[List[str]] = None,
) -> List[str]:
    """Pure command-builder (no I/O) so it can be unit tested without spawning ffmpeg.

    Inputs are the members' channel sessions, which never 502, follow failover,
    and are normalized (fixed PIDs, continuous timestamps), so one member's
    provider switching no longer freezes its pane or the whole grid. The run
    writes one HLS output per member audio under a{N}/ (relative to cwd=out_dir).

    `audio_presence[i]` must be True for member i's audio to be mapped: None
    (unknown) gets the silent track like False, because mapping a missing
    N:a:0 makes ffmpeg fail the whole run. `input_ids[i]` is the channel
    session that feeds pane i (default: the member itself; the placeholder for
    a member that wasn't ready in time). `hw_decode` (default: encoder is
    nvenc) adds -hwaccel cuda per input."""
    member_team_ids: List[str] = data["member_team_ids"]
    layout = data.get("layout", "grid_2x2")
    encoder = (encoder or MULTIVIEW_HWACCEL).lower()
    if hw_decode is None:
        hw_decode = encoder == "nvenc"
    audio_presence = (
        [has_audio is True for has_audio in audio_presence] if audio_presence else [True] * len(member_team_ids)
    )
    sources = list(input_ids) if input_ids else list(member_team_ids)

    loglevel = os.getenv("MULTIVIEW_FFMPEG_LOGLEVEL", "warning").strip() or "warning"
    args: List[str] = ["-y", "-hide_banner", "-loglevel", loglevel]
    # Progress lines only when debugging with a verbose level.
    args += ["-stats", "-stats_period", "5"] if loglevel in ("info", "verbose", "debug") else ["-nostats"]
    for source_id in sources:
        if hw_decode:
            # Decode on the GPU as well (frames are downloaded for the CPU
            # scale/pad/xstack filters). ffmpeg falls back to software per
            # stream when NVDEC can't handle a codec/profile, but a CUDA
            # *device* failure (no/broken driver) is fatal for the whole run;
            # _start_multiview_run then retries without -hwaccel.
            args += ["-hwaccel", "cuda"]
        args += [
            # Our own normalized sessions (one program, fixed PIDs): a short
            # probe is plenty, and it opens each input much faster than the
            # 5MB/5s default when four members open one after another.
            "-probesize", "1000000",
            "-analyzeduration", "1000000",
            "-reconnect", "1",
            "-reconnect_streamed", "1",
            "-reconnect_on_network_error", "1",
            "-reconnect_delay_max", "5",
            "-rw_timeout", "15000000",
            "-thread_queue_size", "1024",
            "-i", _multiview_input_url(source_id),
        ]

    filter_graph = ";".join(part for part in (
        _build_xstack_filter(layout, len(member_team_ids)),
        _build_multiview_audio_filter(audio_presence),
    ) if part)
    args += ["-filter_complex", filter_graph]
    args += ["-map", "[vout]"]
    for idx, has_audio in enumerate(audio_presence):
        args += ["-map", f"{idx}:a:0" if has_audio else f"[a{idx}]"]
    args += _multiview_video_encoder_args(encoder)
    # Copy member audio as-is; only the generated silent tracks are encoded.
    args += ["-c:a", "copy"]
    for idx, has_audio in enumerate(audio_presence):
        if not has_audio:
            args += [f"-c:a:{idx}", "aac", f"-b:a:{idx}", "96k"]
    # Don't let one briefly starved audio member hold every output for the
    # default 10s interleave window.
    args += ["-max_interleave_delta", "2000000"]
    args += ["-f", "tee", _multiview_tee_outputs(len(member_team_ids))]
    return args


def _child_process_creationflags() -> int:
    """No console window per ffmpeg/taskkill child in the windowed tray exe."""
    if sys.platform == "win32":
        return getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)
    return 0


def _ffmpeg_creationflags() -> int:
    """Multi-View / placeholder ffmpeg: no console window, and below-normal
    priority so a CPU (libx264) fallback encode can't starve Jellyfin's own
    transcodes or this server's request handling."""
    flags = _child_process_creationflags()
    if sys.platform == "win32":
        flags |= getattr(subprocess, "BELOW_NORMAL_PRIORITY_CLASS", 0x00004000)
    return flags


def _multiview_popen_kwargs(out_dir: Path) -> dict:
    kwargs = {
        "cwd": str(out_dir),
        "stdin": asyncio.subprocess.DEVNULL,
        "stdout": asyncio.subprocess.PIPE,
        "stderr": asyncio.subprocess.STDOUT,
    }
    if sys.platform == "win32":
        kwargs["creationflags"] = _ffmpeg_creationflags()
    return kwargs


async def _drain_multiview_log(channel_id: str, process: "asyncio.subprocess.Process", log_lines: "deque[str]") -> None:
    """Continuously read ffmpeg's combined stdout/stderr pipe into a rolling
    buffer. This is not optional: an unread PIPE fills its OS buffer (~64KB) and
    blocks ffmpeg forever. Read in chunks rather than readline(): ffmpeg's
    progress output ends in '\\r', and readline() raises once 64KB arrive with no
    '\\n', which used to kill this task and then hang ffmpeg."""
    pending = b""
    try:
        while True:
            chunk = await process.stdout.read(8192)
            if not chunk:
                break
            pending += chunk
            *lines, pending = re.split(rb"[\r\n]+", pending)
            for line in lines:
                if line.strip():
                    log_lines.append(line.decode(errors="replace").rstrip())
            if len(pending) > 16384:
                log_lines.append(pending[-1024:].decode(errors="replace"))
                pending = b""
        if pending.strip():
            log_lines.append(pending.decode(errors="replace").rstrip())
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _log_failure(f"drain ffmpeg log multiview={channel_id}", exc)


async def _watch_multiview_process(channel_id: str, process: "asyncio.subprocess.Process") -> None:
    returncode = await process.wait()
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if entry and entry["process"] is process:
        entry["exited"] = True
        entry["exit_code"] = returncode
        if returncode != 0:
            log_tail = "\n".join(list(entry.get("log_lines", []))[-25:])
            LOGGER.warning("multiview ffmpeg exited unexpectedly channel=%s code=%s\n%s", channel_id, returncode, log_tail)


class _Win32ProcessApi:
    """The kernel32 calls used for ffmpeg process control, with explicit
    prototypes. Without restype=HANDLE ctypes returns a C int, which truncates
    64-bit handles; a private WinDLL instance keeps these prototypes from
    clashing with any other ctypes user of kernel32."""

    PROCESS_TERMINATE = 0x0001
    PROCESS_SET_QUOTA = 0x0100
    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    SYNCHRONIZE = 0x00100000
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000
    JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS = 9

    def __init__(self) -> None:
        import ctypes
        from ctypes import wintypes

        class _IO_COUNTERS(ctypes.Structure):
            _fields_ = [(name, ctypes.c_ulonglong) for name in (
                "ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
                "ReadTransferCount", "WriteTransferCount", "OtherTransferCount",
            )]

        class _JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_int64),
                ("PerJobUserTimeLimit", ctypes.c_int64),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class _JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", _JOBOBJECT_BASIC_LIMIT_INFORMATION),
                ("IoInfo", _IO_COUNTERS),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        self.ctypes = ctypes
        self.wintypes = wintypes
        self.extended_limit_info = _JOBOBJECT_EXTENDED_LIMIT_INFORMATION
        k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        HANDLE, BOOL, DWORD, UINT = wintypes.HANDLE, wintypes.BOOL, wintypes.DWORD, wintypes.UINT
        PFILETIME = ctypes.POINTER(wintypes.FILETIME)
        prototypes = {
            "CreateJobObjectW": (HANDLE, [wintypes.LPVOID, wintypes.LPCWSTR]),
            "SetInformationJobObject": (BOOL, [HANDLE, ctypes.c_int, wintypes.LPVOID, DWORD]),
            "AssignProcessToJobObject": (BOOL, [HANDLE, HANDLE]),
            "TerminateJobObject": (BOOL, [HANDLE, UINT]),
            "OpenProcess": (HANDLE, [DWORD, BOOL, DWORD]),
            "TerminateProcess": (BOOL, [HANDLE, UINT]),
            "GetProcessTimes": (BOOL, [HANDLE, PFILETIME, PFILETIME, PFILETIME, PFILETIME]),
            "WaitForSingleObject": (DWORD, [HANDLE, DWORD]),
            "CloseHandle": (BOOL, [HANDLE]),
        }
        for name, (restype, argtypes) in prototypes.items():
            function = getattr(k32, name)
            function.restype = restype
            function.argtypes = argtypes
        self.k32 = k32

    def close(self, handle) -> None:
        if handle:
            self.k32.CloseHandle(handle)

    def create_kill_on_close_job(self):
        h_job = self.k32.CreateJobObjectW(None, None)
        if not h_job:
            return None
        info = self.extended_limit_info()
        info.BasicLimitInformation.LimitFlags = self.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if not self.k32.SetInformationJobObject(
            h_job, self.JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS,
            self.ctypes.byref(info), self.ctypes.sizeof(info),
        ):
            self.close(h_job)
            return None
        return h_job

    def assign_pid(self, h_job, pid: int) -> bool:
        h_process = self.k32.OpenProcess(self.PROCESS_TERMINATE | self.PROCESS_SET_QUOTA, False, pid)
        if not h_process:
            return False
        try:
            return bool(self.k32.AssignProcessToJobObject(h_job, h_process))
        finally:
            self.close(h_process)

    def terminate_job(self, h_job) -> bool:
        return bool(self.k32.TerminateJobObject(h_job, 1))

    def _creation_time(self, h_process) -> Optional[int]:
        times = [self.wintypes.FILETIME() for _ in range(4)]
        if not self.k32.GetProcessTimes(h_process, *(self.ctypes.byref(t) for t in times)):
            return None
        return (times[0].dwHighDateTime << 32) | times[0].dwLowDateTime

    def process_creation_time(self, pid: int) -> Optional[int]:
        h_process = self.k32.OpenProcess(self.PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not h_process:
            return None
        try:
            return self._creation_time(h_process)
        finally:
            self.close(h_process)

    def terminate_pid_if_created_at(self, pid: int, created: int, wait_ms: int = 3000) -> bool:
        """Kill `pid` only if it is still the process created at `created`
        (a FILETIME): a bare pid may have been reused by anything since."""
        access = self.PROCESS_TERMINATE | self.PROCESS_QUERY_LIMITED_INFORMATION | self.SYNCHRONIZE
        h_process = self.k32.OpenProcess(access, False, pid)
        if not h_process:
            return False
        try:
            if self._creation_time(h_process) != created:
                return False
            if not self.k32.TerminateProcess(h_process, 1):
                return False
            self.k32.WaitForSingleObject(h_process, wait_ms)
            return True
        finally:
            self.close(h_process)


_WIN32_PROCESS_API = None  # None = not loaded yet, False = unavailable


def _win32_process_api() -> Optional[_Win32ProcessApi]:
    global _WIN32_PROCESS_API
    if sys.platform != "win32":
        return None
    if _WIN32_PROCESS_API is None:
        try:
            _WIN32_PROCESS_API = _Win32ProcessApi()
        except Exception as exc:
            _log_failure("load kernel32 process API", exc)
            _WIN32_PROCESS_API = False
    return _WIN32_PROCESS_API or None


_WINDOWS_CLEANUP_JOB_HANDLE = None


def _get_windows_cleanup_job():
    """Lazily create one Windows Job Object with KILL_ON_JOB_CLOSE for the lifetime
    of this process. Any ffmpeg child assigned to it is terminated by Windows itself
    if this process dies — including an ungraceful crash or Task Manager 'End Task'
    where our own lifespan shutdown / _stop_*_process cleanup never gets to run.
    Only a fallback now: each run normally gets its own job (_create_run_job)."""
    global _WINDOWS_CLEANUP_JOB_HANDLE
    if _WINDOWS_CLEANUP_JOB_HANDLE is not None:
        return _WINDOWS_CLEANUP_JOB_HANDLE
    api = _win32_process_api()
    if api is None:
        return None
    try:
        _WINDOWS_CLEANUP_JOB_HANDLE = api.create_kill_on_close_job()
    except Exception as exc:
        _log_failure("create Windows job object for child-process cleanup", exc)
        return None
    return _WINDOWS_CLEANUP_JOB_HANDLE


def _assign_child_to_cleanup_job(pid: int) -> None:
    """Best-effort; a failure here just means we fall back to explicit stop-on-shutdown
    cleanup (already in place) instead of Windows guaranteeing it on an ungraceful exit."""
    api = _win32_process_api()
    h_job = _get_windows_cleanup_job()
    if api is None or not h_job:
        return
    try:
        api.assign_pid(h_job, pid)
    except Exception as exc:
        _log_failure(f"assign pid={pid} to cleanup job object", exc)


def _create_run_job(pid: int):
    """Put one ffmpeg run in its own Job Object (KILL_ON_JOB_CLOSE). Stopping
    the run is then TerminateJobObject, which acts on the process objects in
    the job and can never hit an unrelated process that reused the pid; and
    Windows still kills the run if Jellyball dies without cleaning up (the job
    handle closes with us). Returns the job handle, or None off Windows or if
    no job could be made (then the shared cleanup job gives crash safety)."""
    api = _win32_process_api()
    if api is None:
        return None
    h_job = None
    try:
        h_job = api.create_kill_on_close_job()
        if h_job and api.assign_pid(h_job, pid):
            return h_job
    except Exception as exc:
        _log_failure(f"create job object for ffmpeg pid={pid}", exc)
    if h_job:
        api.close(h_job)
    _assign_child_to_cleanup_job(pid)
    return None


RUN_PID_FILE = "ffmpeg.pid"


def _write_run_pid_file(run_dir: Path, pid: int) -> None:
    """Record pid + creation time in the run dir so the next startup can kill
    this ffmpeg if Jellyball died without stopping it (and its job didn't take
    it down). Windows only: the creation time is what proves a live pid is
    still our ffmpeg rather than a reused pid."""
    api = _win32_process_api()
    if api is None:
        return
    try:
        created = api.process_creation_time(pid)
        if created is not None:
            (run_dir / RUN_PID_FILE).write_text(json.dumps({"pid": pid, "created": created}), encoding="utf-8")
    except Exception as exc:
        _log_failure(f"write ffmpeg pid file pid={pid}", exc)


def _kill_orphaned_ffmpeg() -> int:
    """Startup: kill ffmpeg runs a crashed previous instance left behind (pid
    and creation time must both match its pid file). Returns how many were
    killed. No-op off Windows."""
    api = _win32_process_api()
    if api is None:
        return 0
    pid_files: List[Path] = []
    for root, pattern in ((MULTIVIEW_OUTPUT_ROOT, f"*/run*/{RUN_PID_FILE}"), (PLACEHOLDER_OUTPUT_DIR, f"run*/{RUN_PID_FILE}")):
        try:
            pid_files += list(root.glob(pattern))
        except OSError:
            continue
    killed = 0
    for pid_file in pid_files:
        try:
            record = json.loads(pid_file.read_text(encoding="utf-8"))
            pid, created = int(record["pid"]), int(record["created"])
        except (OSError, ValueError, KeyError, TypeError):
            continue
        try:
            if api.terminate_pid_if_created_at(pid, created):
                killed += 1
                LOGGER.warning("Killed orphaned ffmpeg pid=%d left by a previous run (%s)", pid, pid_file.parent)
        except Exception as exc:
            _log_failure(f"kill orphaned ffmpeg pid={pid}", exc)
    return killed


async def _taskkill_tree(pid: int, label: str) -> None:
    try:
        kill_proc = await asyncio.create_subprocess_exec(
            "taskkill", "/F", "/T", "/PID", str(pid),
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            creationflags=_child_process_creationflags(),
        )
        await asyncio.wait_for(kill_proc.wait(), timeout=10.0)
    except (OSError, asyncio.TimeoutError) as exc:
        _log_failure(f"taskkill ffmpeg {label}", exc, logging.ERROR)


async def _terminate_ffmpeg(entry: dict, label: str, *, term_timeout: float = 5.0, kill_timeout: float = 3.0) -> None:
    """Stop one Multi-View/placeholder ffmpeg run.

    Windows: TerminateJobObject on the run's own job. It doesn't depend on
    process.returncode (asyncio's Proactor loop has been seen reporting a
    heavily-piped ffmpeg as exited while it was still encoding) and, unlike
    `taskkill /PID`, can't kill an unrelated process that reused the pid.
    Without a job: TerminateProcess through our own process handle (also
    immune to pid reuse), and taskkill only as a last resort while asyncio
    still considers the process alive."""
    process: asyncio.subprocess.Process = entry["process"]
    job = entry.pop("job", None)
    try:
        if sys.platform == "win32":
            api = _win32_process_api()
            terminated = False
            if job and api is not None:
                try:
                    terminated = api.terminate_job(job)
                except Exception as exc:
                    _log_failure(f"terminate job ffmpeg {label}", exc)
            if not terminated and process.returncode is None:
                try:
                    process.kill()
                except (ProcessLookupError, OSError):
                    pass
            try:
                await asyncio.wait_for(process.wait(), timeout=kill_timeout)
            except asyncio.TimeoutError:
                if process.returncode is None:
                    await _taskkill_tree(process.pid, label)
                    try:
                        await asyncio.wait_for(process.wait(), timeout=kill_timeout)
                    except asyncio.TimeoutError:
                        LOGGER.error("ffmpeg %s pid=%s did not exit after kill", label, process.pid)
        elif process.returncode is None:
            try:
                process.terminate()
                await asyncio.wait_for(process.wait(), timeout=term_timeout)
            except (asyncio.TimeoutError, ProcessLookupError):
                try:
                    process.kill()
                    await asyncio.wait_for(process.wait(), timeout=kill_timeout)
                except (asyncio.TimeoutError, ProcessLookupError) as exc:
                    _log_failure(f"kill ffmpeg {label}", exc, logging.ERROR)
    finally:
        if job:
            api = _win32_process_api()
            if api is not None:
                api.close(job)


def _rmtree_with_retries(path: Path, attempts: int = 5, delay: float = 1.0) -> bool:
    """Blocking (run it in a thread). Windows keeps a just-killed process's
    files locked for a moment, so one rmtree right after the kill used to
    leave the whole run directory (~100MB of segments) behind."""
    for attempt in range(attempts):
        try:
            shutil.rmtree(path)
            return True
        except OSError:
            if not path.exists():
                return True
            if attempt + 1 < attempts:
                time.sleep(delay)
    return not path.exists()


def _remove_tree_later(path: Path, label: str) -> None:
    """Delete a run directory off the event loop, retrying while files are locked."""
    _spawn_background_task(asyncio.to_thread(_rmtree_with_retries, path), f"remove {label} output dir")


def _prepare_run_dir(run_dir: Path, audio_outputs: int) -> None:
    """Blocking: a fresh, empty run directory (plus a{N}/ per Multi-View audio output)."""
    if run_dir.exists():
        shutil.rmtree(run_dir, ignore_errors=True)
    run_dir.mkdir(parents=True, exist_ok=True)
    for idx in range(audio_outputs):
        (run_dir / f"a{idx}").mkdir(parents=True, exist_ok=True)


async def _stop_multiview_process(
    channel_id: str,
    *,
    run_id: Optional[int] = None,
    term_timeout: float = 5.0,
    kill_timeout: float = 3.0,
) -> bool:
    """Stop the channel's current run (only if it is `run_id`, when given, so a
    caller holding a stale snapshot can't kill a newer run). True if stopped."""
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if not entry or (run_id is not None and entry.get("run_id") != run_id):
        return False
    _MULTIVIEW_PROCESSES.pop(channel_id, None)
    await _terminate_ffmpeg(entry, f"multiview={channel_id}", term_timeout=term_timeout, kill_timeout=kill_timeout)
    for task_key in ("watch_task", "log_task"):
        task = entry.get(task_key)
        if task and not task.done():
            task.cancel()
    tasks = [entry.get(key) for key in ("watch_task", "log_task") if entry.get(key)]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    _remove_tree_later(entry["output_dir"], f"multiview={channel_id}")
    return True


async def _wait_for_first_segment(
    out_dir: Path,
    process: "asyncio.subprocess.Process",
    timeout: float,
    poll_interval: float = 0.25,
) -> bool:
    playlist = out_dir / "index.m3u8"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.returncode is not None:
            return False
        if playlist.exists():
            try:
                text = playlist.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                text = ""
            if "#EXTINF" in text:
                first_segment = next((line for line in text.splitlines() if line.endswith(".ts")), None)
                if first_segment and (out_dir / first_segment).exists():
                    return True
        await asyncio.sleep(poll_interval)
    return False


def _multiview_entry_alive(entry: dict) -> bool:
    return entry["process"].returncode is None and not entry.get("exited")


def _running_multiview_count(exclude: str = "") -> int:
    """Concurrency slots in use: live runs plus spawns that reserved a slot
    and are still warming up or waiting for first segments."""
    in_use = {cid for cid, entry in _MULTIVIEW_PROCESSES.items() if _multiview_entry_alive(entry)}
    in_use |= _MULTIVIEW_SLOT_RESERVATIONS
    in_use.discard(exclude)
    return len(in_use)


MULTIVIEW_MEMBER_WARM_TIMEOUT = bounded_float(os.getenv("MULTIVIEW_MEMBER_WARM_TIMEOUT", "20"), 20.0, 3.0, 120.0)
MULTIVIEW_WATCHDOG_INTERVAL = bounded_float(os.getenv("MULTIVIEW_WATCHDOG_INTERVAL", "3"), 3.0, 1.0, 60.0)
MULTIVIEW_AUDIO_CHANNELS = os.getenv("MULTIVIEW_AUDIO_CHANNELS", "1").strip().lower() not in ("0", "false", "no")
# Extra wait for the placeholder session when a member has to be replaced by it.
MULTIVIEW_STANDIN_WAIT_SECONDS = 10.0

# MPEG-TS stream_type ffmpeg writes for AAC (ADTS): the generated silent track.
_MULTIVIEW_SILENCE_AUDIO_TYPE = 0x0F
# Monotonic time of the last GPU encoder / CUDA device failure (None = none yet).
_HW_ENCODER_FAILED_AT: Optional[float] = None
_CUDA_DECODE_FAILED_AT: Optional[float] = None
# Encoder-specific failure wording, matched against the message part of a
# failed run's log lines. Deliberately no bare "cuda": NVDEC's per-stream
# software fallback ("Failed setup for format cuda") mentions it too, and
# leaves the GPU encoder perfectly usable.
_HW_ENCODER_MARKERS = {
    "nvenc": (
        "h264_nvenc", "hevc_nvenc", "openencodesessionex", "nvenc", "no capable devices found",
        "cannot load nvcuda", "cannot load libcuda", "nvenc api version",
        "driver does not support the required nvenc api version",
    ),
    "qsv": (
        "h264_qsv", "hevc_qsv", "qsv", "mfx session", "libmfx", "libvpl", "device creation failed",
    ),
}
_HW_ENCODER_NAMES = {"nvenc": ("h264_nvenc", "hevc_nvenc"), "qsv": ("h264_qsv", "hevc_qsv")}
# CUDA *device* setup failures (no/broken NVIDIA driver). Unlike the per-stream
# NVDEC fallback these are fatal for every input opened with -hwaccel cuda
# (ffmpeg 9: "Hardware device setup failed for decoder" aborts the run), so a
# retry must drop -hwaccel.
_CUDA_INIT_FAILURE_MARKERS = (
    "cannot load nvcuda", "cannot load libcuda", "could not dynamically load cuda",
    "device creation failed", "no device available for decoder", "hardware device setup failed",
    "cuinit",
)
_FFMPEG_LOG_CONTEXT_RE = re.compile(r"^\s*\[([^\]]*)\]\s*")


def _ffmpeg_log_parts(line: str) -> Tuple[List[str], str]:
    """'[vost#0:0/h264_nvenc @ 0x1] [enc:h264_nvenc @ 0x2] Error ...' ->
    (['vost#0:0/h264_nvenc', 'enc:h264_nvenc'], 'error ...'), lowercased."""
    contexts: List[str] = []
    rest = line
    while True:
        match = _FFMPEG_LOG_CONTEXT_RE.match(rest)
        if not match:
            break
        contexts.append(match.group(1).split(" @ ")[0].strip().lower())
        rest = rest[match.end():]
    return contexts, rest.strip().lower()


def _is_ffmpeg_stream_info(message: str) -> bool:
    # Stream mapping / description / metadata lines name the encoder even when
    # it works ("Stream #0:0 -> #0:0 (h264 (native) -> h264 (h264_nvenc))").
    return message.startswith(("stream #", "stream mapping", "encoder")) or " -> " in message


def _looks_like_hw_encoder_failure(log_lines, encoder: str = "nvenc") -> bool:
    """True when a failed run's log blames the GPU *encoder* (NVENC or QSV)."""
    encoder = (encoder or "").lower()
    markers = _HW_ENCODER_MARKERS.get(encoder)
    if not markers:
        return False
    names = _HW_ENCODER_NAMES[encoder]
    for line in log_lines:
        contexts, message = _ffmpeg_log_parts(str(line))
        if not message or _is_ffmpeg_stream_info(message):
            continue
        if any(marker in message for marker in markers):
            return True
        # Anything the encoder itself logged on a failed run ("[h264_nvenc @ ..] InitializeEncoder failed").
        if any(context in names for context in contexts):
            return True
        # fftools' wrapper around it ("[enc:h264_nvenc @ ..] Error while opening encoder").
        if ("error while opening encoder" in message or "could not open encoder" in message) and any(
            context.endswith(names) for context in contexts
        ):
            return True
    return False


def _looks_like_cuda_init_failure(log_lines) -> bool:
    for line in log_lines:
        _, message = _ffmpeg_log_parts(str(line))
        if any(marker in message for marker in _CUDA_INIT_FAILURE_MARKERS):
            return True
    return False


def _multiview_encoder_plan(now: Optional[float] = None) -> Tuple[str, bool]:
    """(encoder, use -hwaccel cuda) for the next run. Within
    NVENC_FALLBACK_SECONDS of a GPU encoder failure new runs use libx264, then
    the GPU is tried again. CUDA decoding stays on in that fallback unless the
    CUDA device itself failed."""
    now = time.monotonic() if now is None else now
    encoder = MULTIVIEW_HWACCEL
    if (
        encoder in _HW_ENCODER_MARKERS
        and _HW_ENCODER_FAILED_AT is not None
        and now - _HW_ENCODER_FAILED_AT < NVENC_FALLBACK_SECONDS
    ):
        encoder = "none"
    cuda_broken = _CUDA_DECODE_FAILED_AT is not None and now - _CUDA_DECODE_FAILED_AT < NVENC_FALLBACK_SECONDS
    return encoder, MULTIVIEW_HWACCEL == "nvenc" and not cuda_broken


def _multiview_audio_index(data: dict) -> int:
    members = data.get("member_team_ids") or []
    active = data.get("active_audio_team_id") or (members[0] if members else "")
    return members.index(active) if active in members else 0


def _multiview_member_label(team_id: str) -> str:
    member = stream_state.get(team_id)
    return str(member.get("name") or team_id) if member else f"{team_id} (removed)"


class _MultiviewInput(NamedTuple):
    team_id: str                # the configured member
    session_id: str             # channel session ffmpeg reads for this pane
    has_audio: bool             # known to carry audio (else: silent track)
    audio_type: Optional[int]   # MPEG-TS stream_type of that audio, if known
    standin: bool               # an existing member replaced by the placeholder for this run


async def _warm_multiview_members(member_team_ids: List[str]) -> List[_MultiviewInput]:
    """Start every member's channel session in parallel and wait for each to
    have a playable window before ffmpeg opens them one after another (four
    cold members used to blow the startup timeout).

    A member still not ready after MULTIVIEW_MEMBER_WARM_TIMEOUT is fed from
    the "No Signal" placeholder session for this run instead: its own
    /stream/x.m3u8 would answer 503, ffmpeg's -i would fail, and one slow
    member would push the whole grid into failure backoff. The placeholder is
    only warmed once some member is still pending halfway through the wait,
    and the watchdog swaps the real member in once it is live."""
    session_ids = [team_id if team_id in stream_state else PLACEHOLDER_SESSION_ID for team_id in member_team_ids]
    waits: Dict[str, asyncio.Future] = {}
    for session_id in session_ids:
        if session_id not in waits:
            session = SESSIONS.get(session_id)
            session.touch()
            waits[session_id] = asyncio.ensure_future(session.wait_ready(MULTIVIEW_MEMBER_WARM_TIMEOUT))
    try:
        _, pending = await asyncio.wait(list(waits.values()), timeout=MULTIVIEW_MEMBER_WARM_TIMEOUT / 2)
        if pending and PLACEHOLDER_SESSION_ID not in waits:
            SESSIONS.get(PLACEHOLDER_SESSION_ID).touch()
        await asyncio.gather(*waits.values(), return_exceptions=True)
    finally:
        for wait in waits.values():
            if not wait.done():
                wait.cancel()

    def is_ready(session_id: str) -> bool:
        wait = waits.get(session_id)
        return bool(wait and wait.done() and not wait.cancelled() and wait.exception() is None and wait.result())

    standins = {
        index for index, session_id in enumerate(session_ids)
        if session_id != PLACEHOLDER_SESSION_ID and not is_ready(session_id)
    }
    placeholder_ready = is_ready(PLACEHOLDER_SESSION_ID)
    if (standins or PLACEHOLDER_SESSION_ID in waits) and not placeholder_ready:
        placeholder = SESSIONS.get(PLACEHOLDER_SESSION_ID)
        placeholder.touch()
        placeholder_ready = await placeholder.wait_ready(MULTIVIEW_STANDIN_WAIT_SECONDS)

    inputs: List[_MultiviewInput] = []
    for index, (team_id, session_id) in enumerate(zip(member_team_ids, session_ids)):
        standin = index in standins
        if standin:
            LOGGER.warning(
                "Multi-View member %s not ready after %.0fs; its pane shows No Signal for this run",
                team_id, MULTIVIEW_MEMBER_WARM_TIMEOUT,
            )
            session_id = PLACEHOLDER_SESSION_ID
        ready = placeholder_ready if session_id == PLACEHOLDER_SESSION_ID else True
        session = SESSIONS.peek(session_id)
        # Unknown audio (None) counts as none: -map N:a:0 on an input without
        # audio fails the whole run, the silent track never does.
        has_audio = bool(ready and session is not None and session.has_audio is True)
        signature = session.codec_signature if has_audio and session is not None else None
        audio_type = signature[1] if signature and len(signature) > 1 else None
        inputs.append(_MultiviewInput(team_id, session_id, has_audio, audio_type, standin))
    return inputs


def _multiview_spawn_wanted(channel_id: str, data: dict) -> bool:
    """Re-checked after every await of a spawn: the channel may have been
    removed/replaced, or stopped from the dashboard, in the meantime."""
    return stream_state.get(channel_id) is data and channel_id not in _MULTIVIEW_MANUAL_STOPS


async def _spawn_multiview(channel_id: str, data: dict) -> None:
    """Start ffmpeg for a Multi-View channel unless it's already running.

    Only ever runs as the channel's _MULTIVIEW_START_TASKS entry (see
    _request_multiview_start), which serializes starts per channel; Stop and
    Remove cancel that task. Outcomes are recorded, not returned: failures in
    _MULTIVIEW_FAILURES (backoff), refusals as a short hold, success as a
    ready _MULTIVIEW_PROCESSES entry."""
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if entry and _multiview_entry_alive(entry):
        return
    if not _multiview_spawn_wanted(channel_id, data) or _multiview_cooldown_remaining(channel_id) > 0:
        return
    if not FFMPEG_AVAILABLE:
        _record_multiview_refusal(channel_id, "ffmpeg is unavailable")
        return
    in_use = _running_multiview_count(exclude=channel_id)
    if in_use >= MAX_CONCURRENT_MULTIVIEW:
        _record_multiview_refusal(
            channel_id,
            f"{in_use}/{MAX_CONCURRENT_MULTIVIEW} concurrent Multi-View streams already running "
            "(stop one, or raise MAX_CONCURRENT_MULTIVIEW)",
        )
        return
    _MULTIVIEW_REFUSALS.pop(channel_id, None)
    _MULTIVIEW_SLOT_RESERVATIONS.add(channel_id)
    try:
        await _start_multiview_run(channel_id, data)
    finally:
        _MULTIVIEW_SLOT_RESERVATIONS.discard(channel_id)


async def _start_multiview_run(channel_id: str, data: dict) -> None:
    global _MULTIVIEW_RUN_COUNTER, _HW_ENCODER_FAILED_AT, _CUDA_DECODE_FAILED_AT
    inputs = await _warm_multiview_members(list(data["member_team_ids"]))
    if not _multiview_spawn_wanted(channel_id, data):
        LOGGER.info("Multi-View channel=%s was removed or stopped while warming up; not starting", channel_id)
        return
    stale = _MULTIVIEW_PROCESSES.get(channel_id)
    if stale is not None:  # a dead run the idle monitor hasn't reaped yet
        await _stop_multiview_process(channel_id, run_id=stale.get("run_id"))

    encoder, hw_decode = _multiview_encoder_plan()
    error = "ffmpeg exited unexpectedly"
    for _attempt in range(3):
        _MULTIVIEW_RUN_COUNTER += 1
        outcome, lines = await _launch_multiview_run(channel_id, data, _MULTIVIEW_RUN_COUNTER, inputs, encoder, hw_decode)
        if outcome != "failed":
            return
        error = _multiview_error_from_log(lines) if lines else "ffmpeg exited unexpectedly"
        if encoder in _HW_ENCODER_MARKERS and _looks_like_hw_encoder_failure(lines, encoder):
            # GPU encoder unavailable (driver, session limit shared with
            # Jellyfin's own transcodes, ...): retry this same spawn with
            # libx264 right away, and keep new runs on it for a while.
            cuda_broken = hw_decode and _looks_like_cuda_init_failure(lines)
            _HW_ENCODER_FAILED_AT = time.monotonic()
            if cuda_broken:
                _CUDA_DECODE_FAILED_AT = _HW_ENCODER_FAILED_AT
            hw_decode = hw_decode and not cuda_broken
            LOGGER.warning(
                "%s unavailable for multiview channel=%s (%s); retrying with libx264%s, GPU re-tried in %.0fs",
                encoder.upper(), channel_id, error, " + CUDA decoding" if hw_decode else "", NVENC_FALLBACK_SECONDS,
            )
            encoder = "none"
            continue
        if hw_decode and _looks_like_cuda_init_failure(lines):
            _CUDA_DECODE_FAILED_AT = time.monotonic()
            hw_decode = False
            LOGGER.warning("CUDA decoding unavailable for multiview channel=%s (%s); retrying with software decoding", channel_id, error)
            continue
        break
    _record_multiview_failure(channel_id, error)
    LOGGER.error("multiview channel=%s failed to produce first segments in time: %s", channel_id, error)


async def _launch_multiview_run(
    channel_id: str,
    data: dict,
    run_id: int,
    inputs: List[_MultiviewInput],
    encoder: str,
    hw_decode: bool,
) -> Tuple[str, List[str]]:
    """One ffmpeg attempt: ("ready" | "failed" | "aborted", log lines of a failed run)."""
    run_dir = MULTIVIEW_OUTPUT_ROOT / channel_id / f"run{run_id}"
    _MULTIVIEW_PENDING_RUN_DIRS.add(run_dir)
    try:
        await asyncio.to_thread(_prepare_run_dir, run_dir, len(inputs))
        if not _multiview_spawn_wanted(channel_id, data):
            _remove_tree_later(run_dir, f"multiview={channel_id}")
            return "aborted", []
        args = _build_multiview_ffmpeg_args(
            channel_id, data, run_dir, [item.has_audio for item in inputs], encoder,
            hw_decode=hw_decode, input_ids=[item.session_id for item in inputs],
        )
        try:
            process = await asyncio.create_subprocess_exec(MULTIVIEW_FFMPEG_PATH, *args, **_multiview_popen_kwargs(run_dir))
        except (FileNotFoundError, OSError) as exc:
            _log_failure(f"spawn ffmpeg multiview={channel_id}", exc, logging.ERROR)
            _remove_tree_later(run_dir, f"multiview={channel_id}")
            return "failed", ["could not launch ffmpeg"]
        job = _create_run_job(process.pid)
        _write_run_pid_file(run_dir, process.pid)

        log_lines: "deque[str]" = deque(maxlen=200)
        watch_task = _spawn_background_task(
            _watch_multiview_process(channel_id, process),
            f"watch multiview ffmpeg={channel_id}",
        )
        log_task = _spawn_background_task(
            _drain_multiview_log(channel_id, process, log_lines),
            f"drain multiview ffmpeg log={channel_id}",
        )
        now = time.monotonic()
        entry = {
            "process": process,
            "job": job,
            "output_dir": run_dir,
            "run_id": run_id,
            "audio_count": len(inputs),
            # Output a{N} carries member N's own (stream-copied) audio codec,
            # or the generated AAC silence; part of the views' source key.
            "audio_types": [item.audio_type if item.has_audio else _MULTIVIEW_SILENCE_AUDIO_TYPE for item in inputs],
            "standins": [item.team_id for item in inputs if item.standin],
            "encoder": encoder,
            "hw_decode": hw_decode,
            "ready": False,
            "started_at": now,
            "last_access": max(now, _MULTIVIEW_LAST_VIEWER.get(channel_id, 0.0)),
            "exited": False,
            "exit_code": None,
            "watch_task": watch_task,
            "log_task": log_task,
            "log_lines": log_lines,
        }
        _MULTIVIEW_PROCESSES[channel_id] = entry
        try:
            results = await asyncio.gather(*(
                _wait_for_first_segment(run_dir / f"a{idx}", process, MULTIVIEW_STARTUP_TIMEOUT_SECONDS)
                for idx in range(len(inputs))
            ))
        except asyncio.CancelledError:
            # Stop/Remove cancelled this spawn: never leave its ffmpeg behind.
            await _stop_multiview_process(channel_id, run_id=run_id)
            raise
        if not _multiview_spawn_wanted(channel_id, data) or _MULTIVIEW_PROCESSES.get(channel_id) is not entry:
            # Stopped/removed meanwhile, or the run was already taken down
            # elsewhere (then this stop is a no-op): not a failure to back off.
            await _stop_multiview_process(channel_id, run_id=run_id)
            return "aborted", []
        if all(results):
            entry["ready"] = True
            _clear_multiview_failure(channel_id)
            LOGGER.info(
                "Multi-View running channel=%s run=%d encoder=%s%s",
                channel_id, run_id, encoder, " (CUDA decoding)" if hw_decode else "",
            )
            for session_id in _multiview_view_session_ids(channel_id):
                SESSIONS.poke(session_id)
            return "ready", []
        lines = list(log_lines)
        await _stop_multiview_process(channel_id, run_id=run_id)
        return "failed", lines
    finally:
        _MULTIVIEW_PENDING_RUN_DIRS.discard(run_dir)


def _request_multiview_start(channel_id: str) -> None:
    data = stream_state.get(channel_id)
    if not data or data.get("type") != "multiview":
        return
    if channel_id in _MULTIVIEW_MANUAL_STOPS:
        return
    task = _MULTIVIEW_START_TASKS.get(channel_id)
    if task is not None and not task.done():
        return
    if _multiview_cooldown_remaining(channel_id) > 0:
        return
    task = _spawn_background_task(_spawn_multiview(channel_id, data), f"start multiview channel={channel_id}")
    _MULTIVIEW_START_TASKS[channel_id] = task

    def _forget(done: asyncio.Task, cid: str = channel_id) -> None:
        if _MULTIVIEW_START_TASKS.get(cid) is done:
            _MULTIVIEW_START_TASKS.pop(cid, None)

    task.add_done_callback(_forget)


def _multiview_start_in_progress(channel_id: str) -> bool:
    task = _MULTIVIEW_START_TASKS.get(channel_id)
    return task is not None and not task.done()


async def _cancel_multiview_start(channel_id: str) -> bool:
    """Cancel and await an in-progress spawn (which stops any ffmpeg it had
    already launched). True if one was running."""
    task = _MULTIVIEW_START_TASKS.pop(channel_id, None)
    if task is None or task.done():
        return False
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    return True


async def _stop_multiview_manually(channel_id: str) -> None:
    """Dashboard Stop: stop the grid and keep it stopped (see _MULTIVIEW_MANUAL_STOPS)."""
    _MULTIVIEW_MANUAL_STOPS[channel_id] = time.monotonic()
    await _cancel_multiview_start(channel_id)
    await _stop_multiview_process(channel_id)
    LOGGER.info("Multi-View channel=%s stopped from the dashboard; it stays stopped until a new viewer tunes in", channel_id)
    for session_id in _multiview_view_session_ids(channel_id):
        SESSIONS.poke(session_id)


def _clear_multiview_manual_stop(channel_id: str, why: str) -> None:
    if _MULTIVIEW_MANUAL_STOPS.pop(channel_id, None) is not None:
        LOGGER.info("Multi-View channel=%s may start again (%s)", channel_id, why)


def _multiview_view_session_ids(channel_id: str) -> List[str]:
    data = stream_state.get(channel_id) or {}
    return [channel_id] + [f"{channel_id}#a{idx}" for idx in range(len(data.get("member_team_ids") or []))]


def _multiview_view_for_session(session_id: str) -> Optional[Tuple[str, Optional[int]]]:
    """Map a session id to (multiview channel, audio index or None for 'active audio')."""
    if "#a" in session_id:
        channel_id, _, index_text = session_id.rpartition("#a")
        data = stream_state.get(channel_id)
        if data and data.get("type") == "multiview" and index_text.isdigit():
            return channel_id, int(index_text)
        return None
    data = stream_state.get(session_id)
    if data and data.get("type") == "multiview":
        return session_id, None
    return None


def _multiview_output_audio_tag(entry: dict, index: int) -> str:
    """Source-key component for Multi-View output a{index}: its audio codec
    (MPEG-TS stream_type) when known, else the output index itself."""
    audio_types = entry.get("audio_types") or []
    audio_type = audio_types[index] if 0 <= index < len(audio_types) else None
    return f"audio:{audio_type:#04x}" if isinstance(audio_type, int) else f"audio:a{index}"


def _resolve_multiview_view_source(channel_id: str, audio_index: Optional[int]) -> Optional[SourceSpec]:
    data = stream_state.get(channel_id)
    if not data or data.get("type") != "multiview":
        return None
    if channel_id in _MULTIVIEW_MANUAL_STOPS:
        return _placeholder_source()
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if entry and entry.get("ready") and _multiview_entry_alive(entry):
        members = data.get("member_team_ids") or []
        index = _multiview_audio_index(data) if audio_index is None else audio_index
        index = max(0, min(index, entry["audio_count"] - 1))
        label = _multiview_member_label(members[index]) if index < len(members) else f"audio {index}"
        # One key per (run, audio codec). All outputs of a run share segment
        # numbers and video timestamps, so switching the main channel's audio
        # to an output whose audio has the same codec is just a URL change
        # and continues seamlessly. Member audio is stream-copied, though, so
        # each output carries its own member's codec: switching to a different
        # one (say AAC -> AC-3) gets a new key, i.e. a new normalizer epoch and
        # a discontinuity, so the player re-probes instead of hitting a PMT
        # change mid-stream. When a codec isn't known the key uses the output
        # index, making every switch to/from it a clean discontinuity. A new
        # run (restart) is always a new key.
        return SourceSpec(
            key=("multiview", channel_id, entry["run_id"], _multiview_output_audio_tag(entry, index)),
            url=str(entry["output_dir"] / f"a{index}" / "index.m3u8"),
            local=True,
            label=f"Multi-View {label}",
        )
    _request_multiview_start(channel_id)
    if _multiview_cooldown_remaining(channel_id) > 0 or not FFMPEG_AVAILABLE:
        # Backoff, refusal (concurrency cap) or no ffmpeg: No Signal, not a 503.
        return _placeholder_source()
    return None  # starting: the session waits (and keeps its current window)


def _multiview_view_sessions_running(channel_id: str) -> bool:
    for session_id in _multiview_view_session_ids(channel_id):
        session = SESSIONS.peek(session_id)
        if session is not None and session.is_running:
            return True
    return False


def _touch_multiview_viewer(channel_id: str) -> None:
    now = time.monotonic()
    if channel_id in _MULTIVIEW_MANUAL_STOPS and not _multiview_view_sessions_running(channel_id):
        # No view session is running, so this request starts a new one: a
        # fresh tune after the Stop counts as an explicit play.
        _clear_multiview_manual_stop(channel_id, "new viewer tuned in")
    _MULTIVIEW_LAST_VIEWER[channel_id] = now
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if entry:
        entry["last_access"] = now


def _on_multiview_view_failure(session_id: str, source_key: tuple, reason: str) -> None:
    view = _multiview_view_for_session(session_id)
    if view is None or len(source_key) < 3 or source_key[0] != "multiview":
        return
    channel_id = view[0]
    run_id = source_key[2]
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if entry and entry.get("run_id") == run_id:
        _spawn_background_task(
            _restart_multiview(channel_id, f"view reported {reason}", run_id=run_id),
            f"restart multiview channel={channel_id}",
        )


async def _restart_multiview(
    channel_id: str,
    reason: str,
    run_id: Optional[int] = None,
    *,
    count_toward_backoff: bool = True,
) -> bool:
    """Restart a Multi-View. With `run_id` it only acts if that run is still
    the current one: the watchdog and the view sessions work from snapshots,
    and a restart that already replaced run N with N+1 must not be followed
    by a second one killing N+1. Without `run_id` (a config change, e.g. a
    member was removed) it also cancels a spawn in progress so the new run
    picks up the change. Restarts are counted in a rolling window and backed
    off (see _note_multiview_restart). True if a restart happened."""
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if run_id is not None:
        if entry is None or entry.get("run_id") != run_id:
            LOGGER.info(
                "Ignoring restart of stale multiview run channel=%s run=%s current=%s reason=%s",
                channel_id, run_id, entry.get("run_id") if entry else None, reason,
            )
            return False
    else:
        cancelled = await _cancel_multiview_start(channel_id)
        entry = _MULTIVIEW_PROCESSES.get(channel_id)
        if entry is None and not cancelled:
            return False
    if entry is not None and entry.get("restarting"):
        return False
    if entry is not None:
        entry["restarting"] = True
    count, delay = _note_multiview_restart(channel_id, reason) if count_toward_backoff else (0, 0.0)
    if delay > 0:
        LOGGER.warning(
            "Restarting multiview channel=%s run=%s reason=%s; %d restarts in %.0f min, next start held %.0fs",
            channel_id, entry.get("run_id") if entry else None, reason, count,
            MULTIVIEW_RESTART_WINDOW_SECONDS / 60, delay,
        )
    else:
        LOGGER.warning(
            "Restarting multiview channel=%s run=%s reason=%s",
            channel_id, entry.get("run_id") if entry else None, reason,
        )
    if entry is not None:
        await _stop_multiview_process(channel_id, run_id=entry["run_id"])
    _request_multiview_start(channel_id)  # no-op while held; the next viewer poll starts it
    return True


def _newest_segment_age(output_dir: Path) -> Optional[float]:
    """Seconds since this output last produced a segment, or None if it has
    no playlist either (broken output).

    ffmpeg's delete_segments removes old files constantly, so a file can
    vanish between the glob and its stat: that one file is skipped rather
    than the whole output being reported stalled (which restarted healthy
    grids). An output with no segment file visible falls back to its
    playlist's age, so it only counts as stalled once the playlist is stale."""
    newest = None
    try:
        paths = list(output_dir.glob("seg_*.ts"))
    except OSError:
        paths = []
    for path in paths:
        try:
            mtime = path.stat().st_mtime
        except OSError:
            continue
        newest = mtime if newest is None or mtime > newest else newest
    if newest is None:
        try:
            newest = (output_dir / "index.m3u8").stat().st_mtime
        except FileNotFoundError:
            return None
        except OSError:
            return 0.0  # can't tell right now (e.g. a sharing violation): not evidence of a stall
    return max(0.0, time.time() - newest)


async def _swap_in_ready_members(channel_id: str, entry: dict) -> None:
    """A pane showing the stand-in placeholder gets its real member back once
    that member is live: keep its session warm meanwhile and restart the run
    when it flows, as long as that restart wouldn't be backed off."""
    live = []
    for team_id in entry.get("standins") or []:
        if team_id not in stream_state:
            continue
        session = SESSIONS.get(team_id)
        session.touch()
        source = session.source
        if session.is_flowing() and source is not None and not _is_placeholder_key(source.key):
            live.append(team_id)
    if live and _multiview_restart_delay(_recent_multiview_restarts(channel_id) + 1) == 0:
        await _restart_multiview(channel_id, f"member(s) {live} live now", run_id=entry["run_id"])


async def _multiview_watchdog_pass(stall_after: float) -> None:
    now = time.monotonic()
    for channel_id, entry in list(_MULTIVIEW_PROCESSES.items()):
        if not entry.get("ready") or entry.get("restarting"):
            continue
        run_id = entry["run_id"]
        watched = now - entry.get("last_access", 0.0) < MULTIVIEW_IDLE_TIMEOUT_SECONDS
        if not _multiview_entry_alive(entry):
            if watched:
                await _restart_multiview(channel_id, f"ffmpeg exited code={entry.get('exit_code')}", run_id=run_id)
            continue
        if now - entry["started_at"] < MULTIVIEW_STARTUP_TIMEOUT_SECONDS:
            continue
        ages = await asyncio.to_thread(
            lambda e=entry: [_newest_segment_age(e["output_dir"] / f"a{i}") for i in range(e["audio_count"])]
        )
        stalled = [i for i, age in enumerate(ages) if age is None or age > stall_after]
        if stalled:
            await _restart_multiview(channel_id, f"output stalled audio={stalled}", run_id=run_id)
        elif watched and entry.get("standins"):
            await _swap_in_ready_members(channel_id, entry)


async def multiview_watchdog() -> None:
    """Restart a Multi-View run whose output stalls (one frozen input can hold
    xstack) or whose ffmpeg died while people are watching. The channel
    sessions hide the restart behind a discontinuity."""
    stall_after = max(3.0 * MULTIVIEW_SEGMENT_SECONDS, 12.0)
    while True:
        await asyncio.sleep(MULTIVIEW_WATCHDOG_INTERVAL)
        try:
            await _multiview_watchdog_pass(stall_after)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_failure("multiview watchdog", exc)


async def multiview_idle_monitor() -> None:
    while True:
        try:
            now = time.monotonic()
            for channel_id, entry in list(_MULTIVIEW_PROCESSES.items()):
                if entry.get("restarting"):
                    continue
                if not _multiview_entry_alive(entry):
                    if now - entry.get("last_access", 0.0) >= MULTIVIEW_IDLE_TIMEOUT_SECONDS:
                        LOGGER.warning("Reaping dead multiview ffmpeg channel=%s code=%s", channel_id, entry.get("exit_code"))
                        await _stop_multiview_process(channel_id, run_id=entry.get("run_id"))
                elif now - entry.get("last_access", now) > MULTIVIEW_IDLE_TIMEOUT_SECONDS:
                    LOGGER.info("Stopping idle multiview ffmpeg channel=%s", channel_id)
                    await _stop_multiview_process(channel_id, run_id=entry.get("run_id"))

            # The placeholder runs on demand: channel sessions (re)start it when a
            # channel has nothing live, and it stops after a stretch of disuse.
            state = _PLACEHOLDER_STATE
            if state and now - state.get("last_access", now) > PLACEHOLDER_IDLE_SECONDS:
                LOGGER.info("Stopping idle placeholder ffmpeg")
                await _stop_placeholder_process()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_failure("multiview idle monitor", exc)
        await asyncio.sleep(MULTIVIEW_IDLE_CHECK_INTERVAL)


def _sweep_output_dirs_sync(live_dirs: Set[Path], min_age: float) -> List[Path]:
    """Blocking. Delete Multi-View run dirs (<root>/<channel>/runN) and
    placeholder run dirs that no live run owns, plus channel dirs left empty.
    Dirs younger than `min_age` seconds are kept: a spawn may have just
    created one it hasn't registered yet."""
    removed: List[Path] = []
    now = time.time()
    live_parents = {path.parent for path in live_dirs}

    def stale(path: Path) -> bool:
        if path in live_dirs:
            return False
        try:
            return path.is_dir() and now - path.stat().st_mtime >= min_age
        except OSError:
            return False

    def children(path: Path) -> List[Path]:
        try:
            return list(path.iterdir())
        except OSError:
            return []

    for channel_dir in children(MULTIVIEW_OUTPUT_ROOT):
        if not channel_dir.is_dir():
            continue
        for run_dir in children(channel_dir):
            if stale(run_dir) and _rmtree_with_retries(run_dir, attempts=2, delay=0.5):
                removed.append(run_dir)
        if channel_dir not in live_parents and not children(channel_dir):
            try:
                channel_dir.rmdir()
                removed.append(channel_dir)
            except OSError:
                pass
    for run_dir in children(PLACEHOLDER_OUTPUT_DIR):
        if stale(run_dir) and _rmtree_with_retries(run_dir, attempts=2, delay=0.5):
            removed.append(run_dir)
    return removed


async def _sweep_output_dirs(min_age: float = 60.0) -> List[Path]:
    live: Set[Path] = {entry["output_dir"] for entry in _MULTIVIEW_PROCESSES.values()}
    live |= _MULTIVIEW_PENDING_RUN_DIRS | _PLACEHOLDER_PENDING_DIRS
    if _PLACEHOLDER_STATE:
        live.add(_PLACEHOLDER_STATE["output_dir"])
    removed = await asyncio.to_thread(_sweep_output_dirs_sync, live, min_age)
    if removed:
        LOGGER.info("Removed %d leftover Multi-View/placeholder output dir(s)", len(removed))
    return removed


async def multiview_output_sweeper() -> None:
    """Periodic safety net for run directories a stop couldn't delete (files
    still locked after all retries, a crash mid-stop, ...)."""
    while True:
        await asyncio.sleep(MULTIVIEW_SWEEP_INTERVAL)
        try:
            await _sweep_output_dirs()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_failure("sweep multiview output dirs", exc)


def _multiview_member_validation(member_team_ids: List[str]) -> Optional[str]:
    """Return an error message if the requested member list is invalid, else None."""
    if len(member_team_ids) not in (2, 4):
        return "Select exactly 2 or 4 channels for a Multi-View."
    seen = set()
    for team_id in member_team_ids:
        if team_id in seen:
            return "Duplicate channel selected."
        seen.add(team_id)
        member_data = stream_state.get(team_id)
        if not member_data:
            return f"Unknown channel: {team_id}"
        if member_data.get("type") == "multiview":
            return "A Multi-View channel cannot include another Multi-View channel."
    return None


def _multiview_audio_view(channel_id: str, audio_index: int) -> Optional[dict]:
    data = stream_state.get(channel_id)
    if not data or data.get("type") != "multiview":
        return None
    if not 0 <= audio_index < len(data.get("member_team_ids") or []):
        return None
    return data


async def _serve_multiview_audio_playlist(channel_id: str, audio_index: int, request: Request, segment_prefix: str):
    """Per-audio Multi-View channel ("🔊 Team · Name"): the same composited video
    with one member's audio. Switching audio inside Jellyfin = changing channel,
    which works on every client."""
    if _multiview_audio_view(channel_id, audio_index) is None:
        raise HTTPException(status_code=404, detail="Multi-View channel not found")
    if request.method == "HEAD":
        return Response(status_code=200, media_type=HLS_MEDIA_TYPE, headers={"Cache-Control": "no-cache"})
    _touch_multiview_viewer(channel_id)
    _maybe_record_playback_event(channel_id)
    return await _serve_session_playlist(f"{channel_id}#a{audio_index}", segment_prefix)


# The playlist filename is "audio-N", not "N": when an M3U entry has no
# tvg-chno, Jellyfin falls back to a purely numeric URL filename as the channel
# number, which turned the audio channels into stray channels 1, 2, 3.
@app.api_route("/multiview/{channel_id}/audio-{audio_index}.m3u8", methods=["GET", "HEAD"])
async def serve_multiview_audio_playlist(channel_id: str, audio_index: int, request: Request):
    return await _serve_multiview_audio_playlist(channel_id, audio_index, request, f"audio-{audio_index}/seg/")


@app.api_route("/multiview/{channel_id}/audio/{audio_index}.m3u8", methods=["GET", "HEAD"])
async def serve_multiview_audio_playlist_legacy(channel_id: str, audio_index: int, request: Request):
    # Old URL form, kept until Jellyfin's next guide refresh picks up the new one.
    return await _serve_multiview_audio_playlist(channel_id, audio_index, request, f"{audio_index}/seg/")


@app.get("/multiview/{channel_id}/audio-{audio_index}/seg/{seq}.ts")
@app.get("/multiview/{channel_id}/audio/{audio_index}/seg/{seq}.ts")
async def serve_multiview_audio_segment(channel_id: str, audio_index: int, seq: int):
    if _multiview_audio_view(channel_id, audio_index) is None:
        raise HTTPException(status_code=404, detail="Multi-View channel not found")
    _touch_multiview_viewer(channel_id)
    return _serve_session_segment(f"{channel_id}#a{audio_index}", seq)


# --- SHARED "NO SIGNAL" PLACEHOLDER ---
# A single, always-on synthetic HLS stream shown for ANY channel (regular team,
# always-live special channel, or Multi-View member) that currently has no
# live candidates - instead of a bare 404. This turns Jellyfin's ugly "fatal
# player error" into a clean "no signal" screen, and - since Multi-View's
# ffmpeg pulls every member through this exact same /stream/{team_id} route -
# also transparently fixes a Multi-View channel refusing to start entirely
# just because one of its members isn't currently live: that member's input
# simply resolves to this placeholder instead of a 404, so ffmpeg's -i for it
# always succeeds.
PLACEHOLDER_OUTPUT_DIR = DATA_DIR / "placeholder"
PLACEHOLDER_STARTUP_TIMEOUT_SECONDS = 20.0
PLACEHOLDER_IDLE_SECONDS = bounded_float(os.getenv("PLACEHOLDER_IDLE_SECONDS", "900"), 900.0, 60.0, 86400.0)
_PLACEHOLDER_STATE: Optional[dict] = None
_PLACEHOLDER_LOCK = asyncio.Lock()
# Bumped per placeholder ffmpeg start: names its run dir and is part of its
# source key, so channel sessions treat a restarted placeholder (segment
# numbers back at 0) as a new source instead of a lagging edge.
_PLACEHOLDER_RUN_COUNTER = 0
# Last spawn failure(s): {"count", "last_failure", "last_error"}. The next start
# waits _multiview_backoff_seconds(count), so a broken placeholder ffmpeg is
# no longer respawned (and logged as an ERROR) on every session poll.
_PLACEHOLDER_FAILURE: Optional[dict] = None
# Turned off once drawtext failed with this ffmpeg build (no libfreetype, no
# usable font): later starts render the logo without the caption.
_PLACEHOLDER_DRAWTEXT_OK = True
_PLACEHOLDER_PENDING_DIRS: Set[Path] = set()
_DRAWTEXT_FAILURE_MARKERS = (
    "drawtext", "fontconfig", "freetype", "fontfile", "font file", "could not load font", "cannot find a valid font",
)


def _ffmpeg_filter_path(path: Path) -> str:
    """A path for a single-quoted filter option value: forward slashes, and
    the drive colon escaped (ffmpeg splits filter options on ':')."""
    return str(path).replace("\\", "/").replace(":", "\\:").replace("'", "'\\''")


def _placeholder_font_option() -> str:
    """drawtext font selection. Bundled Windows ffmpeg builds usually have no
    fontconfig configuration, so font='Sans' fails or falls back unpredictably
    there: point at a real Windows font file instead."""
    if sys.platform == "win32":
        fonts_dir = Path(os.environ.get("WINDIR") or os.environ.get("SystemRoot") or r"C:\Windows") / "Fonts"
        for name in ("segoeui.ttf", "arial.ttf", "tahoma.ttf", "verdana.ttf"):
            candidate = fonts_dir / name
            if candidate.is_file():
                return f"fontfile='{_ffmpeg_filter_path(candidate)}':"
        return ""
    return "font='Sans':"


def _build_placeholder_ffmpeg_args(out_dir: Path, drawtext: bool = True) -> List[str]:
    """Pure command-builder for the shared "No Signal" loop (no I/O besides
    locating a font, unit-testable)."""
    logo_path = _resource_path("assets/jellyball-logo.png")
    video_filter = (
        "scale=1920:1080:force_original_aspect_ratio=decrease,"
        "pad=1920:1080:(ow-iw)/2:(oh-ih)/2"
    )
    if drawtext:
        video_filter += (
            f",drawtext={_placeholder_font_option()}text='No Signal':fontcolor=white:fontsize=64:"
            "box=1:boxcolor=black@0.5:boxborderw=16:x=(w-text_w)/2:y=h-200"
        )
    # -re paces both inputs at real time: without it ffmpeg encoded this loop as
    # fast as the CPU allowed (pinning a core and racing segments far ahead of
    # the wall clock). 1080p30 with 2s segments so a channel that starts on the
    # placeholder and then goes live doesn't make Jellyfin lock in a low
    # resolution/frame rate from its initial probe, and so a cold start has a
    # playable buffer within a few seconds.
    return [
        "-y",
        "-hide_banner", "-nostats", "-loglevel", "warning",
        "-re", "-loop", "1", "-framerate", "30", "-i", str(logo_path),
        "-re", "-f", "lavfi", "-i", "anullsrc=r=48000:cl=stereo",
        "-vf", video_filter,
        "-c:v", "libx264", "-preset", "ultrafast", "-tune", "stillimage",
        "-b:v", "1M", "-maxrate", "1M", "-bufsize", "2M",
        "-g", "60", "-keyint_min", "60", "-sc_threshold", "0", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "64k", "-ac", "2", "-ar", "48000",
        "-f", "hls",
        "-hls_time", "2",
        "-hls_list_size", "8",
        "-hls_flags", "delete_segments+independent_segments",
        "-hls_segment_type", "mpegts",
        "-hls_segment_filename", "seg_%05d.ts",
        "index.m3u8",
    ]


def _looks_like_drawtext_failure(log_lines) -> bool:
    text = "\n".join(str(line) for line in log_lines).lower()
    return any(marker in text for marker in _DRAWTEXT_FAILURE_MARKERS)


def _placeholder_cooldown_remaining() -> float:
    record = _PLACEHOLDER_FAILURE
    if not record:
        return 0.0
    elapsed = time.monotonic() - record["last_failure"]
    return max(0.0, _multiview_backoff_seconds(record["count"]) - elapsed)


def _record_placeholder_failure(error: str) -> float:
    global _PLACEHOLDER_FAILURE
    record = _PLACEHOLDER_FAILURE or {"count": 0, "last_failure": 0.0, "last_error": ""}
    record["count"] += 1
    record["last_failure"] = time.monotonic()
    record["last_error"] = error
    _PLACEHOLDER_FAILURE = record
    return _multiview_backoff_seconds(record["count"])


async def _drain_placeholder_log(process: "asyncio.subprocess.Process", log_lines: "deque[str]") -> None:
    # Same chunked reader as Multi-View (readline() can die on CR-only progress output).
    await _drain_multiview_log("placeholder", process, log_lines)


async def _watch_placeholder_process(process: "asyncio.subprocess.Process") -> None:
    returncode = await process.wait()
    if _PLACEHOLDER_STATE and _PLACEHOLDER_STATE["process"] is process:
        _PLACEHOLDER_STATE["exited"] = True
        if returncode != 0:
            LOGGER.warning("placeholder ffmpeg exited unexpectedly code=%s", returncode)


async def _stop_placeholder_process() -> None:
    global _PLACEHOLDER_STATE
    state, _PLACEHOLDER_STATE = _PLACEHOLDER_STATE, None
    if not state:
        return
    await _terminate_ffmpeg(state, "placeholder", term_timeout=5.0, kill_timeout=3.0)
    tasks = [state.get(key) for key in ("watch_task", "log_task") if state.get(key)]
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    _remove_tree_later(state["output_dir"], "placeholder")


async def _launch_placeholder_run(drawtext: bool) -> Tuple[bool, List[str]]:
    """One placeholder ffmpeg attempt (caller holds _PLACEHOLDER_LOCK):
    (ready, log lines of a failed attempt)."""
    global _PLACEHOLDER_STATE, _PLACEHOLDER_RUN_COUNTER
    _PLACEHOLDER_RUN_COUNTER += 1
    run_id = _PLACEHOLDER_RUN_COUNTER
    out_dir = PLACEHOLDER_OUTPUT_DIR / f"run{run_id}"
    _PLACEHOLDER_PENDING_DIRS.add(out_dir)
    try:
        await asyncio.to_thread(_prepare_run_dir, out_dir, 0)
        args = _build_placeholder_ffmpeg_args(out_dir, drawtext=drawtext)
        try:
            process = await asyncio.create_subprocess_exec(
                FFMPEG_PATH, *args,
                cwd=str(out_dir),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                creationflags=_ffmpeg_creationflags(),
            )
        except (FileNotFoundError, OSError) as exc:
            _log_failure("spawn placeholder ffmpeg", exc, logging.ERROR)
            _remove_tree_later(out_dir, "placeholder")
            return False, ["could not launch ffmpeg"]
        job = _create_run_job(process.pid)
        _write_run_pid_file(out_dir, process.pid)

        log_lines: "deque[str]" = deque(maxlen=200)
        watch_task = _spawn_background_task(_watch_placeholder_process(process), "watch placeholder ffmpeg")
        log_task = _spawn_background_task(_drain_placeholder_log(process, log_lines), "drain placeholder ffmpeg log")
        state = {
            "process": process,
            "job": job,
            "run_id": run_id,
            "output_dir": out_dir,
            "exited": False,
            "watch_task": watch_task,
            "log_task": log_task,
            "log_lines": log_lines,
            "ready": False,
            "last_access": time.monotonic(),
        }
        _PLACEHOLDER_STATE = state
        try:
            ready = await _wait_for_first_segment(out_dir, process, PLACEHOLDER_STARTUP_TIMEOUT_SECONDS)
        except asyncio.CancelledError:
            if _PLACEHOLDER_STATE is state:
                await _stop_placeholder_process()
            raise
        if ready and _PLACEHOLDER_STATE is state:
            state["ready"] = True
            return True, []
        lines = list(log_lines)
        if _PLACEHOLDER_STATE is state:
            await _stop_placeholder_process()
        return False, lines
    finally:
        _PLACEHOLDER_PENDING_DIRS.discard(out_dir)


async def _ensure_placeholder_running() -> bool:
    """Start the shared placeholder stream if it isn't already running. Idempotent;
    after a failed start it declines (returns False) until the backoff expires."""
    global _PLACEHOLDER_FAILURE, _PLACEHOLDER_DRAWTEXT_OK
    async with _PLACEHOLDER_LOCK:
        if _PLACEHOLDER_STATE and _PLACEHOLDER_STATE["process"].returncode is None and not _PLACEHOLDER_STATE.get("exited"):
            return True
        if not FFMPEG_AVAILABLE or _placeholder_cooldown_remaining() > 0:
            return False
        if _PLACEHOLDER_STATE is not None:  # a dead run: reap it before starting over
            await _stop_placeholder_process()

        lines: List[str] = []
        for _attempt in range(2):
            ready, lines = await _launch_placeholder_run(_PLACEHOLDER_DRAWTEXT_OK)
            if ready:
                _PLACEHOLDER_FAILURE = None
                return True
            if _PLACEHOLDER_DRAWTEXT_OK and _looks_like_drawtext_failure(lines):
                _PLACEHOLDER_DRAWTEXT_OK = False
                LOGGER.warning(
                    "placeholder ffmpeg: drawtext failed (%s); retrying without the 'No Signal' caption",
                    _multiview_error_from_log(lines),
                )
                continue
            break
        error = _multiview_error_from_log(lines)
        retry_in = _record_placeholder_failure(error)
        LOGGER.error("placeholder ffmpeg failed to produce a first segment: %s (retrying in %.0fs)", error, retry_in)
        return False


# --- CHANNEL SESSIONS (proxy-built continuous playlists; see hls_session.py) ---

SESSION_IDLE_SECONDS = bounded_float(os.getenv("SESSION_IDLE_SECONDS", "60"), 60.0, 10.0, 3600.0)
STREAM_STARTUP_TIMEOUT = bounded_float(os.getenv("STREAM_STARTUP_TIMEOUT", "20"), 20.0, 3.0, 120.0)
# A channel whose source hasn't produced anything by STREAM_STARTUP_TIMEOUT plays
# No Signal for this long (then retries the real source) instead of answering
# 503: Jellyfin's ffmpeg does not retry a failed first open.
STARTUP_PLACEHOLDER_SECONDS = bounded_float(os.getenv("STARTUP_PLACEHOLDER_SECONDS", "30"), 30.0, 5.0, 600.0)
STREAM_MAX_BANDWIDTH = bounded_int(os.getenv("STREAM_MAX_BANDWIDTH", "0"), 0, 0, 1_000_000_000)
# Prefix of every placeholder source key: the actual key is
# PLACEHOLDER_SOURCE_KEY + (placeholder run id,). Test with _is_placeholder_key().
PLACEHOLDER_SOURCE_KEY = ("placeholder",)

_PLACEHOLDER_START_TASK: Optional[asyncio.Task] = None


async def _session_fetch(url: str, headers: Dict[str, str], max_bytes: int, timeout_seconds: float) -> Optional[FetchResult]:
    client = _media_client()
    if client is None:
        return None
    timeout = httpx.Timeout(connect=5.0, read=timeout_seconds, write=10.0, pool=3.0)
    try:
        # httpx read timeouts are per-read; also cap the whole transfer so a
        # trickling upstream can't hold a live segment for minutes.
        result = await asyncio.wait_for(
            _fetch_upstream_body(client, url, headers, max_bytes, timeout),
            timeout_seconds + 5.0,
        )
    except asyncio.TimeoutError:
        return None
    if result is None:
        return None
    status_code, effective_url, content_type, body, _ = result
    return FetchResult(status_code, effective_url, content_type, body)


def _request_placeholder_start() -> None:
    global _PLACEHOLDER_START_TASK
    if not FFMPEG_AVAILABLE or _placeholder_cooldown_remaining() > 0:
        return
    if _PLACEHOLDER_START_TASK is None or _PLACEHOLDER_START_TASK.done():
        _PLACEHOLDER_START_TASK = _spawn_background_task(_ensure_placeholder_running(), "start placeholder stream")


def _placeholder_source_key(run_id: int) -> tuple:
    return PLACEHOLDER_SOURCE_KEY + (run_id,)


def _is_placeholder_key(key) -> bool:
    """True for any placeholder source key, whatever placeholder run it names."""
    return bool(key) and tuple(key)[:len(PLACEHOLDER_SOURCE_KEY)] == PLACEHOLDER_SOURCE_KEY


def _placeholder_source() -> Optional[SourceSpec]:
    state = _PLACEHOLDER_STATE
    if state and state.get("ready") and state["process"].returncode is None and not state.get("exited"):
        state["last_access"] = time.monotonic()
        return SourceSpec(
            # Per placeholder run: a restarted placeholder ffmpeg numbers its
            # segments from 0 again, which under one constant key looked like
            # a lagging edge; a new key is a clean source switch instead.
            key=_placeholder_source_key(state.get("run_id", 0)),
            url=str(state["output_dir"] / "index.m3u8"),
            local=True,
            label="No Signal",
        )
    _request_placeholder_start()
    return None


def _candidate_source(candidate: dict) -> Optional[SourceSpec]:
    url = _validate_upstream_url(str(candidate.get("url") or ""))
    if not url:
        return None
    referer = str(candidate.get("referer") or "")
    if referer and not _validate_upstream_url(referer):
        referer = ""
    return SourceSpec(
        key=candidate_source_key(candidate),
        url=url,
        referer=referer,
        origin=str(candidate.get("origin") or ""),
        label=str(candidate.get("provider") or ""),
    )


def _resolve_session_source(channel_id: str) -> Optional[SourceSpec]:
    """What should this channel's session play right now? (pull model)"""
    if channel_id == PLACEHOLDER_SESSION_ID:
        return _placeholder_source()
    view = _multiview_view_for_session(channel_id)
    if view is not None:
        return _resolve_multiview_view_source(*view)
    data = stream_state.get(channel_id)
    if data is None:
        return None
    candidates = data.get("candidates") or []
    if not candidates or data.get("exhausted"):
        return _placeholder_source()
    if data.get("startup_placeholder_until", 0.0) > time.monotonic():
        return _placeholder_source()
    active_index = data.get("active_index", 0)
    if active_index >= len(candidates):
        active_index = 0
    return _candidate_source(candidates[active_index]) or _placeholder_source()


def _on_session_failure(channel_id: str, source_key: tuple, reason: str) -> None:
    if _is_placeholder_key(source_key):
        return
    if _multiview_view_for_session(channel_id) is not None:
        _on_multiview_view_failure(channel_id, source_key, reason)
        return
    _spawn_background_task(request_failover(channel_id, source_key, reason), f"session failover {channel_id}")


def _on_session_incompatible(channel_id: str, source_key: tuple, reason: str) -> bool:
    """Returns True when a compatible standby exists (the session keeps going
    and picks it up), False to let a cold-starting session use the legacy proxy."""
    if _multiview_view_for_session(channel_id) is not None:
        return False
    data = stream_state.get(channel_id) or {}
    candidates = data.get("candidates") or []
    active_index = data.get("active_index", 0)
    has_alternative = (
        0 <= active_index < len(candidates)
        and candidate_source_key(candidates[active_index]) == tuple(source_key)
        and _pick_next_candidate(candidates, active_index) is not None
    )
    if has_alternative:
        # Mark it now so the next resolve can't hand the session the same source.
        candidates[active_index]["session_compatible"] = False
        candidates[active_index]["incompatible_at"] = time.time()
    _spawn_background_task(
        request_failover(channel_id, source_key, reason, incompatible=True),
        f"session incompatible failover {channel_id}",
    )
    return has_alternative


def _on_session_media_info(channel_id: str, source_key: tuple, has_audio: bool, signature: tuple) -> None:
    data = stream_state.get(channel_id)
    if not data:
        return
    for candidate in data.get("candidates") or []:
        if candidate_source_key(candidate) == tuple(source_key):
            candidate["has_audio"] = has_audio
            candidate["codec_signature"] = tuple(signature)
            break


SESSIONS = SessionRegistry(
    SessionHooks(
        fetch=lambda url, headers, max_bytes, timeout: _session_fetch(url, headers, max_bytes, timeout),
        headers_for=lambda referer, origin: _upstream_media_headers(referer, origin),
        resolve_source=lambda channel_id: _resolve_session_source(channel_id),
        report_failure=lambda channel_id, key, reason: _on_session_failure(channel_id, key, reason),
        report_incompatible=lambda channel_id, key, reason: _on_session_incompatible(channel_id, key, reason),
        on_media_info=lambda channel_id, key, has_audio, sig: _on_session_media_info(channel_id, key, has_audio, sig),
    ),
    SessionConfig(
        idle_timeout=SESSION_IDLE_SECONDS,
        bandwidth_cap=STREAM_MAX_BANDWIDTH,
        # Also editable live in Advanced Settings (TUNABLES); these env vars set the default.
        live_edge_segments=bounded_int(os.getenv("SESSION_LIVE_EDGE_SEGMENTS", "3"), 3, 1, 10),
        window_min_seconds=bounded_float(os.getenv("SESSION_WINDOW_SECONDS", "30"), 30.0, 12.0, 600.0),
        stale_min_seconds=bounded_float(os.getenv("SESSION_STALE_SECONDS", "15"), 15.0, 5.0, 300.0),
        fail_threshold=bounded_int(os.getenv("SESSION_FAIL_THRESHOLD", "3"), 3, 1, 20),
        segment_timeout=bounded_float(os.getenv("SESSION_SEGMENT_TIMEOUT", "15"), 15.0, 3.0, 120.0),
    ),
)


def _start_on_placeholder(channel_id: str) -> bool:
    data = stream_state.get(channel_id)
    if not data or data.get("type") == "multiview" or not FFMPEG_AVAILABLE:
        return False
    data["startup_placeholder_until"] = time.monotonic() + STARTUP_PLACEHOLDER_SECONDS
    LOGGER.info("Stream slow to start; showing No Signal meanwhile channel=%s", channel_id)
    _request_placeholder_start()
    SESSIONS.poke(channel_id)
    return True


async def _serve_session_playlist(session_id: str, segment_prefix: str, legacy=None) -> Response:
    session = SESSIONS.get(session_id)
    session.touch()
    if session.state == "legacy" and legacy is not None:
        return await legacy()
    await session.wait_ready(STREAM_STARTUP_TIMEOUT)
    if session.state == "legacy" and legacy is not None:
        return await legacy()
    if not session.window and _start_on_placeholder(session_id):
        await session.wait_ready(10.0)
    if not session.window:
        return Response(
            status_code=503,
            content="Stream is starting",
            headers={"Retry-After": "2", "Cache-Control": "no-cache"},
        )
    return _hls_response(session.render_playlist(segment_prefix))


def _serve_session_segment(session_id: str, seq: int) -> Response:
    session = SESSIONS.peek(session_id)
    if session is None:
        return Response(status_code=404, content="Segment not found")
    session.touch()
    segment = session.get_segment(seq)
    if segment is None:
        return Response(status_code=404, content="Segment not found")
    return Response(
        content=segment.data,
        media_type="video/mp2t",
        headers={"Cache-Control": "public, max-age=120", "Access-Control-Allow-Origin": "*"},
    )


async def _serve_channel_playlist(team_id: str, request: Request) -> Response:
    if team_id == PLACEHOLDER_SESSION_ID and FFMPEG_AVAILABLE:
        # Stand-in input for a Multi-View member that no longer exists.
        return await _serve_session_playlist(team_id, f"{team_id}/seg/")
    data = stream_state.get(team_id)
    if not data:
        return Response(status_code=404, content="Stream unavailable")
    if request.method == "HEAD":
        # Jellyfin's M3U tuner HEADs extensionless URLs to decide between
        # raw-TS sharing and ffmpeg HLS input; always say HLS.
        return Response(status_code=200, media_type=HLS_MEDIA_TYPE, headers={"Cache-Control": "no-cache"})
    if data.get("type") == "multiview":
        _touch_multiview_viewer(team_id)
    _maybe_record_playback_event(team_id)

    async def legacy():
        return await _legacy_proxy_stream(team_id, request)

    return await _serve_session_playlist(team_id, f"{team_id}/seg/", legacy=None if data.get("type") == "multiview" else legacy)


@app.api_route("/stream/{team_id}.m3u8", methods=["GET", "HEAD"])
async def stream_playlist(team_id: str, request: Request):
    return await _serve_channel_playlist(team_id, request)


@app.api_route("/stream/{team_id}/seg/{seq}.ts", methods=["GET", "HEAD"])
async def stream_segment(team_id: str, seq: int):
    if team_id in stream_state and stream_state[team_id].get("type") == "multiview":
        _touch_multiview_viewer(team_id)
    return _serve_session_segment(team_id, seq)


@app.api_route("/stream/{team_id}", methods=["GET", "HEAD"])
async def proxy_stream(team_id: str, request: Request, provider: str = ""):
    """Extensionless alias kept for existing Jellyfin tuner configs. `?provider=`
    pins one provider for debugging via the legacy passthrough proxy."""
    if provider and request.method == "GET" and team_id in stream_state:
        return await _legacy_proxy_stream(team_id, request, provider)
    return await _serve_channel_playlist(team_id, request)


@app.api_route("/playlist.m3u", methods=["GET", "HEAD"], response_class=PlainTextResponse)
async def generate_m3u(request: Request):
    base_url = _public_base_url(request)
    lines = ["#EXTM3U"]
    for team_id, data in stream_state.items():
        if not _channel_listed(data):
            continue
        # The .m3u8 extension makes Jellyfin treat the URL as an HLS manifest
        # (always through its own ffmpeg, never "direct play" of our URL or
        # raw-TS sharing). Channel names stay stable: health is shown on the
        # dashboard, not appended to the name (which renamed channels in
        # Jellyfin on every guide refresh).
        name = str(data.get("name") or team_id)
        tvg_id = _channel_tvg_id(team_id, data)
        logo = _channel_logo_url(data)
        logo_attr = f' tvg-logo="{_m3u_attribute(logo)}"' if logo else ""
        group = _m3u_attribute(_channel_group_title(data))
        lines.append(
            f'#EXTINF:-1 tvg-id="{_m3u_attribute(tvg_id)}" '
            f'tvg-name="{_m3u_attribute(name)}"{logo_attr} '
            f'group-title="{group}",{_m3u_title(name)}'
        )
        lines.append(f"{base_url}/stream/{urllib.parse.quote(team_id, safe='')}.m3u8")
        for audio_id, audio_name, path in _multiview_audio_channels(team_id, data):
            lines.append(
                f'#EXTINF:-1 tvg-id="{_m3u_attribute(audio_id)}" '
                f'tvg-name="{_m3u_attribute(audio_name)}"{logo_attr} '
                f'group-title="{group}",{_m3u_title(audio_name)}'
            )
            lines.append(f"{base_url}{path}")
    return "\n".join(lines)


def _multiview_audio_tvg_id(base_tvg_id: str, member_team_id: str) -> str:
    """tvg-id for one Multi-View per-audio channel.

    Keyed by the member's own channel id (stable even if the member list is
    reordered) rather than its positional index, and always ending in the
    fixed, non-numeric literal ".audio" - regardless of what the member id
    itself looks like. Jellyfin's M3U tuner derives a channel number from
    tvg-id (or channel-id) when tvg-chno is absent - see
    M3uParser.GetChannelNumber upstream - so ending in a constant word
    instead of a bare digit means it can never be mistaken for one.
    """
    return f"{base_tvg_id}.{member_team_id}.audio"


def _multiview_audio_channels(channel_id: str, data: dict) -> List[Tuple[str, str, str]]:
    """(tvg-id, display name, URL path) for each per-audio Multi-View channel."""
    if data.get("type") != "multiview" or not MULTIVIEW_AUDIO_CHANNELS:
        return []
    base_tvg_id = _channel_tvg_id(channel_id, data)
    mv_name = str(data.get("name") or channel_id)
    return [
        (
            _multiview_audio_tvg_id(base_tvg_id, member),
            f"🔊 {_multiview_member_label(member)} · {mv_name}",
            f"/multiview/{channel_id}/audio-{index}.m3u8",
        )
        for index, member in enumerate(data.get("member_team_ids") or [])
    ]


TVGUIDE_SPECIAL_CHANNEL_IDS = {
    "9200004533": "BigTenNetwork.us",
    "9200006937": "ESPN.us",
    "9200012351": "ESPN2.us",
    "9233011350": "ESPNU.us",
    "9233008440": "FoxSports1.us",
    "9200009884": "FoxSports2.us",
    "9233013235": "CBSSportsNetwork.us",
    "9233011830": "TNT.us",
    "9200017734": "ACCNetwork.us",
    "9233008517": "SECNetwork.us",
    "9200009223": "MLBNetwork.us",
    "9200000070": "NBATV.us",
    "9200004330": "NFLNetwork.us",
    "9233009455": "NHLNetwork.us",
    "9233011874": "ABC.us",
    "9233002271": "FOX.us",
    "9200018514": "CBS.us",
    "9233009876": "NBC.us",
    "9233011398": "CW.us",
    "9233000403": "TBS.us",
    "9233004106": "USANetwork.us",
    "9200009547": "TruTV.us",
}
GUIDE_HORIZON_DAYS = 10
_TVGUIDE_EPG_CACHE: Dict[str, List[dict]] = {}
_TVGUIDE_EPG_CACHED_AT: float = 0.0


async def _fetch_tvguide_epg() -> Dict[str, List[dict]]:
    """Fetch linear schedule blocks for always-live sports channels from TVGuide."""
    global _TVGUIDE_EPG_CACHE, _TVGUIDE_EPG_CACHED_AT
    now = time.monotonic()
    if _TVGUIDE_EPG_CACHE and (now - _TVGUIDE_EPG_CACHED_AT < 1800.0):
        return _TVGUIDE_EPG_CACHE

    url = f"https://backend.tvguide.com/tvschedules/tvguide/9100001138/web?start={int(time.time())}&duration={GUIDE_HORIZON_DAYS * 1440}"
    headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Referer": "https://www.tvguide.com/",
    }
    client = state.SHARED_HTTP_CLIENT or httpx.AsyncClient(timeout=10.0, follow_redirects=True)
    owns_client = state.SHARED_HTTP_CLIENT is None
    try:
        resp = await client.get(url, headers=headers)
        if resp.status_code == 200:
            items = resp.json().get("data", {}).get("items", [])
            schedules: Dict[str, List[dict]] = {}
            for item in items:
                ch_id = str(item.get("channel", {}).get("sourceId", ""))
                tvg_id = TVGUIDE_SPECIAL_CHANNEL_IDS.get(ch_id)
                if tvg_id:
                    schedules[tvg_id] = item.get("programSchedules", [])
            if schedules:
                _TVGUIDE_EPG_CACHE = schedules
                _TVGUIDE_EPG_CACHED_AT = now
    except Exception as exc:
        LOGGER.debug("TVGuide EPG fetch failed: %s", type(exc).__name__)
    finally:
        if owns_client:
            await client.aclose()
    return _TVGUIDE_EPG_CACHE


def _channel_programmes(
    team_id: str,
    data: dict,
    guide_start: datetime,
    guide_end: datetime,
    tvguide_schedules: Dict[str, List[dict]],
) -> List[dict]:
    """Raw (unescaped) programme blocks for one ordinary (non-Multi-View) channel.

    Each entry is {start, stop, title, desc, category, icon}; `category` and
    `icon` may be "" meaning the XML renderer omits that tag. This mirrors the
    guide logic that used to live inline in generate_xmltv() - kept as its own
    helper so a Multi-View channel can pull a member's own guide data (see
    _multiview_member_programmes) to build its composite guide.
    """
    name = str(data.get("name") or team_id)
    logo = _channel_logo_url(data)
    channel_id = _channel_tvg_id(team_id, data)
    programmes: List[dict] = []

    def block(start: datetime, stop: datetime, title: str, desc: str, category: str = "") -> None:
        programmes.append({"start": start, "stop": stop, "title": title, "desc": desc,
                           "category": category, "icon": logo})

    if _channel_is_off_season(data):
        resumes = _season_resume_label(data.get("category", ""))
        block(guide_start, guide_end,
              f"{name}: Off-season" + (f" (resumes {resumes})" if resumes else ""),
              f"{name} is between seasons. The channel returns to the guide automatically"
              + (f" when the season resumes around {resumes}." if resumes else " when the season resumes."))
        return programmes

    dt_start, dt_stop = parse_team_schedule(data)
    has_specific_game = bool(
        dt_start and dt_stop and dt_stop >= guide_start and dt_start <= guide_end
    )

    if has_specific_game:
        if dt_start > guide_start:
            block(guide_start, min(dt_start, guide_end), f"{name} Standby",
                  f"Waiting for the scheduled {name} broadcast.")
        block(max(dt_start, guide_start), min(dt_stop, guide_end), f"{name} Scheduled Event",
              f"Scheduled live stream window for {name}. Searching starts one hour before the event "
              "and continues one hour after the scheduled end.")
        if dt_stop < guide_end:
            block(max(dt_stop, guide_start), guide_end, f"{name} Standby",
                  f"Post-event standby for {name}; the next scheduled event will refresh this guide.")
    else:
        programs = tvguide_schedules.get(channel_id, [])
        if _channel_is_always_live(data) and programs:
            for prog in programs:
                p_start = datetime.fromtimestamp(prog.get("startTime", 0), tz=timezone.utc)
                p_stop = datetime.fromtimestamp(prog.get("endTime", 0), tz=timezone.utc)
                if p_stop <= guide_start or p_start >= guide_end:
                    continue
                block(max(p_start, guide_start), min(p_stop, guide_end),
                      prog.get("title") or f"{name} Live",
                      prog.get("description") or f"Live broadcast on {name}.", "Sports")
        elif _channel_is_always_live(data):
            block(guide_start, guide_end, f"{name} Live",
                  f"Always-live sports channel for {name}; the linear stream is monitored continuously.", "Sports")
        else:
            block(guide_start, guide_end, f"{name} Standby",
                  f"No verified scheduled event is available for {name} in the next {GUIDE_HORIZON_DAYS} days.")
    return programmes


def _programme_xml_lines(channel_id: str, programmes: List[dict]) -> List[str]:
    """Render _channel_programmes()-shaped dicts as <programme> XML lines."""
    lines: List[str] = []
    channel_id_esc = _xml_attr(channel_id)
    for p in programmes:
        lines.append(
            f'  <programme channel="{channel_id_esc}" start="{xmltv_ts(p["start"])}" stop="{xmltv_ts(p["stop"])}">'
        )
        lines.append(f'    <title>{_xml_text(p["title"])}</title>')
        if p.get("category"):
            lines.append(f'    <category>{_xml_text(p["category"])}</category>')
        lines.append(f'    <desc>{_xml_text(p["desc"])}</desc>')
        if p.get("icon"):
            lines.append(f'    <icon src="{_xml_attr(p["icon"])}" />')
        lines.append('  </programme>')
    return lines


MULTIVIEW_PANE_LABELS = {
    "side_by_side_2": ["Left", "Right"],
    "grid_2x2": ["Top-left", "Top-right", "Bottom-left", "Bottom-right"],
}
MULTIVIEW_MAX_PROGRAMMES = 500
MULTIVIEW_MIN_INTERVAL_SECONDS = 300.0
_MULTIVIEW_FILLER_TITLE_MARKERS = ("Standby", "No verified scheduled event")


def _multiview_pane_labels(layout: str, count: int) -> List[str]:
    labels = MULTIVIEW_PANE_LABELS.get(layout)
    if labels and len(labels) == count:
        return labels
    return [f"Pane {i + 1}" for i in range(count)]


def _is_filler_programme_title(title: str) -> bool:
    return any(marker in title for marker in _MULTIVIEW_FILLER_TITLE_MARKERS)


def _multiview_member_programmes(
    member_team_ids: List[str],
    guide_start: datetime,
    guide_end: datetime,
    tvguide_schedules: Dict[str, List[dict]],
) -> Dict[str, List[dict]]:
    """Each member's own programme list, or a single "No Signal" block for a
    member that has since been removed (member_team_ids may reference a
    channel id no longer in stream_state)."""
    result: Dict[str, List[dict]] = {}
    for member_id in member_team_ids:
        member_data = stream_state.get(member_id)
        if member_data is None:
            result[member_id] = [{
                "start": guide_start, "stop": guide_end,
                "title": "No Signal", "desc": "", "category": "", "icon": "",
            }]
        else:
            result[member_id] = _channel_programmes(
                member_id, member_data, guide_start, guide_end, tvguide_schedules
            )
    return result


def _programme_at(programmes: List[dict], instant: datetime) -> Optional[dict]:
    for p in programmes:
        if p["start"] <= instant < p["stop"]:
            return p
    return programmes[-1] if programmes else None


def _multiview_boundaries(
    member_programmes: Dict[str, List[dict]], guide_start: datetime, guide_end: datetime
) -> List[datetime]:
    """Every member programme start/stop within the guide window, plus the
    window's own edges - the points at which the composite guide can change."""
    points = {guide_start, guide_end}
    for programmes in member_programmes.values():
        for p in programmes:
            if guide_start <= p["start"] <= guide_end:
                points.add(p["start"])
            if guide_start <= p["stop"] <= guide_end:
                points.add(p["stop"])
    return sorted(points)


def _merge_short_intervals(
    boundaries: List[datetime], min_seconds: float = MULTIVIEW_MIN_INTERVAL_SECONDS
) -> List[Tuple[datetime, datetime]]:
    """Turn sorted boundary points into (start, stop) intervals, folding any
    interval shorter than `min_seconds` into its neighbor so the guide doesn't
    fill up with sliver-length programmes."""
    if len(boundaries) < 2:
        return []
    intervals: List[Tuple[datetime, datetime]] = []
    start = boundaries[0]
    for i in range(1, len(boundaries)):
        stop = boundaries[i]
        is_last = i == len(boundaries) - 1
        if (stop - start).total_seconds() < min_seconds and not is_last:
            continue  # keep accumulating rather than committing a short interval
        intervals.append((start, stop))
        start = stop
    if len(intervals) >= 2 and (intervals[-1][1] - intervals[-1][0]).total_seconds() < min_seconds:
        prev_start, _ = intervals[-2]
        _, last_stop = intervals[-1]
        intervals[-2] = (prev_start, last_stop)
        intervals.pop()
    return intervals


def _cap_intervals(
    intervals: List[Tuple[datetime, datetime]], cap: int = MULTIVIEW_MAX_PROGRAMMES
) -> List[Tuple[datetime, datetime]]:
    """Repeatedly merge the shortest interval into its shorter neighbor until
    at most `cap` remain, so a channel can never emit an unbounded number of
    programmes."""
    intervals = list(intervals)
    while len(intervals) > cap:
        durations = [(b - a).total_seconds() for a, b in intervals]
        idx = min(range(len(durations)), key=lambda i: durations[i])
        if idx == 0:
            merge_idx = 0
        elif idx == len(intervals) - 1:
            merge_idx = idx - 1
        else:
            merge_idx = idx - 1 if durations[idx - 1] <= durations[idx + 1] else idx
        a_start, _ = intervals[merge_idx]
        _, b_stop = intervals[merge_idx + 1]
        intervals[merge_idx] = (a_start, b_stop)
        intervals.pop(merge_idx + 1)
    return intervals


def _cap_programme_dicts(entries: List[dict], cap: int = MULTIVIEW_MAX_PROGRAMMES) -> List[dict]:
    if len(entries) <= cap:
        return entries
    capped = list(entries[:cap])
    capped[-1] = dict(capped[-1])
    capped[-1]["stop"] = entries[-1]["stop"]
    return capped


def _multiview_pane_title(member_id: str, programme: Optional[dict], present: bool) -> str:
    """The text to show for one pane: the real programme title, or the
    member's own display name when there's nothing but filler ("... Standby"
    / "No verified scheduled event"), or "No Signal" for a removed member."""
    if not present:
        return "No Signal"
    title = str((programme or {}).get("title") or "")
    if not title or _is_filler_programme_title(title):
        return _multiview_member_label(member_id)
    return title


def _multiview_pane_info(
    member_team_ids: List[str],
    layout: str,
    member_programmes: Dict[str, List[dict]],
    instant: datetime,
) -> List[Tuple[str, str, str]]:
    """(position label, current pane title, member display name) for every
    pane, sampled at `instant`. `member display name` is "" for a removed
    member (nothing to show in parentheses)."""
    labels = _multiview_pane_labels(layout, len(member_team_ids))
    info: List[Tuple[str, str, str]] = []
    for label, member_id in zip(labels, member_team_ids):
        present = member_id in stream_state
        programme = _programme_at(member_programmes.get(member_id) or [], instant)
        pane_title = _multiview_pane_title(member_id, programme, present)
        member_name = _multiview_member_label(member_id) if present else ""
        info.append((label, pane_title, member_name))
    return info


def _multiview_pane_desc_lines(pane_info: List[Tuple[str, str, str]]) -> List[str]:
    lines = []
    for label, pane_title, member_name in pane_info:
        if member_name and pane_title != member_name:
            lines.append(f"{label}: {pane_title} ({member_name})")
        else:
            lines.append(f"{label}: {pane_title}")
    return lines


def _multiview_programmes(
    channel_id: str,
    data: dict,
    guide_start: datetime,
    guide_end: datetime,
    tvguide_schedules: Dict[str, List[dict]],
) -> Tuple[List[dict], Dict[str, List[dict]]]:
    """Programme dicts for a Multi-View main channel, plus one programme list
    per per-audio channel (keyed by the audio channel's own tvg-id, see
    _multiview_audio_tvg_id / _multiview_audio_channels).

    The main channel's guide window is split at every member's own programme
    boundary (so the composite title/desc only changes when a pane's content
    actually changes), short slivers are merged into a neighbor, and the
    result is capped so a Multi-View channel can never flood the guide.
    """
    member_team_ids = list(data.get("member_team_ids") or [])
    layout = data.get("layout") or ""
    logo = _channel_logo_url(data)
    mv_name = str(data.get("name") or channel_id)
    base_tvg_id = _channel_tvg_id(channel_id, data)

    if not member_team_ids:
        # Malformed/legacy entry with no members recorded - fall back to a
        # single placeholder block rather than emitting an empty title.
        return (
            [{
                "start": guide_start, "stop": guide_end,
                "title": f"{mv_name} Live", "desc": mv_name,
                "category": "Sports", "icon": logo,
            }],
            {},
        )

    member_programmes = _multiview_member_programmes(
        member_team_ids, guide_start, guide_end, tvguide_schedules
    )
    boundaries = _multiview_boundaries(member_programmes, guide_start, guide_end)
    intervals = _cap_intervals(_merge_short_intervals(boundaries))

    active_audio_id = data.get("active_audio_team_id") or member_team_ids[0]
    active_audio_name = _multiview_member_label(active_audio_id)

    main_programmes: List[dict] = []
    for start, stop in intervals:
        pane_info = _multiview_pane_info(member_team_ids, layout, member_programmes, start)
        title = " | ".join(pane_title for _, pane_title, _ in pane_info)
        desc_lines = _multiview_pane_desc_lines(pane_info)
        desc_lines.append(f"Audio: {active_audio_name} — change channel to the 🔊 channels to switch audio")
        main_programmes.append({
            "start": start, "stop": stop,
            "title": title, "desc": "\n".join(desc_lines),
            "category": "Sports", "icon": logo,
        })

    audio_programmes: Dict[str, List[dict]] = {}
    for member_id in member_team_ids:
        audio_id = _multiview_audio_tvg_id(base_tvg_id, member_id)
        present = member_id in stream_state
        member_name = _multiview_member_label(member_id)
        entries: List[dict] = []
        for p in member_programmes.get(member_id) or []:
            pane_info = _multiview_pane_info(member_team_ids, layout, member_programmes, p["start"])
            pane_title = _multiview_pane_title(member_id, p, present)
            desc_lines = [f"Audio from {member_name} in {mv_name}."]
            desc_lines.extend(_multiview_pane_desc_lines(pane_info))
            entries.append({
                "start": p["start"], "stop": p["stop"],
                "title": f"🔊 {pane_title}", "desc": "\n".join(desc_lines),
                "category": "Sports", "icon": logo,
            })
        audio_programmes[audio_id] = _cap_programme_dicts(entries)

    return main_programmes, audio_programmes


@app.api_route("/epg.xml", methods=["GET", "HEAD"], response_class=PlainTextResponse)
async def generate_xmltv(request: Request = None):
    now_utc = datetime.now(timezone.utc)
    xml = ['<?xml version="1.0" encoding="UTF-8"?>', '<tv>']

    for team_id, data in stream_state.items():
        if not _channel_listed(data):
            continue
        channel_id = _channel_tvg_id(team_id, data)
        name_esc = _xml_text(data["name"])
        logo = _channel_logo_url(data)
        icon_tag = f'\n    <icon src="{_xml_attr(logo)}" />' if logo else ""
        xml.append(f'  <channel id="{_xml_attr(channel_id)}">')
        xml.append(f'    <display-name>{name_esc}</display-name>{icon_tag}')
        xml.append('  </channel>')
        for audio_id, audio_name, _ in _multiview_audio_channels(team_id, data):
            xml.append(f'  <channel id="{_xml_attr(audio_id)}">')
            xml.append(f'    <display-name>{_xml_text(audio_name)}</display-name>{icon_tag}')
            xml.append('  </channel>')

    guide_start = now_utc - timedelta(hours=1)
    guide_end = now_utc + timedelta(days=GUIDE_HORIZON_DAYS)
    tvguide_schedules = await _fetch_tvguide_epg()

    for team_id, data in stream_state.items():
        if not _channel_listed(data):
            continue
        channel_id = _channel_tvg_id(team_id, data)

        if data.get("type") == "multiview":
            main_programmes, audio_programmes = _multiview_programmes(
                team_id, data, guide_start, guide_end, tvguide_schedules
            )
            xml.extend(_programme_xml_lines(channel_id, main_programmes))
            for audio_id, programmes in audio_programmes.items():
                xml.extend(_programme_xml_lines(audio_id, programmes))
        else:
            programmes = _channel_programmes(team_id, data, guide_start, guide_end, tvguide_schedules)
            xml.extend(_programme_xml_lines(channel_id, programmes))

    xml.append('</tv>')
    return "\n".join(xml)


@app.get("/api/status")
async def api_status(auth: bool = Depends(verify_dashboard_auth)):
    """Return a compact status snapshot for local dashboards and health checks."""
    channels = []
    for team_id, data in stream_state.items():
        if data.get("type") == "multiview":
            # Multi-View cards are rendered separately (no .channel-card class,
            # a different status model - running/stopped rather than
            # healthy/searching) and are polled via /api/ffmpeg-status instead.
            # Including them here would make applyStatusSnapshot()'s channel
            # count permanently mismatch the .channel-card DOM count, which
            # forces a reload-loop on every 5s poll for as long as any
            # Multi-View channel exists.
            continue
        candidates = data.get("candidates", [])
        active_index = data.get("active_index", 0)
        active = candidates[active_index] if 0 <= active_index < len(candidates) else None
        session = SESSIONS.peek(team_id)
        channels.append({
            "team_id": team_id,
            "name": data.get("name", team_id),
            "healthy": bool(data.get("is_healthy")),
            "watching": bool(session is not None and session.is_watched()),
            "on_placeholder": bool(session is not None and session.is_watched() and _session_on_placeholder(session)),
            "exhausted": bool(data.get("exhausted")),
            "failover_count": int(data.get("failover_count", 0)),
            "candidate_count": len(candidates),
            "active_provider": active.get("provider") if active else None,
            "stream_window_active": is_stream_window_active(data),
            "start_time": data.get("start_time", ""),
            "stop_time": data.get("stop_time", ""),
            "schedule_status": data.get("schedule_status", ""),
            "season_resume_label": _season_resume_label(data.get("category", "")),
            "category": data.get("category", "custom"),
            "content_type": data.get("content_type", "team"),
            "always_live": bool(data.get("always_live")),
            "catalog_key": data.get("catalog_key", ""),
            "scrape_in_progress": bool(data.get("scrape_in_progress", False)),
            "last_scrape_started": data.get("last_scrape_started", 0.0),
            "last_scrape_completed": data.get("last_scrape_completed", 0.0),
            "scrape_result": data.get("scrape_result", "pending"),
            "scrape_error": data.get("scrape_error", ""),
        })

    return {
        "status": "ok",
        "port": config.PORT,
        "channels": channels,
        "channel_count": len(channels),
    }


def _session_status(channel_id: str, snapshot: dict) -> dict:
    data = stream_state.get(channel_id) or {}
    candidates = data.get("candidates") or []
    active_index = data.get("active_index", 0)
    active = candidates[active_index] if 0 <= active_index < len(candidates) else {}
    source_key = snapshot.get("source_key") or []
    return {
        **snapshot,
        "name": data.get("name", channel_id),
        "on_placeholder": bool(source_key and source_key[0] == "placeholder"),
        "exhausted": bool(data.get("exhausted")),
        "active_provider": active.get("provider"),
        "codec_signature": list(active.get("codec_signature") or []) or None,
        "failover_count": int(data.get("failover_count", 0)),
        "last_failover": data.get("last_failover"),
        "candidate_count": len(candidates),
    }


@app.get("/api/sessions")
async def api_sessions(auth: bool = Depends(verify_dashboard_auth)):
    """Live per-channel playback state: what each running channel session is
    playing, its throughput/latency, and its failover history."""
    return {
        "sessions": {cid: _session_status(cid, snap) for cid, snap in SESSIONS.snapshot().items()},
    }


def _prometheus_label(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", " ").replace('"', '\\"')


@app.get("/metrics", response_class=PlainTextResponse)
async def prometheus_metrics(auth: bool = Depends(verify_dashboard_auth)):
    """Prometheus text exposition of the main health numbers (dashboard auth applies)."""
    lines = [
        "# HELP jellyball_channels Configured channels.",
        "# TYPE jellyball_channels gauge",
        f"jellyball_channels {len(stream_state)}",
        "# HELP jellyball_channel_healthy 1 when the channel's active source is healthy.",
        "# TYPE jellyball_channel_healthy gauge",
    ]
    for team_id, data in stream_state.items():
        lines.append(f'jellyball_channel_healthy{{channel="{_prometheus_label(team_id)}"}} {1 if data.get("is_healthy") else 0}')
    lines += [
        "# HELP jellyball_channel_failovers_total Failovers since start.",
        "# TYPE jellyball_channel_failovers_total counter",
    ]
    for team_id, data in stream_state.items():
        lines.append(f'jellyball_channel_failovers_total{{channel="{_prometheus_label(team_id)}"}} {int(data.get("failover_count", 0))}')
    snapshots = SESSIONS.snapshot()
    lines += [
        "# HELP jellyball_session_watched 1 while someone is watching the channel.",
        "# TYPE jellyball_session_watched gauge",
    ]
    for cid, snap in snapshots.items():
        lines.append(f'jellyball_session_watched{{channel="{_prometheus_label(cid)}"}} {1 if snap.get("watched") else 0}')
    lines += [
        "# HELP jellyball_session_bitrate_kbps Recent segment bitrate.",
        "# TYPE jellyball_session_bitrate_kbps gauge",
    ]
    for cid, snap in snapshots.items():
        if snap.get("bitrate_kbps") is not None:
            lines.append(f'jellyball_session_bitrate_kbps{{channel="{_prometheus_label(cid)}"}} {snap["bitrate_kbps"]}')
    lines += [
        "# HELP jellyball_session_segment_seconds_p95 95th percentile segment download time.",
        "# TYPE jellyball_session_segment_seconds_p95 gauge",
    ]
    for cid, snap in snapshots.items():
        if snap.get("segment_ms_p95") is not None:
            lines.append(
                f'jellyball_session_segment_seconds_p95{{channel="{_prometheus_label(cid)}"}} {snap["segment_ms_p95"] / 1000:.3f}'
            )
    return "\n".join(lines) + "\n"


@app.get("/api/logs")
async def api_logs(limit: int = 200, auth: bool = Depends(verify_dashboard_auth)):
    """Return recent application log lines for the local dashboard."""
    limit = max(1, min(limit, 1000))
    lines = await asyncio.to_thread(_tail_log_file, limit)
    return {"logs": lines, "count": len(lines)}


def _tail_log_file(limit: int) -> List[str]:
    """Read only the end of the log (off the event loop) instead of the whole file."""
    try:
        with LOG_FILE.open("rb") as log_file:
            log_file.seek(0, os.SEEK_END)
            size = log_file.tell()
            chunk = min(size, max(64 * 1024, limit * 400))
            log_file.seek(size - chunk)
            data = log_file.read(chunk)
    except OSError:
        return []
    lines = data.decode("utf-8", errors="replace").splitlines()
    if chunk < size and lines:
        lines = lines[1:]  # first line is probably partial
    return lines[-limit:]


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, tab: str = "channels", status: str = "", auth: bool = Depends(verify_dashboard_auth)):
    base_url = _public_base_url(request)
    dashboard_metrics = await _load_dashboard_metrics_async()
    metrics = dashboard_metrics
    favorites = dashboard_metrics["favorites"]
    provider_totals = dashboard_metrics["provider_totals"]
    notif_cfg = await get_notification_config()
    jellyfin_cfg = await get_jellyfin_config()
    provider_rotation_enabled = await get_setting_async("provider_rotation_mode", "0") == "1"
    provider_url_rows = [
        {
            "name": provider.name,
            "base_url": provider.base_url,
            "default_base_url": provider._default_base_url,
        }
        for provider in ACTIVE_PROVIDERS
        if isinstance(provider, HtmlAggregatorScraper)
    ]
    update_check_enabled = await get_setting_async("update_check_enabled", "0") == "1"
    update_banner = None
    if update_check_enabled and _update_available():
        update_banner = {
            "latest": _UPDATE_STATE["latest"],
            "url": _UPDATE_STATE.get("url") or "",
        }
    catalog_entries = await get_catalog_entries()
    active_catalog_keys = {
        data.get("catalog_key")
        for data in stream_state.values()
        if data.get("catalog_key")
    }

    provider_health = []
    if metrics["failovers_by_provider"]:
        provider_stats = {}
        for prov, count in metrics["failovers_by_provider"].items():
            total = provider_totals.get(prov, 0) or 1
            success_rate = max(0, 100 - (count * 100 / total)) if total > 0 else 100
            provider_stats[prov] = (success_rate, count, total)

        for prov in sorted(provider_stats.keys(), key=lambda p: provider_stats[p][0], reverse=True):
            rate, fails, total = provider_stats[prov]
            rate_color = "var(--success)" if rate > 90 else ("var(--warning)" if rate > 70 else "var(--danger)")
            provider_health.append({"provider": prov, "rate": rate, "rate_color": rate_color, "total": total})

    catalog_group_defs = (
        ("ncaaf", "College Football"),
        ("ncaam", "College Basketball"),
        ("nfl", "NFL"),
        ("mlb", "MLB"),
        ("nhl", "NHL"),
        ("nba", "NBA"),
        ("special", "Always-Live Sports Channels"),
    )
    catalog_groups = []
    for group_key, group_label in catalog_group_defs:
        group_entries = [
            entry for entry in catalog_entries
            if ("special" if entry["content_type"] == "channel" else entry["category"]) == group_key
        ]
        if not group_entries:
            continue
        catalog_groups.append({
            "label": group_label,
            "entries": [
                {
                    "catalog_key": entry["catalog_key"],
                    "name": entry["name"],
                    "checked": entry["catalog_key"] in active_catalog_keys,
                    "always_live": entry["always_live"],
                }
                for entry in group_entries
            ],
        })
    catalog_source_text = (
        "College directories refreshed from ESPN."
        if catalog._CATALOG_REMOTE_LOADED
        else "Using bundled teams; ESPN college directories will be retried on the next refresh."
    )

    failover_stats = [
        {"provider": prov, "count": count}
        for prov, count in metrics["failovers_by_provider"].items()
    ]

    events = []
    for ts, team_id, prov, ev_type, details in metrics["recent_events"]:
        badge_color = "var(--danger)" if ev_type in ["exhausted", "danger"] else ("var(--warning)" if ev_type == "failover" else "var(--success)")
        events.append({"ts": ts, "team_id": team_id, "prov": prov, "ev_type": ev_type, "details": details, "badge_color": badge_color})

    if security.DASHBOARD_AUTH_MODE == "open":
        auth_badge = {"text": "\U0001f513 Local Open Access", "title": "No DASHBOARD_PASSWORD; only reachable from this computer"}
    elif security.DASHBOARD_AUTH_MODE == "generated":
        auth_badge = {"text": "\U0001f512 Generated Password", "title": f"Generated password in {security.DASHBOARD_PASSWORD_FILE}"}
    else:
        auth_badge = {"text": "\U0001f512 Password Protected", "title": ""}
    webhook_discord_badge = {"text": "Discord Alert On" if notif_cfg["discord_webhook_url"] else "Discord Off"}
    webhook_telegram_badge = {"text": "Telegram Alert On" if (notif_cfg["telegram_bot_token"] and notif_cfg["telegram_chat_id"]) else "Telegram Off"}
    jellyfin_badge = {"text": "\U0001f347 Jellyfin Auto-Refresh On" if jellyfin_cfg["jellyfin_api_key"] else "\U0001f347 Jellyfin Manual"}

    def _channel_watch_fields(t_id: str, data: dict) -> dict:
        session = SESSIONS.peek(t_id)
        watching = bool(session is not None and session.is_watched())
        on_placeholder = bool(watching and _session_on_placeholder(session))
        return {
            "watching": watching,
            "on_placeholder": on_placeholder,
            "failover_count": int(data.get("failover_count", 0)),
        }

    def _build_channel(t_id: str, data: dict) -> dict:
        dot_class = "online" if data.get('is_healthy') else "offline"
        if data.get('is_healthy'):
            status_text = "Stream Stable & Active"
        elif data.get('schedule_status') == "off_season":
            resume_label = _season_resume_label(data.get('category', ''))
            status_text = f"Off-season (resumes {resume_label})" if resume_label else "Off-season"
        else:
            status_text = "Searching / Re-evaluating"
        candidates_list = data.get('candidates', [])

        candidates_options = []
        for idx, cand in enumerate(candidates_list):
            is_active = "★ " if idx == data.get('active_index', 0) else ""
            prov = cand.get('provider', 'Unknown')
            title_trunc = cand.get("match_title", "Stream")[:25]
            candidates_options.append({"idx": idx, "label": f"{is_active}{prov} - {title_trunc}"})

        channel_badge = "Always live" if data.get("always_live") else (
            str(data.get("category") or "manual").upper()
        )
        remove_label = "Disable" if data.get("catalog_key") else "Remove"

        active_provider = ""
        if candidates_list:
            active_index = data.get("active_index", 0)
            if 0 <= active_index < len(candidates_list):
                active_provider = candidates_list[active_index].get("provider", "")

        channel = {
            "team_id": t_id,
            "name": data["name"],
            "badge": channel_badge,
            "dot_class": dot_class,
            "status_text": status_text,
            "candidates_count": len(candidates_list),
            "has_candidates": bool(candidates_list),
            "candidates_options": candidates_options,
            "active_provider": active_provider,
            "is_favorite": t_id in favorites,
            "remove_label": remove_label,
            "auto_disable_after": data.get('auto_disable_after', ''),
        }
        channel.update(_channel_watch_fields(t_id, data))
        return channel

    favorites_channels = []
    other_channels = []
    for t_id, data in stream_state.items():
        if data.get('type') == "multiview":
            continue
        channel = _build_channel(t_id, data)
        if channel["is_favorite"]:
            favorites_channels.append(channel)
        else:
            other_channels.append(channel)

    multiview_checkbox_list = [
        {"team_id": mv_t_id, "name": mv_data.get("name", mv_t_id)}
        for mv_t_id, mv_data in stream_state.items()
        if mv_data.get("type") != "multiview"
    ]

    multiview_rows = []
    for mv_id, mv_data in stream_state.items():
        if mv_data.get("type") != "multiview":
            continue
        mv_entry = _MULTIVIEW_PROCESSES.get(mv_id)
        mv_running = bool(mv_entry and mv_entry["process"].returncode is None and not mv_entry.get("exited"))
        member_ids = mv_data.get("member_team_ids", [])
        member_names = ", ".join(stream_state.get(m, {}).get("name", m) for m in member_ids)
        audio_options = [
            {
                "team_id": m,
                "name": stream_state.get(m, {}).get("name", m),
                "selected": m == mv_data.get("active_audio_team_id"),
            }
            for m in member_ids
        ]
        mv_failure = _MULTIVIEW_FAILURES.get(mv_id)
        failure = None
        if not mv_running and mv_failure:
            failure = {
                "last_error": mv_failure["last_error"],
                "count": mv_failure["count"],
                "retry_in": round(_multiview_cooldown_remaining(mv_id)),
            }
        multiview_rows.append({
            "mv_id": mv_id,
            "name": mv_data.get("name", mv_id),
            "layout": mv_data.get("layout", ""),
            "status": "\U0001f7e2 Running" if mv_running else "⚪ Stopped",
            "member_names": member_names,
            "audio_options": audio_options,
            "failure": failure,
        })

    multiview = {
        "ffmpeg_available": FFMPEG_AVAILABLE,
        "ffmpeg_path": FFMPEG_PATH,
        "checkbox_list": multiview_checkbox_list,
        "rows": multiview_rows,
    }

    context = {
        "request": request,
        "tab": tab,
        "version": __version__,
        "base_url": base_url,
        "update_banner": update_banner,
        "auth_badge": auth_badge,
        "jellyfin_badge": jellyfin_badge,
        "webhook_discord_badge": webhook_discord_badge,
        "webhook_telegram_badge": webhook_telegram_badge,
        "channels_empty": not any(data.get('type') != "multiview" for data in stream_state.values()),
        "auth_mode": security.DASHBOARD_AUTH_MODE,
        "dashboard_password_file": str(security.DASHBOARD_PASSWORD_FILE),
        "catalog_source_text": catalog_source_text,
        "catalog_entries_count": len(catalog_entries),
        "active_catalog_count": len(active_catalog_keys),
        "catalog_groups": catalog_groups,
        "favorites": favorites_channels,
        "others": other_channels,
        "multiview": multiview,
        "provider_health": provider_health,
        "failover_stats": failover_stats,
        "events": events,
        "provider_url_rows": provider_url_rows,
        "jellyfin_cfg": jellyfin_cfg,
        "notif_cfg": notif_cfg,
        "jellyfin_api_key_secret": _secret_input("jellyfin_api_key", jellyfin_cfg['jellyfin_api_key']),
        "discord_webhook_secret": _secret_input("discord_webhook_url", notif_cfg['discord_webhook_url']),
        "telegram_bot_token_secret": _secret_input("telegram_bot_token", notif_cfg['telegram_bot_token']),
        "advanced_settings_groups": _advanced_settings_html(),
        "update_check_enabled": update_check_enabled,
        "show_offseason_channels": catalog.SHOW_OFFSEASON_CHANNELS,
        "provider_rotation_enabled": provider_rotation_enabled,
        "dashboard_data": {},
    }
    return TEMPLATES.TemplateResponse(request, "dashboard.html", context)

def _secret_input(name: str, saved_value: str) -> dict:
    """Data for a password-type input that never echoes a saved secret back
    into the page (rendered by the dashboard template's secret_input macro).
    Blank on submit keeps the saved value; the checkbox clears it."""
    return {
        "name": name,
        "has_value": bool(saved_value),
        "masked_tail": saved_value[-4:] if saved_value else "",
    }


def _submitted_secret(submitted: str, clear: str, current: str) -> str:
    """Resolve a _secret_input submission against the current value."""
    if clear:
        return ""
    return (submitted or "").strip() or current


@app.post("/settings/notifications")
async def update_notifications(
    discord_webhook_url: str = Form(""),
    telegram_bot_token: str = Form(""),
    telegram_chat_id: str = Form(""),
    clear_discord_webhook_url: str = Form(""),
    clear_telegram_bot_token: str = Form(""),
    auth: bool = Depends(verify_dashboard_auth),
):
    current = await get_notification_config()
    discord = _submitted_secret(discord_webhook_url, clear_discord_webhook_url, current["discord_webhook_url"])
    token = _submitted_secret(telegram_bot_token, clear_telegram_bot_token, current["telegram_bot_token"])
    await set_setting_async("discord_webhook_url", discord)
    await set_setting_async("telegram_bot_token", token)
    await set_setting_async("telegram_chat_id", telegram_chat_id.strip())
    return RedirectResponse(url="/?tab=alerts&status=saved", status_code=303)

@app.post("/settings/jellyfin")
async def update_jellyfin_settings(
    jellyfin_url: str = Form("http://localhost:8096"),
    jellyfin_api_key: str = Form(""),
    jellyfin_task_id: str = Form(""),
    clear_jellyfin_api_key: str = Form(""),
    auth: bool = Depends(verify_dashboard_auth),
):
    jellyfin_url = jellyfin_url.strip().rstrip("/")
    if not _validate_upstream_url(jellyfin_url, allow_private=True):
        return RedirectResponse(url="/?tab=alerts&status=jellyfin_url_invalid", status_code=303)
    current = await get_jellyfin_config()
    new_key = (jellyfin_api_key or "").strip()
    old_host = urllib.parse.urlsplit(current["jellyfin_url"]).netloc.lower()
    new_host = urllib.parse.urlsplit(jellyfin_url).netloc.lower()
    # The saved key must never follow a URL change to another host (that would
    # hand the key to whatever server the new URL names): require it again.
    if new_host != old_host and current["jellyfin_api_key"] and not new_key and not clear_jellyfin_api_key:
        return RedirectResponse(url="/?tab=alerts&status=jellyfin_key_required", status_code=303)
    api_key = "" if clear_jellyfin_api_key else (new_key or current["jellyfin_api_key"])
    await set_setting_async("jellyfin_url", jellyfin_url)
    await set_setting_async("jellyfin_api_key", api_key)
    await set_setting_async("jellyfin_task_id", jellyfin_task_id.strip())
    return RedirectResponse(url="/?tab=alerts&status=jellyfin_saved", status_code=303)

@app.post("/settings/test_jellyfin")
async def test_jellyfin_refresh_endpoint(auth: bool = Depends(verify_dashboard_auth)):
    success = await trigger_jellyfin_refresh()
    status_code = "jellyfin_success" if success else "jellyfin_failed"
    return RedirectResponse(url=f"/?tab=alerts&status={status_code}", status_code=303)

@app.post("/settings/test_alert")
async def test_alert(auth: bool = Depends(verify_dashboard_auth)):
    await send_alert("🧪 Jellyball Notification Test", "Proactive monitoring alerts are functioning correctly.", "info")
    return RedirectResponse(url="/?tab=alerts&status=test_sent", status_code=303)


async def _set_catalog_entry_enabled(entry: dict, enabled: bool) -> bool:
    team_id = entry["team_id"]
    if enabled:
        if team_id not in stream_state:
            await save_team_async(
                team_id,
                entry["name"],
                entry["query"],
                entry["logo_url"],
                "",
                "",
                entry["category"],
                entry["source_id"],
                entry["content_type"],
                entry["search_terms"],
                entry["always_live"],
                entry["catalog_key"],
            )
            stream_state[team_id] = {
                "name": entry["name"],
                "query": entry["query"],
                "candidates": [],
                "active_index": 0,
                "is_healthy": False,
                "logo_url": entry["logo_url"],
                "start_time": "",
                "stop_time": "",
                "category": entry["category"],
                "source_id": entry["source_id"],
                "content_type": entry["content_type"],
                "search_terms": entry["search_terms"],
                "always_live": entry["always_live"],
                "catalog_key": entry["catalog_key"],
                "tvg_id": entry.get("tvg_id", ""),
                "group_title": entry.get("group_title", ""),
            **_scrape_lifecycle_defaults(),
            }
            _start_team_scrape_loop(team_id)
            return True
    else:
        if team_id in stream_state:
            await _remove_channel(team_id)
            return True
        await delete_team_async(team_id)
    return False


def _catalog_selection_changes(
    entries: List[dict],
    selected_keys: Set[str],
    active_catalog_keys: Set[str],
) -> tuple[Dict[str, dict], Set[str], Set[str]]:
    entries_by_key = {entry["catalog_key"]: entry for entry in entries}
    unknown_keys = selected_keys - set(entries_by_key)
    if unknown_keys:
        raise ValueError("Invalid catalog selection")
    return (
        entries_by_key,
        selected_keys - active_catalog_keys,
        (active_catalog_keys & set(entries_by_key)) - selected_keys,
    )


@app.post("/catalog/apply")
async def apply_catalog(
    catalog_keys: Optional[List[str]] = Form(None),
    auth: bool = Depends(verify_dashboard_auth),
):
    entries = await get_catalog_entries()
    selected_keys = {key.strip() for key in (catalog_keys or []) if key.strip()}
    active_catalog_keys = {
        data.get("catalog_key")
        for data in stream_state.values()
        if data.get("catalog_key")
    }
    try:
        entries_by_key, keys_to_enable, keys_to_disable = _catalog_selection_changes(
            entries,
            selected_keys,
            active_catalog_keys,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    changed_count = 0
    for catalog_key in keys_to_enable:
        changed_count += int(await _set_catalog_entry_enabled(entries_by_key[catalog_key], True))
    for catalog_key in keys_to_disable:
        changed_count += int(await _set_catalog_entry_enabled(entries_by_key[catalog_key], False))

    if changed_count:
        _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after catalog apply")
    return RedirectResponse(url=f"/?tab=channels&status=catalog_applied&changed={changed_count}", status_code=303)


@app.post("/catalog/toggle")
async def toggle_catalog(
    catalog_key: str = Form(...),
    enabled: bool = Form(False),
    auth: bool = Depends(verify_dashboard_auth),
):
    entries = await get_catalog_entries()
    entry = next((item for item in entries if item["catalog_key"] == catalog_key), None)
    if not entry:
        raise HTTPException(status_code=404, detail="Catalog entry not found")

    await _set_catalog_entry_enabled(entry, enabled)
    _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after catalog toggle")
    return RedirectResponse(url="/?tab=channels&status=catalog_toggled", status_code=303)


TEAM_NAME_MAX_LENGTH = 120
TEAM_QUERY_MAX_LENGTH = 200


async def _add_manual_team(
    team_name: str,
    search_query: str,
    *,
    team_id: str = "",
    logo_url: str = "",
    category: str = "custom",
) -> Optional[str]:
    """Sanitize, persist and start one manually defined channel. Shared by the
    dashboard form and config import so both apply the same rules. Returns the
    team id, or None when the input is unusable."""
    team_name = _clean_label(team_name, TEAM_NAME_MAX_LENGTH)
    search_query = _clean_label(search_query, TEAM_QUERY_MAX_LENGTH)
    if not team_name or not search_query:
        return None
    team_id = _safe_team_id(team_id or team_name)
    # Jellyfin fetches channel logos server-side: only public http(s) URLs.
    logo_url = (_validate_upstream_url(logo_url.strip()) or "") if logo_url else ""
    category = _clean_label(category, 40) or "custom"
    search_terms = get_team_search_terms(team_name, search_query, team_id)
    await save_team_async(team_id, team_name, search_query, logo_url, category=category,
                          search_terms=search_terms, content_type="manual")
    stream_state[team_id] = {
        "name": team_name,
        "query": search_query,
        "candidates": [],
        "active_index": 0,
        "is_healthy": False,
        "logo_url": logo_url,
        "start_time": "",
        "stop_time": "",
        "category": category,
        "source_id": "",
        "content_type": "manual",
        "search_terms": search_terms,
        "always_live": False,
        "catalog_key": "",
        **_scrape_lifecycle_defaults(),
    }
    _start_team_scrape_loop(team_id)
    return team_id


@app.post("/add_team")
async def add_team(team_name: str = Form(...), search_query: str = Form(...), auth: bool = Depends(verify_dashboard_auth)):
    team_id = await _add_manual_team(team_name, search_query)
    if team_id is None:
        raise HTTPException(status_code=400, detail="Team name and search query are required")
    return RedirectResponse(url="/?tab=channels&status=team_added", status_code=303)

@app.post("/override/{team_id}")
async def override_stream(team_id: str, candidate_index: int = Form(...), auth: bool = Depends(verify_dashboard_auth)):
    if team_id in stream_state and stream_state[team_id].get("candidates"):
        max_idx = len(stream_state[team_id]["candidates"]) - 1
        if 0 <= candidate_index <= max_idx:
            stream_state[team_id]["active_index"] = candidate_index
            stream_state[team_id]["is_healthy"] = True
            stream_state[team_id]["exhausted"] = False
            stream_state[team_id]["self_retries"] = 0
            candidate = stream_state[team_id]["candidates"][candidate_index]
            candidate["consecutive_failures"] = 0
            candidate.pop("session_compatible", None)
            SESSIONS.poke(team_id)
            
            team_name = stream_state[team_id]["name"]
            prov = stream_state[team_id]["candidates"][candidate_index].get("provider", "Unknown")
            _spawn_background_task(
                send_alert("🛠️ Manual Override Activated", f"Stream for **{team_name}** forced to {prov}.", "info"),
                f"send manual-override alert team={team_id}",
            )
            
    return RedirectResponse(url="/?tab=channels&status=override_saved", status_code=303)

async def _remove_channel(channel_id: str) -> None:
    """Single removal path for every kind of channel: stops its scrape loop or
    Multi-View ffmpeg, closes its channel sessions, and deletes its DB row."""
    data = stream_state.get(channel_id)
    _LAST_PLAYBACK_EVENT.pop(channel_id, None)
    if data is not None and data.get("type") == "multiview":
        view_sessions = _multiview_view_session_ids(channel_id)
        # Out of stream_state first: a spawn already past its warm-up re-checks
        # this after every await and gives up instead of launching ffmpeg for
        # a deleted channel; then cancel it (it stops anything it launched).
        stream_state.pop(channel_id, None)
        await _cancel_multiview_start(channel_id)
        await _stop_multiview_process(channel_id)
        _forget_multiview_channel(channel_id)
        _remove_tree_later(MULTIVIEW_OUTPUT_ROOT / channel_id, f"multiview={channel_id}")
        await delete_multiview_channel_async(channel_id)
        for session_id in view_sessions:
            await SESSIONS.close(session_id)
        return
    if data is not None:
        await _stop_team_scrape_loop(channel_id)
        stream_state.pop(channel_id, None)
    await delete_team_async(channel_id)
    await SESSIONS.close(channel_id)
    dependents = [
        cid for cid, other in stream_state.items()
        if other.get("type") == "multiview" and channel_id in (other.get("member_team_ids") or [])
    ]
    for multiview_id in dependents:
        # The removed member's pane now shows "No Signal"; restart a running
        # (or starting) run so it picks up the placeholder input. A config
        # change, so it doesn't count toward the restart backoff.
        LOGGER.warning("Channel %s removed but used by Multi-View %s; its pane will show No Signal", channel_id, multiview_id)
        if multiview_id in _MULTIVIEW_PROCESSES or _multiview_start_in_progress(multiview_id):
            _spawn_background_task(
                _restart_multiview(multiview_id, f"member {channel_id} removed", count_toward_backoff=False),
                f"restart multiview channel={multiview_id}",
            )


@app.post("/remove_team/{team_id}")
async def remove_team(team_id: str, auth: bool = Depends(verify_dashboard_auth)):
    if team_id in stream_state:
        await _remove_channel(team_id)
        _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after team removal")
    return RedirectResponse(url="/?tab=channels&status=team_removed", status_code=303)


@app.post("/multiview/create")
async def create_multiview(
    name: str = Form(...),
    layout: str = Form("grid_2x2"),
    member_team_ids: str = Form(...),
    auth: bool = Depends(verify_dashboard_auth),
):
    name = name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Name is required")
    if layout not in MULTIVIEW_LAYOUTS:
        raise HTTPException(status_code=400, detail="Invalid layout")
    members = [m.strip() for m in member_team_ids.split(",") if m.strip()]
    error = _multiview_member_validation(members)
    if error:
        raise HTTPException(status_code=400, detail=error)
    if MULTIVIEW_LAYOUTS[layout]["count"] != len(members):
        raise HTTPException(status_code=400, detail=f"Layout {layout} requires exactly {MULTIVIEW_LAYOUTS[layout]['count']} channels")

    channel_id = _safe_team_id(f"multiview_{name}")
    if channel_id in stream_state:
        # INSERT OR REPLACE used to silently overwrite an existing Multi-View
        # (leaving its old ffmpeg running against the old members).
        raise HTTPException(status_code=409, detail="A channel with that name already exists")
    active_audio_team_id = members[0]
    _forget_multiview_channel(channel_id)
    await save_multiview_channel_async(channel_id, name, layout, members, active_audio_team_id)
    stream_state[channel_id] = {
        "name": name, "query": "", "type": "multiview",
        "candidates": [{"synthetic": True}],
        "active_index": 0, "is_healthy": False,
        "logo_url": "", "start_time": "", "stop_time": "",
        "category": "multiview", "source_id": "", "content_type": "multiview",
        "search_terms": [], "always_live": True, "catalog_key": "",
        "tvg_id": "", "group_title": "Multi-View",
        "layout": layout, "member_team_ids": members,
        "active_audio_team_id": active_audio_team_id,
        **_scrape_lifecycle_defaults(),
    }
    _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after multiview creation")
    return RedirectResponse(url="/?tab=channels&status=multiview_created", status_code=303)


@app.post("/multiview/{channel_id}/remove")
async def remove_multiview(channel_id: str, auth: bool = Depends(verify_dashboard_auth)):
    data = stream_state.get(channel_id)
    if data and data.get("type") == "multiview":
        await _remove_channel(channel_id)
        _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after multiview removal")
    return RedirectResponse(url="/?tab=channels&status=multiview_removed", status_code=303)


@app.post("/multiview/{channel_id}/stop")
async def stop_multiview(channel_id: str, auth: bool = Depends(verify_dashboard_auth)):
    data = stream_state.get(channel_id)
    if data and data.get("type") == "multiview":
        # Cancels a spawn in progress too, and keeps the grid stopped (viewers
        # get No Signal) until a new viewer tunes in or /start is posted.
        await _stop_multiview_manually(channel_id)
    return RedirectResponse(url="/?tab=channels&status=multiview_stopped", status_code=303)


@app.post("/multiview/{channel_id}/start")
async def start_multiview(channel_id: str, auth: bool = Depends(verify_dashboard_auth)):
    """Explicit start: clears a manual stop and any backoff, then starts the
    grid now (the idle monitor stops it again if nobody watches)."""
    data = stream_state.get(channel_id)
    if data and data.get("type") == "multiview":
        _clear_multiview_manual_stop(channel_id, "explicit start")
        for mapping in (_MULTIVIEW_FAILURES, _MULTIVIEW_HOLDS, _MULTIVIEW_REFUSALS):
            mapping.pop(channel_id, None)
        _MULTIVIEW_LAST_VIEWER[channel_id] = time.monotonic()
        _request_multiview_start(channel_id)
    return RedirectResponse(url="/?tab=channels&status=multiview_started", status_code=303)


@app.post("/multiview/{channel_id}/set-audio")
async def set_multiview_audio(
    channel_id: str,
    active_audio_team_id: str = Form(...),
    auth: bool = Depends(verify_dashboard_auth),
):
    data = stream_state.get(channel_id)
    if not data or data.get("type") != "multiview":
        raise HTTPException(status_code=404, detail="Multi-View channel not found")
    if active_audio_team_id not in data.get("member_team_ids", []):
        raise HTTPException(status_code=400, detail="Not a member of this Multi-View channel")
    data["active_audio_team_id"] = active_audio_team_id
    await save_multiview_channel_async(
        channel_id, data["name"], data["layout"], data["member_team_ids"], active_audio_team_id,
        data.get("tvg_id", ""), data.get("group_title", ""), data.get("logo_url", ""),
    )
    # Every member's audio is already in its own output (stream-copied), so
    # switching the main channel's audio is just pointing its session at
    # another output: no ffmpeg restart. The view's source key carries the
    # output's audio codec (see _resolve_multiview_view_source): same codec
    # continues seamlessly, a different codec becomes a clean discontinuity.
    SESSIONS.poke(channel_id)
    return RedirectResponse(url="/?tab=channels&status=multiview_audio_set", status_code=303)


@app.get("/api/ffmpeg-status", response_class=JSONResponse)
async def ffmpeg_status(auth: bool = Depends(verify_dashboard_auth)):
    channels = []
    for channel_id, data in stream_state.items():
        if data.get("type") != "multiview":
            continue
        entry = _MULTIVIEW_PROCESSES.get(channel_id)
        running = bool(entry and entry["process"].returncode is None and not entry.get("exited"))
        failure = _MULTIVIEW_FAILURES.get(channel_id)
        channels.append({
            "channel_id": channel_id,
            "name": data.get("name", channel_id),
            "running": running,
            "exit_code": entry.get("exit_code") if entry else None,
            "uptime_seconds": (time.monotonic() - entry["started_at"]) if entry and running else 0,
            "recent_log_lines": list(entry.get("log_lines", []))[-20:] if entry else [],
            "last_error": failure["last_error"] if failure else None,
            "failure_count": failure["count"] if failure else 0,
            "retry_in_seconds": round(_multiview_cooldown_remaining(channel_id)) if failure else 0,
        })
    return {"available": FFMPEG_AVAILABLE, "version": FFMPEG_VERSION_INFO, "channels": channels}


@app.post("/favorite/{team_id}")
async def toggle_favorite(team_id: str, auth: bool = Depends(verify_dashboard_auth)):
    try:
        await asyncio.to_thread(_toggle_favorite_sync, team_id)
    except Exception as exc:
        _log_failure(f"toggle favorite for {team_id}", exc)
    return RedirectResponse(url="/?tab=channels&status=favorite_toggled", status_code=303)

@app.post("/rescrape/{team_id}")
async def manual_rescrape(team_id: str, auth: bool = Depends(verify_dashboard_auth)):
    _spawn_background_task(trigger_scrape(team_id, force=True), f"manual rescrape {team_id}")
    return RedirectResponse(url="/?tab=channels&status=rescrape_started", status_code=303)


@app.post("/bulk-favorite")
async def bulk_favorite(team_ids: str = Form(""), auth: bool = Depends(verify_dashboard_auth)):
    ids = [t.strip() for t in team_ids.split(",") if t.strip()]
    try:
        await asyncio.to_thread(_bulk_set_favorite_sync, ids, True)
    except Exception as exc:
        _log_failure("bulk favorite", exc)
    return RedirectResponse(url="/?tab=channels&status=bulk_favorited", status_code=303)

@app.post("/bulk-unfavorite")
async def bulk_unfavorite(team_ids: str = Form(""), auth: bool = Depends(verify_dashboard_auth)):
    ids = [t.strip() for t in team_ids.split(",") if t.strip()]
    try:
        await asyncio.to_thread(_bulk_set_favorite_sync, ids, False)
    except Exception as exc:
        _log_failure("bulk unfavorite", exc)
    return RedirectResponse(url="/?tab=channels&status=bulk_unfavorited", status_code=303)

@app.post("/bulk-remove")
async def bulk_remove(team_ids: str = Form(""), auth: bool = Depends(verify_dashboard_auth)):
    ids = [t.strip() for t in team_ids.split(",") if t.strip()]
    try:
        for team_id in ids:
            await _remove_channel(team_id)
    except Exception as exc:
        _log_failure("bulk remove", exc)
    _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after bulk removal")
    return RedirectResponse(url="/?tab=channels&status=bulk_removed", status_code=303)


@app.get("/api/export-config", response_class=JSONResponse)
async def export_config(auth: bool = Depends(verify_dashboard_auth)):
    try:
        teams = await asyncio.to_thread(_export_teams_sync)
        config = {
            "version": "2.0",
            "app_version": __version__,
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "teams": teams
        }
        return JSONResponse(config)
    except Exception as exc:
        _log_failure("export config", exc)
        raise HTTPException(status_code=500, detail="Export failed")


IMPORT_MAX_TEAMS = 500
IMPORT_MAX_BYTES = 2 * 1024 * 1024


@app.post("/api/import-config")
async def import_config(file: Request, auth: bool = Depends(verify_dashboard_auth)):
    """Import channels from an export. Goes through the same sanitizing path as
    the dashboard form and starts each new channel right away (imports used to
    sit inert in the DB until a restart). Existing channels are left alone."""
    try:
        raw = await file.body()
        if len(raw) > IMPORT_MAX_BYTES:
            raise HTTPException(status_code=413, detail="Import file too large")
        body = json.loads(raw.decode("utf-8"))
        teams = body.get("teams", []) if isinstance(body, dict) else None
        if not isinstance(teams, list):
            raise ValueError("teams must be a list")
    except HTTPException:
        raise
    except Exception as exc:
        _log_failure("import config", exc)
        raise HTTPException(status_code=400, detail="Import failed: not a Jellyball export")

    catalog_by_key = {entry["catalog_key"]: entry for entry in await get_catalog_entries()}
    imported, skipped, favorites = 0, 0, []
    for team in teams[:IMPORT_MAX_TEAMS]:
        if not isinstance(team, dict):
            skipped += 1
            continue
        requested_id = _safe_team_id(str(team.get("team_id") or team.get("name") or ""))
        if requested_id in stream_state:
            skipped += 1
            continue
        catalog_entry = catalog_by_key.get(str(team.get("catalog_key") or ""))
        if catalog_entry is not None:
            # A catalog channel: enable the real catalog entry (keeps its
            # search terms, schedule and guide metadata) instead of a copy.
            if catalog_entry["team_id"] in stream_state:
                skipped += 1
                continue
            await _set_catalog_entry_enabled(catalog_entry, True)
            imported += 1
            if team.get("is_favorite"):
                favorites.append(catalog_entry["team_id"])
            continue
        team_id = await _add_manual_team(
            str(team.get("name") or ""),
            str(team.get("query") or ""),
            team_id=requested_id,
            logo_url=str(team.get("logo_url") or ""),
            category=str(team.get("category") or "imported"),
        )
        if team_id is None:
            skipped += 1
            continue
        imported += 1
        if team.get("is_favorite"):
            favorites.append(team_id)
    skipped += max(0, len(teams) - IMPORT_MAX_TEAMS)
    if favorites:
        await asyncio.to_thread(_bulk_set_favorite_sync, favorites, True)
    if imported:
        request_jellyfin_guide_refresh_if_changed()
    return JSONResponse({"status": "imported", "count": imported, "skipped": skipped})

@dataclass(frozen=True)
class Tunable:
    """A setting the dashboard can change at runtime. The env var (or built-in
    default) is the default; a value saved in app_settings overrides it and is
    applied live - no restart."""

    name: str  # env var name; also the form field
    label: str
    group: str
    kind: type  # int or float
    minimum: float
    maximum: float
    target: str  # module global, or "session.<SessionConfig field>"
    help: str = ""
    db_key: str = ""  # keys saved by 1.x's Playback Settings sliders

    @property
    def key(self) -> str:
        return self.db_key or f"tunable:{self.name}"


TUNABLES: Tuple[Tunable, ...] = (
    Tunable("IDLE_HEALTH_INTERVAL", "Unwatched channel probe interval (s)", "Health & failover", float, 5, 600,
            "IDLE_HEALTH_INTERVAL", "How often the active source of a channel nobody is watching is checked."),
    Tunable("HEALTH_FAILURE_THRESHOLD", "Failed probes before failover", "Health & failover", int, 1, 10,
            "HEALTH_FAILURE_THRESHOLD"),
    Tunable("HEALTH_PROBE_TIMEOUT", "Probe timeout (s)", "Health & failover", float, 3, 60, "HEALTH_PROBE_TIMEOUT"),
    Tunable("STANDBY_HEALTH_INTERVAL", "Standby check interval, watched channels (s)", "Health & failover", float,
            10, 3600, "STANDBY_HEALTH_INTERVAL"),
    Tunable("UNWATCHED_STANDBY_INTERVAL", "Standby check interval, unwatched channels (s)", "Health & failover",
            float, 30, 7200, "UNWATCHED_STANDBY_INTERVAL"),
    Tunable("EXHAUSTED_PROBE_INTERVAL", "Recovery probe interval when all sources failed (s)", "Health & failover",
            float, 5, 600, "EXHAUSTED_PROBE_INTERVAL"),
    Tunable("WINDOW_CLOSE_GRACE_SECONDS", "Keep a finished game on air without playback for (s)", "Health & failover",
            float, 0, 3600, "WINDOW_CLOSE_GRACE_SECONDS"),
    Tunable("SELF_RETRY_LIMIT", "Retries of a channel's only source before No Signal", "Health & failover", int,
            0, 10, "SELF_RETRY_LIMIT"),
    Tunable("EMERGENCY_SCRAPE_COOLDOWN", "Min seconds between emergency rescans", "Health & failover", float,
            10, 3600, "EMERGENCY_SCRAPE_COOLDOWN"),
    Tunable("FAILOVER_ALERT_COOLDOWN", "Min seconds between failover alerts per channel", "Health & failover",
            float, 0, 86400, "FAILOVER_ALERT_COOLDOWN"),
    Tunable("HEALTHY_RESCRAPE_SECONDS", "Refresh standbys of healthy channels every (s)", "Scraping", float,
            300, 86400, "HEALTHY_RESCRAPE_SECONDS"),
    Tunable("MAX_STREAM_CANDIDATES", "Max stream candidates per channel", "Scraping", int, 1, 200,
            "MAX_STREAM_CANDIDATES"),
    Tunable("PROVIDER_BREAKER_FAILURES", "Provider failures before its breaker opens", "Scraping", int, 1, 20,
            "PROVIDER_BREAKER_FAILURES"),
    Tunable("PROVIDER_BREAKER_COOLDOWN", "Provider breaker cooldown (s)", "Scraping", float, 5, 3600,
            "PROVIDER_BREAKER_COOLDOWN"),
    Tunable("PROVIDER_TIMEOUT_MIN", "Provider search timeout, minimum (s)", "Scraping", float, 5, 300,
            "PROVIDER_TIMEOUT_MIN"),
    Tunable("PROVIDER_TIMEOUT_MAX", "Provider search timeout, maximum (s)", "Scraping", float, 10, 600,
            "PROVIDER_TIMEOUT_MAX"),
    Tunable("SESSION_IDLE_SECONDS", "Stop a channel session after idle (s)", "Channel sessions", float, 10, 3600,
            "session.idle_timeout"),
    Tunable("SESSION_LIVE_EDGE_SEGMENTS", "Segments of buffer at tune-in", "Channel sessions", int, 1, 10,
            "session.live_edge_segments"),
    Tunable("SESSION_WINDOW_SECONDS", "Playlist window (s)", "Channel sessions", float, 12, 600,
            "session.window_min_seconds"),
    Tunable("SESSION_STALE_SECONDS", "Fail over when no new segment for at least (s)", "Channel sessions", float,
            5, 300, "session.stale_min_seconds"),
    Tunable("SESSION_FAIL_THRESHOLD", "Consecutive fetch failures before failover", "Channel sessions", int, 1, 20,
            "session.fail_threshold"),
    Tunable("SESSION_SEGMENT_TIMEOUT", "Segment download timeout (s)", "Channel sessions", float, 3, 120,
            "session.segment_timeout"),
    Tunable("STREAM_STARTUP_TIMEOUT", "Wait for a channel to start before No Signal (s)", "Channel sessions", float,
            3, 120, "STREAM_STARTUP_TIMEOUT"),
    Tunable("STARTUP_PLACEHOLDER_SECONDS", "No Signal shown for a slow start (s)", "Channel sessions", float,
            5, 600, "STARTUP_PLACEHOLDER_SECONDS"),
    Tunable("MULTIVIEW_IDLE_TIMEOUT_SECONDS", "Stop an unwatched Multi-View after (s)", "Multi-View", float, 30, 3600,
            "MULTIVIEW_IDLE_TIMEOUT_SECONDS"),
    Tunable("STREAM_STARTUP_BUFFER_SECONDS", "Warm-up cache time (s)", "Legacy proxy (fMP4 / separate-audio sources)",
            float, 0, 120, "STREAM_STARTUP_BUFFER_SECONDS", db_key="startup_buffer_seconds"),
    Tunable("PREFETCH_CHUNK_COUNT", "Read-ahead chunks", "Legacy proxy (fMP4 / separate-audio sources)", int, 0, 32,
            "PREFETCH_CHUNK_COUNT", db_key="prefetch_chunk_count"),
    Tunable("STREAM_CHUNK_CACHE_TTL", "Chunk cache TTL (s)", "Legacy proxy (fMP4 / separate-audio sources)", float,
            1, 600, "STREAM_CHUNK_CACHE_TTL", db_key="stream_chunk_cache_ttl"),
)
_TUNABLES_BY_NAME = {t.name: t for t in TUNABLES}
# A non-session Tunable.target is a global of the module whose code reads it.
# It must be rebound on that module - a `from x import NAME` copy elsewhere
# would not see the change - so these are the modules that own the targets.
_TUNABLE_TARGET_MODULES = (scrapers, legacy_proxy, sys.modules[__name__])


def _tunable_module(tunable: Tunable):
    """The module that owns `tunable.target` (a plain module global)."""
    for module in _TUNABLE_TARGET_MODULES:
        if hasattr(module, tunable.target):
            return module
    raise LookupError(f"no module owns tunable target {tunable.target}")


def _tunable_value(tunable: Tunable):
    if tunable.target.startswith("session."):
        return getattr(SESSIONS.config, tunable.target.split(".", 1)[1])
    return getattr(_tunable_module(tunable), tunable.target)


_TUNABLE_DEFAULTS = {t.name: _tunable_value(t) for t in TUNABLES}


def _coerce_tunable(tunable: Tunable, raw: object):
    """Parse and clamp; None when the value is unusable."""
    try:
        value = float(str(raw).strip())
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value):
        return None
    value = min(max(value, tunable.minimum), tunable.maximum)
    return int(round(value)) if tunable.kind is int else float(value)


def _apply_tunable(tunable: Tunable, value) -> None:
    if tunable.target.startswith("session."):
        # Every channel session shares this config object, so running
        # sessions pick the change up on their next poll.
        setattr(SESSIONS.config, tunable.target.split(".", 1)[1], value)
    else:
        setattr(_tunable_module(tunable), tunable.target, value)


def _load_tunable_overrides() -> None:
    for tunable in TUNABLES:
        raw = get_setting(tunable.key, "")
        if raw == "":
            continue
        value = _coerce_tunable(tunable, raw)
        if value is not None:
            _apply_tunable(tunable, value)


def _advanced_settings_snapshot() -> List[dict]:
    return [
        {
            "name": t.name, "label": t.label, "group": t.group, "help": t.help,
            "value": _tunable_value(t), "default": _TUNABLE_DEFAULTS[t.name],
            "min": t.minimum, "max": t.maximum, "type": t.kind.__name__,
        }
        for t in TUNABLES
    ]


def _advanced_settings_html() -> List[dict]:
    """Advanced-settings tunables grouped for the dashboard template, one
    dict per fieldset: {"name": group name, "items": [...]}."""
    groups: Dict[str, List[dict]] = {}
    for item in _advanced_settings_snapshot():
        overridden = item["value"] != item["default"]
        step = "1" if item["type"] == "int" else "any"
        hint = f"Default {item['default']:g}" if isinstance(item["default"], (int, float)) else ""
        if item["help"]:
            hint = f"{hint}. {item['help']}" if hint else item["help"]
        groups.setdefault(item["group"], []).append({
            "name": item["name"],
            "label": item["label"],
            "overridden": overridden,
            "step": step,
            "min": f'{item["min"]:g}',
            "max": f'{item["max"]:g}',
            "value": item["value"] if overridden else "",
            "default": item["default"],
            "hint": hint,
        })
    return [{"name": group, "fields": fields} for group, fields in groups.items()]


@app.get("/api/settings/advanced", response_class=JSONResponse)
async def get_advanced_settings(auth: bool = Depends(verify_dashboard_auth)):
    return {"settings": _advanced_settings_snapshot()}


@app.post("/settings/advanced")
async def update_advanced_settings(request: Request, auth: bool = Depends(verify_dashboard_auth)):
    """Blank field = back to the default (env var or built-in). Applied live."""
    form = await request.form()
    for tunable in TUNABLES:
        if tunable.name not in form:
            continue
        raw = str(form.get(tunable.name) or "").strip()
        if raw == "":
            await set_setting_async(tunable.key, "")
            _apply_tunable(tunable, _TUNABLE_DEFAULTS[tunable.name])
            continue
        value = _coerce_tunable(tunable, raw)
        if value is None:
            return RedirectResponse(url="/?tab=playback&status=advanced_invalid", status_code=303)
        await set_setting_async(tunable.key, str(value))
        _apply_tunable(tunable, value)
    return RedirectResponse(url="/?tab=playback&status=advanced_saved", status_code=303)

@app.get("/api/cache-metrics", response_class=JSONResponse)
async def get_cache_metrics(auth: bool = Depends(verify_dashboard_auth)):
    try:
        return await CHUNK_CACHE.stats()
    except Exception as exc:
        _log_failure("get cache metrics", exc)
        return {"hit_rate": 0, "total_hits": 0, "total_misses": 0}


@app.get("/api/performance-stats", response_class=JSONResponse)
async def get_performance_stats(auth: bool = Depends(verify_dashboard_auth)):
    try:
        return await asyncio.to_thread(_performance_stats_sync)
    except Exception as exc:
        _log_failure("get performance stats", exc)
        return {"playback_sessions_hour": 0, "failovers_hour": 0, "db_size_mb": 0, "provider_health": []}


@app.get("/api/playback-stats", response_class=JSONResponse)
async def get_playback_stats(auth: bool = Depends(verify_dashboard_auth)):
    try:
        return await asyncio.to_thread(_playback_stats_sync)
    except Exception as exc:
        _log_failure("get playback stats", exc)
        return {"top_watched_teams": [], "total_playbacks_week": 0}


@app.post("/api/test-stream/{team_id}")
async def test_stream(team_id: str, auth: bool = Depends(verify_dashboard_auth)):
    try:
        data = stream_state.get(team_id)
        candidates = data.get("candidates", []) if data else []
        is_live = len(candidates) > 0 and data.get("is_healthy", False)
        await asyncio.to_thread(_record_stream_test_sync, team_id, is_live, len(candidates))
        return {"team_id": team_id, "is_live": is_live, "candidate_count": len(candidates), "status": "✅ Live" if is_live else "❌ Offline"}
    except Exception as exc:
        _log_failure(f"test stream {team_id}", exc)
        return {"status": "error"}

@app.post("/settings/provider-rotation")
async def set_provider_rotation(enabled: bool = Form(False), auth: bool = Depends(verify_dashboard_auth)):
    await set_setting_async("provider_rotation_mode", "1" if enabled else "0")
    return RedirectResponse(url="/?tab=playback&status=saved", status_code=303)


@app.get("/api/version")
async def api_version(auth: bool = Depends(verify_dashboard_auth)):
    return {
        "version": __version__,
        "latest": _UPDATE_STATE.get("latest") or None,
        "update_available": _update_available(),
        "release_url": _UPDATE_STATE.get("url") or None,
    }


@app.post("/settings/update-check")
async def set_update_check(enabled: bool = Form(False), auth: bool = Depends(verify_dashboard_auth)):
    await set_setting_async("update_check_enabled", "1" if enabled else "0")
    if enabled:
        _spawn_background_task(check_for_update(), "check for updates")
    return RedirectResponse(url="/?tab=playback&status=saved", status_code=303)


@app.post("/settings/offseason")
async def set_offseason_listing(enabled: bool = Form(False), auth: bool = Depends(verify_dashboard_auth)):
    await set_setting_async("show_offseason_channels", "1" if enabled else "0")
    if catalog.SHOW_OFFSEASON_CHANNELS != bool(enabled):
        catalog.SHOW_OFFSEASON_CHANNELS = bool(enabled)
        request_jellyfin_guide_refresh_if_changed()
    return RedirectResponse(url="/?tab=playback&status=saved", status_code=303)


def _parse_disable_date(value: str) -> Optional[str]:
    """'' clears the schedule; anything else must be a real YYYY-MM-DD date
    (it is compared as text against date('now'), so other formats never fire)."""
    value = (value or "").strip()
    if not value:
        return ""
    try:
        return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


@app.post("/team/{team_id}/schedule-disable")
async def schedule_team_disable(team_id: str, disable_date: str = Form(""), auth: bool = Depends(verify_dashboard_auth)):
    parsed = _parse_disable_date(disable_date)
    if parsed is None or team_id not in stream_state:
        return RedirectResponse(url="/?tab=channels&status=schedule_invalid", status_code=303)
    try:
        await asyncio.to_thread(_schedule_team_disable_sync, team_id, parsed)
        stream_state[team_id]["auto_disable_after"] = parsed
    except Exception as exc:
        _log_failure(f"schedule disable {team_id}", exc)
        return RedirectResponse(url="/?tab=channels&status=schedule_failed", status_code=303)
    return RedirectResponse(url="/?tab=channels&status=schedule_saved", status_code=303)


@app.post("/settings/providers")
async def update_provider_domains(request: Request, auth: bool = Depends(verify_dashboard_auth)):
    """Save per-provider aggregator base-URL overrides (D1). A blank field
    resets that provider to its env/default URL. Every non-blank value must
    validate as an absolute public http(s) URL; if any doesn't, nothing is
    saved and the dashboard shows a validation error, matching how
    /settings/jellyfin reports an invalid URL."""
    form = await request.form()
    providers = [p for p in ACTIVE_PROVIDERS if isinstance(p, HtmlAggregatorScraper)]

    updates: List[Tuple["HtmlAggregatorScraper", str]] = []
    for provider in providers:
        raw_value = str(form.get(f"url_{provider.name}", "") or "").strip()
        if not raw_value:
            updates.append((provider, ""))
            continue
        validated = _validate_upstream_url(raw_value)
        if not validated:
            return RedirectResponse(url="/?tab=alerts&status=providers_invalid", status_code=303)
        updates.append((provider, validated.rstrip("/")))

    for provider, value in updates:
        await set_setting_async(_provider_url_setting_key(provider.name), value)
        _set_provider_url_override(provider, value)

    return RedirectResponse(url="/?tab=alerts&status=providers_saved", status_code=303)


def _create_tray_image():
    from PIL import Image, ImageDraw

    logo_path = _resource_path("assets/jellyball-icon.png")
    try:
        return Image.open(logo_path).convert("RGBA").resize((64, 64), Image.Resampling.LANCZOS)
    except (OSError, ValueError) as exc:
        LOGGER.warning("Could not load JellyBall tray icon error=%s", type(exc).__name__)
    image = Image.new("RGBA", (64, 64), (15, 23, 42, 255))
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((8, 8, 56, 56), radius=12, fill=(37, 99, 235, 255))
    draw.ellipse((20, 20, 44, 44), fill=(255, 255, 255, 255))
    return image


PORT_BIND_WAIT_SECONDS = bounded_float(os.getenv("PORT_BIND_WAIT_SECONDS", "30"), 30.0, 0.0, 600.0)


def _port_is_free(host: str, port: int) -> bool:
    bind_host = "0.0.0.0" if host in ("", "0.0.0.0") else host
    family = socket.AF_INET6 if ":" in bind_host else socket.AF_INET
    try:
        with socket.socket(family, socket.SOCK_STREAM) as probe:
            probe.bind((bind_host, port))
        return True
    except OSError:
        return False


def _wait_for_port(host: str, port: int, timeout: float) -> bool:
    """A restart can race the previous instance releasing the port; wait for it
    instead of silently moving to another port (which broke Jellyfin's saved
    tuner and guide URLs)."""
    deadline = time.monotonic() + timeout
    while True:
        if _port_is_free(host, port):
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.5)


def _existing_jellyball_instance(port: int) -> bool:
    """True if a Jellyball (e.g. the installed Windows service) already answers
    on this port."""
    try:
        response = httpx.get(f"http://127.0.0.1:{port}/healthz", timeout=2.0)
        return response.status_code == 200 and response.json().get("app") == "jellyball"
    except Exception:
        return False


def build_server(host: str, port: int):
    import uvicorn

    _configure_dashboard_auth(host)

    try:
        import httptools  # noqa: F401
        http_impl = "httptools"
    except ImportError:
        http_impl = "auto"
    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        reload=False,
        log_config=None,
        access_log=False,
        http=http_impl,
        lifespan="on",
        # Jellyfin's ffmpeg re-polls playlists every 2-6s; keep its connection
        # open between polls instead of reconnecting each time.
        timeout_keep_alive=30,
        # Streaming responses would otherwise hold shutdown open indefinitely.
        timeout_graceful_shutdown=10,
    )
    return uvicorn.Server(config)


def _headless_host() -> str:
    return os.getenv("JELLYBALL_HOST", "0.0.0.0" if sys.platform != "win32" else "127.0.0.1").strip() or "127.0.0.1"


def run_headless(stop_event: Optional[threading.Event] = None) -> int:
    """Run the server in the foreground (console mode, Docker, Windows service).
    Returns a process exit code; non-zero lets a service manager restart us."""
    if os.getenv("WEB_CONCURRENCY", "1") not in {"", "1"}:
        LOGGER.error("Jellyball must run with a single worker (WEB_CONCURRENCY=1)")
        return 2
    host = _headless_host()
    if not _wait_for_port(host, config.PORT, PORT_BIND_WAIT_SECONDS):
        LOGGER.error("Port %s on %s is in use; refusing to start on a different port", config.PORT, host)
        return 3
    server = build_server(host, config.PORT)
    if stop_event is not None:
        def _watch_stop() -> None:
            stop_event.wait()
            server.should_exit = True

        threading.Thread(target=_watch_stop, name="jellyball-stop-watch", daemon=True).start()
    LOGGER.info("Jellyball %s running headless at http://%s:%s", __version__, host, config.PORT)
    try:
        server.run()
    finally:
        if _LOG_LISTENER is not None:
            _LOG_LISTENER.stop()
    return 0 if server.started else 1


class TrayApplication:
    def __init__(self):
        import pystray

        self.server = None
        self.server_thread = None
        self.host = "127.0.0.1"
        self._pending_notices: List[str] = []
        self.icon = pystray.Icon(
            "jellyball",
            _create_tray_image(),
            "Jellyball Sports Proxy",
            pystray.Menu(
                pystray.MenuItem("View", self.open_gui, default=True),
                pystray.MenuItem("Restart", self.restart),
                pystray.MenuItem("Quit", self.quit),
            ),
        )

    def open_gui(self, icon, item):
        webbrowser.open(f"http://127.0.0.1:{config.PORT}/")

    def quit(self, icon, item):
        if self.server:
            self.server.should_exit = True
        icon.stop()

    def restart(self, icon, item):
        if self.server:
            self.server.should_exit = True

        def relaunch():
            if self.server_thread:
                self.server_thread.join(timeout=15)
            if getattr(sys, "frozen", False):
                command = [sys.executable, *sys.argv[1:]]
            else:
                command = [sys.executable, str(Path(sys.argv[0]).resolve()), *sys.argv[1:]]
            env = dict(os.environ)
            # PyInstaller 6 treats a child launched with the parent's environment
            # as a worker sharing the parent's (about to be deleted) extraction
            # directory; reset it. Also drop the browser path we derived from it.
            env["PYINSTALLER_RESET_ENVIRONMENT"] = "1"
            if _PLAYWRIGHT_PATH_SET_BY_APP:
                env.pop("PLAYWRIGHT_BROWSERS_PATH", None)
            child = subprocess.Popen(command, close_fds=True, env=env, creationflags=_child_process_creationflags())
            # Only hand over once the new instance answers; if it dies (e.g. a
            # broken .env edit), keep this tray alive and bring the server back.
            deadline = time.monotonic() + 45.0
            while time.monotonic() < deadline:
                if _existing_jellyball_instance(config.PORT):
                    self.icon.stop()
                    return
                if child.poll() is not None:
                    break
                time.sleep(1.0)
            LOGGER.error("Restarted Jellyball did not come up (exit=%s); keeping this instance", child.poll())
            if child.poll() is None:
                child.terminate()
            self._notify("Restart failed - Jellyball kept running the previous instance. See jellyball.log.")
            self._start_server()

        threading.Thread(target=relaunch, name="jellyball-restart", daemon=True).start()

    def _notify(self, message: str) -> None:
        try:
            self.icon.notify(message, "Jellyball")
        except Exception as exc:  # not every tray backend supports notifications
            _log_failure("tray notification", exc, logging.DEBUG)

    def _on_icon_ready(self, icon) -> None:
        icon.visible = True
        for message in self._pending_notices:
            self._notify(message)
        self._pending_notices.clear()

    def _start_server(self) -> None:
        self.server = build_server(self.host, config.PORT)
        self.server_thread = threading.Thread(target=self.server.run, name="jellyball-server", daemon=True)
        self.server_thread.start()

    def run(self):
        host = os.getenv("JELLYBALL_HOST", "127.0.0.1").strip() or "127.0.0.1"
        self.host = host
        if not _port_is_free(host, config.PORT):
            if _existing_jellyball_instance(config.PORT):
                # The Windows service (or another tray instance) already runs
                # Jellyball here: just open its dashboard.
                LOGGER.info("Jellyball already running on port %s; opening its dashboard", config.PORT)
                webbrowser.open(f"http://127.0.0.1:{config.PORT}/")
                return
            if not _wait_for_port(host, config.PORT, 10.0):
                selected_port = _find_available_port(config.PORT)
                LOGGER.warning(
                    "Configured port %s is in use by another program; using %s for this desktop session "
                    "(Jellyfin tuner URLs pointing at %s will not work until it is free)",
                    config.PORT, selected_port, config.PORT,
                )
                self._pending_notices.append(
                    f"Port {config.PORT} is in use, so Jellyball is on port {selected_port} for now. "
                    f"Jellyfin URLs using port {config.PORT} won't work until it's free."
                )
                config.PORT = selected_port

        self._start_server()
        if security.DASHBOARD_AUTH_MODE == "generated":
            self._pending_notices.append(
                f"Dashboard password generated (user {security.DASHBOARD_USERNAME}); it is in {security.DASHBOARD_PASSWORD_FILE}."
            )
        LOGGER.info("Jellyball %s web GUI available at http://127.0.0.1:%s", __version__, config.PORT)
        threading.Timer(1.0, lambda: webbrowser.open(f"http://127.0.0.1:{config.PORT}/")).start()
        try:
            self.icon.run(setup=self._on_icon_ready)
        finally:
            self.server.should_exit = True
            self.server_thread.join(timeout=15)
            if _LOG_LISTENER is not None:
                _LOG_LISTENER.stop()


def _wants_headless() -> bool:
    return sys.platform != "win32" or os.getenv("JELLYBALL_HEADLESS", "").strip().lower() in {"1", "true", "yes"}


if __name__ == "__main__":
    if _wants_headless() or "--console" in sys.argv[1:]:
        sys.exit(run_headless())
    TrayApplication().run()
