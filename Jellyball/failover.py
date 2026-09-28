"""Scrape lifecycle and failover: per-channel scrape loops, candidate
merging, health probes, the failover monitor and emergency rescrapes.
"""

import asyncio
import logging
import os
import random
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import httpx

from config import _log_failure, _positive_env_number, LOGGER
import state
from state import _spawn_background_task, stream_state
from db import log_metric_event_async, update_team_meta_async
import scrapers
from scrapers import master_scrape
from catalog import (
    fetch_espn_team_schedule,
    is_in_season,
    is_stream_window_active,
    parse_team_schedule,
    resolve_espn_logo,
    SCRAPE_REFRESH_SECONDS,
    STREAM_LEAD_TIME,
    STREAM_TRAIL_TIME,
    xmltv_ts,
)
from alerts import request_jellyfin_guide_refresh_if_changed, send_alert
from sessions import _is_placeholder_key, candidate_source_key, SESSIONS
from multiview import _MULTIVIEW_PROCESSES
from network_safety import bounded_float, bounded_int
from stream_extractor import verify_stream_live


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
