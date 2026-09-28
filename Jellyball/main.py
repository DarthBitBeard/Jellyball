# First: config resolves DATA_DIR, loads .env and starts logging before anything else.
import config  # noqa: F401

import asyncio
import json
import logging
import math
import os
import random
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
import webbrowser
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import httpx
from fastapi import Depends, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
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
    _public_base_url,
    _resource_path,
    _safe_team_id,
    _validate_upstream_url,
    _xml_attr,
    _xml_text,
    LOG_FILE,
    LOGGER,
)
import state
from state import _cancel_background_tasks, _spawn_background_task, stream_state
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
    prune_database_logs,
    save_multiview_channel_async,
    save_team_async,
    set_setting_async,
)
import security
from security import _configure_dashboard_auth, CsrfOriginMiddleware, verify_dashboard_auth
import scrapers
from scrapers import (
    _provider_url_setting_key,
    _set_provider_url_override,
    ACTIVE_PROVIDERS,
    HtmlAggregatorScraper,
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
    get_catalog_entries,
    is_stream_window_active,
    parse_team_schedule,
    resolve_espn_logo,
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
from legacy_proxy import _legacy_proxy_stream, _STARTUP_BUFFER_TASKS, CHUNK_CACHE, PREFETCH_CONCURRENCY
import ffmpeg_proc
from ffmpeg_proc import _check_ffmpeg_available, _child_process_creationflags, _remove_tree_later, FFMPEG_PATH
import placeholder
from placeholder import _stop_placeholder_process
import sessions
from sessions import _LAST_PLAYBACK_EVENT, _serve_channel_playlist, _serve_session_segment, SESSIONS
import multiview
from multiview import (
    _cancel_multiview_start,
    _clear_multiview_manual_stop,
    _forget_multiview_channel,
    _kill_orphaned_ffmpeg,
    _multiview_audio_view,
    _multiview_cooldown_remaining,
    _MULTIVIEW_FAILURES,
    _MULTIVIEW_HOLDS,
    _MULTIVIEW_LAST_VIEWER,
    _multiview_member_label,
    _multiview_member_validation,
    _MULTIVIEW_PROCESSES,
    _MULTIVIEW_REFUSALS,
    _multiview_start_in_progress,
    _multiview_view_session_ids,
    _request_multiview_start,
    _restart_multiview,
    _serve_multiview_audio_playlist,
    _stop_multiview_manually,
    _stop_multiview_process,
    _touch_multiview_viewer,
    MULTIVIEW_AUDIO_CHANNELS,
    multiview_idle_monitor,
    MULTIVIEW_LAYOUTS,
    multiview_output_sweeper,
    multiview_watchdog,
)
import failover
from failover import (
    _SCRAPE_IN_FLIGHT,
    _scrape_lifecycle_defaults,
    _session_on_placeholder,
    _start_team_scrape_loop,
    _stop_team_scrape_loop,
    _TEAM_SCRAPE_TASKS,
    _TEAM_SCRAPE_WAKE_EVENTS,
    _TEAM_STATE_LOCKS,
    failover_monitor,
    STARTUP_SCRAPE_SPREAD_SECONDS,
    trigger_scrape,
)
from network_safety import bounded_float
from sports_matcher import get_team_search_terms
from stream_extractor import DEFAULT_USER_AGENT
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
    shutil.rmtree(multiview.MULTIVIEW_OUTPUT_ROOT, ignore_errors=True)
    multiview.MULTIVIEW_OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    shutil.rmtree(placeholder.PLACEHOLDER_OUTPUT_DIR, ignore_errors=True)
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
    shutil.rmtree(multiview.MULTIVIEW_OUTPUT_ROOT, ignore_errors=True)
    shutil.rmtree(placeholder.PLACEHOLDER_OUTPUT_DIR, ignore_errors=True)
        
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
        "ffmpeg_available": ffmpeg_proc.FFMPEG_AVAILABLE,
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
        _remove_tree_later(multiview.MULTIVIEW_OUTPUT_ROOT / channel_id, f"multiview={channel_id}")
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
    return {"available": ffmpeg_proc.FFMPEG_AVAILABLE, "version": ffmpeg_proc.FFMPEG_VERSION_INFO, "channels": channels}


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
_TUNABLE_TARGET_MODULES = (scrapers, legacy_proxy, sessions, multiview, failover)


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
