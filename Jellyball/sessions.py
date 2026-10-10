"""Channel sessions (see hls_session.py): the SessionRegistry, its hooks,
the placeholder source, and the playlist/segment responses.

The hooks need Multi-View and failover, which themselves import this module;
those few calls use function-level (late) imports - see _resolve_session_source.
"""

import asyncio
import os
import re
import time
import urllib.parse
from typing import Dict, Optional, Tuple

import httpx
from fastapi import Request
from fastapi.responses import Response

from config import _upstream_media_headers, _validate_upstream_url, LOGGER
from state import _media_client, _spawn_background_task, PLACEHOLDER_SESSION_ID, stream_state
from db import _METRIC_WRITER
import engine_settings
import engine_stats
from upstream import _fetch_upstream_body, _hls_response, HLS_MEDIA_TYPE
from legacy_proxy import _sample_aes_proxy_stream
import ffmpeg_proc
import placeholder
from placeholder import _ensure_placeholder_running, _placeholder_cooldown_remaining
from hls_session import FetchResult, SessionConfig, SessionHooks, SessionRegistry, SourceSpec
from network_safety import bounded_float, bounded_int


# Jellyfin re-polls a channel's manifest every few seconds during live playback, so
# recording a playback_events row on every proxy_stream() call would count one viewing
# session as dozens of rows. De-dupe to at most one row per team per this window.
PLAYBACK_EVENT_DEDUPE_SECONDS = 300.0
_LAST_PLAYBACK_EVENT: Dict[str, float] = {}


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


# --- CHANNEL SESSIONS (proxy-built continuous playlists; see hls_session.py) ---

SESSION_IDLE_SECONDS = bounded_float(os.getenv("SESSION_IDLE_SECONDS", "60"), 60.0, 10.0, 3600.0)
# Jellyfin's ffmpeg does not retry a failed first open, so the startup budget
# must cover a slow candidate race plus a cold failover chain. The No-Signal
# placeholder (started immediately on cold start when ffmpeg exists) means
# Jellyfin normally gets a playable playlist long before this expires.
STREAM_STARTUP_TIMEOUT = bounded_float(os.getenv("STREAM_STARTUP_TIMEOUT", "45"), 45.0, 3.0, 180.0)
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
    if not ffmpeg_proc.FFMPEG_AVAILABLE or _placeholder_cooldown_remaining() > 0:
        return
    if _PLACEHOLDER_START_TASK is None or _PLACEHOLDER_START_TASK.done():
        _PLACEHOLDER_START_TASK = _spawn_background_task(_ensure_placeholder_running(), "start placeholder stream")


def _placeholder_source_key(run_id: int) -> tuple:
    return PLACEHOLDER_SOURCE_KEY + (run_id,)


def _is_placeholder_key(key) -> bool:
    """True for any placeholder source key, whatever placeholder run it names."""
    return bool(key) and tuple(key)[:len(PLACEHOLDER_SOURCE_KEY)] == PLACEHOLDER_SOURCE_KEY


def _placeholder_source() -> Optional[SourceSpec]:
    state = placeholder._PLACEHOLDER_STATE
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
        cookies=str(candidate.get("cookies") or ""),
    )


def _resolve_session_source(channel_id: str) -> Optional[SourceSpec]:
    """What should this channel's session play right now? (pull model)"""
    # Late import: multiview and failover import this module, so they are reached at call time.
    from multiview import _multiview_view_for_session, _resolve_multiview_view_source
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
    # Late import: multiview and failover import this module, so they are reached at call time.
    from multiview import _multiview_view_for_session, _on_multiview_view_failure
    from failover import request_failover
    if _is_placeholder_key(source_key):
        return
    if _multiview_view_for_session(channel_id) is not None:
        _on_multiview_view_failure(channel_id, source_key, reason)
        return
    # Cold start + expired token: fail over to the next candidate's token
    # immediately instead of only waiting for the background rescrape.
    # Jellyfin is already waiting on this tune.
    session = SESSIONS.peek(channel_id)
    force = reason == "playlist forbidden" and session is not None and session.is_cold
    _spawn_background_task(
        request_failover(channel_id, source_key, reason, force_failover=force),
        f"session failover {channel_id}",
    )


def _on_session_incompatible(channel_id: str, source_key: tuple, reason: str) -> bool:
    """Returns True when another standby exists (the session keeps going and
    picks it up), False to let a cold-starting session park in the SAMPLE-AES
    shim. In 3.0 every source is session-playable (fMP4 via remux), so this is
    only reached for SAMPLE-AES cold starts and not_ts garbage."""
    # Late import: multiview and failover import this module, so they are reached at call time.
    from multiview import _multiview_view_for_session
    from failover import _pick_next_candidate, request_failover
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
    _spawn_background_task(
        request_failover(channel_id, source_key, reason),
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
        headers_for=lambda source: _upstream_media_headers(source.referer, source.origin, source.cookies),
        resolve_source=lambda channel_id: _resolve_session_source(channel_id),
        report_failure=lambda channel_id, key, reason: _on_session_failure(channel_id, key, reason),
        report_incompatible=lambda channel_id, key, reason: _on_session_incompatible(channel_id, key, reason),
        on_media_info=lambda channel_id, key, has_audio, sig: _on_session_media_info(channel_id, key, has_audio, sig),
        preferred_audio_language=engine_settings.preferred_audio_language,
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
    if not data or data.get("type") == "multiview" or not ffmpeg_proc.FFMPEG_AVAILABLE:
        return False
    data["startup_placeholder_until"] = time.monotonic() + STARTUP_PLACEHOLDER_SECONDS
    LOGGER.info("Stream slow to start; showing No Signal meanwhile channel=%s", channel_id)
    engine_stats.record_placeholder_fallback()
    _request_placeholder_start()
    SESSIONS.poke(channel_id)
    return True


def _is_raceable_channel(session_id: str) -> bool:
    """True for a regular channel with candidates to race (not the placeholder
    session, not Multi-View)."""
    if session_id == PLACEHOLDER_SESSION_ID:
        return False
    data = stream_state.get(session_id)
    if not data or data.get("type") == "multiview":
        return False
    return bool(data.get("candidates"))


async def _race_cold_candidates(channel_id: str, limit: int = 3, timeout: float = 10.0) -> bool:
    """Cold-start: fetch the top candidates' playlists in parallel and make the
    first healthy one (HTTP 200 with HLS content) the active candidate, instead
    of failing over serially through dead ones. Returns True when a healthy
    candidate was selected. Best-effort: failures leave active_index alone."""
    from failover import _team_state_lock

    async with _team_state_lock(channel_id):
        data = stream_state.get(channel_id)
        if not data or data.get("type") == "multiview":
            return False
        candidates = list(data.get("candidates") or [])[:limit]
        if len(candidates) < 2:
            return False
        specs = []
        for candidate in candidates:
            spec = _candidate_source(candidate)
            if spec is not None:
                specs.append((candidate_source_key(candidate), spec))
    if len(specs) < 2:
        return False

    async def _probe(key: tuple, spec: SourceSpec) -> Optional[tuple]:
        headers = _upstream_media_headers(spec.referer, spec.origin, spec.cookies)
        try:
            result = await _session_fetch(spec.url, headers, 2 * 1024 * 1024, 8.0)
        except Exception:
            return None
        if result is None or result.status != 200:
            return None
        if b"#EXTM3U" not in result.body:
            return None
        return key

    tasks = [asyncio.create_task(_probe(key, spec)) for key, spec in specs]
    winner: Optional[tuple] = None
    try:
        for coro in asyncio.as_completed(tasks, timeout=timeout):
            try:
                key = await coro
            except Exception:
                continue
            if key is not None:
                winner = key
                break
    except asyncio.TimeoutError:
        pass
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
    if winner is None:
        return False

    async with _team_state_lock(channel_id):
        data = stream_state.get(channel_id)
        if not data:
            return False
        current = data.get("candidates") or []
        for index, candidate in enumerate(current):
            if candidate_source_key(candidate) == winner:
                if index != data.get("active_index", 0):
                    data["active_index"] = index
                    data["exhausted"] = False
                    LOGGER.info(
                        "Cold-start race picked candidate channel=%s provider=%s",
                        channel_id, candidate.get("provider", "Unknown"),
                    )
                SESSIONS.poke(channel_id)
                return True
        return False


async def _serve_session_playlist(session_id: str, segment_prefix: str, legacy=None) -> Response:
    session = SESSIONS.get(session_id)
    cold = not session.is_running and not session.window and session.state != "sample_aes"
    if cold and _is_raceable_channel(session_id):
        # Fix 1: race the top candidates for a healthy one before the
        # session's serial poll begins, so one dead first candidate can't eat
        # the whole startup budget.
        try:
            race_won = await _race_cold_candidates(session_id)
        except Exception:
            # The race does network I/O; an unexpected error here must not
            # turn a tune-in into a 500.
            LOGGER.exception("Cold-start race failed channel=%s; starting No-Signal placeholder", session_id)
            race_won = False
        engine_stats.record_cold_race(race_won)
        if not race_won:
            # Fix 2: no healthy candidate found quickly: start No-Signal NOW
            # so Jellyfin's ffmpeg always gets a playable playlist on first
            # open (it does not retry a failed first open). The real source
            # takes over through the normal discontinuity machinery when it
            # becomes ready.
            _start_on_placeholder(session_id)
    session.touch()
    if session.state == "sample_aes" and legacy is not None:
        return await legacy()
    await session.wait_ready(STREAM_STARTUP_TIMEOUT)
    if session.state == "sample_aes" and legacy is not None:
        return await legacy()
    if not session.window and _start_on_placeholder(session_id):
        await session.wait_ready(10.0)
    if not session.window:
        # Genuinely exhausted/dead: no segments and no placeholder (ffmpeg
        # missing or failed to start). Jellyfin won't retry a 503, but there
        # is nothing playable to hand it.
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
    # Late import: multiview and failover import this module, so they are reached at call time.
    from multiview import _touch_multiview_viewer
    if team_id == PLACEHOLDER_SESSION_ID and ffmpeg_proc.FFMPEG_AVAILABLE:
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

    async def sample_aes():
        return await _sample_aes_proxy_stream(team_id, request)

    return await _serve_session_playlist(team_id, f"{team_id}/seg/", legacy=None if data.get("type") == "multiview" else sample_aes)
