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
from legacy_proxy import _fetch_upstream_body, _hls_response, _legacy_proxy_stream, HLS_MEDIA_TYPE
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
    _spawn_background_task(request_failover(channel_id, source_key, reason), f"session failover {channel_id}")


def _on_session_incompatible(channel_id: str, source_key: tuple, reason: str) -> bool:
    """Returns True when a compatible standby exists (the session keeps going
    and picks it up), False to let a cold-starting session use the legacy proxy."""
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
    if not data or data.get("type") == "multiview" or not ffmpeg_proc.FFMPEG_AVAILABLE:
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

    async def legacy():
        return await _legacy_proxy_stream(team_id, request)

    return await _serve_session_playlist(team_id, f"{team_id}/seg/", legacy=None if data.get("type") == "multiview" else legacy)
