"""Legacy relay proxy for sources the channel sessions can't normalize
(fMP4, separate audio): manifest rewriting, the /substream, /resource and
/chunk* relay routes, read-ahead prefetch, the chunk cache and the startup buffer.
"""

import asyncio
import logging
import os
import re
import threading
import time
import urllib.parse
from collections import OrderedDict
from typing import Callable, Dict, List, Optional, Set, Tuple

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse, Response, StreamingResponse

from config import (
    _log_failure,
    _positive_env_number,
    _public_base_url,
    _upstream_media_headers,
    _validate_upstream_url,
    LOGGER,
)
from state import _media_client, _spawn_background_task, PLACEHOLDER_SESSION_ID, stream_state
from security import _relay_signature, _relay_signature_ok
from network_safety import bounded_float, bounded_int, validate_http_url_async
# Shared with the channel sessions; defined in upstream.py and re-exported here.
from upstream import (
    _fetch_upstream_body,
    _hls_response,
    _log_upstream_exception,
    _log_upstream_rejection,
    _UPSTREAM_EXCEPTION_LOGGED,
    _UPSTREAM_REJECTION_LOGGED,
    HLS_MEDIA_TYPE,
    MAX_UPSTREAM_REDIRECTS,
    STREAM_REQUEST_TIMEOUT,
)

router = APIRouter()


STREAM_CHUNK_CACHE_TTL = _positive_env_number("STREAM_CHUNK_CACHE_TTL", 15.0)
STREAM_STARTUP_BUFFER_SECONDS = bounded_float(
    os.getenv("STREAM_STARTUP_BUFFER_SECONDS", "15"), 15.0, 0.0, 120.0
)
STREAM_CHUNK_CACHE_CAPACITY = bounded_int(os.getenv("STREAM_CHUNK_CACHE_CAPACITY", "60"), 60, 1, 2000)
STREAM_CHUNK_CACHE_MAX_BYTES = bounded_int(
    os.getenv("STREAM_CHUNK_CACHE_MAX_BYTES", str(128 * 1024 * 1024)), 128 * 1024 * 1024, 1024 * 1024, 1024 * 1024 * 1024
)
PREFETCH_CHUNK_COUNT = bounded_int(os.getenv("PREFETCH_CHUNK_COUNT", "5"), 5, 0, 32)
PREFETCH_CONCURRENCY = bounded_int(os.getenv("PREFETCH_CONCURRENCY", "2"), 2, 1, 32)
MAX_MANIFEST_BYTES = bounded_int(os.getenv("MAX_MANIFEST_BYTES", str(2 * 1024 * 1024)), 2 * 1024 * 1024, 64 * 1024, 16 * 1024 * 1024)
MAX_RESOURCE_BYTES = bounded_int(os.getenv("MAX_RESOURCE_BYTES", str(8 * 1024 * 1024)), 8 * 1024 * 1024, 1024, 64 * 1024 * 1024)
MAX_CACHEABLE_CHUNK_BYTES = bounded_int(os.getenv("MAX_CACHEABLE_CHUNK_BYTES", str(4 * 1024 * 1024)), 4 * 1024 * 1024, 1024, 32 * 1024 * 1024)

class LRUChunkCache:
    """LRU + TTL cache, bounded by both entry count and total bytes held. Entry-count
    alone let capacity*MAX_CACHEABLE_CHUNK_BYTES (60*4MB=240MB) sit as a worst case;
    max_bytes gives a real memory ceiling regardless of individual chunk sizes."""

    def __init__(self, capacity: int = 60, max_bytes: int = 0):
        self.cache = OrderedDict()
        self.capacity = capacity
        self.max_bytes = max_bytes
        self.total_bytes = 0
        self.hits = 0
        self.misses = 0
        self.lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[bytes]:
        async with self.lock:
            if key not in self.cache:
                self.misses += 1
                return None
            val, exp = self.cache[key]
            if time.monotonic() > exp:
                self.total_bytes -= len(val)
                del self.cache[key]
                self.misses += 1
                return None
            self.cache.move_to_end(key)
            self.hits += 1
            return val

    async def stats(self) -> dict:
        """In-memory hit/miss counters since process start — avoids the DB write on
        every chunk fetch that a per-request cache_metrics INSERT would require on
        this hot path, at the cost of resetting on restart rather than persisting."""
        async with self.lock:
            total = self.hits + self.misses
            hit_rate = (self.hits / total * 100) if total > 0 else 0.0
            return {"hit_rate": round(hit_rate, 1), "total_hits": self.hits, "total_misses": self.misses}

    async def put(self, key: str, value: bytes, ttl: float) -> None:
        async with self.lock:
            now = time.monotonic()
            for expired_key, (expired_value, expires_at) in list(self.cache.items()):
                if now > expires_at:
                    self.total_bytes -= len(expired_value)
                    del self.cache[expired_key]
            existing = self.cache.get(key)
            if existing is not None:
                self.total_bytes -= len(existing[0])
            self.cache[key] = (value, time.monotonic() + ttl)
            self.total_bytes += len(value)
            self.cache.move_to_end(key)
            while len(self.cache) > self.capacity or (self.max_bytes and self.total_bytes > self.max_bytes):
                if not self.cache:
                    break
                _, (evicted_val, _) = self.cache.popitem(last=False)
                self.total_bytes -= len(evicted_val)

    async def purge_expired(self) -> None:
        async with self.lock:
            now = time.monotonic()
            for key, (value, expires_at) in list(self.cache.items()):
                if now > expires_at:
                    self.total_bytes -= len(value)
                    del self.cache[key]

CHUNK_CACHE = LRUChunkCache(capacity=STREAM_CHUNK_CACHE_CAPACITY, max_bytes=STREAM_CHUNK_CACHE_MAX_BYTES)

_PREFETCH_IN_FLIGHT: Set[str] = set()
_PREFETCH_LOCK = threading.Lock()
_PREFETCH_SEMAPHORE: Optional[asyncio.Semaphore] = None
_STARTUP_BUFFER_TASKS: Dict[str, asyncio.Task] = {}
_STARTUP_BUFFER_LOCK = asyncio.Lock()


def _remove_startup_buffer_task(done: asyncio.Task) -> None:
    for key, task in list(_STARTUP_BUFFER_TASKS.items()):
        if task is done:
            _STARTUP_BUFFER_TASKS.pop(key, None)


def _manifest_uri_is_playlist(url: str) -> bool:
    parsed = urllib.parse.urlsplit(str(url or ""))
    path = parsed.path.lower()
    query = parsed.query.lower()
    return (
        path.endswith(".m3u8")
        or "/playlist/" in path
        or "/manifest" in path
        or "/load-playlist" in path
        or ".m3u8" in query
    )


def _chunk_route_for_url(url: str) -> str:
    path = urllib.parse.urlsplit(str(url or "")).path.lower()
    if path.endswith((".m4s", ".mp4", ".m4v")):
        return "/chunk.mp4"
    if path.endswith((".aac", ".m4a")):
        return "/chunk.aac"
    if path.endswith((".vtt", ".webvtt")):
        return "/chunk.vtt"
    return "/chunk.ts"


_MAX_STORED_MANIFESTS = 50
_MAX_STORED_SEGMENTS = 5000
_MANIFEST_SEGMENTS_LOCK = threading.Lock()
_MANIFEST_MEDIA_SEGMENTS: "OrderedDict[str, List[str]]" = OrderedDict()
_SEGMENT_TO_MANIFEST: "OrderedDict[str, str]" = OrderedDict()

# --- Legacy playback failure reporting -------------------------------------
# Failures on the legacy passthrough proxy used to be invisible to the
# failover machinery: a dying legacy source waited out the 30s probe cycle
# instead of failing over in seconds. The hook below is registered by
# sessions.py at import ((team_id, reason) -> None) and feeds the same
# report_failure path session playback uses.
_LEGACY_FAILURE_HOOK: Optional[Callable[[str, str], None]] = None
# (team_id, manifest_url) -> last report, monotonic: one failover request per
# window per source no matter how often the player polls.
_LEGACY_FAILURE_THROTTLE: Dict[Tuple[str, str], float] = {}
_LEGACY_FAILURE_THROTTLE_SECONDS = 30.0
# (team_id, manifest_url) -> consecutive upstream failures (manifest fetch or
# chunk relay). Manifests are polled every few seconds during playback, so 2
# consecutive failures mean the source is dead, not blipping; chunks get 3.
_LEGACY_UPSTREAM_FAILURES: Dict[Tuple[str, str], int] = {}
_LEGACY_MANIFEST_FAILURE_THRESHOLD = 2
_LEGACY_CHUNK_FAILURE_THRESHOLD = 3
# manifest/effective URL -> team_id, so chunk failures (which carry only a
# signed upstream URL) can be attributed to their channel.
_MANIFEST_TEAM: "OrderedDict[str, str]" = OrderedDict()
_MAX_MANIFEST_TEAMS = 128


def set_legacy_failure_hook(hook: Optional[Callable[[str, str], None]]) -> None:
    """Registered once by sessions.py: (team_id, reason) -> None."""
    global _LEGACY_FAILURE_HOOK
    _LEGACY_FAILURE_HOOK = hook


def _note_manifest_team(manifest_url: str, team_id: str) -> None:
    if not manifest_url or not team_id:
        return
    _MANIFEST_TEAM[manifest_url] = team_id
    _MANIFEST_TEAM.move_to_end(manifest_url)
    while len(_MANIFEST_TEAM) > _MAX_MANIFEST_TEAMS:
        _MANIFEST_TEAM.popitem(last=False)


def _manifest_team_for_chunk(chunk_url: str) -> Tuple[str, str]:
    """(team_id, manifest_url) for a chunk relay URL, or ("", "") when unknown."""
    with _MANIFEST_SEGMENTS_LOCK:
        manifest_url = _SEGMENT_TO_MANIFEST.get(chunk_url) or ""
    if not manifest_url:
        return "", ""
    return _MANIFEST_TEAM.get(manifest_url) or "", manifest_url


def _legacy_upstream_ok(team_id: str, manifest_url: str) -> None:
    _LEGACY_UPSTREAM_FAILURES.pop((team_id, manifest_url), None)


def _legacy_upstream_failed(team_id: str, manifest_url: str, reason: str, threshold: int) -> None:
    """Count a consecutive upstream failure; report through the failover hook
    when it reaches threshold, then reset the count so a still-dead source
    re-reports after another `threshold` failures (one report per throttle
    window regardless)."""
    if not team_id or not manifest_url:
        return
    key = (team_id, manifest_url)
    count = _LEGACY_UPSTREAM_FAILURES.get(key, 0) + 1
    _LEGACY_UPSTREAM_FAILURES[key] = count
    if count < threshold:
        return
    _LEGACY_UPSTREAM_FAILURES[key] = 0
    hook = _LEGACY_FAILURE_HOOK
    if hook is None:
        return
    now = time.monotonic()
    if now - _LEGACY_FAILURE_THROTTLE.get(key, 0.0) < _LEGACY_FAILURE_THROTTLE_SECONDS:
        return
    _LEGACY_FAILURE_THROTTLE[key] = now
    try:
        hook(team_id, reason)
    except Exception:
        LOGGER.exception("legacy failure hook failed team=%s", team_id)


def _reorder_hls_variants(manifest_lines: List[str]) -> List[str]:
    """Reorder HLS variants by BANDWIDTH (highest first) for quality preference.

    Non-variant lines (#EXTM3U, #EXT-X-VERSION, #EXT-X-MEDIA groups, etc.) are
    preserved in place; only the STREAM-INF/URI pairs are moved, as sorted, to
    where the first variant originally appeared.
    """
    variants = []
    other_lines = []
    first_variant_pos = None
    i = 0
    while i < len(manifest_lines):
        line = manifest_lines[i].strip()
        if line.startswith("#EXT-X-STREAM-INF:") and i + 1 < len(manifest_lines):
            if first_variant_pos is None:
                first_variant_pos = len(other_lines)
            bandwidth_match = re.search(r'BANDWIDTH=(\d+)', line)
            bandwidth = int(bandwidth_match.group(1)) if bandwidth_match else 0
            variants.append((bandwidth, manifest_lines[i], manifest_lines[i + 1]))
            i += 2
            continue
        other_lines.append(manifest_lines[i])
        i += 1

    if not variants:
        return manifest_lines

    variants.sort(key=lambda x: x[0], reverse=True)
    variant_lines = []
    for _, inf_line, url_line in variants:
        variant_lines.append(inf_line)
        variant_lines.append(url_line)

    return other_lines[:first_variant_pos] + variant_lines + other_lines[first_variant_pos:]

def extract_manifest_media_urls(manifest_text: str, target_url: str) -> List[str]:
    """Parse all playable media chunk URLs from an HLS manifest in playlist order."""
    urls: List[str] = []
    seen: Set[str] = set()
    expect_variant_playlist = False
    for line in manifest_text.splitlines():
        value = line.strip()
        if not value:
            continue
        if value.startswith("#"):
            expect_variant_playlist = value.startswith("#EXT-X-STREAM-INF:")
            continue
        if expect_variant_playlist:
            expect_variant_playlist = False
            continue
        resolved = urllib.parse.urljoin(target_url, value)
        if _manifest_uri_is_playlist(resolved):
            continue
        if _validate_upstream_url(resolved) and resolved not in seen:
            seen.add(resolved)
            urls.append(resolved)
    return urls


def _register_manifest_segments(manifest_text: str, manifest_url: str) -> List[str]:
    """Store manifest segment URLs for upcoming chunk prefetch lookups."""
    segments = extract_manifest_media_urls(manifest_text, manifest_url)
    if not segments:
        return []
    with _MANIFEST_SEGMENTS_LOCK:
        _MANIFEST_MEDIA_SEGMENTS[manifest_url] = segments
        if len(_MANIFEST_MEDIA_SEGMENTS) > _MAX_STORED_MANIFESTS:
            _MANIFEST_MEDIA_SEGMENTS.popitem(last=False)
        for seg in segments:
            _SEGMENT_TO_MANIFEST[seg] = manifest_url
            if len(_SEGMENT_TO_MANIFEST) > _MAX_STORED_SEGMENTS:
                _SEGMENT_TO_MANIFEST.popitem(last=False)
    return segments


def _get_next_manifest_chunks(current_chunk_url: str, count: int = 5) -> List[str]:
    """Return upcoming media segment URLs following current_chunk_url in the manifest."""
    if count <= 0:
        return []
    with _MANIFEST_SEGMENTS_LOCK:
        manifest_url = _SEGMENT_TO_MANIFEST.get(current_chunk_url)
        segments = _MANIFEST_MEDIA_SEGMENTS.get(manifest_url) if manifest_url else None

        if not segments:
            current_path = urllib.parse.urlsplit(current_chunk_url).path
            for m_url, seg_list in reversed(_MANIFEST_MEDIA_SEGMENTS.items()):
                for seg in seg_list:
                    if seg == current_chunk_url or urllib.parse.urlsplit(seg).path == current_path:
                        manifest_url = m_url
                        segments = seg_list
                        break
                if segments:
                    break

        if not segments:
            return []

        idx = -1
        try:
            idx = segments.index(current_chunk_url)
        except ValueError:
            current_path = urllib.parse.urlsplit(current_chunk_url).path
            for i, seg in enumerate(segments):
                if urllib.parse.urlsplit(seg).path == current_path:
                    idx = i
                    break

        if idx < 0:
            return []

        return segments[idx + 1 : idx + 1 + count]


def rewrite_m3u8(
    manifest_text: str,
    target_url: str,
    referer: str,
    host: str,
    origin: str = "",
) -> str:
    _register_manifest_segments(manifest_text, target_url)
    rewritten_manifest = []
    enc_ref = urllib.parse.quote(referer or "")
    enc_origin = urllib.parse.quote(origin or "")
    expect_variant_playlist = False
    has_variants = "#EXT-X-STREAM-INF:" in manifest_text

    def _proxy_resource_uri(
        uri: str,
        resource_path: Optional[str] = None,
        force_playlist: bool = False,
    ) -> str:
        resolved_url = urllib.parse.urljoin(target_url, uri)
        if not _validate_upstream_url(resolved_url):
            return uri
        enc_url = urllib.parse.quote(resolved_url)
        if force_playlist or _manifest_uri_is_playlist(resolved_url):
            route = f"{host}/substream.m3u8?url={enc_url}&ref={enc_ref}"
        else:
            route = f"{host}{resource_path or _chunk_route_for_url(resolved_url)}?url={enc_url}&ref={enc_ref}"
        if enc_origin:
            route = f"{route}&org={enc_origin}"
        return f"{route}&sig={_relay_signature(resolved_url, referer, origin)}"

    manifest_lines = manifest_text.splitlines()
    if has_variants:
        manifest_lines = _reorder_hls_variants(manifest_lines)

    for line in manifest_lines:
        line_clean = line.strip()
        if not line_clean:
            rewritten_manifest.append(line)
        elif line_clean.startswith("#"):
            if 'URI="' in line:
                is_playlist_attribute = line_clean.startswith((
                    "#EXT-X-MEDIA:",
                    "#EXT-X-I-FRAME-STREAM-INF:",
                ))
                line = re.sub(
                    r'URI="([^"]+)"',
                    lambda match: f'URI="{_proxy_resource_uri(
                        match.group(1),
                        "/resource" if line_clean.startswith(("#EXT-X-KEY", "#EXT-X-MAP", "#EXT-X-SESSION-KEY")) else None,
                        force_playlist=is_playlist_attribute,
                    )}"',
                    line,
                )
            rewritten_manifest.append(line)
            expect_variant_playlist = line_clean.startswith("#EXT-X-STREAM-INF:")
        else:
            rewritten_manifest.append(_proxy_resource_uri(line_clean, force_playlist=expect_variant_playlist))
            expect_variant_playlist = False

    return "\n".join(rewritten_manifest)


def _chunk_media_type(url: str) -> str:
    path = urllib.parse.urlsplit(str(url or "")).path.lower()
    if path.endswith((".m4s", ".mp4", ".m4v")):
        return "video/mp4"
    if path.endswith((".aac", ".m4a")):
        return "audio/aac"
    if path.endswith((".vtt", ".webvtt")):
        return "text/vtt"
    return "video/mp2t"


def _startup_media_urls(manifest_text: str, target_url: str) -> List[str]:
    """The newest segments: a live player starts ~3 from the end, so warming
    the oldest ones (as this used to) fetched segments nobody asked for."""
    urls = extract_manifest_media_urls(manifest_text, target_url)
    limit = PREFETCH_CHUNK_COUNT if PREFETCH_CHUNK_COUNT > 0 else 5
    return urls[-limit:]


async def _warm_startup_buffer(
    client: httpx.AsyncClient,
    media_urls: List[str],
    referer: str,
    origin: str = "",
) -> None:
    global _PREFETCH_SEMAPHORE
    if not media_urls:
        return
    if _PREFETCH_SEMAPHORE is None:
        _PREFETCH_SEMAPHORE = asyncio.Semaphore(PREFETCH_CONCURRENCY)
    cache_ttl = max(STREAM_CHUNK_CACHE_TTL, STREAM_STARTUP_BUFFER_SECONDS + 30.0)

    async def warm_one(media_url: str) -> None:
        cache_key = f"{media_url}\0{referer}\0{origin}"
        if await CHUNK_CACHE.get(cache_key):
            return
        async with _PREFETCH_SEMAPHORE:
            result = await _fetch_upstream_body(
                client,
                media_url,
                _upstream_media_headers(referer, origin),
                MAX_CACHEABLE_CHUNK_BYTES,
            )
        if result:
            status_code, _, _, body, _ = result
            if status_code in (200, 206) and body:
                await CHUNK_CACHE.put(cache_key, body, cache_ttl)

    await asyncio.gather(*(warm_one(url) for url in media_urls), return_exceptions=True)


async def _ensure_startup_buffer(
    cache_key: str,
    client: httpx.AsyncClient,
    media_urls: List[str],
    referer: str,
    origin: str = "",
) -> None:
    if not media_urls or STREAM_STARTUP_BUFFER_SECONDS <= 0:
        return
    async with _STARTUP_BUFFER_LOCK:
        task = _STARTUP_BUFFER_TASKS.get(cache_key)
        if task is None:
            task = _spawn_background_task(
                _warm_startup_buffer(client, media_urls, referer, origin),
                "warm stream startup buffer",
            )
            _STARTUP_BUFFER_TASKS[cache_key] = task
            task.add_done_callback(_remove_startup_buffer_task)


async def _legacy_proxy_stream(team_id: str, request: Request, provider: str = ""):
    """Pre-session passthrough proxy: rewrites the upstream playlist's URIs to
    /substream.m3u8 and /chunk*. Only used for sources a channel session can't
    normalize (fMP4, separate audio renditions, SAMPLE-AES) and for ?provider=."""
    data = stream_state.get(team_id)
    if not data:
        return Response(status_code=404, content="Stream unavailable")
    if not data.get("candidates"):
        # No Signal comes from the placeholder channel session now.
        return RedirectResponse(url=f"/stream/{PLACEHOLDER_SESSION_ID}.m3u8", status_code=307)
    active_idx = data.get("active_index", 0)
    if active_idx >= len(data["candidates"]):
        active_idx = 0

    if provider and len(data["candidates"]) > 0:
        for idx, candidate in enumerate(data["candidates"]):
            if candidate.get("provider", "").lower() == provider.lower():
                active_idx = idx
                break

    active_stream = data["candidates"][active_idx]
    target_url = active_stream["url"]
    if not _validate_upstream_url(target_url):
        LOGGER.warning("Rejected invalid active stream URL team=%s", team_id)
        return Response(status_code=502, content="Invalid upstream stream URL")
    referer = active_stream.get("referer", "")
    origin = active_stream.get("origin", "")
    if referer and not _validate_upstream_url(referer):
        referer = ""
    headers = _upstream_media_headers(referer, origin)
    proxy_origin = _public_base_url(request)

    owns_client = _media_client() is None
    client = _media_client() or httpx.AsyncClient(follow_redirects=False, timeout=12.0, http2=True)
    try:
        result = await _fetch_upstream_body(client, target_url, headers, MAX_MANIFEST_BYTES)
        if result is None:
            _legacy_upstream_failed(team_id, target_url, "legacy manifest unreachable", _LEGACY_MANIFEST_FAILURE_THRESHOLD)
            return Response(status_code=502, content="Upstream manifest unavailable")
        status_code, effective_url, _, body, _ = result
        if status_code != 200:
            _legacy_upstream_failed(team_id, target_url, f"legacy manifest status {status_code}", _LEGACY_MANIFEST_FAILURE_THRESHOLD)
            return Response(status_code=status_code)
        _legacy_upstream_ok(team_id, target_url)
        _note_manifest_team(target_url, team_id)
        _note_manifest_team(effective_url, team_id)
        manifest_text = body.decode("utf-8", errors="replace")
        await _ensure_startup_buffer(
            f"{effective_url}\0{referer}\0{origin}",
            client,
            _startup_media_urls(manifest_text, effective_url),
            referer,
            origin,
        )
        rewritten = rewrite_m3u8(manifest_text, effective_url, referer, proxy_origin, origin)
        return _hls_response(rewritten)
    except Exception as exc:
        _log_failure(f"proxy manifest team={team_id}", exc, logging.ERROR)
        _legacy_upstream_failed(team_id, target_url, "legacy manifest error", _LEGACY_MANIFEST_FAILURE_THRESHOLD)
        return Response(status_code=502, content="Upstream manifest unavailable")
    finally:
        if owns_client:
            await client.aclose()


@router.get("/substream.m3u8")
async def proxy_substream(request: Request, url: str, ref: str = "", org: str = "", sig: str = ""):
    decoded_url = (await validate_http_url_async(url)) or ""
    decoded_ref = ((await validate_http_url_async(ref)) if ref else "") or ""
    decoded_origin = org or ""
    if not _relay_signature_ok(decoded_url, decoded_ref, decoded_origin, sig):
        return Response(status_code=403, content="Unsigned relay URL")
    if not _validate_upstream_url(decoded_url):
        return Response(status_code=400, content="Invalid upstream manifest URL")
    if decoded_ref and not _validate_upstream_url(decoded_ref):
        return Response(status_code=400, content="Invalid upstream referer URL")
    headers = _upstream_media_headers(decoded_ref, decoded_origin)
    proxy_origin = _public_base_url(request)
    
    owns_client = _media_client() is None
    client = _media_client() or httpx.AsyncClient(follow_redirects=False, timeout=12.0, http2=True)
    try:
        result = await _fetch_upstream_body(client, decoded_url, headers, MAX_MANIFEST_BYTES)
        if result is None:
            return Response(status_code=502, content="Upstream manifest unavailable")
        status_code, effective_url, _, body, _ = result
        if status_code != 200:
            return Response(status_code=status_code)
        manifest_text = body.decode("utf-8", errors="replace")
        await _ensure_startup_buffer(
            f"{effective_url}\0{decoded_ref}\0{decoded_origin}",
            client,
            _startup_media_urls(manifest_text, effective_url),
            decoded_ref,
            decoded_origin,
        )
        rewritten = rewrite_m3u8(manifest_text, effective_url, decoded_ref, proxy_origin, decoded_origin)
        return Response(
            content=rewritten,
            media_type="application/vnd.apple.mpegurl",
            headers={"Cache-Control": "no-cache", "Access-Control-Allow-Origin": "*"},
        )
    except Exception as exc:
        _log_failure("proxy submanifest", exc, logging.ERROR)
        return Response(status_code=502, content="Upstream manifest unavailable")
    finally:
        if owns_client:
            await client.aclose()


@router.get("/resource")
async def proxy_resource(request: Request, url: str, ref: str = "", org: str = "", sig: str = ""):
    """Proxy HLS key and initialization resources with the original media type."""
    decoded_url = (await validate_http_url_async(url)) or ""
    decoded_ref = ((await validate_http_url_async(ref)) if ref else "") or ""
    decoded_origin = org or ""
    if not _relay_signature_ok(decoded_url, decoded_ref, decoded_origin, sig):
        return Response(status_code=403, content="Unsigned relay URL")
    if not _validate_upstream_url(decoded_url):
        return Response(status_code=400, content="Invalid upstream resource URL")
    if decoded_ref and not _validate_upstream_url(decoded_ref):
        return Response(status_code=400, content="Invalid upstream referer URL")

    owns_client = _media_client() is None
    client = _media_client() or httpx.AsyncClient(
        follow_redirects=False,
        timeout=STREAM_REQUEST_TIMEOUT,
        http2=True,
    )
    try:
        range_header = request.headers.get("range", "").strip()
        if range_header and not re.fullmatch(r"bytes=\d*-\d*", range_header):
            range_header = ""
        upstream_headers = _upstream_media_headers(decoded_ref, decoded_origin)
        if range_header:
            upstream_headers["Range"] = range_header
        result = await _fetch_upstream_body(
            client,
            decoded_url,
            upstream_headers,
            MAX_RESOURCE_BYTES,
        )
        if result is None:
            return Response(status_code=502, content="Upstream resource unavailable")
        status_code, _, content_type, body, response_headers = result
        if status_code not in (200, 206):
            headers = {
                key: response_headers[key]
                for key in ("content-range", "accept-ranges")
                if response_headers.get(key)
            }
            return Response(content=body, status_code=status_code, headers=headers)
        media_type = content_type.split(";", 1)[0] or "application/octet-stream"
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-cache",
        }
        for key in ("content-range", "accept-ranges"):
            if response_headers.get(key):
                headers[key] = response_headers[key]
        return Response(content=body, status_code=status_code, media_type=media_type, headers=headers)
    except Exception as exc:
        _log_failure("proxy HLS resource", exc, logging.ERROR)
        return Response(status_code=502, content="Upstream resource unavailable")
    finally:
        if owns_client:
            await client.aclose()


async def prefetch_next_chunks(current_chunk_url: str, referer: str = "", origin: str = "") -> None:
    global _PREFETCH_SEMAPHORE
    if PREFETCH_CHUNK_COUNT <= 0:
        return

    client = _media_client()
    if client is None:
        return

    next_urls = _get_next_manifest_chunks(current_chunk_url, PREFETCH_CHUNK_COUNT)
    if not next_urls:
        return

    if _PREFETCH_SEMAPHORE is None:
        _PREFETCH_SEMAPHORE = asyncio.Semaphore(PREFETCH_CONCURRENCY)

    async def prefetch_one(next_url: str) -> None:
        cache_key = f"{next_url}\0{referer}\0{origin}"
        if await CHUNK_CACHE.get(cache_key):
            return
        with _PREFETCH_LOCK:
            if cache_key in _PREFETCH_IN_FLIGHT:
                return
            _PREFETCH_IN_FLIGHT.add(cache_key)

        try:
            async with _PREFETCH_SEMAPHORE:
                result = await _fetch_upstream_body(
                    client,
                    next_url,
                    _upstream_media_headers(referer, origin),
                    MAX_CACHEABLE_CHUNK_BYTES,
                )
            if result:
                status_code, _, _, body, _ = result
                if status_code in (200, 206) and body:
                    await CHUNK_CACHE.put(cache_key, body, STREAM_CHUNK_CACHE_TTL)
        except Exception as exc:
            LOGGER.debug("Chunk prefetch failed error=%s", type(exc).__name__)
        finally:
            with _PREFETCH_LOCK:
                _PREFETCH_IN_FLIGHT.discard(cache_key)

    await asyncio.gather(
        *(prefetch_one(url) for url in next_urls),
        return_exceptions=True,
    )


async def _open_upstream_media(
    client: httpx.AsyncClient,
    url: str,
    headers: Dict[str, str],
) -> Tuple[Optional[object], Optional[httpx.Response]]:
    """Open a streaming GET, following redirects manually so every hop is
    SSRF-validated. Returns (stream context, response) or (None, None)."""
    current_url = url
    for _ in range(MAX_UPSTREAM_REDIRECTS + 1):
        safe_url = await validate_http_url_async(current_url)
        if not safe_url:
            return None, None
        stream_ctx = client.stream("GET", safe_url, headers=headers, timeout=STREAM_REQUEST_TIMEOUT, follow_redirects=False)
        response = await stream_ctx.__aenter__()
        if 300 <= response.status_code < 400:
            location = response.headers.get("location")
            await stream_ctx.__aexit__(None, None, None)
            if not location:
                return None, None
            current_url = urllib.parse.urljoin(str(response.url), location)
            continue
        return stream_ctx, response
    return None, None


class _UpstreamBodyInterrupted(Exception):
    """Raised mid-body so the server aborts the connection instead of ending a
    chunked response cleanly: a cleanly-ended truncated segment looks complete
    to ffmpeg, while an aborted one makes it retry."""


class _ClosingStreamingResponse(StreamingResponse):
    """StreamingResponse that always runs `on_close`, even when the client
    disconnects before the body generator starts (its own `finally` then never
    runs, which leaked the upstream stream and its pool slot)."""

    def __init__(self, *args, on_close=None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._on_close = on_close

    async def __call__(self, scope, receive, send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            if self._on_close is not None:
                await self._on_close()


@router.get("/chunk.mp4")
@router.get("/chunk.aac")
@router.get("/chunk.vtt")
@router.get("/chunk.ts")
@router.get("/chunk")
async def proxy_chunk(request: Request, url: str, ref: str = "", org: str = "", sig: str = ""):
    """Legacy segment relay (for sources channel sessions can't normalize)."""
    decoded_url = url
    decoded_ref = ref or ""
    decoded_origin = org or ""
    if not _relay_signature_ok(decoded_url, decoded_ref, decoded_origin, sig):
        return Response(status_code=403, content="Unsigned relay URL")
    if not _validate_upstream_url(decoded_url):
        return Response(status_code=400, content="Invalid upstream chunk URL")
    if decoded_ref and not _validate_upstream_url(decoded_ref):
        return Response(status_code=400, content="Invalid upstream referer URL")

    range_header = request.headers.get("range", "").strip()
    if range_header and not re.fullmatch(r"bytes=\d*-\d*", range_header):
        range_header = ""
    # Same key format as prefetch/startup warm (previously this had an extra
    # trailing field, so prefetched chunks could never be served from cache).
    cache_key = f"{decoded_url}\0{decoded_ref}\0{decoded_origin}"
    media_type = _chunk_media_type(decoded_url)
    cache_headers = {"Access-Control-Allow-Origin": "*", "Cache-Control": "public, max-age=15"}

    if not range_header:
        cached_bytes = await CHUNK_CACHE.get(cache_key)
        if cached_bytes is not None:
            if PREFETCH_CHUNK_COUNT > 0:
                _spawn_background_task(prefetch_next_chunks(decoded_url, decoded_ref, decoded_origin), "prefetch cached stream chunks")
            return Response(content=cached_bytes, media_type=media_type, headers=cache_headers)

    owns_singleflight = False
    if not range_header:
        for _ in range(20):
            with _PREFETCH_LOCK:
                if cache_key not in _PREFETCH_IN_FLIGHT:
                    _PREFETCH_IN_FLIGHT.add(cache_key)
                    owns_singleflight = True
                    break
            await asyncio.sleep(0.05)
            cached_bytes = await CHUNK_CACHE.get(cache_key)
            if cached_bytes is not None:
                return Response(content=cached_bytes, media_type=media_type, headers=cache_headers)

    def release_singleflight() -> None:
        if owns_singleflight:
            with _PREFETCH_LOCK:
                _PREFETCH_IN_FLIGHT.discard(cache_key)

    headers = _upstream_media_headers(decoded_ref, decoded_origin)
    if range_header:
        headers["Range"] = range_header
    elif PREFETCH_CHUNK_COUNT > 0:
        _spawn_background_task(prefetch_next_chunks(decoded_url, decoded_ref, decoded_origin), "prefetch stream chunks")

    owns_client = _media_client() is None
    client = _media_client() or httpx.AsyncClient(timeout=STREAM_REQUEST_TIMEOUT, follow_redirects=False, http2=True)

    # Open upstream *before* committing a response status: previously the 200
    # went out first and any upstream error became an empty "successful"
    # segment that ffmpeg never retried. Retry only here, before any byte.
    stream_ctx = None
    response = None
    status_code = 502
    try:
        for attempt in range(3):
            try:
                stream_ctx, response = await _open_upstream_media(client, decoded_url, headers)
            except httpx.HTTPError as exc:
                LOGGER.debug("Chunk open attempt failed attempt=%d error=%s", attempt + 1, type(exc).__name__)
                stream_ctx, response = None, None
            if response is None:
                status_code = 502
            elif response.status_code in (200, 206):
                break
            else:
                status_code = response.status_code
                await stream_ctx.__aexit__(None, None, None)
                stream_ctx, response = None, None
                if status_code not in (429, 500, 502, 503, 504):
                    break
            if attempt < 2:
                await asyncio.sleep(0.25 * (attempt + 1))
    except BaseException:
        # Anything else (client gone mid-retry, an unexpected error): never
        # leave the single-flight key or an open upstream stream behind.
        if stream_ctx is not None:
            await stream_ctx.__aexit__(None, None, None)
        release_singleflight()
        if owns_client:
            await client.aclose()
        raise

    if response is None:
        release_singleflight()
        if owns_client:
            await client.aclose()
        chunk_team_id, chunk_manifest_url = _manifest_team_for_chunk(decoded_url)
        _legacy_upstream_failed(chunk_team_id, chunk_manifest_url, "legacy chunk unavailable", _LEGACY_CHUNK_FAILURE_THRESHOLD)
        return Response(status_code=status_code, content="Upstream chunk unavailable")

    # Attribute this chunk's team for mid-body failure reporting below; a
    # clean open also resets the consecutive-failure count.
    chunk_team_id, chunk_manifest_url = _manifest_team_for_chunk(decoded_url)
    _legacy_upstream_ok(chunk_team_id, chunk_manifest_url)

    out_headers = dict(cache_headers) if not range_header else {"Access-Control-Allow-Origin": "*", "Cache-Control": "no-cache"}
    for key in ("content-range", "accept-ranges"):
        if response.headers.get(key):
            out_headers[key] = response.headers[key]
    opened_ctx, opened_response = stream_ctx, response
    closed = False

    async def close_upstream() -> None:
        nonlocal closed
        if closed:
            return
        closed = True
        try:
            await opened_ctx.__aexit__(None, None, None)
        finally:
            release_singleflight()
            if owns_client:
                await client.aclose()

    async def relay():
        chunk_buffer = bytearray()
        cacheable = not range_header
        try:
            async for block in opened_response.aiter_bytes(chunk_size=131072):
                if cacheable:
                    if len(chunk_buffer) + len(block) <= MAX_CACHEABLE_CHUNK_BYTES:
                        chunk_buffer.extend(block)
                    else:
                        cacheable = False
                        chunk_buffer.clear()
                yield block
            if cacheable and chunk_buffer:
                await CHUNK_CACHE.put(cache_key, bytes(chunk_buffer), STREAM_CHUNK_CACHE_TTL)
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
            _log_failure("relay upstream chunk", exc)
            # A stall mid-body: the player sees a truncated segment, so count
            # it like an open failure toward failover.
            _legacy_upstream_failed(chunk_team_id, chunk_manifest_url, "legacy chunk interrupted", _LEGACY_CHUNK_FAILURE_THRESHOLD)
            raise _UpstreamBodyInterrupted() from exc
        finally:
            await close_upstream()

    return _ClosingStreamingResponse(
        relay(), status_code=opened_response.status_code, media_type=media_type, headers=out_headers,
        on_close=close_upstream,
    )
