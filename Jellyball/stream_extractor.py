"""
stream_extractor.py - Universal Stream Extraction Engine
Finds HLS playlists (.m3u8, /playlist/, /load-playlist, /manifest),
decodes base64-obfuscated streams, navigates nested iframes,
and validates stream health against CDN anti-hotlinking protections.
"""

import re
import base64
import binascii
import asyncio
import html
import logging
import os
import time
import urllib.parse
from contextlib import asynccontextmanager
from typing import AsyncIterator, List, Dict, Tuple, Set, Optional, Iterable
import httpx
from bs4 import BeautifulSoup
from playwright.async_api import Browser, Page
from network_safety import validate_http_url, validate_http_url_async, bounded_int
from ts_normalize import find_ts_start

LOGGER = logging.getLogger("jellyball.stream_extractor")
MAX_INSPECTION_BYTES = 2 * 1024 * 1024
MAX_REDIRECTS = 3
MAX_STREAMER_LINKS = 8
MAX_EXTRACTED_STREAMS = 32
_STREAM_HINTS = (".m3u8", "playlist", "manifest", "load-playlist", "stream", "hls", "live")

# Playwright 1.62 (the version pinned in requirements.txt) bundles Chromium
# 151.0.7922.34 -- checked directly via
# `sync_playwright().chromium.launch().version` against this pin, not guessed.
# JELLYBALL_USER_AGENT overrides this at runtime for providers that start
# fingerprinting the UA string itself; both are read once at import time.
DEFAULT_USER_AGENT = os.getenv("JELLYBALL_USER_AGENT", "").strip() or (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/151.0.0.0 Safari/537.36"
)

# lxml's C parser is materially faster than html.parser on the multi-hundred-KB
# aggregator pages this module parses; fall back cleanly if it isn't installed.
try:
    import lxml  # noqa: F401
    _BS_PARSER = "lxml"
except ImportError:  # pragma: no cover - exercised only when lxml is missing
    _BS_PARSER = "html.parser"


def make_soup(html_text: str) -> BeautifulSoup:
    """Parse HTML with lxml when available, otherwise the stdlib html.parser.

    Behavior (what gets found by find_all/get_text/etc.) is unchanged either
    way; this only picks the faster backend when it's present.
    """
    return BeautifulSoup(html_text or "", _BS_PARSER)


# --- Global cap on concurrent Playwright pages/contexts -----------------------
# Every scraper that falls back to a headless Chromium page shares this single
# semaphore so a scrape cycle can never pile up dozens of live pages against the
# one shared browser that also has to keep serving live video segments.
PLAYWRIGHT_MAX_PAGES = bounded_int(os.getenv("PLAYWRIGHT_MAX_PAGES", "3"), 3, 1, 16)
_PLAYWRIGHT_PAGE_SEMAPHORE: Optional[asyncio.Semaphore] = None
_PLAYWRIGHT_PAGE_SEMAPHORE_LOOP = None
_PLAYWRIGHT_PAGES_IN_USE = 0


def playwright_page_semaphore() -> asyncio.Semaphore:
    """Global asyncio.Semaphore capping concurrent Playwright pages.

    Created lazily against the currently running loop (and recreated if the
    loop changes) so tests that spin up a fresh event loop per run don't bind
    to a semaphore tied to an already-closed loop.
    """
    global _PLAYWRIGHT_PAGE_SEMAPHORE, _PLAYWRIGHT_PAGE_SEMAPHORE_LOOP
    loop = asyncio.get_running_loop()
    if _PLAYWRIGHT_PAGE_SEMAPHORE is None or _PLAYWRIGHT_PAGE_SEMAPHORE_LOOP is not loop:
        _PLAYWRIGHT_PAGE_SEMAPHORE = asyncio.Semaphore(PLAYWRIGHT_MAX_PAGES)
        _PLAYWRIGHT_PAGE_SEMAPHORE_LOOP = loop
    return _PLAYWRIGHT_PAGE_SEMAPHORE


def playwright_pages_in_use() -> int:
    """Number of Playwright pages/contexts currently open across every scraper.

    Used to gate opportunistic browser recycling: never tear down the shared
    browser while a scrape still has a page open against it.
    """
    return _PLAYWRIGHT_PAGES_IN_USE


@asynccontextmanager
async def playwright_page(browser: Browser, *, user_agent: Optional[str] = None) -> AsyncIterator[Page]:
    """Create a Playwright page, optionally inside its own context, bounded by
    the global PLAYWRIGHT_MAX_PAGES cap, and guarantee cleanup on the way out.

    Every scraper that previously called `browser.new_context()`/`new_page()`
    directly closed it only on success or a caught `Exception`. A provider
    search timeout raises `asyncio.CancelledError` (via `asyncio.wait_for`),
    which isn't an `Exception`, so those call sites leaked a live Chromium
    context/page on every cancellation. Routing every page/context creation
    through this context manager's try/finally closes it in all cases,
    including cancellation, and enforces the shared concurrency cap for the
    full lifetime of the page.
    """
    global _PLAYWRIGHT_PAGES_IN_USE
    async with playwright_page_semaphore():
        _PLAYWRIGHT_PAGES_IN_USE += 1
        context = None
        page = None
        try:
            if user_agent:
                context = await browser.new_context(user_agent=user_agent)
                page = await context.new_page()
            else:
                page = await browser.new_page()
            yield page
        finally:
            _PLAYWRIGHT_PAGES_IN_USE -= 1
            if context is not None:
                try:
                    await context.close()
                except Exception as exc:
                    LOGGER.debug("Playwright context cleanup failed error=%s", type(exc).__name__)
            elif page is not None:
                try:
                    await page.close()
                except Exception as exc:
                    LOGGER.debug("Playwright page cleanup failed error=%s", type(exc).__name__)


def rank_streams(streams: Iterable[Dict], provider_priority: Optional[Dict[str, int]] = None) -> List[Dict]:
    """Return stream candidates in a deterministic, best-first order.

    Match quality is the primary signal. Provider priority is optional and only
    affects streams with the same match quality, while URL hints provide a safe
    tie-breaker when providers return incomplete metadata.
    """
    priority = provider_priority or {}

    def sort_key(stream: Dict) -> tuple:
        url = str(stream.get("url") or "")
        low_url = url.lower()
        hls_hint = sum([
            30 if ".m3u8" in low_url else 0,
            15 if any(token in low_url for token in ("/playlist", "manifest")) else 0,
            5 if any(token in low_url for token in ("/hls/", "live", "stream")) else 0,
            3 if stream.get("discovery_method") == "http" else 0,
        ])
        try:
            match_score = float(stream.get("match_score", 0) or 0)
        except (TypeError, ValueError):
            match_score = 0
        try:
            provider_rank = int(priority.get(str(stream.get("provider", "")), 10_000))
        except (TypeError, ValueError):
            provider_rank = 10_000
        return (-match_score, provider_rank, -hls_hint, url)

    return sorted((stream for stream in streams if stream.get("url")), key=sort_key)

_IGNORED_DOMAINS = [
    "google-analytics.com", "googletagmanager.com", "doubleclick.net",
    "jsdelivr.net", "cdnjs.cloudflare.com", "unpkg.com", "jquery.com",
    "facebook.com", "twitter.com", "cloudflare.com", "adsco.re",
    "histats.com", "whos.amung.us", "disqus.com", "bidgear.com",
    "chatango.com", "googlesyndication.com"
]

def is_ignored_url(url: str) -> bool:
    """Checks if a URL points to third-party libraries or tracking/ad scripts."""
    low = url.lower()
    try: hostname = urllib.parse.urlsplit(url).hostname or ""
    except ValueError: hostname = ""
    if hostname and any(hostname == domain or hostname.endswith(f".{domain}") for domain in _IGNORED_DOMAINS): return True
    if low.endswith((".js", ".css", ".png", ".jpg", ".jpeg", ".svg", ".gif", ".woff", ".woff2")): return True
    return False

def clean_url(url: str) -> str:
    """Unescapes slashes and cleans up extracted URLs."""
    url = html.unescape(str(url))
    url = url.replace(r"\/", "/")
    url = url.replace(r"\u0026", "&")
    url = url.replace(r"\u002f", "/")
    url = url.replace(r"\u003d", "=")
    url = url.replace(r"\u003f", "?")
    return url.strip(' \'"\\`<>);,]')


def _resolve_stream_url(raw_url: str, referer: str = "") -> Optional[str]:
    """Resolve an extracted URL and apply the SSRF/ignored-resource filters."""
    cleaned = clean_url(raw_url)
    if not cleaned or cleaned.startswith(("#", "data:", "javascript:", "blob:")):
        return None
    if cleaned.startswith("//"):
        parsed_referer = urllib.parse.urlsplit(referer)
        if parsed_referer.scheme in {"http", "https"}:
            cleaned = f"{parsed_referer.scheme}:{cleaned}"
    elif not urllib.parse.urlsplit(cleaned).scheme:
        if not referer or not validate_http_url(referer):
            return None
        cleaned = urllib.parse.urljoin(referer, cleaned)
    safe_url = validate_http_url(cleaned)
    if not safe_url or is_ignored_url(safe_url):
        return None
    return safe_url

def unpack_js(script: str) -> str:
    """Unpacks JS obfuscated with eval(function(p,a,c,k,e,d)...) common in sports streams."""
    unpacked_script = script
    for match in re.finditer(r"}\s*\('([^']*)',\s*(\d+),\s*(\d+),\s*'([^']*)'\.split\('\|'\)", script):
        try:
            p, a, c, k = match.group(1), int(match.group(2)), int(match.group(3)), match.group(4).split('|')
            def decode_base(n, base):
                charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
                if n < base: return charset[n]
                return decode_base(n // base, base) + charset[n % base]

            unpacked = p
            for i in range(c - 1, -1, -1):
                if k[i]:
                    unpacked = re.sub(r'\\b' + decode_base(i, a) + r'\\b', k[i], unpacked)
            unpacked_script = unpacked_script.replace(match.group(0), unpacked)
        except Exception:
            continue
    return unpacked_script

def extract_streams_from_text(text: str, referer: str = "") -> List[str]:
    """
    Extracts candidate streaming URLs from arbitrary text / HTML / JS content.
    Finds:
    1. Standard .m3u8 URLs
    2. Extensionless playlist endpoints (/playlist/, /load-playlist, /manifest, /hls/)
    3. Base64 encoded URLs in atob(...) or standalone base64 strings
    4. JS player variables (source:, file:, hls.loadSource, etc.)
    """
    candidates: Set[str] = set()
    if not text:
        return []
    text = unpack_js(text[:MAX_INSPECTION_BYTES])

    def add_candidate(raw_url: str, require_stream_hint: bool = True) -> None:
        resolved = _resolve_stream_url(raw_url, referer)
        if resolved and (not require_stream_hint or any(hint in resolved.lower() for hint in _STREAM_HINTS)):
            candidates.add(resolved)

    # 1. Standard .m3u8 URLs
    for match in re.findall(r'(https?://[^\s\'"<>]+?\.m3u8[^\s\'"<>]*)', text, re.IGNORECASE):
        add_candidate(match, require_stream_hint=False)
    for match in re.findall(r'(?<![\w])((?:https?:)?//[^\s\'"<>]+?\.m3u8[^\s\'"<>]*)', text, re.IGNORECASE):
        add_candidate(match, require_stream_hint=False)
    for match in re.findall(r'(?<![\w])((?:\.\.?/|/)[^\s\'"<>]+?\.m3u8[^\s\'"<>]*)', text, re.IGNORECASE):
        add_candidate(match, require_stream_hint=False)

    # 2. Extensionless HLS playlist endpoints (/playlist/..., /load-playlist..., /manifest..., /hls/...)
    for match in re.findall(r'(https?://[^\s\'"<>]+/(?:load-playlist|playlist|manifest|hls)/[^\s\'"<>]*)', text, re.IGNORECASE):
        add_candidate(match)
    for match in re.findall(r'(?<![\w])((?:\.\.?/|/)(?:load-playlist|playlist|manifest|hls)/[^\s\'"<>]*)', text, re.IGNORECASE):
        add_candidate(match)

    # 3. Common JS video player sources (source: "...", file: "...", hls.loadSource("..."))
    js_patterns = [
        r'''(?:source|file|src|url)\s*[:=]\s*['"]([^'"]+)['"]''',
        r'''hls\.loadSource\(['"]([^'"]+)['"]\)''',
        r'''player\.src\(['"]([^'"]+)['"]\)''',
        r'''<video[^>]+src=['"]([^'"]+)['"]''',
        r'''<source[^>]+src=['"]([^'"]+)['"]''',
    ]
    for pattern in js_patterns:
        for match in re.findall(pattern, text, re.IGNORECASE):
            add_candidate(match)

    # 4. Base64 encoded strings (e.g. atob('...') or raw base64)
    atob_matches = re.findall(r'atob\([\'"]([A-Za-z0-9+/=]{16,})[\'"]\)', text)
    generic_b64 = re.findall(r'[\'"]([A-Za-z0-9+/]{24,}={0,2})[\'"]', text)
    
    for b64_str in set(atob_matches + generic_b64):
        try:
            # Pad base64 if needed
            padded = b64_str + "=" * ((4 - len(b64_str) % 4) % 4)
            decoded = base64.b64decode(padded).decode('utf-8', errors='ignore')
            add_candidate(decoded)
        except (binascii.Error, UnicodeError, ValueError):
            pass

    return list(candidates)[:MAX_EXTRACTED_STREAMS]

def extract_iframes_and_streamers(soup: BeautifulSoup, page_url: str) -> Tuple[List[str], List[str]]:
    """
    Finds embedded iframes and alternate streamer links from match pages.
    """
    iframe_urls: List[str] = []
    streamer_urls: List[str] = []

    # 1. Iframes
    for ifr in soup.find_all('iframe'):
        src = ifr.get('src') or ifr.get('data-src')
        if src and not src.startswith("about:") and not src.startswith("javascript:"):
            full_url = urllib.parse.urljoin(page_url, src)
            if validate_http_url(full_url) and not is_ignored_url(full_url) and full_url not in iframe_urls:
                iframe_urls.append(full_url)

    # 2. Streamer links in tables / buttons
    for a in soup.find_all('a', href=True):
        href = a.get('href')
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue
        full_url = urllib.parse.urljoin(page_url, href)
        if full_url == page_url or not validate_http_url(full_url) or is_ignored_url(full_url):
            continue

        low_href = full_url.lower()
        link_context = " ".join(
            part for part in (
                a.get_text(" ", strip=True),
                str(a.get("title") or ""),
                str(a.get("aria-label") or ""),
            ) if part
        ).lower()
        # Look for links to known streamer platforms
        streamer_keywords = [
            "thestreameast", "streameast", "1stream", "buffstream",
            "crackstream", "methstream", "thetvapp", "footybite",
            "topstreams", "streamed.su"
        ]
        if any(k in low_href or k in link_context for k in streamer_keywords):
            if full_url not in streamer_urls and full_url not in iframe_urls:
                streamer_urls.append(full_url)
                if len(streamer_urls) >= MAX_STREAMER_LINKS:
                    break

    return iframe_urls, streamer_urls


def _parse_iframes_and_streamers_from_html(html_text: str, page_url: str) -> Tuple[List[str], List[str]]:
    """Sync CPU work (BeautifulSoup parse + iframe/streamer-link scan) split out
    of the async code path so it can run via asyncio.to_thread instead of
    blocking the event loop that also serves live video segments."""
    soup = make_soup(html_text)
    return extract_iframes_and_streamers(soup, page_url)


async def fetch_bounded_text(
    client: httpx.AsyncClient,
    url: str,
    headers: Dict[str, str],
    timeout: float,
    max_bytes: int = MAX_INSPECTION_BYTES,
) -> Optional[str]:
    current_url = url
    for _ in range(MAX_REDIRECTS + 1):
        safe_url = await validate_http_url_async(current_url)
        if not safe_url:
            return None
        try:
            async with client.stream(
                "GET",
                safe_url,
                headers=headers,
                timeout=timeout,
                follow_redirects=False,
            ) as response:
                if 300 <= response.status_code < 400:
                    location = response.headers.get("location")
                    if not location:
                        return None
                    current_url = urllib.parse.urljoin(str(response.url), location)
                    continue
                if response.status_code != 200:
                    return None
                content_length = response.headers.get("content-length")
                try:
                    if content_length and int(content_length) > max_bytes:
                        return None
                except ValueError:
                    pass
                body = bytearray()
                async for block in response.aiter_bytes():
                    if len(body) + len(block) > max_bytes:
                        return None
                    body.extend(block)
                return bytes(body).decode(response.encoding or "utf-8", errors="replace")
        except Exception as exc:
            LOGGER.debug("Bounded upstream fetch failed error=%s", type(exc).__name__)
            return None
    return None

async def verify_stream_live(
    client: httpx.AsyncClient,
    url: str,
    referer: str = "",
    timeout: float = 5.0,
    origin: str = "",
    *,
    probe_state: Optional[dict] = None,
) -> bool:
    """Verify a stream URL and, for HLS, one actual media segment.

    probe_state is an optional caller-owned dict, one per candidate, that must
    be passed back in on every subsequent probe of the *same* candidate. It is
    used to detect a frozen live playlist (media sequence and last segment URI
    not advancing): see _playlist_is_fresh below. Pass None (the default) to
    skip freshness tracking, e.g. for a one-shot check.
    """
    if not await validate_http_url_async(url) or (referer and not await validate_http_url_async(referer)):
        return False

    headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "*/*",
    }
    if referer:
        headers["Referer"] = referer
    if origin:
        safe_origin = validate_http_url(origin)
        if safe_origin:
            parsed_origin = urllib.parse.urlsplit(safe_origin)
            headers["Origin"] = f"{parsed_origin.scheme}://{parsed_origin.netloc}"

    async def next_safe_url(current_url: str, response: httpx.Response) -> Optional[str]:
        location = response.headers.get("location")
        if not location:
            return None
        return await validate_http_url_async(urllib.parse.urljoin(str(response.url or current_url), location))

    def media_sample_is_playable(sample: bytes, content_type: str) -> bool:
        if not sample:
            return False
        content_type = content_type.lower()
        # Some providers prefix real MPEG-TS with a fake image header to dodge
        # naive hotlink/probe checks; find_ts_start skips such a prefix and
        # locates the real aligned TS sync bytes, so check it before rejecting
        # on image magic bytes.
        if find_ts_start(sample) >= 0:
            return True
        if content_type.startswith("image/") or sample.startswith((b"\x89PNG", b"\xff\xd8\xff", b"GIF8", b"RIFF")):
            return False
        if sample[:1] == b"<":
            return False
        return (
            sample[:1] == b"\x47"
            or sample.startswith((b"ftyp", b"styp", b"moof", b"ID3"))
            or any(kind in content_type for kind in ("video", "octet-stream", "audio"))
        )

    async def read_sample(current_url: str, request_headers: Dict[str, str], max_bytes: int = 64 * 1024):
        # Range is capped to match max_bytes: a 206 (or a 200 from a server
        # that ignores Range) both let us stop after max_bytes without having
        # to abort the connection mid-body. max_bytes must stay >= 64KB so a
        # sample is always large enough for find_ts_start to see past a fake
        # image-header prefix.
        range_headers = dict(request_headers)
        range_headers["Range"] = f"bytes=0-{max_bytes - 1}"
        for _ in range(MAX_REDIRECTS + 1):
            safe_url = await validate_http_url_async(current_url)
            if not safe_url:
                return None
            async with client.stream(
                "GET",
                safe_url,
                headers=range_headers,
                timeout=timeout,
                follow_redirects=False,
            ) as response:
                if 300 <= response.status_code < 400:
                    current_url = await next_safe_url(current_url, response)
                    if not current_url:
                        return None
                    continue
                if response.status_code not in (200, 206):
                    return None
                sample = bytearray()
                async for block in response.aiter_bytes(chunk_size=4096):
                    sample.extend(block)
                    if len(sample) >= max_bytes:
                        break
                return str(response.url or safe_url), response.headers.get("content-type", ""), bytes(sample)
        return None

    def _last_playlist_uri(text: str) -> str:
        """Last non-comment URI line in a playlist: the newest segment for a
        media playlist, or the last variant for a master playlist. Computed
        independently of the #EXT-X-MAP short-circuit below, which would
        otherwise pin this to a constant init-segment URI on fMP4 playlists."""
        last = ""
        for line in text.splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                last = stripped
        return last

    def _playlist_is_fresh(text: str, is_master: bool) -> bool:
        """False when a *media* playlist's sequence and last segment URI have
        gone stale: unchanged for longer than max(3 * target_duration, 20s)
        since they were first seen unchanged (i.e. since they last advanced).

        probe_state is the caller-owned, per-candidate dict passed into
        verify_stream_live; it must be handed back in on the next probe of the
        same candidate for this to detect anything. With no probe_state, or
        on a master playlist (which has no media sequence), freshness can't
        be judged, so this passes.
        """
        if probe_state is None or is_master:
            return True
        seq_match = re.search(r'#EXT-X-MEDIA-SEQUENCE:(\d+)', text)
        media_sequence = int(seq_match.group(1)) if seq_match else 0
        duration_match = re.search(r'#EXT-X-TARGETDURATION:(\d+(?:\.\d+)?)', text)
        target_duration = float(duration_match.group(1)) if duration_match else 6.0
        last_uri = _last_playlist_uri(text)

        now = time.monotonic()
        prev_sequence = probe_state.get("media_sequence")
        prev_uri = probe_state.get("last_segment_uri")

        probe_state["segment_count"] = text.count("#EXTINF")
        probe_state["target_duration"] = target_duration

        if prev_sequence is None or media_sequence != prev_sequence or last_uri != prev_uri:
            # First probe of this candidate, or the playlist advanced: (re)start
            # the clock. The first probe can't judge freshness yet, so either
            # way this passes.
            probe_state["media_sequence"] = media_sequence
            probe_state["last_segment_uri"] = last_uri
            probe_state["advanced_at"] = now
            return True

        stalled_for = now - probe_state.get("advanced_at", now)
        threshold = max(3 * target_duration, 20.0)
        return stalled_for <= threshold

    async def verify_hls_media(media_url: str, manifest_url: str, depth: int = 0) -> bool:
        result = await read_sample(media_url, headers)
        if not result:
            return False
        effective_url, content_type, sample = result
        text = sample.decode("utf-8", errors="replace")
        if sample.startswith(b"#EXTM3U") or "mpegurl" in content_type.lower():
            if depth >= 2:
                return False
            if "#EXT-X-ENDLIST" in text:
                # A finished/VOD playlist, not a live one.
                return False
            # Master playlists: the first variant is as good as any. Media playlists:
            # sample the *newest* segment - the oldest one in a live sliding window is
            # the likeliest to have already been deleted from the CDN, which made a
            # perfectly healthy stream fail its health check.
            media_uri = ""
            is_master = "#EXT-X-STREAM-INF" in text
            for line in text.splitlines():
                stripped = line.strip()
                if stripped.startswith("#EXT-X-MAP:"):
                    match = re.search(r'URI="([^"]+)"', stripped)
                    if match:
                        media_uri = match.group(1)
                        break
                if stripped and not stripped.startswith("#"):
                    media_uri = stripped
                    if is_master:
                        break
            if not media_uri:
                return False
            if not _playlist_is_fresh(text, is_master):
                return False
            next_url = await validate_http_url_async(urllib.parse.urljoin(effective_url, media_uri))
            if not next_url:
                return False
            return await verify_hls_media(next_url, effective_url, depth + 1)
        return media_sample_is_playable(sample, content_type)

    # HEAD remains useful for direct media URLs, but a playlist must be read
    # and sampled because a healthy manifest can still contain image pixels or
    # tracking requests instead of playable media. Skip it outright for a
    # .m3u8 URL, master or media: verify_hls_media always ends up doing its
    # own GET for those, so the HEAD would just be a wasted request, and
    # skipping it keeps a master URL's chain at 3 GETs total (master, media
    # playlist, segment).
    is_playlist_url = urllib.parse.urlsplit(url).path.lower().endswith(".m3u8")
    if not is_playlist_url:
        try:
            current_url = url
            for _ in range(MAX_REDIRECTS + 1):
                head_resp = await client.head(current_url, headers=headers, timeout=timeout, follow_redirects=False)
                if 300 <= head_resp.status_code < 400:
                    current_url = await next_safe_url(current_url, head_resp)
                    if not current_url:
                        break
                    continue
                if head_resp.status_code in (200, 206):
                    ctype = head_resp.headers.get("content-type", "").lower()
                    if "mpegurl" not in ctype and media_sample_is_playable(b"\x47", ctype):
                        return True
                break
        except Exception as exc:
            LOGGER.debug("Stream HEAD health check failed error=%s", type(exc).__name__)

    try:
        return await verify_hls_media(url, url)
    except Exception as exc:
        LOGGER.debug("Stream media health check failed error=%s", type(exc).__name__)
        return False

async def fetch_streams_from_page(
    client: httpx.AsyncClient,
    match_url: str,
    provider_name: str,
    match_score: int,
    match_title: str,
    max_iframe_depth: int = 2
) -> List[Dict]:
    """
    Fetches match page via fast HTTP, traverses iframes,
    extracts streams, and verifies their health.
    """
    verified_streams: List[Dict] = []
    seen_urls: Set[str] = set()

    if not validate_http_url(match_url):
        return verified_streams

    headers = {"User-Agent": DEFAULT_USER_AGENT, "Referer": match_url}

    try:
        page_html = await fetch_bounded_text(client, match_url, headers, 10.0)
        if page_html is None:
            return []
        # Direct streams on match page. Regex scanning + unpack_js over up to
        # 2MB of text is pure CPU work, so it runs off the event loop.
        direct_streams = await asyncio.to_thread(extract_streams_from_text, page_html, match_url)
        for url in direct_streams[:MAX_EXTRACTED_STREAMS]:
            if url not in seen_urls:
                seen_urls.add(url)
                if await verify_stream_live(client, url, match_url):
                    verified_streams.append({
                        "url": url,
                        "referer": match_url,
                        "provider": provider_name,
                        "match_score": match_score,
                        "match_title": match_title,
                        "discovery_method": "http"
                    })

        # Parse iframes and streamer links (BeautifulSoup parse is also CPU work).
        iframe_urls, streamer_urls = await asyncio.to_thread(
            _parse_iframes_and_streamers_from_html, page_html, match_url
        )

        # Fetch iframe pages concurrently, while bounding fan-out for providers.
        initial_urls = list(iframe_urls)
        if not initial_urls:
            initial_urls = streamer_urls[:MAX_STREAMER_LINKS]

        visited_pages: Set[str] = set()
        visited_lock = asyncio.Lock()
        fetch_semaphore = asyncio.Semaphore(8)

        async def fetch_iframe(curr_url: str, parent_ref: str, depth: int) -> None:
            if depth > max_iframe_depth:
                return

            async with visited_lock:
                if curr_url in visited_pages:
                    return
                visited_pages.add(curr_url)

            try:
                sub_headers = {"User-Agent": DEFAULT_USER_AGENT, "Referer": parent_ref}
                async with fetch_semaphore:
                    sub_html = await fetch_bounded_text(client, curr_url, sub_headers, 8.0)
                if sub_html is None:
                    return
                sub_streams = await asyncio.to_thread(extract_streams_from_text, sub_html, curr_url)
                stream_tasks = []
                for s_url in sub_streams:
                    async with visited_lock:
                        is_new_stream = s_url not in seen_urls
                        if is_new_stream:
                            seen_urls.add(s_url)
                    if is_new_stream:
                        ref_to_use = curr_url if ("gooz" in curr_url or "embed" in curr_url or "player" in curr_url) else parent_ref
                        stream_tasks.append((s_url, ref_to_use))

                async def verify_and_add(s_url: str, ref_to_use: str) -> None:
                    async with fetch_semaphore:
                        is_live = await verify_stream_live(client, s_url, ref_to_use)
                    if is_live:
                        verified_streams.append({
                            "url": s_url,
                            "referer": ref_to_use,
                            "provider": provider_name,
                            "match_score": match_score,
                            "match_title": match_title,
                            "discovery_method": "http"
                        })

                await asyncio.gather(
                    *(verify_and_add(s_url, ref_to_use) for s_url, ref_to_use in stream_tasks),
                    return_exceptions=True
                )

                if depth < max_iframe_depth:
                    nested_iframes, _ = await asyncio.to_thread(
                        _parse_iframes_and_streamers_from_html, sub_html, curr_url
                    )
                    await asyncio.gather(
                        *(fetch_iframe(n_url, curr_url, depth + 1) for n_url in nested_iframes),
                        return_exceptions=True
                    )
            except Exception as exc:
                LOGGER.warning("Stream page inspection failed provider=%s error=%s", provider_name, type(exc).__name__)

        await asyncio.gather(
            *(fetch_iframe(url, match_url, 1) for url in initial_urls),
            return_exceptions=True
        )

    except Exception as exc:
        LOGGER.warning("HTTP stream fetch failed provider=%s error=%s", provider_name, type(exc).__name__)

    return verified_streams

async def playwright_intercept_streams(
    browser: Browser,
    page_url: str,
    provider_name: str,
    match_score: int,
    match_title: str,
    referer: str = "",
    http_client: Optional[httpx.AsyncClient] = None,
) -> List[Dict]:
    """
    Playwright fallback that launches the player in a headless Chromium page
    and intercepts all network requests for .m3u8 or HLS playlists.
    """
    streams: List[Dict] = []
    captured_urls: Set[str] = set()

    try:
        if not validate_http_url(page_url):
            return streams
        async with playwright_page(browser) as page:
            async def guard_request(route, request):
                if request.url.lower().startswith(("http://", "https://")) and not validate_http_url(request.url):
                    await route.abort()
                    return
                await route.continue_()

            await page.route("**/*", guard_request)
            if referer:
                await page.set_extra_http_headers({"Referer": referer})

            def handle_request(request):
                req_url = request.url
                req_low = req_url.lower()
                if any(k in req_low for k in [".m3u8", "playlist", "load-playlist", "manifest"]):
                    if validate_http_url(req_url) and not is_ignored_url(req_url) and req_url not in captured_urls:
                        captured_urls.add(req_url)
                        streams.append({
                            "url": req_url,
                            "referer": page_url,
                            "origin": request.headers.get("origin", ""),
                            "provider": provider_name,
                            "match_score": match_score,
                            "match_title": match_title,
                            "discovery_method": "playwright"
                        })

            page.on("request", handle_request)

            try:
                await page.goto(page_url, timeout=12000, wait_until="domcontentloaded")
            except Exception as exc:
                LOGGER.debug("Playwright navigation failed provider=%s error=%s", provider_name, type(exc).__name__)

            await page.wait_for_timeout(2500)

            # Also inspect page content in case it's in DOM. Regex scanning is
            # CPU work, so it runs off the event loop.
            content = await page.content()
            for s_url in await asyncio.to_thread(extract_streams_from_text, content, page_url):
                if s_url not in captured_urls:
                    captured_urls.add(s_url)
                    streams.append({
                        "url": s_url,
                        "referer": page_url,
                        "provider": provider_name,
                        "match_score": match_score,
                        "match_title": match_title,
                        "discovery_method": "playwright"
                    })

            # Try clicking play button if no stream captured yet. The request
            # handler stays installed so post-click manifests are captured too.
            if not streams:
                play_btn = await page.query_selector("button[data-plyr='play'], .play-wrapper, .media-control, button")
                try:
                    if play_btn:
                        await play_btn.click()
                        await page.wait_for_timeout(2000)
                except Exception as exc:
                    LOGGER.debug("Playwright play-button interaction failed provider=%s error=%s", provider_name, type(exc).__name__)

        if streams:
            verify_client = http_client
            owns_client = verify_client is None
            if owns_client:
                verify_client = httpx.AsyncClient(follow_redirects=False, timeout=8.0)
            try:
                verified: List[Dict] = []
                for stream in streams:
                    if await verify_stream_live(
                        verify_client,
                        stream["url"],
                        stream.get("referer", page_url),
                        timeout=5.0,
                        origin=stream.get("origin", ""),
                    ):
                        verified.append(stream)
                streams = verified
            finally:
                if owns_client:
                    await verify_client.aclose()

    except Exception as exc:
        LOGGER.warning("Playwright stream interception failed provider=%s error=%s", provider_name, type(exc).__name__)

    return streams

