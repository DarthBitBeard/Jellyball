"""
stream_extractor.py - Universal Stream Extraction Engine
Finds HLS playlists (.m3u8, /playlist/, /load-playlist, /manifest),
decodes base64-obfuscated streams, navigates nested iframes,
and validates stream health against CDN anti-hotlinking protections.
"""

import re
import base64
import asyncio
import logging
import urllib.parse
from typing import List, Dict, Tuple, Set, Optional, Iterable
import httpx
from bs4 import BeautifulSoup
from playwright.async_api import Browser, Page

LOGGER = logging.getLogger("jellyball.stream_extractor")

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


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

# Common domains to ignore (analytics, ads, CDN libraries)
_IGNORED_DOMAINS = [
    "google-analytics.com", "googletagmanager.com", "doubleclick.net",
    "jsdelivr.net", "cdnjs.cloudflare.com", "unpkg.com", "jquery.com",
    "facebook.com", "twitter.com", "cloudflare.com", "adsco.re",
    "histats.com", "whos.amung.us", "disqus.com"
]

def is_ignored_url(url: str) -> bool:
    """Checks if a URL points to third-party libraries or tracking/ad scripts."""
    low = url.lower()
    if any(domain in low for domain in _IGNORED_DOMAINS):
        return True
    if low.endswith(".js") or low.endswith(".css") or low.endswith(".png") or low.endswith(".jpg") or low.endswith(".svg"):
        return True
    return False

def clean_url(url: str) -> str:
    """Unescapes slashes and cleans up extracted URLs."""
    url = url.replace(r"\/", "/")
    url = url.replace(r"\u0026", "&")
    url = url.strip('\'"\\`')
    return url

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

    # 1. Standard .m3u8 URLs
    for match in re.findall(r'(https?://[^\s\'"<>]+?\.m3u8[^\s\'"<>]*)', text, re.IGNORECASE):
        cleaned = clean_url(match)
        if not is_ignored_url(cleaned):
            candidates.add(cleaned)

    # 2. Extensionless HLS playlist endpoints (/playlist/..., /load-playlist..., /manifest..., /hls/...)
    for match in re.findall(r'(https?://[^\s\'"<>]+/(?:load-playlist|playlist|manifest|hls)/[^\s\'"<>]*)', text, re.IGNORECASE):
        cleaned = clean_url(match)
        if not is_ignored_url(cleaned):
            candidates.add(cleaned)

    # 3. Common JS video player sources (source: "...", file: "...", hls.loadSource("..."))
    js_patterns = [
        r'''(?:source|file|src|url)\s*[:=]\s*['"](https?://[^'"]+)['"]''',
        r'''hls\.loadSource\(['"](https?://[^'"]+)['"]\)''',
        r'''player\.src\(['"](https?://[^'"]+)['"]\)''',
        r'''<video[^>]+src=['"](https?://[^'"]+)['"]''',
        r'''<source[^>]+src=['"](https?://[^'"]+)['"]''',
    ]
    for pattern in js_patterns:
        for match in re.findall(pattern, text, re.IGNORECASE):
            cleaned = clean_url(match)
            if not is_ignored_url(cleaned):
                if any(k in cleaned.lower() for k in [".m3u8", "playlist", "manifest", "stream", "hls", "live"]):
                    candidates.add(cleaned)

    # 4. Base64 encoded strings (e.g. atob('...') or raw base64)
    atob_matches = re.findall(r'atob\([\'"]([A-Za-z0-9+/=]{16,})[\'"]\)', text)
    generic_b64 = re.findall(r'[\'"]([A-Za-z0-9+/]{24,}={0,2})[\'"]', text)
    
    for b64_str in set(atob_matches + generic_b64):
        try:
            # Pad base64 if needed
            padded = b64_str + "=" * ((4 - len(b64_str) % 4) % 4)
            decoded = base64.b64decode(padded).decode('utf-8', errors='ignore')
            if decoded.startswith("http://") or decoded.startswith("https://"):
                cleaned = clean_url(decoded)
                if not is_ignored_url(cleaned):
                    if any(k in cleaned.lower() for k in [".m3u8", "playlist", "manifest", "stream", "hls", "live"]):
                        candidates.add(cleaned)
        except Exception:
            pass

    return list(candidates)

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
            if not is_ignored_url(full_url) and full_url not in iframe_urls:
                iframe_urls.append(full_url)

    # 2. Streamer links in tables / buttons
    for a in soup.find_all('a', href=True):
        href = a.get('href')
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue
        full_url = urllib.parse.urljoin(page_url, href)
        if full_url == page_url or is_ignored_url(full_url):
            continue

        low_href = full_url.lower()
        # Look for links to known streamer platforms
        streamer_keywords = [
            "thestreameast", "streameast", "1stream", "buffstream",
            "crackstream", "methstream", "thetvapp", "footybite",
            "topstreams", "streamed.su"
        ]
        if any(k in low_href for k in streamer_keywords):
            if full_url not in streamer_urls and full_url not in iframe_urls:
                streamer_urls.append(full_url)

    return iframe_urls, streamer_urls

async def verify_stream_live(client: httpx.AsyncClient, url: str, referer: str = "", timeout: float = 5.0) -> bool:
    """
    Verifies that a stream URL is responsive and returns valid HLS/video data.
    Uses HEAD first, falling back to a Range-limited GET with proper Referer.
    """
    headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "*/*",
    }
    if referer:
        headers["Referer"] = referer

    # 1. Try HEAD request
    try:
        head_resp = await client.head(url, headers=headers, timeout=timeout)
        if head_resp.status_code == 200:
            ctype = head_resp.headers.get("content-type", "").lower()
            if any(k in ctype for k in ["mpegurl", "video", "octet-stream", "text/plain"]):
                return True
            # If Content-Type is text/html, it might be an error or captive page, fall through to GET
    except Exception as exc:
        LOGGER.debug("Stream HEAD health check failed error=%s", type(exc).__name__)

    # 2. Try GET request with stream check (reading first 1KB)
    try:
        headers["Range"] = "bytes=0-1024"
        async with client.stream("GET", url, headers=headers, timeout=timeout) as resp:
            if resp.status_code in [200, 206]:
                ctype = resp.headers.get("content-type", "").lower()
                chunk = b""
                async for block in resp.aiter_bytes():
                    chunk += block
                    if len(chunk) >= 512:
                        break

                # Check if it looks like an HLS manifest or MPEG-TS
                if chunk.startswith(b"#EXTM3U") or chunk.startswith(b"#EXTINF"):
                    return True
                if len(chunk) > 0 and chunk[0] == 0x47: # MPEG-TS sync byte
                    return True
                if any(k in ctype for k in ["mpegurl", "video"]):
                    return True
    except Exception as exc:
        LOGGER.debug("Stream GET health check failed error=%s", type(exc).__name__)

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

    headers = {"User-Agent": DEFAULT_USER_AGENT, "Referer": match_url}

    try:
        resp = await client.get(match_url, headers=headers, timeout=10.0)
        if resp.status_code != 200:
            return []

        page_html = resp.text
        # Direct streams on match page
        for url in extract_streams_from_text(page_html, match_url):
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

        # Parse iframes and streamer links
        soup = BeautifulSoup(page_html, 'html.parser')
        iframe_urls, streamer_urls = extract_iframes_and_streamers(soup, match_url)

        # Fetch iframe pages concurrently, while bounding fan-out for providers.
        initial_urls = list(iframe_urls)
        if not initial_urls:
            initial_urls = streamer_urls[:2]

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
                    sub_resp = await client.get(curr_url, headers=sub_headers, timeout=8.0)
                if sub_resp.status_code != 200:
                    return

                sub_html = sub_resp.text
                sub_streams = extract_streams_from_text(sub_html, curr_url)
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
                    sub_soup = BeautifulSoup(sub_html, 'html.parser')
                    nested_iframes, _ = extract_iframes_and_streamers(sub_soup, curr_url)
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
    referer: str = ""
) -> List[Dict]:
    """
    Playwright fallback that launches the player in a headless Chromium page
    and intercepts all network requests for .m3u8 or HLS playlists.
    """
    streams: List[Dict] = []
    page: Optional[Page] = None
    captured_urls: Set[str] = set()

    try:
        page = await browser.new_page()
        if referer:
            await page.set_extra_http_headers({"Referer": referer})

        def handle_request(request):
            req_url = request.url
            req_low = req_url.lower()
            if any(k in req_low for k in [".m3u8", "playlist", "load-playlist", "manifest"]):
                if not is_ignored_url(req_url) and req_url not in captured_urls:
                    captured_urls.add(req_url)
                    streams.append({
                        "url": req_url,
                        "referer": page_url,
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

        # Also inspect page content in case it's in DOM
        content = await page.content()
        for s_url in extract_streams_from_text(content, page_url):
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

        # Try clicking play button if no stream captured yet
        if not streams:
            play_btn = await page.query_selector("button[data-plyr='play'], .play-wrapper, .media-control, button")
            if play_btn:
                try:
                    await play_btn.click()
                    await page.wait_for_timeout(2000)
                except Exception as exc:
                    LOGGER.debug("Playwright play-button interaction failed provider=%s error=%s", provider_name, type(exc).__name__)

    except Exception as exc:
        LOGGER.warning("Playwright stream interception failed provider=%s error=%s", provider_name, type(exc).__name__)
    finally:
        if page:
            try:
                await page.close()
            except Exception as exc:
                LOGGER.debug("Playwright page cleanup failed provider=%s error=%s", provider_name, type(exc).__name__)

    return streams

