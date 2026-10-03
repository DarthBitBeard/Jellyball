"""Stream providers: the scraper classes, the provider circuit breaker, the
shared index-page cache, per-provider base-URL overrides, the shared Playwright
browser, and master_scrape() which searches every provider for one channel.
"""

import asyncio
import logging
import os
import re
import time
import urllib.parse
from collections import OrderedDict
from typing import Awaitable, Callable, Dict, List, Optional, Set, Tuple, Union

import httpx
from playwright.async_api import async_playwright, Browser, Playwright

from config import _log_failure, _validate_upstream_url, LOGGER
import provider_alerts
import provider_settings
import provider_telemetry as telemetry
import state
from db import _db_session, get_setting, get_setting_async
from network_safety import bounded_float, bounded_int, validate_http_url
from sports_matcher import canonical_team_name, clean_sports_text, get_team_search_terms, match_team
from stream_extractor import (
    DEFAULT_USER_AGENT,
    fetch_bounded_text,
    fetch_streams_from_page,
    is_ignored_url,
    make_soup,
    playwright_intercept_streams,
    playwright_page,
    playwright_pages_in_use,
    rank_streams,
    verify_stream_live,
)


_provider_priority = {
    name.strip(): index
    for index, name in enumerate(os.getenv("STREAM_PROVIDER_PRIORITY", "").split(","))
    if name.strip()
}

class BaseProvider:
    name = "Base"
    base_url = ""
    categories: List[str] = []

    def get_scan_urls(self) -> List[str]:
        if not self.base_url:
            return []
        urls = [self.base_url.rstrip("/")]
        base_url = self.base_url.rstrip("/") + "/"
        for cat in self.categories:
            url = urllib.parse.urljoin(base_url, str(cat).lstrip("/"))
            if url not in urls:
                urls.append(url)
        return urls

    async def search(
        self,
        query_or_terms,
        browser: Optional[Browser] = None,
        http_client: Optional[httpx.AsyncClient] = None
    ) -> List[dict]:
        return []


def _is_playlist_request_url(value: str) -> bool:
    parsed = urllib.parse.urlsplit(str(value or ""))
    path = parsed.path.lower()
    if is_ignored_url(value) or path.endswith((
        ".gif", ".png", ".jpg", ".jpeg", ".webp", ".js", ".css",
        ".ts", ".m4s", ".aac", ".mp4",
    )):
        return False
    return (
        path.endswith(".m3u8")
        or "/playlist/" in path
        or "/manifest" in path
        or "/load-playlist" in path
        or "/hls/" in path
        or ".m3u8" in parsed.query.lower()
    )


async def _verify_provider_streams(streams: List[dict], http_client: Optional[httpx.AsyncClient]) -> List[dict]:
    owns_client = http_client is None
    client = http_client or httpx.AsyncClient(follow_redirects=False, timeout=12.0)
    try:
        verify_semaphore = asyncio.Semaphore(VERIFY_STREAM_CONCURRENCY)

        async def verify_one(stream: dict) -> Optional[dict]:
            try:
                async with verify_semaphore:
                    is_live = await verify_stream_live(
                        client,
                        stream["url"],
                        stream.get("referer", ""),
                        timeout=8.0,
                        origin=stream.get("origin", ""),
                    )
            except Exception as exc:
                _log_failure("verify provider stream", exc)
                is_live = False
            return stream if is_live else None

        results = await asyncio.gather(*(verify_one(stream) for stream in streams), return_exceptions=True)
        return [result for result in results if isinstance(result, dict)]
    finally:
        if owns_client:
            await client.aclose()

PROVIDER_EVENT_CONCURRENCY = bounded_int(os.getenv("PROVIDER_EVENT_CONCURRENCY", "6"), 6, 1, 16)
MAX_PROVIDER_EVENTS = bounded_int(os.getenv("MAX_PROVIDER_EVENTS", "60"), 60, 1, 200)
PROVIDER_SEARCH_TIMEOUT = bounded_float(os.getenv("PROVIDER_SEARCH_TIMEOUT", "45"), 45.0, 1.0, 300.0)
PROVIDER_EVENT_TIMEOUT = bounded_float(os.getenv("PROVIDER_EVENT_TIMEOUT", "20"), 20.0, 1.0, 120.0)
PROVIDER_SEARCH_CONCURRENCY = bounded_int(os.getenv("PROVIDER_SEARCH_CONCURRENCY", "6"), 6, 1, 16)
# Caps how many of the global PROVIDER_SEARCH_CONCURRENCY slots a single team's scrape
# can hold at once, so one team querying many slow/timing-out providers can't starve
# every other team's concurrently running scrape of a slot.
PER_TEAM_PROVIDER_CONCURRENCY = bounded_int(os.getenv("PER_TEAM_PROVIDER_CONCURRENCY", "3"), 3, 1, 16)
VERIFY_STREAM_CONCURRENCY = bounded_int(os.getenv("VERIFY_STREAM_CONCURRENCY", "6"), 6, 1, 16)
PROVIDER_BREAKER_FAILURES = bounded_int(os.getenv("PROVIDER_BREAKER_FAILURES", "3"), 3, 1, 20)
PROVIDER_BREAKER_COOLDOWN = bounded_float(os.getenv("PROVIDER_BREAKER_COOLDOWN", "120"), 120.0, 5.0, 3600.0)
MAX_STREAM_CANDIDATES = bounded_int(os.getenv("MAX_STREAM_CANDIDATES", "24"), 24, 1, 200)
_PROVIDER_SEARCH_SEMAPHORE: Optional[asyncio.Semaphore] = None
_PROVIDER_SEARCH_LOOP = None
_PROVIDER_BREAKERS: Dict[str, dict] = {}


def _provider_breaker_open(provider: str) -> bool:
    record = _PROVIDER_BREAKERS.get(provider)
    if not record or record["failures"] < PROVIDER_BREAKER_FAILURES:
        return False
    if time.monotonic() - record["opened_at"] >= PROVIDER_BREAKER_COOLDOWN:
        return False
    return True


def _provider_breaker_success(provider: str) -> None:
    _PROVIDER_BREAKERS.pop(provider, None)


def _provider_breaker_failure(provider: str) -> None:
    record = _PROVIDER_BREAKERS.setdefault(provider, {"failures": 0, "opened_at": 0.0})
    record["failures"] += 1
    if record["failures"] >= PROVIDER_BREAKER_FAILURES:
        record["opened_at"] = time.monotonic()


def provider_breaker_snapshot() -> Dict[str, dict]:
    """Public view of circuit-breaker state for /metrics and dashboards."""
    return {
        name: {
            "failures": int(record.get("failures", 0)),
            "open": _provider_breaker_open(name),
        }
        for name, record in _PROVIDER_BREAKERS.items()
    }


def _provider_search_semaphore() -> asyncio.Semaphore:
    global _PROVIDER_SEARCH_SEMAPHORE, _PROVIDER_SEARCH_LOOP
    loop = asyncio.get_running_loop()
    if _PROVIDER_SEARCH_SEMAPHORE is None or _PROVIDER_SEARCH_LOOP is not loop:
        _PROVIDER_SEARCH_SEMAPHORE = asyncio.Semaphore(PROVIDER_SEARCH_CONCURRENCY)
        _PROVIDER_SEARCH_LOOP = loop
    return _PROVIDER_SEARCH_SEMAPHORE


# --- Shared TTL cache + single-flight for aggregator index/category pages -----
# Every team's scrape cycle calls the same handful of ACTIVE_PROVIDERS, each of
# which re-fetches (and, on a Cloudflare block, re-renders via Playwright) the
# exact same index/category URLs. With dozens of teams configured this repeats
# the same network fetch (and possibly a full Chromium render) many times a
# minute for content that changes at most once a minute. Caching the raw HTML
# per URL, with single-flight de-duplication for concurrent callers, collapses
# all of that down to one fetch per URL per TTL window.
SCRAPE_INDEX_CACHE_SECONDS = bounded_float(os.getenv("SCRAPE_INDEX_CACHE_SECONDS", "60"), 60.0, 0.0, 3600.0)
_SCRAPE_INDEX_CACHE_MAX_ENTRIES = 64
# Failures (including "not found") are cached only briefly, so a transient
# outage doesn't leave every team blind for a full cache window.
_SCRAPE_INDEX_FAILURE_CACHE_SECONDS = min(5.0, SCRAPE_INDEX_CACHE_SECONDS) if SCRAPE_INDEX_CACHE_SECONDS > 0 else 0.0
_SCRAPE_INDEX_CACHE: "OrderedDict[str, tuple[float, Optional[str]]]" = OrderedDict()
_SCRAPE_INDEX_INFLIGHT: Dict[str, asyncio.Future] = {}
_SCRAPE_INDEX_INFLIGHT_LOOP = None


async def _get_cached_index_html(url: str, fetcher) -> Optional[str]:
    """Return `url`'s index-page HTML from the shared TTL cache, or run
    `fetcher()` (an async, argument-less callable that performs the actual
    fetch, HTTP and/or Playwright fallback) once per TTL window/failure window,
    sharing the in-flight call across any other team requesting the same URL
    at the same time.
    """
    global _SCRAPE_INDEX_INFLIGHT_LOOP

    now = time.monotonic()
    cached = _SCRAPE_INDEX_CACHE.get(url)
    if cached is not None:
        expires_at, cached_html = cached
        if now < expires_at:
            _SCRAPE_INDEX_CACHE.move_to_end(url)
            return cached_html
        _SCRAPE_INDEX_CACHE.pop(url, None)

    loop = asyncio.get_running_loop()
    if _SCRAPE_INDEX_INFLIGHT_LOOP is not loop:
        # A fresh loop (e.g. a new test run) can't share futures created on a
        # prior, now-closed loop.
        _SCRAPE_INDEX_INFLIGHT.clear()
        _SCRAPE_INDEX_INFLIGHT_LOOP = loop

    existing = _SCRAPE_INDEX_INFLIGHT.get(url)
    if existing is not None:
        return await asyncio.shield(existing)

    future: asyncio.Future = loop.create_future()
    _SCRAPE_INDEX_INFLIGHT[url] = future
    html_text: Optional[str] = None
    try:
        try:
            html_text = await fetcher()
        except Exception as exc:
            _log_failure(f"fetch index page {url}", exc)
            telemetry.report_page_error(exc)
            html_text = None
        ttl = SCRAPE_INDEX_CACHE_SECONDS if html_text is not None else _SCRAPE_INDEX_FAILURE_CACHE_SECONDS
        if ttl > 0:
            _SCRAPE_INDEX_CACHE[url] = (time.monotonic() + ttl, html_text)
            _SCRAPE_INDEX_CACHE.move_to_end(url)
            while len(_SCRAPE_INDEX_CACHE) > _SCRAPE_INDEX_CACHE_MAX_ENTRIES:
                _SCRAPE_INDEX_CACHE.popitem(last=False)
        return html_text
    finally:
        # Resolve the future even on cancellation so any other team waiting on
        # this URL isn't left hanging until its own provider-level timeout.
        if not future.done():
            future.set_result(html_text)
        if _SCRAPE_INDEX_INFLIGHT.get(url) is future:
            _SCRAPE_INDEX_INFLIGHT.pop(url, None)


def _invalidate_index_cache_for_prefix(url_prefix: str) -> None:
    """Drop any cached or in-flight aggregator index-page fetch whose URL
    starts with `url_prefix`, so a provider base-URL change doesn't keep
    serving HTML fetched from the old domain out of the shared TTL cache."""
    if not url_prefix:
        return
    for cache_key in [key for key in _SCRAPE_INDEX_CACHE if key.startswith(url_prefix)]:
        _SCRAPE_INDEX_CACHE.pop(cache_key, None)
    for cache_key in [key for key in _SCRAPE_INDEX_INFLIGHT if key.startswith(url_prefix)]:
        _SCRAPE_INDEX_INFLIGHT.pop(cache_key, None)


# --- Per-provider base-URL overrides (D1) ---------------------------------
# Aggregator domains change often; today fixing one means editing .env and
# restarting the service. Every HtmlAggregatorScraper subclass still has an
# env-var default (passed into __init__ below), but a DB override stored
# under app_settings key "provider_url:<ProviderName>" can replace it live.
# Scrapers read the CURRENT value through the `base_url` property, backed by
# this module-level dict -- refreshed from the DB once at startup (see
# `_load_provider_url_overrides`, called from `init_db`) and updated again,
# in-process, whenever the dashboard's Provider Domains card saves a change
# (see `_set_provider_url_override`) -- rather than doing a DB read per
# request on the event loop.
_PROVIDER_BASE_URL_OVERRIDES: Dict[str, str] = {}


def _provider_url_setting_key(provider_name: str) -> str:
    return f"provider_url:{provider_name}"


def _set_provider_url_override(provider: "HtmlAggregatorScraper", url: str) -> None:
    """Live-apply a provider base-URL change: update the in-memory override
    dict that the `base_url` property reads, and invalidate any index pages
    cached under a base URL that's no longer current (the old override/default
    and, defensively, the new one) so the next search re-fetches fresh HTML."""
    previous_effective = provider.base_url
    if url:
        _PROVIDER_BASE_URL_OVERRIDES[provider.name] = url
    else:
        _PROVIDER_BASE_URL_OVERRIDES.pop(provider.name, None)
    new_effective = provider.base_url
    for stale in {previous_effective, new_effective}:
        if stale:
            _invalidate_index_cache_for_prefix(stale.rstrip("/"))


def _load_provider_url_overrides() -> None:
    """Populate `_PROVIDER_BASE_URL_OVERRIDES` from `app_settings` once at
    startup (called from `init_db`). One indexed SELECT per provider; never
    called on the request path. Re-callable/idempotent: a provider with no
    stored override has any stale in-memory entry cleared too, which is what
    keeps tests that reinitialize a fresh temp DB isolated from each other."""
    for provider in ACTIVE_PROVIDERS:
        if not isinstance(provider, HtmlAggregatorScraper):
            continue
        stored = get_setting(_provider_url_setting_key(provider.name), "")
        if stored:
            _PROVIDER_BASE_URL_OVERRIDES[provider.name] = stored
        else:
            _PROVIDER_BASE_URL_OVERRIDES.pop(provider.name, None)
    provider_settings.load()


class HtmlAggregatorScraper(BaseProvider):
    """Configurable adapter for aggregators that expose linked event pages."""

    def __init__(self, name: str, base_url: str, categories: Optional[List[str]] = None, event_path_hints: Optional[List[str]] = None):
        self.name = name.strip() or "Aggregator"
        self._default_base_url = (base_url or "").strip().rstrip("/")
        self.categories = list(categories or [])
        self.event_path_hints = tuple(hint.lower() for hint in (event_path_hints or []) if hint)

    @property
    def base_url(self) -> str:
        """The provider's current base URL: a DB override (set via the
        dashboard's Provider Domains card / POST /settings/providers) if one
        is active, otherwise the env-var default this scraper was constructed
        with. Read fresh on every access from a module-level dict that's kept
        in sync at startup and on save -- never a DB read at search time."""
        return _PROVIDER_BASE_URL_OVERRIDES.get(self.name) or self._default_base_url

    @base_url.setter
    def base_url(self, value: str) -> None:
        self._default_base_url = (value or "").strip().rstrip("/")

    @staticmethod
    def _anchor_context(anchor) -> str:
        parts = [
            anchor.get_text(" ", strip=True),
            str(anchor.get("title") or ""),
            str(anchor.get("aria-label") or ""),
        ]
        parent = anchor.parent
        if parent is not None:
            parts.append(parent.get_text(" ", strip=True))
        return " ".join(part for part in parts if part)[:600]

    def _is_event_link(self, href: str, page_url: str) -> bool:
        if not href or href.startswith(("#", "javascript:", "mailto:")):
            return False
        candidate = urllib.parse.urljoin(page_url, href)
        if not _validate_upstream_url(candidate):
            return False
        base_host = urllib.parse.urlparse(self.base_url).netloc.lower()
        if urllib.parse.urlparse(candidate).netloc.lower() != base_host:
            return False
        if candidate.rstrip("/") == page_url.rstrip("/"):
            return False
        if self.event_path_hints and not any(hint in candidate.lower() for hint in self.event_path_hints):
            return False
        return True

    async def _fetch_html(
        self,
        client: httpx.AsyncClient,
        page_url: str,
        browser: Optional[Browser],
        *,
        settle_seconds: float = 0,
        goto_timeout: int = 35000,
        networkidle_timeout: int = 8000,
    ) -> Optional[str]:
        """Fetch one page: fast HTTP first, falling back to a Playwright-
        rendered page when the site is behind a Cloudflare check (or otherwise
        returns nothing usable over plain HTTP). Shared by every
        HtmlAggregatorScraper subclass so this HTTP->Playwright fallback isn't
        reimplemented per provider.

        `settle_seconds`, when set, waits a fixed amount of time after
        navigation instead of waiting for the network to go idle -- some sites
        (e.g. DaddyLive's channel directory) never reach a quiet "networkidle"
        state.
        """
        page_html = await fetch_bounded_text(
            client,
            page_url,
            headers={"User-Agent": DEFAULT_USER_AGENT, "Referer": self.base_url},
            timeout=8.0,
        )
        if not page_html and browser and browser.is_connected():
            try:
                async with playwright_page(browser, user_agent=DEFAULT_USER_AGENT) as page:
                    await page.goto(page_url, wait_until="domcontentloaded", timeout=goto_timeout)
                    if settle_seconds:
                        await asyncio.sleep(settle_seconds)
                    else:
                        try:
                            await page.wait_for_load_state("networkidle", timeout=networkidle_timeout)
                        except Exception:
                            pass
                    page_html = await page.content()
            except Exception as exc:
                _log_failure(f"{self.name} Cloudflare bypass for {page_url}", exc)
        return page_html

    async def _fetch_index_page(self, client: httpx.AsyncClient, page_url: str, browser: Optional[Browser]) -> Optional[str]:
        """Fetch one index/category page: fast HTTP first, falling back to a
        Playwright-rendered page when the site is behind a Cloudflare check."""
        return await self._fetch_html(client, page_url, browser)

    def _parse_matches_from_html(
        self,
        page_html: str,
        page_url: str,
        search_terms: List[str],
        matches: List[tuple[str, int, str]],
        seen_matches: Set[str],
    ) -> None:
        """Pure CPU work (BeautifulSoup parse + per-anchor fuzzy matching),
        split out so it can run in a worker thread via asyncio.to_thread instead
        of blocking the shared event loop that also serves live video segments.
        Mutates `matches`/`seen_matches` in place to preserve the original
        cross-page, cumulative MAX_PROVIDER_EVENTS cutoff.
        """
        soup = make_soup(page_html)
        listed: Set[str] = set()
        try:
            self._parse_anchors(soup, page_url, search_terms, matches, seen_matches, listed)
        finally:
            telemetry.report_index_events(len(listed))

    def _parse_anchors(self, soup, page_url, search_terms, matches, seen_matches, listed: Set[str]) -> None:
        for anchor in soup.find_all("a", href=True):
            href = str(anchor.get("href") or "")
            if not self._is_event_link(href, page_url):
                continue
            match_url = urllib.parse.urljoin(page_url, href)
            listed.add(match_url)
            if match_url in seen_matches:
                continue
            title = str(anchor.get("title") or "")
            raw_text = anchor.get_text(" ", strip=True)
            direct_text = " ".join(
                part for part in (raw_text, title, str(anchor.get("aria-label") or ""), href) if part
            )
            matched, score, _ = match_team(
                search_terms,
                direct_text,
                href=href,
                title=title,
            )
            if not matched:
                # Some providers put the team names in the card rather
                # than the anchor. Only accept that fallback for a
                # high-confidence full identity, not a generic nickname.
                candidate_text = self._anchor_context(anchor) or href
                context_matched, context_score, _ = match_team(search_terms, candidate_text, href=href, title=title)
                if context_matched and context_score >= 110:
                    matched, score = context_matched, context_score
            if matched:
                seen_matches.add(match_url)
                matches.append((match_url, score, raw_text or title or match_url))
                if len(matches) >= MAX_PROVIDER_EVENTS:
                    return

    async def _find_matches_using(
        self,
        client: httpx.AsyncClient,
        search_terms: List[str],
        browser: Optional[Browser],
        *,
        fetch_page,
        parse_matches,
        catch_page_errors: bool = True,
    ) -> List[tuple[str, int, str]]:
        """Shared skeleton behind `_find_matches`: fetch (through the shared
        TTL cache) and parse each of this provider's scan URLs, stopping once
        MAX_PROVIDER_EVENTS matches have accumulated. `fetch_page` and
        `parse_matches` let each subclass keep its own fetch-fallback timing
        and anchor-parsing rules while sharing this loop. `catch_page_errors`
        preserves each subclass's original error-handling: some log and move
        on to the next scan URL, others let the exception propagate."""
        matches: List[tuple[str, int, str]] = []
        seen_matches: Set[str] = set()

        async def process(page_url: str) -> None:
            page_html = await _get_cached_index_html(page_url, lambda pu=page_url: fetch_page(client, pu, browser))
            if not page_html:
                telemetry.report_page_error("no_content")
                return
            await asyncio.to_thread(parse_matches, page_html, page_url, search_terms, matches, seen_matches)

        for page_url in self.get_scan_urls():
            if catch_page_errors:
                try:
                    await process(page_url)
                except Exception as exc:
                    _log_failure(f"scan provider={self.name} page", exc)
                    telemetry.report_page_error(exc)
            else:
                await process(page_url)
            if len(matches) >= MAX_PROVIDER_EVENTS:
                break
        telemetry.report_matches(len(matches))
        return matches

    async def _find_matches(self, client: httpx.AsyncClient, search_terms: List[str], browser: Optional[Browser] = None) -> List[tuple[str, int, str]]:
        return await self._find_matches_using(
            client,
            search_terms,
            browser,
            fetch_page=self._fetch_index_page,
            parse_matches=self._parse_matches_from_html,
            catch_page_errors=True,
        )

    async def search(
        self,
        query_or_terms,
        browser: Optional[Browser] = None,
        http_client: Optional[httpx.AsyncClient] = None,
    ) -> List[dict]:
        if not self.base_url or not _validate_upstream_url(self.base_url):
            return []
        search_terms = (
            get_team_search_terms(query_or_terms, query_or_terms)
            if isinstance(query_or_terms, str)
            else list(query_or_terms or [])
        )
        owns_client = http_client is None
        client = http_client or httpx.AsyncClient(follow_redirects=True, timeout=12.0)
        try:
            # Pass the browser object explicitly to the updated _find_matches method
            matched_events = await self._find_matches(client, search_terms, browser=browser)
            event_semaphore = asyncio.Semaphore(PROVIDER_EVENT_CONCURRENCY)

            async def inspect_event(event: tuple[str, int, str]) -> List[dict]:
                match_url, score, match_title = event
                async with event_semaphore:
                    async def inspect() -> List[dict]:
                        event_streams = await fetch_streams_from_page(
                            client, match_url, self.name, score, match_title
                        )
                        for stream in event_streams:
                            stream.setdefault("match_url", match_url)
                        if not event_streams and browser and browser.is_connected():
                            event_streams = await playwright_intercept_streams(
                                browser, match_url, self.name, score, match_title, http_client=client
                            )
                            for stream in event_streams:
                                stream.setdefault("match_url", match_url)
                        return event_streams

                    try:
                        return await asyncio.wait_for(inspect(), timeout=PROVIDER_EVENT_TIMEOUT)
                    except asyncio.TimeoutError:
                        LOGGER.warning("Provider event timed out provider=%s url_host=%s", self.name, urllib.parse.urlparse(match_url).netloc)
                        return []
                    except Exception as exc:
                        _log_failure(f"inspect provider={self.name} event", exc)
                        return []

            results = await asyncio.wait_for(
                asyncio.gather(
                    *(inspect_event(event) for event in matched_events),
                    return_exceptions=True,
                ),
                timeout=PROVIDER_SEARCH_TIMEOUT,
            )
            return [stream for result in results if isinstance(result, list) for stream in result]
        except asyncio.TimeoutError:
            LOGGER.warning("Provider search timed out provider=%s", self.name)
            return []
        finally:
            if owns_client:
                await client.aclose()


# --- Hybrid fast-HTTP and Playwright scrapers ---
class ISportSurgeScraper(HtmlAggregatorScraper):
    def __init__(self):
        super().__init__(
            "iSportSurge",
            os.getenv("AGGREGATOR_1_URL", "https://isportsurge.ws"),
            [
                "/cfb/livestreams2", "/nfl/livestreams3", "/mlb/livestreams2",
                "/nba/livestreams3", "/nhl/livestreams3", "/soccer/livestreams",
            ],
            ["/watch/", "/event/", "/title-game/"],
        )

class MyBuffStreamsScraper(HtmlAggregatorScraper):
    def __init__(self):
        super().__init__(
            "MyBuffStreams",
            os.getenv("AGGREGATOR_2_URL", "https://mybuffstreams.plus"),
            [
                "/cfbstreams2", "/nflstreams2", "/mlb-live-streams",
                "/nbastreams2", "/nhlstreams2", "/soccer-live-streams",
            ],
            ["/cfb/", "/mlb/", "/nfl/", "/nba/", "/nhl/", "/title-game/", "/watch/", "/soccer/"],
        )


class MethStreamsScraper(HtmlAggregatorScraper):
    def __init__(self):
        super().__init__(
            "MethStreams",
            os.getenv("AGGREGATOR_3_URL", "https://methstreams.click"),
            [],
            ["/game/", "/match/", "/live/"],
        )


class StreamEastScraper(HtmlAggregatorScraper):
    def __init__(self):
        super().__init__(
            "StreamEast",
            os.getenv("AGGREGATOR_4_URL", "https://thestreameast.top"),
            [],
            ["/stream/", "/match/", "/live/"],
        )
class FootybiteScraper(HtmlAggregatorScraper):
    def __init__(self):
        super().__init__(
            "Footybite",
            os.getenv("AGGREGATOR_9_URL", "https://footybite.im"),
            [],
            ["/watch/", "/stream/", "/live/", "/match/"],
        )


class OneStreamScraper(HtmlAggregatorScraper):
    def __init__(self):
        super().__init__(
            "1Stream",
            os.getenv("AGGREGATOR_10_URL", "https://1stream.ws"),
            [],
            ["/match/", "/stream/", "/live/"],
        )


class StreamedSuScraper(HtmlAggregatorScraper):
    def __init__(self):
        super().__init__(
            "Streamed",
            os.getenv("AGGREGATOR_11_URL", "https://streamed.su"),
            ["/category/football", "/category/american-football", "/category/basketball", "/category/baseball", "/category/hockey"],
            ["/watch/", "/live/"],
        )


class TopStreamsScraper(HtmlAggregatorScraper):
    def __init__(self):
        super().__init__(
            "TopStreams",
            os.getenv("AGGREGATOR_12_URL", "https://topstreams.info"),
            ["/nfl", "/nba", "/nhl", "/mlb", "/soccer"],
            ["/watch/", "/match/", "/live/"],
        )


_M3U_ATTR_RE = re.compile(r'([a-zA-Z0-9_-]+)="([^"]*)"')


def _parse_m3u_playlist(text: str) -> List[dict]:
    """Parse a #EXTM3U playlist into entries with tvg_id/tvg_name/group_title/url.
    Deliberately minimal (no external M3U library) since only a handful of
    #EXTINF attributes are needed here."""
    entries: List[dict] = []
    pending: Optional[dict] = None
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#EXTINF"):
            attrs = dict(_M3U_ATTR_RE.findall(line))
            display_name = line.rsplit(",", 1)[-1].strip() if "," in line else ""
            pending = {
                "tvg_id": attrs.get("tvg-id", ""),
                "tvg_name": attrs.get("tvg-name", "") or display_name,
                "group_title": attrs.get("group-title", ""),
                "display_name": display_name,
            }
        elif line.startswith("#"):
            continue
        elif pending is not None:
            pending["url"] = line
            entries.append(pending)
            pending = None
    return entries


IPTV_ORG_PLAYLIST_URL_DEFAULT = "https://iptv-org.github.io/iptv/categories/sports.m3u"
IPTV_ORG_REFRESH_SECONDS = bounded_float(os.getenv("IPTV_ORG_REFRESH_SECONDS", "21600"), 21600.0, 300.0, 86400.0)
_IPTV_ORG_CACHE: List[dict] = []
_IPTV_ORG_CACHE_LOADED_AT = 0.0
_IPTV_ORG_LOCK = asyncio.Lock()


class IptvOrgScraper(BaseProvider):
    """Curated, static free-to-air playlist (iptv-org) used as a structurally
    independent backup for always-live special channels: a plain cached HTTP
    fetch with no scraping and no Cloudflare exposure, so it survives failure
    modes (anti-bot changes, mass site outages) that could take out every
    HTML-scraping provider in ACTIVE_PROVIDERS at once. Only useful for 24/7
    linear channels (ESPN, FS1, NFL Network, ...) — it carries no per-game team
    broadcasts, so it belongs in LINEAR_PROVIDERS, not the general team search."""

    name = "IPTV-Org"

    def __init__(self):
        self.base_url = os.getenv("IPTV_ORG_PLAYLIST_URL", IPTV_ORG_PLAYLIST_URL_DEFAULT)

    async def _get_entries(self, http_client: Optional[httpx.AsyncClient]) -> List[dict]:
        global _IPTV_ORG_CACHE, _IPTV_ORG_CACHE_LOADED_AT
        now = time.monotonic()
        if _IPTV_ORG_CACHE and now - _IPTV_ORG_CACHE_LOADED_AT < IPTV_ORG_REFRESH_SECONDS:
            return _IPTV_ORG_CACHE

        async with _IPTV_ORG_LOCK:
            now = time.monotonic()
            if _IPTV_ORG_CACHE and now - _IPTV_ORG_CACHE_LOADED_AT < IPTV_ORG_REFRESH_SECONDS:
                return _IPTV_ORG_CACHE

            url = _validate_upstream_url(self.base_url)
            if not url:
                return _IPTV_ORG_CACHE

            owns_client = http_client is None
            client = http_client or httpx.AsyncClient(timeout=15.0, follow_redirects=True)
            try:
                resp = await client.get(url, headers={"User-Agent": DEFAULT_USER_AGENT})
                if resp.status_code == 200:
                    _IPTV_ORG_CACHE = _parse_m3u_playlist(resp.text)
                    _IPTV_ORG_CACHE_LOADED_AT = now
            except Exception as exc:
                _log_failure("fetch iptv-org playlist", exc)
            finally:
                if owns_client:
                    await client.aclose()
            return _IPTV_ORG_CACHE

    async def search(self, query_or_terms, browser: Optional[Browser] = None, http_client: Optional[httpx.AsyncClient] = None) -> List[dict]:
        search_terms = list(query_or_terms) if isinstance(query_or_terms, list) else [str(query_or_terms)]
        entries = await self._get_entries(http_client)
        if not entries:
            telemetry.report_page_error("no_content")
            return []
        telemetry.report_index_events(len(entries))

        candidates = []
        for entry in entries:
            name_text = f"{entry.get('tvg_name', '')} {entry.get('display_name', '')}"
            if not _channel_term_matches(name_text, search_terms):
                continue
            url = _validate_upstream_url(entry.get("url", ""))
            if not url:
                continue
            candidates.append({
                "url": url,
                "referer": "",
                "origin": "",
                "provider": self.name,
                # match_team() scores a confident exact-word match 95-100 (110 only for
                # multi-word phrases); _channel_term_matches() above is at least that
                # precise — it's a word-boundary match against curated official channel
                # names with explicit ESPN/ESPN2/ESPN+ disambiguation, not noisy fuzzy
                # anchor text — so this shouldn't be scored as a weaker match than that.
                "match_score": 100,
                "match_title": entry.get("tvg_name") or entry.get("display_name") or "",
                "discovery_method": "http",
            })

        telemetry.report_matches(len(candidates))
        if not candidates:
            return []
        return await _verify_provider_streams(candidates, http_client)


class TheTVAppScraper(HtmlAggregatorScraper):
    def __init__(self):
        super().__init__(
            "TheTVApp", 
            os.getenv("AGGREGATOR_5_URL", "https://thetvapp67.com"), 
            ["/tv/"], 
            ["/tv/", "/watch/", "/channel/", "/sports-channels/"]
        )

    def _find_best_channel_match(
        self, html_text: str, raw_terms_lower: List[str], search_terms: List[str]
    ) -> tuple[Optional[str], int, str]:
        """Pure CPU work (BeautifulSoup parse + per-anchor fuzzy matching over
        every listed channel), split out so it can run via asyncio.to_thread
        instead of blocking the shared event loop."""
        soup = make_soup(html_text)
        best_url, best_score, best_title = None, 0, ""
        listed = 0

        for anchor in soup.find_all("a", href=True):
            href = str(anchor.get("href") or "")
            if self._is_event_link(href, self.base_url + "/tv/"):
                listed += 1
            text = self._anchor_context(anchor) or anchor.get_text(" ", strip=True)
            text_lower = text.lower()

            # Direct linear network match override (e.g., ESPN, RedZone, FS1)
            matched = False
            score = 0
            for rt in raw_terms_lower:
                if _channel_term_matches(text_lower, [rt]) or _channel_term_matches(href, [rt]):
                    matched = True
                    score = 120
                    break

            # Fallback to standard team matcher if direct string isn't found
            if not matched:
                matched, score, _ = match_team(search_terms, text, href=href)

            if matched and not _is_non_english_channel(text):
                # Prefer the most specific matching channel when a page
                # contains both a base network and numbered variants.
                specificity = max(
                    (len(str(term).split()) * 10 + len(str(term)) for term in raw_terms_lower if _channel_term_matches(text, [term])),
                    default=0,
                )
                ranked_score = score + specificity
            else:
                ranked_score = 0

            if ranked_score > best_score:
                best_url = urllib.parse.urljoin(self.base_url, href)
                best_score = ranked_score
                best_title = text

        telemetry.report_index_events(listed)
        telemetry.report_matches(1 if best_url else 0)
        return best_url, best_score, best_title

    async def search(self, query_or_terms, browser: Optional[Browser] = None, http_client: Optional[httpx.AsyncClient] = None) -> List[dict]:
        if not browser or not browser.is_connected():
            return []

        search_terms = get_team_search_terms(query_or_terms, query_or_terms) if isinstance(query_or_terms, str) else list(query_or_terms or [])
        raw_terms_lower = [t.lower() for t in (query_or_terms if isinstance(query_or_terms, list) else [query_or_terms])]
        streams = []

        try:
            async with playwright_page(browser, user_agent=DEFAULT_USER_AGENT) as page:
                target_url = f"{self.base_url}/tv/"
                try:
                    await page.goto(target_url, wait_until="domcontentloaded", timeout=25000)
                    try:
                        await page.wait_for_load_state("networkidle", timeout=8000)
                    except Exception:
                        pass
                except Exception as exc:
                    _log_failure("TheTVApp channel list load", exc)
                    telemetry.report_page_error(exc)

                html = await page.content()
                if "just a moment" in html.lower() or "cf-browser-verification" in html.lower():
                    telemetry.report_page_error("blocked")
                    return []

                best_url, best_score, best_title = await asyncio.to_thread(
                    self._find_best_channel_match, html, raw_terms_lower, search_terms
                )

                if best_url:
                    def handle_request(request):
                        req_url = request.url
                        if _is_playlist_request_url(req_url) and validate_http_url(req_url):
                            streams.append({
                                "url": req_url,
                                "referer": request.headers.get("referer", best_url),
                                "origin": request.headers.get("origin", ""),
                                "provider": self.name,
                                "match_score": best_score,
                                "match_title": best_title,
                                "discovery_method": "playwright"
                            })

                    page.on("request", handle_request)

                    try:
                        await page.goto(best_url, wait_until="domcontentloaded", timeout=25000)
                        try:
                            await page.wait_for_load_state("networkidle", timeout=8000)
                        except Exception:
                            pass
                        await page.mouse.click(400, 300)
                        try:
                            await page.wait_for_load_state("networkidle", timeout=5000)
                        except Exception:
                            pass
                    except Exception:
                        pass
        except Exception as exc:
            _log_failure("TheTVApp custom extraction", exc)
            telemetry.report_page_error(exc)

        seen = set()
        deduped = [s for s in streams if s["url"] not in seen and not seen.add(s["url"])]
        return await _verify_provider_streams(deduped[:MAX_STREAM_CANDIDATES], http_client)


# Region tags that mark a DaddyLive channel listing as non-English. English
# feeds are tagged "USA"; anything carrying one of these suffixes is excluded.
_NON_ENGLISH_CHANNEL_TAGS = frozenset({
    "de", "fr", "es", "it", "pt", "brasil", "brazil", "argentina", "mexico",
    "colombia", "chile", "peru", "ecuador", "uruguay", "paraguay", "bolivia",
    "venezuela", "deportes", "latino", "latin", "espanol", "spanish", "arab",
    "arabic", "turkiye", "turkish", "poland", "polska", "greece", "greek",
    "romania", "hungary", "czech", "slovakia", "bulgaria", "croatia", "serbia",
    "russia", "ukraine", "india", "hindi", "pakistan", "indonesia", "malaysia",
    "thailand", "vietnam", "philippines", "china", "korea", "japan", "australia",
    "canada", "ireland", "belgium", "switzerland", "austria", "sweden", "norway",
    "denmark", "finland", "iceland", "israel", "hebrew", "africa", "nigeria",
    "egypt", "saudi", "uae", "qatar", "caribbean",
})
_NON_ENGLISH_CHANNEL_CODES = frozenset({
    "nl", "de", "fr", "es", "it", "pt", "br", "tr", "pl", "gr", "ro", "hu",
    "cz", "sk", "bg", "hr", "rs", "ru", "ua", "pk", "id", "my", "th", "vn",
    "ph", "cn", "hk", "tw", "kr", "jp", "au", "nz", "ie", "be", "ch", "at",
    "se", "no", "dk", "fi", "is", "il", "za", "ng", "eg", "sa", "ae", "qa",
    "mx", "ar", "cl", "co", "pe", "ve", "uy",
})


# D6: lets an operator extend the non-English tag list without a code change
# (e.g. a new aggregator's own regional suffixes). Comma-separated, additive —
# the built-in defaults above are never replaced, only extended.
_EXTRA_NON_ENGLISH_MARKERS = frozenset(
    token.strip().casefold()
    for token in os.getenv("EXTRA_NON_ENGLISH_MARKERS", "").split(",")
    if token.strip()
)


def _is_non_english_channel(text: str) -> bool:
    """Return True when a channel listing carries an explicit non-English tag."""
    value = str(text or "").casefold()
    tokens = [token for token in re.split(r"[^a-z]+", value) if token]
    if any(token in _NON_ENGLISH_CHANNEL_TAGS or token in _EXTRA_NON_ENGLISH_MARKERS for token in tokens):
        return True
    # Extra markers are also checked as plain substrings (not just whole
    # tokens), so a multi-word or punctuated marker still matches.
    if any(marker in value for marker in _EXTRA_NON_ENGLISH_MARKERS):
        return True
    # Two-letter region codes are accepted only as explicit suffixes or inside
    # delimiters. This avoids treating ordinary words such as "in" as regions.
    code_pattern = r"(?:^|[\s(\[/_|-])(" + "|".join(sorted(_NON_ENGLISH_CHANNEL_CODES, key=len, reverse=True)) + r")(?:$|[\s)\]/_|-])"
    return re.search(code_pattern, value) is not None


_DISALLOWED_CHANNEL_SUBSTRINGS = {
    "fox": ("sports", "news", "business", "weather", "cricket", "soccer", "deportes", "league", "hd bulgaria"),
    "cbs": ("sports", "golazo", "news"),
    "nbc": ("sports", "news", "universo"),
    "abc": ("news",),
}


def _channel_term_matches(text: str, terms: List[str]) -> bool:
    """Match a channel name without allowing ESPN to match ESPN2/ESPN+, or broadcast nets to match sports/news spin-offs."""
    value = str(text or "").casefold()
    for term in terms:
        if not term:
            continue
        term_fold = str(term).casefold()
        if re.search(rf"(?<![a-z0-9]){re.escape(term_fold)}(?![a-z0-9])", value):
            # If searching for ESPN/ESPN2/ESPNU/FS1/FS2, do not falsely match ESPN+ or RedZone+
            if term_fold in {"espn", "espn2", "espnu"} and re.search(rf"(?<![a-z0-9]){re.escape(term_fold)}\s*\+", value):
                continue
            disallowed = _DISALLOWED_CHANNEL_SUBSTRINGS.get(term_fold)
            if disallowed and any(re.search(rf"(?<![a-z0-9]){re.escape(sub)}(?![a-z0-9])", value) for sub in disallowed):
                continue
            return True
    return False


class DaddyLiveScraper(HtmlAggregatorScraper):
    def __init__(self):
        super().__init__("DaddyLive", os.getenv("AGGREGATOR_6_URL", "https://dlhd.pk"), ["/24-7-channels.php"], ["watch.php"])

    async def _fetch_directory_page(self, client: httpx.AsyncClient, page_url: str, browser: Optional[Browser]) -> Optional[str]:
        """DaddyLive's directory never reaches Playwright's "networkidle"
        state, so this waits a fixed 2s after navigation instead (via the
        shared `_fetch_html` HTTP->Playwright fallback)."""
        return await self._fetch_html(client, page_url, browser, settle_seconds=2, goto_timeout=25000)

    def _parse_channel_matches_from_html(
        self,
        page_html: str,
        page_url: str,
        search_terms: List[str],
        matches: List[tuple[str, int, str]],
        seen_matches: Set[str],
    ) -> None:
        """Pure CPU work (BeautifulSoup parse + per-card channel-term matching),
        split out so it can run via asyncio.to_thread instead of blocking the
        shared event loop. Mutates `matches`/`seen_matches` in place to preserve
        the original cross-page, cumulative MAX_PROVIDER_EVENTS cutoff."""
        soup = make_soup(page_html)
        listed: Set[str] = set()
        try:
            self._parse_channel_anchors(soup, page_url, search_terms, matches, seen_matches, listed)
        finally:
            telemetry.report_index_events(len(listed))

    def _parse_channel_anchors(self, soup, page_url, search_terms, matches, seen_matches, listed: Set[str]) -> None:
        for anchor in soup.find_all("a", href=True):
            href = str(anchor.get("href") or "")
            if not self._is_event_link(href, page_url):
                continue
            match_url = urllib.parse.urljoin(page_url, href)
            listed.add(match_url)
            if match_url in seen_matches:
                continue
            title = str(anchor.get("title") or anchor.get("data-title") or "")
            raw_text = anchor.get_text(" ", strip=True)
            card_text = " ".join(part for part in (raw_text, title, str(anchor.get("aria-label") or "")) if part)
            if _is_non_english_channel(card_text):
                continue
            matched = _channel_term_matches(card_text, search_terms) or _channel_term_matches(href, search_terms)
            score = max(
                (len(str(term).split()) * 10 + len(str(term)) for term in search_terms if _channel_term_matches(card_text, [term])),
                default=0,
            )
            if matched:
                seen_matches.add(match_url)
                matches.append((match_url, score, raw_text or title or match_url))
                if len(matches) >= MAX_PROVIDER_EVENTS:
                    return

    async def _find_matches(self, client: httpx.AsyncClient, search_terms: List[str], browser: Optional[Browser] = None) -> List[tuple[str, int, str]]:
        """Match individual directory cards without inheriting the grid's text."""
        return await self._find_matches_using(
            client,
            search_terms,
            browser,
            fetch_page=self._fetch_directory_page,
            parse_matches=self._parse_channel_matches_from_html,
            catch_page_errors=False,
        )

    async def _extract_player_streams(self, browser: Browser, watch_url: str, score: int, match_title: str) -> List[dict]:
        """Follow the embedded player iframe chain and capture the real HLS manifest.

        DaddyLive watch pages embed the actual player one or two iframes deep
        (watch.php -> dlive.sx/stream/stream-<id>.php -> <player-host>). Listening
        for playlist requests across every frame in the page reliably surfaces the
        genuine manifest, which is requested from the player iframe's origin.
        """
        streams: List[dict] = []
        if not browser or not browser.is_connected():
            return streams

        try:
            async with playwright_page(browser, user_agent=DEFAULT_USER_AGENT) as page:
                def on_request(request) -> None:
                    if not _is_playlist_request_url(request.url):
                        return
                    url = request.url
                    if any(s["url"] == url for s in streams):
                        return
                    try:
                        frame_url = request.frame.url if request.frame else ""
                    except Exception:
                        frame_url = ""
                    referer = request.headers.get("referer") or frame_url or watch_url
                    streams.append({
                        "url": url,
                        "provider": self.name,
                        "match_score": score,
                        "match_title": match_title,
                        "referer": referer,
                        "origin": request.headers.get("origin", ""),
                        "discovery_method": "playwright",
                    })

                page.on("request", on_request)
                try:
                    await page.goto(watch_url, wait_until="domcontentloaded", timeout=30000)
                except Exception as exc:
                    _log_failure("DaddyLive watch page load", exc)

                # Let nested iframes attach, then nudge the player to start.
                await asyncio.sleep(5)
                for frame in list(page.frames):
                    try:
                        await frame.mouse.click(400, 300)
                    except Exception:
                        pass
                try:
                    await page.mouse.click(400, 300)
                except Exception:
                    pass
                await asyncio.sleep(5)
        except Exception as exc:
            _log_failure("DaddyLive player extraction", exc)

        seen = set()
        return [s for s in streams if s["url"] not in seen and not seen.add(s["url"])]

    async def search(self, query_or_terms, browser: Optional[Browser] = None, http_client: Optional[httpx.AsyncClient] = None) -> List[dict]:
        if not browser or not browser.is_connected():
            return []

        search_terms = get_team_search_terms(query_or_terms, query_or_terms) if isinstance(query_or_terms, str) else list(query_or_terms or [])
        owns_client = http_client is None
        client = http_client or httpx.AsyncClient(follow_redirects=True, timeout=12.0)

        try:
            matches = await self._find_matches(client, search_terms, browser=browser)
            if not matches:
                return []

            def match_specificity(event: tuple[str, int, str]) -> tuple[int, int, int]:
                _, score, title = event
                clean_title = clean_sports_text(title)
                exact = 0
                for term in search_terms:
                    clean_term = clean_sports_text(term)
                    if clean_term and re.search(rf"\b{re.escape(clean_term)}\b", clean_title):
                        exact = max(exact, len(clean_term.split()) * 10 + len(clean_term))
                # Strongly prioritize US English feeds (e.g. "USA" in card title)
                us_priority = 100 if re.search(r"\b(?:usa|us)\b", title, re.IGNORECASE) else 0
                return us_priority, exact, score

            specific_matches = [match for match in matches if match_specificity(match)[1] > 0 or match_specificity(match)[0] > 0]
            matches = sorted(specific_matches or matches, key=match_specificity, reverse=True)

            verified_streams: List[dict] = []
            for match_url, score, match_title in matches[:8]:
                streams = await self._extract_player_streams(browser, match_url, score, match_title)
                verified = await _verify_provider_streams(streams, client)
                if verified:
                    verified_streams.extend(verified)

            seen_urls: Set[str] = set()
            return [
                stream for stream in verified_streams
                if stream.get("url") and not (stream["url"] in seen_urls or seen_urls.add(stream["url"]))
            ]
        finally:
            if owns_client:
                await client.aclose()


ACTIVE_PROVIDERS = [
    TheTVAppScraper(),
    DaddyLiveScraper(),
    ISportSurgeScraper(),
    MyBuffStreamsScraper(),
    MethStreamsScraper(),
    StreamEastScraper(),
    FootybiteScraper(),
    OneStreamScraper(),
    StreamedSuScraper(),
    TopStreamsScraper(),
    IptvOrgScraper(),
]
LINEAR_PROVIDERS = tuple(provider for provider in ACTIVE_PROVIDERS if provider.name in {"TheTVApp", "DaddyLive", "IPTV-Org"})

PLAYWRIGHT_CLIENT: Optional[Playwright] = None
SHARED_BROWSER: Optional[Browser] = None
_PLAYWRIGHT_LOCK = asyncio.Lock()
# Periodically recycle the shared Chromium instance so a slow memory/handle leak
# inside the browser itself can't accumulate for the life of the process. 0
# disables recycling. Tracked as (id(SHARED_BROWSER), monotonic launch time) so
# a browser launched by lifespan() (outside get_healthy_browser) still gets a
# correct clock the first time it's observed here, and so the clock resets
# whenever the browser object actually changes instead of trusting stale state.
PLAYWRIGHT_RECYCLE_HOURS = bounded_float(os.getenv("PLAYWRIGHT_RECYCLE_HOURS", "6"), 6.0, 0.0, 168.0)
_BROWSER_LAUNCH_INFO: Optional[Tuple[int, float]] = None


async def get_healthy_browser() -> Optional[Browser]:
    """Retrieve the shared browser, relaunching it if it crashed, disconnected,
    or is due for a periodic recycle (PLAYWRIGHT_RECYCLE_HOURS). The recycle
    check runs opportunistically every time a caller acquires the browser, and
    is skipped whenever a page/context is still open against it, so an
    in-flight scrape is never torn down mid-navigation.
    """
    global SHARED_BROWSER, PLAYWRIGHT_CLIENT, _BROWSER_LAUNCH_INFO

    async with _PLAYWRIGHT_LOCK:
        if SHARED_BROWSER and SHARED_BROWSER.is_connected():
            if _BROWSER_LAUNCH_INFO is None or _BROWSER_LAUNCH_INFO[0] != id(SHARED_BROWSER):
                _BROWSER_LAUNCH_INFO = (id(SHARED_BROWSER), time.monotonic())
            launched_at = _BROWSER_LAUNCH_INFO[1]
            recycle_due = (
                PLAYWRIGHT_RECYCLE_HOURS > 0
                and time.monotonic() - launched_at >= PLAYWRIGHT_RECYCLE_HOURS * 3600
            )
            if not recycle_due or playwright_pages_in_use() > 0:
                return SHARED_BROWSER
            LOGGER.info(
                "Recycling Playwright browser after %.1f hour(s) in service",
                PLAYWRIGHT_RECYCLE_HOURS,
            )
        else:
            LOGGER.warning("Playwright browser disconnected or missing. Relaunching...")

        try:
            if SHARED_BROWSER:
                await SHARED_BROWSER.close()
        except Exception:
            pass

        try:
            if not PLAYWRIGHT_CLIENT:
                PLAYWRIGHT_CLIENT = await async_playwright().start()
            browser = await PLAYWRIGHT_CLIENT.chromium.launch(headless=True)
            if not browser.is_connected():
                await browser.close()
                return None
            SHARED_BROWSER = browser
            _BROWSER_LAUNCH_INFO = (id(browser), time.monotonic())
            return browser
        except Exception as exc:
            _log_failure("relaunch Playwright browser", exc, logging.ERROR)
            return None


def _providers_for_search(always_live: bool = False):
    """Keep 24/7 channel discovery on dedicated linear-channel providers."""
    return provider_settings.filter_enabled(LINEAR_PROVIDERS if always_live else ACTIVE_PROVIDERS)


async def _get_active_provider_priority() -> Dict[str, int]:
    """Return the provider tie-break priority used by rank_streams().

    When provider rotation mode is enabled, the preferred provider rotates
    hourly so no single aggregator is hammered with every search, spreading
    load across sources instead of always preferring the same one.

    Uses get_setting_async() (a thread-offloaded SQLite read) instead of the
    synchronous get_setting(), since this runs on the same event loop that
    also serves live video segments for every other channel.
    """
    if await get_setting_async("provider_rotation_mode", "0") != "1":
        return provider_settings.apply_priority(_provider_priority)
    names = [provider.name for provider in ACTIVE_PROVIDERS]
    if not names:
        return _provider_priority
    offset = int(time.time() // 3600) % len(names)
    rotated = names[offset:] + names[:offset]
    return {name: index for index, name in enumerate(rotated)}


def _stream_matches_requested_event(stream: dict, search_terms: List[str]) -> bool:
    """Reject a provider stream whose source page identifies another event."""
    match_title = str(stream.get("match_title") or "")
    match_url = str(stream.get("match_url") or "")
    if not match_title and not match_url:
        return True
    matched, _, _ = match_team(search_terms, match_title, href=match_url, title=match_title)
    return matched

async def master_scrape(
    query: str,
    team_name: str = "",
    team_id: str = "",
    search_terms: Optional[List[str]] = None,
    always_live: bool = False,
    on_partial: Optional[Callable[[List[dict]], Union[None, Awaitable[None]]]] = None,
) -> List[dict]:
    """Search every provider for the team's streams. `on_partial`, if given, is
    called with each provider's playable results as they arrive (ranked), so an
    emergency rescrape can put a channel back on air without waiting for the
    slowest provider's timeout. May be sync or async."""
    identity = canonical_team_name(team_name or query)
    if search_terms:
        search_terms = list(dict.fromkeys(term.strip() for term in search_terms if term and term.strip()))
    else:
        search_terms = get_team_search_terms(identity or team_name or query, query, team_id)
    display_title = team_name or query
    LOGGER.info("Starting stream search for team=%s terms=%s", display_title, search_terms[:4])
    providers = _providers_for_search(always_live)

    owns_client = state.SHARED_HTTP_CLIENT is None
    client = state.SHARED_HTTP_CLIENT or httpx.AsyncClient(
        limits=httpx.Limits(max_keepalive_connections=50, max_connections=150),
        timeout=12.0,
        follow_redirects=True,
        http2=True
    )

    # Per-team semaphore caps how many of this team's own providers run at once,
    # AND how many of the global PROVIDER_SEARCH_CONCURRENCY slots this team can hold
    # simultaneously — so a team stuck on slow providers can't starve other teams'
    # concurrently running scrapes of every global slot.
    team_semaphore = asyncio.Semaphore(min(PER_TEAM_PROVIDER_CONCURRENCY, PROVIDER_SEARCH_CONCURRENCY))

    async def _jittered_search(provider):
        if _provider_breaker_open(provider.name):
            LOGGER.info("Skipping provider=%s while circuit breaker is open", provider.name)
            return []
        dynamic_timeout = 45.0
        run = telemetry.begin_run(provider.name)
        started = time.time()
        res: List[dict] = []
        exc_seen: Optional[BaseException] = None
        timed_out = False
        try:
            async with team_semaphore, _provider_search_semaphore():
                LOGGER.info("Querying provider=%s team=%s", provider.name, display_title)
                browser = await get_healthy_browser()
                dynamic_timeout = await _get_dynamic_provider_timeout(provider.name)
                started = time.time()
                res = await asyncio.wait_for(
                    provider.search(search_terms, browser=browser, http_client=client),
                    timeout=dynamic_timeout,
                )
                elapsed_ms = int((time.time() - started) * 1000)
                outcome, error_class = telemetry.derive_outcome(res, run)
                await _track_provider_response_time(
                    provider.name, elapsed_ms, success=True,
                    outcome=outcome, error_class=error_class, run=run,
                )
                _provider_breaker_success(provider.name)
                LOGGER.info("Provider returned provider=%s team=%s streams=%d", provider.name, display_title, len(res))
                return res
        except asyncio.TimeoutError:
            timed_out = True
            _provider_breaker_failure(provider.name)
            await _track_provider_response_time(
                provider.name, int(dynamic_timeout * 1000), success=False,
                outcome="timeout", error_class="timeout", run=run,
            )
            LOGGER.warning("Provider search timed out provider=%s team=%s", provider.name, display_title)
            return []
        except Exception as e:
            exc_seen = e
            _provider_breaker_failure(provider.name)
            await _track_provider_response_time(
                provider.name, 0, success=False,
                outcome="error", error_class=telemetry.classify_error(e), run=run,
            )
            _log_failure(f"provider search {provider.name} for {display_title}", e)
            return []
        finally:
            elapsed = int((time.time() - started) * 1000)
            outcome, error_class = telemetry.derive_outcome(res, run, exc=exc_seen, timed_out=timed_out)
            telemetry.finish_run(provider.name, outcome, error_class, run, elapsed, len(res))
            provider_alerts.check_soon(provider.name, _provider_breaker_open(provider.name))

    async def _search_and_report(provider):
        res = await _jittered_search(provider)
        if on_partial is not None and res:
            playable = [s for s in res if always_live or _stream_matches_requested_event(s, search_terms)]
            if playable:
                try:
                    ranked = rank_streams(playable, await _get_active_provider_priority())
                    result = on_partial(ranked[:MAX_STREAM_CANDIDATES])
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as exc:
                    _log_failure("apply partial scrape results", exc)
        return res

    try:
        tasks = [_search_and_report(provider) for provider in providers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_streams = [
            stream
            for sublist in results if isinstance(sublist, list)
            for stream in sublist
            if always_live or _stream_matches_requested_event(stream, search_terms)
        ]
    finally:
        if owns_client:
            await client.aclose()

    seen_urls = set()
    deduped = []
    all_streams = rank_streams(all_streams, await _get_active_provider_priority())
    for s in all_streams:
        u = s.get("url")
        if u and u not in seen_urls:
            seen_urls.add(u)
            deduped.append(s)

    # Cap the returned candidate list: downstream merge steps already cap
    # stream_state[...]['candidates'] to MAX_STREAM_CANDIDATES, but capping
    # here too bounds how many streams every caller of master_scrape (and any
    # future one) has to carry around and log.
    return deduped[:MAX_STREAM_CANDIDATES]


PROVIDER_TIMEOUT_MIN = bounded_float(os.getenv("PROVIDER_TIMEOUT_MIN", "20"), 20.0, 5.0, 300.0)
PROVIDER_TIMEOUT_MAX = bounded_float(os.getenv("PROVIDER_TIMEOUT_MAX", "90"), 90.0, 10.0, 600.0)
PROVIDER_TIMEOUT_DEFAULT = bounded_float(os.getenv("PROVIDER_TIMEOUT_DEFAULT", "45"), 45.0, 5.0, 600.0)


def _dynamic_provider_timeout_sync(provider: str) -> float:
    # Only successful searches: failures were recorded as the timeout itself
    # (ratcheting a slow provider up to the maximum for good) or as 0 ms
    # (dragging a failing one down to the minimum).
    with _db_session() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT AVG(response_time_ms) FROM provider_performance "
            "WHERE provider=? AND success=1 AND timestamp > datetime('now', '-1 hour')",
            (provider,)
        )
        row = cursor.fetchone()
        if row and row[0]:
            avg_ms = row[0]
            return float(max(PROVIDER_TIMEOUT_MIN, min(PROVIDER_TIMEOUT_MAX, (avg_ms / 1000) * 3)))
    return PROVIDER_TIMEOUT_DEFAULT


async def _get_dynamic_provider_timeout(provider: str) -> float:
    """Calculate dynamic timeout based on provider's historical response times."""
    try:
        return await asyncio.to_thread(_dynamic_provider_timeout_sync, provider)
    except Exception as exc:
        _log_failure("calculate dynamic provider timeout", exc)
        return PROVIDER_TIMEOUT_DEFAULT


def _track_provider_response_time_sync(
    provider: str,
    response_time_ms: int,
    success: bool,
    outcome: Optional[str] = None,
    error_class: Optional[str] = None,
    index_events: Optional[int] = None,
    matches: Optional[int] = None,
) -> None:
    if outcome is None:
        outcome = "ok" if success else "error"
    telemetry.record_outcome_sync(provider, response_time_ms, outcome, error_class, index_events, matches)


async def _track_provider_response_time(
    provider: str,
    response_time_ms: int,
    success: bool = True,
    *,
    outcome: Optional[str] = None,
    error_class: Optional[str] = None,
    run: Optional[telemetry.RunStats] = None,
) -> None:
    """Record one provider search (timing for timeout tuning plus its outcome)."""
    index_events = run.index_events if run is not None and run.index_known else None
    matches = run.matches if run is not None and run.index_known else None
    try:
        await asyncio.to_thread(
            _track_provider_response_time_sync,
            provider, response_time_ms, success, outcome, error_class, index_events, matches,
        )
    except Exception as exc:
        _log_failure("track provider response time", exc)
