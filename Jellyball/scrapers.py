"""Stream providers: the scraper classes, the provider circuit breaker, the
shared index-page cache, per-provider base-URL overrides, the shared Playwright
browser, and master_scrape() which searches every provider for one channel.
"""

import asyncio
import json
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
from db import _db_session, get_setting, get_setting_async, set_setting_async
from network_safety import bounded_float, bounded_int, safe_get, validate_http_url
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


# --- Provider classes (3.0.0) --------------------------------------------------
# The provider classes moved to plugins/builtin/ (one module per provider)
# and the SDK base classes moved to plugins/sdk.py. Aliases are defined at
# the end of this module, after the plugin loader runs.


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
                    enrich: dict = {}
                    is_live = await verify_stream_live(
                        client,
                        stream["url"],
                        stream.get("referer", ""),
                        timeout=8.0,
                        origin=stream.get("origin", ""),
                        enrich=enrich,
                    )
                    if is_live and enrich:
                        stream["quality"] = enrich
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
    # A working provider needs no suggested replacement domain.
    _PROVIDER_SUGGESTED_DOMAINS.pop(provider, None)


def reset_provider_breaker(provider_name: str) -> None:
    """Clear a provider's circuit-breaker state (dashboard 'Retry now')."""
    _PROVIDER_BREAKERS.pop(provider_name, None)


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


# --- Automatic provider domain failover (2.2.0) ---------------------------
# Aggregator domains die regularly; when one does, the circuit breaker just
# skips the provider until a human edits the Provider Domains card. Operators
# can list known mirror domains per provider in PROVIDER_MIRROR_DOMAINS, a JSON
# object like {"iSportSurge": ["https://mirror1.example", "..."]}. When the
# breaker opens after repeated failures, the mirrors are probed automatically
# in the background (bounded HTTP fetch of each mirror's base URL). The first
# mirror that serves a usable page is recorded as the provider's suggested
# domain: the dashboard's provider card shows it with one-click Apply, or --
# when PROVIDER_DOMAIN_AUTOSWITCH=1 -- it is applied immediately (persisted to
# the provider_url:<name> setting and live-applied like a manual domain edit).
def _load_provider_mirror_domains() -> Dict[str, List[str]]:
    raw = os.getenv("PROVIDER_MIRROR_DOMAINS", "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception as exc:
        LOGGER.warning("Ignoring invalid PROVIDER_MIRROR_DOMAINS: %s", exc)
        return {}
    mirrors: Dict[str, List[str]] = {}
    if isinstance(data, dict):
        for name, urls in data.items():
            if not isinstance(name, str) or not isinstance(urls, list):
                continue
            cleaned = []
            for url in urls:
                parsed = urllib.parse.urlsplit(str(url or "").strip())
                if parsed.scheme in ("http", "https") and parsed.netloc:
                    cleaned.append(f"{parsed.scheme}://{parsed.netloc}".rstrip("/"))
            if cleaned:
                mirrors[name.strip()] = cleaned
    return mirrors


PROVIDER_MIRROR_DOMAINS: Dict[str, List[str]] = _load_provider_mirror_domains()
PROVIDER_DOMAIN_AUTOSWITCH = os.getenv("PROVIDER_DOMAIN_AUTOSWITCH", "0") == "1"

# --- Built-in provider mirror domains (2.2.1) ---------------------------------
# Aggregator domains die regularly; the 2.2.0 failover only probed mirrors the
# operator listed in PROVIDER_MIRROR_DOMAINS, so a dead default domain just
# failed forever on a fresh install. These well-known alternates are probed too
# (env-configured mirrors first), keeping the suggestion-only default and the
# PROVIDER_DOMAIN_AUTOSWITCH=1 auto-apply behavior unchanged.
BUILTIN_PROVIDER_MIRRORS: Dict[str, List[str]] = {
    "DaddyLive": [
        "https://dlhd.st",
        "https://dlhd.so",
        "https://dlive.sx",
        "https://daddylive.app",
    ],
}


def _provider_mirror_list(provider_name: str) -> List[str]:
    """Env-configured mirrors first, then built-ins, deduplicated."""
    seen: Set[str] = set()
    ordered: List[str] = []
    for mirror in PROVIDER_MIRROR_DOMAINS.get(provider_name, []) + BUILTIN_PROVIDER_MIRRORS.get(provider_name, []):
        key = (mirror or "").rstrip("/").lower()
        if key and key not in seen:
            seen.add(key)
            ordered.append(key)
    return ordered


# The Playwright Cloudflare-bypass warning fires per page per search; when a
# provider's domain is down (DNS failure, Cloudflare block) it would spam every
# refresh cycle. Warn once per page per cooldown, debug-log the repeats.
_BYPASS_WARN_COOLDOWN_SECONDS = 900.0
_BYPASS_WARNED_AT: Dict[str, float] = {}


def _log_bypass_failure_throttled(provider_name: str, page_url: str, exc: BaseException) -> None:
    key = f"{provider_name}|{page_url}"
    now = time.monotonic()
    # Missing keys must warn: defaulting the last-warn time to 0.0 falsely
    # throttles every first warning on hosts whose monotonic clock is still
    # under the cooldown (fresh CI VMs, recently rebooted boxes).
    last = _BYPASS_WARNED_AT.get(key)
    if last is None or now - last >= _BYPASS_WARN_COOLDOWN_SECONDS:
        _BYPASS_WARNED_AT[key] = now
        _log_failure(f"{provider_name} Cloudflare bypass for {page_url}", exc)
    else:
        LOGGER.debug("%s Cloudflare bypass for %s failed again (throttled)", provider_name, page_url)


# Mirror that probed healthy while the provider's breaker was open, awaiting
# the operator's one-click Apply (cleared on apply, dismiss, or a successful
# search through _provider_breaker_success).
_PROVIDER_SUGGESTED_DOMAINS: Dict[str, str] = {}
_MIRROR_PROBE_INFLIGHT: Set[str] = set()
_MIRROR_PROBE_MIN_HTML_BYTES = 500


def get_suggested_domain(provider_name: str) -> Optional[str]:
    """The auto-probed replacement domain awaiting operator approval, if any."""
    return _PROVIDER_SUGGESTED_DOMAINS.get(provider_name)


def dismiss_suggested_domain(provider_name: str) -> None:
    _PROVIDER_SUGGESTED_DOMAINS.pop(provider_name, None)


def _maybe_probe_provider_mirrors(provider_name: str) -> None:
    """After the breaker opens, probe the provider's configured mirrors once
    in the background. Guarded against duplicate probes and against providers
    with no mirrors configured or a suggestion already pending."""
    if not _provider_mirror_list(provider_name):
        return
    if provider_name in _MIRROR_PROBE_INFLIGHT:
        return
    if _PROVIDER_SUGGESTED_DOMAINS.get(provider_name):
        return
    _MIRROR_PROBE_INFLIGHT.add(provider_name)
    state._spawn_background_task(_probe_provider_mirrors(provider_name), f"mirror probe {provider_name}")


async def _probe_provider_mirrors(provider_name: str) -> None:
    try:
        provider = next((p for p in ACTIVE_PROVIDERS if p.name == provider_name), None)
        if provider is None or not isinstance(provider, HtmlAggregatorScraper):
            return
        current = (provider.base_url or "").rstrip("/")
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            for mirror in _provider_mirror_list(provider_name):
                if mirror == current:
                    continue
                if not _validate_upstream_url(mirror):
                    continue
                try:
                    html = await fetch_bounded_text(
                        client,
                        mirror,
                        headers={"User-Agent": DEFAULT_USER_AGENT, "Referer": mirror},
                        timeout=8.0,
                    )
                except Exception as exc:
                    _log_failure(f"probe mirror {mirror} for {provider_name}", exc)
                    continue
                if html and len(html) >= _MIRROR_PROBE_MIN_HTML_BYTES and "<" in html:
                    await _on_working_mirror(provider, mirror)
                    return
        LOGGER.info("Mirror probe found no working domain provider=%s", provider_name)
    finally:
        _MIRROR_PROBE_INFLIGHT.discard(provider_name)


async def _on_working_mirror(provider: "HtmlAggregatorScraper", mirror: str) -> None:
    if PROVIDER_DOMAIN_AUTOSWITCH:
        await set_setting_async(_provider_url_setting_key(provider.name), mirror)
        _set_provider_url_override(provider, mirror)
        _PROVIDER_SUGGESTED_DOMAINS.pop(provider.name, None)
        reset_provider_breaker(provider.name)
        LOGGER.warning("Auto-switched provider domain provider=%s url=%s", provider.name, mirror)
        try:
            from alerts import send_alert  # late: alerts pulls in the catalog

            await send_alert(
                "Provider domain auto-switched",
                f"**{provider.name}** moved to {mirror} after its old domain failed.",
                "warning",
            )
        except Exception as exc:
            _log_failure("send domain auto-switch alert", exc)
    else:
        _PROVIDER_SUGGESTED_DOMAINS[provider.name] = mirror
        LOGGER.warning(
            "Suggested new domain for provider=%s url=%s (apply from the dashboard)",
            provider.name,
            mirror,
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
    "espn": ("deportes",),
}


def _espn_number_mismatch(term_fold: str, value: str) -> bool:
    """True when the channel name carries an ESPN-family number the term didn't ask for.

    The IPTV-Org playlist (and some aggregator listings) carries numbered regional
    feeds like "ESPN 3"/"ESPN 4" that word-boundary matching would otherwise accept
    for a bare "espn" search, letting the wrong feed win the candidate ranking.
    """
    m = re.search(r"(?<![a-z0-9])espn\s*([0-9]+)", value)
    if not m:
        return False
    want = {"espn": None, "espn2": "2", "espnu": None}.get(term_fold)
    return m.group(1) != want


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
            # Bare "espn" must not match numbered regional feeds ("ESPN 3", "ESPN 4");
            # "espn2" must not match "ESPN 3", etc.
            if term_fold in {"espn", "espn2", "espnu"} and _espn_number_mismatch(term_fold, value):
                continue
            disallowed = _DISALLOWED_CHANNEL_SUBSTRINGS.get(term_fold)
            if disallowed and any(re.search(rf"(?<![a-z0-9]){re.escape(sub)}(?![a-z0-9])", value) for sub in disallowed):
                continue
            return True
    return False


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
                    provider.search(
                        search_terms,
                        browser=permitted_browser(provider, browser),
                        http_client=client,
                    ),
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
            if (exc_seen is not None or timed_out) and _provider_breaker_open(provider.name):
                _maybe_probe_provider_mirrors(provider.name)

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


# IPTV-Org playlist cache. Lives here (not in the builtin plugin module) so the
# module-level cache stays shared and test-visible as scrapers._IPTV_ORG_CACHE.
IPTV_ORG_PLAYLIST_URL_DEFAULT = "https://iptv-org.github.io/iptv/categories/sports.m3u"
IPTV_ORG_REFRESH_SECONDS = bounded_float(os.getenv("IPTV_ORG_REFRESH_SECONDS", "21600"), 21600.0, 300.0, 86400.0)
_IPTV_ORG_CACHE: List[dict] = []
_IPTV_ORG_CACHE_LOADED_AT = 0.0
_IPTV_ORG_LOCK = asyncio.Lock()


# --- Provider plugin system (3.0.0) -------------------------------------------
# The 11 built-in providers now live in plugins/builtin/ (one module each) and
# the SDK base classes in plugins/sdk.py. The loader runs here, at the end of
# this module, so every engine name the SDK needs is already defined (this
# ordering is what keeps the scrapers <-> plugins import cycle safe).
from plugins import get_plugin_errors, get_plugin_records, load_plugins, permitted_browser  # noqa: E402
from plugins import sdk as _plugin_sdk  # noqa: E402

Provider = _plugin_sdk.Provider
BaseProvider = _plugin_sdk.Provider
HtmlAggregatorProvider = _plugin_sdk.HtmlAggregatorProvider
HtmlAggregatorScraper = _plugin_sdk.HtmlAggregatorProvider

ACTIVE_PROVIDERS: List = []
LINEAR_PROVIDERS: tuple = ()


def _rebuild_provider_lists(records=None) -> None:
    """(Re)build the engine's provider lists from plugin records.

    ACTIVE_PROVIDERS is mutated in place so existing `from scrapers import
    ACTIVE_PROVIDERS` bindings keep working across a dashboard reload.
    """
    global LINEAR_PROVIDERS
    recs = records if records is not None else get_plugin_records()
    ACTIVE_PROVIDERS[:] = [r.instance for r in recs]
    LINEAR_PROVIDERS = tuple(r.instance for r in recs if r.linear)


_rebuild_provider_lists(load_plugins())


# Re-export the built-in provider classes so `from scrapers import X` keeps
# working during 3.x (tests, routes, and any external tooling).
from plugins.builtin.daddylive.provider import DaddyLiveScraper  # noqa: E402
from plugins.builtin.footybite.provider import FootybiteScraper  # noqa: E402
from plugins.builtin.iptvorg.provider import IptvOrgScraper  # noqa: E402
from plugins.builtin.isportsurge.provider import ISportSurgeScraper  # noqa: E402
from plugins.builtin.methstreams.provider import MethStreamsScraper  # noqa: E402
from plugins.builtin.mybuffstreams.provider import MyBuffStreamsScraper  # noqa: E402
from plugins.builtin.onestream.provider import OneStreamScraper  # noqa: E402
from plugins.builtin.streameast.provider import StreamEastScraper  # noqa: E402
from plugins.builtin.streamedsu.provider import StreamedSuScraper  # noqa: E402
from plugins.builtin.thetvapp.provider import TheTVAppScraper  # noqa: E402
from plugins.builtin.topstreams.provider import TopStreamsScraper  # noqa: E402
