"""Jellyball provider plugin SDK: the stable, versioned public API for scrapers.

Third-party plugins import ONLY from this module (plus the standard library,
httpx, and playwright for type hints). Everything here is covered by the
backwards-compatibility policy documented in plugins/README.md: within a major
API version, changes are additive only.

API_VERSION = 1
"""

import asyncio
import logging
import os
import re
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Pattern, Set, Tuple, TypedDict, cast

import httpx
from playwright.async_api import Browser

from config import _log_failure, _validate_upstream_url, LOGGER
from cpu_pool import run_cpu
from network_safety import validate_http_url_async
import provider_settings
import provider_telemetry as telemetry
from stream_extractor import (
    DEFAULT_USER_AGENT,
    fetch_bounded_text,
    fetch_streams_from_page,
    make_soup,
    playwright_intercept_streams,
    playwright_page,
)
from sports_matcher import get_team_search_terms, match_team, matches_required_teams


API_VERSION = 1


class StreamDict(TypedDict, total=False):
    """The provider-to-engine stream contract.

    Required: url, provider, match_score. Everything else is optional but
    encouraged; the engine tolerates missing keys.
    """

    url: str
    referer: str
    origin: str
    provider: str
    match_score: int
    match_title: str
    match_url: str
    # The listing text the provider matched the event on when the link text
    # itself is generic ("Watch", "HD 1"): lets the engine's event filter
    # confirm the stream belongs to the requested game.
    match_context: str
    discovery_method: str
    quality: Dict[str, Any]


async def fetch_text(
    client: httpx.AsyncClient,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 10.0,
    max_bytes: int = 2_000_000,
) -> Optional[str]:
    """SSRF-safe HTTP GET returning response text (or None).

    Every URL and redirect hop is validated with
    :func:`network_safety.validate_http_url_async`, so SSRF protection is
    inherited, not optional. This and :func:`fetch_html` are the only
    sanctioned network access for plugins.
    """
    safe_url = await validate_http_url_async(url)
    if not safe_url:
        return None
    return await fetch_bounded_text(
        client,
        safe_url,
        headers=headers or {"User-Agent": DEFAULT_USER_AGENT},
        timeout=timeout,
        max_bytes=max_bytes,
    )


async def fetch_html(
    client: httpx.AsyncClient,
    url: str,
    *,
    browser: Optional[Browser] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 10.0,
) -> Optional[str]:
    """SSRF-safe page fetch: fast HTTP first, Playwright fallback for
    Cloudflare-protected pages. Pass ``browser=None`` when the plugin has no
    ``browser`` permission (the loader enforces this automatically)."""
    html = await fetch_text(client, url, headers=headers, timeout=timeout)
    if not html and browser is not None and browser.is_connected():
        try:
            async with playwright_page(browser, user_agent=DEFAULT_USER_AGENT) as page:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                try:
                    await page.wait_for_load_state("networkidle", timeout=8000)
                except Exception:
                    pass
                html = await page.content()
        except Exception as exc:
            _log_failure("plugin Playwright fallback", exc)
    return html


# --- Why a provider page could not be used (issue #49) ----------------------------
# A provider that lists nothing used to look the same whether its site was down,
# blocked us, was a parked domain or had changed its layout. These helpers name
# the state for telemetry (error_class) and a throttled log line. Recognising a
# block is all they do: a blocked site is reported, never worked around.
_CHALLENGE_TITLES = ("just a moment", "attention required", "access denied", "verify you are human", "ddos-guard")
_CHALLENGE_MARKERS = ("cf-browser-verification", "cf_chl_opt", "id=\"challenge-form\"", "id=\"challenge-running\"")
_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.S)
_PARKED_MARKERS = (
    "parklogic.com", "parkingcrew", "sedoparking", "bodis.com", "domain is for sale",
    "this domain may be for sale", "buy this domain", "domain has expired",
)
_BLOCK_STATUSES = frozenset({401, 403, 429})
_UNREACHABLE_ERRORS = frozenset({"timeout", "connect", "tls", "network"})
_SITE_STATE_MESSAGES = {
    "blocked": "site blocked automated access",
    "unreachable": "site unreachable",
    "parked": "site is a parked domain with no stream listings",
    "not_found": "index page not found",
    "no_channel_directory": "no channel directory at the expected page",
    "no_event_links": "index pages list no event links (quiet day, or the site layout changed)",
}
_SITE_STATE_LOG_COOLDOWN_SECONDS = 900.0
_SITE_STATE_LOGGED_AT: Dict[str, float] = {}


def classify_page_html(page_html: str) -> Optional[Tuple[str, str]]:
    """("blocked"|"parked", detail) when a fetched page is a bot check or a
    parked domain rather than the provider's own content, else None."""
    head = (page_html or "")[:200_000].lower()
    title_match = _TITLE_RE.search(head)
    title = title_match.group(1).strip() if title_match else ""
    if any(marker in title for marker in _CHALLENGE_TITLES) or any(marker in head for marker in _CHALLENGE_MARKERS):
        return "blocked", "bot-check page"
    if any(marker in head for marker in _PARKED_MARKERS):
        return "parked", "parking page"
    return None


def classify_fetch_failure(info: Dict[str, Any]) -> Tuple[str, str]:
    """(state, detail) for a failed :func:`stream_extractor.fetch_bounded_text`
    call, from the `info` dict it filled in."""
    status = info.get("status")
    exc = info.get("exception")
    if exc is not None:
        error_class = telemetry.classify_error(exc)
        if error_class in _UNREACHABLE_ERRORS:
            return "unreachable", error_class
        return error_class, type(exc).__name__
    error = info.get("error")
    if error:
        return str(error), str(error)
    if isinstance(status, int):
        body = str(info.get("body_head") or "")
        if status in _BLOCK_STATUSES or (status == 503 and classify_page_html(body)):
            return "blocked", f"HTTP {status}"
        if status in (404, 410):
            return "not_found", f"HTTP {status}"
        return f"http_{status}", f"HTTP {status}"
    return "no_content", "empty response"


def _pinned_url_hint(host: str) -> str:
    """When the failing host comes from an AGGREGATOR_<n>_URL setting, say so:
    a value saved in .env overrides the provider's built-in default, so an old
    domain stays in use after the app ships a new one."""
    if not host:
        return ""
    for name, value in sorted(os.environ.items()):
        if name.startswith("AGGREGATOR_") and name.endswith("_URL") and urllib.parse.urlsplit(value).netloc == host:
            return f" hint=this address comes from {name} in your .env, which overrides the built-in default; remove that line to use the current default"
    return ""


def log_site_state(provider_name: str, state: str, detail: str, page_url: str, level: int = logging.WARNING) -> None:
    """One log line (WARNING by default) per provider/state per 15 minutes,
    DEBUG for the repeats. Logs only the host and path, never the query."""
    parts = urllib.parse.urlsplit(page_url or "")
    key = f"{provider_name}|{state}"
    now = time.monotonic()
    last = _SITE_STATE_LOGGED_AT.get(key)
    message = _SITE_STATE_MESSAGES.get(state, "index page unusable")
    if last is None or now - last >= _SITE_STATE_LOG_COOLDOWN_SECONDS:
        _SITE_STATE_LOGGED_AT[key] = now
        LOGGER.log(
            level,
            "provider %s: %s (%s) host=%s path=%s%s",
            provider_name, message, detail, parts.netloc, parts.path or "/", _pinned_url_hint(parts.netloc),
        )
    else:
        LOGGER.debug("provider %s: %s again (%s) host=%s", provider_name, message, detail, parts.netloc)


def get_setting(provider_name: str, key: str, default: Any = None) -> Any:
    """Read a provider's persisted config by provider name.

    ``key`` is one of ``"enabled"``, ``"priority"``, or ``"base_url"``.
    Provider names are the source of truth, so existing dashboard settings
    keep working for plugins that reuse a built-in provider's name.
    """
    if key == "enabled":
        return provider_settings.is_enabled(provider_name)
    if key == "priority":
        return provider_settings.priority_override(provider_name)
    if key == "base_url":
        import scrapers

        return scrapers._PROVIDER_BASE_URL_OVERRIDES.get(provider_name, "")
    return default


class Provider:
    """Base class for every provider plugin.

    Subclasses set :attr:`name` and implement :meth:`search`. The engine calls
    ``search`` with keyword arguments; ``browser`` is None when the plugin did
    not declare the ``"browser"`` permission.
    """

    name = "Base"
    categories: List[str] = []
    # False for providers that only list 24/7 linear channels and never carry a
    # team's game (e.g. a curated free-to-air playlist): team channels do not
    # search them, so a team can never take an unrelated linear channel.
    carries_team_events: bool = True

    def __init__(self) -> None:
        self._default_base_url = ""

    @property
    def base_url(self) -> str:
        """The provider's current base URL: a DB override (set via the
        dashboard's Provider Domains card) if one is active, otherwise the
        default the provider was constructed with. Read fresh on every access
        from a dict that's kept in sync at startup and on save."""
        return _engine._PROVIDER_BASE_URL_OVERRIDES.get(self.name) or self._default_base_url

    @base_url.setter
    def base_url(self, value: str) -> None:
        self._default_base_url = (value or "").strip().rstrip("/")

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
        *,
        browser: Optional[Browser] = None,
        http_client: Optional[httpx.AsyncClient] = None,
    ) -> List[StreamDict]:
        return []


class _MatchedEvent(tuple):
    """A (match_url, score, match_title) event tuple that can also carry the
    listing text it was matched on (``match_context``)."""

    match_context: str = ""


def _matched_event(match_url: str, score: int, match_title: str, context: str = "") -> tuple[str, int, str]:
    event = _MatchedEvent((match_url, score, match_title))
    if context:
        event.match_context = context
    return cast("tuple[str, int, str]", event)


class HtmlAggregatorProvider(Provider):
    """Configurable adapter for aggregators that expose linked event pages."""

    def __init__(
        self,
        name: str,
        base_url: str,
        categories: Optional[List[str]] = None,
        event_path_hints: Optional[List[str]] = None,
        event_path_patterns: Optional[List[str]] = None,
    ):
        super().__init__()
        self.name = name.strip() or "Aggregator"
        self._default_base_url = (base_url or "").strip().rstrip("/")
        self.categories = list(categories or [])
        self.event_path_hints = tuple(hint.lower() for hint in (event_path_hints or []) if hint)
        # Regular expressions matched (case-insensitively) against an event
        # link's path, for sites whose event URLs carry no fixed word such as
        # "/watch/" (e.g. "/<league>/<teams-slug>/<numeric id>"). A link is an
        # event link when it matches a hint OR a pattern.
        self.event_path_patterns: Tuple[Pattern[str], ...] = tuple(
            re.compile(pattern, re.IGNORECASE) for pattern in (event_path_patterns or []) if pattern
        )

    @property
    def _page_fetch_state(self) -> Dict[str, Dict[str, Any]]:
        """Per scan URL: the URL the last fetch ended on (after redirects) and,
        when it failed, why. Kept on the instance, beside the shared HTML
        cache, so a cache hit still knows both."""
        state = self.__dict__.get("_page_fetch_state_store")
        if state is None:
            state = {}
            self.__dict__["_page_fetch_state_store"] = state
        return state

    def _effective_page_url(self, page_url: str) -> str:
        """The URL `page_url` was really served from: a redirect target when
        the site moved (e.g. an old domain answering 301 to its new one)."""
        return str(self._page_fetch_state.get(page_url, {}).get("final_url") or page_url)

    def _record_page_fetch(self, page_url: str, final_url: str, failure: Optional[Tuple[str, str]]) -> None:
        requested_host = urllib.parse.urlsplit(page_url).netloc.lower()
        final_host = urllib.parse.urlsplit(final_url or page_url).netloc.lower()
        if final_url and final_host and final_host != requested_host and failure is None:
            key = f"{self.name}|redirect|{final_host}"
            if key not in _SITE_STATE_LOGGED_AT:
                _SITE_STATE_LOGGED_AT[key] = time.monotonic()
                LOGGER.info(
                    "provider %s: index page moved from host=%s to host=%s; following the redirect "
                    "(set the provider domain to the new host to skip it)",
                    self.name, requested_host, final_host,
                )
        self._page_fetch_state[page_url] = {"final_url": final_url or page_url, "failure": failure}

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

    def _card_context(self, anchor, page_url: str, match_url: str, card_links: Dict[int, Set[str]]) -> str:
        """The text of the card a link sits in, for links that only say "Watch".
        When the parent is a whole listing (it also links other events) its
        text names every game on the page, so only the link's own text counts:
        otherwise every link on a page whose first row is the searched game
        matched it (60 matches for Oregon at Ohio State on iSportSurge)."""
        parent = anchor.parent
        if parent is not None:
            key = id(parent)
            if key not in card_links:
                card_links[key] = {
                    urllib.parse.urljoin(page_url, str(other.get("href") or ""))
                    for other in parent.find_all("a", href=True)
                    if self._is_event_link(str(other.get("href") or ""), page_url)
                }
            if card_links[key] - {match_url}:
                return " ".join(
                    part for part in (
                        anchor.get_text(" ", strip=True), str(anchor.get("title") or ""),
                        str(anchor.get("aria-label") or ""),
                    ) if part
                )
        return self._anchor_context(anchor)

    def _is_event_link(self, href: str, page_url: str) -> bool:
        if not href or href.startswith(("#", "javascript:", "mailto:")):
            return False
        candidate = urllib.parse.urljoin(page_url, href)
        if not _validate_upstream_url(candidate):
            return False
        # The provider's own host, plus the host the page was actually served
        # from when the site redirected its index page to a new domain.
        site_hosts = {
            urllib.parse.urlparse(self.base_url).netloc.lower(),
            urllib.parse.urlparse(page_url).netloc.lower(),
        }
        if urllib.parse.urlparse(candidate).netloc.lower() not in site_hosts:
            return False
        if candidate.rstrip("/") == page_url.rstrip("/"):
            return False
        if not (self.event_path_hints or self.event_path_patterns):
            return True
        if any(hint in candidate.lower() for hint in self.event_path_hints):
            return True
        path = urllib.parse.urlparse(candidate).path
        return any(pattern.search(path) for pattern in self.event_path_patterns)

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
        HtmlAggregatorProvider subclass so this HTTP->Playwright fallback isn't
        reimplemented per provider.

        `settle_seconds`, when set, waits a fixed amount of time after
        navigation instead of waiting for the network to go idle -- some sites
        (e.g. DaddyLive's channel directory) never reach a quiet "networkidle"
        state.
        """
        # Resolved via _engine at call time (not the from-import above) so tests
        # patching scrapers.fetch_bounded_text keep working, same as the other
        # _engine.* tunables.
        info: Dict[str, Any] = {}
        page_html = await _engine.fetch_bounded_text(
            client,
            page_url,
            headers={"User-Agent": DEFAULT_USER_AGENT, "Referer": self.base_url},
            timeout=8.0,
            info=info,
        )
        final_url = str(info.get("final_url") or page_url)
        failure = None if page_html else classify_fetch_failure(info)
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
                    rendered_url = getattr(page, "url", "")
                    if isinstance(rendered_url, str) and rendered_url.startswith(("http://", "https://")):
                        final_url = rendered_url
                    if page_html:
                        failure = None
            except Exception as exc:
                _engine._log_bypass_failure_throttled(self.name, page_url, exc)
        self._record_page_fetch(page_url, final_url, failure)
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
        split out so it can run in a worker thread via cpu_pool.run_cpu instead
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
        card_links: Dict[int, Set[str]] = {}
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
            context = ""
            if not matched:
                # Some providers put the team names in the card rather
                # than the anchor. Only accept that fallback for a
                # high-confidence full identity, not a generic nickname.
                candidate_text = self._card_context(anchor, page_url, match_url, card_links) or href
                context_matched, context_score, _ = match_team(search_terms, candidate_text, href=href, title=title)
                if context_matched and context_score >= 110:
                    matched, score = context_matched, context_score
                    context = candidate_text
            if matched and not matches_required_teams(direct_text, href=href, title=title):
                # A game search needs both teams: the link may name one and
                # its card the other.
                candidate_text = context or self._card_context(anchor, page_url, match_url, card_links)
                if candidate_text and matches_required_teams(candidate_text, href=href, title=title):
                    context = candidate_text
                else:
                    matched = False
            if matched:
                seen_matches.add(match_url)
                # The card context rides along (as an attribute, so the event
                # stays a 3-tuple for existing subclasses) to the streams: the
                # engine's event filter needs what identified the game when
                # the link text itself cannot.
                matches.append(_matched_event(match_url, score, raw_text or title or match_url, context))
                if len(matches) >= _engine.MAX_PROVIDER_EVENTS:
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
            page_html = await _engine._get_cached_index_html(page_url, lambda pu=page_url: fetch_page(client, pu, browser))
            fetch_state = self._page_fetch_state.get(page_url, {})
            if not page_html:
                state, detail = fetch_state.get("failure") or ("no_content", "empty response")
                telemetry.report_page_error(state)
                log_site_state(self.name, state, detail, page_url)
                return
            page_state = classify_page_html(page_html)
            if page_state is not None:
                # A bot check or a parked domain: nothing to parse, and no
                # attempt to get past it. Say so instead of "0 events".
                telemetry.report_page_error(page_state[0])
                log_site_state(self.name, page_state[0], page_state[1], self._effective_page_url(page_url))
                return
            effective_url = self._effective_page_url(page_url)
            await run_cpu(parse_matches, page_html, effective_url, search_terms, matches, seen_matches)

        for page_url in self.get_scan_urls():
            if catch_page_errors:
                try:
                    await process(page_url)
                except Exception as exc:
                    _log_failure(f"scan provider={self.name} page", exc)
                    telemetry.report_page_error(exc)
            else:
                await process(page_url)
            if len(matches) >= _engine.MAX_PROVIDER_EVENTS:
                break
        telemetry.report_matches(len(matches))
        run = telemetry.current_run()
        if run is not None and run.index_pages and not run.index_events and not run.page_errors:
            # Every page loaded but none listed an event link: either a quiet
            # day or (when it persists) a layout the link rules no longer fit.
            log_site_state(
                self.name, "no_event_links", f"{run.index_pages} page(s) read", self.base_url, level=logging.INFO
            )
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
        *,
        browser: Optional[Browser] = None,
        http_client: Optional[httpx.AsyncClient] = None,
    ) -> List[StreamDict]:
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
            matched_events = await self._find_matches(client, search_terms, browser=browser)
            event_semaphore = asyncio.Semaphore(_engine.PROVIDER_EVENT_CONCURRENCY)

            async def inspect_event(event: tuple[str, int, str]) -> List[dict]:
                match_url, score, match_title = event
                match_context = str(getattr(event, "match_context", "") or "")
                async with event_semaphore:
                    async def inspect() -> List[dict]:
                        event_streams = await fetch_streams_from_page(
                            client, match_url, self.name, score, match_title
                        )
                        if not event_streams and browser and browser.is_connected():
                            event_streams = await playwright_intercept_streams(
                                browser, match_url, self.name, score, match_title, http_client=client
                            )
                        for stream in event_streams:
                            stream.setdefault("match_url", match_url)
                            if match_context:
                                stream.setdefault("match_context", match_context)
                        return event_streams

                    try:
                        return await asyncio.wait_for(inspect(), timeout=_engine.PROVIDER_EVENT_TIMEOUT)
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
                timeout=_engine.PROVIDER_SEARCH_TIMEOUT,
            )
            return [stream for result in results if isinstance(result, list) for stream in result]
        except asyncio.TimeoutError:
            LOGGER.warning("Provider search timed out provider=%s", self.name)
            return []
        finally:
            if owns_client:
                await client.aclose()


# --- Engine reference (last) -----------------------------------------------------
# The scrapers engine module, imported last so that by the time this line runs
# the engine is fully initialized (scrapers imports the plugins loader at the
# END of its own module). Engine tunables are read via module attribute access
# at call time so runtime patching (e.g. tests) keeps working.
import scrapers as _engine  # noqa: E402
