"""Jellyball provider plugin SDK: the stable, versioned public API for scrapers.

Third-party plugins import ONLY from this module (plus the standard library,
httpx, and playwright for type hints). Everything here is covered by the
backwards-compatibility policy documented in plugins/README.md: within a major
API version, changes are additive only.

API_VERSION = 1
"""

import asyncio
import urllib.parse
from typing import Any, Dict, List, Optional, Set, TypedDict

import httpx
from playwright.async_api import Browser

from config import _log_failure, _validate_upstream_url, LOGGER
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
from sports_matcher import get_team_search_terms, match_team


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


class HtmlAggregatorProvider(Provider):
    """Configurable adapter for aggregators that expose linked event pages."""

    def __init__(self, name: str, base_url: str, categories: Optional[List[str]] = None, event_path_hints: Optional[List[str]] = None):
        super().__init__()
        self.name = name.strip() or "Aggregator"
        self._default_base_url = (base_url or "").strip().rstrip("/")
        self.categories = list(categories or [])
        self.event_path_hints = tuple(hint.lower() for hint in (event_path_hints or []) if hint)

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
        page_html = await _engine.fetch_bounded_text(
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
                _engine._log_bypass_failure_throttled(self.name, page_url, exc)
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
            if len(matches) >= _engine.MAX_PROVIDER_EVENTS:
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
