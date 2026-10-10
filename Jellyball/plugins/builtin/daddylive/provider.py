"""Built-in provider plugin: DaddyLive (linear channel directory + player extraction)."""

import asyncio
import os
import re
import urllib.parse
from typing import List, Optional, Set

import httpx
from playwright.async_api import Browser

from config import _log_failure
import provider_telemetry as telemetry
from stream_extractor import DEFAULT_USER_AGENT, make_soup, playwright_page
from sports_matcher import clean_sports_text, get_team_search_terms
from plugins.sdk import HtmlAggregatorProvider

from scrapers import (
    _channel_term_matches,
    _is_non_english_channel,
    _is_playlist_request_url,
    _verify_provider_streams,
    MAX_PROVIDER_EVENTS,
)


class DaddyLiveScraper(HtmlAggregatorProvider):
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
