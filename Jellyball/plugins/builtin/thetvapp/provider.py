"""Built-in provider plugin: TheTVApp (linear channel directory)."""

import asyncio
import os
import urllib.parse
from typing import List, Optional

import httpx
from playwright.async_api import Browser

from config import _log_failure
import provider_telemetry as telemetry
from network_safety import validate_http_url
from stream_extractor import DEFAULT_USER_AGENT, make_soup, playwright_page
from sports_matcher import get_team_search_terms, match_team
from plugins.sdk import HtmlAggregatorProvider

from scrapers import (
    _channel_term_matches,
    _is_non_english_channel,
    _is_playlist_request_url,
    _verify_provider_streams,
    MAX_STREAM_CANDIDATES,
)


class TheTVAppScraper(HtmlAggregatorProvider):
    def __init__(self):
        super().__init__(
            "TheTVApp", 
            os.getenv("AGGREGATOR_5_URL", "https://thetvapp.st"), 
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
