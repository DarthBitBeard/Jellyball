"""Built-in provider plugin: IPTV-Org (curated free-to-air playlist)."""

import asyncio
import os
import re
import time
from typing import List, Optional

import httpx
from playwright.async_api import Browser

from config import _log_failure, _validate_upstream_url
from network_safety import bounded_float, safe_get
import provider_telemetry as telemetry
from stream_extractor import DEFAULT_USER_AGENT
from plugins.sdk import Provider

import scrapers
from scrapers import (
    _channel_term_matches,
    _parse_m3u_playlist,
    _verify_provider_streams,
    IPTV_ORG_PLAYLIST_URL_DEFAULT,
    IPTV_ORG_REFRESH_SECONDS,
    IPTV_ORG_VERIFY_LIMIT,
)
import stream_language


_RESOLUTION_RE = re.compile(r"\((\d{3,4})[pi]\)")


def _entry_rank(candidate: dict, entry: dict, prefs: List[str]) -> tuple:
    """Best first: the viewer's languages, then a title that does not name
    another country's edition ("Fox (Poland)" is listed under English too),
    then streams that run around the clock, then higher resolution."""
    title = str(candidate.get("match_title") or "")
    title_language = stream_language.detect_language(title)
    other_edition = title_language is not None and title_language not in prefs
    part_time = "not 24/7" in title.casefold()
    match = _RESOLUTION_RE.search(title)
    resolution = int(match.group(1)) if match else 0
    return (stream_language.language_rank(candidate, prefs), other_edition, part_time, -resolution, title)


class IptvOrgScraper(Provider):
    """Curated, static free-to-air playlist (iptv-org) used as a structurally
    independent backup for always-live special channels: a plain cached HTTP
    fetch with no scraping and no Cloudflare exposure, so it survives failure
    modes (anti-bot changes, mass site outages) that could take out every
    HTML-scraping provider in ACTIVE_PROVIDERS at once. Only useful for 24/7
    linear channels (ESPN, FS1, NFL Network, ...). It carries no per-game team
    broadcasts, so it belongs in LINEAR_PROVIDERS, not the general team search."""

    name = "IPTV-Org"
    # 24/7 channels only: a team search matched e.g. a snooker channel named
    # with a "GB" region code for the Packers (issue #48).
    carries_team_events = False

    def __init__(self):
        self.base_url = os.getenv("IPTV_ORG_PLAYLIST_URL", IPTV_ORG_PLAYLIST_URL_DEFAULT)

    async def _get_entries(self, http_client: Optional[httpx.AsyncClient]) -> List[dict]:
        # The cache lives on the scrapers module (shared, test-visible).
        now = time.monotonic()
        if scrapers._IPTV_ORG_CACHE and now - scrapers._IPTV_ORG_CACHE_LOADED_AT < IPTV_ORG_REFRESH_SECONDS:
            return scrapers._IPTV_ORG_CACHE

        async with scrapers._IPTV_ORG_LOCK:
            now = time.monotonic()
            if scrapers._IPTV_ORG_CACHE and now - scrapers._IPTV_ORG_CACHE_LOADED_AT < IPTV_ORG_REFRESH_SECONDS:
                return scrapers._IPTV_ORG_CACHE

            url = _validate_upstream_url(self.base_url)
            if not url:
                return scrapers._IPTV_ORG_CACHE

            owns_client = http_client is None
            client = http_client or httpx.AsyncClient(timeout=15.0, follow_redirects=False)
            try:
                resp = await safe_get(client, url, headers={"User-Agent": DEFAULT_USER_AGENT})
                if resp is not None and resp.status_code == 200:
                    scrapers._IPTV_ORG_CACHE = _parse_m3u_playlist(resp.text)
                    scrapers._IPTV_ORG_CACHE_LOADED_AT = now
            except Exception as exc:
                _log_failure("fetch iptv-org playlist", exc)
            finally:
                if owns_client:
                    await client.aclose()
            return scrapers._IPTV_ORG_CACHE

    async def search(self, query_or_terms, browser: Optional[Browser] = None, http_client: Optional[httpx.AsyncClient] = None) -> List[dict]:
        search_terms = list(query_or_terms) if isinstance(query_or_terms, list) else [str(query_or_terms)]
        entries = await self._get_entries(http_client)
        if not entries:
            telemetry.report_page_error("no_content")
            return []
        telemetry.report_index_events(len(entries))

        prefs = stream_language.preferred_languages()
        only = stream_language.language_mode() == "only"
        # The language index lists a stream once per language it is in: merge
        # those into one candidate carrying every language.
        merged: dict = {}
        for entry in entries:
            name_text = f"{entry.get('tvg_name', '')} {entry.get('display_name', '')}"
            if not _channel_term_matches(name_text, search_terms):
                continue
            url = _validate_upstream_url(entry.get("url", ""))
            if not url:
                continue
            item = merged.setdefault(url, {"entry": entry, "languages": []})
            code = stream_language.code_for(entry.get("group_title"))
            if code and code not in item["languages"]:
                item["languages"].append(code)

        ranked = []
        for url, item in merged.items():
            entry = item["entry"]
            title = entry.get("tvg_name") or entry.get("display_name") or ""
            candidate = {
                "url": url,
                "referer": "",
                "origin": "",
                "provider": self.name,
                # match_team() scores a confident exact-word match 95-100 (110 only for
                # multi-word phrases); _channel_term_matches() above is at least that
                # precise: it's a word-boundary match against curated official channel
                # names with explicit ESPN/ESPN2/ESPN+ disambiguation, not noisy fuzzy
                # anchor text, so this shouldn't be scored as a weaker match than that.
                "match_score": 100,
                "match_title": title,
                "discovery_method": "http",
                "language": list(item["languages"]),
            }
            if only and stream_language.is_excluded(candidate, prefs, "only"):
                continue
            ranked.append((_entry_rank(candidate, entry, prefs), candidate))
        ranked.sort(key=lambda pair: pair[0])
        candidates = [candidate for _, candidate in ranked[:IPTV_ORG_VERIFY_LIMIT]]

        telemetry.report_matches(len(candidates))
        if not candidates:
            return []
        return await _verify_provider_streams(candidates, http_client)
