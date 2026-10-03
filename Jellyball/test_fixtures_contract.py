"""Parser contract tests driven only by the stored fixtures in Jellyball/fixtures/.

They pin what the parsers extract from third-party data so a refactor cannot
silently change it. Everything is offline and deterministic: HTTP goes through
httpx.MockTransport, DNS is stubbed, Playwright is replaced by scripted fakes
built on the ones in test_scraper_hardening.py, and "now" is fixed.

The provider fixtures are hand-authored skeletons (see fixtures/README.md):
they prove regression stability, not that a real site still matches. The
TheTVApp and DaddyLive network logs are scripted too; only a real browser can
show what a real player requests, so that part is not covered.
"""
import asyncio
import copy
import json
import re
import socket
import sys
import time
import unittest
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))

import catalog
import epg
import espn_schedule
import scrapers
import state
import stream_extractor
from network_safety import clear_dns_cache
from sports_catalog import normalize_team_label, parse_espn_team_directory
from sports_matcher import get_team_search_terms
from test_scraper_hardening import FakeBrowser, FakeContext, FakePage

FIXTURES = Path(__file__).resolve().parent / "fixtures"
TS_SEGMENT = (b"\x47" + bytes(187)) * 4
_REAL_SLEEP = asyncio.sleep


def fixture_text(*parts):
    return FIXTURES.joinpath(*parts).read_text(encoding="utf-8")


def fixture_json(*parts):
    return json.loads(fixture_text(*parts))


def _public_getaddrinfo(host, port, *args, **kwargs):
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", port))]


async def _instant_sleep(delay, result=None):
    await _REAL_SLEEP(0)
    return result


class OfflineCase(unittest.IsolatedAsyncioTestCase):
    """Stub DNS and reset the scraper module caches around every test."""

    def setUp(self):
        clear_dns_cache()
        patcher = patch.object(socket, "getaddrinfo", side_effect=_public_getaddrinfo)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(clear_dns_cache)
        overrides = patch.dict(scrapers._PROVIDER_BASE_URL_OVERRIDES, {}, clear=True)
        overrides.start()
        self.addCleanup(overrides.stop)
        scrapers._SCRAPE_INDEX_CACHE.clear()
        scrapers._SCRAPE_INDEX_INFLIGHT.clear()
        self.addCleanup(scrapers._SCRAPE_INDEX_CACHE.clear)
        self.addCleanup(scrapers._SCRAPE_INDEX_INFLIGHT.clear)


# --------------------------------------------------------------------------- #
# A fake internet made of fixtures
# --------------------------------------------------------------------------- #
class FixtureSite:
    """Serves fixtures through httpx.MockTransport and records every request."""

    def __init__(self):
        self.pages = {}
        self.requests = []

    def add(self, host, path, *fixture, status=200, content_type="text/html; charset=utf-8"):
        self.pages[(host, path)] = (status, content_type, fixture_text(*fixture).encode("utf-8"))

    def handler(self, request):
        self.requests.append(request)
        key = (request.url.host, request.url.path)
        if key in self.pages:
            status, content_type, body = self.pages[key]
            return httpx.Response(status, headers={"content-type": content_type}, content=body)
        path = request.url.path
        mpegurl = {"content-type": "application/vnd.apple.mpegurl"}
        if path.endswith(".ts"):
            return httpx.Response(200, headers={"content-type": "video/mp2t"}, content=TS_SEGMENT)
        if path.endswith(".m3u8") or path.startswith("/manifest/"):
            if "/dead/" in path:
                return httpx.Response(404)
            name = "vod_replay" if "/replay/" in path else "master" if path.endswith("master.m3u8") else "live_media"
            return httpx.Response(200, headers=mpegurl, text=fixture_text("providers", "hls", name + ".m3u8"))
        return httpx.Response(404)

    def client(self):
        return httpx.AsyncClient(transport=httpx.MockTransport(self.handler))

    def requested(self, host, path):
        return any((r.url.host, r.url.path) == (host, path) for r in self.requests)


def json_client(payload, status=200, seen=None):
    def handler(request):
        if seen is not None:
            seen.append(request)
        return httpx.Response(status, json=payload)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# --------------------------------------------------------------------------- #
# ESPN
# --------------------------------------------------------------------------- #
FAKE_NOW = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)


class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return FAKE_NOW if tz is None else FAKE_NOW.astimezone(tz)


def shifted(document, offsets):
    """Copy of an ESPN schedule whose events sit at FAKE_NOW + offsets (same date format)."""
    result = copy.deepcopy(document)
    for event, offset in zip(result["events"], offsets):
        stamp = (FAKE_NOW + offset).strftime("%Y-%m-%dT%H:%MZ")
        event["date"] = stamp
        for competition in event.get("competitions", []):
            competition["date"] = stamp
    return result


class EspnScheduleContractTests(unittest.IsolatedAsyncioTestCase):
    NFL = fixture_json("espn", "nfl_team_schedule_buf.json")
    MLS = fixture_json("espn", "mls_team_schedule_inter_miami.json")
    FAR = timedelta(days=40)

    async def fetch(self, document, status=200, **kwargs):
        seen = []
        client = json_client(document, status, seen)
        try:
            with patch.object(state, "SHARED_HTTP_CLIENT", client), patch.object(catalog, "datetime", FrozenDatetime), \
                    patch.object(espn_schedule, "datetime", FrozenDatetime):
                result = await catalog.fetch_espn_team_schedule("Test Team", **kwargs)
        finally:
            await client.aclose()
        return result, seen

    def test_fixtures_have_the_shape_the_parser_reads(self):
        for document in (self.NFL, self.MLS):
            self.assertTrue(1 <= len(document["events"]) <= 3)
            for event in document["events"]:
                self.assertRegex(event["date"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}Z$")
                datetime.fromisoformat(event["date"].replace("Z", "+00:00"))

    async def test_nfl_picks_the_next_game_inside_the_window(self):
        document = shifted(self.NFL, [timedelta(hours=-72), timedelta(hours=48), timedelta(days=20)])
        (start, stop, ok), seen = await self.fetch(document, category="nfl", source_id="buf")

        self.assertTrue(ok)
        self.assertEqual(start, FAKE_NOW + timedelta(hours=48))
        self.assertEqual(stop - start, timedelta(hours=4))
        self.assertEqual(str(seen[0].url), "https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/buf/schedule")

    async def test_mls_uses_the_soccer_endpoint_and_duration_whatever_the_event_order(self):
        # ESPN lists this team's events newest first; the parser must not depend on order.
        document = shifted(self.MLS, [timedelta(days=20), timedelta(hours=48), timedelta(hours=-72)])
        (start, stop, ok), seen = await self.fetch(document, category="soccer", source_id="20232")

        self.assertTrue(ok)
        self.assertEqual(start, FAKE_NOW + timedelta(hours=48))
        self.assertEqual(stop - start, timedelta(hours=2.5))
        self.assertEqual(seen[0].url.path, "/apis/site/v2/sports/soccer/usa.1/teams/20232/schedule")

    async def test_window_edges(self):
        cases = [
            (timedelta(hours=-5, minutes=-30), False),  # ended more than an hour ago
            (timedelta(hours=-4, minutes=-30), True),  # ended 30 minutes ago
            (timedelta(hours=-1), True),  # in progress
            (timedelta(days=13, hours=23), True),
            (timedelta(days=14, hours=1), False),  # beyond the 14 day horizon
        ]
        for offset, included in cases:
            with self.subTest(offset=offset):
                document = shifted(self.NFL, [offset, self.FAR, self.FAR * 2])
                (start, _, ok), _ = await self.fetch(document, category="nfl", source_id="buf")
                self.assertTrue(ok)
                self.assertEqual(start, FAKE_NOW + offset if included else None)

    async def test_no_game_in_window_is_a_successful_lookup(self):
        document = shifted(self.NFL, [self.FAR, self.FAR * 2, self.FAR * 3])
        (result, _) = await self.fetch(document, category="nfl", source_id="buf")
        self.assertEqual(result, (None, None, True))

    async def test_http_error_is_a_failed_lookup(self):
        (result, _) = await self.fetch({"events": []}, status=503, category="nfl", source_id="buf")
        self.assertEqual(result, (None, None, False))


class EspnDirectoryContractTests(unittest.IsolatedAsyncioTestCase):
    NFL = fixture_json("espn", "nfl_teams.json")
    COLLEGE = fixture_json("espn", "college_football_teams.json")

    def test_nfl_directory_records(self):
        records = parse_espn_team_directory(self.NFL, "nfl")
        self.assertEqual(len(records), 3)
        first = records[0]
        self.assertEqual(
            (first.canonical, first.category, first.slug, first.logo_sport, first.team_id, first.aliases),
            ("arizona cardinals", "nfl", "arizona-cardinals", "nfl", "22", ("ari", "arizona", "arizona cardinals", "cardinals")),
        )
        teams = [entry["team"] for entry in self.NFL["sports"][0]["leagues"][0]["teams"]]
        for record, team in zip(records, teams):
            self.assertEqual(record.team_id, team["id"])
            self.assertEqual(record.slug, team["slug"])
            self.assertEqual(record.canonical, normalize_team_label(team["displayName"]))
            for value in (team["abbreviation"], team["location"], team["name"]):
                self.assertIn(normalize_team_label(value), record.aliases)

    def test_college_directory_records_are_college_scoped(self):
        records = parse_espn_team_directory(self.COLLEGE, "ncaaf")
        self.assertEqual(len(records), 3)
        for record in records:
            self.assertEqual((record.category, record.logo_sport), ("college", "ncaa"))
            self.assertTrue(record.team_id.isdigit() and record.slug)
            self.assertEqual(record.for_category("ncaaf").category, "ncaaf")

    async def test_directory_fetch_requests_the_league_endpoint_and_scopes_records(self):
        seen = []
        async with json_client(self.COLLEGE, seen=seen) as client:
            records = await catalog._fetch_espn_directory("ncaaf", client)
        self.assertEqual(seen[0].url.path, "/apis/site/v2/sports/football/college-football/teams")
        self.assertEqual(seen[0].url.params["limit"], "1000")
        self.assertEqual([r.category for r in records], ["ncaaf"] * 3)

    async def test_bad_directory_responses_yield_nothing(self):
        for status, payload in ((503, {}), (200, {"unexpected": "shape"}), (200, ["not", "a", "dict"])):
            with self.subTest(status=status):
                async with json_client(payload, status) as client:
                    self.assertEqual(await catalog._fetch_espn_directory("nfl", client), ())


# --------------------------------------------------------------------------- #
# TVGuide
# --------------------------------------------------------------------------- #
class TvGuideContractTests(unittest.IsolatedAsyncioTestCase):
    DOCUMENT = fixture_json("tvguide", "schedule.json")

    def setUp(self):
        saved = (epg._TVGUIDE_EPG_CACHE, epg._TVGUIDE_EPG_CACHED_AT)
        epg._TVGUIDE_EPG_CACHE, epg._TVGUIDE_EPG_CACHED_AT = {}, 0.0
        self.addCleanup(lambda: (setattr(epg, "_TVGUIDE_EPG_CACHE", saved[0]), setattr(epg, "_TVGUIDE_EPG_CACHED_AT", saved[1])))

    async def fetch(self, document, status=200):
        seen = []
        client = json_client(document, status, seen)
        try:
            with patch.object(state, "SHARED_HTTP_CLIENT", client):
                return await epg._fetch_tvguide_epg(), seen
        finally:
            await client.aclose()

    def test_fixture_shape(self):
        items = self.DOCUMENT["data"]["items"]
        self.assertEqual(len(items), 2)
        for item in items:
            self.assertIn(str(item["channel"]["sourceId"]), epg.TVGUIDE_SPECIAL_CHANNEL_IDS)
            self.assertEqual(len(item["programSchedules"]), 4)

    async def test_special_channels_are_keyed_by_tvg_id_and_unknown_ones_ignored(self):
        document = copy.deepcopy(self.DOCUMENT)
        decoy = copy.deepcopy(document["data"]["items"][0])
        decoy["channel"]["sourceId"] = 1
        document["data"]["items"].append(decoy)

        schedules, seen = await self.fetch(document)

        expected = {
            epg.TVGUIDE_SPECIAL_CHANNEL_IDS[str(item["channel"]["sourceId"])]: item["programSchedules"]
            for item in self.DOCUMENT["data"]["items"]
        }
        self.assertEqual(schedules, expected)
        self.assertEqual(set(schedules), {"ESPN.us", "FoxSports1.us"})

        request = seen[0]
        self.assertEqual(request.url.host, "backend.tvguide.com")
        self.assertEqual(request.url.path, "/tvschedules/tvguide/9100001138/web")
        self.assertEqual(request.url.params["duration"], str(epg.GUIDE_HORIZON_DAYS * 1440))
        self.assertLess(abs(int(request.url.params["start"]) - time.time()), 60)
        self.assertEqual(request.headers["referer"], "https://www.tvguide.com/")
        self.assertEqual(request.headers["user-agent"], stream_extractor.DEFAULT_USER_AGENT)

    async def test_repeat_calls_are_served_from_cache(self):
        _, first = await self.fetch(self.DOCUMENT)
        schedules, second = await self.fetch({"data": {"items": []}})
        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])  # no request: the cache answered
        self.assertEqual(set(schedules), {"ESPN.us", "FoxSports1.us"})

    async def test_bad_responses_leave_the_guide_empty(self):
        for status, payload in ((503, {}), (200, {"data": {}}), (200, {"data": {"items": [{"channel": {"sourceId": 1}}]}})):
            with self.subTest(status=status):
                schedules, _ = await self.fetch(payload, status)
                self.assertEqual(schedules, {})

    async def test_always_live_channel_programmes(self):
        schedules, _ = await self.fetch(self.DOCUMENT)
        programs = schedules["ESPN.us"]
        utc = timezone.utc
        first_start = datetime.fromtimestamp(programs[0]["startTime"], utc)
        last_end = datetime.fromtimestamp(programs[-1]["endTime"], utc)
        data = {"name": "ESPN", "always_live": True, "tvg_id": "ESPN.us", "category": "special"}

        full = epg._channel_programmes("special_espn", data, first_start - timedelta(hours=1), last_end + timedelta(hours=1), schedules)
        self.assertEqual([p["title"] for p in full], [p["title"] for p in programs])
        self.assertEqual([p["start"] for p in full], [datetime.fromtimestamp(p["startTime"], utc) for p in programs])
        self.assertEqual([p["stop"] for p in full], [datetime.fromtimestamp(p["endTime"], utc) for p in programs])
        self.assertTrue(all(p["category"] == "Sports" and p["desc"] for p in full))

        # A window opening inside the second programme drops the first and clips the second.
        mid = datetime.fromtimestamp(programs[1]["startTime"] + 60, utc)
        clipped = epg._channel_programmes("special_espn", data, mid, last_end + timedelta(hours=1), schedules)
        self.assertEqual([p["title"] for p in clipped], [p["title"] for p in programs[1:]])
        self.assertEqual(clipped[0]["start"], mid)

    def test_channel_without_guide_data_gets_a_placeholder_block(self):
        start, end = FAKE_NOW, FAKE_NOW + timedelta(hours=6)
        live = {"name": "ESPN", "always_live": True, "tvg_id": "ESPN.us", "category": "special"}
        blocks = epg._channel_programmes("special_espn", live, start, end, {})
        self.assertEqual([(b["title"], b["start"], b["stop"]) for b in blocks], [("ESPN Live", start, end)])
        team = {"name": "Buffalo Bills", "category": "nfl"}
        self.assertEqual(epg._channel_programmes("nfl_buf", team, start, end, {})[0]["title"], "Buffalo Bills Standby")


# --------------------------------------------------------------------------- #
# Provider fixtures: hygiene
# --------------------------------------------------------------------------- #
class FixtureHygieneTests(unittest.TestCase):
    URL_HOST = re.compile(r"(?:[A-Za-z][A-Za-z0-9+.-]*:)?(?:\\?/){2}([A-Za-z0-9.-]+)")
    BARE_HOST = re.compile(r"\b[a-z0-9-]+(?:\.[a-z0-9-]+)*\.(?:com|net|org|io|tv|ws|su|pk|im|to|sx|xyz|top|live|click|plus|me|cc|co)\b", re.I)
    PUBLIC_API_HOSTS = {
        "a.espncdn.com", "www.espn.com", "m.espn.com", "sports.core.api.espn.pvt", "x-callback-url", "backend.tvguide.com",
    }

    def hosts(self, path):
        text = path.read_text(encoding="utf-8")
        return set(self.URL_HOST.findall(text)) | set(self.BARE_HOST.findall(text))

    def test_provider_fixtures_only_use_example_test_hosts(self):
        files = [p for p in (FIXTURES / "providers").rglob("*") if p.is_file()]
        self.assertGreater(len(files), 15)
        for path in files:
            for host in self.hosts(path):
                with self.subTest(file=path.name, host=host):
                    self.assertTrue(host == "example.test" or host.endswith(".example.test"), host)

    def test_api_fixtures_only_mention_the_apis_they_came_from(self):
        for path in list((FIXTURES / "espn").glob("*.json")) + list((FIXTURES / "tvguide").glob("*.json")):
            self.assertLessEqual(self.hosts(path), self.PUBLIC_API_HOSTS, path.name)


# --------------------------------------------------------------------------- #
# Aggregator providers
# --------------------------------------------------------------------------- #
AGG_HOST = "agg.example.test"
AGG_BASE = "https://" + AGG_HOST
GATORS_LINKS = {
    "watch": "/watch/florida-gators-vs-georgia-bulldogs-live",
    "event": "/event/game-4402",
    "title": "/title-game/game-4403",
    "cfb": "/cfb/game-4404",
    "game": "/game/florida-gators-vs-texas-longhorns",
    "match": "/match/game-4406",
    "live": "/live/florida-gators-vs-kentucky-wildcats-4407",
    "stream": "/stream/florida-gators-vs-missouri-tigers-4408",
}
# Each subclass differs only in its URL hints, so one index serves all eight.
EXPECTED_LINKS = {
    scrapers.ISportSurgeScraper: ("watch", "event", "title"),
    scrapers.MyBuffStreamsScraper: ("watch", "title", "cfb"),
    scrapers.MethStreamsScraper: ("game", "match", "live"),
    scrapers.StreamEastScraper: ("match", "live", "stream"),
    scrapers.FootybiteScraper: ("watch", "match", "live", "stream"),
    scrapers.OneStreamScraper: ("match", "live", "stream"),
    scrapers.StreamedSuScraper: ("watch", "live"),
    scrapers.TopStreamsScraper: ("watch", "match", "live"),
}
OTHER_LINKS = (
    "/watch/ohio-state-buckeyes-vs-michigan-wolverines-live",
    "/match/florida-state-seminoles-vs-clemson-tigers-4410",
    "/live/florida-panthers-vs-tampa-bay-lightning-4411",
    "/stream/buffalo-bills-vs-new-york-jets-4412",
    "/watch/miami-dolphins-vs-new-england-patriots-4501",
    "/live/dallas-cowboys-vs-philadelphia-eagles-4502",
    "/schedule/florida-gators",
)
PLAYER_1 = "https://player.example.test/embed/gators-1"
PLAYER_2 = "https://embed.example.test/frame/gators-2"
PLAYER_NESTED = "https://embed.example.test/frame/gators-nested"
STREAMS = {
    "direct": "https://cdn.example.test/live/gators-georgia/index.m3u8?token=fixture-token&expires=1999999999",
    "jw": "https://cdn.example.test/hls/gators-alt/master.m3u8",
    "b64": "https://cdn.example.test/live/gators-b64/playlist.m3u8",
    "proto": "https://cdn.example.test/live/gators-proto/index.m3u8",
    "nested": "https://embed.example.test/live/gators-nested/index.m3u8",
}
REPLAY = "https://cdn.example.test/replay/gators-2025/index.m3u8"
GATORS_TERMS = get_team_search_terms("Florida Gators", "Florida Gators")


def event_pairs(event_url):
    return {
        (STREAMS["direct"], event_url), (STREAMS["jw"], PLAYER_1), (STREAMS["b64"], PLAYER_2),
        (STREAMS["proto"], PLAYER_2), (STREAMS["nested"], PLAYER_NESTED),
    }


def aggregator_site():
    site = FixtureSite()
    site.add(AGG_HOST, "/", "providers", "aggregator", "index.html")
    for scraper_type in EXPECTED_LINKS:
        for category in scraper_type().categories:
            site.add(AGG_HOST, category, "providers", "aggregator", "category.html")
    for path in GATORS_LINKS.values():
        site.add(AGG_HOST, path, "providers", "aggregator", "event.html")
    for path in OTHER_LINKS:
        site.add(AGG_HOST, path, "providers", "aggregator", "event_other_game.html")
    site.add("player.example.test", "/embed/gators-1", "providers", "aggregator", "player_embed_1.html")
    site.add("embed.example.test", "/frame/gators-2", "providers", "aggregator", "player_embed_2.html")
    site.add("embed.example.test", "/frame/gators-nested", "providers", "aggregator", "player_nested.html")
    return site


def make(scraper_type):
    scraper = scraper_type()
    scraper.base_url = AGG_BASE
    return scraper


class AggregatorContractTests(OfflineCase):
    def test_every_aggregator_subclass_is_covered(self):
        linear = {scrapers.TheTVAppScraper, scrapers.DaddyLiveScraper}
        self.assertEqual(set(scrapers.HtmlAggregatorScraper.__subclasses__()), set(EXPECTED_LINKS) | linear)

    async def test_index_parsing_per_subclass(self):
        for scraper_type, keys in EXPECTED_LINKS.items():
            with self.subTest(scraper=scraper_type.__name__):
                scrapers._SCRAPE_INDEX_CACHE.clear()
                site, scraper = aggregator_site(), make(scraper_type)
                async with site.client() as client:
                    matches = await scraper._find_matches(client, GATORS_TERMS)

                urls = [url for url, _, _ in matches]
                self.assertEqual(sorted(urls), sorted(AGG_BASE + GATORS_LINKS[k] for k in keys))
                self.assertEqual(len(urls), len(set(urls)))  # pages repeating a link do not duplicate it
                self.assertTrue(all(score >= 95 for _, score, _ in matches))
                scanned = {(r.url.host, r.url.path or "/") for r in site.requests}
                for scan_url in scraper.get_scan_urls():
                    parsed = urllib.parse.urlsplit(scan_url)
                    self.assertIn((parsed.hostname, parsed.path or "/"), scanned)

    async def test_other_teams_never_match(self):
        for scraper_type in EXPECTED_LINKS:
            with self.subTest(scraper=scraper_type.__name__):
                scrapers._SCRAPE_INDEX_CACHE.clear()
                async with aggregator_site().client() as client:
                    matches = await make(scraper_type)._find_matches(client, GATORS_TERMS)
                matched_paths = {urllib.parse.urlsplit(url).path for url, _, _ in matches}
                self.assertFalse(matched_paths & set(OTHER_LINKS))

    async def test_filter_is_selective_not_broken(self):
        terms = get_team_search_terms("Ohio State Buckeyes", "Ohio State Buckeyes")
        async with aggregator_site().client() as client:
            matches = await make(scrapers.ISportSurgeScraper)._find_matches(client, terms)
        self.assertEqual([url for url, _, _ in matches], [AGG_BASE + OTHER_LINKS[0]])

    async def test_search_end_to_end_per_subclass(self):
        for scraper_type, keys in EXPECTED_LINKS.items():
            with self.subTest(scraper=scraper_type.__name__):
                scrapers._SCRAPE_INDEX_CACHE.clear()
                site, scraper = aggregator_site(), make(scraper_type)
                async with site.client() as client:
                    streams = await scraper.search(GATORS_TERMS, http_client=client)

                event_urls = {AGG_BASE + GATORS_LINKS[k] for k in keys}
                expected = set().union(*(event_pairs(url) for url in event_urls))
                self.assertEqual({(s["url"], s["referer"]) for s in streams}, expected)
                self.assertEqual({s["match_url"] for s in streams}, event_urls)
                self.assertEqual({s["provider"] for s in streams}, {scraper.name})
                self.assertEqual({s["discovery_method"] for s in streams}, {"http"})
                self.assertNotIn(REPLAY, {s["url"] for s in streams})  # finished playlist is not live
                self.assertFalse(any("osu-michigan" in s["url"] for s in streams))
                for path in OTHER_LINKS:
                    self.assertFalse(site.requested(AGG_HOST, path), path)
                # master_scrape's event filter judges the link text and URL: a team-named
                # event passes (generic "Watch" links to /event/game-4402 are not judged here).
                named = [s for s in streams if s["match_url"].endswith(GATORS_LINKS["watch"])]
                self.assertEqual(bool(named), "watch" in keys)
                self.assertTrue(all(scrapers._stream_matches_requested_event(s, GATORS_TERMS) for s in named))

    def test_stream_from_another_event_is_rejected_by_the_event_filter(self):
        other = {"url": "https://cdn.example.test/live/osu-michigan/index.m3u8", "match_title": "Ohio State Buckeyes vs Michigan Wolverines",
                 "match_url": AGG_BASE + OTHER_LINKS[0]}
        self.assertFalse(scrapers._stream_matches_requested_event(other, GATORS_TERMS))

    async def test_blocked_http_falls_back_to_the_rendered_page(self):
        site = aggregator_site()
        site.add(AGG_HOST, "/", "providers", "common", "cloudflare_challenge.html", status=403)
        browser = ScriptedBrowser({AGG_BASE: fixture_text("providers", "aggregator", "index.html")})
        scraper = make(scrapers.MethStreamsScraper)
        async with site.client() as client:
            matches = await scraper._find_matches(client, GATORS_TERMS, browser=browser)
        self.assertEqual(sorted(u for u, _, _ in matches), sorted(AGG_BASE + GATORS_LINKS[k] for k in ("game", "match", "live")))
        self.assertEqual(browser.scripted_pages[0].visited, [AGG_BASE])

    async def test_challenge_page_served_as_200_yields_no_matches(self):
        site = aggregator_site()
        site.add(AGG_HOST, "/", "providers", "common", "cloudflare_challenge.html")
        async with site.client() as client:
            self.assertEqual(await make(scrapers.MethStreamsScraper)._find_matches(client, GATORS_TERMS), [])


class ExtractionContractTests(OfflineCase):
    def test_candidate_extraction_per_page_style(self):
        event_url = AGG_BASE + GATORS_LINKS["watch"]
        cases = [
            (("aggregator", "event.html"), event_url, {STREAMS["direct"], REPLAY}),
            (("aggregator", "player_embed_1.html"), PLAYER_1, {STREAMS["jw"]}),
            (("aggregator", "player_embed_2.html"), PLAYER_2, {STREAMS["b64"], STREAMS["proto"]}),
            (("aggregator", "player_nested.html"), PLAYER_NESTED, {STREAMS["nested"]}),
            (("aggregator", "streamer_thestreameast.html"), "https://thestreameast.example.test/stream/gators-georgia",
             {"https://cdn.example.test/live/streameast-gators/index.m3u8"}),
            (("aggregator", "streamer_buffstream.html"), "https://buffstream.example.test/live/gators-georgia",
             {"https://cdn.example.test/live/buffstream-gators/playlist.m3u8"}),
            (("aggregator", "event_other_game.html"), AGG_BASE + OTHER_LINKS[0], {"https://cdn.example.test/live/osu-michigan/index.m3u8"}),
        ]
        for parts, referer, expected in cases:
            with self.subTest(page=parts[-1]):
                found = stream_extractor.extract_streams_from_text(fixture_text("providers", *parts), referer)
                self.assertEqual(set(found), expected)

    def test_iframes_and_streamer_links(self):
        event_url = AGG_BASE + GATORS_LINKS["watch"]
        soup = stream_extractor.make_soup(fixture_text("providers", "aggregator", "event.html"))
        self.assertEqual(stream_extractor.extract_iframes_and_streamers(soup, event_url), ([PLAYER_1, PLAYER_2], []))

        page = AGG_BASE + "/watch/florida-gators-vs-georgia-bulldogs-streamers"
        soup = stream_extractor.make_soup(fixture_text("providers", "aggregator", "event_streamer_links.html"))
        iframes, streamers = stream_extractor.extract_iframes_and_streamers(soup, page)
        self.assertEqual(iframes, [])
        self.assertEqual(streamers, ["https://thestreameast.example.test/stream/gators-georgia", "https://buffstream.example.test/live/gators-georgia"])

    async def test_streamer_links_are_followed_when_a_page_has_no_iframes(self):
        page = AGG_BASE + "/watch/florida-gators-vs-georgia-bulldogs-streamers"
        site = FixtureSite()
        site.add(AGG_HOST, "/watch/florida-gators-vs-georgia-bulldogs-streamers", "providers", "aggregator", "event_streamer_links.html")
        site.add("thestreameast.example.test", "/stream/gators-georgia", "providers", "aggregator", "streamer_thestreameast.html")
        site.add("buffstream.example.test", "/live/gators-georgia", "providers", "aggregator", "streamer_buffstream.html")
        async with site.client() as client:
            streams = await stream_extractor.fetch_streams_from_page(client, page, "Fixture", 100, "Gators")
        self.assertEqual(
            {(s["url"], s["referer"]) for s in streams},
            {("https://cdn.example.test/live/streameast-gators/index.m3u8", page),
             ("https://cdn.example.test/live/buffstream-gators/playlist.m3u8", page)},
        )
        self.assertFalse(site.requested("sponsor.example.test", "/offer"))


# --------------------------------------------------------------------------- #
# IPTV-Org M3U
# --------------------------------------------------------------------------- #
class IptvOrgContractTests(OfflineCase):
    PLAYLIST = fixture_text("providers", "iptv_org", "sports.m3u")

    def setUp(self):
        super().setUp()
        saved = (scrapers._IPTV_ORG_CACHE, scrapers._IPTV_ORG_CACHE_LOADED_AT)
        scrapers._IPTV_ORG_CACHE, scrapers._IPTV_ORG_CACHE_LOADED_AT = [], 0.0
        self.addCleanup(lambda: (setattr(scrapers, "_IPTV_ORG_CACHE", saved[0]), setattr(scrapers, "_IPTV_ORG_CACHE_LOADED_AT", saved[1])))

    def test_parser_reads_every_entry(self):
        entries = scrapers._parse_m3u_playlist(self.PLAYLIST)
        self.assertEqual(len(entries), self.PLAYLIST.count("#EXTINF"))
        self.assertEqual(entries[1], {
            "tvg_id": "ESPN.us@SD", "tvg_name": "ESPN (720p)", "group_title": "Sports",
            "display_name": "ESPN (720p)", "url": "https://iptv.example.test/espn/index.m3u8",
        })
        fs1 = next(e for e in entries if e["tvg_id"].startswith("FoxSports1"))
        self.assertEqual((fs1["tvg_name"], fs1["display_name"]), ("FS1", "Fox Sports 1 (1080p)"))
        nfl = next(e for e in entries if e["tvg_id"].startswith("NFLNetwork"))
        self.assertEqual(nfl["url"], "https://iptv.example.test/nfl-network/index.m3u8")  # option lines skipped
        self.assertEqual((entries[-1]["tvg_id"], entries[-1]["group_title"], entries[-1]["display_name"]), ("", "", "Local Sports 7 (480p)"))

    def test_parser_accepts_crlf_line_endings(self):
        self.assertEqual(scrapers._parse_m3u_playlist(self.PLAYLIST.replace("\n", "\r\n")), scrapers._parse_m3u_playlist(self.PLAYLIST))

    async def search(self, terms):
        site = FixtureSite()
        site.add("playlist.example.test", "/sports.m3u", "providers", "iptv_org", "sports.m3u", content_type="audio/x-mpegurl")
        scraper = scrapers.IptvOrgScraper()
        scraper.base_url = "https://playlist.example.test/sports.m3u"
        async with site.client() as client:
            return await scraper.search(terms, http_client=client)

    async def test_search_disambiguates_channels_and_verifies_streams(self):
        results = await self.search(["espn"])
        # 720p and 480p are live; 360p is dead, the RTMP entry is not HTTP, ESPN2/U/News/+ are other channels.
        self.assertEqual(
            [r["url"] for r in results],
            ["https://iptv.example.test/espn/index.m3u8", "https://iptv.example.test/espn-480p/index.m3u8"],
        )
        self.assertEqual({r["provider"] for r in results}, {"IPTV-Org"})
        self.assertTrue(all(r["match_score"] >= 95 for r in results))

    async def test_search_other_channels(self):
        cases = [
            (["espn2"], ["https://iptv.example.test/espn2/index.m3u8"]),
            (["fs1", "fox sports 1"], ["https://iptv.example.test/fs1/index.m3u8"]),
            (["nfl network"], ["https://iptv.example.test/nfl-network/index.m3u8"]),
            (["no such channel"], []),
        ]
        for terms, urls in cases:
            with self.subTest(terms=terms):
                scrapers._IPTV_ORG_CACHE, scrapers._IPTV_ORG_CACHE_LOADED_AT = [], 0.0
                self.assertEqual([r["url"] for r in await self.search(terms)], urls)


# --------------------------------------------------------------------------- #
# Linear providers (scripted Playwright)
# --------------------------------------------------------------------------- #
class ScriptedRequest:
    def __init__(self, entry):
        self.url = entry["url"]
        self.headers = entry.get("headers", {})
        frame_url = entry.get("frame_url")
        self.frame = type("Frame", (), {"url": frame_url})() if frame_url else None


class ScriptedMouse:
    async def click(self, x, y):
        return None


class ScriptedFrame:
    mouse = ScriptedMouse()


class ScriptedPage(FakePage):
    """FakePage plus the slice of Playwright's Page API the providers use.

    content() serves stored HTML; navigating to a URL replays its stored network
    log to the "request" listeners. Scripted, so it shows nothing about a real site.
    """

    def __init__(self, documents, network_logs):
        super().__init__()
        self.documents, self.network_logs = documents, network_logs
        self.visited, self.listeners = [], []
        self.url = ""
        self.frames = [ScriptedFrame(), ScriptedFrame()]
        self.mouse = ScriptedMouse()

    def on(self, event, handler):
        if event == "request":
            self.listeners.append(handler)

    async def goto(self, url, **kwargs):
        self.url = url
        self.visited.append(url)
        for entry in self.network_logs.get(url, []):
            for listener in self.listeners:
                listener(ScriptedRequest(entry))

    async def wait_for_load_state(self, *args, **kwargs):
        return None

    async def content(self):
        return self.documents.get(self.url, "<html></html>")


class ScriptedContext(FakeContext):
    def __init__(self, browser):
        super().__init__()
        self.browser = browser

    async def new_page(self):
        self.page = self.browser.make_page()
        return self.page


class ScriptedBrowser(FakeBrowser):
    def __init__(self, documents=None, network_logs=None):
        super().__init__()
        self.documents, self.network_logs, self.scripted_pages = documents or {}, network_logs or {}, []

    def make_page(self):
        page = ScriptedPage(self.documents, self.network_logs)
        self.scripted_pages.append(page)
        return page

    async def new_context(self, user_agent=None):
        context = ScriptedContext(self)
        self.contexts.append(context)
        return context

    async def new_page(self):
        page = self.make_page()
        self.pages.append(page)
        return page


class TheTVAppContractTests(OfflineCase):
    BASE = "https://thetvapp.example.test"
    DIRECTORY = fixture_text("providers", "thetvapp", "tv.html")
    LOG = fixture_json("providers", "thetvapp", "network_log_espn.json")

    def scraper(self):
        scraper = scrapers.TheTVAppScraper()
        scraper.base_url = self.BASE
        return scraper

    def test_best_channel_for_each_search(self):
        cases = {
            "espn": "/tv/espn-live-stream/",  # not ESPN2, ESPNU or ESPN Deportes
            "espn2": "/tv/espn2-live-stream/",
            "fs1": "/tv/fs1-live-stream/",
            "nfl network": "/tv/nfl-network-live-stream/",
            "abc": "/tv/abc-live-stream/",  # not ABC News
        }
        for term, path in cases.items():
            with self.subTest(term=term):
                url, score, _ = self.scraper()._find_best_channel_match(self.DIRECTORY, [term], [term])
                self.assertEqual(url, self.BASE + path)
                self.assertGreaterEqual(score, 120)

    def test_unknown_channel_matches_nothing(self):
        self.assertEqual(self.scraper()._find_best_channel_match(self.DIRECTORY, ["curling"], ["curling"]), (None, 0, ""))

    async def test_search_captures_only_playlist_requests_and_verifies_them(self):
        browser = ScriptedBrowser({self.BASE + "/tv/": self.DIRECTORY}, {self.LOG["page_url"]: self.LOG["requests"]})
        site = FixtureSite()
        async with site.client() as client:
            results = await self.scraper().search(["espn"], browser=browser, http_client=client)

        self.assertEqual(len(results), 1)  # the .ts, .js and .gif requests and the duplicate are dropped
        stream = results[0]
        self.assertEqual(stream["url"], "https://edge.example.test/hls/espn/index.m3u8?md5=fixture&expires=1999999999")
        self.assertEqual(stream["referer"], self.LOG["page_url"])
        self.assertEqual(stream["origin"], self.BASE)
        self.assertEqual((stream["provider"], stream["discovery_method"]), ("TheTVApp", "playwright"))
        self.assertIn("ESPN", stream["match_title"])
        self.assertEqual([p.visited for p in browser.scripted_pages], [[self.BASE + "/tv/", self.LOG["page_url"]]])

    async def test_cloudflare_interstitial_ends_the_search(self):
        challenge = fixture_text("providers", "common", "cloudflare_challenge.html")
        browser = ScriptedBrowser({self.BASE + "/tv/": challenge})
        async with FixtureSite().client() as client:
            self.assertEqual(await self.scraper().search(["espn"], browser=browser, http_client=client), [])
        self.assertEqual(browser.scripted_pages[0].visited, [self.BASE + "/tv/"])

    async def test_without_a_browser_nothing_is_found(self):
        self.assertEqual(await self.scraper().search(["espn"], browser=None), [])


class DaddyLiveContractTests(OfflineCase):
    BASE = "https://dlhd.example.test"
    DIRECTORY_URL = BASE + "/24-7-channels.php"
    DIRECTORY = fixture_text("providers", "daddylive", "24-7-channels.html")
    LOG = fixture_json("providers", "daddylive", "network_log_espn_usa.json")

    def scraper(self):
        scraper = scrapers.DaddyLiveScraper()
        scraper.base_url = self.BASE
        return scraper

    def parse(self, terms):
        matches, seen = [], set()
        self.scraper()._parse_channel_matches_from_html(self.DIRECTORY, self.DIRECTORY_URL, terms, matches, seen)
        return [(url.replace(self.BASE, ""), title) for url, _, title in matches]

    def test_directory_matches(self):
        cases = {
            ("espn",): [("/watch.php?id=44", "ESPN USA")],  # not ESPN2/ESPNU, nor the Deportes/Brasil feeds
            ("espn2",): [("/watch.php?id=45", "ESPN2 USA")],
            ("espnu",): [("/watch.php?id=46", "ESPNU USA")],
            ("fox sports 1",): [("/watch.php?id=51", "FOX Sports 1 USA")],
            ("nfl network",): [("/watch.php?id=52", "NFL Network USA")],
            ("curling",): [],
        }
        for terms, expected in cases.items():
            with self.subTest(terms=terms):
                self.assertEqual(self.parse(list(terms)), expected)

    async def test_find_matches_over_http(self):
        site = FixtureSite()
        site.add("dlhd.example.test", "/24-7-channels.php", "providers", "daddylive", "24-7-channels.html")
        async with site.client() as client:
            matches = await self.scraper()._find_matches(client, ["espn"])
        self.assertEqual([url for url, _, _ in matches], [self.BASE + "/watch.php?id=44"])

    async def test_search_follows_the_player_and_verifies_manifests(self):
        site = FixtureSite()
        site.add("dlhd.example.test", "/24-7-channels.php", "providers", "daddylive", "24-7-channels.html")
        browser = ScriptedBrowser(network_logs={self.LOG["page_url"]: self.LOG["requests"]})
        async with site.client() as client:
            with patch.object(scrapers.asyncio, "sleep", _instant_sleep):
                results = await self.scraper().search(["espn"], browser=browser, http_client=client)

        self.assertEqual(
            [(s["url"], s["referer"], s["origin"]) for s in results],
            [
                ("https://edge.example.test/hls/espn-usa/index.m3u8?hmac=fixture", "https://player-host.example.test/", "https://player-host.example.test"),
                ("https://edge.example.test/manifest/espn-usa-backup", "https://player-host.example.test/stream/stream-44.php", ""),
            ],
        )
        self.assertEqual({(s["provider"], s["match_title"], s["discovery_method"]) for s in results}, {("DaddyLive", "ESPN USA", "playwright")})
        self.assertIn(self.LOG["page_url"], [url for page in browser.scripted_pages for url in page.visited])

    async def test_without_a_browser_nothing_is_found(self):
        self.assertEqual(await self.scraper().search(["espn"], browser=None), [])


if __name__ == "__main__":
    unittest.main()
