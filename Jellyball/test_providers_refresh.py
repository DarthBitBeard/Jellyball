"""Issue #49 part 1: providers that list 0 events.

Covers the maintenance changes for the built-in providers: following a moved
domain, new event-link shapes, and saying plainly why a site cannot be used
(blocked, parked, unreachable, every stream rejected) instead of "0 events".
Nothing here works around a block: a blocked site is only reported. Offline;
fixtures use *.example.test hosts only.
"""
import asyncio
import gc
import logging
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))

import db
import provider_telemetry as telemetry
import scrapers
import state
from plugins import sdk
from sports_matcher import get_team_search_terms
from test_fixtures_contract import FixtureSite, OfflineCase, ScriptedBrowser, fixture_text

HOST = "agg.example.test"
BASE = "https://" + HOST
GATORS = get_team_search_terms("Florida Gators", "Florida Gators")


def make(scraper_type, base=BASE):
    scraper = scraper_type()
    scraper.base_url = base
    return scraper


class ClassifyPageTests(unittest.TestCase):
    def test_challenge_page_is_blocked(self):
        html = fixture_text("providers", "common", "cloudflare_challenge.html")
        self.assertEqual(sdk.classify_page_html(html)[0], "blocked")

    def test_parked_domain_is_parked(self):
        html = fixture_text("providers", "common", "parked_domain.html")
        self.assertEqual(sdk.classify_page_html(html)[0], "parked")

    def test_ordinary_pages_are_not_classified(self):
        for name in (("aggregator", "index.html"), ("thetvapp", "home_matches.html"), ("aggregator", "new_layout_index.html")):
            with self.subTest(page=name):
                self.assertIsNone(sdk.classify_page_html(fixture_text("providers", *name)))
        self.assertIsNone(sdk.classify_page_html(""))

    def test_fetch_failures_are_named(self):
        cases = [
            ({"status": 403}, "blocked"),
            ({"status": 429}, "blocked"),
            ({"status": 503, "body_head": "<title>Just a moment...</title>"}, "blocked"),
            ({"status": 503, "body_head": "oops"}, "http_503"),
            ({"status": 404}, "not_found"),
            ({"exception": httpx.ConnectTimeout("x")}, "unreachable"),
            ({"exception": httpx.ConnectError("x")}, "unreachable"),
            ({"error": "too_large"}, "too_large"),
            ({}, "no_content"),
        ]
        for info, expected in cases:
            with self.subTest(info=info):
                self.assertEqual(sdk.classify_fetch_failure(info)[0], expected)


class SiteStateLoggingTests(unittest.TestCase):
    def setUp(self):
        sdk._SITE_STATE_LOGGED_AT.clear()
        self.addCleanup(sdk._SITE_STATE_LOGGED_AT.clear)

    def test_blocked_site_is_logged_once_with_host_and_path_only(self):
        with self.assertLogs(sdk.LOGGER, level="DEBUG") as logs:
            sdk.log_site_state("Prov", "blocked", "HTTP 403", BASE + "/live?token=secret")
            sdk.log_site_state("Prov", "blocked", "HTTP 403", BASE + "/live?token=secret")
        warnings = [r for r in logs.records if r.levelno == logging.WARNING]
        self.assertEqual(len(warnings), 1)
        text = warnings[0].getMessage()
        self.assertIn("site blocked automated access", text)
        self.assertIn(HOST, text)
        self.assertNotIn("secret", " ".join(r.getMessage() for r in logs.records))


class EventLinkShapeTests(OfflineCase):
    def test_streameast_event_pattern_takes_events_not_listing_pages(self):
        scraper = make(scrapers.StreamEastScraper)
        index = BASE + "/"
        ok = ["/nfl/florida-gators-georgia-bulldogs/49229088", "/nba/orlando-magic-phoenix-suns/49229090/"]
        bad = ["/nfl/florida-gators-georgia-bulldogs/", "/nfl-streams/standings", "/about", "/nflstreams4"]
        for path in ok:
            self.assertTrue(scraper._is_event_link(path, index), path)
        for path in bad:
            self.assertFalse(scraper._is_event_link(path, index), path)

    def test_new_event_url_shapes_are_recognised(self):
        self.assertTrue(make(scrapers.FootybiteScraper)._is_event_link("/game/f1-grand-prix-0511061530", BASE + "/"))
        self.assertTrue(make(scrapers.OneStreamScraper)._is_event_link("/nfl/ravens-jaguars-live-stream/1538409", BASE + "/"))
        self.assertTrue(make(scrapers.MethStreamsScraper)._is_event_link("/live/nfl/ravens-jaguars-live-streaming-1", BASE + "/nflstreams"))

    def test_default_domains_and_scan_pages(self):
        with patch.dict(os.environ, {}, clear=False):
            for name in ("AGGREGATOR_3_URL", "AGGREGATOR_4_URL"):
                os.environ.pop(name, None)
            self.assertIn("/nflstreams", scrapers.MethStreamsScraper().categories)
            streameast = scrapers.StreamEastScraper()
            self.assertEqual(streameast.base_url, "https://streameast.cool")
            self.assertIn("https://thestreameast.top", scrapers.BUILTIN_PROVIDER_MIRRORS["StreamEast"])

    async def test_streameast_index_end_to_end(self):
        site = FixtureSite()
        for category in make(scrapers.StreamEastScraper).categories:
            site.add(HOST, category, "providers", "aggregator", "new_layout_index.html")
        site.add(HOST, "/", "providers", "aggregator", "new_layout_index.html")
        async with site.client() as client:
            matches = await make(scrapers.StreamEastScraper)._find_matches(client, GATORS)
        paths = sorted(url.replace(BASE, "") for url, _, _ in matches)
        self.assertEqual(paths, ["/nfl/florida-gators-georgia-bulldogs/49229088", "/nfl/florida-gators-live-stream/1538409"])


class MovedDomainTests(OfflineCase):
    OLD = "old.example.test"
    NEW = "new.example.test"

    def site(self):
        index = fixture_text("providers", "aggregator", "new_layout_index.html")

        def handler(request):
            if request.url.host == self.OLD:
                return httpx.Response(301, headers={"location": f"https://{self.NEW}{request.url.path}"})
            if request.url.host == self.NEW:
                return httpx.Response(200, headers={"content-type": "text/html"}, content=index.encode())
            return httpx.Response(404)

        return httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async def test_events_on_the_new_domain_are_followed(self):
        scraper = make(scrapers.StreamEastScraper, "https://" + self.OLD)
        scraper.categories = []
        telemetry.begin_run("StreamEast")
        async with self.site() as client:
            with self.assertLogs(sdk.LOGGER, level="INFO") as logs:
                matches = await scraper._find_matches(client, GATORS)
        urls = sorted(url for url, _, _ in matches)
        self.assertEqual(urls, [
            f"https://{self.NEW}/nfl/florida-gators-georgia-bulldogs/49229088",
            f"https://{self.NEW}/nfl/florida-gators-live-stream/1538409",
        ])
        self.assertTrue(any("moved" in r.getMessage() and self.NEW in r.getMessage() for r in logs.records))
        self.assertGreater(telemetry.current_run().index_events, 0)

    async def test_a_redirect_to_a_private_address_is_still_refused(self):
        def handler(request):
            return httpx.Response(301, headers={"location": "http://127.0.0.1/"})

        scraper = make(scrapers.StreamEastScraper)
        scraper.categories = []
        run = telemetry.begin_run("StreamEast")
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            self.assertEqual(await scraper._find_matches(client, GATORS), [])
        self.assertTrue(run.page_errors)


class UnusableSiteTests(OfflineCase):
    def setUp(self):
        super().setUp()
        sdk._SITE_STATE_LOGGED_AT.clear()
        self.addCleanup(sdk._SITE_STATE_LOGGED_AT.clear)

    async def run_with(self, handler, scraper_type=scrapers.TopStreamsScraper):
        scraper = make(scraper_type)
        scraper.categories = []
        run = telemetry.begin_run(scraper.name)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with self.assertLogs(sdk.LOGGER, level="INFO") as logs:
                matches = await scraper._find_matches(client, GATORS)
        return matches, run, [r.getMessage() for r in logs.records]

    async def test_parked_domain_is_reported_as_parked(self):
        body = fixture_text("providers", "common", "parked_domain.html")
        matches, run, logs = await self.run_with(lambda request: httpx.Response(200, headers={"content-type": "text/html"}, text=body))
        self.assertEqual((matches, run.page_errors), ([], ["parked"]))
        self.assertTrue(any("parked domain" in line for line in logs))

    async def test_bot_check_is_reported_and_not_retried_around(self):
        body = fixture_text("providers", "common", "cloudflare_challenge.html")
        seen = []

        def handler(request):
            seen.append(request)
            return httpx.Response(403, headers={"content-type": "text/html"}, text=body)

        matches, run, logs = await self.run_with(handler)
        self.assertEqual((matches, run.page_errors), ([], ["blocked"]))
        self.assertTrue(any("site blocked automated access" in line for line in logs))
        self.assertEqual(len(seen), 1)  # no second attempt with other headers

    async def test_unreachable_site_is_reported(self):
        def handler(request):
            raise httpx.ConnectTimeout("timed out")

        matches, run, logs = await self.run_with(handler)
        self.assertEqual((matches, run.page_errors), ([], ["unreachable"]))
        self.assertTrue(any("site unreachable" in line for line in logs))

    async def test_a_loaded_page_without_event_links_is_not_an_error(self):
        matches, run, logs = await self.run_with(lambda request: httpx.Response(200, headers={"content-type": "text/html"}, text="<html><a href='/about'>About</a></html>"))
        self.assertEqual((matches, run.page_errors, run.index_events), ([], [], 0))
        self.assertTrue(any("list no event links" in line for line in logs))
        self.assertEqual(telemetry.derive_outcome([], run), ("empty", None))


class TheTVAppHomePageTests(OfflineCase):
    BASE = "https://thetvapp.example.test"
    HOME = fixture_text("providers", "thetvapp", "home_matches.html")

    def scraper(self):
        return make(scrapers.TheTVAppScraper, self.BASE)

    def test_each_match_link_matches_only_its_own_teams(self):
        scraper = self.scraper()
        for term, path in (("orlando magic", "/watch/31740"), ("jacksonville jaguars", "/watch/31495"), ("golden state warriors", "/watch/31741")):
            with self.subTest(term=term):
                url, score, title = scraper._scan_channel_anchors(self.HOME, [term], [term], self.BASE + "/")[:3]
                self.assertEqual(url, self.BASE + path)
                self.assertIn(term.split()[0], title.lower())
        self.assertIsNone(scraper._scan_channel_anchors(self.HOME, ["boston celtics"], ["boston celtics"], self.BASE + "/")[0])

    def test_listed_counts_match_links_only(self):
        listed = self.scraper()._scan_channel_anchors(self.HOME, ["x"], ["x"], self.BASE + "/")[3]
        self.assertEqual(listed, 3)

    async def test_home_page_is_read_when_the_channel_directory_is_gone(self):
        browser = ScriptedBrowser({self.BASE + "/tv/": "<html><body><a href='/'>Home</a></body></html>", self.BASE + "/": self.HOME})
        run = telemetry.begin_run("TheTVApp")
        async with FixtureSite().client() as client:
            await self.scraper().search(["orlando magic"], browser=browser, http_client=client)
        visited = browser.scripted_pages[0].visited
        self.assertEqual(visited[:2], [self.BASE + "/tv/", self.BASE + "/"])
        self.assertIn(self.BASE + "/watch/31740", visited)
        self.assertEqual((run.index_events, run.matches), (3, 1))

    async def test_parked_or_blocked_home_ends_the_search_with_a_state(self):
        for name, state_name in (("cloudflare_challenge.html", "blocked"), ("parked_domain.html", "parked")):
            with self.subTest(page=name):
                sdk._SITE_STATE_LOGGED_AT.clear()
                browser = ScriptedBrowser({self.BASE + "/tv/": fixture_text("providers", "common", name)})
                run = telemetry.begin_run("TheTVApp")
                async with FixtureSite().client() as client:
                    self.assertEqual(await self.scraper().search(["espn"], browser=browser, http_client=client), [])
                self.assertEqual(run.page_errors, [state_name])
                self.assertEqual(browser.scripted_pages[0].visited, [self.BASE + "/tv/"])


class RejectedStreamsDemotionTests(unittest.IsolatedAsyncioTestCase):
    """A provider whose every verified candidate is rejected (e.g. its stream CDN
    answers 403 to non-browser clients) is counted by the circuit breaker."""

    def test_all_rejected_is_an_error_named_rejected(self):
        run = telemetry.RunStats(provider="P", index_events=4, index_pages=1, matches=1)
        run.candidates, run.rejected, run.reject_reasons = 3, 3, ["playlist http_403"] * 3
        self.assertEqual(telemetry.derive_outcome([], run), ("error", "rejected"))
        self.assertEqual(telemetry.common_reject_reason(run), "playlist http_403")

    def test_partly_verified_is_not_rejected(self):
        run = telemetry.RunStats(provider="P", index_events=4, index_pages=1, matches=1)
        run.candidates, run.rejected = 3, 2
        self.assertEqual(telemetry.derive_outcome([], run), ("empty", None))
        self.assertEqual(telemetry.derive_outcome([{"url": "u"}], run), ("ok", None))

    async def test_verification_reports_candidates_and_reasons(self):
        run = telemetry.begin_run("P")
        stream = {"url": "https://cdn.example.test/live/a.m3u8", "match_title": "T"}
        reject_reason = {"reason": "playlist http_403"}
        with patch.object(scrapers, "verify_stream_live", AsyncMock(return_value=False)) as verify:
            async def fake(client, url, *args, reject=None, **kwargs):
                if reject is not None:
                    reject.update(reject_reason)
                return False
            verify.side_effect = fake
            async with httpx.AsyncClient() as client:
                self.assertEqual(await scrapers._verify_provider_streams([stream, dict(stream, url=stream["url"] + "?2")], client), [])
        self.assertEqual((run.candidates, run.rejected), (2, 2))

    async def test_master_scrape_counts_rejection_toward_the_breaker_without_probing_mirrors(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(tmp, ignore_errors=True))
        patcher = patch.object(db, "DB_FILE", os.path.join(tmp, "t.db"))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(lambda: (db.close_all_db_connections(), gc.collect()))
        db.init_db()
        scrapers._PROVIDER_BREAKERS.clear()
        self.addCleanup(scrapers._PROVIDER_BREAKERS.clear)

        class Unplayable:
            name = "Unplayable"

            async def search(self, terms, browser=None, http_client=None):
                telemetry.report_index_events(10)
                telemetry.report_matches(1)
                telemetry.report_verification(2, ["playlist http_403", "playlist http_403"])
                return []

        probes = []
        for _ in range(scrapers.PROVIDER_BREAKER_FAILURES):
            with patch.object(scrapers, "_providers_for_search", lambda always_live=False: [Unplayable()]), \
                    patch.object(scrapers, "get_healthy_browser", AsyncMock(return_value=None)), \
                    patch.object(scrapers.provider_alerts, "check_soon"), \
                    patch.object(scrapers, "_maybe_probe_provider_mirrors", side_effect=probes.append), \
                    patch.object(state, "SHARED_HTTP_CLIENT", None):
                await scrapers.master_scrape("T", team_name="T", team_id="t", always_live=True)
        self.assertTrue(scrapers._provider_breaker_open("Unplayable"))
        self.assertEqual(probes, [])
        with db._db_session() as conn:
            rows = conn.execute("SELECT outcome, error_class FROM provider_performance WHERE provider='Unplayable'").fetchall()
        self.assertEqual({tuple(r) for r in rows}, {("error", "rejected")})


class DaddyLiveStopsWhenEveryManifestIsRejectedTests(OfflineCase):
    async def test_search_stops_after_consecutive_rejected_channels(self):
        scraper = make(scrapers.DaddyLiveScraper, "https://dlhd.example.test")
        matches = [(f"https://dlhd.example.test/watch.php?id={i}", 100, f"ESPN {i}") for i in range(8)]
        extracted = []

        async def extract(browser, url, score, title):
            extracted.append(url)
            return [{"url": "https://edge.example.test/hls/x.m3u8", "provider": "DaddyLive"}]

        class Browser:
            def is_connected(self):
                return True

        with patch.object(scraper, "_find_matches", AsyncMock(return_value=matches)), \
                patch.object(scraper, "_extract_player_streams", side_effect=extract), \
                patch("plugins.builtin.daddylive.provider._verify_provider_streams", AsyncMock(return_value=[])):
            async with httpx.AsyncClient() as client:
                result = await scraper.search(["espn"], browser=Browser(), http_client=client)
        self.assertEqual(result, [])
        self.assertEqual(len(extracted), scraper.MAX_REJECTED_PLAYERS_IN_A_ROW)

    async def test_a_verified_channel_resets_the_count(self):
        scraper = make(scrapers.DaddyLiveScraper, "https://dlhd.example.test")
        matches = [(f"https://dlhd.example.test/watch.php?id={i}", 100, f"ESPN {i}") for i in range(5)]
        good = {"url": "https://edge.example.test/hls/ok.m3u8", "provider": "DaddyLive"}
        verdicts = iter([[], [good], [], [], [good]])

        async def extract(browser, url, score, title):
            return [good]

        class Browser:
            def is_connected(self):
                return True

        with patch.object(scraper, "_find_matches", AsyncMock(return_value=matches)), \
                patch.object(scraper, "_extract_player_streams", side_effect=extract), \
                patch("plugins.builtin.daddylive.provider._verify_provider_streams", AsyncMock(side_effect=lambda *a, **k: next(verdicts))):
            async with httpx.AsyncClient() as client:
                result = await scraper.search(["espn"], browser=Browser(), http_client=client)
        self.assertEqual([s["url"] for s in result], [good["url"]])  # deduplicated; all five channels were tried




class PinnedUrlHintTests(unittest.TestCase):
    def test_a_failing_host_from_an_aggregator_setting_is_called_out(self):
        from unittest.mock import patch

        import plugins.sdk as sdk

        with patch.dict("os.environ", {"AGGREGATOR_4_URL": "https://old.example.test"}):
            self.assertIn("AGGREGATOR_4_URL", sdk._pinned_url_hint("old.example.test"))
            self.assertEqual(sdk._pinned_url_hint("other.example.test"), "")
            self.assertEqual(sdk._pinned_url_hint(""), "")


if __name__ == "__main__":
    unittest.main()
