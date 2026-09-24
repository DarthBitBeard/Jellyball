"""Regression tests for the scraper-hardening pass:

1. Playwright context/page leaks on cancellation (try/finally everywhere).
2. A global cap on concurrent Playwright pages (PLAYWRIGHT_MAX_PAGES).
3. Periodic browser recycling (PLAYWRIGHT_RECYCLE_HOURS), gated on idle pages.
4. CPU-heavy HTML/regex parsing moved off the event loop via asyncio.to_thread.
5. A shared TTL cache + single-flight for aggregator index pages.
6. _get_active_provider_priority() no longer does a sync SQLite read on the loop.
7. master_scrape() caps its returned candidate list to MAX_STREAM_CANDIDATES.

Everything here uses fakes/mocks: no real Chromium, no real network calls.
"""
import asyncio
import gc
import os
import shutil
import sys
import tempfile
import time
import unittest
import urllib.parse
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

import main
import stream_extractor


# --------------------------------------------------------------------------- #
# Fakes for Playwright's Browser/BrowserContext/Page, sufficient for the
# call sites under test. None of these touch a real browser process.
# --------------------------------------------------------------------------- #
class FakePage:
    def __init__(self):
        self.closed = False

    async def close(self):
        self.closed = True


class FakeContext:
    def __init__(self):
        self.closed = False
        self.page = None

    async def new_page(self):
        self.page = FakePage()
        return self.page

    async def close(self):
        self.closed = True


class FakeBrowser:
    def __init__(self, connected: bool = True):
        self.contexts = []
        self.pages = []
        self._connected = connected

    def is_connected(self):
        return self._connected

    async def new_context(self, user_agent=None):
        ctx = FakeContext()
        self.contexts.append(ctx)
        return ctx

    async def new_page(self):
        page = FakePage()
        self.pages.append(page)
        return page


class HangingPage(FakePage):
    async def goto(self, *args, **kwargs):
        await asyncio.sleep(10)

    async def wait_for_load_state(self, *args, **kwargs):
        return None

    async def content(self):
        return "<html></html>"


class HangingContext(FakeContext):
    async def new_page(self):
        self.page = HangingPage()
        return self.page


class HangingBrowser(FakeBrowser):
    async def new_context(self, user_agent=None):
        ctx = HangingContext()
        self.contexts.append(ctx)
        return ctx


def _reset_playwright_page_globals():
    stream_extractor._PLAYWRIGHT_PAGE_SEMAPHORE = None
    stream_extractor._PLAYWRIGHT_PAGE_SEMAPHORE_LOOP = None
    stream_extractor._PLAYWRIGHT_PAGES_IN_USE = 0


# --------------------------------------------------------------------------- #
# 1 & 2: context/page cleanup on cancellation, and the global page cap.
# --------------------------------------------------------------------------- #
class PlaywrightPageLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        _reset_playwright_page_globals()

    async def asyncTearDown(self):
        _reset_playwright_page_globals()

    async def test_context_closed_when_cancelled_mid_flight(self):
        """A provider search timeout raises CancelledError inside the `async
        with` block (mirroring asyncio.wait_for(..., PROVIDER_SEARCH_TIMEOUT)).
        Before the fix, browser.new_context()/new_page() were only closed on
        success or a caught Exception, leaking a live Chromium context here."""
        browser = FakeBrowser()
        started = asyncio.Event()

        async def run():
            async with stream_extractor.playwright_page(browser, user_agent="UA") as page:
                started.set()
                await asyncio.sleep(10)

        task = asyncio.ensure_future(run())
        await started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertEqual(len(browser.contexts), 1)
        self.assertTrue(browser.contexts[0].closed)
        self.assertEqual(stream_extractor.playwright_pages_in_use(), 0)

    async def test_bare_page_closed_when_cancelled_mid_flight(self):
        """playwright_intercept_streams() creates a page with no context; that
        path must also survive cancellation."""
        browser = FakeBrowser()
        started = asyncio.Event()

        async def run():
            async with stream_extractor.playwright_page(browser) as page:
                started.set()
                await asyncio.sleep(10)

        task = asyncio.ensure_future(run())
        await started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertEqual(len(browser.pages), 1)
        self.assertTrue(browser.pages[0].closed)
        self.assertEqual(stream_extractor.playwright_pages_in_use(), 0)

    async def test_html_aggregator_playwright_fallback_closes_context_on_timeout(self):
        """Exercises the actual scraper code path (HtmlAggregatorScraper's
        Cloudflare-bypass fallback) under a real asyncio.wait_for timeout, the
        exact scenario the audit flagged: a slow page.goto() gets cancelled by
        PROVIDER_SEARCH_TIMEOUT and must not leak the context."""
        scraper = main.ISportSurgeScraper()
        browser = HangingBrowser()

        async def fake_fetch_bounded_text(*args, **kwargs):
            return None  # force the Playwright fallback branch

        with patch.object(main, "fetch_bounded_text", fake_fetch_bounded_text):
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(
                    scraper._fetch_index_page(None, "https://isportsurge.ws/cfb/livestreams2", browser),
                    timeout=0.05,
                )

        self.assertEqual(len(browser.contexts), 1)
        self.assertTrue(browser.contexts[0].closed)
        self.assertEqual(stream_extractor.playwright_pages_in_use(), 0)

    async def test_semaphore_limits_concurrent_pages(self):
        browser = FakeBrowser()
        concurrent = 0
        peak = 0
        lock = asyncio.Lock()

        async def worker():
            nonlocal concurrent, peak
            async with stream_extractor.playwright_page(browser) as _page:
                async with lock:
                    concurrent += 1
                    peak = max(peak, concurrent)
                await asyncio.sleep(0.03)
                async with lock:
                    concurrent -= 1

        with patch.object(stream_extractor, "PLAYWRIGHT_MAX_PAGES", 2):
            await asyncio.gather(*(worker() for _ in range(6)))

        self.assertLessEqual(peak, 2)
        self.assertGreaterEqual(peak, 1)
        self.assertEqual(stream_extractor.playwright_pages_in_use(), 0)


# --------------------------------------------------------------------------- #
# 3: periodic browser recycling, gated on idle pages.
# --------------------------------------------------------------------------- #
class FakeAsyncBrowser:
    def __init__(self, label: str):
        self.label = label
        self._connected = True
        self.closed = False

    def is_connected(self):
        return self._connected

    async def close(self):
        self.closed = True
        self._connected = False


class FakeChromium:
    def __init__(self, browser_to_return):
        self.browser_to_return = browser_to_return
        self.launch_calls = 0

    async def launch(self, headless=True):
        self.launch_calls += 1
        return self.browser_to_return


class FakePlaywrightClient:
    def __init__(self, chromium):
        self.chromium = chromium


class BrowserRecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._orig_browser = main.SHARED_BROWSER
        self._orig_client = main.PLAYWRIGHT_CLIENT
        self._orig_launch_info = main._BROWSER_LAUNCH_INFO

    async def asyncTearDown(self):
        main.SHARED_BROWSER = self._orig_browser
        main.PLAYWRIGHT_CLIENT = self._orig_client
        main._BROWSER_LAUNCH_INFO = self._orig_launch_info

    async def test_recycles_when_due_and_idle(self):
        old_browser = FakeAsyncBrowser("old")
        new_browser = FakeAsyncBrowser("new")
        chromium = FakeChromium(new_browser)
        main.SHARED_BROWSER = old_browser
        main.PLAYWRIGHT_CLIENT = FakePlaywrightClient(chromium)
        main._BROWSER_LAUNCH_INFO = (id(old_browser), time.monotonic() - 7 * 3600)

        with patch.object(main, "PLAYWRIGHT_RECYCLE_HOURS", 6.0), \
             patch.object(main, "playwright_pages_in_use", lambda: 0):
            result = await main.get_healthy_browser()

        self.assertIs(result, new_browser)
        self.assertTrue(old_browser.closed)
        self.assertEqual(chromium.launch_calls, 1)
        self.assertIs(main.SHARED_BROWSER, new_browser)

    async def test_skips_recycle_while_pages_are_in_use(self):
        old_browser = FakeAsyncBrowser("old")
        chromium = FakeChromium(FakeAsyncBrowser("unused"))
        main.SHARED_BROWSER = old_browser
        main.PLAYWRIGHT_CLIENT = FakePlaywrightClient(chromium)
        main._BROWSER_LAUNCH_INFO = (id(old_browser), time.monotonic() - 7 * 3600)

        with patch.object(main, "PLAYWRIGHT_RECYCLE_HOURS", 6.0), \
             patch.object(main, "playwright_pages_in_use", lambda: 1):
            result = await main.get_healthy_browser()

        self.assertIs(result, old_browser)
        self.assertFalse(old_browser.closed)
        self.assertEqual(chromium.launch_calls, 0)

    async def test_no_recycle_before_due(self):
        old_browser = FakeAsyncBrowser("old")
        chromium = FakeChromium(FakeAsyncBrowser("unused"))
        main.SHARED_BROWSER = old_browser
        main.PLAYWRIGHT_CLIENT = FakePlaywrightClient(chromium)
        main._BROWSER_LAUNCH_INFO = (id(old_browser), time.monotonic())

        with patch.object(main, "PLAYWRIGHT_RECYCLE_HOURS", 6.0), \
             patch.object(main, "playwright_pages_in_use", lambda: 0):
            result = await main.get_healthy_browser()

        self.assertIs(result, old_browser)
        self.assertFalse(old_browser.closed)
        self.assertEqual(chromium.launch_calls, 0)

    async def test_recycle_disabled_when_hours_is_zero(self):
        old_browser = FakeAsyncBrowser("old")
        chromium = FakeChromium(FakeAsyncBrowser("unused"))
        main.SHARED_BROWSER = old_browser
        main.PLAYWRIGHT_CLIENT = FakePlaywrightClient(chromium)
        main._BROWSER_LAUNCH_INFO = (id(old_browser), time.monotonic() - 1000 * 3600)

        with patch.object(main, "PLAYWRIGHT_RECYCLE_HOURS", 0.0), \
             patch.object(main, "playwright_pages_in_use", lambda: 0):
            result = await main.get_healthy_browser()

        self.assertIs(result, old_browser)
        self.assertFalse(old_browser.closed)


# --------------------------------------------------------------------------- #
# 5: shared TTL cache + single-flight for aggregator index pages.
# --------------------------------------------------------------------------- #
class IndexCacheTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        main._SCRAPE_INDEX_CACHE.clear()
        main._SCRAPE_INDEX_INFLIGHT.clear()

    async def asyncTearDown(self):
        main._SCRAPE_INDEX_CACHE.clear()
        main._SCRAPE_INDEX_INFLIGHT.clear()

    async def test_cache_hit_avoids_refetch(self):
        calls = 0

        async def fetcher():
            nonlocal calls
            calls += 1
            return "<html>ok</html>"

        with patch.object(main, "SCRAPE_INDEX_CACHE_SECONDS", 60.0), \
             patch.object(main, "_SCRAPE_INDEX_FAILURE_CACHE_SECONDS", 5.0):
            first = await main._get_cached_index_html("https://example.test/idx1", fetcher)
            second = await main._get_cached_index_html("https://example.test/idx1", fetcher)

        self.assertEqual(first, "<html>ok</html>")
        self.assertEqual(second, "<html>ok</html>")
        self.assertEqual(calls, 1)

    async def test_cache_expiry_triggers_refetch(self):
        calls = 0

        async def fetcher():
            nonlocal calls
            calls += 1
            return f"<html>{calls}</html>"

        with patch.object(main, "SCRAPE_INDEX_CACHE_SECONDS", 0.01), \
             patch.object(main, "_SCRAPE_INDEX_FAILURE_CACHE_SECONDS", 0.01):
            first = await main._get_cached_index_html("https://example.test/idx2", fetcher)
            await asyncio.sleep(0.05)
            second = await main._get_cached_index_html("https://example.test/idx2", fetcher)

        self.assertEqual(calls, 2)
        self.assertNotEqual(first, second)

    async def test_single_flight_shares_one_fetch(self):
        calls = 0
        started = asyncio.Event()

        async def fetcher():
            nonlocal calls
            calls += 1
            started.set()
            await asyncio.sleep(0.05)
            return "<html>shared</html>"

        with patch.object(main, "SCRAPE_INDEX_CACHE_SECONDS", 60.0):
            results = await asyncio.gather(
                main._get_cached_index_html("https://example.test/idx3", fetcher),
                main._get_cached_index_html("https://example.test/idx3", fetcher),
                main._get_cached_index_html("https://example.test/idx3", fetcher),
            )

        self.assertEqual(calls, 1)
        self.assertTrue(all(r == "<html>shared</html>" for r in results))

    async def test_failures_are_cached_only_briefly(self):
        calls = 0

        async def fetcher():
            nonlocal calls
            calls += 1
            return None

        with patch.object(main, "SCRAPE_INDEX_CACHE_SECONDS", 60.0), \
             patch.object(main, "_SCRAPE_INDEX_FAILURE_CACHE_SECONDS", 0.01):
            first = await main._get_cached_index_html("https://example.test/idx4", fetcher)
            second = await main._get_cached_index_html("https://example.test/idx4", fetcher)
            await asyncio.sleep(0.05)
            third = await main._get_cached_index_html("https://example.test/idx4", fetcher)

        self.assertIsNone(first)
        self.assertIsNone(second)
        self.assertIsNone(third)
        self.assertEqual(calls, 2)

    async def test_cache_is_bounded(self):
        async def fetcher():
            return "<html>x</html>"

        with patch.object(main, "SCRAPE_INDEX_CACHE_SECONDS", 60.0), \
             patch.object(main, "_SCRAPE_INDEX_CACHE_MAX_ENTRIES", 2):
            await main._get_cached_index_html("https://example.test/a", fetcher)
            await main._get_cached_index_html("https://example.test/b", fetcher)
            await main._get_cached_index_html("https://example.test/c", fetcher)

        self.assertLessEqual(len(main._SCRAPE_INDEX_CACHE), 2)


# --------------------------------------------------------------------------- #
# 4: CPU-heavy parsing moved into sync helpers (asyncio.to_thread call sites).
# These call the sync helpers directly, so they verify the extracted logic
# produces the same matches as the original inline code did.
# --------------------------------------------------------------------------- #
class ParseHelperRegressionTests(unittest.TestCase):
    def test_html_aggregator_parses_matching_anchor(self):
        scraper = main.HtmlAggregatorScraper("TestAgg", "https://agg.example.test", ["/cfb"], ["/watch/"])
        page_url = "https://agg.example.test/cfb"
        # Each anchor sits in its own wrapping <div>, like real aggregator
        # cards, so _anchor_context()'s parent-text fallback doesn't bleed
        # sibling anchors' text into each other.
        html = (
            "<div><a href='/watch/florida-gators-vs-georgia'>Florida Gators vs Georgia Bulldogs</a></div>"
            "<div><a href='/watch/ohio-state-vs-michigan'>Ohio State vs Michigan</a></div>"
            "<div><a href='/about'>About</a></div>"
        )
        search_terms = main.get_team_search_terms("Florida Gators", "Florida Gators")
        matches = []
        seen = set()
        scraper._parse_matches_from_html(html, page_url, search_terms, matches, seen)

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0][0], "https://agg.example.test/watch/florida-gators-vs-georgia")

        # A second pass with the same seen_matches must not duplicate.
        scraper._parse_matches_from_html(html, page_url, search_terms, matches, seen)
        self.assertEqual(len(matches), 1)

    def test_html_aggregator_respects_cumulative_event_cutoff(self):
        scraper = main.HtmlAggregatorScraper("TestAgg", "https://agg.example.test", ["/cfb"], ["/watch/"])
        html = "".join(f"<a href='/watch/game-{i}'>Florida Gators vs Team {i}</a>" for i in range(10))
        search_terms = main.get_team_search_terms("Florida Gators", "Florida Gators")
        matches = []
        seen = set()
        with patch.object(main, "MAX_PROVIDER_EVENTS", 3):
            scraper._parse_matches_from_html(html, "https://agg.example.test/cfb", search_terms, matches, seen)
        self.assertEqual(len(matches), 3)

    def test_thetvapp_finds_most_specific_channel(self):
        scraper = main.TheTVAppScraper()
        html = (
            "<div><a href='/watch/espn'>ESPN</a></div>"
            "<div><a href='/watch/espn2'>ESPN2</a></div>"
            "<div><a href='/watch/fs1'>FS1</a></div>"
        )
        search_terms = main.get_team_search_terms("ESPN", "ESPN")
        best_url, best_score, best_title = scraper._find_best_channel_match(html, ["espn"], search_terms)

        self.assertEqual(best_url, urllib.parse.urljoin(scraper.base_url, "/watch/espn"))
        self.assertGreaterEqual(best_score, 120)
        # _anchor_context() concatenates the anchor's own text with its parent's
        # text (to catch team names living outside the <a> tag), so a lone
        # anchor's title can legitimately repeat; just check it identifies ESPN
        # and not one of the other listed channels.
        self.assertEqual(set(best_title.split()), {"ESPN"})

    def test_daddylive_filters_non_english_and_matches_channel(self):
        scraper = main.DaddyLiveScraper()
        page_url = urllib.parse.urljoin(scraper.base_url, "/24-7-channels.php")
        html = (
            "<a href='watch.php?id=1' data-title='ESPN USA'>ESPN USA</a>"
            "<a href='watch.php?id=2'>RTVE Espanol</a>"
            "<a href='other.php?id=3'>Random Link</a>"
        )
        matches = []
        seen = set()
        scraper._parse_channel_matches_from_html(html, page_url, ["espn"], matches, seen)

        self.assertEqual(len(matches), 1)
        self.assertIn("id=1", matches[0][0])


class StreamExtractorParseHelperTests(unittest.TestCase):
    def test_extract_streams_from_text_finds_m3u8(self):
        text = "<script>var source = 'https://cdn.example.test/live/index.m3u8?token=abc';</script>"
        streams = stream_extractor.extract_streams_from_text(text, referer="https://player.example.test/watch")
        self.assertIn("https://cdn.example.test/live/index.m3u8?token=abc", streams)

    def test_parse_iframes_and_streamers_from_html_matches_direct_call(self):
        html = (
            "<iframe src='https://embed.example.test/player/1'></iframe>"
            "<a href='https://thestreameast.example.test/watch/1'>StreamEast Backup</a>"
        )
        page_url = "https://match.example.test/game/1"
        expected = stream_extractor.extract_iframes_and_streamers(stream_extractor.make_soup(html), page_url)
        actual = stream_extractor._parse_iframes_and_streamers_from_html(html, page_url)
        self.assertEqual(actual, expected)
        self.assertEqual(actual[0], ["https://embed.example.test/player/1"])

    def test_make_soup_uses_lxml_when_available(self):
        try:
            import lxml  # noqa: F401
        except ImportError:
            self.skipTest("lxml not installed")
        self.assertEqual(stream_extractor._BS_PARSER, "lxml")


# --------------------------------------------------------------------------- #
# 6: async provider-priority read (no sync SQLite call on the event loop).
# --------------------------------------------------------------------------- #
class ProviderPriorityAsyncTests(unittest.IsolatedAsyncioTestCase):
    def test_is_a_coroutine_function(self):
        self.assertTrue(asyncio.iscoroutinefunction(main._get_active_provider_priority))

    async def test_default_priority_reads_via_async_wrapper(self):
        tmpdir = tempfile.mkdtemp()
        try:
            db_path = os.path.join(tmpdir, "test_priority.db")
            with patch.object(main, "DB_FILE", db_path):
                main.init_db()
                result = await main._get_active_provider_priority()
            self.assertEqual(result, main._provider_priority)
        finally:
            gc.collect()
            shutil.rmtree(tmpdir, ignore_errors=True)

    async def test_rotation_mode_rotates_providers(self):
        tmpdir = tempfile.mkdtemp()
        try:
            db_path = os.path.join(tmpdir, "test_priority_rotate.db")
            with patch.object(main, "DB_FILE", db_path):
                main.init_db()
                main.set_setting("provider_rotation_mode", "1")
                result = await main._get_active_provider_priority()
            names = {p.name for p in main.ACTIVE_PROVIDERS}
            self.assertEqual(set(result.keys()), names)
        finally:
            gc.collect()
            shutil.rmtree(tmpdir, ignore_errors=True)


# --------------------------------------------------------------------------- #
# 7: master_scrape() caps its returned candidate list.
# --------------------------------------------------------------------------- #
class FakeCappingProvider:
    name = "FakeCappingProvider"

    async def search(self, search_terms, browser=None, http_client=None):
        return [
            {
                "url": f"https://example.test/stream/{i}",
                "provider": self.name,
                "match_score": 100 - i,
                "match_title": "Test Team",
                "match_url": "https://example.test/match/1",
                "referer": "",
                "origin": "",
            }
            for i in range(main.MAX_STREAM_CANDIDATES + 10)
        ]


class CandidateCapTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        main._PROVIDER_BREAKERS.pop(FakeCappingProvider.name, None)

    async def asyncTearDown(self):
        main._PROVIDER_BREAKERS.pop(FakeCappingProvider.name, None)

    async def test_master_scrape_caps_candidates(self):
        tmpdir = tempfile.mkdtemp()
        try:
            db_path = os.path.join(tmpdir, "test_cap.db")
            with patch.object(main, "DB_FILE", db_path):
                main.init_db()
                provider = FakeCappingProvider()
                with patch.object(main, "_providers_for_search", lambda always_live=False: [provider]), \
                     patch.object(main, "get_healthy_browser", AsyncMock(return_value=None)), \
                     patch.object(main, "SHARED_HTTP_CLIENT", None):
                    result = await main.master_scrape(
                        "Test Team", team_name="Test Team", team_id="test_team", always_live=True
                    )
            # More than MAX_STREAM_CANDIDATES unique URLs were returned by the
            # provider; master_scrape must trim them itself rather than rely on
            # a downstream merge step to do it.
            self.assertEqual(len(result), main.MAX_STREAM_CANDIDATES)
        finally:
            gc.collect()
            shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
