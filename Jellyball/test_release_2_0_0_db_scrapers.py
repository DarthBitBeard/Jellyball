"""Regression tests for the 2.0.0 DB/scraper/alerting work:

D1: per-provider base-URL overrides stored in app_settings, live-reloaded
    without a DB read on the request path, plus the dashboard's
    POST /settings/providers route.
D2: the shared HtmlAggregatorScraper._fetch_html() HTTP->Playwright fallback,
    reused by DaddyLiveScraper instead of duplicating it.
D6: EXTRA_NON_ENGLISH_MARKERS extends (never replaces) the built-in
    non-English channel tag list.
D7: schema_migrations-based migrations (idempotent on old and new DBs), a
    thread-local/per-DB-path connection cache with reconnect-on-broken-
    connection, and close_all_db_connections().
D9: send_alert()'s webhook retries (network errors, 5xx, 429 w/ Retry-After),
    reusing one httpx.AsyncClient per send.

Everything here uses fakes/mocks/temp SQLite files: no real Chromium, no real
network calls, and asyncio.sleep is patched out wherever retry backoff would
otherwise slow the suite down.
"""
import asyncio
import gc
import os
import shutil
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))

import main
import threading
import stream_extractor


def _reset_playwright_page_globals():
    stream_extractor._PLAYWRIGHT_PAGE_SEMAPHORE = None
    stream_extractor._PLAYWRIGHT_PAGE_SEMAPHORE_LOOP = None
    stream_extractor._PLAYWRIGHT_PAGES_IN_USE = 0


class _FakePage:
    def __init__(self, html="<html>ok</html>"):
        self.html = html
        self.goto_calls = []
        self.waited_networkidle = False
        self.closed = False

    async def goto(self, url, wait_until=None, timeout=None):
        self.goto_calls.append({"url": url, "wait_until": wait_until, "timeout": timeout})

    async def wait_for_load_state(self, state, timeout=None):
        self.waited_networkidle = True

    async def content(self):
        return self.html

    async def close(self):
        self.closed = True


class _FakeContext:
    def __init__(self, page):
        self._page = page
        self.closed = False

    async def new_page(self):
        return self._page

    async def close(self):
        self.closed = True


class _FakeBrowser:
    def __init__(self, page):
        self._page = page
        self._connected = True

    def is_connected(self):
        return self._connected

    async def new_context(self, user_agent=None):
        return _FakeContext(self._page)

    async def new_page(self):
        return self._page


class _TempDbCase(unittest.TestCase):
    """A fresh temp-file SQLite DB per test, with DB_FILE patched onto it and
    cached connections closed on teardown (so Windows can delete the temp
    dir)."""

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self._db_path = os.path.join(self._tmpdir, "test.db")
        self._db_patch = patch.object(main, "DB_FILE", self._db_path)
        self._db_patch.start()

    def tearDown(self):
        self._db_patch.stop()
        main.close_all_db_connections()
        gc.collect()
        shutil.rmtree(self._tmpdir, ignore_errors=True)


class _ProviderOverrideCase(unittest.TestCase):
    """Snapshots/restores the module-level override dict, since it's shared
    global state across the whole test process (and across test files)."""

    def setUp(self):
        self._overrides_snapshot = dict(main._PROVIDER_BASE_URL_OVERRIDES)

    def tearDown(self):
        main._PROVIDER_BASE_URL_OVERRIDES.clear()
        main._PROVIDER_BASE_URL_OVERRIDES.update(self._overrides_snapshot)


# --------------------------------------------------------------------------- #
# D1: provider base-URL override + live reload
# --------------------------------------------------------------------------- #
class ProviderUrlOverrideTests(_ProviderOverrideCase):
    def _provider(self):
        for provider in main.ACTIVE_PROVIDERS:
            if isinstance(provider, main.HtmlAggregatorScraper):
                return provider
        raise AssertionError("no HtmlAggregatorScraper in ACTIVE_PROVIDERS")

    def test_defaults_to_env_url_with_no_override(self):
        provider = self._provider()
        main._PROVIDER_BASE_URL_OVERRIDES.pop(provider.name, None)
        self.assertEqual(provider.base_url, provider._default_base_url)

    def test_set_override_applies_live(self):
        provider = self._provider()
        main._set_provider_url_override(provider, "https://overridden.example.test")
        self.assertEqual(provider.base_url, "https://overridden.example.test")

    def test_blank_override_resets_to_default(self):
        provider = self._provider()
        main._set_provider_url_override(provider, "https://overridden.example.test")
        main._set_provider_url_override(provider, "")
        self.assertEqual(provider.base_url, provider._default_base_url)

    def test_get_scan_urls_reflects_override_without_reconstruction(self):
        provider = self._provider()
        original_scan_urls = provider.get_scan_urls()
        main._set_provider_url_override(provider, "https://overridden.example.test")
        new_scan_urls = provider.get_scan_urls()
        self.assertNotEqual(original_scan_urls, new_scan_urls)
        self.assertTrue(new_scan_urls[0].startswith("https://overridden.example.test"))

    def test_override_change_invalidates_stale_index_cache_entries(self):
        provider = self._provider()
        old_base = provider.base_url
        stale_key = f"{old_base}/some/page"
        other_key = "https://totally-unrelated.example.test/page"
        main._SCRAPE_INDEX_CACHE[stale_key] = (1e18, "<html>stale</html>")
        main._SCRAPE_INDEX_CACHE[other_key] = (1e18, "<html>unrelated</html>")
        try:
            main._set_provider_url_override(provider, "https://new-domain.example.test")
            self.assertNotIn(stale_key, main._SCRAPE_INDEX_CACHE)
            self.assertIn(other_key, main._SCRAPE_INDEX_CACHE)
        finally:
            main._SCRAPE_INDEX_CACHE.pop(stale_key, None)
            main._SCRAPE_INDEX_CACHE.pop(other_key, None)

    def test_load_provider_url_overrides_reads_from_db(self):
        provider = self._provider()
        tmpdir = tempfile.mkdtemp()
        try:
            db_path = os.path.join(tmpdir, "test_overrides.db")
            with patch.object(main, "DB_FILE", db_path):
                main.init_db()
                main.set_setting(main._provider_url_setting_key(provider.name), "https://from-db.example.test")
                main._load_provider_url_overrides()
                self.assertEqual(provider.base_url, "https://from-db.example.test")
        finally:
            main.close_all_db_connections()
            gc.collect()
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_load_provider_url_overrides_clears_stale_entry_for_fresh_db(self):
        """A provider with no stored override in the DB being (re)loaded from
        must have any leftover in-memory override cleared -- this is what
        keeps tests (and successive real starts against different DBs)
        isolated from each other."""
        provider = self._provider()
        main._PROVIDER_BASE_URL_OVERRIDES[provider.name] = "https://leftover.example.test"
        tmpdir = tempfile.mkdtemp()
        try:
            db_path = os.path.join(tmpdir, "fresh.db")
            with patch.object(main, "DB_FILE", db_path):
                main.init_db()  # calls _load_provider_url_overrides() itself
            self.assertEqual(provider.base_url, provider._default_base_url)
        finally:
            main.close_all_db_connections()
            gc.collect()
            shutil.rmtree(tmpdir, ignore_errors=True)


# --------------------------------------------------------------------------- #
# D1: POST /settings/providers
# --------------------------------------------------------------------------- #
def _dashboard_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=main.app),
        base_url="http://127.0.0.1:8000",
        follow_redirects=False,
    )


class ProviderSettingsRouteTests(_TempDbCase, _ProviderOverrideCase):
    def setUp(self):
        _TempDbCase.setUp(self)
        _ProviderOverrideCase.setUp(self)
        main.init_db()
        self._password_patch = patch.object(main, "DASHBOARD_PASSWORD", "")
        self._password_patch.start()
        self._provider = next(
            p for p in main.ACTIVE_PROVIDERS if isinstance(p, main.HtmlAggregatorScraper)
        )

    def tearDown(self):
        self._password_patch.stop()
        _ProviderOverrideCase.tearDown(self)
        _TempDbCase.tearDown(self)

    def test_saves_valid_url_live_and_redirects(self):
        async def exercise():
            async with _dashboard_client() as client:
                return await client.post(
                    "/settings/providers",
                    data={f"url_{self._provider.name}": "https://new-domain.example.test"},
                )

        response = asyncio.run(exercise())
        self.assertEqual(response.status_code, 303)
        self.assertIn("providers_saved", response.headers["location"])
        self.assertEqual(self._provider.base_url, "https://new-domain.example.test")
        self.assertEqual(
            main.get_setting(main._provider_url_setting_key(self._provider.name)),
            "https://new-domain.example.test",
        )

    def test_rejects_invalid_url_and_saves_nothing(self):
        async def exercise():
            async with _dashboard_client() as client:
                return await client.post(
                    "/settings/providers",
                    data={f"url_{self._provider.name}": "not-a-url"},
                )

        response = asyncio.run(exercise())
        self.assertEqual(response.status_code, 303)
        self.assertIn("providers_invalid", response.headers["location"])
        self.assertEqual(main.get_setting(main._provider_url_setting_key(self._provider.name)), "")
        self.assertEqual(self._provider.base_url, self._provider._default_base_url)

    def test_rejects_private_host_url(self):
        async def exercise():
            async with _dashboard_client() as client:
                return await client.post(
                    "/settings/providers",
                    data={f"url_{self._provider.name}": "http://127.0.0.1/evil"},
                )

        response = asyncio.run(exercise())
        self.assertIn("providers_invalid", response.headers["location"])

    def test_blank_field_resets_to_default(self):
        main._set_provider_url_override(self._provider, "https://was-overridden.example.test")
        main.set_setting(main._provider_url_setting_key(self._provider.name), "https://was-overridden.example.test")

        async def exercise():
            async with _dashboard_client() as client:
                return await client.post("/settings/providers", data={})

        response = asyncio.run(exercise())
        self.assertEqual(response.status_code, 303)
        self.assertIn("providers_saved", response.headers["location"])
        self.assertEqual(self._provider.base_url, self._provider._default_base_url)
        self.assertEqual(main.get_setting(main._provider_url_setting_key(self._provider.name)), "")

    def test_one_invalid_field_blocks_the_whole_save(self):
        """All-or-nothing: a second, valid field in the same submission must
        not be saved when another field in it fails validation."""
        other_provider = next(
            p for p in main.ACTIVE_PROVIDERS
            if isinstance(p, main.HtmlAggregatorScraper) and p is not self._provider
        )

        async def exercise():
            async with _dashboard_client() as client:
                return await client.post(
                    "/settings/providers",
                    data={
                        f"url_{self._provider.name}": "https://good.example.test",
                        f"url_{other_provider.name}": "javascript:alert(1)",
                    },
                )

        response = asyncio.run(exercise())
        self.assertIn("providers_invalid", response.headers["location"])
        self.assertEqual(main.get_setting(main._provider_url_setting_key(self._provider.name)), "")
        self.assertNotEqual(self._provider.base_url, "https://good.example.test")


# --------------------------------------------------------------------------- #
# D2: shared _fetch_html() HTTP->Playwright fallback helper
# --------------------------------------------------------------------------- #
class SharedFetchHtmlTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        _reset_playwright_page_globals()

    async def asyncTearDown(self):
        _reset_playwright_page_globals()

    async def test_http_first_short_circuits_playwright(self):
        scraper = main.ISportSurgeScraper()
        browser = _FakeBrowser(_FakePage("<html>should not be used</html>"))

        async def fake_fetch_bounded_text(*args, **kwargs):
            return "<html>from http</html>"

        with patch.object(main, "fetch_bounded_text", fake_fetch_bounded_text):
            result = await scraper._fetch_html(None, "https://isportsurge.ws/cfb/livestreams2", browser)
        self.assertEqual(result, "<html>from http</html>")

    async def test_playwright_fallback_waits_for_networkidle_by_default(self):
        scraper = main.ISportSurgeScraper()
        page = _FakePage("<html>rendered</html>")
        browser = _FakeBrowser(page)

        async def fake_fetch_bounded_text(*args, **kwargs):
            return None

        with patch.object(main, "fetch_bounded_text", fake_fetch_bounded_text):
            result = await scraper._fetch_html(None, "https://isportsurge.ws/x", browser)
        self.assertEqual(result, "<html>rendered</html>")
        self.assertTrue(page.waited_networkidle)
        self.assertEqual(page.goto_calls[0]["timeout"], 35000)

    async def test_playwright_fallback_settle_seconds_skips_networkidle_wait(self):
        scraper = main.ISportSurgeScraper()
        page = _FakePage("<html>settled</html>")
        browser = _FakeBrowser(page)

        async def fake_fetch_bounded_text(*args, **kwargs):
            return None

        with patch.object(main, "fetch_bounded_text", fake_fetch_bounded_text), \
                patch.object(asyncio, "sleep", AsyncMockNoOp()) as sleep_mock:
            result = await scraper._fetch_html(
                None, "https://isportsurge.ws/x", browser, settle_seconds=2, goto_timeout=25000
            )
        self.assertEqual(result, "<html>settled</html>")
        self.assertFalse(page.waited_networkidle)
        self.assertEqual(page.goto_calls[0]["timeout"], 25000)
        sleep_mock.assert_awaited_once_with(2)

    async def test_fetch_index_page_delegates_to_fetch_html_with_base_defaults(self):
        scraper = main.ISportSurgeScraper()
        page = _FakePage("<html>index</html>")
        browser = _FakeBrowser(page)

        async def fake_fetch_bounded_text(*args, **kwargs):
            return None

        with patch.object(main, "fetch_bounded_text", fake_fetch_bounded_text):
            result = await scraper._fetch_index_page(None, "https://isportsurge.ws/x", browser)
        self.assertEqual(result, "<html>index</html>")
        self.assertTrue(page.waited_networkidle)
        self.assertEqual(page.goto_calls[0]["timeout"], 35000)

    async def test_daddylive_fetch_directory_page_uses_settle_and_shorter_timeout(self):
        scraper = main.DaddyLiveScraper()
        page = _FakePage("<html>directory</html>")
        browser = _FakeBrowser(page)

        async def fake_fetch_bounded_text(*args, **kwargs):
            return None

        with patch.object(main, "fetch_bounded_text", fake_fetch_bounded_text), \
                patch.object(asyncio, "sleep", AsyncMockNoOp()) as sleep_mock:
            result = await scraper._fetch_directory_page(None, "https://dlhd.pk/x", browser)
        self.assertEqual(result, "<html>directory</html>")
        self.assertFalse(page.waited_networkidle)
        self.assertEqual(page.goto_calls[0]["timeout"], 25000)
        sleep_mock.assert_awaited_once_with(2)

    async def test_base_find_matches_catches_per_page_errors(self):
        """HtmlAggregatorScraper._find_matches must keep scanning other scan
        URLs (and not raise) when one page's fetch blows up -- the original
        behavior before the shared _find_matches_using() skeleton. (Note:
        _get_cached_index_html already swallows a `fetch_page` exception on
        its own, so this exercises catch_page_errors via the loop's outer
        try/except the same way the original per-page try/except did.)"""
        scraper = main.ISportSurgeScraper()

        async def boom(client, url, browser):
            raise RuntimeError("boom")

        with patch.object(scraper, "_fetch_index_page", boom):
            result = await scraper._find_matches(None, ["test"], browser=None)
        self.assertEqual(result, [])

    async def test_base_find_matches_catches_per_page_parse_errors(self):
        """Unlike a fetch error (already swallowed inside
        _get_cached_index_html), a *parse* error only reaches _find_matches's
        own try/except -- this is where catch_page_errors=True actually
        matters for the base class."""
        scraper = main.ISportSurgeScraper()

        async def fake_fetch_index_page(client, url, browser):
            return "<html></html>"

        def boom_parse(*args, **kwargs):
            raise RuntimeError("boom")

        with patch.object(scraper, "_fetch_index_page", fake_fetch_index_page), \
                patch.object(scraper, "_parse_matches_from_html", boom_parse):
            result = await scraper._find_matches(None, ["test"], browser=None)
        self.assertEqual(result, [])

    async def test_daddylive_find_matches_propagates_per_page_parse_errors(self):
        """DaddyLiveScraper._find_matches never wrapped page errors in a
        try/except; the shared skeleton must preserve that via
        catch_page_errors=False. (A fetch-step exception is already absorbed
        by _get_cached_index_html for both provider types, so this has to
        come from the parse step to exercise the actual difference.)"""
        scraper = main.DaddyLiveScraper()

        async def fake_fetch_directory_page(client, url, browser):
            return "<html></html>"

        def boom_parse(*args, **kwargs):
            raise RuntimeError("boom")

        with patch.object(scraper, "_fetch_directory_page", fake_fetch_directory_page), \
                patch.object(scraper, "_parse_channel_matches_from_html", boom_parse):
            with self.assertRaises(RuntimeError):
                await scraper._find_matches(None, ["test"], browser=None)


class AsyncMockNoOp:
    """A minimal awaitable-call recorder, standing in for
    unittest.mock.AsyncMock so this file has no hard dependency on its
    exact import path across Python versions."""

    def __init__(self):
        self.calls = []

    async def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))

    def assert_awaited_once_with(self, *args, **kwargs):
        assert len(self.calls) == 1, f"expected exactly 1 call, got {len(self.calls)}"
        assert self.calls[0] == (args, kwargs), f"call args {self.calls[0]} != expected {(args, kwargs)}"


# --------------------------------------------------------------------------- #
# D6: EXTRA_NON_ENGLISH_MARKERS extends (not replaces) the default list
# --------------------------------------------------------------------------- #
class ExtraNonEnglishMarkersTests(unittest.TestCase):
    def test_defaults_still_work_with_no_env_var(self):
        with patch.object(main, "_EXTRA_NON_ENGLISH_MARKERS", frozenset()):
            self.assertFalse(main._is_non_english_channel("ESPN USA"))
            self.assertTrue(main._is_non_english_channel("ESPN DE"))

    def test_extra_single_word_marker_extends_detection(self):
        with patch.object(main, "_EXTRA_NON_ENGLISH_MARKERS", frozenset({"klingon"})):
            self.assertTrue(main._is_non_english_channel("Sports Klingon Feed"))
            self.assertFalse(main._is_non_english_channel("ESPN USA"))

    def test_extra_multi_word_marker_matches_as_substring(self):
        with patch.object(main, "_EXTRA_NON_ENGLISH_MARKERS", frozenset({"feed alt lang"})):
            self.assertTrue(main._is_non_english_channel("Some Feed Alt Lang Channel"))


# --------------------------------------------------------------------------- #
# D7(a): schema_migrations table + idempotent migrations
# --------------------------------------------------------------------------- #
class SchemaMigrationTests(_TempDbCase):
    def test_fresh_db_records_every_migration_and_gets_new_indexes(self):
        main.init_db()
        conn = sqlite3.connect(self._db_path)
        try:
            versions = {row[0] for row in conn.execute("SELECT version FROM schema_migrations")}
            self.assertEqual(versions, {version for version, _ in main.SCHEMA_MIGRATIONS})
            index_names = {row[1] for row in conn.execute("PRAGMA index_list(teams)")}
            self.assertIn("idx_teams_catalog_key", index_names)
            self.assertIn("idx_teams_is_favorite", index_names)
        finally:
            conn.close()

    def test_missing_columns_are_added_and_existing_rows_preserved(self):
        """Simulate a pre-migration DB: a `teams` table missing every column
        the old ad-hoc ALTER TABLE loop used to backfill."""
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("CREATE TABLE teams (team_id TEXT PRIMARY KEY, name TEXT, query TEXT)")
            conn.execute("INSERT INTO teams (team_id, name, query) VALUES ('t1', 'Team One', 'team one')")
            conn.commit()
        finally:
            conn.close()

        main.init_db()

        conn = sqlite3.connect(self._db_path)
        try:
            columns = {row[1] for row in conn.execute("PRAGMA table_info(teams)").fetchall()}
            for expected in (
                "logo_url", "start_time", "stop_time", "category", "source_id",
                "content_type", "search_terms", "always_live", "catalog_key",
                "is_favorite", "auto_disable_after",
            ):
                self.assertIn(expected, columns)
            row = conn.execute(
                "SELECT team_id, name, query, is_favorite FROM teams WHERE team_id='t1'"
            ).fetchone()
            self.assertEqual(row, ("t1", "Team One", "team one", 0))
        finally:
            conn.close()

    def test_init_db_is_idempotent_when_run_twice(self):
        main.init_db()
        main.init_db()  # must not raise (duplicate ALTER/duplicate migration row)
        conn = sqlite3.connect(self._db_path)
        try:
            rows = conn.execute("SELECT version FROM schema_migrations").fetchall()
            self.assertEqual(sorted(r[0] for r in rows), sorted(v for v, _ in main.SCHEMA_MIGRATIONS))
        finally:
            conn.close()

    def test_migration_function_is_idempotent_when_columns_already_exist(self):
        main.init_db()
        with main._db_session() as conn:
            # Must not raise even though every column from migration 1 is
            # already present (a fresh CREATE TABLE already declares them).
            main._migrate_add_team_columns(conn)


# --------------------------------------------------------------------------- #
# D7(b): thread-local/per-DB-path connection cache + reconnect + shutdown
# --------------------------------------------------------------------------- #
class ConnectionCacheTests(_TempDbCase):
    def test_same_thread_same_path_reuses_connection(self):
        first = main._connect_db()
        second = main._connect_db()
        self.assertIs(first, second)

    def test_different_db_path_on_same_thread_gets_its_own_connection(self):
        first = main._connect_db()
        other_path = os.path.join(self._tmpdir, "other.db")
        with patch.object(main, "DB_FILE", other_path):
            other = main._connect_db()
            self.assertIsNot(first, other)
            other.execute("SELECT 1")
        # Switching DB_FILE back returns the original cached connection.
        again = main._connect_db()
        self.assertIs(first, again)

    def test_reconnects_after_underlying_connection_is_closed(self):
        first = main._connect_db()
        first.close()
        second = main._connect_db()
        self.assertIsNot(first, second)
        # Must be a live, usable connection, not another closed one.
        second.execute("SELECT 1")

    def test_close_all_db_connections_clears_the_cache(self):
        conn = main._connect_db()
        self.assertIn((threading.get_ident(), main.DB_FILE), main._DB_CONNECTIONS)
        main.close_all_db_connections()
        self.assertEqual(main._DB_CONNECTIONS, {})
        # A later call transparently opens a fresh connection.
        fresh = main._connect_db()
        fresh.execute("SELECT 1")

    def test_db_session_commits_without_closing_the_reused_connection(self):
        main.init_db()
        with main._db_session() as conn:
            conn.execute("INSERT OR REPLACE INTO app_settings (key, value) VALUES ('k', 'v')")
        # The connection used inside the `with` block must still be open and
        # usable afterward (proof it wasn't closed), and the write must have
        # been committed (proof commit doesn't depend on close).
        cached = main._connect_db()
        cached.execute("SELECT 1")
        self.assertEqual(main.get_setting("k"), "v")

    def test_db_session_rolls_back_on_exception_without_closing(self):
        main.init_db()
        main.set_setting("rollback_key", "before")
        with self.assertRaises(ValueError):
            with main._db_session() as conn:
                conn.execute("UPDATE app_settings SET value='after' WHERE key='rollback_key'")
                raise ValueError("boom")
        self.assertEqual(main.get_setting("rollback_key"), "before")
        # Connection still usable after the rollback (not closed).
        main._connect_db().execute("SELECT 1")


# --------------------------------------------------------------------------- #
# D9: send_alert() webhook retries
# --------------------------------------------------------------------------- #
class WebhookRetryTests(unittest.IsolatedAsyncioTestCase):
    async def _client_with(self, handler):
        return httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async def test_success_on_first_attempt_no_retry(self):
        calls = []

        def handler(request):
            calls.append(request)
            return httpx.Response(200)

        client = await self._client_with(handler)
        async with client:
            with patch.object(asyncio, "sleep", AsyncMockNoOp()):
                await main._post_webhook_with_retries(client, "Discord", "https://discord.example/webhook")
        self.assertEqual(len(calls), 1)

    async def test_5xx_is_retried_then_succeeds(self):
        calls = []

        def handler(request):
            calls.append(request)
            if len(calls) == 1:
                return httpx.Response(503)
            return httpx.Response(200)

        client = await self._client_with(handler)
        sleep_mock = AsyncMockNoOp()
        async with client:
            with patch.object(asyncio, "sleep", sleep_mock):
                await main._post_webhook_with_retries(client, "Discord", "https://discord.example/webhook")
        self.assertEqual(len(calls), 2)
        self.assertEqual(len(sleep_mock.calls), 1)
        self.assertEqual(sleep_mock.calls[0][0], (1.0,))

    async def test_5xx_exhausts_retries_and_logs_provider_only(self):
        def handler(request):
            return httpx.Response(500)

        client = await self._client_with(handler)
        async with client:
            with patch.object(asyncio, "sleep", AsyncMockNoOp()):
                with self.assertLogs("jellyball", level="WARNING") as cm:
                    await main._post_webhook_with_retries(
                        client, "Discord", "https://discord.example/webhook/supersecrettoken"
                    )
        joined = "\n".join(cm.output)
        self.assertIn("Discord", joined)
        self.assertNotIn("supersecrettoken", joined)
        self.assertNotIn("discord.example", joined)

    async def test_plain_4xx_is_not_retried(self):
        calls = []

        def handler(request):
            calls.append(request)
            return httpx.Response(404)

        client = await self._client_with(handler)
        async with client:
            with patch.object(asyncio, "sleep", AsyncMockNoOp()) as sleep_mock:
                await main._post_webhook_with_retries(client, "Telegram", "https://telegram.example/webhook")
        self.assertEqual(len(calls), 1)
        self.assertEqual(sleep_mock.calls, [])

    async def test_429_honors_retry_after_header(self):
        calls = []

        def handler(request):
            calls.append(request)
            if len(calls) == 1:
                return httpx.Response(429, headers={"Retry-After": "5"})
            return httpx.Response(200)

        client = await self._client_with(handler)
        sleep_mock = AsyncMockNoOp()
        async with client:
            with patch.object(asyncio, "sleep", sleep_mock):
                await main._post_webhook_with_retries(client, "Discord", "https://discord.example/webhook")
        self.assertEqual(len(calls), 2)
        self.assertEqual(sleep_mock.calls[0][0], (5.0,))

    async def test_429_retry_after_is_capped_at_ten_seconds(self):
        calls = []

        def handler(request):
            calls.append(request)
            if len(calls) == 1:
                return httpx.Response(429, headers={"Retry-After": "999"})
            return httpx.Response(200)

        client = await self._client_with(handler)
        sleep_mock = AsyncMockNoOp()
        async with client:
            with patch.object(asyncio, "sleep", sleep_mock):
                await main._post_webhook_with_retries(client, "Discord", "https://discord.example/webhook")
        self.assertEqual(sleep_mock.calls[0][0], (10.0,))

    async def test_network_error_is_retried(self):
        calls = []

        def handler(request):
            calls.append(request)
            if len(calls) == 1:
                raise httpx.ConnectError("boom", request=request)
            return httpx.Response(200)

        client = await self._client_with(handler)
        sleep_mock = AsyncMockNoOp()
        async with client:
            with patch.object(asyncio, "sleep", sleep_mock):
                await main._post_webhook_with_retries(client, "Discord", "https://discord.example/webhook")
        self.assertEqual(len(calls), 2)
        self.assertEqual(sleep_mock.calls[0][0], (1.0,))

    async def test_send_alert_reuses_one_client_per_webhook_not_per_attempt(self):
        """send_alert() must construct exactly one httpx.AsyncClient per
        webhook (Discord/Telegram), even though _post_webhook_with_retries
        makes multiple attempts against it."""
        created_clients = []
        real_async_client = httpx.AsyncClient

        attempt_counts = {"n": 0}

        def handler(request):
            attempt_counts["n"] += 1
            if attempt_counts["n"] == 1:
                return httpx.Response(500)
            return httpx.Response(200)

        def counting_client_factory(*args, **kwargs):
            kwargs.pop("timeout", None)
            client = real_async_client(transport=httpx.MockTransport(handler))
            created_clients.append(client)
            return client

        with patch.object(main, "get_notification_config", _AsyncReturn({
            "discord_webhook_url": "https://discord.example/webhook",
            "telegram_bot_token": "",
            "telegram_chat_id": "",
        })):
            with patch.object(httpx, "AsyncClient", counting_client_factory):
                with patch.object(asyncio, "sleep", AsyncMockNoOp()):
                    await main.send_alert("Title", "Message", "warning")

        self.assertEqual(len(created_clients), 1)
        self.assertEqual(attempt_counts["n"], 2)


class _AsyncReturn:
    """Callable returning a coroutine that resolves to `value` -- used in
    place of unittest.mock.AsyncMock(return_value=...) for get_notification_config."""

    def __init__(self, value):
        self._value = value

    async def __call__(self, *args, **kwargs):
        return self._value


if __name__ == "__main__":
    unittest.main()
