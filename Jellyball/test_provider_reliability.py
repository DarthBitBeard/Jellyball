"""Provider reliability lane (R1-R5): outcomes, attribution, alerts, dry-run, settings."""

import asyncio
import gc
import os
import shutil
import sqlite3
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

import db
import failover
import main
import provider_alerts
import provider_settings
import provider_telemetry as telemetry
import provider_tools
import scrapers
import security
import sessions
import state


class TempDbCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self._patch = patch.object(db, "DB_FILE", os.path.join(self.tmp, "t.db"))
        self._patch.start()
        db.init_db()
        provider_settings.load()

    def tearDown(self):
        self._patch.stop()
        db.close_all_db_connections()
        gc.collect()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def rows(self, sql, args=()):
        with db._db_session() as conn:
            return conn.execute(sql, args).fetchall()


class MigrationTests(TempDbCase):
    def test_provider_performance_gains_outcome_columns(self):
        columns = {row[1] for row in self.rows("PRAGMA table_info(provider_performance)")}
        self.assertTrue({"outcome", "error_class", "index_events", "matches"} <= columns)

    def test_a_2_0_0_table_is_upgraded_in_place_and_keeps_its_rows(self):
        import migrations_providers
        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE provider_performance (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, provider TEXT, response_time_ms INTEGER, success INTEGER DEFAULT 1)"
        )
        conn.execute("INSERT INTO provider_performance (provider, response_time_ms, success) VALUES ('X', 5, 1)")
        migrations_providers._provider_performance_outcomes(conn)
        migrations_providers._provider_performance_outcomes(conn)  # idempotent
        self.assertEqual(conn.execute("SELECT provider, outcome FROM provider_performance").fetchall(), [("X", None)])

    def test_retention_is_fourteen_days(self):
        with db._db_session() as conn:
            conn.execute("INSERT INTO provider_performance (provider, response_time_ms, success, timestamp) VALUES ('Old', 1, 1, datetime('now', '-15 days'))")
            conn.execute("INSERT INTO provider_performance (provider, response_time_ms, success, timestamp) VALUES ('Mid', 1, 1, datetime('now', '-10 days'))")
            conn.commit()
        db.prune_database_logs_once()
        self.assertEqual([r[0] for r in self.rows("SELECT provider FROM provider_performance")], ["Mid"])


class OutcomeTests(unittest.TestCase):
    def test_outcomes(self):
        run = telemetry.RunStats()
        self.assertEqual(telemetry.derive_outcome([{"url": "u"}], run), ("ok", None))
        self.assertEqual(telemetry.derive_outcome([], run), ("empty", None))
        self.assertEqual(telemetry.derive_outcome([], run, timed_out=True), ("timeout", "timeout"))
        self.assertEqual(telemetry.derive_outcome([], run, exc=httpx.ConnectError("x")), ("error", "connect"))
        run.page_errors.append("http_403")
        self.assertEqual(telemetry.derive_outcome([], run), ("error", "http_403"))
        run.index_events = 12  # the page loaded fine: a page error elsewhere does not make it an error
        self.assertEqual(telemetry.derive_outcome([], run), ("empty", None))

    def test_error_classes_carry_no_text(self):
        self.assertEqual(telemetry.classify_error(RuntimeError("https://x.test/?token=SECRET")), "other")

    def test_hooks_are_noops_outside_a_run_and_visible_inside(self):
        telemetry._RUN.set(None)
        telemetry.report_index_events(5)  # must not raise
        run = telemetry.begin_run("P")
        telemetry.report_index_events(3)
        telemetry.report_index_events(4)
        telemetry.report_matches(2)
        telemetry.report_page_error("no_content")
        self.assertEqual((run.index_events, run.matches, run.page_errors), (7, 2, ["no_content"]))

    def test_hook_reports_from_a_worker_thread_into_the_same_run(self):
        async def go():
            run = telemetry.begin_run("P")
            await asyncio.to_thread(telemetry.report_index_events, 9)
            return run

        self.assertEqual(asyncio.run(go()).index_events, 9)


class IndexParserHookTests(unittest.TestCase):
    HTML = (
        '<a href="/watch/one">Team A vs Team B</a><a href="/watch/two">Other vs Another</a>'
        '<a href="/about">About</a>'
    )

    def test_generic_parser_reports_events_listed_even_when_none_match(self):
        provider = scrapers.HtmlAggregatorScraper("Fake", "https://fake.example", event_path_hints=["/watch/"])
        run = telemetry.begin_run("Fake")
        provider._parse_matches_from_html(self.HTML, "https://fake.example/", ["Zzz Nomatch"], [], set())
        self.assertEqual(run.index_events, 2)

    def test_empty_index_page_reports_zero(self):
        provider = scrapers.HtmlAggregatorScraper("Fake", "https://fake.example", event_path_hints=["/watch/"])
        run = telemetry.begin_run("Fake")
        provider._parse_matches_from_html("<html></html>", "https://fake.example/", ["Team A"], [], set())
        self.assertEqual((run.index_events, run.index_pages), (0, 1))


class PageErrorsAreNotSwallowedTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_page_fetch_is_reported_as_a_page_error(self):
        provider = scrapers.HtmlAggregatorScraper("Fake", "https://fake.example", ["/cat"])
        scrapers._SCRAPE_INDEX_CACHE.clear()
        run = telemetry.begin_run("Fake")

        async def boom(client, url, browser):
            raise httpx.ConnectError("down")

        await provider._find_matches_using(
            None, ["x"], None, fetch_page=boom, parse_matches=lambda *a: None, catch_page_errors=True
        )
        scrapers._SCRAPE_INDEX_CACHE.clear()
        self.assertIn("connect", run.page_errors)

    async def test_empty_page_is_reported_as_no_content(self):
        provider = scrapers.HtmlAggregatorScraper("Fake", "https://fake.example")
        scrapers._SCRAPE_INDEX_CACHE.clear()
        run = telemetry.begin_run("Fake")

        async def nothing(client, url, browser):
            return None

        await provider._find_matches_using(
            None, ["x"], None, fetch_page=nothing, parse_matches=lambda *a: None
        )
        scrapers._SCRAPE_INDEX_CACHE.clear()
        self.assertEqual(run.page_errors, ["no_content"])


class FakeProvider:
    def __init__(self, name, streams=None, exc=None, index_events=None):
        self.name, self._streams, self._exc, self._events = name, streams or [], exc, index_events

    async def search(self, terms, browser=None, http_client=None):
        if self._events is not None:
            telemetry.report_index_events(self._events)
        if self._exc:
            raise self._exc
        return list(self._streams)


def _stream(provider):
    return {"url": f"https://{provider.lower()}.example/a.m3u8", "provider": provider, "match_score": 100,
            "match_title": "T", "match_url": "https://x.example/m", "referer": "", "origin": ""}


class MasterScrapeRecordsOutcomesTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.mkdtemp()
        self._patch = patch.object(db, "DB_FILE", os.path.join(self.tmp, "t.db"))
        self._patch.start()
        db.init_db()
        scrapers._PROVIDER_BREAKERS.clear()

    async def asyncTearDown(self):
        self._patch.stop()
        db.close_all_db_connections()
        gc.collect()
        shutil.rmtree(self.tmp, ignore_errors=True)
        scrapers._PROVIDER_BREAKERS.clear()

    async def _run(self, providers):
        with patch.object(scrapers, "_providers_for_search", lambda always_live=False: providers), \
                patch.object(scrapers, "get_healthy_browser", AsyncMock(return_value=None)), \
                patch.object(scrapers.provider_alerts, "check_soon"), \
                patch.object(state, "SHARED_HTTP_CLIENT", None):
            await scrapers.master_scrape("T", team_name="T", team_id="t", always_live=True)
        with db._db_session() as conn:
            return {r[0]: r[1:] for r in conn.execute(
                "SELECT provider, outcome, error_class, index_events, matches, success FROM provider_performance")}

    async def test_ok_empty_and_error_are_told_apart(self):
        rows = await self._run([
            FakeProvider("Ok", [_stream("Ok")], index_events=40),
            FakeProvider("Empty", [], index_events=0),
            FakeProvider("Boom", exc=httpx.ConnectError("x")),
        ])
        self.assertEqual(rows["Ok"][0], "ok")
        self.assertEqual(rows["Ok"][2], 40)
        self.assertEqual((rows["Empty"][0], rows["Empty"][2], rows["Empty"][4]), ("empty", 0, 1))
        self.assertEqual((rows["Boom"][0], rows["Boom"][1], rows["Boom"][4]), ("error", "connect", 0))

    async def test_a_search_that_returns_nothing_because_every_page_failed_is_an_error(self):
        class SilentlyBroken(FakeProvider):
            async def search(self, terms, browser=None, http_client=None):
                telemetry.report_page_error("http_503")
                return []

        rows = await self._run([SilentlyBroken("Broken")])
        self.assertEqual(rows["Broken"][:2], ("error", "http_503"))
        self.assertEqual(rows["Broken"][4], 0)  # success=0 now, though search() "returned"
        self.assertEqual(telemetry.last_run("Broken")["outcome"], "error")

    async def test_last_run_tracks_last_success(self):
        await self._run([FakeProvider("Flaky", [_stream("Flaky")])])
        first = telemetry.last_run("Flaky")["last_success_at"]
        self.assertIsNotNone(first)
        await self._run([FakeProvider("Flaky", [])])
        self.assertEqual(telemetry.last_run("Flaky")["last_success_at"], first)


class LeaderboardTests(TempDbCase):
    def test_failovers_do_not_divide_by_themselves(self):
        rows = telemetry.build_leaderboard({"A": {"ok": 9, "error": 1}}, {"A": 3, "B": 2})
        by_name = {r["provider"]: r for r in rows}
        self.assertEqual(by_name["A"]["rate"], 90.0)
        self.assertEqual(by_name["A"]["failovers"], 3)
        self.assertIsNone(by_name["B"]["rate"])  # failovers but no searches: no rate, not 0%
        self.assertEqual([r["provider"] for r in rows], ["A", "B"])

    def test_leaderboard_reads_the_database(self):
        telemetry.record_outcome_sync("A", 10, "ok")
        telemetry.record_outcome_sync("A", 10, "timeout", "timeout")
        with db._db_session() as conn:
            conn.execute("INSERT INTO stream_events (team_id, provider, event_type, details) VALUES ('t', 'A', 'failover', 'x')")
            conn.commit()
        row = telemetry.leaderboard_sync()[0]
        self.assertEqual((row["provider"], row["rate"], row["total"], row["failovers"]), ("A", 50.0, 2, 1))


class FailoverAttributionTests(unittest.IsolatedAsyncioTestCase):
    async def test_failover_event_is_charged_to_the_failing_provider_and_names_the_successor(self):
        a = {"provider": "A", "url": "https://a.example/l.m3u8", "referer": "", "origin": ""}
        b = {"provider": "B", "url": "https://b.example/l.m3u8", "referer": "", "origin": ""}
        backup = dict(state.stream_state)
        state.stream_state.clear()
        state.stream_state["t"] = {"name": "Team", "candidates": [a, b], "active_index": 0}
        try:
            with patch.object(failover, "send_alert", new=AsyncMock()), \
                    patch.object(failover, "_spawn_background_task", side_effect=lambda c, n: c.close()), \
                    patch.object(failover, "log_metric_event_async", new=AsyncMock()) as metric:
                moved = await failover.request_failover("t", sessions.candidate_source_key(a), "segments failing")
        finally:
            last = state.stream_state["t"].get("last_failover")
            state.stream_state.clear()
            state.stream_state.update(backup)
        self.assertTrue(moved)
        metric.assert_awaited_once_with("t", "A", "failover", "segments failing (to B)")
        self.assertEqual((last["from"], last["to"]), ("A", "B"))


class AlertTests(TempDbCase):
    def setUp(self):
        super().setUp()
        provider_alerts.reset_for_tests()

    def _seed_silent(self, provider="Quiet", n=6):
        for _ in range(n):
            telemetry.record_outcome_sync(provider, 5, "empty", None, 0, 0)

    def test_silent_provider_is_detected_only_with_enough_zero_event_samples(self):
        self._seed_silent(n=4)
        self.assertEqual(telemetry.silent_providers_sync(), [])
        self._seed_silent(n=2)
        self.assertEqual([p["provider"] for p in telemetry.silent_providers_sync()], ["Quiet"])

    def test_a_provider_that_lists_events_is_not_silent(self):
        for _ in range(6):
            telemetry.record_outcome_sync("Fine", 5, "empty", None, 30, 0)
        self.assertEqual(telemetry.silent_providers_sync(), [])

    def test_a_provider_with_a_success_is_not_silent(self):
        self._seed_silent(n=6)
        telemetry.record_outcome_sync("Quiet", 5, "ok", None, 0, 1)
        self.assertEqual(telemetry.silent_providers_sync(), [])

    def test_alert_is_sent_once_per_cooldown_then_again_after_it(self):
        self._seed_silent()
        sent = AsyncMock()
        with patch("alerts.send_alert", sent):
            kinds = asyncio.run(provider_alerts.check_and_alert("Quiet", False, now=1000.0))
            again = asyncio.run(provider_alerts.check_and_alert("Quiet", False, now=1100.0))
            later = asyncio.run(provider_alerts.check_and_alert("Quiet", False, now=1000.0 + provider_alerts.ALERT_COOLDOWN + 1))
        self.assertEqual((kinds, again, later), (["silent"], [], ["silent"]))
        self.assertEqual(sent.await_count, 2)
        self.assertNotIn("http", sent.await_args.args[1])

    def test_open_breaker_alerts_and_healthy_provider_does_not(self):
        sent = AsyncMock()
        with patch("alerts.send_alert", sent):
            self.assertEqual(asyncio.run(provider_alerts.check_and_alert("P", False, now=1.0)), [])
            self.assertEqual(asyncio.run(provider_alerts.check_and_alert("P", True, now=2.0)), ["breaker"])

    def test_dead_for_five_days_alerts(self):
        for _ in range(10):
            telemetry.record_outcome_sync("Dead", 5, "error", "connect")
        sent = AsyncMock()
        with patch("alerts.send_alert", sent):
            self.assertEqual(asyncio.run(provider_alerts.check_and_alert("Dead", False, now=1.0)), ["dead"])

    def test_check_soon_is_rate_limited(self):
        calls = []
        with patch("state._spawn_background_task", side_effect=lambda c, n: (calls.append(n), c.close())):
            async def go():
                provider_alerts.check_soon("P", False)
                provider_alerts.check_soon("P", False)
            asyncio.run(go())
        self.assertEqual(len(calls), 1)


class ProviderSettingsTests(TempDbCase):
    def test_defaults_change_nothing(self):
        providers = [FakeProvider("A"), FakeProvider("B")]
        self.assertEqual(provider_settings.filter_enabled(providers), providers)
        base = {"A": 1}
        self.assertIs(provider_settings.apply_priority(base), base)

    def test_disable_and_priority_persist_and_reload(self):
        provider_settings.set_provider_sync("A", False, 5)
        provider_settings._SETTINGS.clear()
        provider_settings.load()
        self.assertFalse(provider_settings.is_enabled("A"))
        self.assertEqual(provider_settings.apply_priority({"A": 1, "B": 2}), {"A": 5, "B": 2})
        self.assertEqual([p.name for p in provider_settings.filter_enabled([FakeProvider("A"), FakeProvider("B")])], ["B"])

    def test_search_skips_disabled_providers(self):
        provider_settings.set_provider_sync(scrapers.ACTIVE_PROVIDERS[0].name, False, None)
        names = [p.name for p in scrapers._providers_for_search()]
        self.assertNotIn(scrapers.ACTIVE_PROVIDERS[0].name, names)
        self.assertEqual(len(names), len(scrapers.ACTIVE_PROVIDERS) - 1)

    def test_priority_override_reorders_streams_with_equal_match_scores(self):
        from stream_extractor import rank_streams
        provider_settings.set_provider_sync("B", True, 0)
        streams = [_stream("A"), _stream("B")]
        ranked = rank_streams(streams, provider_settings.apply_priority({"A": 1, "B": 2}))
        self.assertEqual(ranked[0]["provider"], "B")


class DryRunTests(unittest.IsolatedAsyncioTestCase):
    async def test_dry_run_reports_events_streams_and_error_class_without_recording(self):
        ok = await provider_tools.dry_run(FakeProvider("P", [_stream("P")], index_events=25), ["x"])
        self.assertEqual((ok["outcome"], ok["index_events"], ok["streams"], ok["error_class"]), ("ok", 25, 1, None))
        bad = await provider_tools.dry_run(FakeProvider("P", exc=httpx.ConnectError("x")), ["x"])
        self.assertEqual((bad["outcome"], bad["error_class"]), ("error", "connect"))
        self.assertNotIn("x.test", str(bad))

    async def test_dry_run_times_out(self):
        class Slow(FakeProvider):
            async def search(self, *a, **k):
                await asyncio.sleep(5)

        result = await provider_tools.dry_run(Slow("S"), ["x"], timeout=0.05)
        self.assertEqual((result["outcome"], result["error_class"]), ("timeout", "timeout"))

    async def test_concurrent_dry_runs_of_one_provider_are_refused(self):
        gate = asyncio.Event()

        class Gated(FakeProvider):
            async def search(self, *a, **k):
                await gate.wait()
                return []

        task = asyncio.create_task(provider_tools.dry_run(Gated("G"), ["x"]))
        await asyncio.sleep(0)
        with self.assertRaises(provider_tools.ProviderBusy):
            await provider_tools.dry_run(Gated("G"), ["x"])
        gate.set()
        await task

    def test_terms_are_bounded(self):
        self.assertEqual(provider_tools.clean_terms(None), ["ESPN"])
        self.assertEqual(len(provider_tools.clean_terms([str(i) for i in range(20)])), provider_tools.MAX_TERMS)
        self.assertEqual(len(provider_tools.clean_terms(["x" * 500])[0]), provider_tools.MAX_TERM_LENGTH)


class ProbeTests(unittest.IsolatedAsyncioTestCase):
    async def test_probe_really_fetches_candidates_and_reports_live_count(self):
        results = {"https://live.example/a.m3u8": True, "https://dead.example/a.m3u8": False}

        async def fake_verify(client, url, referer="", timeout=5.0, origin="", **kw):
            return results[url]

        candidates = [{"url": u} for u in results]
        with patch.object(provider_tools, "verify_stream_live", fake_verify), patch.object(state, "SHARED_HTTP_CLIENT", None):
            self.assertEqual(await provider_tools.probe_candidates(candidates), {"probed": 2, "live": 1})
            self.assertEqual(await provider_tools.probe_candidates([candidates[1]]), {"probed": 1, "live": 0})
            self.assertEqual(await provider_tools.probe_candidates([]), {"probed": 0, "live": 0})


class RoutesTests(TempDbCase):
    def _call(self, method, path, **kwargs):
        async def go():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=main.app), base_url="http://127.0.0.1:8000"
            ) as client:
                return await client.request(method, path, **kwargs)

        with patch.object(security, "DASHBOARD_PASSWORD", ""):
            return asyncio.run(go())

    def test_provider_list_has_status_fields(self):
        telemetry.record_outcome_sync("TheTVApp", 5, "ok", None, 12, 1)
        data = self._call("GET", "/api/providers").json()["providers"]
        row = next(p for p in data if p["name"] == "TheTVApp")
        self.assertEqual((row["enabled"], row["breaker_open"], row["index_events"]), (True, False, 12))
        self.assertIsNotNone(row["last_success"])

    def test_settings_endpoint_validates_and_saves(self):
        name = scrapers.ACTIVE_PROVIDERS[0].name
        bad = self._call("POST", f"/api/providers/{name}/settings", json={"enabled": True, "priority": -3})
        self.assertEqual(bad.status_code, 422)
        self.assertEqual(self._call("POST", "/api/providers/Nope/settings", json={"enabled": True}).status_code, 404)
        ok = self._call("POST", f"/api/providers/{name}/settings", json={"enabled": False, "priority": 2})
        self.assertEqual(ok.status_code, 200)
        self.assertFalse(provider_settings.is_enabled(name))

    def test_test_endpoint_runs_the_dry_run(self):
        name = scrapers.ACTIVE_PROVIDERS[0].name
        fake = {"provider": name, "outcome": "ok", "streams": 1}
        with patch.object(scrapers, "get_healthy_browser", AsyncMock(return_value=None)), \
                patch.object(provider_tools, "dry_run", AsyncMock(return_value=fake)) as dry:
            response = self._call("POST", f"/api/providers/{name}/test", json={"terms": ["Bills"]})
        self.assertEqual(response.json(), fake)
        self.assertEqual(dry.await_args.args[1], ["Bills"])
        self.assertEqual(self._call("POST", "/api/providers/Nope/test").status_code, 404)

    def test_test_endpoint_reports_busy_as_429(self):
        name = scrapers.ACTIVE_PROVIDERS[0].name
        with patch.object(scrapers, "get_healthy_browser", AsyncMock(return_value=None)), \
                patch.object(provider_tools, "dry_run", AsyncMock(side_effect=provider_tools.ProviderBusy(name))):
            self.assertEqual(self._call("POST", f"/api/providers/{name}/test").status_code, 429)

    def test_test_stream_probes_instead_of_echoing_the_in_memory_flag(self):
        backup = dict(state.stream_state)
        state.stream_state["probe_t"] = {
            "is_healthy": True,  # the in-memory flag says healthy...
            "candidates": [{"url": "https://dead.example/a.m3u8", "referer": "", "origin": ""}],
        }
        try:
            with patch.object(provider_tools, "verify_stream_live", AsyncMock(return_value=False)):
                body = self._call("POST", "/api/test-stream/probe_t").json()
        finally:
            state.stream_state.clear()
            state.stream_state.update(backup)
        self.assertFalse(body["is_live"])  # ...but the probe found nothing
        self.assertTrue(body["reported_healthy"])
        self.assertEqual((body["probed"], body["live_candidates"]), (1, 0))

    def test_card_is_registered_on_the_performance_tab_with_its_script(self):
        import dashboard_cards
        card = next(c for c in dashboard_cards.cards_by_tab()["performance"] if c.name == "provider_status")
        self.assertEqual(card.scripts, ("/static/js/providers.js",))


class DashboardPollingTests(unittest.TestCase):
    def test_performance_polling_does_not_depend_on_the_landing_tab(self):
        source = open(os.path.join(os.path.dirname(__file__), "static", "dashboard.js"), encoding="utf-8").read()
        self.assertNotIn("if (activeTab === 'performance')", source)
        self.assertIn("performancePaneActive()", source)
        self.assertIn("window.refreshPerformanceTab", source)


class MirrorDomainTests(TempDbCase):
    """Automatic provider domain failover: mirror probing, suggestions,
    auto-switch, and breaker reset."""

    def setUp(self):
        super().setUp()
        scrapers._PROVIDER_SUGGESTED_DOMAINS.clear()
        scrapers._MIRROR_PROBE_INFLIGHT.clear()
        scrapers._PROVIDER_BREAKERS.clear()

    def tearDown(self):
        scrapers._PROVIDER_SUGGESTED_DOMAINS.clear()
        scrapers._MIRROR_PROBE_INFLIGHT.clear()
        scrapers._PROVIDER_BREAKERS.clear()
        super().tearDown()

    def _run_probe(self, name, mirrors, html_by_mirror):
        async def fake_fetch(client, url, headers=None, timeout=None):
            return html_by_mirror.get(url)

        async def go():
            # The probe builds a real httpx.AsyncClient, which the sandbox's
            # proxy env breaks; the fetch itself is stubbed, so stub the
            # client too.
            with patch.dict(scrapers.PROVIDER_MIRROR_DOMAINS, {name: mirrors}), \
                    patch.object(scrapers, "fetch_bounded_text", side_effect=fake_fetch), \
                    patch("httpx.AsyncClient", return_value=AsyncMock()):
                await scrapers._probe_provider_mirrors(name)

        asyncio.run(go())

    def test_working_mirror_is_recorded_as_suggestion(self):
        name = "MethStreams"
        big_html = "<html><body>" + "<a href='/game/1'>Team A vs Team B</a>" * 40 + "</body></html>"
        self._run_probe(
            name,
            ["https://methstreams-dead.example", "https://methstreams-alive.example"],
            {"https://methstreams-alive.example": big_html},
        )
        self.assertEqual(
            scrapers.get_suggested_domain(name), "https://methstreams-alive.example"
        )

    def test_probe_with_no_working_mirror_records_nothing(self):
        name = "MethStreams"
        self._run_probe(name, ["https://methstreams-dead.example"], {})
        self.assertIsNone(scrapers.get_suggested_domain(name))

    def test_probe_skips_the_current_base_url(self):
        name = "MethStreams"
        provider = next(p for p in scrapers.ACTIVE_PROVIDERS if p.name == name)
        current = provider.base_url.rstrip("/")
        big_html = "<html><body>" + "<a href='/game/1'>x</a>" * 40 + "</body></html>"
        self._run_probe(name, [current], {current: big_html})
        # The current URL is never suggested, even when it serves HTML.
        self.assertIsNone(scrapers.get_suggested_domain(name))

    def test_autoswitch_applies_the_mirror_and_clears_the_breaker(self):
        name = "MethStreams"
        provider = next(p for p in scrapers.ACTIVE_PROVIDERS if p.name == name)
        original_override = scrapers._PROVIDER_BASE_URL_OVERRIDES.get(name)
        big_html = "<html><body>" + "<a href='/game/1'>x</a>" * 40 + "</body></html>"
        for _ in range(scrapers.PROVIDER_BREAKER_FAILURES):
            scrapers._provider_breaker_failure(name)
        self.assertTrue(scrapers._provider_breaker_open(name))
        try:
            with patch.object(scrapers, "PROVIDER_DOMAIN_AUTOSWITCH", True), \
                    patch("alerts.send_alert", new=AsyncMock()):
                self._run_probe(
                    name,
                    ["https://methstreams-alive.example"],
                    {"https://methstreams-alive.example": big_html},
                )
            self.assertEqual(provider.base_url, "https://methstreams-alive.example")
            self.assertFalse(scrapers._provider_breaker_open(name))
            self.assertIsNone(scrapers.get_suggested_domain(name))
        finally:
            if original_override:
                scrapers._PROVIDER_BASE_URL_OVERRIDES[name] = original_override
            else:
                scrapers._PROVIDER_BASE_URL_OVERRIDES.pop(name, None)

    def test_reset_provider_breaker_clears_an_open_breaker(self):
        name = "MethStreams"
        for _ in range(scrapers.PROVIDER_BREAKER_FAILURES):
            scrapers._provider_breaker_failure(name)
        self.assertTrue(scrapers._provider_breaker_open(name))
        scrapers.reset_provider_breaker(name)
        self.assertFalse(scrapers._provider_breaker_open(name))

    def test_successful_search_clears_a_pending_suggestion(self):
        name = "MethStreams"
        scrapers._PROVIDER_SUGGESTED_DOMAINS[name] = "https://methstreams-alive.example"
        scrapers._provider_breaker_success(name)
        self.assertIsNone(scrapers.get_suggested_domain(name))

    def test_dismiss_suggested_domain(self):
        name = "MethStreams"
        scrapers._PROVIDER_SUGGESTED_DOMAINS[name] = "https://methstreams-alive.example"
        scrapers.dismiss_suggested_domain(name)
        self.assertIsNone(scrapers.get_suggested_domain(name))

    def test_maybe_probe_spawns_one_background_probe(self):
        name = "MethStreams"

        async def go():
            with patch.dict(scrapers.PROVIDER_MIRROR_DOMAINS, {name: ["https://m.example"]}), \
                    patch.object(scrapers, "_probe_provider_mirrors", new=AsyncMock()) as probe:
                scrapers._maybe_probe_provider_mirrors(name)
                scrapers._maybe_probe_provider_mirrors(name)  # duplicate suppressed
                for _ in range(10):
                    await asyncio.sleep(0)
                self.assertEqual(probe.await_count, 1)

        asyncio.run(go())
        # A provider with no mirrors configured never probes.
        with patch.object(scrapers, "_probe_provider_mirrors", new=AsyncMock()) as probe:
            scrapers._maybe_probe_provider_mirrors("NoMirrorsProv")
            self.assertNotIn("NoMirrorsProv", scrapers._MIRROR_PROBE_INFLIGHT)
            self.assertEqual(probe.await_count, 0)

    def test_mirror_env_parsing_ignores_garbage(self):
        with patch.dict(os.environ, {"PROVIDER_MIRROR_DOMAINS": "not json"}):
            self.assertEqual(scrapers._load_provider_mirror_domains(), {})
        with patch.dict(os.environ, {"PROVIDER_MIRROR_DOMAINS": '{"A": ["notaurl", "https://ok.example/x/"]}'}):
            self.assertEqual(
                scrapers._load_provider_mirror_domains(), {"A": ["https://ok.example"]}
            )


class RetryRouteTests(TempDbCase):
    def _call(self, method, path, **kwargs):
        async def go():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=main.app), base_url="http://127.0.0.1:8000"
            ) as client:
                return await client.request(method, path, **kwargs)

        with patch.object(security, "DASHBOARD_PASSWORD", ""):
            return asyncio.run(go())

    def test_retry_endpoint_clears_the_breaker_and_runs_a_dry_run(self):
        name = scrapers.ACTIVE_PROVIDERS[0].name
        for _ in range(scrapers.PROVIDER_BREAKER_FAILURES):
            scrapers._provider_breaker_failure(name)
        self.assertTrue(scrapers._provider_breaker_open(name))
        fake = {"provider": name, "outcome": "ok", "streams": 2}
        with patch.object(scrapers, "get_healthy_browser", AsyncMock(return_value=None)), \
                patch.object(provider_tools, "dry_run", AsyncMock(return_value=fake)):
            response = self._call("POST", f"/api/providers/{name}/retry", json={"terms": ["Bills"]})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), fake)
        self.assertFalse(scrapers._provider_breaker_open(name))

    def test_retry_endpoint_reports_busy_as_429(self):
        name = scrapers.ACTIVE_PROVIDERS[0].name
        with patch.object(scrapers, "get_healthy_browser", AsyncMock(return_value=None)), \
                patch.object(provider_tools, "dry_run", AsyncMock(side_effect=provider_tools.ProviderBusy(name))):
            response = self._call("POST", f"/api/providers/{name}/retry")
        self.assertEqual(response.status_code, 429)

    def test_apply_domain_endpoint_validates_applies_and_dismisses(self):
        name = "MethStreams"
        provider = next(p for p in scrapers.ACTIVE_PROVIDERS if p.name == name)
        original_override = scrapers._PROVIDER_BASE_URL_OVERRIDES.get(name)
        try:
            bad = self._call("POST", f"/api/providers/{name}/apply-domain", json={"url": "notaurl"})
            self.assertEqual(bad.status_code, 400)
            ok = self._call(
                "POST", f"/api/providers/{name}/apply-domain",
                json={"url": "https://methstreams-new.example/"},
            )
            self.assertEqual(ok.status_code, 200)
            self.assertEqual(ok.json()["url"], "https://methstreams-new.example")
            self.assertEqual(provider.base_url, "https://methstreams-new.example")
            dismissed = self._call("POST", f"/api/providers/{name}/apply-domain", json={"dismiss": True})
            self.assertEqual(dismissed.json(), {"name": name, "dismissed": True})
            self.assertEqual(self._call("POST", "/api/providers/Nope/apply-domain", json={"dismiss": True}).status_code, 404)
        finally:
            if original_override:
                scrapers._PROVIDER_BASE_URL_OVERRIDES[name] = original_override
            else:
                scrapers._PROVIDER_BASE_URL_OVERRIDES.pop(name, None)

    def test_provider_list_includes_suggested_domain(self):
        name = scrapers.ACTIVE_PROVIDERS[0].name
        scrapers._PROVIDER_SUGGESTED_DOMAINS[name] = "https://suggested.example"
        try:
            rows = self._call("GET", "/api/providers").json()["providers"]
            row = next(p for p in rows if p["name"] == name)
            self.assertEqual(row["suggested_domain"], "https://suggested.example")
        finally:
            scrapers._PROVIDER_SUGGESTED_DOMAINS.pop(name, None)


class SelftestTests(TempDbCase):
    def test_degraded_providers_are_collected(self):
        async def fake_dry_run(provider, terms, browser=None, timeout=None):
            if provider.name == "BadProv":
                return {"provider": "BadProv", "outcome": "error", "error_class": "connect"}
            return {"provider": provider.name, "outcome": "ok", "error_class": None}

        providers = [MagicMock(name="GoodProv"), MagicMock(name="BadProv")]
        providers[0].name = "GoodProv"
        providers[1].name = "BadProv"
        with patch("provider_settings.filter_enabled", return_value=providers), \
                patch.object(scrapers, "get_healthy_browser", AsyncMock(return_value=None)), \
                patch.object(provider_tools, "dry_run", side_effect=fake_dry_run):
            summary = asyncio.run(provider_tools.run_provider_selftest())
        self.assertEqual(summary["checked"], 2)
        self.assertEqual(len(summary["degraded"]), 1)
        self.assertEqual(summary["degraded"][0]["provider"], "BadProv")

    def test_digest_is_sent_only_when_something_is_degraded(self):
        async def go():
            with patch("alerts.send_alert", new=AsyncMock()) as send:
                sent = await provider_tools.send_selftest_digest({"checked": 3, "degraded": []})
                self.assertFalse(sent)
                send.assert_not_awaited()
                sent = await provider_tools.send_selftest_digest(
                    {"checked": 3, "degraded": [{"provider": "BadProv", "outcome": "error", "error_class": "connect"}]}
                )
                self.assertTrue(sent)
                send.assert_awaited_once()
                title = send.await_args.args[0]
                self.assertIn("BadProv", send.await_args.args[1])
                self.assertEqual(title, "Nightly provider self-test")

        asyncio.run(go())


if __name__ == "__main__":
    unittest.main()
