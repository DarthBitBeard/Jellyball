"""Failover/health regressions fixed for 2.0.0 (exhaustion recovery, window-close
grace, failure counters, self-retry, alert cooldown, source keys, ...)."""

import asyncio
import os
import shutil
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import main
import db
import scrapers
import state
import config
from datetime import datetime, timedelta, timezone


def _candidate(provider, url, **extra):
    return {"provider": provider, "url": url, "referer": "", "origin": "", **extra}


class StateMixin:
    def setUp(self):
        super().setUp()
        self._state_backup = dict(state.stream_state)
        state.stream_state.clear()
        self._alerts = patch.object(main, "send_alert", new=AsyncMock())
        self._alerts.start()
        self._metric = patch.object(main, "log_metric_event_async", new=AsyncMock())
        self._metric.start()

    def tearDown(self):
        self._alerts.stop()
        self._metric.stop()
        state.stream_state.clear()
        state.stream_state.update(self._state_backup)
        super().tearDown()


class ExhaustedRecoveryTests(StateMixin, unittest.IsolatedAsyncioTestCase):
    async def test_exhausted_channel_recovers_when_a_source_plays_again(self):
        data = {
            "name": "Team", "exhausted": True, "is_healthy": False, "active_index": 0,
            "candidates": [
                _candidate("A", "https://a.example/live.m3u8", last_health_ok=False),
                _candidate("B", "https://b.example/live.m3u8", last_health_ok=False, consecutive_failures=4),
            ],
        }
        state.stream_state["t"] = data

        async def health(url, referer, origin="", **kwargs):
            return url.startswith("https://b.")

        with patch.object(main, "check_stream_health", side_effect=health):
            await main._probe_exhausted_candidates("t", data)

        self.assertFalse(data["exhausted"])
        self.assertTrue(data["is_healthy"])
        self.assertEqual(data["active_index"], 1)
        self.assertEqual(data["candidates"][1]["consecutive_failures"], 0)
        self.assertFalse(data["exhausted_probe_in_flight"])

    async def test_monitor_schedules_exhausted_probes_even_when_unwatched(self):
        data = {"name": "Team", "exhausted": True, "active_index": 0,
                "candidates": [_candidate("A", "https://a.example/live.m3u8")],
                "start_time": "", "stop_time": "", "always_live": True}
        state.stream_state["t"] = data
        with patch.object(main, "_spawn_background_task") as spawn, \
                patch.object(main, "_probe_exhausted_candidates", new=lambda *a: None):
            main._failover_monitor_tick()
            main._failover_monitor_tick()  # throttled by EXHAUSTED_PROBE_INTERVAL
        self.assertEqual(spawn.call_count, 1)
        self.assertTrue(data["exhausted_probe_in_flight"])


class FailoverCountersTests(StateMixin, unittest.IsolatedAsyncioTestCase):
    async def test_new_active_candidate_starts_with_zero_failures(self):
        a = _candidate("A", "https://a.example/live.m3u8")
        b = _candidate("B", "https://b.example/live.m3u8", consecutive_failures=2, last_health_ok=True,
                       last_health_check=time.time())
        state.stream_state["t"] = {"name": "Team", "candidates": [a, b], "active_index": 0}
        moved = await main.request_failover("t", main.candidate_source_key(a), "health probe failed")
        self.assertTrue(moved)
        self.assertEqual(state.stream_state["t"]["active_index"], 1)
        self.assertEqual(b["consecutive_failures"], 0)
        self.assertEqual(state.stream_state["t"]["failover_count"], 1)

    async def test_good_standby_probe_clears_its_failure_count(self):
        standby = _candidate("B", "https://b.example/live.m3u8", consecutive_failures=3)
        with patch.object(main, "check_stream_health", new=AsyncMock(return_value=True)):
            await main._probe_standby_candidate(standby, asyncio.Semaphore(1))
        self.assertEqual(standby["consecutive_failures"], 0)
        self.assertTrue(standby["last_health_ok"])

    async def test_single_source_is_retried_before_no_signal(self):
        only = _candidate("A", "https://a.example/live.m3u8")
        data = {"name": "Team", "candidates": [only], "active_index": 0}
        state.stream_state["t"] = data
        key = main.candidate_source_key(only)
        with patch.object(main, "_handle_candidates_exhausted") as exhausted:
            for _ in range(main.SELF_RETRY_LIMIT):
                self.assertFalse(await main.request_failover("t", key, "playlist stale"))
                self.assertFalse(data.get("exhausted", False))
            await main.request_failover("t", key, "playlist stale")
        self.assertTrue(data["exhausted"])
        exhausted.assert_called_once()

    async def test_failover_alerts_are_rate_limited_per_channel(self):
        a = _candidate("A", "https://a.example/live.m3u8")
        b = _candidate("B", "https://b.example/live.m3u8")
        data = {"name": "Team", "candidates": [a, b], "active_index": 0}
        state.stream_state["t"] = data
        with patch.object(main, "_spawn_background_task") as spawn:
            for _ in range(4):
                active = data["candidates"][data["active_index"]]
                # Make the other one eligible again each round.
                for c in data["candidates"]:
                    c["last_health_check"] = 0.0
                await main.request_failover("t", main.candidate_source_key(active), "segments failing")
        alert_calls = [c for c in spawn.call_args_list if "failover alert" in c.args[1]]
        self.assertEqual(len(alert_calls), 1)
        self.assertEqual(data["suppressed_failover_alerts"], 3)
        for call in spawn.call_args_list:
            coro = call.args[0]
            if hasattr(coro, "close"):
                coro.close()

    async def test_forbidden_playlist_triggers_token_refresh(self):
        a = _candidate("A", "https://a.example/live.m3u8")
        b = _candidate("B", "https://b.example/live.m3u8")
        state.stream_state["t"] = {"name": "Team", "candidates": [a, b], "active_index": 0}
        with patch.object(main, "_trigger_scrape", new=AsyncMock()) as rescrape:
            await main.request_failover("t", main.candidate_source_key(a), "playlist forbidden")
            await asyncio.sleep(0)
        rescrape.assert_awaited_once_with("t", force=True)


class WindowCloseTests(StateMixin, unittest.TestCase):
    def test_close_needs_the_grace_period(self):
        data = {"name": "Team", "schedule_status": "no_event", "candidates": [_candidate("A", "https://a/x.m3u8")]}
        with patch.object(main, "WINDOW_CLOSE_GRACE_SECONDS", 100.0):
            self.assertFalse(main._stream_window_should_close("t", data))
            data["window_close_pending_since"] -= 101.0
            self.assertTrue(main._stream_window_should_close("t", data))

    def test_placeholder_does_not_count_as_flowing(self):
        session = SimpleNamespace(
            source=SimpleNamespace(key=("placeholder",)), is_flowing=lambda: True, is_watched=lambda: True,
        )
        self.assertFalse(main._session_playing_real_source(session))
        session.source = SimpleNamespace(key=("prov", "cdn", "/live.m3u8"))
        self.assertTrue(main._session_playing_real_source(session))


class CandidateSelectionTests(unittest.TestCase):
    def test_rebuilt_list_starts_at_healthiest_candidate(self):
        now = time.time()
        failed = _candidate("A", "https://a.example/1.m3u8", last_health_ok=False, last_health_check=now)
        good = _candidate("B", "https://b.example/1.m3u8", last_health_ok=True, last_health_check=now)
        fresh = [dict(failed), dict(good)]
        merged, index = main._merge_stream_candidates([failed, good], 0, fresh, keep_active=False)
        self.assertEqual(merged[index]["provider"], "B")

    def test_incompatible_sources_get_another_chance_after_expiry(self):
        now = time.time()
        c = {"session_compatible": False, "incompatible_at": now}
        self.assertFalse(main._candidate_session_compatible(c, now))
        self.assertTrue(main._candidate_session_compatible(c, now + main.INCOMPATIBLE_RETRY_SECONDS + 1))

    def test_token_like_path_segments_do_not_change_the_source_key(self):
        a = _candidate("P", "https://cdn.example/hls/Zx81kQ2mTyp0aLr9bnW3cDeF45/espn/index.m3u8")
        b = _candidate("P", "https://cdn.example/hls/Q9w8E7r6T5y4U3i2O1p0aSdF12/espn/index.m3u8")
        c = _candidate("P", "https://cdn.example/hls/Q9w8E7r6T5y4U3i2O1p0aSdF12/espn2/index.m3u8")
        self.assertEqual(main.candidate_source_key(a), main.candidate_source_key(b))
        self.assertNotEqual(main.candidate_source_key(b), main.candidate_source_key(c))
        # Short numeric/channel ids are identity, not tokens.
        d = _candidate("P", "https://cdn.example/live/1234/index.m3u8")
        e = _candidate("P", "https://cdn.example/live/5678/index.m3u8")
        self.assertNotEqual(main.candidate_source_key(d), main.candidate_source_key(e))


class SessionHookTests(StateMixin, unittest.TestCase):
    def test_incompatible_hook_reports_whether_an_alternative_exists(self):
        a = _candidate("A", "https://a.example/1.m3u8")
        b = _candidate("B", "https://b.example/1.m3u8")
        state.stream_state["t"] = {"name": "Team", "candidates": [a, b], "active_index": 0}
        with patch.object(main, "_spawn_background_task") as spawn:
            self.assertTrue(main._on_session_incompatible("t", main.candidate_source_key(a), "fMP4"))
            self.assertFalse(a.get("session_compatible", True))
            state.stream_state["solo"] = {"name": "Solo", "candidates": [dict(a)], "active_index": 0}
            self.assertFalse(main._on_session_incompatible("solo", main.candidate_source_key(a), "fMP4"))
        for call in spawn.call_args_list:
            call.args[0].close()

    def test_startup_placeholder_window_serves_no_signal(self):
        state.stream_state["t"] = {
            "name": "Team", "candidates": [_candidate("A", "https://a.example/1.m3u8")], "active_index": 0,
            "startup_placeholder_until": time.monotonic() + 30,
        }
        sentinel = object()
        with patch.object(main, "_placeholder_source", return_value=sentinel):
            self.assertIs(main._resolve_session_source("t"), sentinel)
            state.stream_state["t"]["startup_placeholder_until"] = 0.0
            self.assertIsNot(main._resolve_session_source("t"), sentinel)

    def test_override_clears_exhaustion(self):
        a = _candidate("A", "https://a.example/1.m3u8")
        state.stream_state["t"] = {"name": "Team", "candidates": [a], "active_index": 0, "exhausted": True}
        with patch.object(main, "_spawn_background_task") as spawn:
            asyncio.run(main.override_stream("t", candidate_index=0, auth=True))
        self.assertFalse(state.stream_state["t"]["exhausted"])
        for call in spawn.call_args_list:
            call.args[0].close()


class ProviderTimeoutTests(unittest.TestCase):
    def test_failures_do_not_ratchet_the_dynamic_timeout(self):
        tmpdir = tempfile.mkdtemp()
        try:
            with patch.object(db, "DB_FILE", os.path.join(tmpdir, "t.db")):
                db.init_db()
                for _ in range(5):
                    scrapers._track_provider_response_time_sync("Slow", 90000, False)
                scrapers._track_provider_response_time_sync("Slow", 10000, True)
                self.assertEqual(scrapers._dynamic_provider_timeout_sync("Slow"), 30.0)
                self.assertEqual(scrapers._dynamic_provider_timeout_sync("Unknown"), scrapers.PROVIDER_TIMEOUT_DEFAULT)
                closer = getattr(main, "close_all_db_connections", None)
                if closer:
                    closer()
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


class LogScrubbingTests(unittest.TestCase):
    def test_exception_detail_keeps_only_url_hosts(self):
        exc = RuntimeError("POST https://api.telegram.org/bot123:SECRET/sendMessage failed")
        detail = config._safe_exception_detail(exc)
        self.assertNotIn("SECRET", detail)
        self.assertIn("api.telegram.org", detail)


if __name__ == "__main__":
    unittest.main()


class OffSeasonGuideTests(StateMixin, unittest.TestCase):
    def test_off_season_channels_hidden_by_default_and_listed_when_enabled(self):
        state.stream_state["t"] = {"name": "Team", "query": "t", "candidates": [], "category": "nfl",
                                  "schedule_status": "off_season"}
        with patch.object(main, "SHOW_OFFSEASON_CHANNELS", False):
            self.assertFalse(main._channel_listed(state.stream_state["t"]))
        with patch.object(main, "SHOW_OFFSEASON_CHANNELS", True):
            self.assertTrue(main._channel_listed(state.stream_state["t"]))
            now = datetime.now(timezone.utc)
            blocks = main._channel_programmes("t", state.stream_state["t"], now, now + timedelta(days=1), {})
            self.assertEqual(len(blocks), 1)
            self.assertIn("Off-season", blocks[0]["title"])

    def test_guide_signature_changes_with_the_toggle(self):
        with patch.object(main, "SHOW_OFFSEASON_CHANNELS", False):
            off = main._guide_signature()
        with patch.object(main, "SHOW_OFFSEASON_CHANNELS", True):
            on = main._guide_signature()
        self.assertNotEqual(off, on)


class UpdateCheckTests(unittest.TestCase):
    def test_version_comparison(self):
        with patch.dict(main._UPDATE_STATE, {"latest": "2.1.0"}):
            with patch.object(main, "__version__", "2.0.0"):
                self.assertTrue(main._update_available())
            with patch.object(main, "__version__", "2.1.0"):
                self.assertFalse(main._update_available())
        with patch.dict(main._UPDATE_STATE, {"latest": ""}):
            self.assertFalse(main._update_available())
        self.assertGreater(main._version_tuple("v10.0.0"), main._version_tuple("9.9.9"))
