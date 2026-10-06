"""Tests for the 2.2.0 engine-reliability chunk: legacy playback failures
driving failover, the guarded cold-start race, Multi-View warm-up quorum,
persisted next_seq, and the new observability counters.

No network and no ffmpeg: everything is mocked at the seam.
"""
import asyncio
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

import engine_stats
import failover
import hls_session
import legacy_proxy
import multiview
import placeholder
import sessions
from hls_session import ChannelSession, SessionConfig, SessionHooks, SourceSpec


def _quiet_hooks(**overrides):
    hooks = SessionHooks(
        fetch=AsyncMock(return_value=None),
        headers_for=lambda source: {},
        resolve_source=lambda channel_id: None,
        report_failure=lambda *a: None,
        report_incompatible=lambda *a: False,
        on_media_info=lambda *a: None,
    )
    for key, value in overrides.items():
        setattr(hooks, key, value)
    return hooks


def _candidate(provider, url):
    return {"provider": provider, "url": url, "referer": "", "origin": ""}


class LegacyFailureReportingTest(unittest.TestCase):
    def setUp(self):
        legacy_proxy._LEGACY_UPSTREAM_FAILURES.clear()
        legacy_proxy._LEGACY_FAILURE_THROTTLE.clear()
        legacy_proxy._MANIFEST_TEAM.clear()
        legacy_proxy.set_legacy_failure_hook(None)
        engine_stats.reset()

    def tearDown(self):
        legacy_proxy.set_legacy_failure_hook(None)
        legacy_proxy._LEGACY_UPSTREAM_FAILURES.clear()
        legacy_proxy._LEGACY_FAILURE_THROTTLE.clear()

    def test_manifest_failures_report_after_threshold(self):
        calls = []
        legacy_proxy.set_legacy_failure_hook(lambda team_id, reason: calls.append((team_id, reason)))
        legacy_proxy._legacy_upstream_failed("t1", "http://x/m.m3u8", "legacy manifest unreachable", 2)
        self.assertEqual(calls, [])  # first failure: below threshold
        legacy_proxy._legacy_upstream_failed("t1", "http://x/m.m3u8", "legacy manifest unreachable", 2)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], "t1")

    def test_success_resets_consecutive_count(self):
        calls = []
        legacy_proxy.set_legacy_failure_hook(lambda team_id, reason: calls.append(reason))
        legacy_proxy._legacy_upstream_failed("t1", "http://x/m.m3u8", "x", 2)
        legacy_proxy._legacy_upstream_ok("t1", "http://x/m.m3u8")
        legacy_proxy._legacy_upstream_failed("t1", "http://x/m.m3u8", "x", 2)
        self.assertEqual(calls, [])  # counter was reset by the success

    def test_reports_throttled_per_source(self):
        calls = []
        legacy_proxy.set_legacy_failure_hook(lambda team_id, reason: calls.append(reason))
        for _ in range(4):  # two threshold crossings back to back
            legacy_proxy._legacy_upstream_failed("t1", "http://x/m.m3u8", "x", 2)
        self.assertEqual(len(calls), 1)  # second report inside the 30s throttle window

    def test_chunk_attribution_via_manifest_team(self):
        legacy_proxy._note_manifest_team("http://x/m.m3u8", "team-9")
        legacy_proxy._SEGMENT_TO_MANIFEST["http://x/seg1.ts"] = "http://x/m.m3u8"
        try:
            team_id, manifest_url = legacy_proxy._manifest_team_for_chunk("http://x/seg1.ts")
        finally:
            legacy_proxy._SEGMENT_TO_MANIFEST.pop("http://x/seg1.ts", None)
        self.assertEqual((team_id, manifest_url), ("team-9", "http://x/m.m3u8"))

    def test_on_legacy_proxy_failure_resolves_active_candidate(self):
        reported = []
        data = {
            "candidates": [_candidate("A", "http://a/x.m3u8"), _candidate("B", "http://b/x.m3u8")],
            "active_index": 1,
        }
        with patch.dict(sessions.stream_state, {"team-1": data}), \
             patch.object(sessions, "_on_session_failure",
                          side_effect=lambda cid, key, reason: reported.append((cid, key, reason))):
            sessions._on_legacy_proxy_failure("team-1", "legacy manifest unreachable")
        self.assertEqual(len(reported), 1)
        cid, key, reason = reported[0]
        self.assertEqual(cid, "team-1")
        self.assertEqual(reason, "legacy manifest unreachable")
        # Key matches the ACTIVE (index 1) candidate, so request_failover accepts it.
        self.assertEqual(key, sessions.candidate_source_key(data["candidates"][1]))

    def test_on_legacy_proxy_failure_no_candidates_is_noop(self):
        with patch.dict(sessions.stream_state, {"team-1": {"candidates": []}}), \
             patch.object(sessions, "_on_session_failure") as failure:
            sessions._on_legacy_proxy_failure("team-1", "x")
        failure.assert_not_called()


class ResetLegacyTest(unittest.TestCase):
    def test_reset_legacy_restarts_engine(self):
        session = ChannelSession("team-1", _quiet_hooks(), SessionConfig())
        session.state = "legacy"
        session.legacy_reason = "fmp4"
        session.legacy_since = time.monotonic()
        session._ready.set()
        with patch.object(session, "ensure_running") as ensure:
            session.reset_legacy()
        self.assertEqual(session.state, "idle")
        self.assertEqual(session.legacy_reason, "")
        self.assertFalse(session._ready.is_set())
        ensure.assert_called_once_with()

    def test_reset_legacy_noop_when_not_legacy(self):
        session = ChannelSession("team-1", _quiet_hooks(), SessionConfig())
        session.state = "live"
        with patch.object(session, "ensure_running") as ensure:
            session.reset_legacy()
        self.assertEqual(session.state, "live")
        ensure.assert_not_called()

    def test_failover_calls_reset_legacy_on_session(self):
        session = MagicMock()
        session.state = "legacy"
        data = {
            "type": "team",
            "name": "Team",
            "candidates": [
                {"provider": "A", "url": "http://a/x.m3u8", "last_health_ok": False,
                 "last_health_check": time.time()},
                {"provider": "B", "url": "http://b/x.m3u8"},
            ],
            "active_index": 0,
        }
        key = sessions.candidate_source_key(data["candidates"][0])
        with patch.object(failover, "stream_state", {"team-1": data}), \
             patch.object(failover, "SESSIONS") as mock_sessions, \
             patch.object(failover, "log_metric_event_async", new=AsyncMock()), \
             patch.object(failover, "_send_failover_alert"):
            mock_sessions.peek.return_value = session
            asyncio.run(
                failover._request_failover_unlocked("team-1", key, "playlist stale")
            )
        session.reset_legacy.assert_called_once_with()


class ColdStartGuardTest(unittest.TestCase):
    def setUp(self):
        engine_stats.reset()

    def _cold_session(self):
        session = MagicMock()
        session.is_running = False
        session.window = []
        session.state = "starting"

        async def fake_wait_ready(timeout):
            session.window = ["seg"]  # the session produced a window
            return True

        session.wait_ready = fake_wait_ready
        return session

    def test_race_exception_falls_back_to_placeholder(self):
        session = self._cold_session()
        started = []

        async def fake_serve():
            return await sessions._serve_session_playlist("team-1", "team-1/seg/")

        with patch.object(sessions, "SESSIONS") as mock_sessions, \
             patch.object(sessions, "_is_raceable_channel", return_value=True), \
             patch.object(sessions, "_race_cold_candidates", side_effect=RuntimeError("boom")), \
             patch.object(sessions, "_start_on_placeholder",
                          side_effect=lambda cid: started.append(cid) or True), \
             patch.object(sessions, "_hls_response", return_value="PLAYLIST"):
            mock_sessions.get.return_value = session
            result = asyncio.run(fake_serve())
        self.assertEqual(started, ["team-1"])
        self.assertEqual(result, "PLAYLIST")  # no 500, placeholder path taken
        stats = engine_stats.cold_race_stats()
        self.assertEqual(stats["total"], 1)
        self.assertEqual(stats["won"], 0)

    def test_race_win_recorded(self):
        session = self._cold_session()

        async def fake_serve():
            return await sessions._serve_session_playlist("team-1", "team-1/seg/")

        with patch.object(sessions, "SESSIONS") as mock_sessions, \
             patch.object(sessions, "_is_raceable_channel", return_value=True), \
             patch.object(sessions, "_race_cold_candidates", return_value=True), \
             patch.object(sessions, "_hls_response", return_value="PLAYLIST"):
            mock_sessions.get.return_value = session
            asyncio.run(fake_serve())
        stats = engine_stats.cold_race_stats()
        self.assertEqual(stats["total"], 1)
        self.assertEqual(stats["won"], 1)


class NextSeqPersistenceTest(unittest.TestCase):
    def test_seed_uses_persisted_when_wall_clock_went_backwards(self):
        with patch("db.get_setting", return_value="9999999999"):
            self.assertEqual(hls_session._load_persisted_next_seq(), 9999999999)

    def test_seed_falls_back_to_wall_clock(self):
        before = int(time.time())
        with patch("db.get_setting", return_value="0"):
            seeded = hls_session._load_persisted_next_seq()
        self.assertGreaterEqual(seeded, before)

    def test_seed_tolerates_db_errors(self):
        before = int(time.time())
        with patch("db.get_setting", side_effect=RuntimeError("db down")):
            seeded = hls_session._load_persisted_next_seq()
        self.assertGreaterEqual(seeded, before)

    def test_persist_writes_new_high_water(self):
        written = {}
        hls_session._NEXT_SEQ_HIGH_WATER = 0
        with patch("db.set_setting", side_effect=lambda k, v: written.update({k: v})):
            hls_session._persist_next_seq(12345)
        self.assertEqual(written.get("engine.next_seq_max"), "12345")
        # A lower value does not overwrite the high-water mark.
        with patch("db.set_setting", side_effect=lambda k, v: written.update({k: v})):
            hls_session._persist_next_seq(100)
        self.assertEqual(written.get("engine.next_seq_max"), "12345")


class TuneInObservabilityTest(unittest.TestCase):
    def setUp(self):
        engine_stats.reset()

    def test_engine_stats_counters(self):
        engine_stats.record_cold_race(True)
        engine_stats.record_cold_race(False)
        engine_stats.record_placeholder_fallback()
        engine_stats.record_tune_in_latency(1500.0)
        engine_stats.record_tune_in_latency(500.0)
        self.assertEqual(engine_stats.cold_race_stats(), {"total": 2, "won": 1})
        self.assertEqual(engine_stats.placeholder_fallback_total(), 1)
        latency = engine_stats.tune_in_latency_stats()
        self.assertEqual(latency["runs"], 2)
        self.assertEqual(latency["avg_ms"], 1000.0)
        self.assertEqual(latency["max_ms"], 1500.0)

    def test_placeholder_health_shape(self):
        health = placeholder.placeholder_health()
        self.assertEqual(
            set(health.keys()),
            {"running", "ready", "run_id", "consecutive_failures", "last_error",
             "cooldown_remaining_seconds"},
        )
        # Nothing running in the test env: not ready, no crash.
        self.assertFalse(health["ready"])

    def test_session_metrics_include_latency(self):
        session = ChannelSession("team-1", _quiet_hooks(), SessionConfig())
        session.tune_in_latency_ms = 1234.5
        self.assertEqual(session.metrics()["tune_in_latency_ms"], 1234.5)
        self.assertEqual(session.snapshot()["tune_in_latency_ms"], 1234.5)


class MultiviewQuorumTest(unittest.TestCase):
    def test_quorum_does_not_wait_for_slowest_member(self):
        ready_session = MagicMock()
        ready_session.wait_ready = AsyncMock(return_value=True)
        ready_session.has_audio = True
        ready_session.codec_signature = ("h264", "aac")

        async def slow_wait_ready(timeout):
            await asyncio.sleep(30)  # never ready within the test
            return False

        slow_session = MagicMock()
        slow_session.wait_ready = slow_wait_ready
        slow_session.has_audio = None
        slow_session.codec_signature = None
        placeholder_session = MagicMock()
        placeholder_session.wait_ready = AsyncMock(return_value=True)
        placeholder_session.has_audio = None

        def fake_get(session_id):
            return {"m1": ready_session, "m2": slow_session}.get(session_id, placeholder_session)

        with patch.object(multiview, "SESSIONS") as mock_sessions, \
             patch.object(multiview, "stream_state", {"m1": {}, "m2": {}}), \
             patch.object(multiview, "MULTIVIEW_MEMBER_WARM_TIMEOUT", 4.0), \
             patch.object(multiview, "MULTIVIEW_WARM_QUORUM_GRACE_SECONDS", 0.2):
            mock_sessions.get.side_effect = fake_get
            mock_sessions.peek.side_effect = fake_get
            start = time.monotonic()
            inputs = asyncio.run(
                multiview._warm_multiview_members(["m1", "m2"])
            )
            elapsed = time.monotonic() - start
        # Old behavior waited the full 4s; quorum (1 of 2 ready) proceeds after
        # ~half the timeout without spending the grace period.
        self.assertLess(elapsed, 3.0)
        self.assertEqual(len(inputs), 2)
        # The slow member got a placeholder stand-in.
        standins = [i for i in inputs if i.standin]
        self.assertEqual(len(standins), 1)
        self.assertEqual(standins[0].team_id, "m2")


if __name__ == "__main__":
    unittest.main()
