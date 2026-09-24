"""Regression tests for the stability/performance overhaul."""
import asyncio
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

import main


class CandidateMergeTests(unittest.TestCase):
    def _candidate(self, provider, url, **extra):
        return {"provider": provider, "url": url, "referer": "", "origin": "", **extra}

    def test_healthy_active_candidate_stays_active_across_rescrape(self):
        active = self._candidate("A", "https://cdn-a.test/live/index.m3u8?token=old")
        standby = self._candidate("B", "https://cdn-b.test/live.m3u8")
        fresh = [
            self._candidate("C", "https://cdn-c.test/x.m3u8"),
            self._candidate("B", "https://cdn-b.test/live.m3u8"),
        ]
        merged, index = main._merge_stream_candidates([standby, active], 1, fresh, keep_active=True)
        self.assertIs(merged[index], active)
        self.assertEqual([c["provider"] for c in merged], ["A", "C", "B"])

    def test_rotated_token_refreshes_active_url_without_replacing_candidate(self):
        active = self._candidate("A", "https://cdn-a.test/live/index.m3u8?token=old")
        fresh = [self._candidate("A", "https://cdn-a.test/live/index.m3u8?token=new")]
        merged, index = main._merge_stream_candidates([active], 0, fresh, keep_active=True)
        self.assertEqual(len(merged), 1)
        self.assertIs(merged[index], active)
        self.assertTrue(active["url"].endswith("token=new"))

    def test_unhealthy_active_is_not_kept(self):
        active = self._candidate("A", "https://cdn-a.test/live.m3u8")
        fresh = [self._candidate("B", "https://cdn-b.test/live.m3u8")]
        merged, index = main._merge_stream_candidates([active], 0, fresh, keep_active=False)
        self.assertEqual([c["provider"] for c in merged], ["B"])
        self.assertEqual(index, 0)

    def test_standby_health_history_is_preserved(self):
        old = self._candidate("B", "https://cdn-b.test/live.m3u8?t=1", last_health_ok=False, last_health_check=123.0)
        fresh = [self._candidate("B", "https://cdn-b.test/live.m3u8?t=2")]
        merged, _ = main._merge_stream_candidates([old], 0, fresh, keep_active=False)
        self.assertFalse(merged[0]["last_health_ok"])
        self.assertEqual(merged[0]["last_health_check"], 123.0)

    def test_merge_respects_candidate_cap(self):
        fresh = [self._candidate("P", f"https://cdn.test/{i}.m3u8") for i in range(10)]
        with patch.object(main, "MAX_STREAM_CANDIDATES", 3):
            merged, _ = main._merge_stream_candidates([], 0, fresh, keep_active=False)
        self.assertEqual(len(merged), 3)


class HealthCheckOriginTests(unittest.IsolatedAsyncioTestCase):
    async def test_check_stream_health_forwards_origin(self):
        verify = AsyncMock(return_value=True)
        with patch.object(main, "verify_stream_live", verify):
            self.assertTrue(await main.check_stream_health("https://cdn.test/a.m3u8", "https://ref.test/", "https://org.test"))
        self.assertEqual(verify.await_args.kwargs.get("origin"), "https://org.test")


class DashboardPasswordTests(unittest.TestCase):
    def test_plain_password_matches(self):
        self.assertTrue(main._dashboard_password_matches("secret", "secret"))
        self.assertFalse(main._dashboard_password_matches("nope", "secret"))

    def test_non_ascii_password_does_not_raise(self):
        self.assertTrue(main._dashboard_password_matches("pässwörd", "pässwörd"))
        self.assertFalse(main._dashboard_password_matches("pässwörd", "password"))

    @unittest.skipIf(main._bcrypt is None, "bcrypt not installed")
    def test_bcrypt_hash_matches(self):
        hashed = main._bcrypt.hashpw(b"hunter2", main._bcrypt.gensalt(rounds=4)).decode()
        self.assertTrue(main._dashboard_password_matches("hunter2", hashed))
        self.assertFalse(main._dashboard_password_matches("hunter3", hashed))


class GuideRefreshDebounceTests(unittest.IsolatedAsyncioTestCase):
    async def test_unchanged_guide_does_not_schedule_refresh(self):
        state = main._JELLYFIN_REFRESH_STATE
        saved = dict(state)
        try:
            state["pending"] = None
            state["signature"] = main._guide_signature()
            main.request_jellyfin_guide_refresh_if_changed()
            self.assertIsNone(state["pending"])
        finally:
            state.update(saved)


class MetricWriterResilienceTests(unittest.IsolatedAsyncioTestCase):
    async def test_write_failure_does_not_kill_writer(self):
        writer = main.MetricBatchWriter(max_queue=10, batch_size=5, flush_seconds=0.01)
        calls = []

        def failing_then_ok(items):
            calls.append(len(items))
            if len(calls) == 1:
                raise RuntimeError("database is locked")

        with patch.object(main.MetricBatchWriter, "_write_batch_sync", staticmethod(failing_then_ok)):
            await writer.enqueue(("stream_event", ("t", "p", "e", "d"), {}))
            await asyncio.sleep(0.1)
            await writer.enqueue(("stream_event", ("t", "p", "e", "d"), {}))
            await asyncio.sleep(0.1)
            self.assertFalse(writer.task.done())
            await writer.stop(timeout=1.0)
        self.assertEqual(len(calls), 2)


if __name__ == "__main__":
    unittest.main()
