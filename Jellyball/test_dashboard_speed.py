"""Regression tests for issues 49 and 52: GET / took 640 s with ~150 channels.

Two causes, both proven by measurement before they were fixed:
  1. provider_stats_sync() used a correlated MAX(id) subquery that is quadratic
     in provider_performance rows (3k rows 0.4 s, 10k 5 s, 30k 60 s).
  2. Scrape page parsing ran through asyncio.to_thread, i.e. in the default
     executor that also serves the dashboard's database reads, so hundreds of
     queued parse jobs made every dashboard read wait minutes.
"""

import asyncio
import inspect
import logging
import random
import sys
import threading
import time
import unittest
from unittest.mock import patch

import httpx

import cpu_pool
import db
import epg
import loop_monitor
import main
import provider_telemetry
import read_cache
import security
import state
from state import new_channel_state

PROVIDERS = ["prov-a.example.test", "prov-b.example.test", "prov-c.example.test", "prov-d.example.test"]


def _populate(rows: int) -> None:
    db.init_db()
    conn = db._connect_db()
    rnd = random.Random(7)
    conn.execute("DELETE FROM provider_performance")
    conn.executemany(
        "INSERT INTO provider_performance (timestamp, provider, response_time_ms, success, outcome, index_events, matches) "
        "VALUES (datetime('now', ?), ?, 100, 1, ?, ?, 0)",
        [
            (f"-{rnd.randint(0, 6 * 3600)} seconds", PROVIDERS[i % len(PROVIDERS)],
             rnd.choice(["ok", "empty", "timeout"]), rnd.choice([None, 0, 12]))
            for i in range(rows)
        ],
    )
    conn.commit()


def _cpu_job(seconds: float) -> int:
    end = time.perf_counter() + seconds
    total = 0
    while time.perf_counter() < end:
        total += sum(range(2000))
    return total


async def _no_tvguide():
    return {}


class ProviderStatsQueryTests(unittest.TestCase):
    def test_stats_scale_linearly_and_stay_correct(self):
        _populate(30000)  # the old query needed about a minute for this
        started = time.perf_counter()
        stats = provider_telemetry.provider_stats_sync()
        elapsed = time.perf_counter() - started
        self.assertLess(elapsed, 2.0, f"provider_stats_sync took {elapsed:.1f}s for 30k rows")
        conn = db._connect_db()
        for provider in PROVIDERS:
            last_ok = conn.execute(
                "SELECT MAX(timestamp) FROM provider_performance WHERE provider=? AND outcome='ok'", (provider,)
            ).fetchone()[0]
            self.assertEqual(stats[provider]["last_success"], last_ok)
            newest = conn.execute(
                "SELECT index_events, timestamp FROM provider_performance WHERE provider=? "
                "AND index_events IS NOT NULL ORDER BY timestamp DESC, id DESC LIMIT 1", (provider,)
            ).fetchone()
            self.assertEqual(stats[provider]["index_events"], newest[0])
            self.assertEqual(stats[provider]["index_events_at"], newest[1])
            self.assertTrue(stats[provider]["outcomes_24h"])

    def test_provider_without_successes_has_no_last_success(self):
        db.init_db()
        conn = db._connect_db()
        conn.execute("DELETE FROM provider_performance")
        conn.execute(
            "INSERT INTO provider_performance (provider, response_time_ms, success, outcome) "
            "VALUES ('only-fail.example.test', 5, 0, 'timeout')"
        )
        conn.commit()
        stats = provider_telemetry.provider_stats_sync()
        self.assertNotIn("last_success", stats["only-fail.example.test"])
        self.assertNotIn("index_events", stats["only-fail.example.test"])


class DashboardUnderScrapeLoadTests(unittest.TestCase):
    def setUp(self):
        self._backup = dict(state.stream_state)
        self._switch_interval = sys.getswitchinterval()
        cpu_pool.configure_gil()  # main.lifespan does this at startup
        state.stream_state.clear()
        read_cache.clear()
        for i in range(150):
            state.stream_state[f"team_{i}"] = new_channel_state(
                name=f"Team {i}", query=f"team {i}", category="nfl", catalog_key=f"cat_{i}",
                candidates=[{"provider": PROVIDERS[j % 4], "match_title": f"Team {i} vs {j}",
                             "url": f"https://s{j}.example.test/a.m3u8"} for j in range(i % 5)],
                is_healthy=bool(i % 3),
            )
        _populate(3000)

    def tearDown(self):
        sys.setswitchinterval(self._switch_interval)
        state.stream_state.clear()
        state.stream_state.update(self._backup)
        read_cache.clear()

    async def _get_dashboard(self):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://127.0.0.1:8000",
                                     timeout=None) as client:
            await client.get("/healthz")
            started = time.perf_counter()
            response = await client.get("/")
            return response, time.perf_counter() - started

    def _run(self, scrape_jobs):
        async def go():
            await self._get_dashboard()  # warm imports/templates: a cold first render is not what is measured
            read_cache.clear()
            flood = [asyncio.create_task(cpu_pool.run_cpu(_cpu_job, 0.4)) for _ in range(scrape_jobs)]
            await asyncio.sleep(0.2)  # let the pool fill up and start working
            response, elapsed = await self._get_dashboard()
            for task in flood:
                task.cancel()
            await asyncio.gather(*flood, return_exceptions=True)
            return response, elapsed

        with patch.object(security, "DASHBOARD_PASSWORD", ""), patch.object(epg, "_fetch_tvguide_epg", _no_tvguide):
            return asyncio.run(go())

    def test_dashboard_with_150_channels_renders_within_budget(self):
        response, elapsed = self._run(0)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Team 149", response.text)
        self.assertLess(elapsed, 5.0, f"GET / took {elapsed:.1f}s")

    def test_dashboard_is_not_queued_behind_scrape_parse_jobs(self):
        """40 queued parse jobs of 0.4 s each is 16 s of work; the dashboard's
        reads must not wait for them (they used to share a pool), and the
        pure-Python parse thread must not starve the loop of the GIL."""
        response, elapsed = self._run(40)
        self.assertEqual(response.status_code, 200)
        self.assertLess(elapsed, 1.2, f"GET / took {elapsed:.1f}s while scrape parsing was queued")

    def test_parse_pool_is_small_and_separate_from_the_default_executor(self):
        async def go():
            names = await asyncio.gather(*(cpu_pool.run_cpu(lambda: threading.current_thread().name) for _ in range(6)))
            default = await asyncio.to_thread(lambda: threading.current_thread().name)
            return set(names), default

        names, default = asyncio.run(go())
        self.assertTrue(all(n.startswith("jellyball-cpu") for n in names))
        self.assertEqual(len(names), 1)  # CPU_PARSE_WORKERS default
        self.assertFalse(default.startswith("jellyball-cpu"))

    def test_scrape_parsing_uses_the_dedicated_pool(self):
        import stream_extractor
        from plugins import sdk
        for module in (stream_extractor, sdk):
            source = inspect.getsource(module)
            self.assertNotIn("to_thread(extract_streams_from_text", source)
            self.assertNotIn("to_thread(parse_matches", source)
            self.assertNotIn("to_thread(\n            _parse_iframes", source)


class LoopMonitorTests(unittest.TestCase):
    def test_warns_and_names_the_blocking_task_when_the_loop_is_blocked(self):
        async def blocker():
            time.sleep(1.7)  # the bug being detected: synchronous work on the loop

        async def go():
            monitor = loop_monitor.LoopMonitor(warn_after=1.0)
            monitor.start()
            await asyncio.sleep(0.4)
            await asyncio.create_task(blocker(), name="scrape Team 7")
            await asyncio.sleep(0.6)
            monitor.stop()
            return monitor

        with self.assertLogs("jellyball.loop_monitor", level=logging.WARNING) as logs:
            monitor = asyncio.run(go())
        text = "\n".join(logs.output)
        self.assertGreaterEqual(monitor.warnings, 1)
        self.assertIn("Event loop blocked", text)
        self.assertIn("scrape Team 7", text)
        self.assertIn("blocker", text)

    def test_quiet_when_the_loop_keeps_up(self):
        async def go():
            monitor = loop_monitor.LoopMonitor(warn_after=1.0)
            monitor.start()
            await asyncio.sleep(1.5)
            monitor.stop()
            return monitor

        monitor = asyncio.run(go())
        self.assertEqual(monitor.warnings, 0)


class ReadCacheTests(unittest.TestCase):
    def setUp(self):
        read_cache.clear()

    def test_concurrent_callers_share_one_computation_and_results_are_reused(self):
        calls = []

        def work():
            calls.append(1)
            time.sleep(0.2)
            return {"n": len(calls)}

        async def go():
            first = await asyncio.gather(*(read_cache.cached_thread("k", work, ttl=30) for _ in range(5)))
            again = await read_cache.cached_thread("k", work, ttl=30)
            return first, again

        first, again = asyncio.run(go())
        self.assertEqual(len(calls), 1)
        self.assertTrue(all(item == {"n": 1} for item in [*first, again]))

    def test_errors_propagate_and_are_not_cached(self):
        attempts = []

        def flaky():
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError("boom")
            return "ok"

        async def go():
            with self.assertRaises(RuntimeError):
                await read_cache.cached_thread("flaky", flaky, ttl=30)
            return await read_cache.cached_thread("flaky", flaky, ttl=30)

        self.assertEqual(asyncio.run(go()), "ok")


if __name__ == "__main__":
    unittest.main()
