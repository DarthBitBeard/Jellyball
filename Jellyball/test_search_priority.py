"""3.2.1: game searches are served before 24/7 searches, and channels whose
game is another day do not search at all at startup."""

import asyncio
import unittest
from unittest.mock import patch

import failover
import scrapers
from scrapers import GAME_SEARCH_PRIORITY, LINEAR_SEARCH_PRIORITY, PrioritySemaphore


class PrioritySemaphoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_game_waiters_are_served_before_linear_waiters(self):
        sem = PrioritySemaphore(1)
        order = []
        await sem.acquire()

        async def worker(name, priority):
            async with sem.slot(priority):
                order.append(name)

        tasks = [
            asyncio.create_task(worker("linear-1", LINEAR_SEARCH_PRIORITY)),
            asyncio.create_task(worker("linear-2", LINEAR_SEARCH_PRIORITY)),
            asyncio.create_task(worker("game-1", GAME_SEARCH_PRIORITY)),
            asyncio.create_task(worker("game-2", GAME_SEARCH_PRIORITY)),
        ]
        await asyncio.sleep(0)
        sem.release()
        await asyncio.gather(*tasks)
        self.assertEqual(order, ["game-1", "game-2", "linear-1", "linear-2"])

    async def test_cancelled_waiter_does_not_leak_a_slot(self):
        sem = PrioritySemaphore(1)
        await sem.acquire()
        waiter = asyncio.create_task(sem.acquire(GAME_SEARCH_PRIORITY))
        await asyncio.sleep(0)
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        sem.release()
        await asyncio.wait_for(sem.acquire(), timeout=1)  # the slot is free again
        self.assertTrue(sem.locked())

    async def test_plain_async_with_still_works(self):
        sem = PrioritySemaphore(2)
        async with sem:
            self.assertFalse(sem.locked())
        async with sem.slot(LINEAR_SEARCH_PRIORITY), sem:
            self.assertTrue(sem.locked())
        self.assertFalse(sem.locked())

    async def test_master_scrape_uses_linear_priority_only_for_24_7_searches(self):
        seen = []

        class Recorder(PrioritySemaphore):
            def slot(self, priority=0):
                seen.append(priority)
                return super().slot(priority)

        class Provider:
            name = "Fake"

            async def search(self, terms, browser=None, http_client=None):
                return []

        async def no_browser():
            return None

        sem = Recorder(2)
        with patch.object(scrapers, "_provider_search_semaphore", lambda: sem),                 patch.object(scrapers, "get_healthy_browser", no_browser),                 patch.object(scrapers, "_providers_for_search", lambda always_live=False: [Provider()]):
            await scrapers.master_scrape("Team A", team_name="Team A", always_live=False)
            await scrapers.master_scrape("NBC", team_name="NBC", always_live=True)
        self.assertEqual(seen, [GAME_SEARCH_PRIORITY, LINEAR_SEARCH_PRIORITY])


class StartupWindowTests(unittest.TestCase):
    def test_channel_with_no_stream_outside_its_window_skips_at_once(self):
        data = {"name": "Dallas Cowboys", "schedule_status": "no_event", "candidates": []}
        with patch.object(failover, "_session_flowing", return_value=False):
            self.assertTrue(failover._stream_window_should_close("nfl_dal", data))
        self.assertNotIn("window_close_pending_since", data)

    def test_channel_with_a_stream_keeps_the_overtime_grace(self):
        data = {"name": "Dallas Cowboys", "schedule_status": "no_event",
                "candidates": [{"url": "https://a.example.test/live.m3u8"}]}
        with patch.object(failover, "_session_flowing", return_value=False):
            self.assertFalse(failover._stream_window_should_close("nfl_dal", data))
        self.assertIn("window_close_pending_since", data)


if __name__ == "__main__":
    unittest.main()
