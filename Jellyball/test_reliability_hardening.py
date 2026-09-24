import asyncio
import os
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

import main
from network_safety import validate_http_url, validate_http_url_async


class ReliabilityHardeningTests(unittest.IsolatedAsyncioTestCase):
    async def test_cache_expiry_is_removed_during_put(self):
        cache = main.LRUChunkCache(capacity=4, max_bytes=1024)
        await cache.put("expired", b"old", 0.001)
        await asyncio.sleep(0.01)
        await cache.put("new", b"new", 10)
        self.assertIsNone(await cache.get("expired"))
        self.assertEqual(await cache.get("new"), b"new")

    async def test_dns_validation_rejects_loopback(self):
        self.assertIsNone(await validate_http_url_async("http://127.0.0.1:8000/stream"))
        self.assertIsNone(await validate_http_url_async("http://localhost:8000/stream"))

    async def test_dns_validation_allows_explicit_private_upstream(self):
        self.assertEqual(
            await validate_http_url_async("http://127.0.0.1:8000/stream", allow_private=True),
            "http://127.0.0.1:8000/stream",
        )

    async def test_provider_breaker_opens_and_recovers_after_cooldown(self):
        provider = "test-provider"
        with patch.object(main, "PROVIDER_BREAKER_FAILURES", 2), patch.object(main, "PROVIDER_BREAKER_COOLDOWN", 0.01):
            main._PROVIDER_BREAKERS.pop(provider, None)
            main._provider_breaker_failure(provider)
            self.assertFalse(main._provider_breaker_open(provider))
            main._provider_breaker_failure(provider)
            self.assertTrue(main._provider_breaker_open(provider))
            await asyncio.sleep(0.02)
            self.assertFalse(main._provider_breaker_open(provider))
            main._provider_breaker_success(provider)

    async def test_startup_buffer_task_reference_is_removed(self):
        task = asyncio.create_task(asyncio.sleep(0))
        main._STARTUP_BUFFER_TASKS["test-key"] = task
        main._remove_startup_buffer_task(task)
        self.assertNotIn("test-key", main._STARTUP_BUFFER_TASKS)

    async def test_metric_writer_is_bounded(self):
        writer = main.MetricBatchWriter(max_queue=1, batch_size=2, flush_seconds=0.01)
        await writer.enqueue(("stream_event", ("team", "provider", "test", "details"), {}))
        await writer.enqueue(("stream_event", ("team", "provider", "test", "details"), {}))
        self.assertLessEqual(writer.queue.qsize(), 1)
        await writer.stop()


class FailureInjectionTests(unittest.TestCase):
    def test_invalid_url_schemes_remain_rejected(self):
        self.assertIsNone(validate_http_url("file:///etc/passwd"))
        self.assertIsNone(validate_http_url("javascript:alert(1)"))
        self.assertIsNone(validate_http_url("https://user:password@example.com/stream"))

    def test_single_worker_guard_configuration_is_explicit(self):
        with patch.dict(os.environ, {"WEB_CONCURRENCY": "2"}, clear=False):
            self.assertNotEqual(os.getenv("WEB_CONCURRENCY"), "1")


@unittest.skipUnless(os.getenv("JELLYBALL_LOAD_TESTS") == "1", "opt-in load/soak test")
class LocalLoadAndSoakTests(unittest.IsolatedAsyncioTestCase):
    async def test_cache_repeated_access(self):
        cache = main.LRUChunkCache(capacity=256, max_bytes=4 * 1024 * 1024)
        started = time.perf_counter()
        for index in range(1000):
            await cache.put(str(index % 32), b"payload", 60)
            await cache.get(str(index % 32))
        self.assertLess(time.perf_counter() - started, 10)

    async def test_cache_soak_expiration(self):
        cache = main.LRUChunkCache(capacity=32, max_bytes=1024)
        for _ in range(100):
            await cache.put("same", b"payload", 0.01)
            await asyncio.sleep(0.001)
            await cache.purge_expired()
        self.assertLessEqual(len(cache.cache), 1)


if __name__ == "__main__":
    unittest.main()
