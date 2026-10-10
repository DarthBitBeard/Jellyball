import asyncio
import os
import socket
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

import main
import legacy_proxy
import db
import scrapers
import network_safety
from network_safety import clear_dns_cache, validate_http_url, validate_http_url_async


class ReliabilityHardeningTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        clear_dns_cache()

    async def asyncTearDown(self):
        clear_dns_cache()

    async def test_dns_validation_rejects_loopback(self):
        self.assertIsNone(await validate_http_url_async("http://127.0.0.1:8000/stream"))
        self.assertIsNone(await validate_http_url_async("http://localhost:8000/stream"))

    async def test_dns_validation_allows_explicit_private_upstream(self):
        self.assertEqual(
            await validate_http_url_async("http://127.0.0.1:8000/stream", allow_private=True),
            "http://127.0.0.1:8000/stream",
        )

    async def test_dns_cache_hit_avoids_second_lookup(self):
        hostname = "allowed.example.test"
        calls = []

        def fake_getaddrinfo(host, port, *args, **kwargs):
            calls.append(host)
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", port))]

        with patch.object(socket, "getaddrinfo", side_effect=fake_getaddrinfo):
            first = await validate_http_url_async(f"http://{hostname}/a")
            second = await validate_http_url_async(f"http://{hostname}/b")
        self.assertEqual(first, f"http://{hostname}/a")
        self.assertEqual(second, f"http://{hostname}/b")
        self.assertEqual(len(calls), 1)

    async def test_dns_blocked_verdict_is_cached_with_short_ttl(self):
        hostname = "blocked.example.test"
        calls = []

        def fake_getaddrinfo(host, port, *args, **kwargs):
            calls.append(host)
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.1.2.3", port))]

        with patch.object(socket, "getaddrinfo", side_effect=fake_getaddrinfo):
            first = await validate_http_url_async(f"http://{hostname}/a")
            second = await validate_http_url_async(f"http://{hostname}/b")
        self.assertIsNone(first)
        self.assertIsNone(second)
        self.assertEqual(len(calls), 1)
        decision, expiry = network_safety._dns_cache[(hostname, 80, False)]
        self.assertFalse(decision)
        remaining = expiry - time.monotonic()
        self.assertGreater(remaining, 0)
        # Allow a tiny float epsilon: expiry was set as monotonic()+TTL, and
        # subtracting monotonic() again can land a few ulps over the TTL.
        self.assertLessEqual(remaining, network_safety._DNS_CACHE_BLOCKED_TTL + 1e-6)

    async def test_dns_resolution_timeout_returns_none(self):
        hostname = "slow.example.test"

        def slow_getaddrinfo(*args, **kwargs):
            time.sleep(0.2)
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 80))]

        with patch.object(socket, "getaddrinfo", side_effect=slow_getaddrinfo), patch.object(
            network_safety, "DNS_RESOLVE_TIMEOUT", 0.01
        ):
            result = await validate_http_url_async(f"http://{hostname}/stream")
        self.assertIsNone(result)

    async def test_dns_single_flight_shares_one_resolution(self):
        hostname = "shared.example.test"
        call_count = 0
        gate = asyncio.Event()

        async def fake_lookup(host, port, timeout):
            nonlocal call_count
            call_count += 1
            await gate.wait()
            return True, network_safety._DNS_CACHE_ALLOWED_TTL

        with patch.object(network_safety, "_lookup_dns_verdict", side_effect=fake_lookup):
            task1 = asyncio.create_task(validate_http_url_async(f"http://{hostname}/a"))
            task2 = asyncio.create_task(validate_http_url_async(f"http://{hostname}/b"))
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            gate.set()
            result1, result2 = await asyncio.gather(task1, task2)
        self.assertEqual(call_count, 1)
        self.assertEqual(result1, f"http://{hostname}/a")
        self.assertEqual(result2, f"http://{hostname}/b")

    async def test_literal_ip_bypasses_dns_resolution(self):
        with patch.object(socket, "getaddrinfo") as mock_getaddrinfo:
            result = await validate_http_url_async("http://93.184.216.34:8080/stream")
        self.assertEqual(result, "http://93.184.216.34:8080/stream")
        mock_getaddrinfo.assert_not_called()

    async def test_provider_breaker_opens_and_recovers_after_cooldown(self):
        provider = "test-provider"
        with patch.object(scrapers, "PROVIDER_BREAKER_FAILURES", 2), patch.object(scrapers, "PROVIDER_BREAKER_COOLDOWN", 0.01):
            scrapers._PROVIDER_BREAKERS.pop(provider, None)
            scrapers._provider_breaker_failure(provider)
            self.assertFalse(scrapers._provider_breaker_open(provider))
            scrapers._provider_breaker_failure(provider)
            self.assertTrue(scrapers._provider_breaker_open(provider))
            await asyncio.sleep(0.02)
            self.assertFalse(scrapers._provider_breaker_open(provider))
            scrapers._provider_breaker_success(provider)

    async def test_metric_writer_is_bounded(self):
        writer = db.MetricBatchWriter(max_queue=1, batch_size=2, flush_seconds=0.01)
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
        cache = legacy_proxy.LRUChunkCache(capacity=256, max_bytes=4 * 1024 * 1024)
        started = time.perf_counter()
        for index in range(1000):
            await cache.put(str(index % 32), b"payload", 60)
            await cache.get(str(index % 32))
        self.assertLess(time.perf_counter() - started, 10)

    async def test_cache_soak_expiration(self):
        cache = legacy_proxy.LRUChunkCache(capacity=32, max_bytes=1024)
        for _ in range(100):
            await cache.put("same", b"payload", 0.01)
            await asyncio.sleep(0.001)
            await cache.purge_expired()
        self.assertLessEqual(len(cache.cache), 1)


if __name__ == "__main__":
    unittest.main()
