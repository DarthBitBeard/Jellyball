"""Tests for main.RateLimitMiddleware (token-bucket blast-radius control)."""

import unittest
from unittest.mock import patch

from fastapi.responses import JSONResponse

import main


async def _ok_app(scope, receive, send):
    response = JSONResponse({"ok": True})
    await response(scope, receive, send)


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class _FastRules(main.RateLimitMiddleware):
    RULES = (("/limited", 2, 1.0),)  # burst of 2, one token per second


class RateLimitMiddlewareTests(unittest.TestCase):
    def setUp(self):
        self.clock = _Clock()
        self._patcher = patch.object(main, "_monotonic", self.clock)
        self._patcher.start()
        self.addCleanup(self._patcher.stop)
        self.middleware = _FastRules(_ok_app)

    def _call(self, path="/limited", ip="1.2.3.4"):
        scope = {
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "path": path,
            "headers": [],
            "client": (ip, 12345),
        }
        messages = []

        async def receive():
            return {"type": "http.disconnect"}

        async def send(message):
            messages.append(message)

        import asyncio

        asyncio.run(self.middleware(scope, receive, send))
        status = next(m["status"] for m in messages if m["type"] == "http.response.start")
        headers = dict(next(m["headers"] for m in messages if m["type"] == "http.response.start"))
        return status, {k.decode(): v.decode() for k, v in headers.items()}

    def test_unlisted_paths_pass_through(self):
        status, _headers = self._call(path="/api/status")
        self.assertEqual(status, 200)

    def test_non_http_scopes_pass_through(self):
        import asyncio

        called = []

        async def app(scope, receive, send):
            called.append(True)

        middleware = _FastRules(app)
        scope = {"type": "websocket", "path": "/limited", "client": ("1.2.3.4", 1)}
        asyncio.run(middleware(scope, None, None))
        self.assertTrue(called)

    def test_burst_then_429_with_retry_after(self):
        self.assertEqual(self._call()[0], 200)
        self.assertEqual(self._call()[0], 200)
        status, headers = self._call()
        self.assertEqual(status, 429)
        self.assertIn("retry-after", headers)

    def test_bucket_refills_over_time(self):
        self._call()
        self._call()
        self.assertEqual(self._call()[0], 429)
        self.clock.now += 1.5  # a token and a half refills
        self.assertEqual(self._call()[0], 200)
        self.assertEqual(self._call()[0], 429)

    def test_buckets_are_per_client_ip(self):
        self._call(ip="1.2.3.4")
        self._call(ip="1.2.3.4")
        self.assertEqual(self._call(ip="1.2.3.4")[0], 429)
        # A different client still has its own full bucket.
        self.assertEqual(self._call(ip="9.9.9.9")[0], 200)

    def test_missing_client_still_limited(self):
        scope = {"type": "http", "method": "GET", "path": "/limited", "headers": []}
        messages = []

        async def receive():
            return {"type": "http.disconnect"}

        async def send(message):
            messages.append(message)

        import asyncio

        for _ in range(3):
            asyncio.run(self.middleware(scope, receive, send))
        statuses = [m["status"] for m in messages if m["type"] == "http.response.start"]
        self.assertIn(429, statuses)

    def test_real_rules_cover_the_expensive_paths(self):
        prefixes = [rule[0] for rule in main.RateLimitMiddleware.RULES]
        for expected in ("/api/test-stream", "/rescrape/", "/api/import-config", "/stream/"):
            self.assertIn(expected, prefixes)


if __name__ == "__main__":
    unittest.main()
