"""X2: security response headers, through the real ASGI app."""

import asyncio
import unittest
from unittest.mock import patch

import httpx

import epg
import http_hardening
import main
import routes_dashboard
import security
import state
from state import new_channel_state


async def _no_tvguide():
    return {}


def _request(method: str, path: str, **kwargs) -> httpx.Response:
    async def go():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=main.app), base_url="http://127.0.0.1:8000", follow_redirects=False,
        ) as client:
            return await client.request(method, path, **kwargs)

    with patch.object(security, "DASHBOARD_PASSWORD", ""), \
            patch.object(epg, "_fetch_tvguide_epg", _no_tvguide), \
            patch.object(routes_dashboard, "get_catalog_entries", return_value=[]):
        return asyncio.run(go())


def _directives(policy: str) -> dict:
    parsed = {}
    for part in policy.split(";"):
        tokens = part.split()
        if tokens:
            parsed[tokens[0]] = tokens[1:]
    return parsed


class SecurityHeaderTests(unittest.TestCase):
    def setUp(self):
        self._backup = dict(state.stream_state)
        state.stream_state.clear()
        state.stream_state["nfl_buf"] = new_channel_state(name="Buffalo Bills", query="Buffalo Bills", category="nfl")

    def tearDown(self):
        state.stream_state.clear()
        state.stream_state.update(self._backup)

    def test_dashboard_page_gets_the_full_set(self):
        response = _request("GET", "/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")
        self.assertEqual(response.headers["x-frame-options"], "DENY")
        policy = _directives(response.headers["content-security-policy"])
        self.assertEqual(policy["frame-ancestors"], ["'none'"])
        self.assertEqual(policy["object-src"], ["'none'"])
        self.assertEqual(policy["base-uri"], ["'self'"])
        self.assertEqual(policy["form-action"], ["'self'"])
        # Scripts only from this origin (plus the inline handlers that remain).
        self.assertEqual(policy["script-src"], ["'self'", "'unsafe-inline'"])
        self.assertEqual(policy["connect-src"], ["'self'"])

    def test_referrer_policy_keeps_the_origin_header_on_same_origin_posts(self):
        """`no-referrer` would make browsers send `Origin: null` on the dashboard's
        own form POSTs, which the CSRF check rejects: every form would break."""
        response = _request("GET", "/")
        self.assertEqual(response.headers["referrer-policy"], "same-origin")
        self.assertTrue(security._is_cross_site_write("POST", {"host": "h", "origin": "null"}, "http"))

    def test_machine_responses_get_only_the_cheap_headers(self):
        for path in ("/playlist.m3u", "/epg.xml", "/healthz", "/api/status"):
            with self.subTest(path=path):
                response = _request("GET", path)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.headers["x-content-type-options"], "nosniff")
                self.assertNotIn("content-security-policy", response.headers)
                self.assertNotIn("x-frame-options", response.headers)

    def test_error_responses_from_the_middleware_stack_carry_them_too(self):
        response = _request("POST", "/favorite/nfl_buf", headers={"origin": "http://evil.example.test"})
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")

    def test_a_header_the_route_set_is_not_overridden(self):
        headers = http_hardening.security_headers_for("/", "text/html; charset=utf-8")
        self.assertIn(b"content-security-policy", {name for name, _ in headers})

        sent = []

        async def app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": [
                (b"content-type", b"text/html"), (b"content-security-policy", b"default-src 'none'"),
            ]})
            await send({"type": "http.response.body", "body": b"x"})

        async def collect(message):
            sent.append(message)

        middleware = http_hardening.SecurityHeadersMiddleware(app)
        asyncio.run(middleware({"type": "http", "path": "/"}, None, collect))
        csp = [v for k, v in sent[0]["headers"] if k == b"content-security-policy"]
        self.assertEqual(csp, [b"default-src 'none'"])
        self.assertIn((b"x-frame-options", b"DENY"), sent[0]["headers"])

    def test_api_docs_pages_get_a_policy_that_allows_their_cdn(self):
        docs = dict(http_hardening.security_headers_for("/docs", "text/html"))
        self.assertIn(b"cdn.jsdelivr.net", docs[b"content-security-policy"])
        self.assertIn(b"frame-ancestors 'none'", docs[b"content-security-policy"])
        page = dict(http_hardening.security_headers_for("/", "text/html"))
        self.assertNotIn(b"cdn.jsdelivr.net", page[b"content-security-policy"])


if __name__ == "__main__":
    unittest.main()
