"""Unit tests for network_safety.safe_get: GET with manual redirect following
and per-hop SSRF validation (the scrape-path SSRF fix).

Every hostname resolves to a public address via the getaddrinfo stub, so the
DNS check doesn't depend on real network access.
"""

import socket
import unittest
from unittest.mock import patch

import httpx

from network_safety import clear_dns_cache, safe_get


def _public_getaddrinfo(host, port, *args, **kwargs):
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", port))]


def _handler(request):
    url = str(request.url)
    if url == "https://example.com/start":
        return httpx.Response(302, headers={"location": "/middle"}, request=request)
    if url == "https://example.com/middle":
        return httpx.Response(302, headers={"location": "https://example.com/final?x=1"}, request=request)
    if url.startswith("https://example.com/final"):
        return httpx.Response(200, text="OK", request=request)
    if url == "https://example.com/evil":
        return httpx.Response(302, headers={"location": "http://169.254.169.254/latest/meta-data"}, request=request)
    if request.url.path == "/params":
        return httpx.Response(200, text=url, request=request)
    return httpx.Response(404, request=request)


class SafeGetTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        clear_dns_cache()
        self._dns_patcher = patch.object(socket, "getaddrinfo", side_effect=_public_getaddrinfo)
        self._dns_patcher.start()
        self._client = httpx.AsyncClient(transport=httpx.MockTransport(_handler))

    async def asyncTearDown(self):
        await self._client.aclose()
        self._dns_patcher.stop()
        clear_dns_cache()

    async def test_follows_a_legit_redirect_chain(self):
        resp = await safe_get(self._client, "https://example.com/start")
        self.assertIsNotNone(resp)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.text, "OK")

    async def test_blocks_a_redirect_to_a_private_address(self):
        self.assertIsNone(await safe_get(self._client, "https://example.com/evil"))

    async def test_blocks_a_direct_private_address(self):
        self.assertIsNone(await safe_get(self._client, "http://169.254.169.254/latest/meta-data"))
        self.assertIsNone(await safe_get(self._client, "http://127.0.0.1:8000/healthz"))

    async def test_applies_params_to_the_first_request_only(self):
        resp = await safe_get(self._client, "https://example.com/params", params={"a": "b"})
        self.assertIsNotNone(resp)
        self.assertIn("a=b", resp.text)

    async def test_passes_non_redirect_responses_through(self):
        resp = await safe_get(self._client, "https://example.com/nope")
        self.assertIsNotNone(resp)
        self.assertEqual(resp.status_code, 404)

    async def test_redirect_chain_is_capped(self):
        # A redirect hop beyond the budget fails closed: there is no safe
        # final URL to return, so callers see it as a fetch failure.
        self.assertIsNone(await safe_get(self._client, "https://example.com/start", max_redirects=0))


if __name__ == "__main__":
    unittest.main()
