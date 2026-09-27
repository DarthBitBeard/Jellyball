"""Legacy relay path (fMP4 / separate-audio / SAMPLE-AES sources) regressions."""

import asyncio
import unittest
from unittest.mock import patch

import httpx
from starlette.requests import Request

import main
import security
import state


def _request(path="/chunk.ts"):
    return Request({
        "type": "http", "method": "GET", "scheme": "http", "path": path, "query_string": b"",
        "headers": [(b"host", b"127.0.0.1:8000")], "server": ("127.0.0.1", 8000),
        "client": ("127.0.0.1", 50000), "root_path": "", "http_version": "1.1",
    })


class LegacyRelayTests(unittest.TestCase):
    def test_single_flight_key_released_on_unexpected_error(self):
        url = "https://cdn.example.test/live/seg1.ts"
        key = f"{url}\0\0"

        async def boom(*args, **kwargs):
            raise RuntimeError("unexpected")

        async def exercise():
            client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200)))
            try:
                with patch.object(state, "SHARED_HTTP_CLIENT", client), \
                        patch.object(main, "PREFETCH_CHUNK_COUNT", 0), \
                        patch.object(main, "_open_upstream_media", side_effect=boom):
                    with self.assertRaises(RuntimeError):
                        await main.proxy_chunk(_request(), url=url, sig=security._relay_signature(url))
            finally:
                await client.aclose()

        asyncio.run(exercise())
        self.assertNotIn(key, main._PREFETCH_IN_FLIGHT)

    def test_protocol_errors_are_retried_then_answered_502(self):
        url = "https://cdn.example.test/live/seg2.ts"

        def handler(request):
            raise httpx.RemoteProtocolError("bad frame", request=request)

        async def exercise():
            client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            try:
                with patch.object(state, "SHARED_HTTP_CLIENT", client), \
                        patch.object(main, "PREFETCH_CHUNK_COUNT", 0), \
                        patch.object(main, "validate_http_url_async", side_effect=lambda u, **k: u):
                    return await main.proxy_chunk(_request(), url=url, sig=security._relay_signature(url))
            finally:
                await client.aclose()

        response = asyncio.run(exercise())
        self.assertEqual(response.status_code, 502)
        self.assertNotIn(f"{url}\0\0", main._PREFETCH_IN_FLIGHT)

    def test_closing_response_runs_cleanup_when_client_disconnects_first(self):
        closed = []

        async def body():
            yield b"never sent"

        async def on_close():
            closed.append(True)

        async def exercise():
            response = main._ClosingStreamingResponse(body(), on_close=on_close)

            async def receive():
                return {"type": "http.disconnect"}

            async def send(message):
                raise OSError("client gone")

            scope = {"type": "http", "method": "GET", "path": "/chunk.ts", "headers": [], "asgi": {"spec_version": "2.4"}}
            try:
                await response(scope, receive, send)
            except Exception:  # OSError, possibly wrapped in an ExceptionGroup by anyio
                pass

        asyncio.run(exercise())
        self.assertEqual(closed, [True])

    def test_startup_warm_targets_the_live_edge(self):
        manifest = "#EXTM3U\n#EXT-X-TARGETDURATION:4\n" + "".join(f"#EXTINF:4,\nseg{i}.ts\n" for i in range(10))
        with patch.object(main, "PREFETCH_CHUNK_COUNT", 3):
            urls = main._startup_media_urls(manifest, "https://cdn.example.test/live/index.m3u8")
        self.assertEqual([u.rsplit("/", 1)[-1] for u in urls], ["seg7.ts", "seg8.ts", "seg9.ts"])


if __name__ == "__main__":
    unittest.main()
