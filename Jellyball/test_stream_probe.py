"""Unit tests for the stream_extractor.py health probe (verify_stream_live).

Covers:
  - S11: the probe validates redirect hops and nested playlist/segment URLs
    with the DNS-aware validate_http_url_async, not the sync-only check.
  - P9: an image-prefixed MPEG-TS segment (a fake PNG/JPEG/GIF header in front
    of real TS data) is still treated as playable.
  - P10: a frozen live playlist (#EXT-X-ENDLIST, or a media sequence/last
    segment that stops advancing) fails the probe; the initial HEAD request
    is skipped for a .m3u8 URL; the segment fetch is Range-limited.
"""

import socket
import time
import unittest
from unittest.mock import patch

import httpx

from network_safety import clear_dns_cache
from stream_extractor import verify_stream_live

_PLAYLIST_CONTENT_TYPE = "application/vnd.apple.mpegurl"


def _fake_public_getaddrinfo(host, port, *args, **kwargs):
    """Default DNS stub used by these tests: every hostname resolves to a
    public address, so validate_http_url_async's DNS check doesn't depend on
    real network access."""
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", port))]


class StreamProbeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        clear_dns_cache()
        self._dns_patcher = patch.object(socket, "getaddrinfo", side_effect=_fake_public_getaddrinfo)
        self._dns_patcher.start()

    async def asyncTearDown(self):
        self._dns_patcher.stop()
        clear_dns_cache()

    # --- P10(a): ENDLIST ---------------------------------------------------

    async def test_endlist_fails_probe(self):
        def handler(request):
            if request.method == "HEAD":
                return httpx.Response(200, headers={"content-type": _PLAYLIST_CONTENT_TYPE}, request=request)
            if request.url.path.endswith(".m3u8"):
                return httpx.Response(
                    200,
                    headers={"content-type": _PLAYLIST_CONTENT_TYPE},
                    text=(
                        "#EXTM3U\n"
                        "#EXT-X-TARGETDURATION:6\n"
                        "#EXT-X-MEDIA-SEQUENCE:10\n"
                        "#EXTINF:6,\n"
                        "seg10.ts\n"
                        "#EXT-X-ENDLIST\n"
                    ),
                    request=request,
                )
            return httpx.Response(200, headers={"content-type": "video/mp2t"}, content=b"\x47" + b"\x00" * 200, request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await verify_stream_live(client, "https://media.example.test/index.m3u8")

        self.assertFalse(result)

    # --- P10(b): frozen vs advancing media sequence -------------------------

    async def test_frozen_sequence_fails_only_after_threshold(self):
        body = (
            "#EXTM3U\n"
            "#EXT-X-TARGETDURATION:6\n"
            "#EXT-X-MEDIA-SEQUENCE:5\n"
            "#EXTINF:6,\n"
            "seg5.ts\n"
        )

        def handler(request):
            if request.method == "HEAD":
                return httpx.Response(200, headers={"content-type": _PLAYLIST_CONTENT_TYPE}, request=request)
            if request.url.path.endswith(".m3u8"):
                return httpx.Response(200, headers={"content-type": _PLAYLIST_CONTENT_TYPE}, text=body, request=request)
            return httpx.Response(200, headers={"content-type": "video/mp2t"}, content=b"\x47" + b"\x00" * 200, request=request)

        # A single fake wall clock shared by every time.monotonic() call in
        # this process (both the freshness check and network_safety's DNS
        # cache use the same global clock), advanced manually between probes.
        # Anchored to the real monotonic clock (rather than e.g. 0.0) so
        # asyncio's own debug-mode task-timing instrumentation - which also
        # reads time.monotonic() - doesn't see a bogus multi-year jump.
        base = time.monotonic()
        fake_now = {"t": base}

        def fake_monotonic():
            return fake_now["t"]

        probe_state = {}
        with patch("stream_extractor.time.monotonic", side_effect=fake_monotonic):
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                first = await verify_stream_live(
                    client, "https://media.example.test/index.m3u8", probe_state=probe_state
                )
                fake_now["t"] = base + 10.0  # unchanged, but under max(3*6, 20)=20s threshold
                second = await verify_stream_live(
                    client, "https://media.example.test/index.m3u8", probe_state=probe_state
                )
                fake_now["t"] = base + 25.0  # unchanged, and now past the 20s threshold
                third = await verify_stream_live(
                    client, "https://media.example.test/index.m3u8", probe_state=probe_state
                )

        self.assertTrue(first)
        self.assertTrue(second)
        self.assertFalse(third)

    async def test_advancing_sequence_passes(self):
        bodies = [
            "#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:10\n#EXTINF:6,\nseg10.ts\n",
            "#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:11\n#EXTINF:6,\nseg11.ts\n",
            "#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:12\n#EXTINF:6,\nseg12.ts\n",
        ]
        calls = {"n": 0}

        def handler(request):
            if request.method == "HEAD":
                return httpx.Response(200, headers={"content-type": _PLAYLIST_CONTENT_TYPE}, request=request)
            if request.url.path.endswith(".m3u8"):
                body = bodies[min(calls["n"], len(bodies) - 1)]
                calls["n"] += 1
                return httpx.Response(200, headers={"content-type": _PLAYLIST_CONTENT_TYPE}, text=body, request=request)
            return httpx.Response(200, headers={"content-type": "video/mp2t"}, content=b"\x47" + b"\x00" * 200, request=request)

        probe_state = {}
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            results = [
                await verify_stream_live(client, "https://media.example.test/index.m3u8", probe_state=probe_state)
                for _ in range(3)
            ]

        self.assertTrue(all(results))
        self.assertEqual(probe_state["media_sequence"], 12)
        self.assertEqual(probe_state["last_segment_uri"], "seg12.ts")

    # --- P9: image-prefixed TS ----------------------------------------------

    async def test_image_prefixed_ts_segment_passes(self):
        png_header = b"\x89PNG\r\n\x1a\n"
        ts_packet = b"\x47" + b"\x00" * 187
        segment_body = png_header + ts_packet * 4  # 3+ aligned TS packets after the fake header

        def handler(request):
            if request.method == "HEAD":
                return httpx.Response(200, headers={"content-type": _PLAYLIST_CONTENT_TYPE}, request=request)
            if request.url.path.endswith(".m3u8"):
                return httpx.Response(
                    200,
                    headers={"content-type": _PLAYLIST_CONTENT_TYPE},
                    text="#EXTM3U\n#EXTINF:6,\nsegment.ts\n",
                    request=request,
                )
            # A provider disguising the real TS payload as a PNG.
            return httpx.Response(200, headers={"content-type": "image/png"}, content=segment_body, request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await verify_stream_live(client, "https://media.example.test/index.m3u8")

        self.assertTrue(result)

    # --- P10(c): HEAD skipped for .m3u8 --------------------------------------

    async def test_head_skipped_for_m3u8_url(self):
        head_calls = []

        def handler(request):
            if request.method == "HEAD":
                head_calls.append(str(request.url))
                return httpx.Response(200, headers={"content-type": _PLAYLIST_CONTENT_TYPE}, request=request)
            if request.url.path.endswith(".m3u8"):
                return httpx.Response(
                    200,
                    headers={"content-type": _PLAYLIST_CONTENT_TYPE},
                    text="#EXTM3U\n#EXTINF:6,\nsegment.ts\n",
                    request=request,
                )
            return httpx.Response(200, headers={"content-type": "video/mp2t"}, content=b"\x47" + b"\x00" * 200, request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await verify_stream_live(client, "https://media.example.test/index.m3u8")

        self.assertTrue(result)
        self.assertEqual(head_calls, [])

    # --- P10(d): Range-limited segment fetch --------------------------------

    async def test_segment_request_sends_range_header(self):
        observed_ranges = []

        def handler(request):
            if request.method == "HEAD":
                return httpx.Response(200, headers={"content-type": _PLAYLIST_CONTENT_TYPE}, request=request)
            if request.url.path.endswith(".m3u8"):
                return httpx.Response(
                    200,
                    headers={"content-type": _PLAYLIST_CONTENT_TYPE},
                    text="#EXTM3U\n#EXTINF:6,\nsegment.ts\n",
                    request=request,
                )
            observed_ranges.append(request.headers.get("range"))
            return httpx.Response(206, headers={"content-type": "video/mp2t"}, content=b"\x47" + b"\x00" * 200, request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await verify_stream_live(client, "https://media.example.test/index.m3u8")

        self.assertTrue(result)
        self.assertTrue(observed_ranges)
        self.assertEqual(observed_ranges[0], "bytes=0-65535")

    # --- S11: DNS-aware redirect validation ----------------------------------

    async def test_redirect_to_private_host_is_rejected(self):
        """internal.example.test doesn't *look* private by hostname (it has no
        .local/.internal/.home.arpa suffix), so the old sync validate_http_url
        would have let a redirect there through. The async validator resolves
        it (mocked here, like network_safety's own DNS tests) to a private
        address and must reject it.
        """

        def fake_getaddrinfo(host, port, *args, **kwargs):
            if host == "internal.example.test":
                return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.1.2.3", port))]
            return _fake_public_getaddrinfo(host, port, *args, **kwargs)

        def handler(request):
            if request.method == "HEAD":
                return httpx.Response(200, headers={"content-type": _PLAYLIST_CONTENT_TYPE}, request=request)
            if request.url.path.endswith(".m3u8"):
                return httpx.Response(
                    200,
                    headers={"content-type": _PLAYLIST_CONTENT_TYPE},
                    text="#EXTM3U\n#EXTINF:6,\nsegment.ts\n",
                    request=request,
                )
            if request.url.path == "/segment.ts":
                return httpx.Response(
                    302,
                    headers={"location": "http://internal.example.test/real-segment.ts"},
                    request=request,
                )
            # Should never be reached: the redirect target must be rejected
            # before a request to it is ever made.
            return httpx.Response(200, headers={"content-type": "video/mp2t"}, content=b"\x47" + b"\x00" * 200, request=request)

        with patch.object(socket, "getaddrinfo", side_effect=fake_getaddrinfo):
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                result = await verify_stream_live(client, "https://media.example.test/index.m3u8")

        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
