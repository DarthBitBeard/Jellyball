"""3.2.2: a web page is never a stream. MethStreams' home page (15,672 bytes)
passed verification because one 0x47 ("G") near its end counted as an MPEG-TS
packet, so channels cycled through pages and played No Signal."""

import asyncio
import socket
import unittest
from unittest.mock import patch

import httpx

from stream_extractor import verify_stream_live
from ts_normalize import TS_PACKET_SIZE, find_ts_start

PAGE = b"<!DOCTYPE html><html><head><title>Watch</title></head><body>" + b"x" * 15330 + b"Google" + b"y" * 258 + b"</body></html>"


def _public_dns(host, port, *args, **kwargs):
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", port))]


def _ts(packets=4):
    return b"".join(b"\x47" + bytes(TS_PACKET_SIZE - 1) for _ in range(packets))


class TsStartTests(unittest.TestCase):
    def test_a_lone_sync_byte_near_the_end_is_not_ts(self):
        self.assertEqual(find_ts_start(PAGE), -1)

    def test_real_ts_and_disguised_ts_still_found(self):
        self.assertEqual(find_ts_start(_ts()), 0)
        self.assertEqual(find_ts_start(b"\x89PNG\r\n\x1a\n" + b"\x00" * 50 + _ts()), 58)
        self.assertEqual(find_ts_start(_ts(2)), 0)  # a two-packet sample is still TS


class VerifyTests(unittest.TestCase):
    def verify(self, body, content_type):
        def handler(request):
            return httpx.Response(200, content=body, headers={"content-type": content_type})

        async def run():
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                reject = {}
                ok = await verify_stream_live(client, "https://cdn.example.test/live/stream", reject=reject)
                return ok, reject.get("reason", "")

        with patch("socket.getaddrinfo", _public_dns):
            return asyncio.run(run())

    def test_html_page_is_rejected(self):
        ok, reason = self.verify(PAGE, "text/html; charset=UTF-8")
        self.assertFalse(ok)
        self.assertIn("not_media", reason)

    def test_page_with_a_misleading_type_is_rejected(self):
        self.assertFalse(self.verify(b"\n  " + PAGE, "application/octet-stream")[0])

    def test_ts_media_still_passes(self):
        self.assertTrue(self.verify(_ts(), "video/mp2t")[0])
        self.assertTrue(self.verify(_ts(), "image/png")[0])  # disguised segment


if __name__ == "__main__":
    unittest.main()
