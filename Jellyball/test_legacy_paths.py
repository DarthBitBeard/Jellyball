"""Legacy relay paths that had no direct tests: the pre-session passthrough
playlist (_legacy_proxy_stream), the signed /resource relay, and the way the
session engine is retried after a source pushed a channel onto the legacy relay.

The upstream is always an httpx.MockTransport, and the SSRF check's DNS lookup is
replaced by a pass-through, so nothing here touches the network. Clocks are fake
`time` stand-ins patched into the one module under test (never the global
`time` module, which the asyncio event loop also reads).
"""

import asyncio
import unittest
import urllib.parse
from unittest.mock import patch

import httpx
from fastapi.responses import Response
from starlette.requests import Request

import main  # noqa: F401  (the full import graph, as the running server has it)
import failover
import hls_session
import legacy_proxy
import security
import sessions
import state
import upstream
from hls_session import ChannelSession, SessionConfig, SessionHooks, SessionRegistry, SourceSpec
from state import new_channel_state


HOST = "127.0.0.1:8000"


def _request(path: str, headers=()) -> Request:
    return Request({
        "type": "http", "method": "GET", "scheme": "http", "path": path, "query_string": b"",
        "headers": [(b"host", HOST.encode()), *headers], "server": ("127.0.0.1", 8000),
        "client": ("127.0.0.1", 50000), "root_path": "", "http_version": "1.1",
    })


async def _accept_url(url, **kwargs):
    """validate_http_url_async without its DNS lookup (the URLs are fake)."""
    return url


class _FakeTime:
    """Replaces the `time` module inside one module: monotonic() and time()
    return whatever the test sets."""

    def __init__(self, monotonic: float = 1000.0, wall: float = 1_700_000_000.0) -> None:
        self.now = monotonic
        self.wall = wall

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return self.wall


def _relay_query(line: str) -> dict:
    return {key: values[0] for key, values in urllib.parse.parse_qs(urllib.parse.urlsplit(line).query).items()}


class _StreamStateTestCase(unittest.TestCase):
    def setUp(self):
        self._backup = dict(state.stream_state)
        state.stream_state.clear()

    def tearDown(self):
        state.stream_state.clear()
        state.stream_state.update(self._backup)


class LegacyProxyStreamTests(_StreamStateTestCase):
    MANIFEST_URL = "https://cdn.example.test/live/index.m3u8?token=abc"
    REFERER = "https://provider.example.test/watch/game"
    MANIFEST = "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\nseg1.ts\n#EXTINF:4,\nseg2.m4s\n"

    def _serve(self, handler, team_id: str, provider: str = ""):
        async def exercise():
            client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            try:
                with patch.object(state, "MEDIA_HTTP_CLIENT", client), \
                        patch.object(upstream, "validate_http_url_async", _accept_url), \
                        patch.object(legacy_proxy, "STREAM_STARTUP_BUFFER_SECONDS", 0):
                    return await legacy_proxy._legacy_proxy_stream(team_id, _request(f"/stream/{team_id}"), provider)
            finally:
                await client.aclose()

        return asyncio.run(exercise())

    def _manifest_handler(self, seen: list):
        def handler(request):
            seen.append(request)
            return httpx.Response(
                200, headers={"content-type": upstream.HLS_MEDIA_TYPE}, text=self.MANIFEST, request=request,
            )
        return handler

    def test_upstream_playlist_is_rewritten_to_signed_relay_urls(self):
        state.stream_state["legacy_team"] = new_channel_state(
            name="Legacy Team", query="legacy team",
            candidates=[{"provider": "Origin", "url": self.MANIFEST_URL, "referer": self.REFERER, "origin": ""}],
        )
        seen = []
        response = self._serve(self._manifest_handler(seen), "legacy_team")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.media_type, upstream.HLS_MEDIA_TYPE)
        self.assertEqual(response.headers["cache-control"], "no-cache")
        self.assertEqual(response.headers["access-control-allow-origin"], "*")
        self.assertEqual([str(r.url) for r in seen], [self.MANIFEST_URL])
        self.assertEqual(seen[0].headers["referer"], self.REFERER)

        lines = response.body.decode().splitlines()
        uris = [line for line in lines if line and not line.startswith("#")]
        self.assertEqual(len(uris), 2)
        self.assertTrue(uris[0].startswith(f"http://{HOST}/chunk.ts?"))
        self.assertTrue(uris[1].startswith(f"http://{HOST}/chunk.mp4?"))
        expected = ["https://cdn.example.test/live/seg1.ts", "https://cdn.example.test/live/seg2.m4s"]
        for uri, upstream_url in zip(uris, expected):
            query = _relay_query(uri)
            self.assertEqual(query["url"], upstream_url)
            self.assertEqual(query["ref"], self.REFERER)
            self.assertTrue(security._relay_signature_ok(query["url"], query["ref"], "", query["sig"]))
        self.assertNotIn("cdn.example.test/live/seg1.ts\n", response.body.decode())

    def test_provider_parameter_pins_that_candidate(self):
        backup_url = "https://backup.example.test/live/index.m3u8"
        state.stream_state["legacy_team"] = new_channel_state(
            name="Legacy Team", query="legacy team",
            candidates=[
                {"provider": "Origin", "url": self.MANIFEST_URL, "referer": "", "origin": ""},
                {"provider": "Backup", "url": backup_url, "referer": "", "origin": ""},
            ],
        )
        seen = []
        response = self._serve(self._manifest_handler(seen), "legacy_team", provider="backup")
        self.assertEqual(response.status_code, 200)
        self.assertEqual([str(r.url) for r in seen], [backup_url])

    def test_upstream_error_status_is_passed_through(self):
        state.stream_state["legacy_team"] = new_channel_state(
            name="Legacy Team", candidates=[{"provider": "Origin", "url": self.MANIFEST_URL}],
        )
        with patch.object(upstream, "_log_upstream_rejection"):
            response = self._serve(lambda request: httpx.Response(404, request=request), "legacy_team")
        self.assertEqual(response.status_code, 404)

    def test_channel_without_candidates_is_sent_to_no_signal(self):
        state.stream_state["empty_team"] = new_channel_state(name="Empty Team", candidates=[])
        response = self._serve(lambda request: self.fail("no upstream request expected"), "empty_team")
        self.assertEqual(response.status_code, 307)
        self.assertEqual(response.headers["location"], f"/stream/{state.PLACEHOLDER_SESSION_ID}.m3u8")

    def test_unknown_channel_is_not_found(self):
        response = self._serve(lambda request: self.fail("no upstream request expected"), "missing")
        self.assertEqual(response.status_code, 404)


class ProxyResourceTests(unittest.TestCase):
    URL = "https://cdn.example.test/keys/key1.bin"
    REFERER = "https://provider.example.test/watch/game"
    KEY = b"0123456789abcdef"

    def _call(self, handler, sig: str, headers=(), ref: str = REFERER):
        async def exercise():
            client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            try:
                with patch.object(state, "MEDIA_HTTP_CLIENT", client), \
                        patch.object(upstream, "validate_http_url_async", _accept_url):
                    return await legacy_proxy.proxy_resource(
                        _request("/resource", headers), url=self.URL, ref=ref, sig=sig,
                    )
            finally:
                await client.aclose()

        return asyncio.run(exercise())

    def test_signed_url_is_relayed_with_its_media_type(self):
        seen = []

        def handler(request):
            seen.append(request)
            return httpx.Response(
                200, headers={"content-type": "application/octet-stream; charset=binary"},
                content=self.KEY, request=request,
            )

        response = self._call(handler, security._relay_signature(self.URL, self.REFERER))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.body, self.KEY)
        self.assertEqual(response.media_type, "application/octet-stream")
        self.assertEqual(response.headers["access-control-allow-origin"], "*")
        self.assertEqual(response.headers["cache-control"], "no-cache")
        self.assertEqual([str(r.url) for r in seen], [self.URL])
        self.assertEqual(seen[0].headers["referer"], self.REFERER)

    def test_range_request_is_forwarded_and_partial_content_kept(self):
        seen = []

        def handler(request):
            seen.append(request)
            return httpx.Response(
                206, headers={"content-type": "video/mp4", "content-range": "bytes 0-3/16", "accept-ranges": "bytes"},
                content=self.KEY[:4], request=request,
            )

        response = self._call(
            handler, security._relay_signature(self.URL, self.REFERER), headers=[(b"range", b"bytes=0-3")],
        )
        self.assertEqual(seen[0].headers["range"], "bytes=0-3")
        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.body, self.KEY[:4])
        self.assertEqual(response.headers["content-range"], "bytes 0-3/16")
        self.assertEqual(response.headers["accept-ranges"], "bytes")

    def test_bad_signature_is_refused_without_contacting_the_upstream(self):
        seen = []

        def handler(request):
            seen.append(request)
            return httpx.Response(200, content=self.KEY, request=request)

        good = security._relay_signature(self.URL, self.REFERER)
        for label, sig, ref in (
            ("forged", "0" * 32, self.REFERER),
            ("empty", "", self.REFERER),
            ("signed for another referer", good, "https://other.example.test/"),
        ):
            with self.subTest(label):
                response = self._call(handler, sig, ref=ref)
                self.assertEqual(response.status_code, 403)
                self.assertEqual(response.body, b"Unsigned relay URL")
        self.assertEqual(seen, [])


def _session_hooks(resolved: list) -> SessionHooks:
    async def fetch(url, headers, max_bytes, timeout):
        return None

    def resolve_source(channel_id):
        resolved.append(channel_id)
        return None

    return SessionHooks(
        fetch=fetch,
        headers_for=lambda source: {},
        resolve_source=resolve_source,
        report_failure=lambda channel_id, key, reason: None,
        # No compatible standby: a cold start falls back to the legacy relay.
        report_incompatible=lambda channel_id, key, reason: False,
    )


_FMP4_SOURCE = SourceSpec(key=("origin", "cdn.example.test", "/live"), url="https://cdn.example.test/live.m3u8")


class LegacyRetryTests(unittest.TestCase):
    """A source the session engine cannot normalize (fMP4, separate audio) puts
    a cold channel session on the legacy relay; SessionConfig.legacy_retry_seconds
    (300 s) later the next viewer request retries the session engine."""

    def test_default_retry_interval_is_five_minutes(self):
        self.assertEqual(SessionConfig().legacy_retry_seconds, 300.0)

    def test_session_engine_is_retried_once_the_interval_has_passed(self):
        clock = _FakeTime()
        resolved: list = []

        async def scenario():
            config = SessionConfig()
            session = ChannelSession("fmp4_channel", _session_hooks(resolved), config)
            session.source = _FMP4_SOURCE
            session._incompatible("fMP4 or SAMPLE-AES source")
            self.assertEqual(session.state, "legacy")
            self.assertEqual(session.legacy_reason, "fMP4 or SAMPLE-AES source")

            clock.now += config.legacy_retry_seconds - 1.0
            session.touch()
            self.assertEqual(session.state, "legacy")
            self.assertFalse(session.is_running)

            clock.now += 1.0
            session.touch()
            self.assertEqual(session.state, "starting")
            self.assertEqual(session.legacy_reason, "")
            self.assertTrue(session.is_running)
            await asyncio.sleep(0)  # let the new poll loop run its first poll
            self.assertEqual(resolved, ["fmp4_channel"])
            await session.close()

        with patch.object(hls_session, "time", clock):
            asyncio.run(scenario())

    def test_playlist_requests_use_the_legacy_relay_until_the_retry(self):
        clock = _FakeTime()
        legacy_calls: list = []

        async def legacy():
            legacy_calls.append(clock.now)
            return Response(content="legacy playlist", media_type=upstream.HLS_MEDIA_TYPE)

        async def scenario():
            registry = SessionRegistry(_session_hooks([]), SessionConfig())
            session = registry.get("fmp4_channel")
            session.source = _FMP4_SOURCE
            session._incompatible("separate audio rendition")
            try:
                with patch.object(sessions, "SESSIONS", registry), \
                        patch.object(sessions, "STREAM_STARTUP_TIMEOUT", 0.05):
                    clock.now += 299.0
                    early = await sessions._serve_session_playlist("fmp4_channel", "fmp4_channel/seg/", legacy=legacy)
                    clock.now += 2.0
                    late = await sessions._serve_session_playlist("fmp4_channel", "fmp4_channel/seg/", legacy=legacy)
            finally:
                await registry.close_all()
            return early, late

        with patch.object(hls_session, "time", clock):
            early, late = asyncio.run(scenario())
        self.assertEqual(early.body, b"legacy playlist")
        self.assertEqual(len(legacy_calls), 1)
        # The retried session has nothing to play yet: "starting", not the relay.
        self.assertEqual(late.status_code, 503)


class IncompatibleCandidateRetryTests(unittest.TestCase):
    """failover.INCOMPATIBLE_RETRY_SECONDS: a candidate the session engine
    found unplayable is skipped by failover, then gets another chance."""

    def test_incompatible_standby_is_skipped_until_its_retry_time(self):
        marked_at = 1_700_000_000.0
        candidates = [
            {"provider": "Active", "url": "https://active.example.test/live.m3u8"},
            {"provider": "Standby", "url": "https://standby.example.test/live.m3u8",
             "session_compatible": False, "incompatible_at": marked_at},
        ]
        clock = _FakeTime(wall=marked_at + failover.INCOMPATIBLE_RETRY_SECONDS - 1.0)
        with patch.object(failover, "time", clock):
            self.assertIsNone(failover._pick_next_candidate(candidates, 0))
            clock.wall = marked_at + failover.INCOMPATIBLE_RETRY_SECONDS + 1.0
            self.assertEqual(failover._pick_next_candidate(candidates, 0), 1)


if __name__ == "__main__":
    unittest.main()
