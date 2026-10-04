import os
import sys
import json
import shutil
import socket
import tempfile
import unittest
import asyncio
import httpx
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from scrapers import (
    ACTIVE_PROVIDERS,
    BaseProvider,
    IptvOrgScraper,
    _parse_m3u_playlist,
    DaddyLiveScraper,
    HtmlAggregatorScraper,
    ISportSurgeScraper,
    MyBuffStreamsScraper,
    MethStreamsScraper,
    StreamEastScraper,
    TheTVAppScraper,
    FootybiteScraper,
    OneStreamScraper,
    StreamedSuScraper,
    TopStreamsScraper,
    _is_playlist_request_url,
    _providers_for_search,
    _channel_term_matches,
    _is_non_english_channel,
    _stream_matches_requested_event,
)
from db import (
    _performance_stats_sync,
    save_multiview_channel,
    load_multiview_channels,
    delete_multiview_channel,
)
from routes_dashboard import _catalog_selection_changes
from routes_api import api_status
from epg import generate_m3u, generate_xmltv
from failover import (
    _mark_scrape_finished,
    _mark_scrape_started,
    _scrape_lifecycle_defaults,
    _resolve_schedule_status,
)
from multiview import (
    _build_xstack_filter,
    _build_multiview_ffmpeg_args,
    _multiview_bufsize,
    _multiview_member_validation,
    _multiview_cooldown_remaining,
)
from ffmpeg_proc import _wait_for_first_segment, _multiview_backoff_seconds, _multiview_error_from_log
from placeholder import _build_placeholder_ffmpeg_args
from legacy_proxy import (
    LRUChunkCache,
    _chunk_route_for_url,
    _ensure_startup_buffer,
    _manifest_uri_is_playlist,
    _startup_media_urls,
    _chunk_media_type,
    proxy_chunk,
    proxy_substream,
    rewrite_m3u8,
    extract_manifest_media_urls,
    _register_manifest_segments,
    _get_next_manifest_chunks,
    prefetch_next_chunks,
)
from catalog import _resolve_espn_team, xmltv_ts
from state import stream_state
from config import _upstream_media_headers

from stream_extractor import extract_streams_from_text, verify_stream_live
from network_safety import clear_dns_cache, validate_http_url
from starlette.requests import Request
from sports_catalog import SPECIAL_CHANNELS
import config
import state
import security
import db
import scrapers
import legacy_proxy
import multiview


def _public_getaddrinfo(host, port, *args, **kwargs):
    """Stub DNS: every hostname resolves to a public address, so
    validate_http_url_async's DNS check doesn't depend on real network access."""
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", port))]


class JellyballChangesTests(unittest.TestCase):
    def test_linear_network_aliases_match_provider_labels(self):
        tnt = next(channel for channel in SPECIAL_CHANNELS if channel.name == "TNT Sports")
        nbc = next(channel for channel in SPECIAL_CHANNELS if channel.name == "NBC Sports")
        self.assertTrue(_channel_term_matches("TNT Sports Network", list(tnt.search_terms)))
        self.assertTrue(_channel_term_matches("NBCSN", list(nbc.search_terms)))

    def test_event_candidate_rejects_unrelated_game_stream(self):
        terms = ["buffalo bills", "bills", "buf"]
        self.assertTrue(_stream_matches_requested_event({
            "match_title": "Buffalo Bills vs Pittsburgh Steelers",
            "match_url": "https://provider.example.test/watch/buffalo-bills-steelers",
        }, terms))
        self.assertFalse(_stream_matches_requested_event({
            "match_title": "Syracuse vs Pittsburgh",
            "match_url": "https://provider.example.test/watch/syracuse-pittsburgh",
        }, terms))

    def test_hls_proxy_preserves_scheme_and_segment_media_type(self):
        rewritten = rewrite_m3u8(
            "#EXTM3U\n#EXTINF:6,\nsegment.m4s\n",
            "https://media.example.test/live/index.m3u8",
            "https://provider.example.test/watch/game",
            "https://127.0.0.1:8000",
        )
        self.assertIn("https://127.0.0.1:8000/chunk.mp4", rewritten)
        self.assertNotIn("https://media.example.test/live/index.m3u8", rewritten)
        self.assertEqual(_chunk_media_type("https://media.example.test/live/segment.m4s"), "video/mp4")
        self.assertEqual(_chunk_media_type("https://media.example.test/live/segment.ts"), "video/mp2t")

    def test_hls_proxy_rewrites_extensionless_variants_and_media_routes(self):
        manifest = (
            "#EXTM3U\n"
            "#EXT-X-STREAM-INF:BANDWIDTH=1000000\n"
            "video/720p\n"
            "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",URI=\"audio/eng\"\n"
            "#EXT-X-MAP:URI=\"init.mp4\"\n"
            "#EXTINF:6,\n"
            "segment.m4s\n"
        )
        rewritten = rewrite_m3u8(
            manifest,
            "https://media.example.test/live/master",
            "https://provider.example.test/watch/game",
            "http://127.0.0.1:8000",
        )

        self.assertIn("/substream.m3u8?url=https%3A//media.example.test/live/video/720p", rewritten)
        self.assertIn("/substream.m3u8?url=https%3A//media.example.test/live/audio/eng", rewritten)
        self.assertIn("/resource?url=https%3A//media.example.test/live/init.mp4", rewritten)
        self.assertIn("/chunk.mp4?url=https%3A//media.example.test/live/segment.m4s", rewritten)

    def test_hls_helpers_recognize_playlist_urls_and_container_routes(self):
        self.assertTrue(_manifest_uri_is_playlist("https://media.example.test/live/index.m3u8"))
        self.assertEqual(_chunk_route_for_url("https://media.example.test/live/segment.m4s"), "/chunk.mp4")
        self.assertEqual(_chunk_route_for_url("https://media.example.test/live/audio.aac"), "/chunk.aac")
        self.assertEqual(_chunk_route_for_url("https://media.example.test/live/segment.ts"), "/chunk.ts")

    def test_always_live_channel_matching_does_not_confuse_numbered_networks(self):
        self.assertTrue(_channel_term_matches("ESPN USA", ["espn"]))
        self.assertFalse(_channel_term_matches("ESPN2 USA", ["espn"]))
        self.assertTrue(_channel_term_matches("ESPN2 USA", ["espn2"]))
        self.assertFalse(_channel_term_matches("NESN USA", ["espn"]))
        # Broadcast networks disambiguation
        self.assertTrue(_channel_term_matches("FOX USA", ["fox"]))
        self.assertFalse(_channel_term_matches("FOX Sports 1 USA", ["fox"]))
        self.assertTrue(_channel_term_matches("FOX Sports 1 USA", ["fox sports 1", "fs1"]))
        self.assertFalse(_channel_term_matches("Fox News", ["fox"]))
        self.assertTrue(_channel_term_matches("CBS USA", ["cbs"]))
        self.assertFalse(_channel_term_matches("CBS Sports Network", ["cbs"]))
        self.assertTrue(_channel_term_matches("CBS Sports Network", ["cbs sports network"]))
        self.assertTrue(_channel_term_matches("NBC USA", ["nbc"]))
        self.assertFalse(_channel_term_matches("NBC Sports Philadelphia", ["nbc"]))
        self.assertTrue(_channel_term_matches("ABC USA", ["abc"]))
        self.assertTrue(_channel_term_matches("ABC NY USA", ["abc"]))
        self.assertFalse(_channel_term_matches("ABC News", ["abc"]))
        self.assertTrue(_channel_term_matches("USA Network", ["usa network"]))
        self.assertFalse(_channel_term_matches("ABC USA", ["usa network"]))
        self.assertTrue(_channel_term_matches("CW USA", ["cw"]))
        self.assertTrue(_channel_term_matches("TBS USA", ["tbs"]))
        self.assertTrue(_channel_term_matches("TruTV USA", ["trutv"]))

    def test_always_live_filter_accepts_english_and_rejects_explicit_regions(self):
        self.assertFalse(_is_non_english_channel("ESPN USA"))
        self.assertFalse(_is_non_english_channel("NBC Sports Network"))
        self.assertTrue(_is_non_english_channel("ESPN DE"))
        self.assertTrue(_is_non_english_channel("FOX Sports Espanol"))
        self.assertFalse(_is_non_english_channel("Sports in America"))

    def test_always_live_searches_use_only_linear_providers(self):
        self.assertEqual(
            [provider.name for provider in _providers_for_search(always_live=True)],
            ["TheTVApp", "DaddyLive", "IPTV-Org"],
        )
        self.assertEqual(
            [provider.name for provider in _providers_for_search(always_live=False)],
            [provider.name for provider in ACTIVE_PROVIDERS],
        )

    def test_playlist_request_filter_rejects_tracking_images(self):
        self.assertTrue(_is_playlist_request_url("https://media.example.test/secure_hls.php?path=feed/index.m3u8"))
        self.assertFalse(_is_playlist_request_url("https://tracker.example.test/ping.gif?mu=https%3A%2F%2Fmedia.example.test%2Ffeed.m3u8"))
        self.assertFalse(_is_playlist_request_url("https://cdn.example.test/frame.png"))
        self.assertFalse(_is_playlist_request_url("https://cdn.example.test/hls/feed-123.ts"))
        self.assertFalse(_is_playlist_request_url("https://cdn.jsdelivr.net/npm/player.js"))

    def test_hls_health_check_rejects_image_segments(self):
        async def exercise():
            def handler(request):
                if request.method == "HEAD":
                    return httpx.Response(200, headers={"content-type": "application/vnd.apple.mpegurl"}, request=request)
                if request.url.path.endswith(".m3u8"):
                    return httpx.Response(200, headers={"content-type": "application/vnd.apple.mpegurl"}, text="#EXTM3U\n#EXTINF:6,\nsegment.ts\n", request=request)
                return httpx.Response(200, headers={"content-type": "image/png"}, content=b"\x89PNG\r\n\x1a\n", request=request)

            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                return await verify_stream_live(client, "https://media.example.test/index.m3u8")

        self.assertFalse(asyncio.run(exercise()))

    def test_hls_health_check_accepts_mpeg_ts_segments(self):
        async def exercise():
            def handler(request):
                if request.method == "HEAD":
                    return httpx.Response(200, headers={"content-type": "application/vnd.apple.mpegurl"}, request=request)
                if request.url.path.endswith(".m3u8"):
                    return httpx.Response(200, headers={"content-type": "application/vnd.apple.mpegurl"}, text="#EXTM3U\n#EXTINF:6,\nsegment.ts\n", request=request)
                return httpx.Response(200, headers={"content-type": "video/mp2t"}, content=b"\x47" + b"\x00" * 1023, request=request)

            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                return await verify_stream_live(client, "https://media.example.test/index.m3u8")

        self.assertTrue(asyncio.run(exercise()))

    def test_hls_health_check_forwards_captured_origin(self):
        observed = {}

        async def exercise():
            def handler(request):
                observed["origin"] = request.headers.get("origin")
                if request.method == "HEAD":
                    return httpx.Response(200, headers={"content-type": "application/vnd.apple.mpegurl"}, request=request)
                if request.url.path.endswith(".m3u8"):
                    return httpx.Response(200, headers={"content-type": "application/vnd.apple.mpegurl"}, text="#EXTM3U\n#EXTINF:6,\nsegment.ts\n", request=request)
                return httpx.Response(200, headers={"content-type": "video/mp2t"}, content=b"\x47" + b"\x00" * 1023, request=request)

            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                return await verify_stream_live(
                    client,
                    "https://media.example.test/index.m3u8",
                    origin="https://player.example.test",
                )

        self.assertTrue(asyncio.run(exercise()))
        self.assertEqual(observed["origin"], "https://player.example.test")

    def test_upstream_headers_normalize_origin_to_scheme_and_host(self):
        headers = _upstream_media_headers(
            "https://provider.example.test/watch/game",
            "https://player.example.test/path?ignored=yes",
        )
        self.assertEqual(headers["Origin"], "https://player.example.test")

    def test_active_providers_use_shared_base_configuration(self):
        # IPTV-Org is deliberately not an HtmlAggregatorScraper: it has no event
        # pages to scan (categories/event_path_hints are meaningless for a static
        # playlist), so it extends BaseProvider directly instead of inheriting
        # scan-URL machinery it would never use.
        html_providers = [p for p in ACTIVE_PROVIDERS if p.name != "IPTV-Org"]
        self.assertTrue(all(isinstance(provider, HtmlAggregatorScraper) for provider in html_providers))
        self.assertIsInstance(next(p for p in ACTIVE_PROVIDERS if p.name == "IPTV-Org"), BaseProvider)

    def test_all_core_aggregators_use_shared_base_configuration(self):
        expected = {
            ISportSurgeScraper: ("iSportSurge", "/watch/"),
            MyBuffStreamsScraper: ("MyBuffStreams", "/cfb/"),
            MethStreamsScraper: ("MethStreams", "/game/"),
            StreamEastScraper: ("StreamEast", "/stream/"),
        }
        for scraper_type, (name, hint) in expected.items():
            with self.subTest(scraper=scraper_type.__name__):
                scraper = scraper_type()
                self.assertIs(type(scraper).search, HtmlAggregatorScraper.search)
                self.assertEqual(scraper.name, name)
                self.assertIn(hint, scraper.event_path_hints)

    def test_catalog_selection_changes_only_returns_state_deltas(self):
        entries = [
            {"catalog_key": "team:enabled"},
            {"catalog_key": "team:new"},
            {"catalog_key": "team:removed"},
        ]

        entries_by_key, to_enable, to_disable = _catalog_selection_changes(
            entries,
            {"team:enabled", "team:new"},
            {"team:enabled", "team:removed"},
        )
        self.assertEqual(set(entries_by_key), {"team:enabled", "team:new", "team:removed"})
        self.assertEqual(to_enable, {"team:new"})
        self.assertEqual(to_disable, {"team:removed"})

    def test_stream_extractor_resolves_relative_and_protocol_relative_urls(self):
        html = (
            '<script>const source = "/media/live/index.m3u8";</script>'
            '<video src="//cdn.example.test/channel/playlist.m3u8"></video>'
        )
        self.assertEqual(
            set(extract_streams_from_text(html, "https://player.example.test/watch/game")),
            {
                "https://player.example.test/media/live/index.m3u8",
                "https://cdn.example.test/channel/playlist.m3u8",
            },
        )

    def test_stream_extractor_rejects_private_relative_resolution(self):
        self.assertEqual(
            extract_streams_from_text('<script>const src="/live/index.m3u8";</script>', "http://127.0.0.1/watch"),
            [],
        )

    def test_chunk_cache_uses_monotonic_deadlines(self):
        async def exercise():
            cache = LRUChunkCache(capacity=1)
            await cache.put("key", b"data", 10)
            return await cache.get("key")

        self.assertEqual(asyncio.run(exercise()), b"data")

    def test_startup_buffer_selects_media_segments_not_nested_manifests(self):
        manifest = "#EXTM3U\nvariant.m3u8\n#EXTINF:6,\nsegment-1.ts\n#EXTINF:6,\nsegment-2.ts\n"
        self.assertEqual(
            _startup_media_urls(manifest, "https://media.example.test/live/master.m3u8"),
            [
                "https://media.example.test/live/segment-1.ts",
                "https://media.example.test/live/segment-2.ts",
            ],
        )

    def test_startup_buffer_does_not_wait_for_warming(self):
        import main

        async def exercise():
            original_seconds = legacy_proxy.STREAM_STARTUP_BUFFER_SECONDS
            original_warm = legacy_proxy._warm_startup_buffer
            try:
                legacy_proxy.STREAM_STARTUP_BUFFER_SECONDS = 15.0

                async def slow_warm(*args, **kwargs):
                    await asyncio.sleep(0.2)

                legacy_proxy._warm_startup_buffer = slow_warm
                started = asyncio.get_running_loop().time()
                await _ensure_startup_buffer(
                    "startup-test",
                    None,
                    ["https://media.example.test/live/segment.ts"],
                    "https://provider.example.test/watch/game",
                )
                return asyncio.get_running_loop().time() - started
            finally:
                legacy_proxy.STREAM_STARTUP_BUFFER_SECONDS = original_seconds
                legacy_proxy._warm_startup_buffer = original_warm
                task = legacy_proxy._STARTUP_BUFFER_TASKS.pop("startup-test", None)
                if task and not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

        self.assertLess(asyncio.run(exercise()), 0.1)

    def test_proxy_substream_returns_jellyfin_compatible_variant_playlist(self):
        import main

        manifest_url = "https://media.example.test/live/master.m3u8?token=proxy-test"
        manifest = "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nvideo/720p\n"

        def handler(request):
            self.assertEqual(request.url, httpx.URL(manifest_url))
            return httpx.Response(
                200,
                headers={"content-type": "application/vnd.apple.mpegurl"},
                text=manifest,
                request=request,
            )

        def request_for(path: str):
            return Request({
                "type": "http",
                "method": "GET",
                "scheme": "http",
                "path": path,
                "query_string": b"",
                "headers": [(b"host", b"127.0.0.1:8000")],
                "server": ("127.0.0.1", 8000),
                "client": ("127.0.0.1", 50000),
                "root_path": "",
                "http_version": "1.1",
            })

        async def exercise():
            client = httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=False)
            try:
                with patch.object(state, "SHARED_HTTP_CLIENT", client), patch.object(legacy_proxy, "STREAM_STARTUP_BUFFER_SECONDS", 0):
                    response = await proxy_substream(
                        request_for("/substream.m3u8"),
                        url=manifest_url,
                        ref="https://provider.example.test/watch/game",
                        sig=security._relay_signature(manifest_url, "https://provider.example.test/watch/game"),
                    )
                    return response
            finally:
                await client.aclose()

        response = asyncio.run(exercise())
        self.assertEqual(response.status_code, 200)
        self.assertIn("application/vnd.apple.mpegurl", response.media_type)
        self.assertIn("/substream.m3u8?url=https%3A", response.body.decode())
        self.assertNotIn("/chunk.ts?url=https%3A", response.body.decode())

    def test_proxy_chunk_forwards_range_and_preserves_partial_response(self):
        import main

        chunk_url = "https://media.example.test/live/segment.m4s?token=range-test"
        observed = {}

        def handler(request):
            observed["range"] = request.headers.get("range")
            return httpx.Response(
                206,
                headers={
                    "content-type": "video/mp4",
                    "content-range": "bytes 0-3/8",
                    "accept-ranges": "bytes",
                },
                content=b"moof",
                request=request,
            )

        request = Request({
            "type": "http",
            "method": "GET",
            "scheme": "http",
            "path": "/chunk.mp4",
            "query_string": b"",
            "headers": [(b"host", b"127.0.0.1:8000"), (b"range", b"bytes=0-3")],
            "server": ("127.0.0.1", 8000),
            "client": ("127.0.0.1", 50001),
            "root_path": "",
            "http_version": "1.1",
        })

        async def exercise():
            client = httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=False)
            try:
                with patch.object(state, "SHARED_HTTP_CLIENT", client):
                    response = await proxy_chunk(
                        request,
                        url=chunk_url,
                        ref="https://provider.example.test/watch/game",
                        sig=security._relay_signature(chunk_url, "https://provider.example.test/watch/game"),
                    )
                    # proxy_chunk streams the ranged response rather than
                    # buffering it, so the body must be drained from the
                    # StreamingResponse's async generator.
                    body = b"".join([chunk async for chunk in response.body_iterator])
                    return response, body
            finally:
                await client.aclose()

        response, body = asyncio.run(exercise())
        self.assertEqual(observed["range"], "bytes=0-3")
        self.assertEqual(response.status_code, 206)
        self.assertEqual(body, b"moof")
        self.assertEqual(response.headers["content-range"], "bytes 0-3/8")

    def test_safe_url_validation_rejects_credentials_and_private_hosts(self):
        self.assertIsNone(validate_http_url("https://user:pass@example.test/live.m3u8"))
        self.assertIsNone(validate_http_url("http://127.0.0.1/live.m3u8"))

    def test_catalog_selection_changes_rejects_unknown_keys(self):
        with self.assertRaises(ValueError):
            _catalog_selection_changes(
                [{"catalog_key": "team:known"}],
                {"team:unknown"},
                set(),
            )

    def test_stream_extractor_keeps_existing_absolute_stream_support(self):
        html = '<script>const source = "https://media.example.test/live/index.m3u8";</script>'

        self.assertEqual(
            extract_streams_from_text(html),
            ["https://media.example.test/live/index.m3u8"],
        )

    def test_all_configured_aggregators_are_registered(self):
        self.assertEqual(
            [provider.name for provider in ACTIVE_PROVIDERS],
            [
                "TheTVApp", "DaddyLive", "iSportSurge", "MyBuffStreams", "MethStreams", "StreamEast",
                "Footybite", "1Stream", "Streamed", "TopStreams", "IPTV-Org",
            ],
        )

    def test_added_aggregators_have_expected_scan_configuration(self):
        expected = {
            FootybiteScraper: (
                "https://footybite.im",
                [],
                ["/watch/", "/stream/", "/live/", "/match/"],
            ),
            OneStreamScraper: (
                "https://1stream.ws",
                [],
                ["/match/", "/stream/", "/live/"],
            ),
            StreamedSuScraper: (
                "https://streamed.su",
                ["/category/football", "/category/american-football", "/category/basketball", "/category/baseball", "/category/hockey"],
                ["/watch/", "/live/"],
            ),
            TopStreamsScraper: (
                "https://topstreams.info",
                ["/nfl", "/nba", "/nhl", "/mlb", "/soccer"],
                ["/watch/", "/match/", "/live/"],
            ),
        }

        for scraper_type, (base_url, categories, hints) in expected.items():
            with self.subTest(scraper=scraper_type.__name__):
                scraper = scraper_type()
                self.assertEqual(scraper.base_url, base_url)
                self.assertEqual(scraper.categories, categories)
                self.assertEqual(scraper.event_path_hints, tuple(hints))
                self.assertEqual(scraper.get_scan_urls(), [base_url, *(base_url + path for path in categories)])

    def test_added_aggregators_honor_environment_url_overrides(self):
        overrides = {
            "AGGREGATOR_9_URL": (FootybiteScraper, "https://footybite.example"),
            "AGGREGATOR_10_URL": (OneStreamScraper, "https://onestream.example"),
            "AGGREGATOR_11_URL": (StreamedSuScraper, "https://streamed.example"),
            "AGGREGATOR_12_URL": (TopStreamsScraper, "https://topstreams.example"),
        }

        with patch.dict(os.environ, {key: url for key, (_, url) in overrides.items()}):
            for env_name, (scraper_type, expected_url) in overrides.items():
                with self.subTest(environment_variable=env_name):
                    self.assertEqual(scraper_type().base_url, expected_url)

    def test_iptv_org_honors_environment_url_override(self):
        with patch.dict(os.environ, {"IPTV_ORG_PLAYLIST_URL": "https://iptv-org.example/sports.m3u"}):
            self.assertEqual(IptvOrgScraper().base_url, "https://iptv-org.example/sports.m3u")

    def test_parse_m3u_playlist_extracts_entries(self):
        playlist = (
            "#EXTM3U\n"
            '#EXTINF:-1 tvg-id="ESPN.us" tvg-name="ESPN (US)" group-title="Sports",ESPN (US)\n'
            "https://example.test/espn/index.m3u8\n"
            '#EXTINF:-1 tvg-id="ESPN2.us" tvg-name="ESPN2 (US)" group-title="Sports",ESPN2 (US)\n'
            "https://example.test/espn2/index.m3u8\n"
        )
        entries = _parse_m3u_playlist(playlist)
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0]["tvg_id"], "ESPN.us")
        self.assertEqual(entries[0]["tvg_name"], "ESPN (US)")
        self.assertEqual(entries[0]["url"], "https://example.test/espn/index.m3u8")
        self.assertEqual(entries[1]["tvg_id"], "ESPN2.us")

    def test_iptv_org_search_disambiguates_espn_from_espn2(self):
        import main

        playlist = (
            "#EXTM3U\n"
            '#EXTINF:-1 tvg-id="ESPN.us" tvg-name="ESPN (US)" group-title="Sports",ESPN (US)\n'
            "https://example.test/espn/index.m3u8\n"
            '#EXTINF:-1 tvg-id="ESPN2.us" tvg-name="ESPN2 (US)" group-title="Sports",ESPN2 (US)\n'
            "https://example.test/espn2/index.m3u8\n"
        )

        # The playlist cache is a module-level global (refreshed at most every
        # IPTV_ORG_REFRESH_SECONDS) so other tests/runs can't leave it populated
        # and cause this test to skip the mocked fetch entirely.
        original_cache = list(scrapers._IPTV_ORG_CACHE)
        original_loaded_at = scrapers._IPTV_ORG_CACHE_LOADED_AT
        scrapers._IPTV_ORG_CACHE = []
        scrapers._IPTV_ORG_CACHE_LOADED_AT = 0.0
        try:
            async def exercise():
                scraper = IptvOrgScraper()
                transport = httpx.MockTransport(lambda request: httpx.Response(200, text=playlist))
                async with httpx.AsyncClient(transport=transport) as client:
                    # The playlist fetch now validates every redirect hop with a
                    # DNS check; stub DNS so the test doesn't need real network.
                    clear_dns_cache()
                    with patch.object(socket, "getaddrinfo", side_effect=_public_getaddrinfo), \
                            patch("scrapers.verify_stream_live", return_value=True):
                        try:
                            return await scraper.search(["espn"], http_client=client)
                        finally:
                            clear_dns_cache()

            results = asyncio.run(exercise())
        finally:
            scrapers._IPTV_ORG_CACHE = original_cache
            scrapers._IPTV_ORG_CACHE_LOADED_AT = original_loaded_at

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["url"], "https://example.test/espn/index.m3u8")
        self.assertEqual(results[0]["provider"], "IPTV-Org")

    def test_scrape_lifecycle_transitions(self):
        state = _scrape_lifecycle_defaults()
        self.assertFalse(state["scrape_in_progress"])
        self.assertEqual(state["scrape_result"], "pending")

        _mark_scrape_started(state)
        self.assertTrue(state["scrape_in_progress"])
        self.assertGreater(state["last_scrape_started"], 0)
        self.assertEqual(state["scrape_result"], "running")

        _mark_scrape_finished(state, "healthy")
        self.assertFalse(state["scrape_in_progress"])
        self.assertGreaterEqual(state["last_scrape_completed"], state["last_scrape_started"])
        self.assertEqual(state["scrape_result"], "healthy")

    def test_api_status_exposes_scrape_lifecycle_and_active_provider(self):
        previous_state = dict(stream_state)
        try:
            stream_state.clear()
            stream_state["ncaaf_130"] = {
                "name": "Michigan Wolverines",
                "candidates": [{"provider": "Test Provider", "url": "https://example.test/live.m3u8"}],
                "active_index": 0,
                "is_healthy": True,
                "always_live": True,
                "scrape_in_progress": False,
                "last_scrape_started": 10.0,
                "last_scrape_completed": 20.0,
                "scrape_result": "healthy",
                "scrape_error": "",
            }
            result = asyncio.run(api_status())
            channel = result["channels"][0]
            self.assertEqual(channel["candidate_count"], 1)
            self.assertEqual(channel["active_provider"], "Test Provider")
            self.assertFalse(channel["scrape_in_progress"])
            self.assertEqual(channel["last_scrape_completed"], 20.0)
            self.assertEqual(channel["scrape_result"], "healthy")
        finally:
            stream_state.clear()
            stream_state.update(previous_state)

    def test_prefetch_chunks_parsed_from_manifest_segments(self):
        manifest_text = (
            "#EXTM3U\n"
            "#EXT-X-VERSION:3\n"
            "#EXT-X-TARGETDURATION:6\n"
            "#EXT-X-MEDIA-SEQUENCE:3165856830\n"
            "#EXTINF:6.000,\n"
            "3165856830.ts\n"
            "#EXTINF:6.000,\n"
            "3166226190.ts\n"
            "#EXTINF:6.000,\n"
            "3166595550.ts\n"
            "#EXTINF:6.000,\n"
            "3166961940.ts\n"
        )
        manifest_url = "https://cdn.example.test:8443/live/stream.m3u8"
        _register_manifest_segments(manifest_text, manifest_url)

        # Non-sequential IDs (jump of 369,360) are resolved directly from manifest
        next_chunks = _get_next_manifest_chunks(
            "https://cdn.example.test:8443/live/3165856830.ts",
            count=2,
        )
        self.assertEqual(
            next_chunks,
            [
                "https://cdn.example.test:8443/live/3166226190.ts",
                "https://cdn.example.test:8443/live/3166595550.ts",
            ],
        )

        # End of playlist does not extrapolate fake IDs
        last_chunks = _get_next_manifest_chunks(
            "https://cdn.example.test:8443/live/3166961940.ts",
            count=2,
        )
        self.assertEqual(last_chunks, [])

    def test_prefetch_next_chunks_disabled_when_count_zero(self):
        import main

        requested_urls = []

        def handler(request):
            requested_urls.append(str(request.url))
            return httpx.Response(200, content=b"chunkdata", request=request)

        async def exercise():
            client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            try:
                with patch.object(state, "SHARED_HTTP_CLIENT", client), patch.object(legacy_proxy, "PREFETCH_CHUNK_COUNT", 0):
                    await prefetch_next_chunks(
                        "https://cdn.example.test:8443/live/3165856830.ts"
                    )
            finally:
                await client.aclose()

        asyncio.run(exercise())
        self.assertEqual(requested_urls, [])

    def test_prefetch_next_chunks_requests_manifest_entries_not_numerical_guesses(self):
        import main

        manifest_text = (
            "#EXTM3U\n"
            "#EXTINF:6.000,\n"
            "3165856830.ts\n"
            "#EXTINF:6.000,\n"
            "3166226190.ts\n"
            "#EXTINF:6.000,\n"
            "3166595550.ts\n"
        )
        manifest_url = "https://cdn.example.test:8443/live/stream.m3u8"
        _register_manifest_segments(manifest_text, manifest_url)

        requested_urls = []

        def handler(request):
            requested_urls.append(str(request.url))
            return httpx.Response(200, content=b"chunkdata", request=request)

        async def exercise():
            client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            try:
                with patch.object(state, "SHARED_HTTP_CLIENT", client), patch.object(legacy_proxy, "PREFETCH_CHUNK_COUNT", 2):
                    await prefetch_next_chunks(
                        "https://cdn.example.test:8443/live/3165856830.ts",
                        referer=manifest_url,
                    )
            finally:
                await client.aclose()

        asyncio.run(exercise())
        # Confirms requests were for the real jump IDs, not 3165856831.ts / 3165856832.ts
        # (+1 guesses). prefetch_next_chunks fires these concurrently via
        # asyncio.gather, so completion order isn't guaranteed — compare as a set.
        self.assertEqual(
            sorted(requested_urls),
            sorted([
                "https://cdn.example.test:8443/live/3166226190.ts",
                "https://cdn.example.test:8443/live/3166595550.ts",
            ]),
        )


    def test_multiview_xstack_filters_match_expected_layout_syntax(self):
        side_by_side = _build_xstack_filter("side_by_side_2", 2)
        self.assertIn("xstack=inputs=2:layout=0_0|w0_0,fps=30[vout]", side_by_side)
        self.assertEqual(side_by_side.count("scale=960:1080"), 2)

        grid = _build_xstack_filter("grid_2x2", 4)
        self.assertIn("xstack=inputs=4:layout=0_0|w0_0|0_h0|w0_h0,fps=30[vout]", grid)
        self.assertEqual(grid.count("scale=960:540"), 4)

    def test_multiview_bufsize_preserves_unit_suffix(self):
        self.assertEqual(_multiview_bufsize("6M"), "12M")
        self.assertEqual(_multiview_bufsize("6000k"), "12000k")
        self.assertEqual(_multiview_bufsize("6"), "12")

    def test_multiview_ffmpeg_args_map_active_audio_and_reconnect_flags(self):
        import main

        data = {
            "member_team_ids": ["lions", "dolphins", "bucs", "jets"],
            "layout": "grid_2x2",
            "active_audio_team_id": "bucs",
        }
        members = {team: {"name": team.title()} for team in ("lions", "dolphins", "bucs")}
        with patch.object(config, "PORT", 8000), patch.dict(state.stream_state, members, clear=True),                 patch.dict(os.environ, {"JELLYBALL_HOST": ""}):
            args = _build_multiview_ffmpeg_args("mv_test", data, Path("/fake/out"), [True, True, False, True], "nvenc")

        self.assertEqual(args.count("-reconnect"), 4)
        self.assertEqual(args.count("-hwaccel"), 4)
        inputs = [args[i + 1] for i, a in enumerate(args) if a == "-i"]
        self.assertEqual(inputs, [
            "http://127.0.0.1:8000/stream/lions.m3u8",
            "http://127.0.0.1:8000/stream/dolphins.m3u8",
            "http://127.0.0.1:8000/stream/bucs.m3u8",
            # "jets" isn't a channel any more: its pane shows the placeholder.
            "http://127.0.0.1:8000/stream/__placeholder__.m3u8",
        ])
        # Every member's audio is encoded; the active one is chosen per viewer
        # session, not baked into the command.
        map_values = [args[i + 1] for i, a in enumerate(args) if a == "-map"]
        # Member audio is stream-copied (re-encoding live audio throttled
        # ffmpeg); only the member without audio gets a generated silent track.
        self.assertEqual(map_values, ["[vout]", "0:a:0", "1:a:0", "[a2]", "3:a:0"])
        graph = args[args.index("-filter_complex") + 1]
        self.assertIn("anullsrc=r=48000:cl=stereo[a2]", graph)
        self.assertNotIn("aresample", graph)
        self.assertEqual(args[args.index("-c:a") + 1], "copy")
        self.assertEqual(args[args.index("-c:a:2") + 1], "aac")
        tee = args[args.index("tee") + 1]
        self.assertEqual(tee.count("|"), 3)
        self.assertIn(r"select=\'v:0,a:3\'", tee)
        self.assertIn("hls_segment_filename=a0/seg_%06d.ts]a0/index.m3u8", tee)
        # Relative output paths only (ffmpeg is spawned with cwd=out_dir).
        self.assertNotIn("fake", tee)

    def test_multiview_member_validation_rejects_bad_selections(self):
        import main

        original_state = dict(state.stream_state)
        try:
            state.stream_state.clear()
            state.stream_state["lions"] = {"name": "Lions"}
            state.stream_state["dolphins"] = {"name": "Dolphins"}
            state.stream_state["bucs"] = {"name": "Bucs"}
            state.stream_state["mv1"] = {"name": "MV", "type": "multiview"}

            self.assertIsNone(_multiview_member_validation(["lions", "dolphins"]))
            self.assertIsNotNone(_multiview_member_validation(["lions", "dolphins", "bucs"]))
            self.assertIsNotNone(_multiview_member_validation(["lions", "lions"]))
            self.assertIsNotNone(_multiview_member_validation(["lions", "nope"]))
            self.assertIsNotNone(_multiview_member_validation(["lions", "mv1"]))
        finally:
            state.stream_state.clear()
            state.stream_state.update(original_state)

    def test_multiview_sqlite_round_trip(self):
        import main
        import gc

        tmpdir = tempfile.mkdtemp()
        try:
            db_path = os.path.join(tmpdir, "test_multiview.db")
            with patch.object(db, "DB_FILE", db_path):
                db.init_db()
                save_multiview_channel(
                    "mv_sunday", "NFL Sunday Quad-Box", "grid_2x2",
                    ["lions", "dolphins", "bucs", "jets"], "bucs",
                    "MVSunday.us", "Multi-View", "https://example.test/logo.png",
                )
                rows = load_multiview_channels()
                self.assertEqual(len(rows), 1)
                channel_id, name, layout, encoded_members, active_audio, tvg_id, group_title, logo_url = rows[0]
                self.assertEqual(channel_id, "mv_sunday")
                self.assertEqual(name, "NFL Sunday Quad-Box")
                self.assertEqual(layout, "grid_2x2")
                self.assertEqual(json.loads(encoded_members), ["lions", "dolphins", "bucs", "jets"])
                self.assertEqual(active_audio, "bucs")

                delete_multiview_channel("mv_sunday")
                self.assertEqual(load_multiview_channels(), [])
        finally:
            # sqlite3 connections opened via `with _connect_db() as conn:` are not
            # closed by that context manager (it only commits/rolls back), so force
            # collection before cleanup to avoid a Windows file-lock on rmtree.
            gc.collect()
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_performance_stats_flags_sustained_provider_failure(self):
        import main
        import gc
        import sqlite3

        tmpdir = tempfile.mkdtemp()
        try:
            db_path = os.path.join(tmpdir, "test_perf.db")
            with patch.object(db, "DB_FILE", db_path):
                db.init_db()
                conn = sqlite3.connect(db_path)
                try:
                    # "DeadProvider": 12 failed attempts spread across the last 5 days,
                    # none in the last hour — should be flagged sustained_failure even
                    # though it has no 1-hour data to compute an hourly rate from.
                    for i in range(12):
                        conn.execute(
                            "INSERT INTO provider_performance (provider, response_time_ms, success, timestamp) "
                            "VALUES (?, 0, 0, datetime('now', ?))",
                            ("DeadProvider", f"-{i * 8} hours"),
                        )
                    # "FlakyProvider": mostly failing in the last hour, but not dead overall.
                    for i in range(4):
                        conn.execute(
                            "INSERT INTO provider_performance (provider, response_time_ms, success, timestamp) "
                            "VALUES (?, 500, ?, datetime('now', '-10 minutes'))",
                            ("FlakyProvider", 1 if i == 0 else 0),
                        )
                    conn.commit()
                finally:
                    conn.close()

                stats = _performance_stats_sync()
                by_name = {p["provider"]: p for p in stats["provider_health"]}

                self.assertIn("DeadProvider", by_name)
                self.assertTrue(by_name["DeadProvider"]["sustained_failure"])
                self.assertEqual(by_name["DeadProvider"]["samples_5d"], 12)

                self.assertIn("FlakyProvider", by_name)
                self.assertFalse(by_name["FlakyProvider"]["sustained_failure"])
                self.assertTrue(by_name["FlakyProvider"]["at_risk"])

                # Likely-dead providers must sort first regardless of success_rate.
                self.assertEqual(stats["provider_health"][0]["provider"], "DeadProvider")
        finally:
            gc.collect()
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_multiview_entry_renders_in_m3u_and_xmltv(self):
        import main

        original_state = dict(state.stream_state)
        try:
            state.stream_state.clear()
            state.stream_state["mv_sunday"] = {
                "name": "NFL Sunday Quad-Box",
                "type": "multiview",
                "candidates": [{"synthetic": True}],
                "is_healthy": True,
                "always_live": True,
                "category": "multiview",
                "catalog_key": "",
                "logo_url": "",
                "tvg_id": "",
                "group_title": "Multi-View",
            }
            request = SimpleNamespace(headers={"host": "127.0.0.1:8000"})
            playlist = asyncio.run(generate_m3u(request))
            guide = asyncio.run(generate_xmltv())
        finally:
            state.stream_state.clear()
            state.stream_state.update(original_state)

        self.assertIn("NFL Sunday Quad-Box", playlist)
        # Names stay stable (no health emoji) and URLs carry the .m3u8 extension.
        self.assertNotIn("🟢", playlist)
        self.assertIn("http://127.0.0.1:8000/stream/mv_sunday.m3u8", playlist)
        self.assertIn("NFL Sunday Quad-Box", guide)

    def test_off_season_channel_excluded_from_m3u_and_xmltv(self):
        import main

        original_state = dict(state.stream_state)
        try:
            state.stream_state.clear()
            state.stream_state["gators_football"] = {
                "name": "Florida Gators (Football)",
                "query": "Florida Gators",
                "candidates": [],
                "is_healthy": False,
                "always_live": False,
                "category": "ncaaf",
                "catalog_key": "",
                "logo_url": "",
                "tvg_id": "",
                "group_title": "",
                "schedule_status": "off_season",
            }
            state.stream_state["chiefs"] = {
                "name": "Kansas City Chiefs",
                "query": "Kansas City Chiefs",
                "candidates": [{"url": "https://example.test/live.m3u8", "referer": "", "origin": "", "provider": "ESPN+"}],
                "is_healthy": True,
                "always_live": False,
                "category": "nfl",
                "catalog_key": "",
                "logo_url": "",
                "tvg_id": "",
                "group_title": "",
                "schedule_status": "scheduled",
            }
            request = SimpleNamespace(headers={"host": "127.0.0.1:8000"})
            playlist = asyncio.run(generate_m3u(request))
            guide = asyncio.run(generate_xmltv())
        finally:
            state.stream_state.clear()
            state.stream_state.update(original_state)

        self.assertNotIn("Florida Gators", playlist)
        self.assertNotIn("gators_football", playlist)
        self.assertNotIn("Florida Gators", guide)
        self.assertIn("Kansas City Chiefs", playlist)
        self.assertIn("Kansas City Chiefs", guide)

    def test_resolve_schedule_status_prefers_a_found_event_over_everything_else(self):
        start = datetime(2026, 9, 20, 18, 0, tzinfo=timezone.utc)
        stop = start + timedelta(hours=3)
        start_str, stop_str, status = _resolve_schedule_status(
            start, stop, in_season=False, always_live=False, schedule_ok=True,
            previous_start="stale-start", previous_stop="stale-stop",
        )
        self.assertEqual(status, "scheduled")
        self.assertEqual(start_str, xmltv_ts(start))
        self.assertEqual(stop_str, xmltv_ts(stop))

    def test_resolve_schedule_status_off_season_wins_when_no_event_found(self):
        start_str, stop_str, status = _resolve_schedule_status(
            None, None, in_season=False, always_live=False, schedule_ok=True,
            previous_start="stale-start", previous_stop="stale-stop",
        )
        self.assertEqual((start_str, stop_str, status), ("", "", "off_season"))

    def test_resolve_schedule_status_always_live_ignores_season(self):
        # An always-live channel with no event and in_season=False (never checked)
        # must not be marked off_season — off_season only applies to seasonal teams.
        start_str, stop_str, status = _resolve_schedule_status(
            None, None, in_season=False, always_live=True, schedule_ok=True,
            previous_start="", previous_stop="",
        )
        self.assertEqual(status, "always_live")

    def test_resolve_schedule_status_confirmed_no_event_clears_stale_window(self):
        start_str, stop_str, status = _resolve_schedule_status(
            None, None, in_season=True, always_live=False, schedule_ok=True,
            previous_start="stale-start", previous_stop="stale-stop",
        )
        self.assertEqual((start_str, stop_str, status), ("", "", "no_event"))

    def test_resolve_schedule_status_failed_lookup_retains_previous_window(self):
        start_str, stop_str, status = _resolve_schedule_status(
            None, None, in_season=True, always_live=False, schedule_ok=False,
            previous_start="stale-start", previous_stop="stale-stop",
        )
        self.assertEqual((start_str, stop_str, status), ("stale-start", "stale-stop", "lookup_failed"))

    def test_wait_for_first_segment_detects_ready_playlist(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir)
            fake_process = SimpleNamespace(returncode=None)

            async def exercise():
                async def write_playlist_soon():
                    await asyncio.sleep(0.1)
                    (out_dir / "seg_00001.ts").write_bytes(b"data")
                    (out_dir / "index.m3u8").write_text(
                        "#EXTM3U\n#EXTINF:4.0,\nseg_00001.ts\n", encoding="utf-8"
                    )

                writer = asyncio.create_task(write_playlist_soon())
                try:
                    return await _wait_for_first_segment(out_dir, fake_process, timeout=3.0, poll_interval=0.05)
                finally:
                    await writer

            self.assertTrue(asyncio.run(exercise()))

    def test_wait_for_first_segment_returns_false_when_process_dies(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir)
            fake_process = SimpleNamespace(returncode=1)  # already exited
            result = asyncio.run(_wait_for_first_segment(out_dir, fake_process, timeout=1.0, poll_interval=0.05))
            self.assertFalse(result)

    def test_multiview_backoff_grows_and_caps(self):
        self.assertEqual(_multiview_backoff_seconds(1), 15.0)
        self.assertEqual(_multiview_backoff_seconds(2), 30.0)
        self.assertEqual(_multiview_backoff_seconds(3), 60.0)
        self.assertEqual(_multiview_backoff_seconds(10), 300.0)  # capped

    def test_multiview_circuit_breaker_blocks_spawn_during_cooldown(self):
        import main

        channel_id = "mv_backoff_test"
        original_failures = dict(multiview._MULTIVIEW_FAILURES)
        try:
            multiview._MULTIVIEW_FAILURES.clear()
            self.assertEqual(_multiview_cooldown_remaining(channel_id), 0.0)

            multiview._record_multiview_failure(channel_id, "Server returned 404 Not Found")
            first_cooldown = _multiview_cooldown_remaining(channel_id)
            self.assertGreater(first_cooldown, 0.0)
            self.assertLessEqual(first_cooldown, 15.0)

            # A second failure backs off further (15s -> 30s), not resetting to the same window.
            multiview._record_multiview_failure(channel_id, "Server returned 404 Not Found")
            second_cooldown = _multiview_cooldown_remaining(channel_id)
            self.assertGreater(second_cooldown, first_cooldown)

            multiview._clear_multiview_failure(channel_id)
            self.assertEqual(_multiview_cooldown_remaining(channel_id), 0.0)
        finally:
            multiview._MULTIVIEW_FAILURES.clear()
            multiview._MULTIVIEW_FAILURES.update(original_failures)

    def test_multiview_error_from_log_prefers_input_error_over_boilerplate(self):
        log_lines = [
            "ffmpeg version 9.0.1-essentials_build",
            "libavutil      61.  1.101 / 61.  1.101",
            "[http @ 0x1] HTTP error 404 Not Found",
            "Error opening input file http://127.0.0.1:8000/stream/ncaaf_130.",
            "Error opening input files: Server returned 404 Not Found",
        ]
        self.assertEqual(
            _multiview_error_from_log(log_lines),
            "Error opening input files: Server returned 404 Not Found",
        )

    def test_multiview_error_from_log_falls_back_to_last_line(self):
        self.assertEqual(_multiview_error_from_log(["some progress line", "final line"]), "final line")
        self.assertEqual(_multiview_error_from_log([]), "ffmpeg exited unexpectedly")

    def test_placeholder_ffmpeg_args_produce_infinite_looped_hls_output(self):
        args = _build_placeholder_ffmpeg_args(Path("/fake/out"))
        self.assertIn("-loop", args)
        self.assertIn("anullsrc=r=48000:cl=stereo", args)
        self.assertIn("No Signal", " ".join(args))
        self.assertIn("seg_%05d.ts", args)
        self.assertIn("index.m3u8", args)
        # Must not reference a real dead-team URL - it's a purely synthetic source.
        self.assertNotIn("http://", " ".join(args))


if __name__ == "__main__":
    unittest.main()
