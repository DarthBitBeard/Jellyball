import os
import sys
import unittest
import asyncio
import httpx
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from main import (
    ACTIVE_PROVIDERS,
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
    _catalog_selection_changes,
    _mark_scrape_finished,
    _mark_scrape_started,
    _is_playlist_request_url,
    _providers_for_search,
    _scrape_lifecycle_defaults,
    api_status,
    stream_state,
    LRUChunkCache,
    _resolve_espn_team,
    _channel_term_matches,
    _is_non_english_channel,
    _chunk_route_for_url,
    _ensure_startup_buffer,
    _manifest_uri_is_playlist,
    _startup_media_urls,
    _stream_matches_requested_event,
    _chunk_media_type,
    _upstream_media_headers,
    proxy_chunk,
    proxy_substream,
    rewrite_m3u8,
    extract_manifest_media_urls,
    _register_manifest_segments,
    _get_next_manifest_chunks,
    prefetch_next_chunks,
)

from stream_extractor import extract_streams_from_text, verify_stream_live
from network_safety import validate_http_url
from starlette.requests import Request
from sports_catalog import SPECIAL_CHANNELS


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
            ["TheTVApp", "DaddyLive"],
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
        self.assertTrue(all(isinstance(provider, HtmlAggregatorScraper) for provider in ACTIVE_PROVIDERS))

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
            original_seconds = main.STREAM_STARTUP_BUFFER_SECONDS
            original_warm = main._warm_startup_buffer
            try:
                main.STREAM_STARTUP_BUFFER_SECONDS = 15.0

                async def slow_warm(*args, **kwargs):
                    await asyncio.sleep(0.2)

                main._warm_startup_buffer = slow_warm
                started = asyncio.get_running_loop().time()
                await _ensure_startup_buffer(
                    "startup-test",
                    None,
                    ["https://media.example.test/live/segment.ts"],
                    "https://provider.example.test/watch/game",
                )
                return asyncio.get_running_loop().time() - started
            finally:
                main.STREAM_STARTUP_BUFFER_SECONDS = original_seconds
                main._warm_startup_buffer = original_warm
                task = main._STARTUP_BUFFER_TASKS.pop("startup-test", None)
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
                with patch.object(main, "SHARED_HTTP_CLIENT", client), patch.object(main, "STREAM_STARTUP_BUFFER_SECONDS", 0):
                    response = await proxy_substream(
                        request_for("/substream.m3u8"),
                        url=manifest_url,
                        ref="https://provider.example.test/watch/game",
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
                with patch.object(main, "SHARED_HTTP_CLIENT", client):
                    return await proxy_chunk(
                        request,
                        url=chunk_url,
                        ref="https://provider.example.test/watch/game",
                    )
            finally:
                await client.aclose()

        response = asyncio.run(exercise())
        self.assertEqual(observed["range"], "bytes=0-3")
        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.body, b"moof")
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
                "Footybite", "1Stream", "Streamed", "TopStreams",
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
        manifest_url = "https://4169it.7odxv0l067ka.net:8443/live/stream.m3u8"
        _register_manifest_segments(manifest_text, manifest_url)

        # Non-sequential IDs (jump of 369,360) are resolved directly from manifest
        next_chunks = _get_next_manifest_chunks(
            "https://4169it.7odxv0l067ka.net:8443/live/3165856830.ts",
            count=2,
        )
        self.assertEqual(
            next_chunks,
            [
                "https://4169it.7odxv0l067ka.net:8443/live/3166226190.ts",
                "https://4169it.7odxv0l067ka.net:8443/live/3166595550.ts",
            ],
        )

        # End of playlist does not extrapolate fake IDs
        last_chunks = _get_next_manifest_chunks(
            "https://4169it.7odxv0l067ka.net:8443/live/3166961940.ts",
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
                with patch.object(main, "SHARED_HTTP_CLIENT", client), patch.object(main, "PREFETCH_CHUNK_COUNT", 0):
                    await prefetch_next_chunks(
                        "https://4169it.7odxv0l067ka.net:8443/live/3165856830.ts"
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
        manifest_url = "https://4169it.7odxv0l067ka.net:8443/live/stream.m3u8"
        _register_manifest_segments(manifest_text, manifest_url)

        requested_urls = []

        def handler(request):
            requested_urls.append(str(request.url))
            return httpx.Response(200, content=b"chunkdata", request=request)

        async def exercise():
            client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            try:
                with patch.object(main, "SHARED_HTTP_CLIENT", client), patch.object(main, "PREFETCH_CHUNK_COUNT", 2):
                    await prefetch_next_chunks(
                        "https://4169it.7odxv0l067ka.net:8443/live/3165856830.ts",
                        referer=manifest_url,
                    )
            finally:
                await client.aclose()

        asyncio.run(exercise())
        # Confirms requests were for the real jump IDs, not 3165856831.ts / 3165856832.ts (+1 guesses)
        self.assertEqual(
            requested_urls,
            [
                "https://4169it.7odxv0l067ka.net:8443/live/3166226190.ts",
                "https://4169it.7odxv0l067ka.net:8443/live/3166595550.ts",
            ],
        )


if __name__ == "__main__":
    unittest.main()
