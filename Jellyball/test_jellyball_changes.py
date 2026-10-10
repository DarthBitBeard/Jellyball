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
    _provider_mirror_list,
    _log_bypass_failure_throttled,
    _BYPASS_WARNED_AT,
    BUILTIN_PROVIDER_MIRRORS,
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
    _rewrite_sample_aes_manifest,
    sample_aes_chunk,
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

    def test_sample_aes_shim_rewrites_segments_and_keys(self):
        manifest = (
            "#EXTM3U\n"
            '#EXT-X-KEY:METHOD=SAMPLE-AES,URI="key.bin"\n'
            "#EXTINF:6,\n"
            "segment0.ts\n"
            "#EXTINF:6,\n"
            "segment1.ts\n"
        )
        rewritten = _rewrite_sample_aes_manifest(
            manifest,
            "https://media.example.test/live/index.m3u8",
            "https://provider.example.test/watch/game",
            "",
            "https://127.0.0.1:8000",
        )
        self.assertIn("https://127.0.0.1:8000/sample_aes/chunk?url=", rewritten)
        self.assertIn("https://127.0.0.1:8000/sample_aes/resource?url=", rewritten)
        self.assertIn("sig=", rewritten)

    def test_sample_aes_shim_rejects_unsigned_chunk_relay(self):
        import asyncio

        async def run():
            scope = {"type": "http", "method": "GET", "headers": []}
            request = Request(scope)
            response = await sample_aes_chunk(
                request, url="https://media.example.test/live/seg0.ts",
                ref="", org="", sig="bogus",
            )
            return response.status_code

        self.assertEqual(asyncio.run(run()), 403)

    def test_always_live_channel_matching_does_not_confuse_numbered_networks(self):
        self.assertTrue(_channel_term_matches("ESPN USA", ["espn"]))
        self.assertFalse(_channel_term_matches("ESPN2 USA", ["espn"]))
        self.assertTrue(_channel_term_matches("ESPN2 USA", ["espn2"]))
        self.assertFalse(_channel_term_matches("NESN USA", ["espn"]))
        # Numbered regional feeds must not match the bare network (2.2.1: ESPN 4
        # from the IPTV-Org playlist was winning the ESPN candidate ranking).
        self.assertFalse(_channel_term_matches("ESPN 4 (1080p)", ["espn"]))
        self.assertFalse(_channel_term_matches("ESPN 3 (1080p)", ["espn"]))
        self.assertFalse(_channel_term_matches("ESPN 4 (1080p)", ["espn2"]))
        self.assertFalse(_channel_term_matches("ESPN 3 (1080p)", ["espn2"]))
        self.assertTrue(_channel_term_matches("ESPN (1080p)", ["espn"]))
        self.assertTrue(_channel_term_matches("ESPN 2", ["espn2", "espn 2"]))
        self.assertFalse(_channel_term_matches("ESPN Deportes HD (720p)", ["espn"]))
        self.assertFalse(_channel_term_matches("ESPN 4 (1080p)", ["espnu", "espn u"]))
        self.assertTrue(_channel_term_matches("ESPNU (720p)", ["espnu", "espn u"]))
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

    def test_provider_mirror_list_prefers_env_then_builtins_deduped(self):
        import scrapers as scrapers_module

        mirrors = _provider_mirror_list("DaddyLive")
        # Built-ins are present even with no env config.
        for builtin in BUILTIN_PROVIDER_MIRRORS["DaddyLive"]:
            self.assertIn(builtin, mirrors)
        self.assertEqual(mirrors, sorted(set(mirrors), key=mirrors.index))
        # Providers without built-ins get an empty list.
        self.assertEqual(_provider_mirror_list("NoSuchProvider"), [])
        # Env-configured mirrors come first and duplicates collapse.
        with patch.dict(
            scrapers_module.PROVIDER_MIRROR_DOMAINS,
            {"DaddyLive": ["https://custom.example", "https://dlhd.st"]},
            clear=False,
        ):
            mirrors = _provider_mirror_list("DaddyLive")
            self.assertEqual(mirrors[0], "https://custom.example")
            self.assertEqual(mirrors.count("https://dlhd.st"), 1)

    def test_bypass_failure_warning_is_throttled(self):
        _BYPASS_WARNED_AT.clear()
        exc = RuntimeError("net::ERR_NAME_NOT_RESOLVED")
        with patch("scrapers._log_failure") as mock_log:
            _log_bypass_failure_throttled("DaddyLive", "https://dlhd.pk/x.php", exc)
            _log_bypass_failure_throttled("DaddyLive", "https://dlhd.pk/x.php", exc)
            _log_bypass_failure_throttled("DaddyLive", "https://dlhd.pk/y.php", exc)
            # Two distinct pages warned once each; the repeat was throttled.
            self.assertEqual(mock_log.call_count, 2)
        _BYPASS_WARNED_AT.clear()

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
        lifecycle = _scrape_lifecycle_defaults()
        self.assertFalse(lifecycle["scrape_in_progress"])
        self.assertEqual(lifecycle["scrape_result"], "pending")

        _mark_scrape_started(lifecycle)
        self.assertTrue(lifecycle["scrape_in_progress"])
        self.assertGreater(lifecycle["last_scrape_started"], 0)
        self.assertEqual(lifecycle["scrape_result"], "running")

        _mark_scrape_finished(lifecycle, "healthy")
        self.assertFalse(lifecycle["scrape_in_progress"])
        self.assertGreaterEqual(lifecycle["last_scrape_completed"], lifecycle["last_scrape_started"])
        self.assertEqual(lifecycle["scrape_result"], "healthy")

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

