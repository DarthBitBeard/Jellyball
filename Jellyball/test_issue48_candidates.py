"""Issue #48: a live game where only one of a dozen found streams became a
candidate, "playlist stale" retried the same source forever, and a team
channel took an unrelated linear channel.

Shapes come from the report (no real hosts):
  * iSportSurge lists a game as a card whose link only says "Watch" and points
    at /event/<id>; the provider matched it on the card text, then the engine's
    event filter dropped all 5-7 of its streams.
  * MyBuffStreams' playlist URL has no .m3u8 suffix, and its segments are
    `video-5segment_...txt?path=...` endpoints that 302 to a signed .ts.
  * IPTV-Org matched the Packers' "gb" against a snooker channel's region code.
"""

import gc
import logging
import os
import shutil
import socket
import tempfile
import time
import unittest
from typing import List
from unittest.mock import AsyncMock, patch

import httpx

import db
import failover
import scrapers
import sessions
import state
from hls_session import SourceSpec, segment_identity
from network_safety import clear_dns_cache
from sports_matcher import get_team_search_terms
from stream_extractor import verify_stream_live
from test_hls_session import Harness, fast_config, make_ts_segment
from hls_session import ChannelSession
from upstream import _fetch_upstream_body

NINERS = get_team_search_terms("San Francisco 49ers", "San Francisco 49ers")
PACKERS = get_team_search_terms("Green Bay Packers", "Green Bay Packers")

SURGE_BASE = "https://surge.example.test"
INDEX_HTML = """
<html><body>
  <div class="card"><span>San Francisco 49ers vs Los Angeles Rams</span>
    <a href="/event/game-101">Watch</a></div>
  <div class="card"><span>Dallas Cowboys vs New York Giants</span>
    <a href="/event/game-102">Watch</a></div>
</body></html>
"""


def _public_dns(host, port, *args, **kwargs):
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", port))]


def _candidate(provider, url, **extra):
    return {"provider": provider, "url": url, "referer": "", "origin": "", **extra}


# ---------------------------------------------------------------------------
# (1) Streams a provider returned must become candidates
# ---------------------------------------------------------------------------

class EventFilterTests(unittest.TestCase):
    def test_generic_link_is_judged_on_the_card_it_was_matched_on(self):
        stream = {
            "match_title": "Watch",
            "match_url": SURGE_BASE + "/event/game-101",
            "match_context": "San Francisco 49ers vs Los Angeles Rams Watch",
        }
        self.assertTrue(scrapers._stream_matches_requested_event(stream, NINERS))

    def test_generic_link_without_context_cannot_be_confirmed(self):
        stream = {"match_title": "Watch", "match_url": SURGE_BASE + "/event/game-101"}
        self.assertFalse(scrapers._stream_matches_requested_event(stream, NINERS))

    def test_context_naming_another_game_is_still_rejected(self):
        stream = {
            "match_title": "Watch",
            "match_url": SURGE_BASE + "/event/game-102",
            "match_context": "Dallas Cowboys vs New York Giants Watch",
        }
        self.assertFalse(scrapers._stream_matches_requested_event(stream, NINERS))


class _OneStreamProvider:
    """MyBuffStreams-like provider: one stream with a team-named title."""

    name = "MyBuffStreams"
    carries_team_events = True

    def __init__(self, streams):
        self._streams = streams

    async def search(self, terms, browser=None, http_client=None):
        return [dict(s) for s in self._streams]


class CandidatePipelineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.mkdtemp()
        self._db = patch.object(db, "DB_FILE", os.path.join(self.tmp, "t.db"))
        self._db.start()
        db.init_db()
        scrapers._PROVIDER_BREAKERS.clear()
        scrapers._SCRAPE_INDEX_CACHE.clear()

    async def asyncTearDown(self):
        self._db.stop()
        db.close_all_db_connections()
        gc.collect()
        shutil.rmtree(self.tmp, ignore_errors=True)
        scrapers._PROVIDER_BREAKERS.clear()
        scrapers._SCRAPE_INDEX_CACHE.clear()

    def _surge(self):
        surge = scrapers.ISportSurgeScraper()
        surge.base_url = SURGE_BASE

        async def index_page(client, page_url, browser):
            return INDEX_HTML if page_url.rstrip("/") == SURGE_BASE else None

        surge._fetch_index_page = index_page
        return surge

    async def _event_streams(self, client, match_url, provider, score, match_title):
        # Seven player streams per event, each on its own CDN host; the link
        # text the provider saw ("Watch") is the stream's match_title.
        game = match_url.rsplit("-", 1)[-1]
        return [
            {
                "url": f"https://cdn{i}.example.test/{game}/index.m3u8",
                "referer": match_url,
                "provider": provider,
                "match_score": score,
                "match_title": match_title,
                "discovery_method": "http",
            }
            for i in range(1, 8)
        ]

    async def _scrape(self, providers, terms=NINERS):
        with patch.object(scrapers, "_providers_for_search", lambda always_live=False: providers), \
                patch.object(scrapers, "get_healthy_browser", AsyncMock(return_value=None)), \
                patch.object(scrapers, "_PROVIDER_BASE_URL_OVERRIDES", {}), \
                patch.object(scrapers.provider_alerts, "check_soon"), \
                patch("plugins.sdk.fetch_streams_from_page", side_effect=self._event_streams), \
                patch.object(state, "SHARED_HTTP_CLIENT", None):
            return await scrapers.master_scrape(
                "San Francisco 49ers", team_name="San Francisco 49ers", team_id="nfl_sf", search_terms=terms,
            )

    async def test_card_matched_streams_become_standby_candidates(self):
        mybuff = _OneStreamProvider([{
            "url": "https://buff.example.test/live/caxi-fhd", "provider": "MyBuffStreams",
            "match_score": 100, "match_title": "49ers vs Rams",
            "match_url": "https://buff.example.test/nfl/49ers-vs-rams",
        }])
        found = await self._scrape([self._surge(), mybuff])

        by_provider = {}
        for stream in found:
            by_provider.setdefault(stream["provider"], []).append(stream)
        self.assertEqual(len(by_provider.get("iSportSurge", [])), 7)
        self.assertEqual(len(by_provider.get("MyBuffStreams", [])), 1)
        # Only the requested game: the Cowboys card is never inspected.
        self.assertTrue(all("/101/" in s["url"] for s in by_provider["iSportSurge"]))

        # A channel that was "healthy" on MyBuffStreams keeps it active and
        # gains every iSportSurge stream as a standby; the dashboard count is
        # len(candidates).
        active = _candidate("MyBuffStreams", "https://buff.example.test/live/caxi-fhd")
        merged, index = failover._merge_stream_candidates([active], 0, found, keep_active=True)
        self.assertEqual(index, 0)
        self.assertIs(merged[0], active)
        self.assertEqual(len(merged), 8)

    async def test_dropped_streams_are_logged(self):
        other = _OneStreamProvider([{
            "url": "https://other.example.test/live/index.m3u8", "provider": "MyBuffStreams",
            "match_score": 100, "match_title": "Dallas Cowboys vs New York Giants",
            "match_url": "https://buff.example.test/nfl/cowboys-vs-giants",
        }])
        with self.assertLogs("jellyball", level="INFO") as logs:
            found = await self._scrape([other])
        self.assertEqual(found, [])
        self.assertTrue(any(
            "Dropped provider streams not for the requested event provider=MyBuffStreams" in line
            and "dropped=1 kept=0" in line
            for line in logs.output
        ), logs.output)


# ---------------------------------------------------------------------------
# (2) "playlist stale" must fail over instead of retrying the same source
# ---------------------------------------------------------------------------

class FailoverTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._backup = dict(state.stream_state)
        state.stream_state.clear()
        self._patches = [
            patch.object(failover, "send_alert", new=AsyncMock()),
            patch.object(failover, "log_metric_event_async", new=AsyncMock()),
            patch.object(failover, "_spawn_background_task", side_effect=lambda c, n: c.close()),
        ]
        for p in self._patches:
            p.start()

    def tearDown(self):
        for p in self._patches:
            p.stop()
        state.stream_state.clear()
        state.stream_state.update(self._backup)

    async def test_stale_source_fails_over_to_a_standby(self):
        buff = _candidate("MyBuffStreams", "https://buff.example.test/live/caxi-fhd")
        surge = _candidate("iSportSurge", "https://cdn1.example.test/101/index.m3u8")
        data = {"name": "49ers", "candidates": [buff, surge], "active_index": 0}
        state.stream_state["nfl_sf"] = data
        moved = await failover.request_failover("nfl_sf", sessions.candidate_source_key(buff), "playlist stale")
        self.assertTrue(moved)
        self.assertEqual(data["active_index"], 1)

    async def test_exhausted_channel_is_not_put_back_on_the_source_it_just_gave_up_on(self):
        buff = _candidate("MyBuffStreams", "https://buff.example.test/live/caxi-fhd")
        data = {"name": "49ers", "candidates": [buff], "active_index": 0}
        state.stream_state["nfl_sf"] = data
        key = sessions.candidate_source_key(buff)
        for _ in range(failover.SELF_RETRY_LIMIT + 1):
            await failover.request_failover("nfl_sf", key, "playlist stale")
        self.assertTrue(data["exhausted"])

        # Its playlist still answers, which used to "recover" the channel onto
        # it within seconds and reset the retry budget: an endless loop.
        with patch.object(failover, "check_stream_health", new=AsyncMock(return_value=True)):
            await failover._probe_exhausted_candidates("nfl_sf", data)
            self.assertTrue(data["exhausted"])
            # Once the failure is old, the next probe may try it again.
            buff["failed_over_at"] = time.time() - failover.CANDIDATE_FAILED_RETRY_AFTER - 1
            await failover._probe_exhausted_candidates("nfl_sf", data)
        self.assertFalse(data["exhausted"])
        self.assertEqual(data["active_index"], 0)

    async def test_exhausted_channel_recovers_onto_another_source(self):
        buff = _candidate("MyBuffStreams", "https://buff.example.test/live/caxi-fhd")
        surge = _candidate("iSportSurge", "https://cdn1.example.test/101/index.m3u8",
                           last_health_ok=False, last_health_check=time.time())
        data = {"name": "49ers", "candidates": [buff, surge], "active_index": 0}
        state.stream_state["nfl_sf"] = data
        key = sessions.candidate_source_key(buff)
        for _ in range(failover.SELF_RETRY_LIMIT + 1):
            await failover.request_failover("nfl_sf", key, "playlist stale")
        self.assertTrue(data["exhausted"])
        with patch.object(failover, "check_stream_health", new=AsyncMock(return_value=True)):
            await failover._probe_exhausted_candidates("nfl_sf", data)
        self.assertFalse(data["exhausted"])
        self.assertEqual(data["active_index"], 1)

    def test_rescrape_keeps_the_failure_only_for_the_same_url(self):
        failed = _candidate("MyBuffStreams", "https://buff.example.test/live/caxi-fhd?t=1",
                            failed_over_at=time.time())
        same = _candidate("MyBuffStreams", "https://buff.example.test/live/caxi-fhd?t=1")
        merged, _ = failover._merge_stream_candidates([failed], 0, [same], keep_active=False)
        self.assertIn("failed_over_at", merged[0])
        refreshed = _candidate("MyBuffStreams", "https://buff.example.test/live/caxi-fhd?t=2")
        merged, _ = failover._merge_stream_candidates([failed], 0, [refreshed], keep_active=False)
        self.assertNotIn("failed_over_at", merged[0])


# --- session: playlists without .m3u8, .txt segments, no media sequence ------

PLAYLIST_URL = "https://buff.example.test/live/caxi-fhd"


def _txt_playlist(first: int, count: int, token: str, media_sequence: bool = False) -> str:
    lines = ["#EXTM3U", "#EXT-X-VERSION:3", "#EXT-X-TARGETDURATION:6"]
    if media_sequence:
        lines.append("#EXT-X-MEDIA-SEQUENCE:0")
    for n in range(first, first + count):
        lines.append("#EXTINF:6.000,")
        lines.append(f"https://buff.example.test/video-5segment_.txt?path=seg{n}&token={token}")
    return "\n".join(lines) + "\n"


class SessionFreshnessTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.harness = Harness()
        self.harness.source = SourceSpec(key=("mybuff",), url=PLAYLIST_URL, label="MyBuffStreams")

    def _serve(self, first: int, count: int, token: str, media_sequence: bool = False) -> None:
        self.harness.set_playlist(PLAYLIST_URL, _txt_playlist(first, count, token, media_sequence))
        for n in range(first, first + count):
            self.harness.set_response(
                f"https://buff.example.test/video-5segment_.txt?path=seg{n}&token={token}",
                make_ts_segment(pts=90000 * (n + 1)),
            )

    async def test_sliding_window_without_media_sequence_keeps_playing(self):
        for media_sequence in (False, True):  # missing, and present but stuck at 0
            with self.subTest(media_sequence=media_sequence):
                self.harness.failures.clear()
                session = ChannelSession("nfl_sf", self.harness.hooks(),
                                         fast_config(stale_min_seconds=60.0, window_max_segments=10))
                self._serve(0, 5, "a", media_sequence)
                await session._poll_once()
                self.assertEqual(session.stats["segments"], 3)
                # Same window, every URI re-signed: nothing new, nothing replayed.
                self._serve(0, 5, "b", media_sequence)
                await session._poll_once()
                self.assertEqual(session.stats["segments"], 3)
                # The window slid by two segments (and was re-signed again).
                self._serve(2, 5, "c", media_sequence)
                await session._poll_once()
                self.assertEqual(session.stats["segments"], 5)
                self.assertEqual(self.harness.failures, [])

    async def test_resigned_but_frozen_playlist_is_still_stale(self):
        session = ChannelSession("nfl_sf", self.harness.hooks(), fast_config(stale_min_seconds=0.05))
        self._serve(0, 5, "a")
        await session._poll_once()
        # Longer ago than the stale window (3 x the 6 s target duration).
        session.last_new_segment_at -= 60.0
        session._stale_timer_start -= 60.0
        self._serve(0, 5, "b")
        await session._poll_once()
        self.assertEqual(session.stats["segments"], 3)
        self.assertEqual([f[2] for f in self.harness.failures], ["playlist stale"])

    async def test_html_answer_is_reported_as_not_hls(self):
        self.harness.set_response(PLAYLIST_URL, b"<!doctype html><html><body>player</body></html>",
                                  content_type="text/html")
        session = ChannelSession("nfl_sf", self.harness.hooks(), fast_config())
        await session._poll_once()
        self.assertEqual([f[2] for f in self.harness.failures], ["playlist not HLS"])


class SegmentIdentityTests(unittest.TestCase):
    def test_path_parameter_names_the_segment_and_signatures_do_not(self):
        a1 = "https://edge1.example.test/video-5segment_.txt?path=seg1&token=x&expires=1"
        a2 = "https://edge2.example.test/video-5segment_.txt?path=seg1&token=y&expires=2"
        b = "https://edge1.example.test/video-5segment_.txt?path=seg2&token=x&expires=1"
        self.assertEqual(segment_identity(a1), segment_identity(a2))
        self.assertNotEqual(segment_identity(a1), segment_identity(b))


class RedirectingSegmentTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        clear_dns_cache()
        self._dns = patch.object(socket, "getaddrinfo", side_effect=_public_dns)
        self._dns.start()

    async def asyncTearDown(self):
        self._dns.stop()
        clear_dns_cache()

    async def test_txt_segment_redirect_to_signed_ts_is_followed(self):
        ts = make_ts_segment()
        seen: List[str] = []

        def handler(request):
            seen.append(str(request.url))
            if request.url.path.endswith(".txt"):
                return httpx.Response(302, headers={"location": "https://seg.example.test/s/seg1.ts?sig=abc"})
            return httpx.Response(200, headers={"content-type": "video/mp2t"}, content=ts)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await _fetch_upstream_body(
                client, "https://buff.example.test/video-5segment_.txt?path=seg1", {}, 1 << 20)
        self.assertIsNotNone(result)
        status, effective_url, _, body, _ = result
        self.assertEqual((status, body), (200, ts))
        self.assertTrue(effective_url.startswith("https://seg.example.test/s/seg1.ts"))

    async def test_redirect_into_a_private_address_is_refused(self):
        def handler(request):
            return httpx.Response(302, headers={"location": "http://127.0.0.1/seg1.ts"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await _fetch_upstream_body(
                client, "https://buff.example.test/video-5segment_.txt?path=seg1", {}, 1 << 20)
        self.assertIsNone(result)


class HealthProbeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        clear_dns_cache()
        self._dns = patch.object(socket, "getaddrinfo", side_effect=_public_dns)
        self._dns.start()

    async def asyncTearDown(self):
        self._dns.stop()
        clear_dns_cache()

    async def test_extensionless_playlist_endpoint_is_read_not_passed_on_head(self):
        # The probe used to accept any non-playlist content type on HEAD, so an
        # endpoint answering an HTML page "recovered" a dead channel.
        def handler(request):
            if request.method == "HEAD":
                return httpx.Response(200, headers={"content-type": "text/html"})
            return httpx.Response(200, headers={"content-type": "text/html"}, text="<html>blocked</html>")

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            self.assertFalse(await verify_stream_live(client, PLAYLIST_URL))

    async def test_probe_freshness_follows_segment_identity(self):
        bodies = {"n": 0}
        ts = make_ts_segment()

        def handler(request):
            path = request.url.path
            if path.endswith("caxi-fhd"):
                if request.method == "HEAD":
                    return httpx.Response(200, headers={"content-type": "application/vnd.apple.mpegurl"})
                return httpx.Response(200, text=bodies["text"],
                                      headers={"content-type": "application/vnd.apple.mpegurl"})
            if path.endswith(".txt"):
                return httpx.Response(302, headers={"location": "https://seg.example.test/s/x.ts?sig=1"})
            return httpx.Response(200, headers={"content-type": "video/mp2t"}, content=ts)

        base = time.monotonic()
        clock = {"t": base}

        async def probe(text, at, probe_state):
            bodies["text"] = text
            clock["t"] = base + at
            with patch("stream_extractor.time.monotonic", side_effect=lambda: clock["t"]):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    return await verify_stream_live(client, PLAYLIST_URL, probe_state=probe_state)

        # Frozen window, re-signed on every request: stale after the threshold.
        frozen: dict = {}
        self.assertTrue(await probe(_txt_playlist(0, 3, "a"), 0, frozen))
        self.assertFalse(await probe(_txt_playlist(0, 3, "b"), 30, frozen))
        # Sliding window with no media sequence: fresh.
        sliding: dict = {}
        self.assertTrue(await probe(_txt_playlist(0, 3, "a"), 0, sliding))
        self.assertTrue(await probe(_txt_playlist(5, 3, "b"), 30, sliding))


# ---------------------------------------------------------------------------
# (3) A team channel never takes an unrelated linear channel
# ---------------------------------------------------------------------------

class LinearOnlyProviderTests(unittest.IsolatedAsyncioTestCase):
    def test_team_searches_skip_iptv_org(self):
        team = [p.name for p in scrapers._providers_for_search(always_live=False)]
        linear = [p.name for p in scrapers._providers_for_search(always_live=True)]
        self.assertNotIn("IPTV-Org", team)
        self.assertIn("IPTV-Org", linear)
        self.assertIn("iSportSurge", team)

    async def test_packers_never_get_a_snooker_channel(self):
        iptv = next(p for p in scrapers.ACTIVE_PROVIDERS if p.name == "IPTV-Org")
        snooker = {"tvg_name": "Pluto TV Snooker (GB)", "display_name": "Pluto TV Snooker (GB)",
                   "url": "https://linear.example.test/snooker/index.m3u8"}
        with patch.object(scrapers, "_IPTV_ORG_CACHE", [snooker]), \
                patch.object(scrapers, "_IPTV_ORG_CACHE_LOADED_AT", time.monotonic()), \
                patch("plugins.builtin.iptvorg.provider._verify_provider_streams",
                      new=AsyncMock(side_effect=lambda streams, client: streams)):
            # Why it happened: the curated list matches the "gb" alias...
            self.assertTrue(await iptv.search(PACKERS))
            # ...so team channels no longer search it at all.
            providers = scrapers._providers_for_search(always_live=False)
        self.assertNotIn(iptv, providers)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
