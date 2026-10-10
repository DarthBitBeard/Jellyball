"""Unit tests for Workstream 2 (quality-ranked streams).

Covers:
  - extract_stream_quality: variant parsing from master playlist text
    (multi-variant, single-variant, lying BANDWIDTH, no variants).
  - rank_streams: 1080p beats 720p beats unknown; match_score still dominates;
    provider_rank breaks bandwidth ties within a tier.
  - verify_stream_live: the enrich out-param is filled from the top-level
    master playlist only; non-master URLs leave it empty; the boolean return
    contract is unchanged for callers that ignore enrich.
  - _merge_stream_candidates: quality is refreshed on rescrape, never
    preserved from stale data.
  - Dashboard: the candidate label carries the quality badge.
"""

import socket
import unittest
from unittest.mock import patch

import httpx

from network_safety import clear_dns_cache
from stream_extractor import (
    extract_stream_quality,
    quality_badge,
    rank_streams,
    verify_stream_live,
    _quality_tier,
)
import failover
from failover import _merge_stream_candidates, _CANDIDATE_HEALTH_FIELDS

_MPEGURL = "application/vnd.apple.mpegurl"


def _fake_public_getaddrinfo(host, port, *args, **kwargs):
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", port))]


def _cand(provider, url, **extra):
    return {"provider": provider, "url": url, "match_score": 100, **extra}


def _quality(height, bandwidth=1000000, variants=2):
    return {
        "max_bandwidth": bandwidth,
        "max_resolution": f"Xx{height}",
        "max_height": height,
        "variant_count": variants,
    }


class ExtractQualityTests(unittest.TestCase):
    def test_multi_variant_picks_best_resolution_and_max_bandwidth(self):
        text = (
            "#EXTM3U\n"
            '#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=640x360\n'
            "v360.m3u8\n"
            '#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080\n'
            "v1080/index.m3u8\n"
        )
        quality = extract_stream_quality(text, "https://cdn.example/master.m3u8")
        self.assertEqual(quality["max_height"], 1080)
        self.assertEqual(quality["max_resolution"], "1920x1080")
        self.assertEqual(quality["max_bandwidth"], 5000000)
        self.assertEqual(quality["variant_count"], 2)

    def test_single_variant(self):
        text = (
            "#EXTM3U\n"
            '#EXT-X-STREAM-INF:BANDWIDTH=2500000,RESOLUTION=1280x720\n'
            "v720.m3u8\n"
        )
        quality = extract_stream_quality(text, "https://cdn.example/master.m3u8")
        self.assertEqual(quality["max_height"], 720)
        self.assertEqual(quality["max_bandwidth"], 2500000)
        self.assertEqual(quality["variant_count"], 1)

    def test_lying_bandwidth_does_not_change_tier_inputs(self):
        # A provider declaring 1080p bandwidth on a 480p rendition: the height
        # still comes from RESOLUTION, so tiers stay honest.
        text = (
            "#EXTM3U\n"
            '#EXT-X-STREAM-INF:BANDWIDTH=9000000,RESOLUTION=854x480\n'
            "v.m3u8\n"
        )
        quality = extract_stream_quality(text, "https://cdn.example/master.m3u8")
        self.assertEqual(quality["max_height"], 480)
        self.assertEqual(quality["max_bandwidth"], 9000000)

    def test_no_variants_returns_empty(self):
        self.assertEqual(extract_stream_quality("#EXTM3U\n", "https://cdn.example/x.m3u8"), {})
        self.assertEqual(extract_stream_quality("not a playlist", "https://cdn.example/x.m3u8"), {})

    def test_tier_boundaries(self):
        self.assertEqual(_quality_tier({"quality": _quality(1080)}), 3)
        self.assertEqual(_quality_tier({"quality": _quality(2160)}), 3)
        self.assertEqual(_quality_tier({"quality": _quality(720)}), 2)
        self.assertEqual(_quality_tier({"quality": _quality(480)}), 1)
        self.assertEqual(_quality_tier({"quality": _quality(0)}), 0)
        self.assertEqual(_quality_tier({}), 0)
        self.assertEqual(_quality_tier({"quality": None}), 0)
        self.assertEqual(_quality_tier({"quality": {"max_height": "1080"}}), 3)

    def test_badge(self):
        self.assertEqual(quality_badge({"quality": _quality(1080)}), "1080p")
        self.assertEqual(quality_badge({"quality": _quality(720)}), "720p")
        self.assertEqual(quality_badge({}), "")
        self.assertEqual(quality_badge({"quality": {"max_height": "junk"}}), "")


class RankStreamsQualityTests(unittest.TestCase):
    def test_1080p_beats_720p_beats_unknown(self):
        streams = [
            _cand("A", "https://a.example/1.m3u8"),
            _cand("B", "https://b.example/1.m3u8", quality=_quality(720)),
            _cand("C", "https://c.example/1.m3u8", quality=_quality(1080)),
        ]
        ranked = rank_streams(streams)
        self.assertEqual([s["provider"] for s in ranked], ["C", "B", "A"])

    def test_match_score_still_dominates(self):
        streams = [
            _cand("A", "https://a.example/1.m3u8", match_score=50, quality=_quality(1080)),
            _cand("B", "https://b.example/1.m3u8", match_score=100),
        ]
        ranked = rank_streams(streams)
        self.assertEqual(ranked[0]["provider"], "B")

    def test_provider_rank_breaks_bandwidth_ties_within_a_tier(self):
        streams = [
            _cand("A", "https://a.example/1.m3u8", quality=_quality(1080, bandwidth=3000000)),
            _cand("B", "https://b.example/1.m3u8", quality=_quality(1080, bandwidth=8000000)),
        ]
        # Higher bandwidth wins within the tier...
        ranked = rank_streams(streams, {"A": 1, "B": 1})
        self.assertEqual(ranked[0]["provider"], "B")
        # ...but provider priority breaks equal-bandwidth ties.
        tied = [
            _cand("A", "https://a.example/1.m3u8", quality=_quality(1080, bandwidth=5000000)),
            _cand("B", "https://b.example/1.m3u8", quality=_quality(1080, bandwidth=5000000)),
        ]
        ranked = rank_streams(tied, {"A": 2, "B": 1})
        self.assertEqual(ranked[0]["provider"], "B")

    def test_unknown_quality_sorts_as_before(self):
        # No quality anywhere: provider priority then URL, the old behavior.
        streams = [_cand("B", "https://b.example/1.m3u8"), _cand("A", "https://a.example/1.m3u8")]
        ranked = rank_streams(streams, {"A": 1, "B": 2})
        self.assertEqual([s["provider"] for s in ranked], ["A", "B"])

    def test_tier_beats_raw_bandwidth(self):
        # 720p at high bitrate does not outrank 1080p at lower bitrate.
        streams = [
            _cand("A", "https://a.example/1.m3u8", quality=_quality(720, bandwidth=12000000)),
            _cand("B", "https://b.example/1.m3u8", quality=_quality(1080, bandwidth=4000000)),
        ]
        ranked = rank_streams(streams, {"A": 1, "B": 1})
        self.assertEqual(ranked[0]["provider"], "B")


_MASTER_MULTI = (
    "#EXTM3U\n"
    '#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=640x360\n'
    "v360.m3u8\n"
    '#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080\n'
    "v1080.m3u8\n"
)

_MEDIA_PLAYLIST = (
    "#EXTM3U\n"
    "#EXT-X-TARGETDURATION:6\n"
    "#EXT-X-MEDIA-SEQUENCE:42\n"
    "#EXTINF:6,\n"
    "seg42.ts\n"
)


def _verify_handler(request):
    path = request.url.path
    if path.endswith("master.m3u8"):
        return httpx.Response(200, headers={"content-type": _MPEGURL}, text=_MASTER_MULTI, request=request)
    if path.endswith(".m3u8"):
        return httpx.Response(200, headers={"content-type": _MPEGURL}, text=_MEDIA_PLAYLIST, request=request)
    if path.endswith(".ts"):
        return httpx.Response(200, headers={"content-type": "video/mp2t"}, content=b"\x47" + b"\x00" * 200, request=request)
    return httpx.Response(404, request=request)


class VerifyEnrichmentTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        clear_dns_cache()
        self._dns_patcher = patch.object(socket, "getaddrinfo", side_effect=_fake_public_getaddrinfo)
        self._dns_patcher.start()

    async def asyncTearDown(self):
        self._dns_patcher.stop()
        clear_dns_cache()

    async def test_master_playlist_enriches_quality(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(_verify_handler)) as client:
            enrich: dict = {}
            live = await verify_stream_live(
                client, "https://media.example.test/master.m3u8", enrich=enrich
            )
        self.assertTrue(live)
        self.assertEqual(enrich["max_height"], 1080)
        self.assertEqual(enrich["max_bandwidth"], 5000000)
        self.assertEqual(enrich["variant_count"], 2)
        self.assertEqual(enrich["max_resolution"], "1920x1080")

    async def test_non_master_url_leaves_enrich_empty(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(_verify_handler)) as client:
            enrich: dict = {}
            live = await verify_stream_live(
                client, "https://media.example.test/v1080.m3u8", enrich=enrich
            )
        self.assertTrue(live)
        self.assertEqual(enrich, {})

    async def test_boolean_contract_unchanged_without_enrich(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(_verify_handler)) as client:
            live = await verify_stream_live(client, "https://media.example.test/master.m3u8")
        self.assertTrue(live)

    async def test_enrichment_only_reads_top_level_master(self):
        # A variant playlist that is itself a master (depth 1) must not
        # overwrite the top-level enrichment: the depth guard keeps the
        # depth-0 data even though the nested chain fails the depth<=2 check.
        nested_master = (
            "#EXTM3U\n"
            '#EXT-X-STREAM-INF:BANDWIDTH=100000,RESOLUTION=320x180\n'
            "tiny.m3u8\n"
        )

        def handler(request):
            path = request.url.path
            if path.endswith("top.m3u8"):
                return httpx.Response(200, headers={"content-type": _MPEGURL}, text=_MASTER_MULTI, request=request)
            if path.endswith("v360.m3u8"):
                return httpx.Response(200, headers={"content-type": _MPEGURL}, text=nested_master, request=request)
            return httpx.Response(404, request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            enrich: dict = {}
            live = await verify_stream_live(client, "https://media.example.test/top.m3u8", enrich=enrich)
        self.assertFalse(live)
        self.assertEqual(enrich.get("max_height"), 1080)


class MergeQualityTests(unittest.TestCase):
    def test_active_candidate_quality_is_refreshed(self):
        previous = [_cand("P", "https://cdn.example/live.m3u8", quality=_quality(720, bandwidth=2000000))]
        fresh = [_cand("P", "https://cdn.example/live.m3u8", quality=_quality(1080, bandwidth=6000000))]
        merged, _ = _merge_stream_candidates(previous, 0, fresh, keep_active=True)
        self.assertEqual(merged[0]["quality"]["max_height"], 1080)
        self.assertEqual(merged[0]["quality"]["max_bandwidth"], 6000000)

    def test_quality_is_not_a_preserved_health_field(self):
        self.assertNotIn("quality", _CANDIDATE_HEALTH_FIELDS)
        # A fresh candidate without quality data does not inherit the old
        # candidate's stale quality.
        old = _cand("P", "https://cdn.example/a.m3u8", quality=_quality(1080))
        fresh = [_cand("P", "https://cdn.example/b.m3u8")]
        merged, _ = _merge_stream_candidates([old], 0, fresh, keep_active=False)
        self.assertNotIn("quality", merged[0])

    def test_active_keeps_old_quality_when_fresh_has_none(self):
        previous = [_cand("P", "https://cdn.example/live.m3u8", quality=_quality(720))]
        fresh = [_cand("P", "https://cdn.example/live.m3u8")]
        merged, _ = _merge_stream_candidates(previous, 0, fresh, keep_active=True)
        self.assertEqual(merged[0]["quality"]["max_height"], 720)


class CandidateLabelTests(unittest.TestCase):
    def test_label_contains_quality_badge(self):
        from routes_dashboard import _candidate_option_label

        cand = {"provider": "ESPN", "match_title": "ESPN", "quality": _quality(1080)}
        label = _candidate_option_label(cand, 0, 0)["label"]
        self.assertIn("1080p", label)
        self.assertTrue(label.startswith("★ "))

    def test_label_without_quality_has_no_badge(self):
        from routes_dashboard import _candidate_option_label

        cand = {"provider": "ESPN", "match_title": "ESPN"}
        label = _candidate_option_label(cand, 1, 0)["label"]
        self.assertNotIn("·", label)
        self.assertFalse(label.startswith("★ "))


if __name__ == "__main__":
    unittest.main()
