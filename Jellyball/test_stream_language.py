"""3.1.0: stream language preference and network channels from iptv-org's
language-grouped index."""

import asyncio
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

import httpx

import scrapers
import stream_language
from plugins.builtin.iptvorg.provider import IptvOrgScraper, _entry_rank
from stream_extractor import rank_streams

INDEX = """#EXTM3U
#EXTINF:-1 tvg-id="ESPN.us@SD" group-title="English",ESPN (United States) (432p)
https://us.example.test/espn/index.m3u8
#EXTINF:-1 tvg-id="ESPNLatino.mx@HD" group-title="Spanish",ESPN Latin America Mexico (1080p)
https://mx.example.test/espn/index.m3u8
#EXTINF:-1 tvg-id="ESPNBrasil.br@HD" group-title="Portuguese",ESPN (Brazil) (1080p)
https://br.example.test/espn/index.m3u8
#EXTINF:-1 tvg-id="NBCComedy.us@SD" group-title="English",NBC Comedy Vault (1080p)
https://us.example.test/nbc-comedy/index.m3u8
#EXTINF:-1 tvg-id="KNTV.us@SD" group-title="English",NBC KNTV (1080p)
https://us.example.test/kntv/index.m3u8
#EXTINF:-1 tvg-id="KNTV.us@SD" group-title="Spanish",NBC KNTV (1080p)
https://us.example.test/kntv/index.m3u8
#EXTINF:-1 tvg-id="WXYZ.us@SD" group-title="English",NBC WXYZ (720p) [Not 24/7]
https://us.example.test/wxyz/index.m3u8
"""


class PreferenceMixin:
    def prefs(self, languages, mode="prefer"):
        stream_language._cache.update(languages=list(languages), mode=mode, at=10**12)

    def tearDown(self):
        stream_language.invalidate_cache()


class DetectionTests(unittest.TestCase):
    def test_title_markers(self):
        cases = {
            "ESPN Latin America Mexico (1080p)": "spa", "ESPN Deportes": "spa", "ESPN Latino": "spa",
            "ESPN (Brazil) (1080p)": "por", "ESPN 2 (Netherlands) (360p)": "nld", "Fox (Poland)": "pol",
            "ESPN (United States) HD (720p)": "eng", "ABC USA": "eng", "Sky Sports Main Event UK": "eng",
            "ESPN Premium HD (1080p)": None, "NBC KNTV (1080p)": None,
        }
        for title, code in cases.items():
            with self.subTest(title=title):
                self.assertEqual(stream_language.detect_language(title), code)

    def test_codes_and_names(self):
        self.assertEqual(stream_language.code_for("English"), "eng")
        self.assertEqual(stream_language.code_for("spa"), "spa")
        self.assertIsNone(stream_language.code_for("Klingon"))
        self.assertEqual(stream_language.parse_languages("English, spa;  nope eng"), ["eng", "spa"])

    def test_source_tag_wins_over_title(self):
        stream = {"language": ["spa", "eng"], "match_title": "Something"}
        with patch.object(stream_language, "preferred_languages", return_value=["eng"]):
            self.assertEqual(stream_language.stream_language(stream), "eng")


class RankingTests(PreferenceMixin, unittest.TestCase):
    streams = [
        {"url": "https://a.example.test/1.m3u8", "match_score": 100, "match_title": "ESPN Latin America Mexico (1080p)", "provider": "IPTV-Org"},
        {"url": "https://a.example.test/2.m3u8", "match_score": 100, "match_title": "ESPN (United States) (432p)", "provider": "IPTV-Org"},
        {"url": "https://a.example.test/3.m3u8", "match_score": 100, "match_title": "ESPN Premium HD", "provider": "IPTV-Org"},
    ]

    def test_english_first_then_unknown_then_other_languages(self):
        self.prefs(["eng"])
        titles = [s["match_title"] for s in rank_streams(self.streams)]
        self.assertEqual(titles, ["ESPN (United States) (432p)", "ESPN Premium HD", "ESPN Latin America Mexico (1080p)"])

    def test_spanish_viewer_gets_spanish_first(self):
        self.prefs(["spa", "eng"])
        self.assertEqual(rank_streams(self.streams)[0]["match_title"], "ESPN Latin America Mexico (1080p)")

    def test_match_quality_still_comes_first(self):
        self.prefs(["eng"])
        streams = [dict(self.streams[1], match_score=60), self.streams[0]]
        self.assertEqual(rank_streams(streams)[0]["match_title"], "ESPN Latin America Mexico (1080p)")

    def test_only_mode_excludes_known_other_languages_but_keeps_unknown(self):
        self.prefs(["eng"], "only")
        kept = [s["match_title"] for s in self.streams if not stream_language.is_excluded(s)]
        self.assertEqual(kept, ["ESPN (United States) (432p)", "ESPN Premium HD"])
        self.prefs(["eng"], "prefer")
        self.assertFalse(any(stream_language.is_excluded(s) for s in self.streams))

    def test_listing_filter(self):
        old_check = scrapers._is_non_english_channel
        self.prefs(["eng"])
        self.assertFalse(stream_language.listing_allowed("ESPN Deportes", old_check))
        self.assertFalse(stream_language.listing_allowed("beIN Sports Canada", old_check))  # old region rule kept
        self.assertTrue(stream_language.listing_allowed("ESPN USA", old_check))
        self.prefs(["spa"])
        self.assertTrue(stream_language.listing_allowed("ESPN Deportes", old_check))
        self.assertFalse(stream_language.listing_allowed("ESPN USA", old_check))


class ApplyToChannelTests(PreferenceMixin, unittest.TestCase):
    def channel(self):
        return {
            "candidates": [
                {"url": "https://a.example.test/mx", "match_title": "ESPN Latin America Mexico"},
                {"url": "https://a.example.test/us", "match_title": "ESPN (United States)"},
                {"url": "https://a.example.test/x", "match_title": "ESPN Premium HD"},
            ],
            "active_index": 2,
        }

    def test_only_mode_drops_other_languages_and_keeps_the_playing_stream_selected(self):
        self.prefs(["eng"], "only")
        data = self.channel()
        self.assertEqual(stream_language.apply_to_channel(data), 1)
        self.assertEqual([c["url"][-2:] for c in data["candidates"]], ["us", "/x"])
        self.assertEqual(data["candidates"][data["active_index"]]["url"], "https://a.example.test/x")

    def test_prefer_mode_reorders_without_dropping(self):
        self.prefs(["eng"])
        data = self.channel()
        self.assertEqual(stream_language.apply_to_channel(data), 0)
        self.assertEqual([c["url"][-2:] for c in data["candidates"]], ["us", "/x", "mx"])

    def test_a_dropped_active_stream_resets_to_the_best(self):
        self.prefs(["eng"], "only")
        data = self.channel()
        data["active_index"] = 0
        stream_language.apply_to_channel(data)
        self.assertEqual(data["active_index"], 0)
        self.assertEqual(data["candidates"][0]["url"], "https://a.example.test/us")


class IptvOrgTests(PreferenceMixin, unittest.TestCase):
    def setUp(self):
        self._patches = [
            patch.object(scrapers, "_IPTV_ORG_CACHE", scrapers._parse_m3u_playlist(INDEX)),
            patch.object(scrapers, "_IPTV_ORG_CACHE_LOADED_AT", 10**12),
        ]
        for p in self._patches:
            p.start()

    def tearDown(self):
        for p in self._patches:
            p.stop()
        super().tearDown()

    def search(self, terms):
        async def accept_all(candidates, client):
            return candidates

        async def run():
            async with httpx.AsyncClient() as client:
                return await IptvOrgScraper().search(terms, http_client=client)

        with patch("plugins.builtin.iptvorg.provider._verify_provider_streams", accept_all):
            return asyncio.run(run())

    def test_the_index_default_is_language_grouped(self):
        self.assertTrue(scrapers.IPTV_ORG_PLAYLIST_URL_DEFAULT.endswith("index.language.m3u"))

    def test_espn_english_viewer(self):
        self.prefs(["eng"])
        titles = [c["match_title"] for c in self.search(["espn"])]
        self.assertEqual(titles[0], "ESPN (United States) (432p)")
        self.assertEqual(set(titles), {"ESPN (United States) (432p)", "ESPN Latin America Mexico (1080p)", "ESPN (Brazil) (1080p)"})

    def test_espn_only_english(self):
        self.prefs(["eng"], "only")
        self.assertEqual([c["match_title"] for c in self.search(["espn"])], ["ESPN (United States) (432p)"])

    def test_network_affiliates_merged_and_spin_offs_excluded(self):
        self.prefs(["eng"])
        found = self.search(["nbc"])
        titles = [c["match_title"] for c in found]
        self.assertNotIn("NBC Comedy Vault (1080p)", titles)
        self.assertEqual(titles, ["NBC KNTV (1080p)", "NBC WXYZ (720p) [Not 24/7]"])  # around-the-clock first
        self.assertEqual(sorted(found[0]["language"]), ["eng", "spa"])  # listed under two languages, one candidate

    def test_verification_is_capped(self):
        self.prefs(["eng"])
        with patch("plugins.builtin.iptvorg.provider.IPTV_ORG_VERIFY_LIMIT", 1):
            self.assertEqual(len(self.search(["espn"])), 1)

    def test_rank_penalises_other_country_editions(self):
        us = {"match_title": "Fox (United States) KDFW (1080p)", "language": ["eng"]}
        pl = {"match_title": "Fox (Poland) (576i)", "language": ["eng"]}
        self.assertLess(_entry_rank(us, {}, ["eng"]), _entry_rank(pl, {}, ["eng"]))


class SettingsTests(PreferenceMixin, unittest.TestCase):
    def test_save_and_read_back(self):
        import db

        tmp = tempfile.mkdtemp()
        try:
            with patch.object(db, "DB_FILE", os.path.join(tmp, "lang.db")):
                db.init_db()
                saved = stream_language.save_preference(["spa", "", "eng", "spa"], "only")
                self.assertEqual(saved, {"languages": ["spa", "eng"], "mode": "only"})
                stream_language.invalidate_cache()
                self.assertEqual(stream_language.preferred_languages(), ["spa", "eng"])
                self.assertEqual(stream_language.language_mode(), "only")
                self.assertEqual(stream_language.save_preference([], "bogus")["mode"], "prefer")
                db.close_all_db_connections()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
