import asyncio
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))

from catalog import (
    _catalog_display_name,
    _channel_group_title,
    _channel_logo_url,
    _channel_tvg_id,
    _channel_is_always_live,
    _team_catalog_entry,
)
from main import generate_m3u, generate_xmltv
from sports_catalog import SPECIAL_CHANNELS, STATIC_TEAM_RECORDS
import state


class ChannelMetadataTests(unittest.TestCase):
    def test_always_live_channels_have_guide_ids_and_logos(self):
        self.assertEqual(len(SPECIAL_CHANNELS), 25)
        self.assertTrue(all(channel.tvg_id for channel in SPECIAL_CHANNELS))
        self.assertTrue(all(channel.logo_url.startswith("https://") for channel in SPECIAL_CHANNELS))
        self.assertEqual({channel.group_title for channel in SPECIAL_CHANNELS}, {"24/7 Sports"})
        self.assertIn("Big Ten Network", {channel.name for channel in SPECIAL_CHANNELS})
        self.assertIn("ACC Network", {channel.name for channel in SPECIAL_CHANNELS})
        self.assertIn("SEC Network", {channel.name for channel in SPECIAL_CHANNELS})
        self.assertIn("ABC", {channel.name for channel in SPECIAL_CHANNELS})
        self.assertIn("FOX", {channel.name for channel in SPECIAL_CHANNELS})
        self.assertIn("CBS", {channel.name for channel in SPECIAL_CHANNELS})
        self.assertIn("NBC", {channel.name for channel in SPECIAL_CHANNELS})
        self.assertIn("The CW", {channel.name for channel in SPECIAL_CHANNELS})
        self.assertIn("TBS", {channel.name for channel in SPECIAL_CHANNELS})
        self.assertIn("USA Network", {channel.name for channel in SPECIAL_CHANNELS})
        self.assertIn("TruTV", {channel.name for channel in SPECIAL_CHANNELS})

    def test_network_logos_use_reliable_tv_logos_source(self):
        logos = {channel.name: channel.logo_url for channel in SPECIAL_CHANNELS}
        tv_logos_base = "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/united-states/"
        self.assertEqual(logos["Big Ten Network"], tv_logos_base + "big-ten-network-us.png")
        self.assertEqual(logos["SEC Network"], tv_logos_base + "sec-network-us.png")
        self.assertEqual(logos["NFL Network"], tv_logos_base + "nfl-network-us.png")
        self.assertTrue(all(logo.startswith(tv_logos_base) for logo in logos.values()))

    def test_college_catalog_entries_are_sport_specific(self):
        michigan = next(record for record in STATIC_TEAM_RECORDS if record.canonical == "michigan wolverines")
        football = _team_catalog_entry("ncaaf", michigan.for_category("ncaaf"))
        basketball = _team_catalog_entry("ncaam", michigan.for_category("ncaam"))

        self.assertEqual(_catalog_display_name("ncaaf", michigan.for_category("ncaaf")), "Michigan Wolverines (Football)")
        self.assertEqual(_catalog_display_name("ncaam", michigan.for_category("ncaam")), "Michigan Wolverines (Men's Basketball)")
        self.assertEqual(football["team_id"], "ncaaf_130")
        self.assertEqual(basketball["team_id"], "ncaam_130")
        self.assertNotEqual(football["name"], basketball["name"])

    def test_m3u_contains_special_channel_branding(self):
        import main

        original_state = dict(state.stream_state)
        try:
            state.stream_state.clear()
            state.stream_state["special_espn"] = {
                "name": "ESPN",
                "catalog_key": "special:espn",
                "category": "special",
                "logo_url": "",
                "candidates": [],
            }
            request = SimpleNamespace(headers={"host": "127.0.0.1:8000"})
            playlist = asyncio.run(generate_m3u(request))
        finally:
            state.stream_state.clear()
            state.stream_state.update(original_state)

        self.assertIn('tvg-id="ESPN.us"', playlist)
        self.assertIn('tvg-name="ESPN"', playlist)
        self.assertIn('group-title="24/7 Sports"', playlist)
        self.assertIn('tvg-logo="https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/united-states/espn-us.png"', playlist)
        self.assertIn("http://127.0.0.1:8000/stream/special_espn", playlist)

    def test_xmltv_uses_same_special_channel_id_and_logo(self):
        import main

        original_state = dict(state.stream_state)
        try:
            state.stream_state.clear()
            state.stream_state["special_espn"] = {
                "name": "ESPN",
                "catalog_key": "special:espn",
                "category": "special",
                "logo_url": "",
                "always_live": True,
            }
            guide = asyncio.run(generate_xmltv())
        finally:
            state.stream_state.clear()
            state.stream_state.update(original_state)

        self.assertIn('<channel id="ESPN.us">', guide)
        self.assertIn('<programme channel="ESPN.us"', guide)
        self.assertIn('<icon src="https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/united-states/espn-us.png" />', guide)

    def test_channel_helpers_fall_back_to_team_metadata(self):
        data = {"category": "ncaaf", "name": "Michigan Wolverines (Football)", "logo_url": "logo"}
        self.assertEqual(_channel_tvg_id("ncaaf_130", data), "ncaaf_130")
        self.assertEqual(_channel_group_title(data), "College Football")
        self.assertEqual(_channel_logo_url(data), "logo")

    def test_legacy_special_channel_name_restores_branding(self):
        data = {"name": "Big Ten Network", "query": "big ten network", "logo_url": ""}
        self.assertEqual(_channel_tvg_id("special_big_ten_network", data), "BigTenNetwork.us")
        self.assertEqual(_channel_group_title(data), "24/7 Sports")
        self.assertTrue(_channel_logo_url(data).startswith("https://"))
        self.assertTrue(_channel_is_always_live(data))


if __name__ == "__main__":
    unittest.main()
