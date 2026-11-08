"""Channels & Streams organisation: sections, status-filter states, and what
the dashboard page and /api/status expose for them."""

import unittest
from datetime import datetime, timedelta, timezone

import channel_groups
import state
from channel_groups import channel_group, channel_status, group_channels
from state import new_channel_state
from test_http_surface import _request

NOW = datetime(2026, 11, 6, 12, 0, tzinfo=timezone.utc)
FMT = "%Y%m%d%H%M%S +0000"


def _window(start_hours: float, stop_hours: float) -> dict:
    return {
        "schedule_status": "scheduled",
        "start_time": (NOW + timedelta(hours=start_hours)).strftime(FMT),
        "stop_time": (NOW + timedelta(hours=stop_hours)).strftime(FMT),
    }


class GroupTests(unittest.TestCase):
    def test_sections(self):
        self.assertEqual(channel_group({"category": "nba", "content_type": "team"}), ("nba", "NBA"))
        self.assertEqual(channel_group({"category": "NFL"}), ("nfl", "NFL"))
        self.assertEqual(channel_group({"category": "special", "content_type": "channel", "always_live": True}), channel_groups.SPECIAL_GROUP)
        self.assertEqual(channel_group({"category": "special", "always_live": True}), channel_groups.SPECIAL_GROUP)
        self.assertEqual(channel_group({"category": "custom"}), channel_groups.CUSTOM_GROUP)
        self.assertEqual(channel_group({}), channel_groups.CUSTOM_GROUP)

    def test_sections_follow_catalog_order_and_skip_empty_ones(self):
        channels = [
            {"name": "Always", "group_key": "special"},
            {"name": "Lakers", "group_key": "nba"},
            {"name": "Mine", "group_key": "custom"},
            {"name": "Bills", "group_key": "nfl"},
            {"name": "Celtics", "group_key": "nba"},
        ]
        groups = group_channels(channels)
        self.assertEqual([g["key"] for g in groups], ["nfl", "nba", "special", "custom"])
        self.assertEqual([c["name"] for c in groups[1]["channels"]], ["Lakers", "Celtics"])


class StatusTests(unittest.TestCase):
    def test_states(self):
        cases = {
            "healthy": ({"is_healthy": True, **_window(5, 8)}, "live"),
            "healthy always-live": ({"is_healthy": True, "always_live": True}, "live"),
            "always-live without a stream": ({"always_live": True, "content_type": "channel"}, "no_stream"),
            "event on, no stream": (_window(-1, 2), "no_stream"),
            "event later today": (_window(6, 9), "upcoming"),
            "no event": ({"schedule_status": "no_event"}, "upcoming"),
            "schedule lookup failed": ({"schedule_status": "lookup_failed"}, "no_stream"),
            "off-season": ({"schedule_status": "off_season", "category": "nfl"}, "off_season"),
        }
        for name, (data, expected) in cases.items():
            with self.subTest(name):
                self.assertEqual(channel_status(data, NOW), expected)
                self.assertIn(expected, channel_groups.STATUSES)


class PageTests(unittest.TestCase):
    def setUp(self):
        self._backup = dict(state.stream_state)
        state.stream_state.clear()
        state.stream_state.update({
            "nba_lal": new_channel_state(name="Los Angeles Lakers", query="Los Angeles Lakers", category="nba", source_id="lal"),
            "nfl_buf": new_channel_state(name="Buffalo Bills", query="Buffalo Bills", category="nfl", source_id="buf"),
            "special_espn": new_channel_state(name="ESPN", query="ESPN", category="special", content_type="channel", always_live=True),
        })
        state.stream_state["special_espn"]["is_healthy"] = True

    def tearDown(self):
        state.stream_state.clear()
        state.stream_state.update(self._backup)

    def test_channels_are_listed_in_sections_with_their_state(self):
        html = _request("GET", "/").text
        sections = [html.index(f'data-group-key="{key}"') for key in ("nfl", "nba", "special")]
        self.assertEqual(sections, sorted(sections))
        self.assertNotIn('data-group-key="custom"', html)
        self.assertIn('data-status-filter="no_stream"', html)
        espn = html[html.index('data-team-id="special_espn"'):]
        self.assertIn('data-status="live"', espn[:espn.index(">")])

    def test_status_api_reports_state_and_section(self):
        channels = {
            c["team_id"]: c
            for c in _request("GET", "/api/status", password="secret", auth=("admin", "secret")).json()["channels"]
        }
        self.assertEqual((channels["special_espn"]["status_group"], channels["special_espn"]["group_key"]), ("live", "special"))
        self.assertEqual(channels["nba_lal"]["group_key"], "nba")
        self.assertIn(channels["nba_lal"]["status_group"], channel_groups.STATUSES)


if __name__ == "__main__":
    unittest.main()
