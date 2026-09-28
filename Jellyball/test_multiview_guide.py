"""Tests for the Multi-View XMLTV/M3U guide-generation helpers in main.py.

Covers: splitting/merging the composite guide window at member programme
boundaries, the "No Signal" placeholder for a removed member, pane-label/desc
formatting for both layouts, per-audio-channel programmes following their own
member, the tvg-id scheme used for per-audio channels, and that the rendered
XMLTV stays well-formed.
"""
import asyncio
import re
import sys
import unittest
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

import main
import epg
import state
from epg import (
    generate_m3u,
    generate_xmltv,
    _channel_programmes,
    _multiview_audio_channels,
    _multiview_audio_tvg_id,
    _multiview_boundaries,
    _multiview_member_programmes,
    _multiview_pane_desc_lines,
    _multiview_pane_info,
    _multiview_pane_labels,
    _multiview_pane_title,
    _multiview_programmes,
    _merge_short_intervals,
    _cap_intervals,
    _programme_at,
)
from catalog import xmltv_ts


def _dt(minutes: int, base: datetime) -> datetime:
    return base + timedelta(minutes=minutes)


class MultiviewGuideHelperTests(unittest.TestCase):
    """Pure unit tests for the interval-splitting/merging/cap helpers."""

    def test_merge_short_intervals_folds_sub_five_minute_gap(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        boundaries = [_dt(m, base) for m in (0, 30, 33, 90)]  # 30->33 is 3 minutes
        intervals = _merge_short_intervals(boundaries, min_seconds=300.0)
        # The 3-minute sliver must not appear as its own interval.
        for start, stop in intervals:
            self.assertGreaterEqual((stop - start).total_seconds(), 300.0)
        # The full span is preserved.
        self.assertEqual(intervals[0][0], boundaries[0])
        self.assertEqual(intervals[-1][1], boundaries[-1])

    def test_merge_short_intervals_merges_trailing_sliver_backward(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        # Last boundary is only 2 minutes after the previous one.
        boundaries = [_dt(m, base) for m in (0, 60, 120, 122)]
        intervals = _merge_short_intervals(boundaries, min_seconds=300.0)
        self.assertEqual(intervals[-1], (boundaries[1], boundaries[3]))
        for start, stop in intervals:
            self.assertGreaterEqual((stop - start).total_seconds(), 300.0)

    def test_merge_short_intervals_no_boundaries_is_noop(self):
        self.assertEqual(_merge_short_intervals([]), [])
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.assertEqual(_merge_short_intervals([base]), [])

    def test_cap_intervals_respects_cap(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        boundaries = [_dt(m, base) for m in range(0, 2000, 2)]  # ~1000 boundaries
        intervals = _merge_short_intervals(boundaries, min_seconds=0.0)
        self.assertGreater(len(intervals), 500)
        capped = _cap_intervals(intervals, cap=500)
        self.assertLessEqual(len(capped), 500)
        # Total covered span is unchanged.
        self.assertEqual(capped[0][0], intervals[0][0])
        self.assertEqual(capped[-1][1], intervals[-1][1])

    def test_pane_labels_side_by_side_and_grid(self):
        self.assertEqual(_multiview_pane_labels("side_by_side_2", 2), ["Left", "Right"])
        self.assertEqual(
            _multiview_pane_labels("grid_2x2", 4),
            ["Top-left", "Top-right", "Bottom-left", "Bottom-right"],
        )
        # Unknown layout / mismatched count falls back to generic labels
        # instead of raising or silently mislabeling panes.
        self.assertEqual(_multiview_pane_labels("mystery", 3), ["Pane 1", "Pane 2", "Pane 3"])

    def test_programme_at_returns_covering_block_or_last_as_fallback(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        programmes = [
            {"start": _dt(0, base), "stop": _dt(30, base), "title": "A"},
            {"start": _dt(30, base), "stop": _dt(60, base), "title": "B"},
        ]
        self.assertEqual(_programme_at(programmes, _dt(10, base))["title"], "A")
        self.assertEqual(_programme_at(programmes, _dt(30, base))["title"], "B")
        self.assertIsNone(_programme_at([], _dt(10, base)))


class MultiviewMemberProgrammeTests(unittest.TestCase):
    """Member-level programme building, including the removed-member case."""

    def test_removed_member_shows_no_signal(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        guide_start, guide_end = base, base + timedelta(hours=4)
        with patch.dict(state.stream_state, {}, clear=True):
            programmes = _multiview_member_programmes(
                ["ghost_member"], guide_start, guide_end, {}
            )
        self.assertEqual(len(programmes["ghost_member"]), 1)
        block = programmes["ghost_member"][0]
        self.assertEqual(block["title"], "No Signal")
        self.assertEqual(block["start"], guide_start)
        self.assertEqual(block["stop"], guide_end)

    def test_present_member_uses_channel_programmes(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        guide_start, guide_end = base, base + timedelta(hours=4)
        member = {
            "name": "Atlanta Braves", "always_live": False,
            "start_time": "", "stop_time": "", "logo_url": "",
        }
        with patch.dict(state.stream_state, {"braves": member}, clear=True):
            programmes = _multiview_member_programmes(["braves"], guide_start, guide_end, {})
            expected = _channel_programmes("braves", member, guide_start, guide_end, {})
        self.assertEqual(programmes["braves"], expected)

    def test_pane_title_prefers_member_name_over_filler(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        guide_start, guide_end = base, base + timedelta(hours=1)
        member = {"name": "Atlanta Braves", "always_live": False, "logo_url": ""}
        with patch.dict(state.stream_state, {"braves": member}, clear=True):
            standby = _channel_programmes("braves", member, guide_start, guide_end, {})[0]
            self.assertIn("Standby", standby["title"])
            self.assertEqual(
                _multiview_pane_title("braves", standby, present=True), "Atlanta Braves"
            )

    def test_pane_title_keeps_real_title_from_tvguide(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        guide_start, guide_end = base, base + timedelta(hours=4)
        member = {
            "name": "ESPN", "always_live": True, "tvg_id": "TESTCHAN.us", "logo_url": "",
        }
        schedules = {
            "TESTCHAN.us": [{
                "startTime": int(guide_start.timestamp()),
                "endTime": int(guide_end.timestamp()),
                "title": "Braves vs Rays",
                "description": "Live from Truist Park.",
            }]
        }
        with patch.dict(state.stream_state, {"espn": member}, clear=True):
            programme = _channel_programmes("espn", member, guide_start, guide_end, schedules)[0]
        self.assertEqual(programme["title"], "Braves vs Rays")
        self.assertEqual(
            _multiview_pane_title("espn", programme, present=True), "Braves vs Rays"
        )

    def test_pane_title_for_removed_member_is_no_signal(self):
        self.assertEqual(_multiview_pane_title("ghost", None, present=False), "No Signal")


class MultiviewIntervalSplittingTests(unittest.TestCase):
    """Splitting the composite guide window at each member's own boundaries."""

    def setUp(self):
        self.base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.guide_start = self.base
        self.guide_end = self.base + timedelta(hours=4)

        def scheduled(name, start_min, stop_min):
            return {
                "name": name, "always_live": False, "logo_url": "",
                "start_time": xmltv_ts(_dt(start_min, self.base)),
                "stop_time": xmltv_ts(_dt(stop_min, self.base)),
            }

        # Team A plays 30-90min in; Team B plays 70-150min in. Their Standby/
        # Scheduled-Event boundaries land at different points (0, 30, 70, 90,
        # 150, 240) so every interval in between is >= 5 minutes - no merging.
        self.member_state = {
            "team_a": scheduled("Team A", 30, 90),
            "team_b": scheduled("Team B", 70, 150),
        }

    def test_boundaries_include_every_member_transition(self):
        with patch.dict(state.stream_state, self.member_state, clear=True):
            member_programmes = _multiview_member_programmes(
                ["team_a", "team_b"], self.guide_start, self.guide_end, {}
            )
        boundaries = _multiview_boundaries(member_programmes, self.guide_start, self.guide_end)
        minutes = sorted({int((b - self.base).total_seconds() // 60) for b in boundaries})
        self.assertEqual(minutes, [0, 30, 70, 90, 150, 240])

    def test_multiview_programmes_split_at_every_member_boundary(self):
        data = {
            "name": "Sunday Quad-Box", "type": "multiview",
            "member_team_ids": ["team_a", "team_b"], "layout": "side_by_side_2",
            "active_audio_team_id": "team_a", "logo_url": "",
        }
        with patch.dict(state.stream_state, self.member_state, clear=True):
            main_programmes, audio_programmes = _multiview_programmes(
                "mv_sunday", data, self.guide_start, self.guide_end, {}
            )

        # 6 boundaries -> 5 intervals, all >= 5 minutes so none get merged away.
        self.assertEqual(len(main_programmes), 5)
        got = [
            (int((p["start"] - self.base).total_seconds() // 60),
             int((p["stop"] - self.base).total_seconds() // 60),
             p["title"])
            for p in main_programmes
        ]
        self.assertEqual(got, [
            (0, 30, "Team A | Team B"),
            (30, 70, "Team A Scheduled Event | Team B"),
            (70, 90, "Team A Scheduled Event | Team B Scheduled Event"),
            (90, 150, "Team A | Team B Scheduled Event"),
            (150, 240, "Team A | Team B"),
        ])
        for p in main_programmes:
            self.assertEqual(p["category"], "Sports")

        self.assertEqual(set(audio_programmes.keys()), {
            _multiview_audio_tvg_id("mv_sunday", "team_a"),
            _multiview_audio_tvg_id("mv_sunday", "team_b"),
        })

    def test_pane_desc_lines_use_position_labels_and_member_name(self):
        data = {
            "name": "Sunday Quad-Box", "type": "multiview",
            "member_team_ids": ["team_a", "team_b"], "layout": "side_by_side_2",
            "active_audio_team_id": "team_a", "logo_url": "",
        }
        with patch.dict(state.stream_state, self.member_state, clear=True):
            member_programmes = _multiview_member_programmes(
                ["team_a", "team_b"], self.guide_start, self.guide_end, {}
            )
            # Sample squarely inside the "both playing" interval (70-90min).
            instant = _dt(75, self.base)
            pane_info = _multiview_pane_info(
                ["team_a", "team_b"], "side_by_side_2", member_programmes, instant
            )
            lines = _multiview_pane_desc_lines(pane_info)

        self.assertEqual(lines, [
            "Left: Team A Scheduled Event (Team A)",
            "Right: Team B Scheduled Event (Team B)",
        ])

    def test_grid_2x2_pane_labels_follow_member_order(self):
        data = {
            "name": "Quad Box", "type": "multiview",
            "member_team_ids": ["team_a", "team_b", "ghost_c", "ghost_d"],
            "layout": "grid_2x2", "active_audio_team_id": "team_a", "logo_url": "",
        }
        with patch.dict(state.stream_state, self.member_state, clear=True):
            main_programmes, _ = _multiview_programmes(
                "mv_quad", data, self.guide_start, self.guide_end, {}
            )
        first = main_programmes[0]
        self.assertIn("Top-left: Team A", first["desc"])
        self.assertIn("Top-right: Team B", first["desc"])
        self.assertIn("Bottom-left: No Signal", first["desc"])
        self.assertIn("Bottom-right: No Signal", first["desc"])
        # Removed members show up in the joined title too, and never get a
        # parenthetical member name (there's no display name to show).
        self.assertIn("No Signal", first["title"])
        self.assertNotIn("No Signal (", first["desc"])

    def test_audio_line_names_the_active_member(self):
        data = {
            "name": "Sunday Quad-Box", "type": "multiview",
            "member_team_ids": ["team_a", "team_b"], "layout": "side_by_side_2",
            "active_audio_team_id": "team_b", "logo_url": "",
        }
        with patch.dict(state.stream_state, self.member_state, clear=True):
            main_programmes, _ = _multiview_programmes(
                "mv_sunday", data, self.guide_start, self.guide_end, {}
            )
        for p in main_programmes:
            self.assertIn("Audio: Team B", p["desc"])
            self.assertIn("🔊", p["desc"])


class MultiviewAudioChannelProgrammeTests(unittest.TestCase):
    """Per-audio-channel programmes: they follow their own member, not the merged grid."""

    def setUp(self):
        self.base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.guide_start = self.base
        self.guide_end = self.base + timedelta(hours=4)
        self.member_state = {
            "team_a": {
                "name": "Team A", "always_live": False, "logo_url": "",
                "start_time": xmltv_ts(_dt(30, self.base)),
                "stop_time": xmltv_ts(_dt(90, self.base)),
            },
            "team_b": {
                "name": "Team B", "always_live": False, "logo_url": "",
                "start_time": xmltv_ts(_dt(70, self.base)),
                "stop_time": xmltv_ts(_dt(150, self.base)),
            },
        }
        self.data = {
            "name": "Sunday Quad-Box", "type": "multiview",
            "member_team_ids": ["team_a", "team_b"], "layout": "side_by_side_2",
            "active_audio_team_id": "team_a", "logo_url": "",
        }

    def test_audio_programmes_follow_member_boundaries_not_the_merged_grid(self):
        with patch.dict(state.stream_state, self.member_state, clear=True):
            _, audio_programmes = _multiview_programmes(
                "mv_sunday", self.data, self.guide_start, self.guide_end, {}
            )
            expected_a = _channel_programmes(
                "team_a", self.member_state["team_a"], self.guide_start, self.guide_end, {}
            )
        audio_id_a = _multiview_audio_tvg_id("mv_sunday", "team_a")
        entries = audio_programmes[audio_id_a]
        self.assertEqual(len(entries), len(expected_a))
        for entry, expected in zip(entries, expected_a):
            self.assertEqual(entry["start"], expected["start"])
            self.assertEqual(entry["stop"], expected["stop"])

    def test_audio_programme_title_prefixed_and_desc_mentions_member_and_show(self):
        with patch.dict(state.stream_state, self.member_state, clear=True):
            _, audio_programmes = _multiview_programmes(
                "mv_sunday", self.data, self.guide_start, self.guide_end, {}
            )
        audio_id_a = _multiview_audio_tvg_id("mv_sunday", "team_a")
        entries = audio_programmes[audio_id_a]
        # The "live" block (30-90min in) has a real (non-filler) title.
        live_entry = next(e for e in entries if "Scheduled Event" in e["title"])
        self.assertTrue(live_entry["title"].startswith("🔊 "))
        self.assertIn("Team A Scheduled Event", live_entry["title"])
        self.assertTrue(live_entry["desc"].startswith("Audio from Team A in Sunday Quad-Box."))
        # Pane lines for both members are still included, e.g. so viewers know
        # what's on the other pane while listening to this one's audio.
        self.assertIn("Left: Team A Scheduled Event", live_entry["desc"])
        self.assertIn("Right:", live_entry["desc"])

        # A filler ("Standby") block gets the member's display name instead.
        standby_entry = entries[0]
        self.assertTrue(standby_entry["title"].startswith("🔊 "))
        self.assertEqual(standby_entry["title"], "🔊 Team A")

    def test_audio_channel_for_removed_member_is_no_signal(self):
        data = dict(self.data)
        data["member_team_ids"] = ["team_a", "ghost_member"]
        with patch.dict(state.stream_state, {"team_a": self.member_state["team_a"]}, clear=True):
            _, audio_programmes = _multiview_programmes(
                "mv_sunday", data, self.guide_start, self.guide_end, {}
            )
        audio_id_ghost = _multiview_audio_tvg_id("mv_sunday", "ghost_member")
        entries = audio_programmes[audio_id_ghost]
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["title"], "🔊 No Signal")


class MultiviewTvgIdSchemeTests(unittest.TestCase):
    """The audio-channel tvg-id scheme: stable, unique, no trailing digits."""

    def test_no_trailing_numeric_segment(self):
        # Even a member id ending in digits (a real possibility - team ids are
        # frequently "<category>_<numeric source id>") must not produce a
        # tvg-id any parser could mistake for ending in a bare channel number.
        tvg_id = _multiview_audio_tvg_id("mv_sunday", "nfl_9200004330")
        last_segment = tvg_id.split(".")[-1]
        self.assertFalse(last_segment.isdigit())
        self.assertFalse(re.search(r"\d$", tvg_id))

    def test_stable_across_member_reordering_and_unique_per_member(self):
        first = _multiview_audio_tvg_id("mv_sunday", "team_a")
        second = _multiview_audio_tvg_id("mv_sunday", "team_b")
        self.assertNotEqual(first, second)
        # Same member -> same id regardless of its position among the members.
        self.assertEqual(first, _multiview_audio_tvg_id("mv_sunday", "team_a"))

    def test_tvg_id_matches_between_m3u_and_xmltv(self):
        data = {
            "name": "Sunday Quad-Box", "type": "multiview",
            "member_team_ids": ["team_a", "team_b"], "layout": "side_by_side_2",
            "active_audio_team_id": "team_a", "logo_url": "", "candidates": [{"synthetic": True}],
            "is_healthy": True, "always_live": True, "category": "multiview",
            "catalog_key": "", "tvg_id": "", "group_title": "Multi-View",
        }
        member_state = {
            "team_a": {"name": "Team A", "always_live": False, "logo_url": ""},
            "team_b": {"name": "Team B", "always_live": False, "logo_url": ""},
        }
        channel_state = dict(member_state)
        channel_state["mv_sunday"] = data
        with patch.dict(state.stream_state, channel_state, clear=True), \
                patch.object(epg, "_fetch_tvguide_epg", new=AsyncMock(return_value={})):
            request = SimpleNamespace(headers={"host": "127.0.0.1:8000"})
            playlist = asyncio.run(generate_m3u(request))
            guide = asyncio.run(generate_xmltv())

        m3u_ids = set(re.findall(r'tvg-id="([^"]+)"', playlist))
        xmltv_ids = set(re.findall(r'<channel id="([^"]+)">', guide))

        expected_audio_ids = {
            _multiview_audio_tvg_id("mv_sunday", "team_a"),
            _multiview_audio_tvg_id("mv_sunday", "team_b"),
        }
        self.assertTrue(expected_audio_ids.issubset(m3u_ids))
        self.assertTrue(expected_audio_ids.issubset(xmltv_ids))
        for audio_id in expected_audio_ids:
            self.assertFalse(re.search(r"\d$", audio_id))

    def test_audio_channel_display_name_leads_with_member(self):
        data = {
            "name": "Sunday Quad-Box", "type": "multiview",
            "member_team_ids": ["team_a"], "logo_url": "",
        }
        with patch.dict(state.stream_state, {"team_a": {"name": "Atlanta Braves"}}, clear=True):
            entries = _multiview_audio_channels("mv_sunday", data)
        self.assertEqual(len(entries), 1)
        _, display_name, _ = entries[0]
        # The distinguishing part (member name) comes first so Jellyfin's
        # truncated guide column still shows something meaningful.
        self.assertTrue(display_name.startswith("🔊 Atlanta Braves"))
        self.assertIn("Sunday Quad-Box", display_name)
        self.assertLess(
            display_name.index("Atlanta Braves"), display_name.index("Sunday Quad-Box")
        )


class MultiviewXmltvWellFormedTests(unittest.TestCase):
    """The rendered guide must always be parseable XML."""

    def test_full_guide_is_well_formed_xml_for_2x2_multiview(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        data = {
            "name": "NFL Sunday Ticket", "type": "multiview",
            "member_team_ids": ["team_a", "team_b", "ghost_c", "ghost_d"],
            "layout": "grid_2x2", "active_audio_team_id": "team_a",
            "candidates": [{"synthetic": True}], "is_healthy": True,
            "always_live": True, "category": "multiview", "catalog_key": "",
            "logo_url": "", "tvg_id": "", "group_title": "Multi-View",
        }
        member_state = {
            "team_a": {
                "name": "Team A", "always_live": False, "logo_url": "",
                "start_time": xmltv_ts(_dt(30, base)), "stop_time": xmltv_ts(_dt(90, base)),
            },
            "team_b": {"name": "Team B", "always_live": False, "logo_url": ""},
        }
        channel_state = dict(member_state)
        channel_state["mv_sunday"] = data
        with patch.dict(state.stream_state, channel_state, clear=True), \
                patch.object(epg, "_fetch_tvguide_epg", new=AsyncMock(return_value={})):
            guide = asyncio.run(generate_xmltv())

        root = ET.fromstring(guide)  # raises if malformed
        self.assertEqual(root.tag, "tv")
        channel_ids = {el.get("id") for el in root.findall("channel")}
        self.assertIn("mv_sunday", channel_ids)
        programme_channels = {el.get("channel") for el in root.findall("programme")}
        self.assertIn("mv_sunday", programme_channels)
        # Every programme has a start/stop/title, matching XMLTV expectations.
        for programme in root.findall("programme"):
            self.assertIsNotNone(programme.get("start"))
            self.assertIsNotNone(programme.get("stop"))
            self.assertIsNotNone(programme.find("title"))


if __name__ == "__main__":
    unittest.main()
