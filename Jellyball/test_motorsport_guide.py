"""The guide of a motorsport series channel (epg.py): one programme per
session, titled per session kind with the round as its episode title, Standby
blocks between sessions, the dashboard's now/next, and the <sub-title> tag.

Sessions come from fixtures/espn/f1_scoreboard_two_weekends.json (São Paulo
and Las Vegas GPs 2026), primed into motorsport_schedule's cache."""

import asyncio
import json
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

import epg
import leagues
import motorsport_schedule as ms
import state


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "espn"
TWO_WEEKENDS = json.loads((FIXTURES / "f1_scoreboard_two_weekends.json").read_text(encoding="utf-8"))
SESSIONS = tuple(ms.parse_series_sessions(leagues.F1, TWO_WEEKENDS))


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


QUALI_START = utc(2026, 11, 7, 18, 0)


def _frozen(now: datetime):
    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz) if tz is not None else now.replace(tzinfo=None)

    return FrozenDatetime


def _f1_channel(**overrides) -> dict:
    data = state.new_channel_state(name="Formula 1", query="Formula 1", category="f1")
    data.update(overrides)
    return data


class _Primed(unittest.TestCase):
    def setUp(self):
        ms._SESSIONS_CACHE.clear()
        ms._LAST_GOOD_SESSIONS.clear()
        ms._SESSIONS_CACHE["f1"] = (time.monotonic() + 3600, (SESSIONS, True))
        self.addCleanup(ms._SESSIONS_CACHE.clear)
        self.addCleanup(ms._LAST_GOOD_SESSIONS.clear)

    def programmes(self, start: datetime, end: datetime, data=None):
        return epg._channel_programmes("f1_chan", data or _f1_channel(), start, end, {})


class SeriesProgrammeTests(_Primed):
    def test_every_session_is_a_programme_with_standby_between(self):
        start, end = utc(2026, 11, 5, 9, 0), utc(2026, 11, 15, 9, 0)
        programmes = self.programmes(start, end)
        self.assertEqual(
            [(p["title"], p.get("subtitle", "")) for p in programmes],
            [
                ("Formula 1 Standby", ""),
                ("F1 Practice 1", "São Paulo GP"),
                ("Formula 1 Standby", ""),
                ("F1 Practice 2", "São Paulo GP"),
                ("Formula 1 Standby", ""),
                ("F1 Practice 3", "São Paulo GP"),
                ("Formula 1 Standby", ""),
                ("F1 Qualifying", "São Paulo GP"),
                ("Formula 1 Standby", ""),
                ("F1 Race", "São Paulo GP"),
                ("Formula 1 Standby", ""),
            ],
        )

    def test_programmes_are_contiguous_and_fill_the_window(self):
        start, end = utc(2026, 11, 5, 9, 0), utc(2026, 11, 25, 9, 0)
        programmes = self.programmes(start, end)
        self.assertEqual(programmes[0]["start"], start)
        self.assertEqual(programmes[-1]["stop"], end)
        for before, after in zip(programmes, programmes[1:]):
            self.assertEqual(before["stop"], after["start"])
            self.assertLess(after["start"], after["stop"])
        self.assertEqual(sum(1 for p in programmes if p.get("subtitle") == "Las Vegas GP"), 5)

    def test_session_programme_fields(self):
        quali = next(p for p in self.programmes(utc(2026, 11, 5), utc(2026, 11, 15))
                     if p["title"] == "F1 Qualifying")
        self.assertEqual((quali["start"], quali["stop"]), (QUALI_START, QUALI_START + timedelta(hours=1)))
        self.assertEqual(quali["category"], "Sports")
        self.assertEqual(quali["desc"], "Qualifying of the MSC Cruises São Paulo Grand Prix at "
                                        "Autodromo Jose Carlos Pace, Sao Paulo, Brazil.")
        self.assertEqual(quali["icon"], epg._channel_logo_url(_f1_channel()))

    def test_standby_names_the_next_session(self):
        first = self.programmes(utc(2026, 11, 5, 9, 0), utc(2026, 11, 15))[0]
        self.assertEqual(first["desc"], "Next on Formula 1: São Paulo GP - Practice 1.")

    def test_a_session_in_progress_is_clipped_to_the_window(self):
        programmes = self.programmes(QUALI_START + timedelta(minutes=20), utc(2026, 11, 8, 18, 0))
        self.assertEqual(programmes[0]["title"], "F1 Qualifying")
        self.assertEqual(programmes[0]["start"], QUALI_START + timedelta(minutes=20))
        self.assertEqual(programmes[-1]["title"], "F1 Race")
        self.assertEqual(programmes[-1]["stop"], utc(2026, 11, 8, 18, 0))

    def test_without_sessions_the_channel_keeps_the_standby_guide(self):
        ms._SESSIONS_CACHE.clear()
        programmes = self.programmes(utc(2026, 11, 5), utc(2026, 11, 15))
        self.assertEqual([p["title"] for p in programmes], ["Formula 1 Standby"])

    def test_off_season_wins(self):
        programmes = self.programmes(utc(2026, 11, 5), utc(2026, 11, 15),
                                     _f1_channel(schedule_status="off_season"))
        self.assertEqual(len(programmes), 1)
        self.assertIn("Off-season", programmes[0]["title"])

    def test_team_channels_get_no_series_programmes(self):
        team = state.new_channel_state(name="Buffalo Bills", query="Bills", category="nfl")
        programmes = epg._channel_programmes("bills", team, utc(2026, 11, 5), utc(2026, 11, 15), {})
        self.assertTrue(all("subtitle" not in p for p in programmes))
        self.assertEqual(epg._series_sessions_in(team, utc(2026, 11, 5), utc(2026, 11, 15)), [])


class NowNextTests(_Primed):
    def now_next(self, now: datetime):
        with patch.object(epg, "datetime", _frozen(now)):
            return epg.now_next_for_channel("f1_chan", _f1_channel())

    def test_during_a_session(self):
        self.assertEqual(self.now_next(QUALI_START + timedelta(minutes=30)),
                         {"now": "São Paulo GP - Qualifying", "next": "São Paulo GP - Race"})

    def test_between_sessions(self):
        self.assertEqual(self.now_next(utc(2026, 11, 5, 9, 0)),
                         {"now": "Formula 1 Standby", "next": "São Paulo GP - Practice 1"})

    def test_during_the_last_listed_session(self):
        ms._SESSIONS_CACHE["f1"] = (time.monotonic() + 3600, (SESSIONS[:5], True))
        self.assertEqual(self.now_next(utc(2026, 11, 8, 17, 30)),
                         {"now": "São Paulo GP - Race", "next": "Formula 1 Standby"})


class XmltvTests(_Primed):
    def setUp(self):
        super().setUp()
        self._saved = dict(state.stream_state)
        self.addCleanup(self._restore)

    def _restore(self):
        state.stream_state.clear()
        state.stream_state.update(self._saved)

    def test_sessions_render_with_an_episode_title(self):
        state.stream_state.clear()
        state.stream_state["f1_chan"] = _f1_channel()
        refresh = AsyncMock(return_value=(SESSIONS, True))
        with patch.object(epg, "datetime", _frozen(utc(2026, 11, 5, 9, 0))), \
                patch.object(epg, "_fetch_tvguide_epg", AsyncMock(return_value={})), \
                patch.object(epg, "fetch_series_sessions", refresh):
            xml = asyncio.run(epg.generate_xmltv())
        refresh.assert_awaited_once_with("f1")
        self.assertIn(
            '  <programme channel="f1_chan" start="20261107180000 +0000" stop="20261107190000 +0000">\n'
            "    <title>F1 Qualifying</title>\n"
            "    <sub-title>São Paulo GP</sub-title>\n"
            "    <category>Sports</category>\n",
            xml,
        )
        # The 10-day guide from Nov 5 ends Nov 15: São Paulo's 5 sessions, not Las Vegas (Nov 20).
        self.assertEqual(xml.count("<sub-title>"), 5)
        self.assertNotIn("Las Vegas GP", xml)

    def test_no_series_channel_means_no_series_lookup(self):
        state.stream_state.clear()
        state.stream_state["bills"] = state.new_channel_state(name="Buffalo Bills", query="Bills", category="nfl")
        refresh = AsyncMock()
        with patch.object(epg, "_fetch_tvguide_epg", AsyncMock(return_value={})), \
                patch.object(epg, "fetch_series_sessions", refresh):
            xml = asyncio.run(epg.generate_xmltv())
        refresh.assert_not_awaited()
        self.assertNotIn("<sub-title>", xml)


class LastGoodSessionsTests(unittest.TestCase):
    def setUp(self):
        ms._SESSIONS_CACHE.clear()
        ms._LAST_GOOD_SESSIONS.clear()
        self.addCleanup(ms._SESSIONS_CACHE.clear)
        self.addCleanup(ms._LAST_GOOD_SESSIONS.clear)

    def _fetch(self, handler):
        async def plain_get(client, url):  # skip safe_get's DNS check: no network in tests
            return await client.get(url)

        async def go():
            client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            try:
                with patch.object(state, "SHARED_HTTP_CLIENT", client), \
                        patch.object(ms, "safe_get", plain_get), \
                        patch.object(ms.LOGGER, "warning"):
                    return await ms.fetch_series_sessions("f1")
            finally:
                await client.aclose()
        return asyncio.run(go())

    def test_a_failed_refresh_keeps_the_last_good_sessions_for_the_guide(self):
        self.assertEqual(ms.cached_series_sessions("f1"), ())
        self._fetch(lambda r: httpx.Response(200, json=TWO_WEEKENDS, request=r))
        self.assertEqual(len(ms.cached_series_sessions("f1")), 10)
        ms._SESSIONS_CACHE.clear()  # expire it
        self.assertEqual(self._fetch(lambda r: httpx.Response(503, request=r)), ((), False))
        self.assertEqual(len(ms.cached_series_sessions("f1")), 10)


class SessionTextTests(unittest.TestCase):
    def test_titles_and_location(self):
        race = next(s for s in SESSIONS if s.code == "Race")
        self.assertEqual(race.programme_title, "F1 Race")
        self.assertEqual(race.title, "São Paulo GP - Race")
        self.assertEqual(race.location, "Autodromo Jose Carlos Pace, Sao Paulo, Brazil")

    def test_missing_location_parts_are_left_out(self):
        bare = ms.Session(series="f1", event_id="1", event_name="Monaco Grand Prix", round_name="Monaco GP",
                          code="Race", label="Race", start=QUALI_START, stop=QUALI_START)
        self.assertEqual(bare.description, "Race of the Monaco Grand Prix.")
        unknown = ms.Session(series="zz", event_id="1", event_name="X", round_name="X GP",
                             code="Race", label="Race", start=QUALI_START, stop=QUALI_START)
        self.assertEqual(unknown.programme_title, "X GP - Race")


if __name__ == "__main__":
    unittest.main()
