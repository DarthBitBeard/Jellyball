"""Motorsport series schedules (motorsport_schedule.py): parsing ESPN's racing
scoreboard into sessions, picking the current or next session at fixed times,
the season window, and the scrape loop routing a series channel to it.

The responses are the trimmed ESPN captures in fixtures/espn/f1_scoreboard_*.json
(2026 season), served through httpx.MockTransport."""

import asyncio
import json
import socket
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

import catalog
import config
import failover
import leagues
import motorsport_schedule as ms
import state
from network_safety import clear_dns_cache


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "espn"
TWO_WEEKENDS = json.loads((FIXTURES / "f1_scoreboard_two_weekends.json").read_text(encoding="utf-8"))
SPRINT_WEEKEND = json.loads((FIXTURES / "f1_scoreboard_sprint_weekend.json").read_text(encoding="utf-8"))
SEASON = json.loads((FIXTURES / "f1_scoreboard_season.json").read_text(encoding="utf-8"))
SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/racing/f1/scoreboard"


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


# São Paulo GP 2026 as ESPN lists it (UTC).
SAO_PAULO = {
    "FP1": utc(2026, 11, 6, 15, 30),
    "FP2": utc(2026, 11, 6, 19, 0),
    "FP3": utc(2026, 11, 7, 14, 30),
    "Qual": utc(2026, 11, 7, 18, 0),
    "Race": utc(2026, 11, 8, 17, 0),
}


def _public_getaddrinfo(host, port, *args, **kwargs):
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", port))]


def _frozen(now: datetime):
    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz) if tz is not None else now.replace(tzinfo=None)

    return FrozenDatetime


def _run(coro_factory, handler, now: datetime):
    """Run coro_factory() against a mocked ESPN at `now`; returns (result, request URLs)."""
    requests = []

    def recording(request):
        requests.append(str(request.url))
        return handler(request)

    async def go():
        clear_dns_cache()
        client = httpx.AsyncClient(transport=httpx.MockTransport(recording))
        try:
            with patch.object(state, "SHARED_HTTP_CLIENT", client), \
                    patch.object(ms, "datetime", _frozen(now)), \
                    patch.object(config.LOGGER, "warning"), patch.object(config.LOGGER, "log"), \
                    patch.object(socket, "getaddrinfo", side_effect=_public_getaddrinfo):
                return await coro_factory()
        finally:
            await client.aclose()
            clear_dns_cache()

    return asyncio.run(go()), requests


def _json(payload):
    return lambda request: httpx.Response(200, json=payload, request=request)


class _CacheReset(unittest.TestCase):
    def setUp(self):
        ms._SESSIONS_CACHE.clear()
        ms._SEASON_CACHE.clear()
        self.addCleanup(ms._SESSIONS_CACHE.clear)
        self.addCleanup(ms._SEASON_CACHE.clear)


class SeriesRegistryTests(unittest.TestCase):
    def test_f1_is_the_only_series_and_no_league_key_collides(self):
        self.assertEqual(tuple(leagues.SERIES_BY_KEY), ("f1",))
        for key in leagues.SERIES_BY_KEY:
            self.assertNotIn(key, leagues.SCHEDULE_LEAGUES)
            self.assertNotIn(key, leagues.CATALOG_CATEGORIES)

    def test_session_labels_and_lengths(self):
        f1 = leagues.F1
        self.assertEqual(f1.session_label("Qual"), "Qualifying")
        self.assertEqual(f1.session_label("SS"), "Sprint Qualifying")
        self.assertEqual(f1.session_label("SR"), "Sprint")
        self.assertEqual(f1.session_duration("Race"), timedelta(hours=2))
        self.assertEqual(f1.session_duration("FP1"), timedelta(hours=1))
        # Unknown codes keep the code and the default length.
        self.assertEqual(f1.session_label("FP4"), "FP4")
        self.assertEqual(f1.session_duration("FP4"), f1.default_session_duration)

    def test_series_are_immutable(self):
        with self.assertRaises(AttributeError):
            leagues.F1.name = "F1"


class RoundNameTests(unittest.TestCase):
    def test_sponsor_prefixes_are_dropped(self):
        cases = {
            "MSC Cruises São Paulo Grand Prix": "São Paulo GP",
            "Qatar Airways Qatar Grand Prix": "Qatar GP",
            "Qatar Airways Australian Grand Prix": "Australian GP",
            "Gulf Air Bahrain Grand Prix in Malaysia": "Bahrain GP in Malaysia",
            "Mexico City Grand Prix": "Mexico City GP",
            "MSC Cruises United States Grand Prix": "United States GP",
            "MSC Cruises Barcelona-Catalunya Grand Prix": "Barcelona-Catalunya GP",
            "Moët & Chandon Belgian Grand Prix": "Belgian GP",
            "Monaco Grand Prix": "Monaco GP",
        }
        for name, expected in cases.items():
            with self.subTest(name=name):
                self.assertEqual(ms.round_display_name(name), expected)

    def test_unknown_place_keeps_the_full_name(self):
        self.assertEqual(ms.round_display_name("Acme Moon Grand Prix"), "Acme Moon GP")
        self.assertEqual(ms.round_display_name("Season Finale"), "Season Finale")

    def test_every_2026_round_has_a_known_place(self):
        calendar = SEASON["leagues"][0]["calendar"]
        self.assertGreaterEqual(len(calendar), 24)
        for entry in calendar:
            with self.subTest(round=entry["label"]):
                self.assertIsNotNone(ms._PLACE_RE.search(entry["label"]))


class ParseSessionsTests(unittest.TestCase):
    def test_two_weekends_in_start_order(self):
        sessions = ms.parse_series_sessions(leagues.F1, TWO_WEEKENDS)
        self.assertEqual([s.round_name for s in sessions], ["São Paulo GP"] * 5 + ["Las Vegas GP"] * 5)
        self.assertEqual(sessions, sorted(sessions, key=lambda s: s.start))
        sao_paulo = {s.code: s for s in sessions[:5]}
        self.assertEqual({code: s.start for code, s in sao_paulo.items()}, SAO_PAULO)

    def test_session_fields(self):
        race = next(s for s in ms.parse_series_sessions(leagues.F1, TWO_WEEKENDS)
                    if s.code == "Race" and s.round_name == "São Paulo GP")
        self.assertEqual(race.label, "Race")
        self.assertEqual(race.stop - race.start, timedelta(hours=2))
        self.assertEqual(race.title, "São Paulo GP - Race")
        self.assertEqual(race.event_name, "MSC Cruises São Paulo Grand Prix")
        self.assertEqual(race.circuit, "Autodromo Jose Carlos Pace")
        self.assertEqual(race.city, "Sao Paulo")  # ESPN: "Sao paulo"
        self.assertEqual(race.country, "Brazil")
        self.assertEqual(race.series, "f1")
        self.assertTrue(race.event_id)

    def test_sprint_weekend_sessions(self):
        sessions = ms.parse_series_sessions(leagues.F1, SPRINT_WEEKEND)
        self.assertEqual([s.code for s in sessions], ["FP1", "SS", "SR", "Qual", "Race"])
        self.assertEqual([s.label for s in sessions],
                         ["Practice 1", "Sprint Qualifying", "Sprint", "Qualifying", "Race"])
        self.assertEqual({s.round_name for s in sessions}, {"Chinese GP"})

    def test_malformed_input_yields_nothing(self):
        for payload in (None, [], "x", {"events": "x"}, {"events": [None, {"competitions": [{"date": "bad"}]}]}):
            with self.subTest(payload=payload):
                self.assertEqual(ms.parse_series_sessions(leagues.F1, payload), [])


class SeasonTests(_CacheReset):
    def test_season_bounds_from_the_calendar(self):
        first, last = ms.parse_series_season(SEASON)
        self.assertEqual(first.date(), date(2026, 3, 6))
        self.assertEqual(last.date(), date(2026, 12, 6))
        self.assertIsNone(ms.parse_series_season({"leagues": [{"calendar": []}]}))
        self.assertIsNone(ms.parse_series_season(None))

    def test_in_season_uses_the_cached_season(self):
        _run(lambda: ms.fetch_series_season("f1"), _json(SEASON), utc(2026, 11, 5))
        self.assertTrue(catalog.is_in_season("f1", date(2026, 2, 27)))  # a week before round 1
        self.assertFalse(catalog.is_in_season("f1", date(2026, 2, 26)))
        self.assertTrue(catalog.is_in_season("f1", date(2026, 7, 31)))  # summer break stays in season
        self.assertTrue(catalog.is_in_season("f1", date(2026, 12, 7)))
        self.assertFalse(catalog.is_in_season("f1", date(2026, 12, 8)))

    def test_unknown_season_counts_as_in_season(self):
        self.assertTrue(catalog.is_in_season("f1", date(2026, 1, 15)))
        result, _ = _run(lambda: ms.fetch_series_season("f1"),
                         lambda r: httpx.Response(500, request=r), utc(2026, 1, 15))
        self.assertIsNone(result)
        self.assertTrue(catalog.is_in_season("f1", date(2026, 1, 15)))

    def test_resume_label_only_for_a_season_that_has_not_started(self):
        _run(lambda: ms.fetch_series_season("f1"), _json(SEASON), utc(2026, 1, 15))
        self.assertEqual(ms.series_resume_label("f1", date(2026, 1, 15)), "Mar 6")
        self.assertEqual(ms.series_resume_label("f1", date(2026, 12, 20)), "")
        with patch.object(catalog, "datetime", _frozen(utc(2026, 1, 15))):
            self.assertEqual(catalog._season_resume_label("f1"), "Mar 6")

    def test_season_is_cached(self):
        _, first = _run(lambda: ms.fetch_series_season("f1"), _json(SEASON), utc(2026, 11, 5))
        _, second = _run(lambda: ms.fetch_series_season("f1"), _json(SEASON), utc(2026, 11, 5))
        self.assertEqual(first, [SCOREBOARD])
        self.assertEqual(second, [])

    def test_league_seasons_are_unchanged(self):
        self.assertTrue(catalog.is_in_season("nfl", date(2027, 2, 15)))
        self.assertFalse(catalog.is_in_season("nfl", date(2027, 2, 16)))
        self.assertTrue(catalog.is_in_season("custom", date(2027, 7, 1)))


class CurrentSessionTests(_CacheReset):
    def schedule_at(self, now: datetime):
        return _run(lambda: ms.fetch_series_schedule("f1"), _json(TWO_WEEKENDS), now)

    def assertWindow(self, now: datetime, code: str):
        (start, stop, ok), _ = self.schedule_at(now)
        ms._SESSIONS_CACHE.clear()
        self.assertTrue(ok)
        self.assertEqual(start, SAO_PAULO[code], f"at {now:%a %H:%M}")
        self.assertEqual(stop, start + leagues.F1.session_duration(code))

    def test_window_moves_through_the_weekend(self):
        self.assertWindow(utc(2026, 11, 5, 9, 0), "FP1")  # the day before
        self.assertWindow(utc(2026, 11, 6, 16, 0), "FP1")  # during FP1
        self.assertWindow(utc(2026, 11, 6, 17, 15), "FP1")  # within an hour of FP1's end
        self.assertWindow(utc(2026, 11, 6, 17, 45), "FP2")  # FP1 grace over: FP2 is next
        self.assertWindow(utc(2026, 11, 7, 19, 30), "Qual")
        self.assertWindow(utc(2026, 11, 8, 19, 30), "Race")  # within the race's grace hour

    def test_after_the_weekend_the_next_round_is_chosen(self):
        (start, _, ok), _ = self.schedule_at(utc(2026, 11, 8, 21, 0))
        self.assertTrue(ok)
        self.assertEqual(start.date(), date(2026, 11, 20))  # Las Vegas FP1 (Thursday night local)

    def test_no_session_in_range_is_a_successful_empty_lookup(self):
        result, _ = _run(lambda: ms.fetch_series_schedule("f1"), _json({"events": []}), utc(2026, 12, 20))
        self.assertEqual(result, (None, None, True))

    def test_request_covers_yesterday_to_two_weeks_ahead(self):
        _, urls = self.schedule_at(utc(2026, 11, 5, 9, 0))
        self.assertEqual(urls, [f"{SCOREBOARD}?dates=20261104-20261119"])

    def test_failed_lookups(self):
        for name, handler in {
            "non-200": lambda r: httpx.Response(503, request=r),
            "not json": lambda r: httpx.Response(200, content=b"<html>", request=r),
            "wrong shape": _json({"events": "nope"}),
        }.items():
            with self.subTest(name):
                ms._SESSIONS_CACHE.clear()
                result, _ = _run(lambda: ms.fetch_series_schedule("f1"), handler, utc(2026, 11, 5))
                self.assertEqual(result, (None, None, False))

    def test_unknown_series_makes_no_request(self):
        result, urls = _run(lambda: ms.fetch_series_schedule("indycar"), _json(TWO_WEEKENDS), utc(2026, 11, 5))
        self.assertEqual(result, (None, None, False))
        self.assertEqual(urls, [])

    def test_sessions_are_cached_and_failures_only_briefly(self):
        _, first = self.schedule_at(utc(2026, 11, 5, 9, 0))
        _, second = self.schedule_at(utc(2026, 11, 5, 9, 0))
        self.assertEqual((len(first), len(second)), (1, 0))
        ms._SESSIONS_CACHE.clear()
        _run(lambda: ms.fetch_series_sessions("f1"), lambda r: httpx.Response(500, request=r), utc(2026, 11, 5))
        expires_at, (_, ok) = ms._SESSIONS_CACHE["f1"]
        self.assertFalse(ok)
        import time
        self.assertLessEqual(expires_at - time.monotonic(), ms._FAILURE_CACHE_SECONDS)


class ScrapeLoopRoutingTests(unittest.TestCase):
    def setUp(self):
        self._saved = dict(state.stream_state)
        self.addCleanup(self._restore)

    def _restore(self):
        state.stream_state.clear()
        state.stream_state.update(self._saved)

    def _scrape(self, data):
        state.stream_state["chan"] = data
        series_schedule = AsyncMock(return_value=(SAO_PAULO["Qual"], SAO_PAULO["Qual"] + timedelta(hours=1), True))
        team_schedule = AsyncMock(return_value=(None, None, True))
        season = AsyncMock(return_value=None)
        with patch.object(failover, "master_scrape", AsyncMock(return_value=[])), \
                patch.object(failover, "update_team_meta_async", AsyncMock()), \
                patch.object(failover, "fetch_series_schedule", series_schedule), \
                patch.object(failover, "fetch_series_season", season), \
                patch.object(failover, "fetch_espn_team_schedule", team_schedule), \
                patch.object(failover, "is_stream_window_active", return_value=True), \
                patch.object(failover.LOGGER, "warning"):
            asyncio.run(failover.trigger_scrape("chan", force=True))
        return series_schedule, team_schedule, season

    def test_series_channel_uses_the_series_schedule(self):
        data = state.new_channel_state(name="Formula 1", query="Formula 1", category="f1")
        series_schedule, team_schedule, season = self._scrape(data)
        series_schedule.assert_awaited_once_with("f1")
        season.assert_awaited_once_with("f1")
        team_schedule.assert_not_awaited()
        self.assertEqual(data["schedule_status"], "scheduled")
        self.assertEqual(data["start_time"], catalog.xmltv_ts(SAO_PAULO["Qual"]))

    def test_team_channel_is_unchanged(self):
        data = state.new_channel_state(name="Buffalo Bills", query="Bills", category="nfl", source_id="buf")
        with patch.object(catalog, "is_in_season", return_value=True), \
                patch.object(failover, "is_in_season", return_value=True):
            series_schedule, team_schedule, season = self._scrape(data)
        team_schedule.assert_awaited_once()
        series_schedule.assert_not_awaited()
        season.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
