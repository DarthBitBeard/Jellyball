"""Which team channels Jellyfin's channel list shows (Settings > Channels Between Games):
always, on game day, or around the game; watched channels and channels with
a Jellyfin recording always stay listed; and Jellyfin's guide is refreshed as
the list changes."""

import asyncio
import json
import time
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock, patch

import httpx

import alerts
import catalog
import jellyfin_client
import main
import routes_dashboard
import security
import state
from catalog import LISTING_ALL, LISTING_AROUND, LISTING_GAME_DAY, _channel_listed
from state import new_channel_state
from test_http_surface import _request

MELBOURNE = timezone(timedelta(hours=11))
# Midday Friday 6 November in Melbourne.
NOW = datetime(2026, 11, 6, 1, 0, tzinfo=timezone.utc)
FMT = "%Y%m%d%H%M%S +0000"


def _local_date(moment):
    return moment.astimezone(MELBOURNE).date()


def _game(start: datetime, hours: float = 3, **extra) -> dict:
    return {
        "category": "nba",
        "schedule_status": "scheduled",
        "start_time": start.strftime(FMT),
        "stop_time": (start + timedelta(hours=hours)).strftime(FMT),
        **extra,
    }


def _mode(mode: str, lead: int = 30):
    return patch.multiple(catalog, LISTING_MODE=mode, LISTING_LEAD_MINUTES=lead)


ALWAYS_LISTED = {
    "no ESPN schedule": {"category": "custom", "schedule_status": "lookup_failed"},
    "not looked up yet": {"category": "nba"},
    "always-live channel": {"category": "special", "always_live": True, "schedule_status": "always_live"},
    "multi-view": {"type": "multiview"},
}


class GameDayTests(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(catalog, "_local_date", _local_date)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_which_channels_are_listed(self):
        cases = {
            "game tonight": (_game(datetime(2026, 11, 6, 8, 0, tzinfo=timezone.utc)), True),
            "game after local midnight": (_game(datetime(2026, 11, 6, 13, 30, tzinfo=timezone.utc)), False),
            "game next week": (_game(datetime(2026, 11, 12, 8, 0, tzinfo=timezone.utc)), False),
            "game from last night still on": (_game(datetime(2026, 11, 5, 22, 30, tzinfo=timezone.utc)), True),
            "game earlier today, over": (_game(datetime(2026, 11, 5, 19, 0, tzinfo=timezone.utc), hours=2.5), False),
            "no game scheduled": ({"category": "nba", "schedule_status": "no_event"}, False),
            "no game scheduled, but a stream": ({"category": "nba", "schedule_status": "no_event", "is_healthy": True}, True),
            **{name: (data, True) for name, data in ALWAYS_LISTED.items()},
        }
        with _mode(LISTING_GAME_DAY):
            for name, (data, expected) in cases.items():
                with self.subTest(name):
                    self.assertEqual(_channel_listed(data, NOW), expected)


class AroundTheGameTests(unittest.TestCase):
    # Tip-off 7pm Melbourne; scheduled to finish 10pm.
    START = datetime(2026, 11, 6, 8, 0, tzinfo=timezone.utc)

    def _listed(self, minutes_from_start: float, lead: int = 30, **extra) -> bool:
        with _mode(LISTING_AROUND, lead):
            return _channel_listed(_game(self.START, **extra), self.START + timedelta(minutes=minutes_from_start))

    def test_appears_at_the_lead_time(self):
        self.assertFalse(self._listed(-31))
        self.assertTrue(self._listed(-30))
        self.assertFalse(self._listed(-16, lead=15))
        self.assertTrue(self._listed(-15, lead=15))
        self.assertTrue(self._listed(-60, lead=60))

    def test_appears_sooner_when_a_pregame_stream_is_found(self):
        self.assertTrue(self._listed(-50, is_healthy=True))
        # Searching starts an hour before; nothing found earlier counts.
        self.assertFalse(self._listed(-70, is_healthy=True))

    def test_stays_until_the_game_and_its_stream_are_over(self):
        self.assertTrue(self._listed(60))  # in the game, even between streams
        self.assertTrue(self._listed(180))  # scheduled end
        self.assertFalse(self._listed(181))  # over, no stream
        self.assertTrue(self._listed(200, is_healthy=True))  # overtime, stream still on
        self.assertTrue(self._listed(300, is_healthy=True))
        self.assertFalse(self._listed(301, is_healthy=True))  # a stream left running: capped

    def test_other_channels(self):
        with _mode(LISTING_AROUND):
            self.assertFalse(_channel_listed({"category": "nba", "schedule_status": "no_event", "is_healthy": True}, NOW))
            for name, data in ALWAYS_LISTED.items():
                with self.subTest(name):
                    self.assertTrue(_channel_listed(data, NOW))


class OverrideTests(unittest.TestCase):
    later = _game(datetime(2026, 11, 9, 8, 0, tzinfo=timezone.utc), tvg_id="nba.lal")

    def test_every_channel_with_always(self):
        with _mode(LISTING_ALL):
            self.assertTrue(_channel_listed(self.later, NOW, team_id="nba_lal"))

    def test_watched_or_recorded_channels_stay_listed(self):
        for mode in (LISTING_GAME_DAY, LISTING_AROUND):
            with self.subTest(mode), _mode(mode):
                self.assertFalse(_channel_listed(self.later, NOW, team_id="nba_lal"))
                with patch.object(catalog, "IS_WATCHED", lambda team_id: team_id == "nba_lal"):
                    self.assertTrue(_channel_listed(self.later, NOW, team_id="nba_lal"))
                with patch.object(catalog, "RECORDING_TVG_IDS", frozenset({"nba.lal"})):
                    self.assertTrue(_channel_listed(self.later, NOW, team_id="nba_lal"))

    def test_off_season_follows_its_own_setting(self):
        off_season = {"category": "nfl", "schedule_status": "off_season"}
        with _mode(LISTING_AROUND), patch.object(catalog, "RECORDING_TVG_IDS", frozenset({"nfl_buf"})):
            with patch.object(catalog, "SHOW_OFFSEASON_CHANNELS", False):
                self.assertFalse(_channel_listed(off_season, NOW, team_id="nfl_buf"))
            with patch.object(catalog, "SHOW_OFFSEASON_CHANNELS", True):
                self.assertTrue(_channel_listed(off_season, NOW, team_id="nfl_buf"))

    def test_settings_values_are_checked(self):
        self.assertEqual(catalog.parse_listing_mode("around"), LISTING_AROUND)
        self.assertEqual(catalog.parse_listing_mode("sometimes"), LISTING_ALL)
        self.assertEqual(catalog.parse_listing_lead("60"), 60)
        self.assertEqual(catalog.parse_listing_lead("45"), 30)
        self.assertEqual(catalog.parse_listing_lead(None), 30)


class _StateTest(unittest.TestCase):
    def setUp(self):
        backup = dict(state.stream_state)
        state.stream_state.clear()
        self.addCleanup(lambda: (state.stream_state.clear(), state.stream_state.update(backup)))
        now = datetime.now(timezone.utc)
        state.stream_state["nba_lal"] = dict(new_channel_state(name="Los Angeles Lakers", query="Lakers", category="nba"),
                                             **_game(now + timedelta(minutes=10)))
        state.stream_state["nba_bos"] = dict(new_channel_state(name="Boston Celtics", query="Celtics", category="nba"),
                                             **_game(now + timedelta(days=3)))


class PlaylistTests(_StateTest):
    def test_playlist_leaves_out_unlisted_channels_but_the_guide_keeps_them(self):
        with _mode(LISTING_AROUND):
            playlist = _request("GET", "/playlist.m3u").text
            guide = _request("GET", "/epg.xml").text
        self.assertIn("Los Angeles Lakers", playlist)
        self.assertNotIn("Boston Celtics", playlist)
        # Jellyfin reuses a downloaded guide for about an hour: a channel that
        # appears must already have its programmes (and recordings need them).
        for team_id in ("nba_lal", "nba_bos"):
            self.assertIn(f'id="{team_id}"', guide)
            self.assertIn(f'channel="{team_id}"', guide)
        with _mode(LISTING_ALL):
            self.assertIn("Boston Celtics", _request("GET", "/playlist.m3u").text)


class GuideRefreshTests(_StateTest, unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        super().setUp()
        backup = dict(alerts._JELLYFIN_REFRESH_STATE)
        self.addCleanup(lambda: (alerts._JELLYFIN_REFRESH_STATE.clear(), alerts._JELLYFIN_REFRESH_STATE.update(backup)))
        alerts._JELLYFIN_REFRESH_STATE.update(signature=None, last_run=0.0, pending=None, listed=None, pending_fast=False)

    async def _settle(self):
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    def test_guide_signature_follows_the_listing(self):
        with _mode(LISTING_AROUND):
            with patch.object(catalog, "_channel_around_game", return_value=False):
                before = alerts._guide_signature()
            with patch.object(catalog, "_channel_around_game", return_value=True):
                after = alerts._guide_signature()
        self.assertNotEqual(before, after)

    def test_periodic_check_only_runs_when_channels_can_be_hidden(self):
        with patch.object(alerts, "request_jellyfin_guide_refresh_if_changed") as refresh:
            with _mode(LISTING_ALL):
                alerts.check_guide_listing()
            refresh.assert_not_called()
            with _mode(LISTING_AROUND):
                alerts.check_guide_listing()
            refresh.assert_called_once()

    async def test_a_channel_appearing_reaches_jellyfin_within_a_minute(self):
        trigger = AsyncMock(return_value=True)
        with _mode(LISTING_AROUND), patch.object(alerts, "trigger_jellyfin_refresh", trigger), \
                patch.object(alerts, "_listed_channels", return_value=frozenset({"nba_lal"})):
            # The last refresh was 2 minutes ago and listed nothing; a slow
            # refresh for another change is already queued.
            alerts._JELLYFIN_REFRESH_STATE.update(last_run=time.monotonic() - 120, listed=frozenset(), signature="old")
            slow = asyncio.ensure_future(asyncio.sleep(3600))
            alerts._JELLYFIN_REFRESH_STATE["pending"] = slow
            alerts.request_jellyfin_guide_refresh_if_changed()
            fast = alerts._JELLYFIN_REFRESH_STATE["pending"]
            self.assertIsNot(fast, slow)
            await fast
            await self._settle()
            self.assertTrue(slow.cancelled())
        trigger.assert_awaited_once()
        self.assertIsNone(alerts._JELLYFIN_REFRESH_STATE["pending"])
        self.assertEqual(alerts._JELLYFIN_REFRESH_STATE["listed"], frozenset({"nba_lal"}))

    async def test_other_changes_keep_the_ten_minute_spacing(self):
        with _mode(LISTING_AROUND), patch.object(alerts, "trigger_jellyfin_refresh", AsyncMock()), \
                patch.object(alerts, "_spawn_background_task") as spawn, \
                patch.object(alerts, "_debounced_jellyfin_refresh", Mock()) as debounced:
            alerts._JELLYFIN_REFRESH_STATE.update(last_run=time.monotonic() - 120, listed=alerts._listed_channels(), signature="old")
            alerts.request_jellyfin_guide_refresh_if_changed()
        spawn.assert_called_once()
        delay = debounced.call_args.args[0]
        self.assertGreater(delay, 400)

    async def test_recording_channels_are_remembered(self):
        with patch.object(alerts, "get_jellyfin_config", AsyncMock(return_value={})), \
                patch.object(catalog, "RECORDING_TVG_IDS", frozenset({"keep"})):
            with patch.object(alerts, "fetch_recording_channels", AsyncMock(return_value={"series_f1"})):
                await alerts.refresh_recording_channels()
                self.assertEqual(catalog.RECORDING_TVG_IDS, frozenset({"series_f1"}))
            # Jellyfin unreachable: keep what we knew.
            with patch.object(alerts, "fetch_recording_channels", AsyncMock(return_value=None)):
                await alerts.refresh_recording_channels()
                self.assertEqual(catalog.RECORDING_TVG_IDS, frozenset({"series_f1"}))


class JellyfinRecordingsTests(unittest.IsolatedAsyncioTestCase):
    CFG = {"jellyfin_url": "http://jellyfin.example.test:8096", "jellyfin_api_key": "k"}

    async def _fetch(self, handler):
        def make_client():
            return httpx.AsyncClient(transport=httpx.MockTransport(handler))

        with patch.object(jellyfin_client, "_make_client", make_client):
            return await jellyfin_client.fetch_recording_channels(self.CFG)

    async def test_channels_with_recordings_or_series_recordings(self):
        program = "{}_2026-11-07T18:00:00.0000000+00:00_m3u_6e2a6318"
        bodies = {
            "/LiveTv/Timers": {"Items": [{"ExternalProgramId": program.format("nba_lal")}]},
            "/LiveTv/SeriesTimers": {"Items": [
                {"Name": "F1 Qualifying", "ExternalProgramId": program.format("series_f1")},
                {"Name": "Anything", "RecordAnyChannel": True, "ExternalProgramId": program.format("espn")},
                {"Name": "Odd", "ExternalProgramId": "no-date-here"},
                "junk",
            ]},
        }
        seen_auth = []

        def handler(request):
            seen_auth.append(request.headers.get("Authorization", ""))
            return httpx.Response(200, text=json.dumps(bodies[request.url.path]))

        self.assertEqual(await self._fetch(handler), {"nba_lal", "series_f1"})
        self.assertIn('Token="k"', seen_auth[0])

    async def test_none_when_jellyfin_cannot_be_asked(self):
        self.assertIsNone(await self._fetch(lambda request: httpx.Response(401)))
        self.assertIsNone(await self._fetch(lambda request: httpx.Response(200, text="<html>")))
        self.assertIsNone(await jellyfin_client.fetch_recording_channels({"jellyfin_url": "", "jellyfin_api_key": ""}))


class SettingTests(unittest.TestCase):
    def test_setting_saves_applies_and_refreshes_jellyfin(self):
        saved = {}

        async def set_setting(key, value):
            saved[key] = value

        async def post(data):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://127.0.0.1:8000") as client:
                return await client.post("/settings/channel-listing", data=data, auth=("admin", "secret"))

        recordings = AsyncMock()
        with patch.object(security, "DASHBOARD_PASSWORD", "secret"), \
                patch.object(security, "DASHBOARD_USERNAME", "admin"), \
                patch.object(routes_dashboard, "set_setting_async", set_setting), \
                patch.object(routes_dashboard, "refresh_recording_channels", recordings), \
                patch.object(routes_dashboard, "trigger_jellyfin_refresh", new=Mock()) as refresh, \
                patch.object(routes_dashboard, "_spawn_background_task") as spawn, \
                _mode(LISTING_ALL):
            response = asyncio.run(post({"mode": "around", "lead": "60"}))
            self.assertEqual(response.status_code, 303)
            self.assertEqual((catalog.LISTING_MODE, catalog.LISTING_LEAD_MINUTES), (LISTING_AROUND, 60))
            self.assertEqual(saved, {"channel_listing_mode": "around", "channel_listing_lead_minutes": "60"})
            recordings.assert_awaited_once()
            refresh.assert_called_once()
            spawn.assert_called_once()
            # Unknown values fall back instead of being stored.
            asyncio.run(post({"mode": "sometimes", "lead": "45"}))
            self.assertEqual((catalog.LISTING_MODE, catalog.LISTING_LEAD_MINUTES), (LISTING_ALL, 30))
            self.assertEqual(saved, {"channel_listing_mode": "all", "channel_listing_lead_minutes": "30"})

    def test_settings_card_shows_the_current_choice(self):
        with _mode(LISTING_AROUND, 60):
            html = _request("GET", "/").text
        self.assertIn('<option value="around" selected>', html)
        self.assertIn('<option value="60" selected>', html)


if __name__ == "__main__":
    unittest.main()
