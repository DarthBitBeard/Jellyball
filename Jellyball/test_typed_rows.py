"""Typed DB rows (db.TeamRow / db.MultiviewRow) and Multi-View channels without
the old `candidates=[{"synthetic": True}]` sentinel.

A Multi-View channel now has no stream candidates, like any channel that has
none; state.is_multiview() keeps every decision the fake candidate used to
drive. These tests pin those decisions for both shapes (old sentinel, new empty
list), so the change is provably invisible to Jellyfin and the dashboard.
"""

import asyncio
import os
import shutil
import sqlite3
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi.responses import Response

import alerts
import db
import epg
import failover
import main
import multiview
import routes_api
import routes_dashboard
import security
import sessions
import state
from state import is_multiview, new_channel_state


HOST = "127.0.0.1:8000"


class TypedRowTests(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self._tmpdir, "rows.db")

    def tearDown(self):
        db.close_all_db_connections()
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_field_order_is_the_select_column_order(self):
        self.assertEqual(db.TeamRow._fields, (
            "team_id", "name", "query", "logo_url", "start_time", "stop_time", "category", "source_id",
            "content_type", "search_terms", "always_live", "catalog_key", "auto_disable_after",
        ))
        self.assertEqual(db.MultiviewRow._fields, (
            "channel_id", "name", "layout", "member_team_ids", "active_audio_team_id", "tvg_id", "group_title",
            "logo_url",
        ))

    def test_loaded_rows_are_named_and_still_plain_tuples(self):
        with patch.object(db, "DB_FILE", self.db_path):
            db.init_db()
            db.save_team("nfl_buf", "Buffalo Bills", "buffalo bills", category="nfl", source_id="buf",
                         search_terms=["bills"], always_live=False, catalog_key="team:nfl:buf")
            db.save_multiview_channel("mv", "Grid", "side_by_side_2", ["nfl_buf", "x"], "nfl_buf", "mv", "Multi-View")
            (team,) = db.load_teams()
            (mv,) = db.load_multiview_channels()
        self.assertIsInstance(team, db.TeamRow)
        self.assertEqual((team.team_id, team.category, team.source_id, team.catalog_key), ("nfl_buf", "nfl", "buf", "team:nfl:buf"))
        self.assertEqual(team.search_terms, '["bills"]')
        self.assertEqual(team, tuple(team))
        team_id, *_rest, auto_disable_after = team
        self.assertEqual((team_id, auto_disable_after), ("nfl_buf", ""))
        self.assertIsInstance(mv, db.MultiviewRow)
        self.assertEqual((mv.channel_id, mv.layout, mv.member_team_ids, mv.active_audio_team_id),
                         ("mv", "side_by_side_2", '["nfl_buf", "x"]', "nfl_buf"))
        self.assertEqual(mv, ("mv", "Grid", "side_by_side_2", '["nfl_buf", "x"]', "nfl_buf", "mv", "Multi-View", ""))

    def test_null_columns_still_arrive_as_none(self):
        with patch.object(db, "DB_FILE", self.db_path):
            db.init_db()
            conn = sqlite3.connect(self.db_path)
            try:
                conn.execute("INSERT INTO teams (team_id, name, query, logo_url) VALUES ('t', 'T', 't', NULL)")
                conn.commit()
            finally:
                conn.close()
            (team,) = db.load_teams()
        self.assertIsNone(team.logo_url)


def _multiview(**overrides) -> dict:
    fields = dict(
        name="Grid", query="", type="multiview", category="multiview", content_type="multiview",
        always_live=True, group_title="Multi-View", layout="side_by_side_2",
        member_team_ids=["team_a", "team_b"], active_audio_team_id="team_a",
        **failover._scrape_lifecycle_defaults(),
    )
    fields.update(overrides)
    return new_channel_state(**fields)


def _members() -> dict:
    return {
        "team_a": new_channel_state(name="Team A", query="team a", always_live=True),
        "team_b": new_channel_state(name="Team B", query="team b", always_live=True),
    }


def _http(method: str, path: str, **kwargs):
    async def go():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=main.app), base_url=f"http://{HOST}", follow_redirects=False,
        ) as client:
            return await client.request(method, path, **kwargs)

    with patch.object(security, "DASHBOARD_PASSWORD", ""), \
            patch.object(epg, "_fetch_tvguide_epg", AsyncMock(return_value={})), \
            patch.object(routes_dashboard, "get_catalog_entries", AsyncMock(return_value=[])):
        return asyncio.run(go())


class MultiviewWithoutSentinelTests(unittest.TestCase):
    def setUp(self):
        self._backup = dict(state.stream_state)
        state.stream_state.clear()
        state.stream_state.update(_members())
        self._processes = dict(multiview._MULTIVIEW_PROCESSES)
        multiview._MULTIVIEW_PROCESSES.clear()

    def tearDown(self):
        state.stream_state.clear()
        state.stream_state.update(self._backup)
        multiview._MULTIVIEW_PROCESSES.clear()
        multiview._MULTIVIEW_PROCESSES.update(self._processes)
        multiview._forget_multiview_channel("multiview_grid")

    def test_helper(self):
        self.assertTrue(is_multiview(_multiview()))
        self.assertFalse(is_multiview(state.stream_state["team_a"]))
        self.assertFalse(is_multiview({}))
        self.assertFalse(is_multiview(None))

    def test_dashboard_creates_a_multiview_without_candidates(self):
        with patch.object(routes_dashboard, "save_multiview_channel_async", AsyncMock()), \
                patch.object(routes_dashboard, "trigger_jellyfin_refresh", AsyncMock(return_value=True)):
            response = _http("POST", "/multiview/create", data={
                "name": "Grid", "layout": "side_by_side_2", "member_team_ids": "team_a,team_b",
            })
        self.assertEqual(response.status_code, 303)
        data = state.stream_state["multiview_grid"]
        self.assertTrue(is_multiview(data))
        self.assertEqual(data["candidates"], [])

    def test_still_listed_in_the_playlist_and_guide(self):
        state.stream_state["mv"] = _multiview(tvg_id="mv")
        playlist = _http("GET", "/playlist.m3u").text
        self.assertIn(f"http://{HOST}/stream/mv.m3u8", playlist)
        self.assertIn(f"http://{HOST}/multiview/mv/audio-0.m3u8", playlist)
        guide = _http("GET", "/epg.xml").text
        self.assertIn('<channel id="mv">', guide)
        self.assertIn('<programme channel="mv"', guide)

    def test_playlist_and_guide_do_not_depend_on_the_old_sentinel(self):
        state.stream_state["mv"] = _multiview(tvg_id="mv")
        new = (_http("GET", "/playlist.m3u").text, _http("GET", "/epg.xml").text)
        state.stream_state["mv"] = _multiview(tvg_id="mv", candidates=[{"synthetic": True}])
        old = (_http("GET", "/playlist.m3u").text, _http("GET", "/epg.xml").text)
        self.assertEqual(new[0], old[0])
        # The guide's time stamps move with the clock; compare it without them.
        strip = lambda xml: [line for line in xml.splitlines() if "<programme" not in line]  # noqa: E731
        self.assertEqual(strip(new[1]), strip(old[1]))

    def test_stream_routes_do_not_404(self):
        state.stream_state["mv"] = _multiview()
        for path in ("/stream/mv.m3u8", "/stream/mv", "/multiview/mv/audio-0.m3u8", "/multiview/mv/audio-1.m3u8"):
            with self.subTest(path=path):
                response = _http("HEAD", path)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.headers["content-type"], "application/vnd.apple.mpegurl")
        served = AsyncMock(return_value=Response(content="#EXTM3U\n", media_type="application/vnd.apple.mpegurl"))
        with patch.object(sessions, "_serve_session_playlist", served):
            response = _http("GET", "/stream/mv.m3u8")
        self.assertEqual(response.status_code, 200)
        served.assert_awaited_once()
        self.assertEqual(served.await_args.args[:2], ("mv", "mv/seg/"))
        self.assertIsNone(served.await_args.kwargs["legacy"])

    def test_health_indication_follows_the_grid_process(self):
        state.stream_state.clear()
        state.stream_state["mv"] = _multiview()
        failover._failover_monitor_tick()
        self.assertFalse(state.stream_state["mv"]["is_healthy"])
        multiview._MULTIVIEW_PROCESSES["mv"] = {"ready": True, "exited": False}
        failover._failover_monitor_tick()
        self.assertTrue(state.stream_state["mv"]["is_healthy"])
        self.assertEqual(state.stream_state["mv"]["candidates"], [])

    def test_guide_signature_is_unchanged(self):
        state.stream_state["mv"] = _multiview(candidates=[{"synthetic": True}])
        with_sentinel = alerts._guide_signature()
        state.stream_state["mv"] = _multiview()
        self.assertEqual(alerts._guide_signature(), with_sentinel)

    def test_test_stream_reports_a_running_grid_as_live(self):
        state.stream_state["mv"] = _multiview(is_healthy=True)
        with patch.object(routes_api, "_record_stream_test_sync"):
            result = asyncio.run(routes_api.test_stream("mv", auth=True))
        self.assertTrue(result["is_live"])

    def test_manual_rescrape_bookkeeping_is_unchanged(self):
        state.stream_state["mv"] = data = _multiview(is_healthy=True)
        with patch.object(failover, "master_scrape", AsyncMock(return_value=[])), \
                patch.object(failover, "update_team_meta_async", AsyncMock()), \
                patch.object(failover, "fetch_espn_team_schedule", AsyncMock()), \
                patch.object(failover.LOGGER, "warning"):
            asyncio.run(failover.trigger_scrape("mv", force=True))
        self.assertEqual(data["scrape_result"], "healthy")
        self.assertTrue(data["is_healthy"])
        self.assertEqual(data["candidates"], [])


if __name__ == "__main__":
    unittest.main()
