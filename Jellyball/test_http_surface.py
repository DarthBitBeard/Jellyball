"""HTTP-level smoke tests of the public surface, through the real ASGI app.

These are the requests Jellyfin and monitoring tools actually make. They need no
ffmpeg, browser or network; streaming itself is covered by the opt-in e2e tools.
"""

import asyncio
import unittest
import xml.etree.ElementTree as ET
from unittest.mock import patch

import httpx

import epg
import main
import routes_dashboard
import security
import state
from state import new_channel_state
from version import __version__


async def _no_tvguide():
    return {}


def _request(method: str, path: str, *, password: str = "", auth=None, host: str = "127.0.0.1:8000"):
    async def go():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=main.app),
            base_url=f"http://{host}",
            follow_redirects=False,
        ) as client:
            return await client.request(method, path, auth=auth)

    with patch.object(security, "DASHBOARD_PASSWORD", password), \
            patch.object(security, "DASHBOARD_USERNAME", "admin"), \
            patch.object(epg, "_fetch_tvguide_epg", _no_tvguide), \
            patch.object(routes_dashboard, "get_catalog_entries", return_value=[]):
        return asyncio.run(go())


class HttpSurfaceTests(unittest.TestCase):
    def setUp(self):
        self._backup = dict(state.stream_state)
        state.stream_state.clear()
        state.stream_state["nfl_buf"] = new_channel_state(
            name="Buffalo Bills", query="Buffalo Bills", category="nfl", source_id="buf",
            catalog_key="team:nfl:buf",
        )

    def tearDown(self):
        state.stream_state.clear()
        state.stream_state.update(self._backup)

    def test_healthz_needs_no_password_and_reports_the_version(self):
        response = _request("GET", "/healthz", password="secret")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok", "app": "jellyball", "version": __version__})

    def test_playlist_and_guide_are_served_without_a_password(self):
        """By design: Jellyfin's ffmpeg fetches them with no credentials (README: Security model)."""
        playlist = _request("GET", "/playlist.m3u", password="secret")
        self.assertEqual(playlist.status_code, 200)
        self.assertTrue(playlist.text.startswith("#EXTM3U"))
        self.assertIn("/stream/nfl_buf.m3u8", playlist.text)

        guide = _request("GET", "/epg.xml", password="secret")
        self.assertEqual(guide.status_code, 200)
        root = ET.fromstring(guide.text)
        self.assertEqual(root.tag, "tv")
        self.assertEqual([c.get("id") for c in root.findall("channel")], ["nfl_buf"])

    def test_head_requests_succeed_with_an_empty_body(self):
        for path in ("/playlist.m3u", "/epg.xml"):
            with self.subTest(path=path):
                response = _request("HEAD", path)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.content, b"")

    def test_dashboard_and_machine_endpoints_require_the_password(self):
        paths = ["/", "/api/status", "/api/sessions", "/api/version", "/api/logs", "/api/ffmpeg-status", "/metrics"]
        for path in paths:
            with self.subTest(path=path):
                anonymous = _request("GET", path, password="secret")
                self.assertEqual(anonymous.status_code, 401)
                self.assertEqual(anonymous.headers.get("www-authenticate"), "Basic")
                wrong = _request("GET", path, password="secret", auth=("admin", "nope"))
                self.assertEqual(wrong.status_code, 401)
                allowed = _request("GET", path, password="secret", auth=("admin", "secret"))
                self.assertEqual(allowed.status_code, 200, path)

    def test_unknown_channels_are_not_found(self):
        for method, path in [
            ("GET", "/stream/does_not_exist.m3u8"),
            ("HEAD", "/stream/does_not_exist.m3u8"),
            ("GET", "/stream/does_not_exist/seg/1.ts"),
            ("GET", "/multiview/does_not_exist/audio-0.m3u8"),
            ("GET", "/multiview/does_not_exist/audio-0/seg/1.ts"),
        ]:
            with self.subTest(method=method, path=path):
                self.assertEqual(_request(method, path).status_code, 404)

    def test_a_dashboard_page_is_html_and_names_the_running_version(self):
        response = _request("GET", "/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.headers["content-type"])
        self.assertIn(__version__, response.text)

    def test_api_status_reports_now_next_programme_titles(self):
        response = _request("GET", "/api/status", password="secret", auth=("admin", "secret"))
        self.assertEqual(response.status_code, 200)
        channels = response.json()["channels"]
        self.assertTrue(channels)
        for channel in channels:
            self.assertIn("now_title", channel)
            self.assertIn("next_title", channel)
            self.assertIsInstance(channel["now_title"], str)
            self.assertIsInstance(channel["next_title"], str)


def _sched(hours_from: float, hours_to: float) -> dict:
    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)
    fmt = "%Y%m%d%H%M%S +0000"
    return {
        "start_time": (now + timedelta(hours=hours_from)).strftime(fmt),
        "stop_time": (now + timedelta(hours=hours_to)).strftime(fmt),
    }


class NowNextTests(unittest.TestCase):
    """epg.now_next_for_channel() mirrors the guide title logic without
    fetching: it only reads the already-cached TVGuide data."""

    def test_standby_channel_reports_standby(self):
        guide = epg.now_next_for_channel("test_team", {"name": "Test Team"})
        self.assertEqual(guide, {"now": "Test Team Standby", "next": ""})

    def test_upcoming_event_is_next(self):
        data = {"name": "Test Team", **_sched(1, 3)}
        guide = epg.now_next_for_channel("test_team", data)
        self.assertEqual(guide, {"now": "Test Team Standby", "next": "Test Team Scheduled Event"})

    def test_live_event_is_now(self):
        data = {"name": "Test Team", **_sched(-1, 1)}
        guide = epg.now_next_for_channel("test_team", data)
        self.assertEqual(guide, {"now": "Test Team Scheduled Event", "next": "Test Team Standby"})

    def test_past_event_falls_back_to_standby(self):
        data = {"name": "Test Team", **_sched(-3, -1)}
        guide = epg.now_next_for_channel("test_team", data)
        self.assertEqual(guide, {"now": "Test Team Standby", "next": ""})

    def test_off_season_reports_off_season(self):
        data = {"name": "Test Team", "schedule_status": "off_season", "category": "nfl"}
        guide = epg.now_next_for_channel("test_team", data)
        self.assertTrue(guide["now"].startswith("Off-season"))
        self.assertEqual(guide["next"], "")

    def test_always_live_uses_cached_tvguide_programmes(self):
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        epg._TVGUIDE_EPG_CACHE["test_live"] = [
            {
                "startTime": int((now - timedelta(minutes=30)).timestamp()),
                "endTime": int((now + timedelta(minutes=30)).timestamp()),
                "title": "Big Game Live",
            },
            {
                "startTime": int((now + timedelta(minutes=30)).timestamp()),
                "endTime": int((now + timedelta(minutes=90)).timestamp()),
                "title": "Postgame Show",
            },
        ]
        try:
            guide = epg.now_next_for_channel("test_live", {"name": "Test Live", "always_live": True})
            self.assertEqual(guide, {"now": "Big Game Live", "next": "Postgame Show"})
        finally:
            epg._TVGUIDE_EPG_CACHE.pop("test_live", None)

    def test_always_live_without_cache_falls_back_to_live_label(self):
        guide = epg.now_next_for_channel("no_cache", {"name": "No Cache", "always_live": True})
        self.assertEqual(guide, {"now": "No Cache Live", "next": ""})


if __name__ == "__main__":
    unittest.main()
