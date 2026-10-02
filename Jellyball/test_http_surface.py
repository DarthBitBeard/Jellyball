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


if __name__ == "__main__":
    unittest.main()
