"""Dashboard auth, CSRF, relay signing and output-escaping regressions (2.0.0)."""

import asyncio
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

import main
import tunables
import channels
import epg
import failover
import sessions
import legacy_proxy
import alerts
import db
import security
import state
import config


def _client(host: str = "127.0.0.1:8000") -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=main.app),
        base_url=f"http://{host}",
        follow_redirects=False,
    )


class TempDbMixin:
    def setUp(self):
        super().setUp()
        self._tmpdir = tempfile.mkdtemp()
        self._db_patch = patch.object(db, "DB_FILE", os.path.join(self._tmpdir, "test.db"))
        self._db_patch.start()
        db.init_db()
        self._state_backup = dict(state.stream_state)
        state.stream_state.clear()

    def tearDown(self):
        state.stream_state.clear()
        state.stream_state.update(self._state_backup)
        self._db_patch.stop()
        closer = getattr(db, "close_all_db_connections", None)
        if closer:
            closer()
        shutil.rmtree(self._tmpdir, ignore_errors=True)
        super().tearDown()


class CsrfTests(unittest.TestCase):
    def test_cross_site_write_detection(self):
        detect = security._is_cross_site_write
        own = {"host": "127.0.0.1:8000"}
        self.assertTrue(detect("POST", {**own, "origin": "https://evil.example"}, "http"))
        self.assertTrue(detect("POST", {**own, "origin": "null"}, "http"))
        self.assertTrue(detect("POST", {**own, "referer": "https://evil.example/page"}, "http"))
        self.assertFalse(detect("POST", {**own, "origin": "http://127.0.0.1:8000"}, "http"))
        self.assertFalse(detect("POST", {**own, "referer": "http://127.0.0.1:8000/?tab=alerts"}, "http"))
        # Scripts/curl send neither header and are not a CSRF vector.
        self.assertFalse(detect("POST", own, "http"))
        self.assertFalse(detect("GET", {**own, "origin": "https://evil.example"}, "http"))
        # Default ports are equivalent.
        self.assertFalse(detect("POST", {"host": "jb.local", "origin": "http://jb.local:80"}, "http"))

    def test_cross_origin_post_is_rejected_before_the_route(self):
        async def exercise():
            async with _client() as client:
                return await client.post(
                    "/settings/test_alert",
                    headers={"origin": "https://evil.example"},
                )

        with patch.object(main, "send_alert") as send_alert:
            response = asyncio.run(exercise())
        self.assertEqual(response.status_code, 403)
        send_alert.assert_not_called()


class DashboardAuthTests(unittest.TestCase):
    def test_open_mode_rejects_non_loopback_host_header(self):
        """DNS rebinding: an attacker hostname resolving to 127.0.0.1."""
        async def exercise(host):
            async with _client(host) as client:
                return await client.get("/api/ffmpeg-status")

        with patch.object(security, "DASHBOARD_PASSWORD", ""):
            self.assertEqual(asyncio.run(exercise("evil.example:8000")).status_code, 403)
            self.assertNotEqual(asyncio.run(exercise("127.0.0.1:8000")).status_code, 403)
            self.assertNotEqual(asyncio.run(exercise("localhost:8000")).status_code, 403)

    def test_loopback_bind_stays_open_network_bind_generates_password(self):
        tmpdir = Path(tempfile.mkdtemp())
        try:
            password_file = tmpdir / "dashboard-password.txt"
            with patch.object(security, "DASHBOARD_PASSWORD", ""), \
                    patch.object(security, "DASHBOARD_AUTH_MODE", "open"), \
                    patch.object(security, "DASHBOARD_PASSWORD_FILE", password_file):
                security._configure_dashboard_auth("127.0.0.1")
                self.assertEqual(security.DASHBOARD_PASSWORD, "")
                self.assertEqual(security.DASHBOARD_AUTH_MODE, "open")

                security._configure_dashboard_auth("0.0.0.0")
                generated = security.DASHBOARD_PASSWORD
                self.assertGreaterEqual(len(generated), 12)
                self.assertEqual(security.DASHBOARD_AUTH_MODE, "generated")
                self.assertEqual(password_file.read_text(encoding="utf-8").strip(), generated)

                # A restart reuses the saved password instead of rotating it.
                security.DASHBOARD_PASSWORD = ""
                security._configure_dashboard_auth("0.0.0.0")
                self.assertEqual(security.DASHBOARD_PASSWORD, generated)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_configured_password_is_never_replaced(self):
        with patch.object(security, "DASHBOARD_PASSWORD", "hunter2"), \
                patch.object(security, "DASHBOARD_AUTH_MODE", "configured"):
            security._configure_dashboard_auth("0.0.0.0")
            self.assertEqual(security.DASHBOARD_PASSWORD, "hunter2")

    def test_repeated_failures_lock_out_the_client(self):
        async def exercise(password):
            async with _client() as client:
                return await client.get("/api/ffmpeg-status", auth=("admin", password))

        with patch.object(security, "DASHBOARD_PASSWORD", "correct-horse"), \
                patch.object(security, "DASHBOARD_USERNAME", "admin"), \
                patch.dict(security._AUTH_FAILURES, clear=True), \
                patch.dict(security._VERIFIED_CREDENTIALS, clear=True):
            for _ in range(security.AUTH_FAILURE_LIMIT):
                self.assertEqual(asyncio.run(exercise("wrong")).status_code, 401)
            # Locked: even the right password is refused for a while.
            self.assertEqual(asyncio.run(exercise("correct-horse")).status_code, 429)
            security._AUTH_FAILURES.clear()
            self.assertEqual(asyncio.run(exercise("correct-horse")).status_code, 200)


class SecretSettingsTests(TempDbMixin, unittest.TestCase):
    def test_secret_input_never_echoes_the_value(self):
        # _secret_input now returns data for the dashboard template's
        # secret_input macro (templates/partials/alerts.html) rather than
        # HTML directly, so the saved value itself is never embedded
        # anywhere except as a masked last-4-characters hint.
        data = main._secret_input("jellyfin_api_key", "abcdef123456")
        self.assertNotIn("abcdef123456", repr(data))
        self.assertEqual(data["name"], "jellyfin_api_key")
        self.assertTrue(data["has_value"])
        self.assertEqual(data["masked_tail"], "3456")  # last four as a hint

        async def exercise():
            await db.set_setting_async("jellyfin_api_key", "abcdef123456")
            async with _client() as client:
                return await client.get("/?tab=alerts")

        with patch.object(security, "DASHBOARD_PASSWORD", ""):
            response = asyncio.run(exercise())
        self.assertEqual(response.status_code, 200)
        body = response.text
        self.assertNotIn("abcdef123456", body)
        self.assertIn('type="password"', body)
        self.assertIn("3456", body)

    def test_blank_secret_keeps_saved_value_and_clear_removes_it(self):
        self.assertEqual(main._submitted_secret("", "", "saved"), "saved")
        self.assertEqual(main._submitted_secret("new", "", "saved"), "new")
        self.assertEqual(main._submitted_secret("", "1", "saved"), "")

    def test_changing_jellyfin_host_requires_the_key_again(self):
        async def exercise():
            await db.set_setting_async("jellyfin_url", "http://192.168.1.10:8096")
            await db.set_setting_async("jellyfin_api_key", "secret-key")
            response = await main.update_jellyfin_settings(
                jellyfin_url="https://attacker.example", jellyfin_api_key="",
                jellyfin_task_id="", clear_jellyfin_api_key="", auth=True,
            )
            return response, await alerts.get_jellyfin_config()

        response, config = asyncio.run(exercise())
        self.assertIn("jellyfin_key_required", response.headers["location"])
        self.assertEqual(config["jellyfin_url"], "http://192.168.1.10:8096")
        self.assertEqual(config["jellyfin_api_key"], "secret-key")

    def test_same_host_keeps_saved_key(self):
        async def exercise():
            await db.set_setting_async("jellyfin_url", "http://192.168.1.10:8096")
            await db.set_setting_async("jellyfin_api_key", "secret-key")
            await main.update_jellyfin_settings(
                jellyfin_url="http://192.168.1.10:8096/", jellyfin_api_key="",
                jellyfin_task_id="task", clear_jellyfin_api_key="", auth=True,
            )
            return await alerts.get_jellyfin_config()

        config = asyncio.run(exercise())
        self.assertEqual(config["jellyfin_api_key"], "secret-key")
        self.assertEqual(config["jellyfin_task_id"], "task")


class OutputEscapingTests(unittest.TestCase):
    def setUp(self):
        self._state_backup = dict(state.stream_state)
        state.stream_state.clear()

    def tearDown(self):
        state.stream_state.clear()
        state.stream_state.update(self._state_backup)

    def _request(self, host=b"127.0.0.1:8000"):
        from starlette.requests import Request
        return Request({
            "type": "http", "method": "GET", "scheme": "http", "path": "/playlist.m3u",
            "query_string": b"", "headers": [(b"host", host)], "server": ("127.0.0.1", 8000),
            "client": ("127.0.0.1", 50000), "root_path": "", "http_version": "1.1",
        })

    def test_newline_in_name_cannot_plant_playlist_lines(self):
        state.stream_state["evil"] = {
            "name": 'Evil"\n#EXTINF:-1,Planted\nhttp://attacker.example/x.m3u8',
            "query": "evil", "candidates": [], "category": "custom",
        }
        playlist = asyncio.run(epg.generate_m3u(self._request()))
        lines = playlist.splitlines()
        self.assertEqual(sum(1 for line in lines if line.startswith("#EXTINF")), 1)
        self.assertFalse(any(line.startswith("http://attacker.example") for line in lines))

    def test_malformed_host_header_falls_back_to_loopback(self):
        state.stream_state["t"] = {"name": "T", "query": "t", "candidates": [], "category": "custom"}
        playlist = asyncio.run(epg.generate_m3u(self._request(b"evil.example/<script>")))
        self.assertIn(f"http://127.0.0.1:{config.PORT}/stream/t.m3u8", playlist)

    def test_xml_attributes_escape_quotes_and_drop_invalid_chars(self):
        self.assertEqual(config._xml_attr('a"b<c'), "a&quot;b&lt;c")
        self.assertEqual(config._xml_text("ok\x00\x07text"), "oktext")

    def test_clean_label(self):
        self.assertEqual(config._clean_label("  A\r\nB\tC  "), "A B C")
        self.assertEqual(len(config._clean_label("x" * 500, 120)), 120)


class ImportConfigTests(TempDbMixin, unittest.TestCase):
    def test_import_sanitizes_and_starts_channels(self):
        payload = {"teams": [
            {"team_id": "../../evil id", "name": "Evil\nName", "query": "evil query",
             "logo_url": "http://169.254.169.254/latest/meta-data", "is_favorite": True},
            {"team_id": "", "name": "", "query": ""},
            "not-a-dict",
        ]}

        async def exercise():
            from starlette.requests import Request

            body = json.dumps(payload).encode()

            async def receive():
                return {"type": "http.request", "body": body, "more_body": False}

            request = Request({"type": "http", "method": "POST", "path": "/api/import-config",
                               "headers": [], "query_string": b""}, receive)
            with patch.object(channels, "_start_team_scrape_loop") as start_loop, \
                    patch.object(main, "get_catalog_entries", return_value=[]):
                response = await main.import_config(request, auth=True)
            return response, start_loop

        response, start_loop = asyncio.run(exercise())
        result = json.loads(response.body)
        self.assertEqual(result["count"], 1)
        self.assertEqual(result["skipped"], 2)
        self.assertEqual(list(state.stream_state), ["evil_id"])
        entry = state.stream_state["evil_id"]
        self.assertEqual(entry["name"], "Evil Name")
        self.assertEqual(entry["logo_url"], "")  # link-local metadata address rejected
        start_loop.assert_called_once_with("evil_id")

    def test_schedule_disable_rejects_bad_dates(self):
        self.assertEqual(main._parse_disable_date("2026-10-01"), "2026-10-01")
        self.assertEqual(main._parse_disable_date(""), "")
        self.assertIsNone(main._parse_disable_date("10/01/2026"))
        self.assertIsNone(main._parse_disable_date("2026-02-30"))


class DashboardRenderTests(TempDbMixin, unittest.TestCase):
    def test_dashboard_renders_and_escapes_hostile_names(self):
        state.stream_state["evil"] = {
            "name": '<script>alert(1)</script>', "query": "q", "candidates": [
                {"provider": "<img src=x onerror=alert(2)>", "url": "https://cdn.example/a.m3u8",
                 "match_title": '"><svg onload=alert(3)>'},
            ],
            "active_index": 0, "is_healthy": True, "category": "custom", "logo_url": "",
            "start_time": "", "stop_time": "", **failover._scrape_lifecycle_defaults(),
        }

        async def exercise():
            async with _client() as client:
                return await client.get("/?tab=channels")

        with patch.object(security, "DASHBOARD_PASSWORD", ""),                 patch.object(main, "get_catalog_entries", return_value=[]):
            response = asyncio.run(exercise())
        self.assertEqual(response.status_code, 200)
        body = response.text
        self.assertNotIn("<script>alert(1)</script>", body)
        self.assertNotIn("<img src=x onerror=alert(2)>", body)
        self.assertNotIn('"><svg onload=alert(3)>', body)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", body)
        self.assertIn("Advanced Settings", body)

    def test_every_tab_renders_with_representative_state(self):
        """Templates render cleanly (no leftover Jinja syntax) for every tab,
        with channels that exercise candidates, Multi-View, off-season and
        exhausted states."""
        state.stream_state["with_candidates"] = {
            "name": "Has Candidates", "query": "q",
            "candidates": [
                {"provider": "ProviderA", "url": "https://cdn.example/a.m3u8", "match_title": "Game A"},
                {"provider": "ProviderB", "url": "https://cdn.example/b.m3u8", "match_title": "Game B"},
            ],
            "active_index": 0, "is_healthy": True, "category": "nfl", "logo_url": "",
            "start_time": "", "stop_time": "", **failover._scrape_lifecycle_defaults(),
        }
        state.stream_state["off_season_team"] = {
            "name": "Off Season Team", "query": "q", "candidates": [],
            "active_index": 0, "is_healthy": False, "category": "ncaaf", "logo_url": "",
            "start_time": "", "stop_time": "", **failover._scrape_lifecycle_defaults(),
        }
        state.stream_state["off_season_team"]["schedule_status"] = "off_season"
        state.stream_state["exhausted_team"] = {
            "name": "Exhausted Team", "query": "q", "candidates": [],
            "active_index": 0, "is_healthy": False, "category": "nba", "logo_url": "",
            "start_time": "", "stop_time": "", **failover._scrape_lifecycle_defaults(),
        }
        state.stream_state["exhausted_team"]["exhausted"] = True
        state.stream_state["mv_channel"] = {
            "type": "multiview", "name": "Quad Box", "layout": "grid_2x2",
            "member_team_ids": ["with_candidates", "off_season_team"],
            "active_audio_team_id": "with_candidates",
        }

        async def exercise(tab):
            async with _client() as client:
                return await client.get(f"/?tab={tab}")

        with patch.object(security, "DASHBOARD_PASSWORD", ""), \
                patch.object(main, "get_catalog_entries", return_value=[]):
            for tab in ["channels", "metrics", "performance", "playback", "alerts", "logs"]:
                response = asyncio.run(exercise(tab))
                self.assertEqual(response.status_code, 200, tab)
                body = response.text
                self.assertNotIn("{{", body, tab)
                self.assertNotIn("{%", body, tab)

    def test_advanced_settings_apply_live_and_reset(self):
        async def post(data):
            async with _client() as client:
                return await client.post("/settings/advanced", data=data)

        original = failover.IDLE_HEALTH_INTERVAL
        try:
            with patch.object(security, "DASHBOARD_PASSWORD", ""):
                response = asyncio.run(post({"IDLE_HEALTH_INTERVAL": "45", "SESSION_STALE_SECONDS": "9999"}))
                self.assertEqual(response.status_code, 303)
                self.assertEqual(failover.IDLE_HEALTH_INTERVAL, 45.0)
                # Clamped to the tunable's maximum and applied to live sessions.
                self.assertEqual(sessions.SESSIONS.config.stale_min_seconds, 300.0)
                self.assertEqual(db.get_setting("tunable:IDLE_HEALTH_INTERVAL"), "45.0")

                failover.IDLE_HEALTH_INTERVAL = 1.0
                tunables._load_tunable_overrides()
                self.assertEqual(failover.IDLE_HEALTH_INTERVAL, 45.0)

                asyncio.run(post({"IDLE_HEALTH_INTERVAL": "", "SESSION_STALE_SECONDS": ""}))
                self.assertEqual(failover.IDLE_HEALTH_INTERVAL, tunables._TUNABLE_DEFAULTS["IDLE_HEALTH_INTERVAL"])
                self.assertEqual(sessions.SESSIONS.config.stale_min_seconds,
                                 tunables._TUNABLE_DEFAULTS["SESSION_STALE_SECONDS"])
        finally:
            failover.IDLE_HEALTH_INTERVAL = original
            sessions.SESSIONS.config.stale_min_seconds = tunables._TUNABLE_DEFAULTS["SESSION_STALE_SECONDS"]


class ObservabilityEndpointTests(unittest.TestCase):
    def test_sessions_and_metrics_endpoints(self):
        async def exercise():
            async with _client() as client:
                return await client.get("/api/sessions"), await client.get("/metrics"), await client.get("/api/status")

        with patch.object(security, "DASHBOARD_PASSWORD", ""):
            sessions, metrics, status = asyncio.run(exercise())
        self.assertEqual(sessions.status_code, 200)
        self.assertIn("sessions", sessions.json())
        self.assertEqual(metrics.status_code, 200)
        self.assertIn("jellyball_channels", metrics.text)
        self.assertEqual(status.status_code, 200)

    def test_session_metrics_shape(self):
        from hls_session import ChannelSession, SessionConfig, SessionHooks
        hooks = SessionHooks(fetch=None, headers_for=None, resolve_source=None,
                             report_failure=None, report_incompatible=None)
        session = ChannelSession("c", hooks, SessionConfig())
        session._recent_segments.extend([(750_000, 6.0), (750_000, 6.0)])
        session._download_seconds.extend([0.2, 0.4, 1.0])
        metrics = session.metrics()
        self.assertEqual(metrics["bitrate_kbps"], 1000)
        self.assertEqual(metrics["segment_ms_p95"], 1000)
        self.assertEqual(metrics["segment_ms_avg"], 533)


class RelaySigningTests(unittest.TestCase):
    def test_rewritten_urls_carry_valid_signatures(self):
        import urllib.parse

        manifest = "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\nseg1.m4s\n"
        rewritten = legacy_proxy.rewrite_m3u8(manifest, "https://cdn.example.test/live/index.m3u8",
                                      "https://site.example.test/", "http://127.0.0.1:8000")
        uri = [line for line in rewritten.splitlines() if line and not line.startswith("#")][0]
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(uri).query)
        self.assertTrue(security._relay_signature_ok(
            query["url"][0], query["ref"][0], query.get("org", [""])[0], query["sig"][0]))

    def test_unsigned_relay_requests_are_refused(self):
        async def exercise():
            async with _client() as client:
                return [
                    await client.get("/chunk.ts", params={"url": "https://example.com/a.ts"}),
                    await client.get("/resource", params={"url": "https://example.com/key"}),
                    await client.get("/substream.m3u8", params={"url": "https://example.com/a.m3u8", "sig": "0" * 32}),
                ]

        for response in asyncio.run(exercise()):
            self.assertEqual(response.status_code, 403)


if __name__ == "__main__":
    unittest.main()
