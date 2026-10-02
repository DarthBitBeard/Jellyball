"""Jellyfin 12 compatibility (2.0.1): guide-refresh auth fallback, stale task ids,
visible failures, version detection and the dashboard/API surface for them."""

import asyncio
import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

import alerts
import db
import jellyfin_client
import main
import routes_dashboard
import security


class FakeJellyfin:
    """The few Jellyfin endpoints Jellyball calls, with switchable behaviour."""

    KEY = "fake-api-key-0123456789"

    def __init__(self, *, accept=("Authorization", "X-Emby-Token"), version="12.0.1", tasks=None,
                 trigger_status=204, stale_ids=("stale-id",), redirect_all=False, public_info_status=200):
        self.accept = set(accept)
        self.version = version
        self.tasks = [{"Id": "task-1", "Key": "RefreshGuide", "Name": "Refresh Guide"}] if tasks is None else tasks
        self.trigger_status = trigger_status
        self.stale_ids = set(stale_ids)
        self.redirect_all = redirect_all
        self.public_info_status = public_info_status
        self.calls = []  # (method, path, header style presented, status returned)
        self.app = self._build()

    def _presented_style(self, request: Request) -> str:
        auth = request.headers.get("authorization", "")
        if auth.startswith("MediaBrowser") and f'Token="{self.KEY}"' in auth:
            return jellyfin_client.AUTH_MODERN
        if request.headers.get("x-emby-token") == self.KEY:
            return jellyfin_client.AUTH_LEGACY
        return "none"

    def _build(self) -> FastAPI:
        app = FastAPI()
        fake = self

        def redirect(method: str, path: str):
            fake.calls.append((method, path, "none", 302))
            return Response(status_code=302, headers={"Location": "https://elsewhere.example.test/"})

        def authorize(request: Request, method: str, path: str):
            style = fake._presented_style(request)
            if style not in fake.accept:
                fake.calls.append((method, path, style, 401))
                return style, False
            return style, True

        @app.get("/System/Info/Public")
        async def public_info():
            if fake.redirect_all:
                return redirect("GET", "/System/Info/Public")
            fake.calls.append(("GET", "/System/Info/Public", "none", fake.public_info_status))
            if fake.public_info_status != 200:
                return JSONResponse({}, status_code=fake.public_info_status)
            return {"Version": fake.version, "ServerName": "Test Jellyfin", "ProductName": "Jellyfin Server"}

        @app.get("/ScheduledTasks")
        async def list_tasks(request: Request):
            if fake.redirect_all:
                return redirect("GET", "/ScheduledTasks")
            style, ok = authorize(request, "GET", "/ScheduledTasks")
            if not ok:
                return JSONResponse({}, status_code=401)
            fake.calls.append(("GET", "/ScheduledTasks", style, 200))
            return fake.tasks

        @app.post("/ScheduledTasks/Running/{task_id}")
        async def run_task(task_id: str, request: Request):
            path = f"/ScheduledTasks/Running/{task_id}"
            if fake.redirect_all:
                return redirect("POST", path)
            style, ok = authorize(request, "POST", path)
            if not ok:
                return JSONResponse({}, status_code=401)
            if task_id in fake.stale_ids or not any(t["Id"] == task_id for t in fake.tasks):
                fake.calls.append(("POST", path, style, 404))
                return JSONResponse({}, status_code=404)
            fake.calls.append(("POST", path, style, fake.trigger_status))
            return Response(status_code=fake.trigger_status)

        return app


class RefusingTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request):
        raise httpx.ConnectError("connection refused", request=request)


class JellyfinTestCase(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self._db_patch = patch.object(db, "DB_FILE", os.path.join(self._tmpdir, "test.db"))
        self._db_patch.start()
        db.init_db()
        # The metric writer's queue binds to the first event loop that uses it and
        # every asyncio.run() here makes a new one; the event itself is asserted below.
        self.metric_event = AsyncMock()
        self._metric_patch = patch.object(jellyfin_client, "log_metric_event_async", self.metric_event)
        self._metric_patch.start()
        jellyfin_client._PREFERRED_AUTH.clear()
        jellyfin_client._LOG_STATE.update(key=None, at=0.0, failing=False)

    def tearDown(self):
        self._metric_patch.stop()
        self._db_patch.stop()
        closer = getattr(db, "close_all_db_connections", None)
        if closer:
            closer()
        shutil.rmtree(self._tmpdir, ignore_errors=True)
        jellyfin_client._PREFERRED_AUTH.clear()

    @staticmethod
    def configure(fake, *, key=None, task_id=""):
        db.set_setting("jellyfin_url", "http://jellyfin.example.test:8096")
        db.set_setting("jellyfin_api_key", fake.KEY if key is None else key)
        db.set_setting("jellyfin_task_id", task_id)

    @staticmethod
    def client_patch(fake):
        return patch.object(
            jellyfin_client, "_make_client",
            lambda: httpx.AsyncClient(
                transport=httpx.ASGITransport(app=fake.app), timeout=8.0, follow_redirects=False
            ),
        )

    def refresh(self, fake):
        with self.client_patch(fake):
            return asyncio.run(alerts.refresh_jellyfin_guide())


class VersionAndHeaderTests(unittest.TestCase):
    def test_version_parsing_and_tested_range(self):
        self.assertEqual(jellyfin_client.parse_version("12.0.1"), (12, 0, 1))
        self.assertEqual(jellyfin_client.parse_version("10.11.0-rc1"), (10, 11, 0))
        for good in ("10.11.0", "10.11.6", "12.0.0", "12.1.2"):
            self.assertTrue(jellyfin_client.is_tested_version(good), good)
        for outside in ("10.10.7", "10.9.0", "13.0.0", "9.5.0"):
            self.assertFalse(jellyfin_client.is_tested_version(outside), outside)
        self.assertIsNone(jellyfin_client.is_tested_version(""))
        self.assertIsNone(jellyfin_client.is_tested_version("unknown"))

    def test_auth_header_styles(self):
        modern = jellyfin_client.auth_headers(jellyfin_client.AUTH_MODERN, "abc")
        self.assertTrue(modern["Authorization"].startswith("MediaBrowser "))
        self.assertIn('Token="abc"', modern["Authorization"])
        self.assertNotIn("X-Emby-Token", modern)
        legacy = jellyfin_client.auth_headers(jellyfin_client.AUTH_LEGACY, "abc")
        self.assertEqual(legacy["X-Emby-Token"], "abc")
        self.assertNotIn("Authorization", legacy)
        self.assertTrue(modern["User-Agent"].startswith("Jellyball/"))


class GuideRefreshTests(JellyfinTestCase):
    def test_modern_header_is_used_first_and_the_task_is_remembered(self):
        fake = FakeJellyfin()
        self.configure(fake)
        result = self.refresh(fake)
        self.assertTrue(result.ok)
        self.assertEqual(result.header, jellyfin_client.AUTH_MODERN)
        styles = {style for _, path, style, _ in fake.calls if path.startswith("/ScheduledTasks")}
        self.assertEqual(styles, {jellyfin_client.AUTH_MODERN})
        self.assertEqual(db.get_setting("jellyfin_task_id"), "task-1")
        saved = json.loads(db.get_setting(jellyfin_client.LAST_REFRESH_KEY))
        self.assertTrue(saved["ok"])
        self.assertEqual(json.loads(db.get_setting(jellyfin_client.SERVER_INFO_KEY))["version"], "12.0.1")
        self.metric_event.assert_awaited_once_with(
            "Jellyfin", "API", "guide_refresh", "Triggered Live TV Guide Refresh task"
        )

    def test_a_failed_refresh_records_no_guide_refresh_event(self):
        fake = FakeJellyfin(accept=())
        self.configure(fake, task_id="task-1")
        self.refresh(fake)
        self.metric_event.assert_not_awaited()

    def test_legacy_header_is_the_fallback_and_is_remembered(self):
        fake = FakeJellyfin(accept=("X-Emby-Token",))
        self.configure(fake, task_id="task-1")
        first = self.refresh(fake)
        self.assertTrue(first.ok)
        self.assertEqual(first.header, jellyfin_client.AUTH_LEGACY)
        fake.calls.clear()
        second = self.refresh(fake)
        self.assertTrue(second.ok)
        presented = [style for _, path, style, _ in fake.calls if path.startswith("/ScheduledTasks")]
        self.assertEqual(presented, [jellyfin_client.AUTH_LEGACY], "no wasted attempt with the rejected style")

    def test_rejected_key_is_an_auth_failure_and_never_leaks_the_key(self):
        fake = FakeJellyfin(accept=())
        self.configure(fake, task_id="task-1")
        with self.assertLogs(jellyfin_client.LOGGER, level="WARNING") as logs:
            result = self.refresh(fake)
        self.assertFalse(result.ok)
        self.assertEqual((result.stage, result.status), ("auth", 401))
        stored = db.get_setting(jellyfin_client.LAST_REFRESH_KEY)
        saved = json.loads(stored)
        self.assertEqual((saved["ok"], saved["stage"], saved["status"]), (False, "auth", 401))
        self.assertNotIn(fake.KEY, stored)
        self.assertNotIn(fake.KEY, "\n".join(logs.output))
        tried = {style for _, path, style, _ in fake.calls if path.startswith("/ScheduledTasks")}
        self.assertEqual(tried, {jellyfin_client.AUTH_MODERN, jellyfin_client.AUTH_LEGACY})

    def test_unreachable_server_is_a_reach_failure(self):
        fake = FakeJellyfin()
        self.configure(fake)
        refusing = lambda: httpx.AsyncClient(transport=RefusingTransport(), timeout=8.0)  # noqa: E731
        with patch.object(jellyfin_client, "_make_client", refusing):
            result = asyncio.run(alerts.refresh_jellyfin_guide())
        self.assertEqual(result.stage, "reach")
        self.assertIn("jellyfin.example.test", result.reason)
        self.assertEqual(json.loads(db.get_setting(jellyfin_client.LAST_REFRESH_KEY))["stage"], "reach")

    def test_stale_task_id_is_rediscovered_once(self):
        fake = FakeJellyfin()
        self.configure(fake, task_id="stale-id")
        result = self.refresh(fake)
        self.assertTrue(result.ok)
        self.assertEqual(result.task_id, "task-1")
        self.assertEqual(db.get_setting("jellyfin_task_id"), "task-1")
        posts = [(path, status) for method, path, _, status in fake.calls if method == "POST"]
        self.assertEqual(
            posts,
            [("/ScheduledTasks/Running/stale-id", 404), ("/ScheduledTasks/Running/task-1", 204)],
        )

    def test_missing_refresh_guide_task_is_a_discover_failure(self):
        fake = FakeJellyfin(tasks=[{"Id": "x", "Key": "Other", "Name": "Other task"}])
        self.configure(fake)
        result = self.refresh(fake)
        self.assertEqual(result.stage, "discover")
        self.assertIn("Refresh Guide", result.reason)

    def test_redirect_is_reported_and_not_followed(self):
        fake = FakeJellyfin(redirect_all=True)
        self.configure(fake)
        result = self.refresh(fake)
        self.assertEqual((result.stage, result.status), ("reach", 302))
        self.assertIn("redirect", result.reason)
        # Every call got the redirect and none was followed to another host.
        self.assertEqual([path for _, path, _, _ in fake.calls], ["/System/Info/Public", "/ScheduledTasks"])

    def test_a_proxy_that_blocks_the_public_info_endpoint_does_not_stop_the_refresh(self):
        fake = FakeJellyfin(public_info_status=403)
        self.configure(fake)
        result = self.refresh(fake)
        self.assertTrue(result.ok)
        # No version is known, but the refresh itself worked.
        self.assertEqual(db.get_setting(jellyfin_client.SERVER_INFO_KEY), "")
        self.assertTrue(json.loads(db.get_setting(jellyfin_client.LAST_REFRESH_KEY))["ok"])

    def test_unconfigured_integration_is_quiet(self):
        fake = FakeJellyfin()
        db.set_setting("jellyfin_url", "http://jellyfin.example.test:8096")
        db.set_setting("jellyfin_api_key", "")
        with self.assertNoLogs(jellyfin_client.LOGGER, level="WARNING"):
            result = self.refresh(fake)
        self.assertEqual((result.ok, result.stage), (False, "config"))
        self.assertEqual(fake.calls, [])
        self.assertEqual(db.get_setting(jellyfin_client.LAST_REFRESH_KEY), "")

    def test_key_with_unsafe_characters_is_refused_before_any_request(self):
        fake = FakeJellyfin()
        self.configure(fake, key='bad"key')
        result = self.refresh(fake)
        self.assertEqual(result.stage, "config")
        self.assertEqual(fake.calls, [])

    def test_trigger_wrapper_still_returns_a_bool(self):
        good = FakeJellyfin()
        self.configure(good)
        with self.client_patch(good):
            self.assertIs(asyncio.run(alerts.trigger_jellyfin_refresh()), True)
        bad = FakeJellyfin(accept=())
        with self.client_patch(bad):
            self.assertIs(asyncio.run(alerts.trigger_jellyfin_refresh()), False)

    def test_a_repeating_failure_is_logged_once_and_recovery_is_logged(self):
        fake = FakeJellyfin(accept=())
        self.configure(fake, task_id="task-1")
        with self.assertLogs(jellyfin_client.LOGGER, level="WARNING") as logs:
            for _ in range(3):
                self.refresh(fake)
        self.assertEqual(sum("guide refresh failed" in line for line in logs.output), 1)
        fake.accept = {"Authorization", "X-Emby-Token"}
        with self.assertLogs(jellyfin_client.LOGGER, level="INFO") as recovery:
            self.refresh(fake)
        self.assertTrue(any("works again" in line for line in recovery.output))


def _dashboard_client():
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=main.app),
        base_url="http://127.0.0.1:8000",
        follow_redirects=False,
    )


class DashboardSurfaceTests(JellyfinTestCase):
    def _post_test_button(self, fake=None, *, factory=None):
        async def exercise():
            async with _dashboard_client() as client:
                return await client.post("/settings/test_jellyfin")

        patches = [patch.object(security, "DASHBOARD_PASSWORD", "")]
        patches.append(patch.object(jellyfin_client, "_make_client", factory) if factory else self.client_patch(fake))
        with patches[0], patches[1]:
            return asyncio.run(exercise())

    def _get(self, path):
        async def exercise():
            async with _dashboard_client() as client:
                return await client.get(path)

        with patch.object(security, "DASHBOARD_PASSWORD", ""), \
                patch.object(routes_dashboard, "get_catalog_entries", return_value=[]):
            return asyncio.run(exercise())

    def test_test_button_names_the_step_that_failed(self):
        scenarios = [
            ("jellyfin_success", FakeJellyfin(), {}),
            ("jellyfin_failed_auth", FakeJellyfin(accept=()), {"task_id": "task-1"}),
            ("jellyfin_failed_task", FakeJellyfin(tasks=[]), {}),
            ("jellyfin_failed_config", FakeJellyfin(), {"key": ""}),
        ]
        for expected, fake, options in scenarios:
            with self.subTest(expected):
                self.configure(fake, **options)
                response = self._post_test_button(fake)
                self.assertEqual(response.status_code, 303)
                self.assertIn(f"status={expected}", response.headers["location"])
        self.configure(FakeJellyfin())
        refusing = lambda: httpx.AsyncClient(transport=RefusingTransport(), timeout=8.0)  # noqa: E731
        response = self._post_test_button(factory=refusing)
        self.assertIn("status=jellyfin_failed_unreachable", response.headers["location"])

    def test_alerts_tab_and_header_badge_show_a_failing_refresh(self):
        fake = FakeJellyfin(accept=())
        self.configure(fake, task_id="task-1")
        self._post_test_button(fake)
        body = self._get("/?tab=alerts").text
        self.assertIn("Jellyfin Refresh Failing", body)
        self.assertIn("Last guide refresh", body)
        self.assertIn("rejected the API key", body)
        self.assertIn("12.0.1", body)
        self.assertNotIn(fake.KEY, body)

    def test_alerts_tab_shows_a_working_refresh_and_warns_on_untested_versions(self):
        fake = FakeJellyfin(version="10.9.0")
        self.configure(fake)
        self._post_test_button(fake)
        body = self._get("/?tab=alerts").text
        self.assertIn("Jellyfin Auto-Refresh On", body)
        self.assertNotIn("Jellyfin Refresh Failing", body)
        self.assertIn("OK", body)
        self.assertIn("outside the tested range", body)

    def test_api_version_reports_the_jellyfin_status(self):
        fake = FakeJellyfin()
        self.configure(fake)
        self._post_test_button(fake)
        payload = self._get("/api/version").json()
        self.assertEqual(payload["jellyfin"]["server"]["version"], "12.0.1")
        self.assertIs(payload["jellyfin"]["server"]["tested"], True)
        self.assertIs(payload["jellyfin"]["last_refresh"]["ok"], True)


if __name__ == "__main__":
    unittest.main()
