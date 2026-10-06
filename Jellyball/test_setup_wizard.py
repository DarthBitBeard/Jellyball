"""Tests for the first-run setup wizard (routes_setup)."""

import asyncio
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

import httpx

import db
import main
import routes_setup
import security
import state
from db import get_setting_async

# The sandbox sets proxy env vars that this httpx version cannot parse, which
# breaks AsyncClient creation for every route. Strip them for the test session.
for _proxy_var in (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "http_proxy",
    "https_proxy",
    "ALL_PROXY",
    "all_proxy",
    "NO_PROXY",
    "no_proxy",
):
    os.environ.pop(_proxy_var, None)


def _run(coro):
    return asyncio.run(coro)


async def _client_request(method, path, **kwargs):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=main.app), base_url="http://127.0.0.1:8000"
    ) as client:
        return await client.request(method, path, **kwargs)


def _fake_entries():
    def entry(key, name):
        return {
            "catalog_key": key,
            "team_id": key.replace(":", "_"),
            "name": name,
            "query": name,
            "category": "nfl",
            "source_id": name,
            "content_type": "team",
            "search_terms": [name.lower()],
            "always_live": False,
            "logo_url": "",
        }

    return [entry("team:nfl:teama", "Team A"), entry("team:nfl:teamb", "Team B")]


class SetupWizardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self._db = patch.object(db, "DB_FILE", os.path.join(self.tmp, "wizard.db"))
        self._db.start()
        db.init_db()
        state.stream_state.clear()

    def tearDown(self):
        self._db.stop()
        db.close_all_db_connections()
        shutil.rmtree(self.tmp, ignore_errors=True)
        state.stream_state.clear()

    def _authed(self):
        # No dashboard password set in tests: loopback access is allowed.
        return {}

    # -- page and status ----------------------------------------------------

    def test_setup_page_renders(self):
        async def fake_entries():
            return _fake_entries()

        with patch.object(routes_setup, "get_catalog_entries", fake_entries):
            resp = _run(_client_request("GET", "/setup"))
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Setup Wizard", resp.text)
        self.assertIn("Connect Jellyfin", resp.text)
        self.assertIn("Pick your teams", resp.text)
        self.assertIn("Team A", resp.text)

    def test_setup_status_first_run(self):
        resp = _run(_client_request("GET", "/setup/status"))
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertTrue(data["first_run"])
        self.assertEqual(data["channels"], 0)
        self.assertFalse(data["jellyfin_configured"])

    # -- auth guardrail (spot check; the full sweep is test_auth_guardrail) --

    def test_anonymous_requests_rejected_when_password_set(self):
        with patch.object(security, "DASHBOARD_PASSWORD", "secret"), patch.object(
            security, "DASHBOARD_USERNAME", "admin"
        ):
            for method, path, kwargs in [
                ("GET", "/setup", {}),
                ("GET", "/setup/status", {}),
                ("POST", "/setup/jellyfin", {"json": {}}),
                ("POST", "/setup/jellyfin/test", {"json": {}}),
                ("POST", "/setup/teams", {"json": {}}),
                ("POST", "/setup/verify", {"json": {}}),
            ]:
                resp = _run(_client_request(method, path, **kwargs))
                self.assertEqual(resp.status_code, 401, f"{method} {path}")

    # -- Jellyfin save ------------------------------------------------------

    def test_save_jellyfin_happy_path(self):
        resp = _run(
            _client_request(
                "POST",
                "/setup/jellyfin",
                json={"url": "http://localhost:8096", "api_key": "testkey123", "task_id": ""},
            )
        )
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json()["ok"])
        self.assertEqual(_run(get_setting_async("jellyfin_api_key", "")), "testkey123")
        self.assertEqual(_run(get_setting_async("jellyfin_url", "")), "http://localhost:8096")

    def test_save_jellyfin_rejects_bad_url(self):
        resp = _run(_client_request("POST", "/setup/jellyfin", json={"url": "not a url", "api_key": "x"}))
        self.assertEqual(resp.status_code, 400)
        self.assertFalse(resp.json()["ok"])

    def test_save_jellyfin_requires_key_on_host_change(self):
        _run(
            _client_request(
                "POST", "/setup/jellyfin", json={"url": "http://host-a:8096", "api_key": "key-a"}
            )
        )
        resp = _run(
            _client_request("POST", "/setup/jellyfin", json={"url": "http://host-b:8096", "api_key": ""})
        )
        self.assertEqual(resp.status_code, 400)
        self.assertFalse(resp.json()["ok"])
        # The saved key was not clobbered.
        self.assertEqual(_run(get_setting_async("jellyfin_api_key", "")), "key-a")

    # -- Jellyfin test (unsaved credentials) ---------------------------------

    def test_test_jellyfin_unreachable_reports_reach_stage(self):
        resp = _run(
            _client_request(
                "POST",
                "/setup/jellyfin/test",
                json={"url": "http://127.0.0.1:9", "api_key": "whatever"},
            )
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertFalse(data["ok"])
        self.assertEqual(data["stage"], "reach")
        # A test must not persist the tried credentials.
        self.assertEqual(_run(get_setting_async("jellyfin_api_key", "")), "")

    def test_test_jellyfin_missing_config_reports_config_stage(self):
        resp = _run(_client_request("POST", "/setup/jellyfin/test", json={"url": "", "api_key": ""}))
        data = resp.json()
        self.assertFalse(data["ok"])
        self.assertEqual(data["stage"], "config")

    # -- teams ---------------------------------------------------------------

    def test_apply_teams_happy_path(self):
        calls = []

        async def fake_entries():
            return _fake_entries()

        async def fake_enable(entry, enabled):
            calls.append((entry["catalog_key"], enabled))
            return True

        with patch.object(routes_setup, "get_catalog_entries", fake_entries), patch.object(
            routes_setup, "_set_catalog_entry_enabled", fake_enable
        ):
            resp = _run(
                _client_request("POST", "/setup/teams", json={"catalog_keys": ["team:nfl:teama"]})
            )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["enabled"], 1)
        self.assertEqual(data["disabled"], 0)
        self.assertEqual(calls, [("team:nfl:teama", True)])

    def test_apply_teams_rejects_unknown_keys(self):
        async def fake_entries():
            return _fake_entries()

        with patch.object(routes_setup, "get_catalog_entries", fake_entries):
            resp = _run(_client_request("POST", "/setup/teams", json={"catalog_keys": ["nope"]}))
        self.assertEqual(resp.status_code, 400)
        self.assertFalse(resp.json()["ok"])

    # -- verify ---------------------------------------------------------------

    def test_verify_without_config_reports_config_stage(self):
        resp = _run(_client_request("POST", "/setup/verify", json={}))
        data = resp.json()
        self.assertFalse(data["ok"])
        self.assertEqual(data["stage"], "config")


if __name__ == "__main__":
    unittest.main()
