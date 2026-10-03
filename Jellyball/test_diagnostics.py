"""X6: the diagnostics bundle, and above all that it leaks no secret.

Secrets are planted everywhere the bundle reads from (settings, candidate and
session URLs, the Jellyfin status, the log) with unique markers; none of the
markers may appear anywhere in the zip.
"""

import asyncio
import io
import json
import os
import shutil
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import httpx

import db
import diagnostics
import main
import security
import state
from diagnostics import redact, redact_text
from jellyfin_client import LAST_REFRESH_KEY
from sessions import SESSIONS
from state import new_channel_state
from version import __version__

MARKERS = [
    "CANDTOKEN111", "SESSTOKEN222", "APIKEY333", "WEBHOOK444", "BOT555", "CHAT666", "LOGTOKEN777",
    "LOGKEY888", "LOGAUTH999", "ENC000", "alice", "someone@example.test", "REFERERTOK", "JFREASON123",
    "0123456789abcdef0123456789abcdef", "PWHASHSECRET", "USERINFOPW", "COOKIEVAL",
]


class RedactTextTests(unittest.TestCase):
    def test_urls_keep_only_their_host(self):
        self.assertEqual(
            redact_text("GET https://cdn.example.test/live/CANDTOKEN111/index.m3u8?token=x failed"),
            "GET https://cdn.example.test/... failed",
        )
        self.assertNotIn("USERINFOPW", redact_text("http://user:USERINFOPW@host.example.test/x"))

    def test_relative_urls_lose_their_query(self):
        text = redact_text("/chunk.ts?url=https%3A%2F%2Fcdn.example.test%2Fa%3Ftoken%3DENC000&sig=abc")
        self.assertNotIn("ENC000", text)
        self.assertNotIn("sig=abc", text)
        self.assertTrue(text.startswith("/chunk.ts?"))

    def test_secret_pairs_and_headers(self):
        for line, marker in [
            ("api_key=LOGKEY888 next", "LOGKEY888"),
            ('Authorization: MediaBrowser Token="LOGAUTH999"', "LOGAUTH999"),
            ("X-Emby-Token: LOGAUTH999", "LOGAUTH999"),
            ("password: hunter2hunter2", "hunter2hunter2"),
            ("Cookie: jellyball_session_8000=COOKIEVAL", "COOKIEVAL"),
            ("token eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjMifQ.c2lnbmF0dXJl here", "eyJzdWIiOiIxMjMifQ"),
            ("sig 0123456789abcdef0123456789abcdef", "0123456789abcdef0123456789abcdef"),
        ]:
            with self.subTest(line=line):
                self.assertNotIn(marker, redact_text(line))

    def test_personal_data(self):
        self.assertEqual(redact_text(r"C:\Users\alice\AppData\Local\Jellyball"), r"C:\Users\<user>\AppData\Local\Jellyball")
        self.assertEqual(redact_text("/home/alice/.local/share"), "/home/<user>/.local/share")
        self.assertEqual(redact_text("mail someone@example.test now"), "mail <email> now")

    def test_ordinary_text_survives(self):
        line = "[2026-10-02 12:00:00] [INFO] Failover team=nfl_buf from=ProviderA to=ProviderB reason=stale"
        self.assertEqual(redact_text(line), line)

    def test_redact_blanks_secret_named_fields_recursively(self):
        value = {"jellyfin_api_key": "APIKEY333", "nested": [{"token": "x1"}, {"url": "https://h.example.test/p?a=1"}],
                 "empty_secret": {"password": ""}, "count": 3, "ok": True}
        out = redact(value)
        self.assertEqual(out["jellyfin_api_key"], diagnostics.REDACTED)
        self.assertEqual(out["nested"][0]["token"], diagnostics.REDACTED)
        self.assertEqual(out["nested"][1]["url"], "https://h.example.test/...")
        self.assertEqual(out["empty_secret"]["password"], "")
        self.assertEqual((out["count"], out["ok"]), (3, True))


class _FakeSession:
    pass


class DiagnosticsBundleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self._db = patch.object(db, "DB_FILE", os.path.join(self.tmp, "diag.db"))
        self._db.start()
        db.init_db()
        for key, value in {
            "jellyfin_url": "http://192.168.1.5:8096",
            "jellyfin_api_key": "APIKEY333",
            "discord_webhook_url": "https://discord.example.test/api/webhooks/1/WEBHOOK444",
            "telegram_bot_token": "BOT555",
            "telegram_chat_id": "CHAT666",
            "dashboard_password_hash": "$2b$12$PWHASHSECRET",
            "provider_url:ProviderA": "https://provider.example.test",
            "show_offseason_channels": "1",
            LAST_REFRESH_KEY: json.dumps({
                "ts": 1, "ok": False, "http_status": 401,
                "reason": "HTTP 401 from http://192.168.1.5:8096/ScheduledTasks?api_key=JFREASON123",
            }),
        }.items():
            db.set_setting(key, value)
        self.log = Path(self.tmp) / "jellyball.log"
        self.log.write_text("\n".join([
            "[2026-10-02] [INFO] start",
            "[2026-10-02] [WARNING] fetch https://cdn.example.test/live/seg.ts?token=LOGTOKEN777 failed",
            "[2026-10-02] [WARNING] api_key=LOGKEY888 rejected",
            '[2026-10-02] [WARNING] Authorization: MediaBrowser Token="LOGAUTH999"',
            r"[2026-10-02] [INFO] data dir C:\Users\alice\AppData\Local\Jellyball",
            "[2026-10-02] [INFO] contact someone@example.test",
            "[2026-10-02] [INFO] relay /chunk.ts?url=https%3A%2F%2Fcdn.example.test%2Fa%3Ftoken%3DENC000"
            "&sig=0123456789abcdef0123456789abcdef",
        ]) + "\n", encoding="utf-8")
        self._backup = dict(state.stream_state)
        state.stream_state.clear()
        state.stream_state["nfl_buf"] = new_channel_state(
            name="Buffalo Bills", query="Buffalo Bills", category="nfl",
            candidates=[{
                "provider": "ProviderA",
                "url": "https://cdn.example.test/live/index.m3u8?token=CANDTOKEN111",
                "referer": "https://embed.example.test/watch?k=REFERERTOK",
            }],
            scrape_error="timeout fetching https://embed.example.test/x?token=CANDTOKEN111",
        )

    def tearDown(self):
        state.stream_state.clear()
        state.stream_state.update(self._backup)
        self._db.stop()
        db.close_all_db_connections()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _download(self, auth=("admin", "secret")) -> httpx.Response:
        snapshot = {"nfl_buf": {
            "state": "running", "watched": True,
            "source": "ProviderA https://cdn.example.test/live/index.m3u8?token=SESSTOKEN222",
            "source_key": ["candidate", "https://cdn.example.test/live/index.m3u8?token=SESSTOKEN222"],
        }}

        async def go():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=main.app), base_url="http://127.0.0.1:8000",
            ) as client:
                return await client.get("/api/diagnostics.zip", auth=auth)

        with patch.object(diagnostics, "LOG_FILE", self.log), \
                patch.object(SESSIONS, "snapshot", return_value=snapshot), \
                patch.object(security, "DASHBOARD_PASSWORD", "secret"), \
                patch.object(security, "DASHBOARD_USERNAME", "admin"), \
                patch.dict(security._AUTH_FAILURES, clear=True):
            return asyncio.run(go())

    def _members(self, response: httpx.Response) -> dict:
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            return {name: archive.read(name).decode("utf-8") for name in archive.namelist()}

    def test_requires_the_dashboard_password(self):
        self.assertEqual(self._download(auth=None).status_code, 401)

    def test_bundle_is_a_zip_download(self):
        response = self._download()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "application/zip")
        self.assertIn("attachment;", response.headers["content-disposition"])
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertEqual(
            sorted(self._members(response)),
            sorted(["README.txt", "summary.json", "settings.json", "providers.json", "sessions.json",
                    "channels.json", "log-tail.txt"]),
        )

    def test_no_planted_secret_reaches_the_bundle(self):
        members = self._members(self._download())
        everything = "\n".join(members.values())
        leaked = [marker for marker in MARKERS if marker in everything]
        self.assertEqual(leaked, [], "secrets leaked into the diagnostics bundle")

    def test_the_bundle_is_still_useful(self):
        members = self._members(self._download())
        summary = json.loads(members["summary.json"])
        self.assertEqual(summary["version"], __version__)
        settings = json.loads(members["settings.json"])["app_settings"]
        self.assertEqual(settings["jellyfin_api_key"], diagnostics.REDACTED)
        self.assertEqual(settings["telegram_chat_id"], diagnostics.REDACTED)
        self.assertEqual(settings["show_offseason_channels"], "1")
        self.assertEqual(settings["jellyfin_url"], "http://192.168.1.5:8096/...")
        self.assertEqual(settings[LAST_REFRESH_KEY]["http_status"], 401)
        channels = json.loads(members["channels.json"])
        self.assertEqual(channels[0]["name"], "Buffalo Bills")
        self.assertEqual(channels[0]["providers"], ["ProviderA"])
        self.assertIn("embed.example.test", channels[0]["scrape_error"])
        sessions = json.loads(members["sessions.json"])
        self.assertIn("cdn.example.test", json.dumps(sessions))
        self.assertIn("rejected", members["log-tail.txt"])

    def test_the_card_is_on_the_logs_tab(self):
        async def go():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=main.app), base_url="http://127.0.0.1:8000",
            ) as client:
                return await client.get("/?tab=logs")

        import epg
        import routes_dashboard

        async def no_tvguide():
            return {}

        with patch.object(security, "DASHBOARD_PASSWORD", ""), \
                patch.object(epg, "_fetch_tvguide_epg", no_tvguide), \
                patch.object(routes_dashboard, "get_catalog_entries", return_value=[]):
            page = asyncio.run(go()).text
        self.assertIn('href="/api/diagnostics.zip"', page)


if __name__ == "__main__":
    unittest.main()
