"""X8: complete configuration export/import, backward compatible with the
2.0 teams-only export, and never carrying a secret either way."""

import asyncio
import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

import httpx

import catalog
import channels
import config_transfer
import db
import main
import routes_api
import security
import state
from scrapers import ACTIVE_PROVIDERS, HtmlAggregatorScraper, _set_provider_url_override
from tunables import TUNABLES, _TUNABLE_DEFAULTS, _apply_tunable, _tunable_value

SECRETS = {
    "jellyfin_api_key": "APIKEYSECRET1",
    "discord_webhook_url": "https://discord.example.test/api/webhooks/1/HOOKSECRET2",
    "telegram_bot_token": "BOTSECRET3",
    "telegram_chat_id": "CHATSECRET4",
    "dashboard_password_hash": "$2b$12$HASHSECRET5",
}
TUNABLE = TUNABLES[0]
AGGREGATOR = next(p for p in ACTIVE_PROVIDERS if isinstance(p, HtmlAggregatorScraper))


class _Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self._use_db("a.db")
        self._state_backup = dict(state.stream_state)
        state.stream_state.clear()
        self._offseason = catalog.SHOW_OFFSEASON_CHANNELS
        self._loops = patch.object(channels, "_start_team_scrape_loop")
        self._loops.start()
        self._refresh = patch.object(config_transfer, "request_jellyfin_guide_refresh_if_changed")
        self._refresh.start()

    def _use_db(self, name: str) -> None:
        if getattr(self, "_db", None):
            self._db.stop()
            db.close_all_db_connections()
        self._db = patch.object(db, "DB_FILE", os.path.join(self.tmp, name))
        self._db.start()
        db.init_db()

    def tearDown(self):
        self._loops.stop()
        self._refresh.stop()
        _apply_tunable(TUNABLE, _TUNABLE_DEFAULTS[TUNABLE.name])
        _set_provider_url_override(AGGREGATOR, "")
        catalog.SHOW_OFFSEASON_CHANNELS = self._offseason
        state.stream_state.clear()
        state.stream_state.update(self._state_backup)
        self._db.stop()
        db.close_all_db_connections()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _populate(self):
        """A configured instance: two channels, a Multi-View, settings and secrets."""
        async def go():
            await channels._add_manual_team("Team One", "team one", team_id="team_one")
            await channels._add_manual_team("Team Two", "team two", team_id="team_two")
        asyncio.run(go())
        db._toggle_favorite_sync("team_one")
        db.save_multiview_channel("multiview_pair", "Pair", "side_by_side_2", ["team_one", "team_two"],
                                  "team_two", "mv.pair", "Multi-View", "")
        for key, value in SECRETS.items():
            db.set_setting(key, value)
        db.set_setting(TUNABLE.key, str(TUNABLE.minimum))
        db.set_setting(f"provider_url:{AGGREGATOR.name}", "https://mirror.example.test")
        db.set_setting("show_offseason_channels", "1")
        db.set_setting("jellyfin_url", "http://192.168.1.5:8096")
        db.set_setting("jellyfin_task_id", "abc123")
        db.set_setting("jellyfin_last_refresh", json.dumps({"ok": True}))


class ExportTests(_Base):
    def test_export_is_complete_and_holds_no_secret(self):
        self._populate()
        export = asyncio.run(config_transfer.build_export())
        self.assertEqual(export["version"], "2.1")
        self.assertEqual({t["team_id"] for t in export["teams"]}, {"team_one", "team_two"})
        self.assertEqual(export["multiview_channels"][0]["member_team_ids"], ["team_one", "team_two"])
        self.assertEqual(export["multiview_channels"][0]["tvg_id"], "mv.pair")
        settings = export["settings"]
        self.assertEqual(settings[TUNABLE.key], str(TUNABLE.minimum))
        self.assertEqual(settings[f"provider_url:{AGGREGATOR.name}"], "https://mirror.example.test")
        self.assertEqual(settings["show_offseason_channels"], "1")
        self.assertEqual(settings["jellyfin_url"], "http://192.168.1.5:8096")
        self.assertNotIn("jellyfin_last_refresh", settings)  # status, not configuration
        self.assertEqual(export["redacted_settings"], sorted(SECRETS))
        dumped = json.dumps(export)
        for secret in SECRETS.values():
            self.assertNotIn(secret.split("/")[-1], dumped)


class ImportTests(_Base):
    def _round_trip(self) -> dict:
        self._populate()
        export = asyncio.run(config_transfer.build_export())
        # A different machine: empty database and no channels.
        state.stream_state.clear()
        _apply_tunable(TUNABLE, _TUNABLE_DEFAULTS[TUNABLE.name])
        _set_provider_url_override(AGGREGATOR, "")
        self._use_db("b.db")
        return asyncio.run(config_transfer.apply_import(export, []))

    def test_round_trip_restores_channels_multiview_and_settings(self):
        result = self._round_trip()
        self.assertEqual((result["count"], result["skipped"]), (2, 0))
        self.assertEqual(result["multiview"], {"imported": 1, "skipped": 0})
        self.assertEqual(result["settings"]["skipped"], {})
        self.assertIn("team_one", state.stream_state)
        mv = state.stream_state["multiview_pair"]
        self.assertEqual(mv["member_team_ids"], ["team_one", "team_two"])
        self.assertEqual(mv["active_audio_team_id"], "team_two")
        self.assertEqual(mv["tvg_id"], "mv.pair")
        self.assertEqual(len(db.load_multiview_channels()), 1)
        # Settings are saved and applied live.
        self.assertEqual(db.get_setting(TUNABLE.key), str(TUNABLE.kind(TUNABLE.minimum)))
        self.assertEqual(_tunable_value(TUNABLE), TUNABLE.kind(TUNABLE.minimum))
        self.assertEqual(AGGREGATOR.base_url, "https://mirror.example.test")
        self.assertTrue(catalog.SHOW_OFFSEASON_CHANNELS)
        self.assertEqual(db.get_setting("jellyfin_url"), "http://192.168.1.5:8096")
        self.assertEqual(db.get_setting("jellyfin_task_id"), "abc123")
        # Favorites survive.
        self.assertTrue(dict((t["team_id"], t["is_favorite"]) for t in db._export_teams_sync())["team_one"])
        # And no secret came along.
        for key in SECRETS:
            self.assertEqual(db.get_setting(key, ""), "", key)

    def test_a_2_0_export_still_imports(self):
        legacy = {"version": "2.0", "app_version": "2.0.0", "exported_at": "2026-09-30T00:00:00+00:00",
                  "teams": [{"team_id": "legacy_team", "name": "Legacy Team", "query": "legacy",
                             "logo_url": "", "category": "custom", "is_favorite": False, "catalog_key": ""}]}
        result = asyncio.run(config_transfer.apply_import(legacy, []))
        self.assertEqual((result["count"], result["skipped"]), (1, 0))
        self.assertEqual(result["multiview"], {"imported": 0, "skipped": 0})
        self.assertEqual(result["settings"], {"applied": [], "skipped": {}})
        self.assertIn("legacy_team", state.stream_state)

    def test_handcrafted_secrets_and_unknown_settings_are_refused(self):
        body = {"teams": [], "settings": {
            **SECRETS, "made_up_setting": "1", "show_offseason_channels": "yes",
            f"provider_url:{AGGREGATOR.name}": "http://127.0.0.1:9/evil", TUNABLE.key: "not-a-number",
            "update_check_enabled": True,
        }}
        result = asyncio.run(config_transfer.apply_import(body, []))
        self.assertEqual(result["settings"]["applied"], [])
        self.assertEqual(set(result["settings"]["skipped"]), set(body["settings"]))
        for key in SECRETS:
            self.assertEqual(db.get_setting(key, ""), "")
        self.assertEqual(db.get_setting(f"provider_url:{AGGREGATOR.name}", ""), "")

    def test_the_api_key_never_follows_jellyfin_to_another_host(self):
        db.set_setting("jellyfin_url", "http://192.168.1.5:8096")
        db.set_setting("jellyfin_api_key", "APIKEYSECRET1")
        result = asyncio.run(config_transfer.apply_import(
            {"settings": {"jellyfin_url": "http://attacker.example.test:8096"}}, []))
        self.assertIn("jellyfin_url", result["settings"]["skipped"])
        self.assertEqual(db.get_setting("jellyfin_url"), "http://192.168.1.5:8096")
        # Same host: fine.
        result = asyncio.run(config_transfer.apply_import(
            {"settings": {"jellyfin_url": "http://192.168.1.5:8096/"}}, []))
        self.assertEqual(result["settings"]["applied"], ["jellyfin_url"])

    def test_multiview_rows_are_validated(self):
        async def setup():
            await channels._add_manual_team("Team One", "team one", team_id="team_one")
            await channels._add_manual_team("Team Two", "team two", team_id="team_two")
        asyncio.run(setup())
        state.stream_state["taken"] = {"name": "x", "tvg_id": "dup.tvg"}
        rows = [
            {"channel_id": "mv_ok", "name": "OK", "layout": "side_by_side_2",
             "member_team_ids": ["team_one", "team_two"], "tvg_id": "dup.tvg", "active_audio_team_id": "nope"},
            {"channel_id": "mv_bad_layout", "name": "X", "layout": "nine_up", "member_team_ids": ["team_one", "team_two"]},
            {"channel_id": "mv_missing", "name": "X", "layout": "side_by_side_2", "member_team_ids": ["team_one", "ghost"]},
            {"channel_id": "mv_count", "name": "X", "layout": "grid_2x2", "member_team_ids": ["team_one", "team_two"]},
            {"channel_id": "team_one", "name": "Clash", "layout": "side_by_side_2", "member_team_ids": ["team_one", "team_two"]},
            {"channel_id": "mv_noname", "name": "", "layout": "side_by_side_2", "member_team_ids": ["team_one", "team_two"]},
            "junk",
        ]
        result = asyncio.run(config_transfer.apply_import({"multiview_channels": rows}, []))
        self.assertEqual(result["multiview"], {"imported": 1, "skipped": 6})
        self.assertEqual(state.stream_state["mv_ok"]["tvg_id"], "")  # collided, so cleared
        self.assertEqual(state.stream_state["mv_ok"]["active_audio_team_id"], "team_one")

    def test_malformed_bodies(self):
        for raw in (b"not json", b"[]", b'{"teams": {}}', b'{"settings": []}', b'{"multiview_channels": 3}', b"\xff"):
            with self.subTest(raw=raw), self.assertRaises(config_transfer.ConfigImportError):
                config_transfer.parse_import_body(raw)
        self.assertEqual(config_transfer.parse_import_body(b'{"teams": []}'), {"teams": []})


class HttpTests(_Base):
    def _call(self, method, path, **kwargs):
        async def go():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=main.app), base_url="http://127.0.0.1:8000",
            ) as client:
                return await client.request(method, path, auth=("admin", "secret"), **kwargs)

        with patch.object(security, "DASHBOARD_PASSWORD", "secret"), \
                patch.object(security, "DASHBOARD_USERNAME", "admin"), \
                patch.dict(security._AUTH_FAILURES, clear=True), \
                patch.object(routes_api, "get_catalog_entries", return_value=[]):
            return asyncio.run(go())

    def test_export_and_import_endpoints(self):
        self._populate()
        exported = self._call("GET", "/api/export-config")
        self.assertEqual(exported.status_code, 200)
        self.assertNotIn("APIKEYSECRET1", exported.text)
        state.stream_state.clear()
        self._use_db("c.db")
        imported = self._call("POST", "/api/import-config", content=exported.content)
        self.assertEqual(imported.status_code, 200)
        self.assertEqual(imported.json()["count"], 2)
        self.assertEqual(imported.json()["multiview"]["imported"], 1)

    def test_bad_import_is_a_400_and_huge_is_a_413(self):
        self.assertEqual(self._call("POST", "/api/import-config", content=b"nope").status_code, 400)
        huge = b'{"teams": []' + b" " * (config_transfer.IMPORT_MAX_BYTES + 1) + b"}"
        self.assertEqual(self._call("POST", "/api/import-config", content=huge).status_code, 413)


if __name__ == "__main__":
    unittest.main()
