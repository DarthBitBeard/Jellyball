"""The 2.0.0 compatibility contract.

Jellyfin stores what Jellyball hands it: tuner channels (by URL), guide data (by
tvg-id) and recording timers (by programme). Changing any value frozen in this
file means every user has to re-add their Jellyfin tuner or loses recordings, so
a change here must be a deliberate, announced decision (see "Upgrading from 1.x"
in the CHANGELOG for what that costs) and never a side effect of a refactor.

It also freezes the 2.0.0 database layout, so every later schema migration is
proven to upgrade an existing install without losing data.
"""

import asyncio
import json
import os
import re
import shutil
import sqlite3
import tempfile
import unittest
import xml.etree.ElementTree as ET
from types import SimpleNamespace
from unittest.mock import patch

import db
import epg
import main
import state
from catalog import _special_catalog_entry, _team_catalog_entry
from sports_catalog import SPECIAL_CHANNELS, STATIC_TEAM_RECORDS
from state import new_channel_state


# --- channel identity --------------------------------------------------------

# (category, canonical name) -> (team_id, catalog_key). These are the channel IDs
# in the M3U, in Jellyfin's tuner and in the database.
TEAM_IDENTITY_2_0_0 = [
    ("nfl", "buffalo bills", "nfl_buf", "team:nfl:buf"),
    ("mlb", "new york yankees", "mlb_nyy", "team:mlb:nyy"),
    ("nhl", "boston bruins", "nhl_bos", "team:nhl:bos"),
    ("nba", "boston celtics", "nba_bos", "team:nba:bos"),
    ("ncaaf", "michigan wolverines", "ncaaf_130", "team:ncaaf:130"),
    ("ncaam", "michigan wolverines", "ncaam_130", "team:ncaam:130"),
]

# Catalog key -> (channel id, tvg-id). The tvg-ids are documented in the README
# so they can be mapped to an external XMLTV provider.
SPECIAL_CHANNELS_2_0_0 = {
    "nfl_redzone": ("special_nfl_redzone", "NFLRedZone.us"),
    "espn": ("special_espn", "ESPN.us"),
    "espn2": ("special_espn2", "ESPN2.us"),
    "espnu": ("special_espnu", "ESPNU.us"),
    "fs1": ("special_fs1", "FoxSports1.us"),
    "fs2": ("special_fs2", "FoxSports2.us"),
    "cbs_sports_network": ("special_cbs_sports_network", "CBSSportsNetwork.us"),
    "tnt_sports": ("special_tnt_sports", "TNT.us"),
    "nbc_sports": ("special_nbc_sports", "NBCSports.us"),
    "big_ten_network": ("special_big_ten_network", "BigTenNetwork.us"),
    "acc_network": ("special_acc_network", "ACCNetwork.us"),
    "sec_network": ("special_sec_network", "SECNetwork.us"),
    "longhorn_network": ("special_longhorn_network", "LonghornNetwork.us"),
    "mlb_network": ("special_mlb_network", "MLBNetwork.us"),
    "nba_tv": ("special_nba_tv", "NBATV.us"),
    "nfl_network": ("special_nfl_network", "NFLNetwork.us"),
    "nhl_network": ("special_nhl_network", "NHLNetwork.us"),
    "abc": ("special_abc", "ABC.us"),
    "fox": ("special_fox", "FOX.us"),
    "cbs": ("special_cbs", "CBS.us"),
    "nbc": ("special_nbc", "NBC.us"),
    "the_cw": ("special_the_cw", "CW.us"),
    "tbs": ("special_tbs", "TBS.us"),
    "usa_network": ("special_usa_network", "USANetwork.us"),
    "trutv": ("special_trutv", "TruTV.us"),
}


class ChannelIdentityContractTests(unittest.TestCase):
    def test_team_channel_ids_and_catalog_keys_are_unchanged(self):
        for category, canonical, team_id, catalog_key in TEAM_IDENTITY_2_0_0:
            with self.subTest(category=category, team=canonical):
                record = next(r for r in STATIC_TEAM_RECORDS if r.canonical == canonical)
                if category in ("ncaaf", "ncaam"):
                    record = record.for_category(category)
                entry = _team_catalog_entry(category, record)
                self.assertEqual(entry["team_id"], team_id)
                self.assertEqual(entry["catalog_key"], catalog_key)

    def test_special_channel_ids_and_tvg_ids_are_unchanged(self):
        actual = {}
        for channel in SPECIAL_CHANNELS:
            entry = _special_catalog_entry(channel)
            actual[channel.key] = (entry["team_id"], entry["tvg_id"])
        self.assertEqual(actual, SPECIAL_CHANNELS_2_0_0)


# --- M3U / XMLTV --------------------------------------------------------------

HOST = "jellyball.example.test:8000"

M3U_2_0_0 = "\n".join([
    "#EXTM3U",
    '#EXTINF:-1 tvg-id="nfl_buf" tvg-name="Buffalo Bills" '
    'tvg-logo="https://a.espncdn.com/i/teamlogos/nfl/500/buf.png?v=titan2" group-title="NFL",Buffalo Bills',
    f"http://{HOST}/stream/nfl_buf.m3u8",
    '#EXTINF:-1 tvg-id="ncaaf_130" tvg-name="Michigan Wolverines (Football)" '
    'tvg-logo="https://a.espncdn.com/i/teamlogos/ncaa/500/130.png?v=titan2" '
    'group-title="College Football",Michigan Wolverines (Football)',
    f"http://{HOST}/stream/ncaaf_130.m3u8",
    '#EXTINF:-1 tvg-id="ESPN.us" tvg-name="ESPN" '
    'tvg-logo="https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/united-states/espn-us.png" '
    'group-title="24/7 Sports",ESPN',
    f"http://{HOST}/stream/special_espn.m3u8",
    '#EXTINF:-1 tvg-id="mv_demo" tvg-name="Multi-View Demo" group-title="Multi-View",Multi-View Demo',
    f"http://{HOST}/stream/mv_demo.m3u8",
    '#EXTINF:-1 tvg-id="mv_demo.nfl_buf.audio" tvg-name="\U0001f50a Buffalo Bills · Multi-View Demo" '
    'group-title="Multi-View",\U0001f50a Buffalo Bills · Multi-View Demo',
    f"http://{HOST}/multiview/mv_demo/audio-0.m3u8",
    '#EXTINF:-1 tvg-id="mv_demo.ncaaf_130.audio" '
    'tvg-name="\U0001f50a Michigan Wolverines (Football) · Multi-View Demo" '
    'group-title="Multi-View",\U0001f50a Michigan Wolverines (Football) · Multi-View Demo',
    f"http://{HOST}/multiview/mv_demo/audio-1.m3u8",
])


def _fixture_channels() -> dict:
    return {
        "nfl_buf": new_channel_state(
            name="Buffalo Bills", query="Buffalo Bills", category="nfl", source_id="buf",
            catalog_key="team:nfl:buf",
        ),
        "ncaaf_130": new_channel_state(
            name="Michigan Wolverines (Football)", query="michigan wolverines", category="ncaaf",
            source_id="130", catalog_key="team:ncaaf:130",
        ),
        "special_espn": new_channel_state(
            name="ESPN", query="ESPN", category="special", catalog_key="special:espn", always_live=True,
        ),
        "mv_demo": new_channel_state(
            name="Multi-View Demo", query="", type="multiview", candidates=[{"synthetic": True}],
            category="multiview", content_type="multiview", always_live=True, tvg_id="mv_demo",
            group_title="Multi-View", layout="side_by_side_2",
            member_team_ids=["nfl_buf", "ncaaf_130"], active_audio_team_id="nfl_buf",
        ),
    }


class GuideContractTests(unittest.TestCase):
    def setUp(self):
        self._backup = dict(state.stream_state)
        state.stream_state.clear()
        state.stream_state.update(_fixture_channels())

    def tearDown(self):
        state.stream_state.clear()
        state.stream_state.update(self._backup)

    def _m3u(self) -> str:
        return asyncio.run(epg.generate_m3u(SimpleNamespace(headers={"host": HOST})))

    def _xmltv(self) -> ET.Element:
        async def no_tvguide():
            return {}

        with patch.object(epg, "_fetch_tvguide_epg", no_tvguide):
            return ET.fromstring(asyncio.run(epg.generate_xmltv()))

    def test_m3u_is_byte_for_byte_the_2_0_0_playlist(self):
        self.assertEqual(self._m3u(), M3U_2_0_0)

    def test_stream_urls_keep_the_m3u8_extension(self):
        """The extension makes Jellyfin treat the URL as an HLS manifest."""
        urls = [line for line in self._m3u().splitlines() if line.startswith("http")]
        self.assertEqual(len(urls), 6)
        self.assertTrue(all(url.endswith(".m3u8") for url in urls))

    def test_xmltv_channel_ids_and_names_are_unchanged(self):
        root = self._xmltv()
        self.assertEqual(root.tag, "tv")
        channels = [(c.get("id"), c.findtext("display-name")) for c in root.findall("channel")]
        self.assertEqual(channels, [
            ("nfl_buf", "Buffalo Bills"),
            ("ncaaf_130", "Michigan Wolverines (Football)"),
            ("ESPN.us", "ESPN"),
            ("mv_demo", "Multi-View Demo"),
            ("mv_demo.nfl_buf.audio", "\U0001f50a Buffalo Bills · Multi-View Demo"),
            ("mv_demo.ncaaf_130.audio", "\U0001f50a Michigan Wolverines (Football) · Multi-View Demo"),
        ])

    def test_every_listed_channel_has_well_formed_programmes(self):
        root = self._xmltv()
        channel_ids = {c.get("id") for c in root.findall("channel")}
        stamp = re.compile(r"^\d{14} \+0000$")
        seen = set()
        for programme in root.findall("programme"):
            seen.add(programme.get("channel"))
            start, stop = programme.get("start"), programme.get("stop")
            self.assertRegex(start, stamp)
            self.assertRegex(stop, stamp)
            self.assertLess(start, stop)
            self.assertTrue((programme.findtext("title") or "").strip())
        self.assertEqual(seen, channel_ids)


# --- public routes ------------------------------------------------------------

# (method, path) pairs that Jellyfin, an external EPG mapper or a monitoring tool
# may call. Dashboard form targets are internal and deliberately not frozen. The
# FastAPI /docs, /redoc and /openapi.json pages are not frozen either: they are
# scheduled to be closed.
PUBLIC_ROUTES_2_0_0 = {
    ("GET", "/playlist.m3u"), ("HEAD", "/playlist.m3u"),
    ("GET", "/epg.xml"), ("HEAD", "/epg.xml"),
    ("GET", "/stream/{team_id}"), ("HEAD", "/stream/{team_id}"),
    ("GET", "/stream/{team_id}.m3u8"), ("HEAD", "/stream/{team_id}.m3u8"),
    ("GET", "/stream/{team_id}/seg/{seq}.ts"), ("HEAD", "/stream/{team_id}/seg/{seq}.ts"),
    ("GET", "/multiview/{channel_id}/audio-{audio_index}.m3u8"),
    ("HEAD", "/multiview/{channel_id}/audio-{audio_index}.m3u8"),
    ("GET", "/multiview/{channel_id}/audio-{audio_index}/seg/{seq}.ts"),
    # Deprecated 2.0.0 aliases, kept until Jellyfin's guide data has been refreshed.
    ("GET", "/multiview/{channel_id}/audio/{audio_index}.m3u8"),
    ("HEAD", "/multiview/{channel_id}/audio/{audio_index}.m3u8"),
    ("GET", "/multiview/{channel_id}/audio/{audio_index}/seg/{seq}.ts"),
    # Signed relay for sources the session engine cannot take.
    ("GET", "/substream.m3u8"), ("GET", "/resource"), ("GET", "/chunk"),
    ("GET", "/chunk.ts"), ("GET", "/chunk.mp4"), ("GET", "/chunk.aac"), ("GET", "/chunk.vtt"),
    # Machine-readable endpoints documented in the README.
    ("GET", "/healthz"), ("GET", "/metrics"), ("GET", "/api/status"), ("GET", "/api/sessions"),
    ("GET", "/api/ffmpeg-status"), ("GET", "/api/version"), ("GET", "/api/logs"),
    ("GET", "/"),
}


class RouteContractTests(unittest.TestCase):
    def test_every_2_0_0_public_route_is_still_registered(self):
        registered = set()
        for route in main.app.routes:
            for method in getattr(route, "methods", None) or ():
                registered.add((method, route.path))
        self.assertEqual(sorted(PUBLIC_ROUTES_2_0_0 - registered), [])


# --- database upgrades --------------------------------------------------------

# The 2.0.0 layout, verbatim from init_db() at the v2.0.0 tag. Do not edit it to
# follow a schema change: a later migration must upgrade this layout unchanged.
SCHEMA_2_0_0 = [
    """CREATE TABLE teams
       (team_id TEXT PRIMARY KEY, name TEXT, query TEXT,
        logo_url TEXT DEFAULT '', start_time TEXT DEFAULT '', stop_time TEXT DEFAULT '',
        category TEXT DEFAULT 'custom', source_id TEXT DEFAULT '',
        content_type TEXT DEFAULT 'team', search_terms TEXT DEFAULT '',
        always_live INTEGER DEFAULT 0, catalog_key TEXT DEFAULT '',
        is_favorite INTEGER DEFAULT 0, auto_disable_after TEXT DEFAULT '')""",
    """CREATE TABLE stream_events
       (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        team_id TEXT, provider TEXT, event_type TEXT, details TEXT)""",
    "CREATE TABLE app_settings (key TEXT PRIMARY KEY, value TEXT)",
    """CREATE TABLE cache_metrics
       (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        hits INTEGER DEFAULT 0, misses INTEGER DEFAULT 0)""",
    """CREATE TABLE playback_events
       (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        team_id TEXT, duration_seconds INTEGER, success INTEGER DEFAULT 1)""",
    """CREATE TABLE provider_performance
       (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        provider TEXT, response_time_ms INTEGER, success INTEGER DEFAULT 1)""",
    """CREATE TABLE stream_test_results
       (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        team_id TEXT, is_live INTEGER DEFAULT 0, candidate_count INTEGER DEFAULT 0)""",
    """CREATE TABLE multiview_channels
       (channel_id TEXT PRIMARY KEY, name TEXT NOT NULL, layout TEXT NOT NULL DEFAULT 'grid_2x2',
        member_team_ids TEXT NOT NULL DEFAULT '[]', active_audio_team_id TEXT DEFAULT '',
        tvg_id TEXT DEFAULT '', group_title TEXT DEFAULT '', logo_url TEXT DEFAULT '')""",
    "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT)",
    "INSERT INTO schema_migrations VALUES (1, '2026-09-27T00:00:00+00:00')",
    "INSERT INTO schema_migrations VALUES (2, '2026-09-27T00:00:01+00:00')",
]

# load_teams() tuple order: team_id, name, query, logo_url, start_time, stop_time,
# category, source_id, content_type, search_terms, always_live, catalog_key,
# auto_disable_after.
TEAM_ROWS_2_0_0 = [
    ("nfl_buf", "Buffalo Bills", "Buffalo Bills", "https://a.espncdn.com/i/teamlogos/nfl/500/buf.png?v=titan2",
     "20260927170000 +0000", "20260927210000 +0000", "nfl", "buf", "team",
     '["buffalo bills", "bills"]', 0, "team:nfl:buf", "2026-12-31"),
    ("special_espn", "ESPN", "ESPN", "", "", "", "special", "", "channel",
     '["espn"]', 1, "special:espn", ""),
    ("my_custom_team", "My Custom Team", "my custom team", "", "", "", "custom", "", "team",
     "", 0, "", ""),
]

MULTIVIEW_ROW_2_0_0 = (
    "mv_demo", "Multi-View Demo", "side_by_side_2", '["nfl_buf", "special_espn"]', "nfl_buf",
    "mv_demo", "Multi-View", "",
)

SETTINGS_2_0_0 = {
    "jellyfin_url": "http://jellyfin.example.test:8096",
    "jellyfin_task_id": "abc123",
    "discord_webhook_url": "https://discord.example.test/api/webhooks/1/token",
    "update_check_enabled": "1",
    "tunable:SESSION_WINDOW_SECONDS": "45",
}


def _build_2_0_0_database(path: str) -> None:
    conn = sqlite3.connect(path)
    try:
        for statement in SCHEMA_2_0_0:
            conn.execute(statement)
        conn.executemany(
            """INSERT INTO teams (team_id, name, query, logo_url, start_time, stop_time, category,
                                  source_id, content_type, search_terms, always_live, catalog_key,
                                  auto_disable_after, is_favorite)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)""",
            TEAM_ROWS_2_0_0,
        )
        conn.execute("INSERT INTO multiview_channels VALUES (?, ?, ?, ?, ?, ?, ?, ?)", MULTIVIEW_ROW_2_0_0)
        conn.executemany("INSERT INTO app_settings VALUES (?, ?)", list(SETTINGS_2_0_0.items()))
        conn.execute("INSERT INTO stream_events (team_id, provider, event_type, details) VALUES ('nfl_buf', 'X', 'failover', 'stalled')")
        conn.execute("INSERT INTO provider_performance (provider, response_time_ms, success) VALUES ('X', 120, 1)")
        conn.execute("INSERT INTO playback_events (team_id, duration_seconds) VALUES ('nfl_buf', 300)")
        conn.commit()
    finally:
        conn.close()


class DatabaseUpgradeTests(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self._tmpdir, "existing.db")

    def tearDown(self):
        db.close_all_db_connections()
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _upgrade(self):
        with patch.object(db, "DB_FILE", self.db_path):
            db.init_db()

    def _columns(self, table):
        conn = sqlite3.connect(self.db_path)
        try:
            return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        finally:
            conn.close()

    def test_a_2_0_0_database_upgrades_without_losing_anything(self):
        _build_2_0_0_database(self.db_path)
        self._upgrade()
        with patch.object(db, "DB_FILE", self.db_path):
            self.assertEqual(sorted(db.load_teams()), sorted(TEAM_ROWS_2_0_0))
            self.assertEqual(db.load_multiview_channels(), [MULTIVIEW_ROW_2_0_0])
            for key, value in SETTINGS_2_0_0.items():
                self.assertEqual(db.get_setting(key), value, key)
            with db._db_session() as conn:
                versions = {row[0] for row in conn.execute("SELECT version FROM schema_migrations")}
                counts = {
                    table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    for table in ("stream_events", "provider_performance", "playback_events")
                }
        self.assertTrue({1, 2} <= versions)
        self.assertEqual(counts, {"stream_events": 1, "provider_performance": 1, "playback_events": 1})

    def test_upgrading_twice_changes_nothing(self):
        _build_2_0_0_database(self.db_path)
        self._upgrade()
        with patch.object(db, "DB_FILE", self.db_path):
            first = (sorted(db.load_teams()), db.load_multiview_channels())
        self._upgrade()
        with patch.object(db, "DB_FILE", self.db_path):
            second = (sorted(db.load_teams()), db.load_multiview_channels())
        self.assertEqual(first, second)

    def test_a_1_x_shaped_database_gains_every_column_and_keeps_its_rows(self):
        """A database from before migrations existed: no schema_migrations table and a
        teams table with only the original columns."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("CREATE TABLE teams (team_id TEXT PRIMARY KEY, name TEXT, query TEXT)")
            conn.execute("INSERT INTO teams VALUES ('old_team', 'Old Team', 'old team')")
            conn.execute("CREATE TABLE app_settings (key TEXT PRIMARY KEY, value TEXT)")
            conn.execute("INSERT INTO app_settings VALUES ('jellyfin_api_key', 'kept')")
            conn.commit()
        finally:
            conn.close()
        self._upgrade()
        self.assertTrue(
            {"logo_url", "start_time", "stop_time", "category", "source_id", "content_type", "search_terms",
             "always_live", "catalog_key", "is_favorite", "auto_disable_after"} <= self._columns("teams")
        )
        with patch.object(db, "DB_FILE", self.db_path):
            rows = db.load_teams()
            self.assertEqual(db.get_setting("jellyfin_api_key"), "kept")
        self.assertEqual(len(rows), 1)
        team_id, name, query, logo_url, start, stop, category, source_id, content_type, terms, live, key, disable = rows[0]
        self.assertEqual((team_id, name, query), ("old_team", "Old Team", "old team"))
        self.assertEqual((category, content_type, live, key, disable), ("custom", "team", 0, "", ""))


if __name__ == "__main__":
    unittest.main()
