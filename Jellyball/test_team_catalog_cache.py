"""Persisted team catalog (2.2.0): the merged ESPN catalog survives restarts."""

import asyncio
import gc
import os
import shutil
import sqlite3
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

import catalog
import db
import migrations_sports
from sports_catalog import TeamSlug


class TempDbCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self._patch = patch.object(db, "DB_FILE", os.path.join(self.tmp, "t.db"))
        self._patch.start()
        db.init_db()

    def tearDown(self):
        self._patch.stop()
        db.close_all_db_connections()
        gc.collect()
        shutil.rmtree(self.tmp, ignore_errors=True)


def _sample_grouped():
    return {
        "ncf": (
            TeamSlug(canonical="Michigan Wolverines", category="ncf", slug="130",
                     logo_sport="ncaa", aliases=("Wolverines",), team_id="130"),
        ),
        "nfl": (
            TeamSlug(canonical="Buffalo Bills", category="nfl", slug="buf",
                     logo_sport="nfl", aliases=("Bills",), team_id="2"),
        ),
    }


class MigrationTests(TempDbCase):
    def test_migration_creates_the_table_and_is_idempotent(self):
        conn = sqlite3.connect(":memory:")
        migrations_sports._team_catalog_cache_table(conn)
        migrations_sports._team_catalog_cache_table(conn)  # idempotent
        columns = {row[1] for row in conn.execute("PRAGMA table_info(team_catalog_cache)").fetchall()}
        self.assertTrue({"category", "slug", "canonical", "logo_sport", "aliases", "team_id"} <= columns)
        conn.close()

    def test_migration_version_is_in_the_sports_lane_range(self):
        versions = [version for version, _ in migrations_sports.MIGRATIONS]
        self.assertEqual(versions, [200])
        low, high = db.MIGRATION_RANGES["sports"]
        for version in versions:
            self.assertTrue(low <= version <= high)

    def test_fresh_db_has_the_table(self):
        with db._db_session() as conn:
            tables = {row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        self.assertIn("team_catalog_cache", tables)


class PersistLoadTests(TempDbCase):
    def test_persist_and_load_roundtrip(self):
        catalog._persist_team_catalog_sync(_sample_grouped())
        loaded = catalog._load_persisted_team_catalog_sync()
        self.assertEqual(set(loaded), {"ncf", "nfl"})
        record = loaded["ncf"][0]
        self.assertEqual(
            (record.canonical, record.slug, record.logo_sport, record.aliases, record.team_id),
            ("Michigan Wolverines", "130", "ncaa", ("Wolverines",), "130"),
        )

    def test_persist_replaces_previous_rows(self):
        catalog._persist_team_catalog_sync(_sample_grouped())
        catalog._persist_team_catalog_sync({"nfl": _sample_grouped()["nfl"]})
        loaded = catalog._load_persisted_team_catalog_sync()
        self.assertEqual(set(loaded), {"nfl"})

    def test_load_with_no_rows_returns_empty(self):
        self.assertEqual(catalog._load_persisted_team_catalog_sync(), {})


class CatalogFallbackTests(TempDbCase):
    def _reset_catalog_state(self):
        catalog._CATALOG_CACHE = {}
        catalog._CATALOG_CACHE_LOADED_AT = 0.0
        catalog._CATALOG_REMOTE_LOADED = False

    def test_espn_outage_serves_the_persisted_catalog(self):
        catalog._persist_team_catalog_sync(_sample_grouped())
        self._reset_catalog_state()

        async def failing_fetch(category, client):
            return ()

        async def go():
            # The directory fetch is stubbed out, so the HTTP client is never
            # used; stub its construction too (the sandbox proxy env breaks
            # real httpx.AsyncClient() construction).
            with patch.object(catalog, "_fetch_espn_directory", side_effect=failing_fetch), \
                    patch("httpx.AsyncClient", return_value=AsyncMock()):
                return await catalog.get_team_catalog()

        grouped = asyncio.run(go())
        names = {record.canonical for records in grouped.values() for record in records}
        self.assertIn("Michigan Wolverines", names)
        self.assertIn("Buffalo Bills", names)

    def test_no_persisted_catalog_falls_back_to_static_records(self):
        self._reset_catalog_state()

        async def failing_fetch(category, client):
            return ()

        async def go():
            with patch.object(catalog, "_fetch_espn_directory", side_effect=failing_fetch), \
                    patch("httpx.AsyncClient", return_value=AsyncMock()):
                return await catalog.get_team_catalog()

        grouped = asyncio.run(go())
        # Static records still served; nothing persisted, nothing crashed.
        self.assertTrue(any(grouped.values()))


if __name__ == "__main__":
    unittest.main()
