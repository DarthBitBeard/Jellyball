"""The database is copied aside before its schema is migrated (Phase A, P9)."""

import os
import shutil
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import db
from version import __version__


def _migration_99(conn):
    conn.execute("CREATE TABLE IF NOT EXISTS migrated_99 (id INTEGER)")


class DatabaseBackupTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp, "sports_proxy.db")
        self.backup_name = f"sports_proxy.db.bak-{__version__}"
        self._db_patch = patch.object(db, "DB_FILE", self.db_path)
        self._db_patch.start()

    def tearDown(self):
        self._db_patch.stop()
        db.close_all_db_connections()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def backups(self):
        return sorted(p.name for p in Path(self.tmp).glob("sports_proxy.db.bak-*"))

    @staticmethod
    def seed():
        db.save_team("nfl_buf", "Buffalo Bills", "Buffalo Bills", category="nfl", source_id="buf",
                     catalog_key="team:nfl:buf")
        db.set_setting("jellyfin_url", "http://jellyfin.example.test:8096")

    @staticmethod
    def with_pending_migration():
        return patch.object(db, "SCHEMA_MIGRATIONS", db.SCHEMA_MIGRATIONS + [(99, _migration_99)])

    def read_backup(self, name):
        conn = sqlite3.connect(os.path.join(self.tmp, name))
        try:
            return {
                "teams": [row[0] for row in conn.execute("SELECT team_id FROM teams")],
                "settings": dict(conn.execute("SELECT key, value FROM app_settings").fetchall()),
                "versions": {row[0] for row in conn.execute("SELECT version FROM schema_migrations")},
                "tables": {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")},
            }
        finally:
            conn.close()

    def test_a_first_start_on_a_new_database_makes_no_backup(self):
        db.init_db()
        self.assertEqual(self.backups(), [])

    def test_a_pending_migration_on_an_existing_database_is_backed_up_first(self):
        db.init_db()
        self.seed()
        with self.with_pending_migration():
            db.init_db()
        self.assertEqual(self.backups(), [self.backup_name])

        before = self.read_backup(self.backup_name)
        self.assertEqual(before["teams"], ["nfl_buf"])
        self.assertEqual(before["settings"]["jellyfin_url"], "http://jellyfin.example.test:8096")
        # The copy is the state *before* the migration ...
        self.assertNotIn(99, before["versions"])
        self.assertNotIn("migrated_99", before["tables"])
        # ... and the live database did migrate.
        with db._db_session() as conn:
            self.assertIn(99, {row[0] for row in conn.execute("SELECT version FROM schema_migrations")})

    def test_no_backup_when_nothing_is_pending(self):
        db.init_db()
        self.seed()
        db.init_db()
        self.assertEqual(self.backups(), [])

    def test_a_pre_migration_database_is_backed_up_in_its_old_shape(self):
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("CREATE TABLE teams (team_id TEXT PRIMARY KEY, name TEXT, query TEXT)")
            conn.execute("INSERT INTO teams VALUES ('old_team', 'Old Team', 'old team')")
            conn.commit()
        finally:
            conn.close()
        db.init_db()
        self.assertEqual(self.backups(), [self.backup_name])
        backup = sqlite3.connect(os.path.join(self.tmp, self.backup_name))
        try:
            columns = [row[1] for row in backup.execute("PRAGMA table_info(teams)")]
            rows = backup.execute("SELECT team_id FROM teams").fetchall()
        finally:
            backup.close()
        self.assertEqual(columns, ["team_id", "name", "query"])
        self.assertEqual(rows, [("old_team",)])
        with db._db_session() as live:
            live_columns = {row[1] for row in live.execute("PRAGMA table_info(teams)")}
        self.assertIn("catalog_key", live_columns)

    def test_the_first_backup_made_by_a_version_is_not_overwritten(self):
        db.init_db()
        self.seed()
        Path(self.tmp, self.backup_name).write_bytes(b"the original backup")
        with self.with_pending_migration():
            db.init_db()
        self.assertEqual(Path(self.tmp, self.backup_name).read_bytes(), b"the original backup")

    def test_only_the_newest_backups_are_kept(self):
        db.init_db()
        self.seed()
        base = time.time() - 1000
        for index in range(5):
            path = Path(self.tmp, f"sports_proxy.db.bak-1.0.{index}")
            path.write_bytes(b"old")
            os.utime(path, (base + index, base + index))
        with self.with_pending_migration():
            db.init_db()
        self.assertEqual(
            self.backups(),
            sorted([self.backup_name, "sports_proxy.db.bak-1.0.4", "sports_proxy.db.bak-1.0.3"]),
        )

    def test_a_failed_backup_is_logged_and_does_not_block_the_upgrade(self):
        db.init_db()
        self.seed()
        with self.with_pending_migration(), \
                patch.object(db.os, "replace", side_effect=OSError("disk full")), \
                self.assertLogs(db.LOGGER, level="WARNING") as logs:
            db.init_db()
        self.assertEqual(self.backups(), [])
        self.assertEqual(list(Path(self.tmp).glob("*.part")), [], "no half-written backup is left behind")
        self.assertTrue(any("back up the database" in line for line in logs.output))
        with db._db_session() as conn:
            self.assertIn(99, {row[0] for row in conn.execute("SELECT version FROM schema_migrations")})


if __name__ == "__main__":
    unittest.main()
