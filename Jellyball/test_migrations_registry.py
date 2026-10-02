"""The schema-migration registry: each feature lane owns a reserved range of
version numbers in its own migrations_<lane>.py, and db folds them all into
one ordered list (Phase A pre-wiring, so parallel lanes never collide)."""

import os
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import db

HERE = Path(__file__).resolve().parent


def _lane_versions():
    """{lane: [versions]} for core and every lane module."""
    lanes = {"core": [version for version, _ in db.CORE_MIGRATIONS]}
    for lane, module in db._LANE_MIGRATION_MODULES.items():
        lanes[lane] = [version for version, _ in module.MIGRATIONS]
    return lanes


class MigrationRegistryTests(unittest.TestCase):
    def test_no_version_number_is_used_twice(self):
        seen = {}
        for lane, versions in _lane_versions().items():
            for version in versions:
                self.assertNotIn(
                    version, seen,
                    f"migration version {version} is used by both {seen.get(version)!r} and {lane!r}",
                )
                seen[version] = lane

    def test_every_lane_stays_inside_its_reserved_range(self):
        for lane, versions in _lane_versions().items():
            low, high = db.MIGRATION_RANGES[lane]
            for version in versions:
                self.assertTrue(
                    low <= version <= high,
                    f"{lane} migration {version} is outside its reserved range {low}-{high}",
                )

    def test_reserved_ranges_are_valid_and_do_not_overlap(self):
        ranges = sorted(db.MIGRATION_RANGES.items(), key=lambda item: item[1])
        for _, (low, high) in ranges:
            self.assertLessEqual(low, high)
        for (lane_a, (_, high_a)), (lane_b, (low_b, _)) in zip(ranges, ranges[1:]):
            self.assertLess(high_a, low_b, f"ranges of {lane_a} and {lane_b} overlap")

    def test_every_lane_has_a_range_and_a_module_and_the_reverse(self):
        self.assertEqual(set(db.MIGRATION_RANGES), {"core", *db._LANE_MIGRATION_MODULES})

    def test_every_migrations_module_in_the_tree_is_wired_into_db(self):
        # A new migrations_<lane>.py that db does not import would be silently
        # skipped on every real database.
        on_disk = {path.stem for path in HERE.glob("migrations_*.py")}
        wired = {module.__name__ for module in db._LANE_MIGRATION_MODULES.values()}
        self.assertEqual(on_disk, wired)

    def test_migrations_are_callable_with_integer_versions(self):
        for version, migration in db._collect_migrations():
            self.assertIsInstance(version, int)
            self.assertTrue(callable(migration), f"migration {version} is not callable")

    def test_the_assembled_list_is_every_lane_in_version_order(self):
        expected = sorted(v for versions in _lane_versions().values() for v in versions)
        self.assertEqual([version for version, _ in db.SCHEMA_MIGRATIONS], expected)

    def test_lane_modules_do_not_import_db(self):
        # db imports them; the reverse import would be circular.
        for module in db._LANE_MIGRATION_MODULES.values():
            source = Path(module.__file__).read_text(encoding="utf-8")
            self.assertNotRegex(source, r"(?m)^\s*(import db\b|from db import)", module.__name__)


class LaneMigrationsRunTests(unittest.TestCase):
    """A migration added to a lane module is applied, once, in version order."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp, "sports_proxy.db")
        self._db_patch = patch.object(db, "DB_FILE", self.db_path)
        self._db_patch.start()
        self.calls = []

    def tearDown(self):
        self._db_patch.stop()
        db.close_all_db_connections()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _fake(self, name):
        def migration(conn):
            self.calls.append(name)
            conn.execute(f"CREATE TABLE IF NOT EXISTS {name} (id INTEGER)")
        return migration

    def test_lane_migrations_are_applied_in_version_order_and_recorded(self):
        sports = [(201, self._fake("sports_two")), (200, self._fake("sports_one"))]
        jellyfin = [(100, self._fake("jellyfin_one"))]
        with patch.object(db.migrations_sports, "MIGRATIONS", sports), \
                patch.object(db.migrations_jellyfin, "MIGRATIONS", jellyfin), \
                patch.object(db, "SCHEMA_MIGRATIONS", db._collect_migrations()):
            db.init_db()
            db.init_db()  # a second start applies nothing again

        self.assertEqual(self.calls, ["jellyfin_one", "sports_one", "sports_two"])
        conn = sqlite3.connect(self.db_path)
        try:
            versions = [row[0] for row in conn.execute("SELECT version FROM schema_migrations ORDER BY version")]
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        finally:
            conn.close()
        self.assertEqual(versions, [1, 2, 100, 200, 201])
        self.assertTrue({"jellyfin_one", "sports_one", "sports_two"} <= tables)


if __name__ == "__main__":
    unittest.main()
