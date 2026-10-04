"""Schema migrations owned by the provider reliability lane (versions 300-399).

Add a (version, function) pair to MIGRATIONS; db.py folds every lane's list
into SCHEMA_MIGRATIONS and applies the pending ones in version order. Rules:

- Stay inside this lane's range (db.MIGRATION_RANGES). A test enforces it, so
  two lanes can never pick the same number.
- A function receives the open sqlite3 connection and must be idempotent
  (CREATE TABLE IF NOT EXISTS, a column check before ALTER TABLE): a fresh
  database runs every migration too.
- Never edit or renumber a migration that has shipped. Add a new one.
- This module must not import db (db imports it).
"""

import sqlite3


def _add_missing_columns(conn: sqlite3.Connection, table: str, columns: dict) -> None:
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for name, ddl in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


def _provider_performance_outcomes(conn: sqlite3.Connection) -> None:
    """R1: say *what* happened on each search, not just success/failure.

    outcome: ok / empty / timeout / error. index_events is how many events the
    provider's index page listed (the dead-site signal); matches is how many
    of them matched the team (zero for one team is normal).
    """
    _add_missing_columns(conn, "provider_performance", {
        "outcome": "TEXT",
        "error_class": "TEXT",
        "index_events": "INTEGER",
        "matches": "INTEGER",
    })


def _provider_settings_table(conn: sqlite3.Connection) -> None:
    """R5: per-provider enable/disable and priority, edited from the dashboard."""
    conn.execute(
        """CREATE TABLE IF NOT EXISTS provider_settings
           (name TEXT PRIMARY KEY,
            enabled INTEGER NOT NULL DEFAULT 1,
            priority INTEGER)"""
    )


MIGRATIONS: list = [
    (300, _provider_performance_outcomes),
    (301, _provider_settings_table),
]
