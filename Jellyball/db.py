"""SQLite persistence: connections, schema/migrations, settings, channel and
Multi-View rows, and the metrics tables (batched writer + dashboard queries).

DB_FILE is read at call time as a module global (tests patch `db.DB_FILE`).
"""

import asyncio
import json
import logging
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from config import _log_failure, DATA_DIR, LOGGER
import migrations_engine
import migrations_jellyfin
import migrations_providers
import migrations_setup
import migrations_sports
from version import __version__


SQLITE_BUSY_TIMEOUT_MS = 5000
# How many pre-migration database backups to keep (see _backup_before_migrating).
BACKUPS_TO_KEEP = 3

_configured_db = Path(os.getenv("DB_FILE", "sports_proxy.db"))
DB_FILE = str(_configured_db if _configured_db.is_absolute() else DATA_DIR / _configured_db)

_DB_CONNECTIONS_LOCK = threading.Lock()
# Keyed by (thread id, DB path) rather than just thread id, so a thread that
# later sees a different DB_FILE (as tests do via
# `patch.object(db, "DB_FILE", ...)`) gets a fresh connection to the new
# file instead of reusing a stale one pointed at the old path.
_DB_CONNECTIONS: Dict[Tuple[int, str], sqlite3.Connection] = {}


def _open_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, timeout=SQLITE_BUSY_TIMEOUT_MS / 1000)
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _connect_db() -> sqlite3.Connection:
    """Return a consistently configured SQLite connection for `DB_FILE`.

    Reuses one connection per (thread, DB path) instead of opening a fresh
    connection (and re-running its PRAGMAs) on every call — this sits on the
    hot get_setting()/set_setting() path, invoked via asyncio.to_thread on
    every dashboard/API request. A cached connection that turns out closed or
    broken is transparently dropped and reopened.
    """
    key = (threading.get_ident(), DB_FILE)
    with _DB_CONNECTIONS_LOCK:
        conn = _DB_CONNECTIONS.get(key)
    if conn is not None:
        try:
            conn.execute("SELECT 1")
            return conn
        except (sqlite3.ProgrammingError, sqlite3.OperationalError) as exc:
            _log_failure("reuse cached DB connection", exc, logging.DEBUG)
            with _DB_CONNECTIONS_LOCK:
                _DB_CONNECTIONS.pop(key, None)
            try:
                conn.close()
            except Exception:
                pass
    conn = _open_db_connection()
    with _DB_CONNECTIONS_LOCK:
        _DB_CONNECTIONS[key] = conn
    return conn


def close_all_db_connections() -> None:
    """Close every cached per-thread DB connection.

    Called from lifespan shutdown so the process doesn't hold the DB file
    open after serving stops; also useful directly from tests before removing
    a temp DB directory, since a cached connection otherwise keeps that file
    (and, on Windows, its directory) locked open.
    """
    with _DB_CONNECTIONS_LOCK:
        connections = list(_DB_CONNECTIONS.values())
        _DB_CONNECTIONS.clear()
    for conn in connections:
        try:
            conn.close()
        except Exception as exc:
            _log_failure("close cached DB connection", exc, logging.DEBUG)


@contextmanager
def _db_session():
    """Like `_connect_db()` used as a context manager: commits on success and
    rolls back on exception. The underlying connection is cached per
    (thread, DB path) by `_connect_db()` and is deliberately NOT closed here —
    it's handed back out to the next `_db_session()` call on this thread
    instead of being reopened. Commit/rollback still happen explicitly on
    every exit, so nothing depends on connection close to persist writes;
    `close_all_db_connections()` closes the cached connections for real, on
    shutdown or teardown."""
    conn = _connect_db()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def _migrate_add_team_columns(conn: sqlite3.Connection) -> None:
    """Migration 1: add columns to `teams` that older releases lacked.

    Idempotent — a DB whose `teams` table already has every column (a fresh
    install, whose CREATE TABLE above already declares them all, or a DB
    that's already been through this migration) is left untouched instead of
    re-attempting the ALTER TABLE."""
    existing_columns = {row[1] for row in conn.execute("PRAGMA table_info(teams)").fetchall()}
    for col, definition in [
        ("logo_url", "TEXT DEFAULT ''"),
        ("start_time", "TEXT DEFAULT ''"),
        ("stop_time", "TEXT DEFAULT ''"),
        ("category", "TEXT DEFAULT 'custom'"),
        ("source_id", "TEXT DEFAULT ''"),
        ("content_type", "TEXT DEFAULT 'team'"),
        ("search_terms", "TEXT DEFAULT ''"),
        ("always_live", "INTEGER DEFAULT 0"),
        ("catalog_key", "TEXT DEFAULT ''"),
        ("is_favorite", "INTEGER DEFAULT 0"),
        ("auto_disable_after", "TEXT DEFAULT ''"),
    ]:
        if col in existing_columns:
            continue
        try:
            conn.execute(f"ALTER TABLE teams ADD COLUMN {col} {definition}")
        except sqlite3.OperationalError as exc:
            if "duplicate column name" in str(exc).lower():
                LOGGER.debug("Database column already exists: %s", col)
            else:
                _log_failure(f"migrate database column {col}", exc, logging.ERROR)
                raise


def _migrate_add_team_indexes(conn: sqlite3.Connection) -> None:
    """Migration 2: index the `teams` columns filtered/sorted by most often
    (catalog sync lookups by catalog_key; the dashboard's favorites filter)."""
    conn.execute("CREATE INDEX IF NOT EXISTS idx_teams_catalog_key ON teams (catalog_key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_teams_is_favorite ON teams (is_favorite)")


# (version, migration_function) pairs. Append future migrations rather than
# editing an earlier one — each function receives the open connection and
# must be idempotent, since a fresh DB's CREATE TABLE statements above may
# already include what an earlier migration would otherwise add.
CORE_MIGRATIONS = [
    (1, _migrate_add_team_columns),
    (2, _migrate_add_team_indexes),
]

# Feature lanes keep their migrations in their own migrations_<lane>.py and
# own a reserved range of version numbers (inclusive), so work in parallel
# never picks the same number or edits the same list. test_migrations_registry
# checks that every lane stays inside its range and that no version repeats.
MIGRATION_RANGES = {
    "core": (1, 99),
    "jellyfin": (100, 199),
    "sports": (200, 299),
    "providers": (300, 399),
    "setup": (400, 499),
    "engine": (500, 599),
}
_LANE_MIGRATION_MODULES = {
    "jellyfin": migrations_jellyfin,
    "sports": migrations_sports,
    "providers": migrations_providers,
    "setup": migrations_setup,
    "engine": migrations_engine,
}


def _collect_migrations() -> list:
    """Every lane's migrations merged into one list ordered by version."""
    merged = list(CORE_MIGRATIONS)
    for module in _LANE_MIGRATION_MODULES.values():
        merged.extend(module.MIGRATIONS)
    return sorted(merged, key=lambda item: item[0])


SCHEMA_MIGRATIONS = _collect_migrations()


def _database_has_user_data(conn: sqlite3.Connection) -> bool:
    """True when the database already holds someone's data, i.e. it is not the
    empty file a first start has just created."""
    for table in ("teams", "app_settings", "multiview_channels"):
        try:
            if conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone():
                return True
        except sqlite3.OperationalError:
            continue  # table absent (a very old database): nothing to protect there
    return False


def _prune_old_backups(keep: int = BACKUPS_TO_KEEP) -> None:
    db_path = Path(DB_FILE)
    backups = [p for p in db_path.parent.glob(db_path.name + ".bak-*") if not p.name.endswith(".part")]
    backups.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    for stale in backups[keep:]:
        try:
            stale.unlink()
        except OSError as exc:
            _log_failure(f"remove old database backup {stale.name}", exc, logging.WARNING)


def _backup_before_migrating(conn: sqlite3.Connection) -> Optional[str]:
    """Copy the database to `<DB_FILE>.bak-<app version>` before its schema is
    migrated, so an upgrade that goes wrong can be undone by restoring that file
    while the service is stopped. The first backup made by a given app version
    is kept; only the newest BACKUPS_TO_KEEP are retained. A failure is logged
    and does not block the upgrade: migrations are transactional, and a service
    that will not start is worse than a missing safety copy."""
    target = f"{DB_FILE}.bak-{__version__}"
    if os.path.exists(target):
        return target
    partial = target + ".part"
    try:
        destination = sqlite3.connect(partial)
        try:
            conn.backup(destination)
        finally:
            destination.close()
        os.replace(partial, target)
    except Exception as exc:
        _log_failure("back up the database before migrating it", exc, logging.WARNING)
        try:
            os.remove(partial)
        except OSError:
            pass
        return None
    LOGGER.info("Backed up the database to %s before migrating its schema", target)
    _prune_old_backups()
    return target


def _apply_schema_migrations(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT)"
    )
    applied = {row[0] for row in conn.execute("SELECT version FROM schema_migrations").fetchall()}
    pending = [(version, migration) for version, migration in SCHEMA_MIGRATIONS if version not in applied]
    if pending and _database_has_user_data(conn):
        _backup_before_migrating(conn)
    for version, migration in pending:
        migration(conn)
        conn.execute(
            "INSERT OR REPLACE INTO schema_migrations (version, applied_at) VALUES (?, ?)",
            (version, datetime.now(timezone.utc).isoformat()),
        )


def init_db():
    # Late import: scrapers imports db, so it is reached at call time.
    from scrapers import _load_provider_url_overrides
    db_dir = os.path.dirname(DB_FILE)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    with _db_session() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
        conn.execute('''CREATE TABLE IF NOT EXISTS teams
                        (team_id TEXT PRIMARY KEY, name TEXT, query TEXT,
                         logo_url TEXT DEFAULT '', start_time TEXT DEFAULT '', stop_time TEXT DEFAULT '',
                         category TEXT DEFAULT 'custom', source_id TEXT DEFAULT '',
                         content_type TEXT DEFAULT 'team', search_terms TEXT DEFAULT '',
                         always_live INTEGER DEFAULT 0, catalog_key TEXT DEFAULT '',
                         is_favorite INTEGER DEFAULT 0, auto_disable_after TEXT DEFAULT '')''')
        conn.execute('''CREATE TABLE IF NOT EXISTS stream_events 
                        (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                         timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, 
                         team_id TEXT, 
                         provider TEXT, 
                         event_type TEXT, 
                         details TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS app_settings
                        (key TEXT PRIMARY KEY, value TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS cache_metrics
                        (id INTEGER PRIMARY KEY AUTOINCREMENT,
                         timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                         hits INTEGER DEFAULT 0,
                         misses INTEGER DEFAULT 0)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS playback_events
                        (id INTEGER PRIMARY KEY AUTOINCREMENT,
                         timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                         team_id TEXT,
                         duration_seconds INTEGER,
                         success INTEGER DEFAULT 1)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS provider_performance
                        (id INTEGER PRIMARY KEY AUTOINCREMENT,
                         timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                         provider TEXT,
                         response_time_ms INTEGER,
                         success INTEGER DEFAULT 1)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS stream_test_results
                        (id INTEGER PRIMARY KEY AUTOINCREMENT,
                         timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                         team_id TEXT,
                         is_live INTEGER DEFAULT 0,
                         candidate_count INTEGER DEFAULT 0)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS multiview_channels
                        (channel_id TEXT PRIMARY KEY,
                         name TEXT NOT NULL,
                         layout TEXT NOT NULL DEFAULT 'grid_2x2',
                         member_team_ids TEXT NOT NULL DEFAULT '[]',
                         active_audio_team_id TEXT DEFAULT '',
                         tvg_id TEXT DEFAULT '',
                         group_title TEXT DEFAULT '',
                         logo_url TEXT DEFAULT '')''')
        conn.execute("CREATE INDEX IF NOT EXISTS idx_provider_performance_provider_ts ON provider_performance (provider, timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stream_events_provider ON stream_events (provider)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stream_events_timestamp ON stream_events (timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_provider_performance_timestamp ON provider_performance (timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stream_test_results_timestamp ON stream_test_results (timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_playback_events_timestamp ON playback_events (timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cache_metrics_timestamp ON cache_metrics (timestamp)")
        # Migrations (which backfill columns like `auto_disable_after` on a DB
        # that predates them) must run before any index below that references
        # a migrated column, or CREATE INDEX fails with "no such column" on a
        # DB that hasn't been backfilled yet.
        _apply_schema_migrations(conn)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_teams_auto_disable ON teams (auto_disable_after)")
        conn.commit()
    _load_provider_url_overrides()
    LOGGER.info("Database initialized")

def prune_database_logs_once() -> None:
    """Delete old rows from every table that grows without bound, not just stream_events."""
    with _db_session() as conn:
        conn.execute("DELETE FROM stream_events WHERE timestamp < datetime('now', '-7 days')")
        conn.execute("DELETE FROM provider_performance WHERE timestamp < datetime('now', '-7 days')")
        conn.execute("DELETE FROM stream_test_results WHERE timestamp < datetime('now', '-7 days')")
        conn.execute("DELETE FROM playback_events WHERE timestamp < datetime('now', '-30 days')")
        conn.execute("DELETE FROM cache_metrics WHERE timestamp < datetime('now', '-7 days')")
        conn.commit()


async def prune_database_logs():
    while True:
        try:
            await asyncio.to_thread(prune_database_logs_once)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_failure("prune database logs", exc)
        await asyncio.sleep(86400)


def _fetch_expired_scheduled_teams() -> list:
    with _db_session() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT team_id, name FROM teams WHERE auto_disable_after != '' AND auto_disable_after <= date('now')"
        )
        return cursor.fetchall()


def get_setting(key: str, default: str = "") -> str:
    try:
        with _db_session() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM app_settings WHERE key=?", (key,))
            row = cursor.fetchone()
            if row and row[0] is not None:
                return row[0]
    except Exception as exc:
        _log_failure(f"read setting {key}", exc)
    return default

def set_setting(key: str, value: str):
    with _db_session() as conn:
        conn.execute("INSERT OR REPLACE INTO app_settings (key, value) VALUES (?, ?)", (key, value))
        conn.commit()

async def get_setting_async(key: str, default: str = "") -> str:
    """Non-blocking wrapper for database reads."""
    return await asyncio.to_thread(get_setting, key, default)

async def set_setting_async(key: str, value: str) -> None:
    """Non-blocking wrapper for database writes."""
    await asyncio.to_thread(set_setting, key, value)


def save_team(
    team_id: str,
    name: str,
    query: str,
    logo_url: str = "",
    start_time: str = "",
    stop_time: str = "",
    category: str = "custom",
    source_id: str = "",
    content_type: str = "team",
    search_terms: Optional[List[str]] = None,
    always_live: bool = False,
    catalog_key: str = "",
):
    encoded_terms = json.dumps([str(term) for term in (search_terms or []) if str(term).strip()])
    with _db_session() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO teams
            (team_id, name, query, logo_url, start_time, stop_time, category,
             source_id, content_type, search_terms, always_live, catalog_key)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                team_id,
                name,
                query,
                logo_url,
                start_time,
                stop_time,
                category,
                source_id,
                content_type,
                encoded_terms,
                1 if always_live else 0,
                catalog_key,
            ),
        )
        conn.commit()


async def save_team_async(*args, **kwargs) -> None:
    await asyncio.to_thread(save_team, *args, **kwargs)

def update_team_meta(team_id: str, logo_url: str = "", start_time: str = "", stop_time: str = ""):
    try:
        with _db_session() as conn:
            conn.execute("UPDATE teams SET logo_url=?, start_time=?, stop_time=? WHERE team_id=?", (logo_url, start_time, stop_time, team_id))
            conn.commit()
    except Exception as exc:
        _log_failure("update team metadata", exc)


async def update_team_meta_async(team_id: str, logo_url: str = "", start_time: str = "", stop_time: str = "") -> None:
    await asyncio.to_thread(update_team_meta, team_id, logo_url, start_time, stop_time)

def delete_team(team_id: str):
    with _db_session() as conn:
        conn.execute("DELETE FROM teams WHERE team_id=?", (team_id,))
        conn.commit()


async def delete_team_async(team_id: str) -> None:
    await asyncio.to_thread(delete_team, team_id)

def load_teams() -> list:
    with _db_session() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT team_id, name, query, logo_url, start_time, stop_time,
                      category, source_id, content_type, search_terms, always_live, catalog_key,
                      auto_disable_after
               FROM teams"""
        )
        return cursor.fetchall()


def save_multiview_channel(
    channel_id: str,
    name: str,
    layout: str,
    member_team_ids: List[str],
    active_audio_team_id: str = "",
    tvg_id: str = "",
    group_title: str = "",
    logo_url: str = "",
) -> None:
    with _db_session() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO multiview_channels
            (channel_id, name, layout, member_team_ids, active_audio_team_id, tvg_id, group_title, logo_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                channel_id,
                name,
                layout,
                json.dumps(list(member_team_ids)),
                active_audio_team_id,
                tvg_id,
                group_title,
                logo_url,
            ),
        )
        conn.commit()


async def save_multiview_channel_async(*args, **kwargs) -> None:
    await asyncio.to_thread(save_multiview_channel, *args, **kwargs)


def delete_multiview_channel(channel_id: str) -> None:
    with _db_session() as conn:
        conn.execute("DELETE FROM multiview_channels WHERE channel_id=?", (channel_id,))
        conn.commit()


async def delete_multiview_channel_async(channel_id: str) -> None:
    await asyncio.to_thread(delete_multiview_channel, channel_id)


def load_multiview_channels() -> list:
    with _db_session() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT channel_id, name, layout, member_team_ids, active_audio_team_id,
                      tvg_id, group_title, logo_url
               FROM multiview_channels"""
        )
        return cursor.fetchall()


def log_metric_event(team_id: str, provider: str, event_type: str, details: str):
    try:
        with _db_session() as conn:
            conn.execute("INSERT INTO stream_events (team_id, provider, event_type, details) VALUES (?, ?, ?, ?)", (team_id, provider, event_type, details))
            conn.commit()
    except Exception as exc:
        _log_failure("write metric event", exc)


async def log_metric_event_async(*args, **kwargs) -> None:
    """Queue hot-path metric writes without blocking the event loop."""
    await _METRIC_WRITER.enqueue(("stream_event", args, kwargs))


class MetricBatchWriter:
    def __init__(self, max_queue: int = 1000, batch_size: int = 50, flush_seconds: float = 1.0):
        self.queue = asyncio.Queue(maxsize=max_queue)
        self.batch_size = batch_size
        self.flush_seconds = flush_seconds
        self.task: Optional[asyncio.Task] = None

    def start(self) -> None:
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self._run(), name="sqlite metric batch writer")

    async def enqueue(self, item: tuple) -> None:
        # Never block a streaming hot path on metrics: if SQLite falls behind,
        # drop the row instead of stalling the caller.
        self.start()
        try:
            self.queue.put_nowait(item)
        except asyncio.QueueFull:
            LOGGER.debug("Metric queue full; dropping %s row", item[0])

    @staticmethod
    def _write_batch_sync(items: list[tuple]) -> None:
        with _db_session() as conn:
            for kind, args, kwargs in items:
                if kind == "stream_event":
                    team_id, provider, event_type, details = args
                    conn.execute(
                        "INSERT INTO stream_events (team_id, provider, event_type, details) VALUES (?, ?, ?, ?)",
                        (team_id, provider, event_type, details),
                    )
                elif kind == "playback_event":
                    conn.execute("INSERT INTO playback_events (team_id, success) VALUES (?, 1)", (args[0],))
            conn.commit()

    async def _run(self) -> None:
        try:
            while True:
                batch = [await self.queue.get()]
                deadline = time.monotonic() + self.flush_seconds
                while len(batch) < self.batch_size:
                    timeout = max(0.0, deadline - time.monotonic())
                    if timeout == 0:
                        break
                    try:
                        batch.append(await asyncio.wait_for(self.queue.get(), timeout))
                    except asyncio.TimeoutError:
                        break
                await self._write_batch_safely(batch)
        except asyncio.CancelledError:
            remaining = []
            while not self.queue.empty():
                remaining.append(self.queue.get_nowait())
            if remaining:
                await self._write_batch_safely(remaining)
            raise

    async def _write_batch_safely(self, batch: list[tuple]) -> None:
        """A locked or full database must not kill the writer task: that would
        silently drop every later metric and leave queue.join() in stop() hanging
        shutdown forever. Drop the failed batch, log once, keep running."""
        try:
            await asyncio.to_thread(self._write_batch_sync, batch)
        except Exception as exc:
            _log_failure(f"write metric batch ({len(batch)} rows dropped)", exc)
        finally:
            for _ in batch:
                self.queue.task_done()

    async def stop(self, timeout: float = 5.0) -> None:
        if self.task:
            try:
                await asyncio.wait_for(self.queue.join(), timeout)
            except asyncio.TimeoutError:
                LOGGER.warning("Metric writer did not drain within %.0fs; dropping queued rows", timeout)
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            self.task = None


_METRIC_WRITER = MetricBatchWriter()


def _load_dashboard_metrics() -> dict:
    """Single-connection dashboard() helper: stability metrics + favorites + per-provider
    totals in one pass, instead of the previous N+1 query loop over each provider."""
    try:
        with _db_session() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT provider, COUNT(*) FROM stream_events WHERE event_type = 'failover' GROUP BY provider")
            failovers = dict(cursor.fetchall())
            cursor.execute("SELECT event_type, COUNT(*) FROM stream_events GROUP BY event_type")
            summary = dict(cursor.fetchall())
            cursor.execute("SELECT timestamp, team_id, provider, event_type, details FROM stream_events ORDER BY id DESC LIMIT 8")
            events = cursor.fetchall()
            cursor.execute("SELECT team_id FROM teams WHERE is_favorite=1 ORDER BY name")
            favorites = {row[0] for row in cursor.fetchall()}
            cursor.execute("SELECT provider, COUNT(*) FROM stream_events GROUP BY provider")
            provider_totals = dict(cursor.fetchall())
            return {
                "failovers_by_provider": failovers,
                "events_summary": summary,
                "recent_events": events,
                "favorites": favorites,
                "provider_totals": provider_totals,
            }
    except Exception as exc:
        _log_failure("read dashboard metrics", exc)
        return {
            "failovers_by_provider": {}, "events_summary": {}, "recent_events": [],
            "favorites": set(), "provider_totals": {},
        }


async def _load_dashboard_metrics_async() -> dict:
    return await asyncio.to_thread(_load_dashboard_metrics)


def _toggle_favorite_sync(team_id: str) -> None:
    with _db_session() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT is_favorite FROM teams WHERE team_id=?", (team_id,))
        row = cursor.fetchone()
        is_fav = row[0] if row else 0
        new_fav = 1 - is_fav
        conn.execute("UPDATE teams SET is_favorite=? WHERE team_id=?", (new_fav, team_id))
        conn.commit()


def _bulk_set_favorite_sync(ids: List[str], is_favorite: bool) -> None:
    with _db_session() as conn:
        for team_id in ids:
            conn.execute("UPDATE teams SET is_favorite=? WHERE team_id=?", (1 if is_favorite else 0, team_id))
        conn.commit()


def _export_teams_sync() -> list:
    with _db_session() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT team_id, name, query, logo_url, category, is_favorite, catalog_key FROM teams "
            "ORDER BY is_favorite DESC, name"
        )
        return [
            {
                "team_id": row[0], "name": row[1], "query": row[2], "logo_url": row[3],
                "category": row[4], "is_favorite": bool(row[5]), "catalog_key": row[6] or "",
            }
            for row in cursor.fetchall()
        ]


def _performance_stats_sync() -> dict:
    with _db_session() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM playback_events WHERE timestamp > datetime('now', '-1 hour')")
        playback_count = cursor.fetchone()[0] or 0
        cursor.execute("SELECT COUNT(*) FROM stream_events WHERE event_type='failover' AND timestamp > datetime('now', '-1 hour')")
        failover_count = cursor.fetchone()[0] or 0

        # Real rate-limiting signal: a provider whose recent search attempts are mostly
        # failing/timing out (tracked by _track_provider_response_time on every attempt)
        # is a plausible sign it's throttling or banning us, not just a random blip.
        cursor.execute(
            """SELECT provider, SUM(success), COUNT(*) FROM provider_performance
               WHERE timestamp > datetime('now', '-1 hour') GROUP BY provider"""
        )
        hourly = {provider: (successes or 0, total or 0) for provider, successes, total in cursor.fetchall()}

        # Sites that scrape aggregators run on borrow: any of them can go permanently
        # dark overnight (domain seizure, redesign, ownership change) and, unlike an
        # outright exception, a dead site often just returns zero results forever —
        # nothing else would ever flag that. A 5-day window (provider_performance is
        # pruned at 7 days) with zero successes across enough attempts is a much
        # stronger "this is actually gone" signal than the noisy 1-hour rate above.
        cursor.execute(
            """SELECT provider, SUM(success), COUNT(*) FROM provider_performance
               WHERE timestamp > datetime('now', '-5 days') GROUP BY provider"""
        )
        sustained = {provider: (successes or 0, total or 0) for provider, successes, total in cursor.fetchall()}

        provider_health = []
        for provider in set(hourly) | set(sustained):
            h_successes, h_total = hourly.get(provider, (0, 0))
            s_successes, s_total = sustained.get(provider, (0, 0))
            if h_total < 3 and s_total < 10:
                continue  # too few samples in either window to judge
            sustained_failure = s_total >= 10 and s_successes == 0
            entry = {
                "provider": provider,
                "samples_hour": h_total,
                "success_rate": round(h_successes / h_total * 100, 1) if h_total else None,
                "at_risk": h_total >= 3 and (h_successes / h_total) < 0.5,
                "samples_5d": s_total,
                "sustained_failure": sustained_failure,
            }
            provider_health.append(entry)
        # Likely-dead providers first, then worst 1-hour success rate (unrated providers last).
        provider_health.sort(key=lambda p: (
            not p["sustained_failure"],
            p["success_rate"] if p["success_rate"] is not None else 101,
        ))

        return {
            "playback_sessions_hour": playback_count,
            "failovers_hour": failover_count,
            "db_size_mb": round(os.path.getsize(DB_FILE) / 1024 / 1024, 2),
            "provider_health": provider_health,
        }


def _playback_stats_sync() -> dict:
    with _db_session() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT team_id, COUNT(*) as plays FROM playback_events WHERE timestamp > datetime('now', '-7 days') GROUP BY team_id ORDER BY plays DESC LIMIT 5")
        top_teams = [{"team_id": row[0], "plays": row[1]} for row in cursor.fetchall()]
        cursor.execute("SELECT COUNT(*) FROM playback_events WHERE timestamp > datetime('now', '-7 days')")
        total_plays = cursor.fetchone()[0] or 0
        return {"top_watched_teams": top_teams, "total_playbacks_week": total_plays}


def _record_stream_test_sync(team_id: str, is_live: bool, candidate_count: int) -> None:
    with _db_session() as conn:
        conn.execute("INSERT INTO stream_test_results (team_id, is_live, candidate_count) VALUES (?, ?, ?)", (team_id, 1 if is_live else 0, candidate_count))
        conn.commit()


def _schedule_team_disable_sync(team_id: str, disable_date: str) -> None:
    with _db_session() as conn:
        conn.execute("UPDATE teams SET auto_disable_after=? WHERE team_id=?", (disable_date, team_id))
        conn.commit()
