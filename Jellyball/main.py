import os
import sys
import logging
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
import subprocess
import webbrowser
import socket
import shutil
from pathlib import Path
from html import escape as html_escape

# Resolve configuration and writable data independently of the current directory.
# This is important when the application is launched from a Jellyfin service or a shortcut.
APP_DIR = Path(sys.executable if getattr(sys, "frozen", False) else __file__).resolve().parent
BUNDLE_DIR = Path(getattr(sys, "_MEIPASS", APP_DIR))


def _get_writable_data_dir() -> Path:
    if os.name == "nt":
        roots = [os.getenv("LOCALAPPDATA"), os.getenv("APPDATA"), str(Path.home())]
    else:
        roots = [os.getenv("XDG_DATA_HOME"), str(Path.home() / ".local" / "share")]

    for root in roots:
        if not root:
            continue
        candidate = Path(root) / "Jellyball"
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            return candidate
        except OSError:
            continue

    fallback = Path(os.getenv("TEMP", ".")) / "Jellyball"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


DATA_DIR = _get_writable_data_dir()
USER_ENV_FILE = DATA_DIR / ".env"
LOG_FILE = DATA_DIR / "jellyball.log"
LOGGER = logging.getLogger("jellyball")
SQLITE_BUSY_TIMEOUT_MS = 5000

# Configure root/jellyball logger with 3-day timed rotation
_log_handler = TimedRotatingFileHandler(
    filename=str(LOG_FILE),
    when="midnight",
    interval=1,
    backupCount=3,
    encoding="utf-8",
)
_log_handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"))
_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
if not root_logger.handlers:
    root_logger.addHandler(_log_handler)
    root_logger.addHandler(_console_handler)
LOGGER.setLevel(logging.INFO)

# httpx/httpcore log one INFO line per HTTP request by default — with many active
# channels that's every provider probe and every HLS segment fetch, drowning real
# diagnostics in a 3-file rotating log. Keep them quiet unless something fails.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def _log_failure(operation: str, exc: BaseException, level: int = logging.WARNING) -> None:
    """Log a failure without including exception text that may contain secrets or URLs."""
    LOGGER.log(level, "%s failed (%s)", operation, type(exc).__name__)


def _bootstrap_runtime_files() -> None:
    """Create safe, user-writable first-run files without overwriting user data."""
    if not USER_ENV_FILE.exists():
        template = (
            "# Jellyball settings. Add secrets here; this file is stored per Windows user.\n"
            "PORT=8000\n"
            "# DASHBOARD_PASSWORD=change-this-password\n"
            "# STREAM_PROVIDER_PRIORITY=iSportSurge,MyBuffStreams\n"
            "# ACTIVE_HEALTH_INTERVAL=3\n"
            "# STANDBY_HEALTH_INTERVAL=45\n"
            "# PREFETCH_CHUNK_COUNT=5\n"
            "# STREAM_STARTUP_BUFFER_SECONDS=15\n"
        )
        try:
            USER_ENV_FILE.write_text(template, encoding="utf-8")
            try:
                USER_ENV_FILE.chmod(0o600)
            except OSError:
                pass
        except OSError as exc:
            _log_failure("create first-run environment file", exc, logging.ERROR)


# TheTVAppScraper and DaddyLiveScraper are defined later after HtmlAggregatorScraper
# to ensure the base class is available. See their definitions near the other
# aggregator classes.

_bootstrap_runtime_files()

if not os.getenv("PLAYWRIGHT_BROWSERS_PATH"):
    if getattr(sys, "frozen", False):
        bundled_browser_dir = BUNDLE_DIR / "playwright_browsers"
        browser_dir = bundled_browser_dir if bundled_browser_dir.exists() else DATA_DIR / "playwright_browsers"
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(browser_dir)
    else:
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "0"

import asyncio

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import httpx
import re
import urllib.parse
import sqlite3
import json
import secrets
import time
import random
import threading
from collections import OrderedDict, deque
from contextlib import asynccontextmanager, contextmanager
from datetime import date, datetime, timedelta, timezone
from xml.sax.saxutils import escape as xml_escape
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from fastapi import FastAPI, Form, Request, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import PlainTextResponse, Response, HTMLResponse, RedirectResponse, StreamingResponse, JSONResponse, FileResponse
from typing import Dict, List, Optional, Tuple, Set
from playwright.async_api import async_playwright, Browser, Playwright, Page
import pystray
from PIL import Image, ImageDraw

from sports_matcher import get_team_search_terms, match_team, clean_sports_text, canonical_team_name
from sports_catalog import (
    ESPN_DIRECTORY_ENDPOINTS,
    SEASON_WINDOWS,
    SPECIAL_CHANNELS,
    STATIC_TEAM_RECORDS,
    TeamSlug,
    parse_espn_team_directory,
)
from stream_extractor import (
    fetch_streams_from_page,
    fetch_bounded_text,
    playwright_intercept_streams,
    verify_stream_live,
    DEFAULT_USER_AGENT,
    rank_streams,
    is_ignored_url,
)
from network_safety import bounded_float, bounded_int, validate_http_url, validate_http_url_async

# User settings are defaults; a .env beside the executable can override them.
load_dotenv(dotenv_path=USER_ENV_FILE)
load_dotenv(dotenv_path=APP_DIR / ".env", override=True)
PORT = bounded_int(os.getenv("PORT", "8000"), 8000, 1, 65535)
_configured_db = Path(os.getenv("DB_FILE", "sports_proxy.db"))
DB_FILE = str(_configured_db if _configured_db.is_absolute() else DATA_DIR / _configured_db)
_provider_priority = {
    name.strip(): index
    for index, name in enumerate(os.getenv("STREAM_PROVIDER_PRIORITY", "").split(","))
    if name.strip()
}


def _find_available_port(preferred_port: int) -> int:
    candidates = list(range(preferred_port, 65536)) + list(range(1, preferred_port))
    for candidate in candidates:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                probe.bind(("127.0.0.1", candidate))
            return candidate
        except OSError:
            continue
    raise OSError("No available local TCP port found")


def _connect_db() -> sqlite3.Connection:
    """Open a consistently configured SQLite connection for every code path."""
    conn = sqlite3.connect(DB_FILE, timeout=SQLITE_BUSY_TIMEOUT_MS / 1000)
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


@contextmanager
def _db_session():
    """Like `_connect_db()` used as a context manager, but actually closes the
    connection on exit. sqlite3.Connection.__exit__ only commits/rolls back —
    it never closes — so every prior `with _db_session() as conn:` site leaked
    a raw connection for the lifetime of the process."""
    conn = _connect_db()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _validate_upstream_url(value: str, *, allow_private: Optional[bool] = None) -> Optional[str]:
    """Allow safe absolute HTTP(S) upstream URLs for proxy endpoints."""
    return validate_http_url(value, allow_private=allow_private)


def _upstream_media_headers(referer: str = "", origin: str = "") -> Dict[str, str]:
    headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "*/*",
    }
    if referer:
        headers["Referer"] = referer
    if origin:
        safe_origin = _validate_upstream_url(origin)
        if safe_origin:
            parsed_origin = urllib.parse.urlsplit(safe_origin)
            headers["Origin"] = f"{parsed_origin.scheme}://{parsed_origin.netloc}"
    return headers


def _html(value: object) -> str:
    """Escape dynamic values before inserting them into dashboard HTML."""
    return html_escape(str(value or ""), quote=True)


def _safe_team_id(value: str) -> str:
    """Create a stable, non-empty route/database identifier for a team name."""
    team_id = re.sub(r"[^a-zA-Z0-9_]+", "_", (value or "").lower()).strip("_")
    return team_id[:80] or f"team_{secrets.token_hex(4)}"


def _resource_path(relative_path: str) -> Path:
    """Resolve a packaged resource or a source-tree resource."""
    bundle_dir = Path(getattr(sys, "_MEIPASS", APP_DIR))
    bundled = bundle_dir / relative_path
    if bundled.exists():
        return bundled
    return APP_DIR / relative_path

# --- OPTIONAL DEPENDENCIES FALLBACKS ---
try:
    from thefuzz import fuzz
except ImportError:
    from difflib import SequenceMatcher
    class FuzzFallback:
        @staticmethod
        def partial_ratio(s1, s2):
            return int(SequenceMatcher(None, s1.lower(), s2.lower()).find_longest_match(0, len(s1), 0, len(s2)).size / max(len(s1), 1) * 100)
        @staticmethod
        def token_set_ratio(s1, s2):
            t1 = " ".join(sorted(s1.lower().split()))
            t2 = " ".join(sorted(s2.lower().split()))
            return int(SequenceMatcher(None, t1, t2).ratio() * 100)
    fuzz = FuzzFallback()

try:
    from passlib.context import CryptContext
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
except ImportError:
    pwd_context = None
    if os.getenv("DASHBOARD_PASSWORD"):
        LOGGER.error("passlib[bcrypt] is not installed; hashed dashboard passwords cannot be verified")

def is_fuzzy_match(query: str, text: str, threshold: int = 65) -> tuple[bool, int]:
    """Compatibility wrapper that delegates to the robust sports_matcher engine."""
    terms = get_team_search_terms(query, query)
    matched, score, _ = match_team(terms, text, threshold=threshold)
    return matched, score

DASHBOARD_USERNAME = os.getenv("DASHBOARD_USERNAME", "admin")
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "")
security = HTTPBasic(auto_error=False)

def verify_dashboard_auth(credentials: Optional[HTTPBasicCredentials] = Depends(security)):
    if not DASHBOARD_PASSWORD:
        return True
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Basic"},
        )

    is_user_ok = secrets.compare_digest(credentials.username, DASHBOARD_USERNAME)
    is_pass_ok = False

    if pwd_context:
        try:
            is_pass_ok = pwd_context.verify(credentials.password, DASHBOARD_PASSWORD)
        except ValueError:
            is_pass_ok = secrets.compare_digest(credentials.password, DASHBOARD_PASSWORD)
    else:
        is_pass_ok = secrets.compare_digest(credentials.password, DASHBOARD_PASSWORD)

    if not (is_user_ok and is_pass_ok):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return True

def init_db():
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
        conn.execute("CREATE INDEX IF NOT EXISTS idx_teams_auto_disable ON teams (auto_disable_after)")
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
            try:
                conn.execute(f"ALTER TABLE teams ADD COLUMN {col} {definition}")
            except sqlite3.OperationalError as exc:
                if "duplicate column name" in str(exc).lower():
                    LOGGER.debug("Database column already exists: %s", col)
                else:
                    _log_failure(f"migrate database column {col}", exc, logging.ERROR)
                    raise
        conn.commit()
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


async def enforce_scheduled_disables_once() -> None:
    """Remove teams whose scheduled auto-disable date has passed."""
    try:
        expired = await asyncio.to_thread(_fetch_expired_scheduled_teams)
    except Exception as exc:
        _log_failure("query scheduled team disables", exc)
        return
    if not expired:
        return
    for team_id, name in expired:
        try:
            if team_id in stream_state:
                await _stop_team_scrape_loop(team_id)
                del stream_state[team_id]
            await delete_team_async(team_id)
            LOGGER.info("Auto-disabled scheduled team=%s name=%s", team_id, name)
        except Exception as exc:
            _log_failure(f"auto-disable team={team_id}", exc)
    _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after scheduled disable")


async def enforce_scheduled_disables():
    while True:
        try:
            await enforce_scheduled_disables_once()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_failure("scheduled disable enforcement", exc)
        await asyncio.sleep(3600)

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

async def get_notification_config() -> dict:
    return {
        "discord_webhook_url": await get_setting_async("discord_webhook_url", os.getenv("DISCORD_WEBHOOK_URL", "")),
        "telegram_bot_token": await get_setting_async("telegram_bot_token", os.getenv("TELEGRAM_BOT_TOKEN", "")),
        "telegram_chat_id": await get_setting_async("telegram_chat_id", os.getenv("TELEGRAM_CHAT_ID", ""))
    }

async def send_alert(title: str, message: str, level: str = "warning"):
    config = await get_notification_config()
    discord_url = config["discord_webhook_url"]
    if discord_url and not _validate_upstream_url(discord_url):
        LOGGER.warning("Ignoring invalid Discord webhook URL")
        discord_url = ""
    tg_token = config["telegram_bot_token"]
    tg_chat_id = config["telegram_chat_id"]
    
    color_map = {"info": 3066993, "warning": 16753920, "danger": 14431526, "success": 3647337}
    headers = {"User-Agent": "Jellyball-Proxy/1.0"}
    tasks = []
    
    if discord_url:
        embed = {"title": title, "description": message, "color": color_map.get(level, 16753920), "timestamp": datetime.now(timezone.utc).isoformat()}
        async def _send_discord():
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    await client.post(discord_url, json={"embeds": [embed]}, headers=headers)
            except Exception as exc:
                _log_failure("send Discord alert", exc)
        tasks.append(_send_discord())
        
    if tg_token and tg_chat_id:
        tg_url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
        tg_text = f"*{title}*\n{message}"
        async def _send_telegram():
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    await client.post(tg_url, json={"chat_id": tg_chat_id, "text": tg_text, "parse_mode": "Markdown"}, headers=headers)
            except Exception as exc:
                _log_failure("send Telegram alert", exc)
        tasks.append(_send_telegram())
        
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

async def get_jellyfin_config() -> dict:
    return {
        "jellyfin_url": (await get_setting_async("jellyfin_url", os.getenv("JELLYFIN_URL", "http://localhost:8096"))).rstrip("/"),
        "jellyfin_api_key": await get_setting_async("jellyfin_api_key", os.getenv("JELLYFIN_API_KEY", "")),
        "jellyfin_task_id": await get_setting_async("jellyfin_task_id", os.getenv("JELLYFIN_TASK_ID", ""))
    }

async def trigger_jellyfin_refresh() -> bool:
    cfg = await get_jellyfin_config()
    jellyfin_url = _validate_upstream_url(cfg["jellyfin_url"], allow_private=True)
    api_key = cfg["jellyfin_api_key"]
    task_id = cfg["jellyfin_task_id"]

    if not jellyfin_url or not api_key:
        return False

    headers = {"X-Emby-Token": api_key, "User-Agent": "Jellyball-Proxy/1.0"}
    async with httpx.AsyncClient(timeout=8.0) as client:
        if not task_id:
            try:
                tasks_resp = await client.get(f"{jellyfin_url}/ScheduledTasks", headers=headers)
                if tasks_resp.status_code == 200:
                    for t in tasks_resp.json():
                        if t.get("Key") == "RefreshGuide" or "refresh guide" in t.get("Name", "").lower():
                            task_id = t.get("Id", "")
                            await set_setting_async("jellyfin_task_id", task_id)
                            break
            except Exception as exc:
                _log_failure("discover Jellyfin guide task", exc)

        if not task_id:
            return False

        url = f"{jellyfin_url}/ScheduledTasks/Running/{task_id}"
        try:
            response = await client.post(url, headers=headers)
            if response.status_code in [200, 204]:
                await log_metric_event_async("Jellyfin", "API", "guide_refresh", "Triggered Live TV Guide Refresh task")
                return True
            return False
        except Exception as exc:
            _log_failure("trigger Jellyfin guide refresh", exc)
            return False

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


def _record_playback_event_sync(team_id: str) -> None:
    try:
        with _db_session() as conn:
            conn.execute("INSERT INTO playback_events (team_id, success) VALUES (?, 1)", (team_id,))
            conn.commit()
    except Exception as exc:
        _log_failure("record playback event", exc)


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
        self.start()
        await self.queue.put(item)

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
                await asyncio.to_thread(self._write_batch_sync, batch)
                for _ in batch:
                    self.queue.task_done()
        except asyncio.CancelledError:
            remaining = []
            while not self.queue.empty():
                remaining.append(self.queue.get_nowait())
            if remaining:
                await asyncio.to_thread(self._write_batch_sync, remaining)
                for _ in remaining:
                    self.queue.task_done()
            raise

    async def stop(self) -> None:
        if self.task:
            await self.queue.join()
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            self.task = None


_METRIC_WRITER = MetricBatchWriter()


def _maybe_record_playback_event(team_id: str) -> None:
    """Fire-and-forget, de-duplicated so Jellyfin's frequent manifest re-polling
    during one viewing session doesn't produce dozens of rows for it."""
    now = time.monotonic()
    if now - _LAST_PLAYBACK_EVENT.get(team_id, 0.0) < PLAYBACK_EVENT_DEDUPE_SECONDS:
        return
    _LAST_PLAYBACK_EVENT[team_id] = now
    _spawn_background_task(
        _METRIC_WRITER.enqueue(("playback_event", (team_id,), {})),
        f"record playback event team={team_id}",
    )

def get_stability_metrics() -> dict:
    try:
        with _db_session() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT provider, COUNT(*) FROM stream_events WHERE event_type = 'failover' GROUP BY provider")
            failovers = dict(cursor.fetchall())
            cursor.execute("SELECT event_type, COUNT(*) FROM stream_events GROUP BY event_type")
            summary = dict(cursor.fetchall())
            cursor.execute("SELECT timestamp, team_id, provider, event_type, details FROM stream_events ORDER BY id DESC LIMIT 8")
            events = cursor.fetchall()
            return {"failovers_by_provider": failovers, "events_summary": summary, "recent_events": events}
    except Exception as exc:
        _log_failure("read stability metrics", exc)
        return {"failovers_by_provider": {}, "events_summary": {}, "recent_events": []}


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

async def safe_get_content(page: Page, retries: int = 3, delay: float = 1.0) -> str:
    """Safely retrieves page content, waiting out active page navigations."""
    for attempt in range(retries):
        try:
            return await page.content()
        except Exception as exc:
            _log_failure("read Playwright page content", exc)
            if attempt < retries - 1:
                await asyncio.sleep(delay)
            else:
                return ""
    return ""

class BaseProvider:
    name = "Base"
    base_url = ""
    categories: List[str] = []

    def get_scan_urls(self) -> List[str]:
        if not self.base_url:
            return []
        urls = [self.base_url.rstrip("/")]
        base_url = self.base_url.rstrip("/") + "/"
        for cat in self.categories:
            url = urllib.parse.urljoin(base_url, str(cat).lstrip("/"))
            if url not in urls:
                urls.append(url)
        return urls

    async def search(
        self,
        query_or_terms,
        browser: Optional[Browser] = None,
        http_client: Optional[httpx.AsyncClient] = None
    ) -> List[dict]:
        return []


def _is_playlist_request_url(value: str) -> bool:
    parsed = urllib.parse.urlsplit(str(value or ""))
    path = parsed.path.lower()
    if is_ignored_url(value) or path.endswith((
        ".gif", ".png", ".jpg", ".jpeg", ".webp", ".js", ".css",
        ".ts", ".m4s", ".aac", ".mp4",
    )):
        return False
    return (
        path.endswith(".m3u8")
        or "/playlist/" in path
        or "/manifest" in path
        or "/load-playlist" in path
        or "/hls/" in path
        or ".m3u8" in parsed.query.lower()
    )


async def _verify_provider_streams(streams: List[dict], http_client: Optional[httpx.AsyncClient]) -> List[dict]:
    owns_client = http_client is None
    client = http_client or httpx.AsyncClient(follow_redirects=False, timeout=12.0)
    try:
        verify_semaphore = asyncio.Semaphore(VERIFY_STREAM_CONCURRENCY)

        async def verify_one(stream: dict) -> Optional[dict]:
            try:
                async with verify_semaphore:
                    is_live = await verify_stream_live(
                        client,
                        stream["url"],
                        stream.get("referer", ""),
                        timeout=8.0,
                        origin=stream.get("origin", ""),
                    )
            except Exception as exc:
                _log_failure("verify provider stream", exc)
                is_live = False
            return stream if is_live else None

        results = await asyncio.gather(*(verify_one(stream) for stream in streams), return_exceptions=True)
        return [result for result in results if isinstance(result, dict)]
    finally:
        if owns_client:
            await client.aclose()

PROVIDER_EVENT_CONCURRENCY = bounded_int(os.getenv("PROVIDER_EVENT_CONCURRENCY", "6"), 6, 1, 16)
MAX_PROVIDER_EVENTS = bounded_int(os.getenv("MAX_PROVIDER_EVENTS", "60"), 60, 1, 200)
PROVIDER_SEARCH_TIMEOUT = bounded_float(os.getenv("PROVIDER_SEARCH_TIMEOUT", "45"), 45.0, 1.0, 300.0)
PROVIDER_EVENT_TIMEOUT = bounded_float(os.getenv("PROVIDER_EVENT_TIMEOUT", "20"), 20.0, 1.0, 120.0)
PROVIDER_SEARCH_CONCURRENCY = bounded_int(os.getenv("PROVIDER_SEARCH_CONCURRENCY", "6"), 6, 1, 16)
# Caps how many of the global PROVIDER_SEARCH_CONCURRENCY slots a single team's scrape
# can hold at once, so one team querying many slow/timing-out providers can't starve
# every other team's concurrently running scrape of a slot.
PER_TEAM_PROVIDER_CONCURRENCY = bounded_int(os.getenv("PER_TEAM_PROVIDER_CONCURRENCY", "3"), 3, 1, 16)
VERIFY_STREAM_CONCURRENCY = bounded_int(os.getenv("VERIFY_STREAM_CONCURRENCY", "6"), 6, 1, 16)
PROVIDER_BREAKER_FAILURES = bounded_int(os.getenv("PROVIDER_BREAKER_FAILURES", "3"), 3, 1, 20)
PROVIDER_BREAKER_COOLDOWN = bounded_float(os.getenv("PROVIDER_BREAKER_COOLDOWN", "120"), 120.0, 5.0, 3600.0)
MAX_STREAM_CANDIDATES = bounded_int(os.getenv("MAX_STREAM_CANDIDATES", "24"), 24, 1, 200)
_PROVIDER_SEARCH_SEMAPHORE: Optional[asyncio.Semaphore] = None
_PROVIDER_SEARCH_LOOP = None
_PROVIDER_BREAKERS: Dict[str, dict] = {}


def _provider_breaker_open(provider: str) -> bool:
    record = _PROVIDER_BREAKERS.get(provider)
    if not record or record["failures"] < PROVIDER_BREAKER_FAILURES:
        return False
    if time.monotonic() - record["opened_at"] >= PROVIDER_BREAKER_COOLDOWN:
        return False
    return True


def _provider_breaker_success(provider: str) -> None:
    _PROVIDER_BREAKERS.pop(provider, None)


def _provider_breaker_failure(provider: str) -> None:
    record = _PROVIDER_BREAKERS.setdefault(provider, {"failures": 0, "opened_at": 0.0})
    record["failures"] += 1
    if record["failures"] >= PROVIDER_BREAKER_FAILURES:
        record["opened_at"] = time.monotonic()


def _provider_search_semaphore() -> asyncio.Semaphore:
    global _PROVIDER_SEARCH_SEMAPHORE, _PROVIDER_SEARCH_LOOP
    loop = asyncio.get_running_loop()
    if _PROVIDER_SEARCH_SEMAPHORE is None or _PROVIDER_SEARCH_LOOP is not loop:
        _PROVIDER_SEARCH_SEMAPHORE = asyncio.Semaphore(PROVIDER_SEARCH_CONCURRENCY)
        _PROVIDER_SEARCH_LOOP = loop
    return _PROVIDER_SEARCH_SEMAPHORE


class HtmlAggregatorScraper(BaseProvider):
    """Configurable adapter for aggregators that expose linked event pages."""

    def __init__(self, name: str, base_url: str, categories: Optional[List[str]] = None, event_path_hints: Optional[List[str]] = None):
        self.name = name.strip() or "Aggregator"
        self.base_url = (base_url or "").strip().rstrip("/")
        self.categories = list(categories or [])
        self.event_path_hints = tuple(hint.lower() for hint in (event_path_hints or []) if hint)

    @staticmethod
    def _anchor_context(anchor) -> str:
        parts = [
            anchor.get_text(" ", strip=True),
            str(anchor.get("title") or ""),
            str(anchor.get("aria-label") or ""),
        ]
        parent = anchor.parent
        if parent is not None:
            parts.append(parent.get_text(" ", strip=True))
        return " ".join(part for part in parts if part)[:600]

    def _is_event_link(self, href: str, page_url: str) -> bool:
        if not href or href.startswith(("#", "javascript:", "mailto:")):
            return False
        candidate = urllib.parse.urljoin(page_url, href)
        if not _validate_upstream_url(candidate):
            return False
        base_host = urllib.parse.urlparse(self.base_url).netloc.lower()
        if urllib.parse.urlparse(candidate).netloc.lower() != base_host:
            return False
        if candidate.rstrip("/") == page_url.rstrip("/"):
            return False
        if self.event_path_hints and not any(hint in candidate.lower() for hint in self.event_path_hints):
            return False
        return True

    async def _find_matches(self, client: httpx.AsyncClient, search_terms: List[str], browser: Optional[Browser] = None) -> List[tuple[str, int, str]]:
        matches: List[tuple[str, int, str]] = []
        seen_matches: Set[str] = set()
        for page_url in self.get_scan_urls():
            try:
                page_html = await fetch_bounded_text(
                    client,
                    page_url,
                    headers={"User-Agent": DEFAULT_USER_AGENT, "Referer": self.base_url},
                    timeout=8.0,
                )
                
                # Cloudflare / Timeout Bypass via Playwright Context
                if not page_html and browser and browser.is_connected():
                    try:
                        context = await browser.new_context(user_agent=DEFAULT_USER_AGENT)
                        page = await context.new_page()
                        await page.goto(page_url, wait_until="domcontentloaded", timeout=35000)
                        try:
                            await page.wait_for_load_state("networkidle", timeout=8000)
                        except Exception:
                            pass
                        page_html = await page.content()
                        await context.close()
                    except Exception as exc:
                        _log_failure(f"{self.name} Cloudflare bypass for {page_url}", exc)
                        if 'context' in locals(): await context.close()

                if not page_html:
                    continue
                    
                soup = BeautifulSoup(page_html, "html.parser")
                for anchor in soup.find_all("a", href=True):
                    href = str(anchor.get("href") or "")
                    if not self._is_event_link(href, page_url):
                        continue
                    match_url = urllib.parse.urljoin(page_url, href)
                    if match_url in seen_matches:
                        continue
                    title = str(anchor.get("title") or "")
                    raw_text = anchor.get_text(" ", strip=True)
                    direct_text = " ".join(
                        part for part in (raw_text, title, str(anchor.get("aria-label") or ""), href) if part
                    )
                    matched, score, _ = match_team(
                        search_terms,
                        direct_text,
                        href=href,
                        title=title,
                    )
                    if not matched:
                        # Some providers put the team names in the card rather
                        # than the anchor. Only accept that fallback for a
                        # high-confidence full identity, not a generic nickname.
                        candidate_text = self._anchor_context(anchor) or href
                        context_matched, context_score, _ = match_team(search_terms, candidate_text, href=href, title=title)
                        if context_matched and context_score >= 110:
                            matched, score = context_matched, context_score
                    if matched:
                        seen_matches.add(match_url)
                        matches.append((match_url, score, raw_text or title or match_url))
                        if len(matches) >= MAX_PROVIDER_EVENTS:
                            return matches
            except Exception as exc:
                _log_failure(f"scan provider={self.name} page", exc)
        return matches

    async def search(
        self,
        query_or_terms,
        browser: Optional[Browser] = None,
        http_client: Optional[httpx.AsyncClient] = None,
    ) -> List[dict]:
        if not self.base_url or not _validate_upstream_url(self.base_url):
            return []
        search_terms = (
            get_team_search_terms(query_or_terms, query_or_terms)
            if isinstance(query_or_terms, str)
            else list(query_or_terms or [])
        )
        owns_client = http_client is None
        client = http_client or httpx.AsyncClient(follow_redirects=True, timeout=12.0)
        try:
            # Pass the browser object explicitly to the updated _find_matches method
            matched_events = await self._find_matches(client, search_terms, browser=browser)
            event_semaphore = asyncio.Semaphore(PROVIDER_EVENT_CONCURRENCY)

            async def inspect_event(event: tuple[str, int, str]) -> List[dict]:
                match_url, score, match_title = event
                async with event_semaphore:
                    async def inspect() -> List[dict]:
                        event_streams = await fetch_streams_from_page(
                            client, match_url, self.name, score, match_title
                        )
                        for stream in event_streams:
                            stream.setdefault("match_url", match_url)
                        if not event_streams and browser and browser.is_connected():
                            event_streams = await playwright_intercept_streams(
                                browser, match_url, self.name, score, match_title, http_client=client
                            )
                            for stream in event_streams:
                                stream.setdefault("match_url", match_url)
                        return event_streams

                    try:
                        return await asyncio.wait_for(inspect(), timeout=PROVIDER_EVENT_TIMEOUT)
                    except asyncio.TimeoutError:
                        LOGGER.warning("Provider event timed out provider=%s url_host=%s", self.name, urllib.parse.urlparse(match_url).netloc)
                        return []
                    except Exception as exc:
                        _log_failure(f"inspect provider={self.name} event", exc)
                        return []

            results = await asyncio.wait_for(
                asyncio.gather(
                    *(inspect_event(event) for event in matched_events),
                    return_exceptions=True,
                ),
                timeout=PROVIDER_SEARCH_TIMEOUT,
            )
            return [stream for result in results if isinstance(result, list) for stream in result]
        except asyncio.TimeoutError:
            LOGGER.warning("Provider search timed out provider=%s", self.name)
            return []
        finally:
            if owns_client:
                await client.aclose()


# --- Hybrid fast-HTTP and Playwright scrapers ---
class ISportSurgeScraper(HtmlAggregatorScraper):
    def __init__(self):
        super().__init__(
            "iSportSurge",
            os.getenv("AGGREGATOR_1_URL", "https://isportsurge.ws"),
            [
                "/cfb/livestreams2", "/nfl/livestreams3", "/mlb/livestreams2",
                "/nba/livestreams3", "/nhl/livestreams3", "/soccer/livestreams",
            ],
            ["/watch/", "/event/", "/title-game/"],
        )

class MyBuffStreamsScraper(HtmlAggregatorScraper):
    def __init__(self):
        super().__init__(
            "MyBuffStreams",
            os.getenv("AGGREGATOR_2_URL", "https://mybuffstreams.plus"),
            [
                "/cfbstreams2", "/nflstreams2", "/mlb-live-streams",
                "/nbastreams2", "/nhlstreams2", "/soccer-live-streams",
            ],
            ["/cfb/", "/mlb/", "/nfl/", "/nba/", "/nhl/", "/title-game/", "/watch/", "/soccer/"],
        )


class MethStreamsScraper(HtmlAggregatorScraper):
    def __init__(self):
        super().__init__(
            "MethStreams",
            os.getenv("AGGREGATOR_3_URL", "https://methstreams.click"),
            [],
            ["/game/", "/match/", "/live/"],
        )


class StreamEastScraper(HtmlAggregatorScraper):
    def __init__(self):
        super().__init__(
            "StreamEast",
            os.getenv("AGGREGATOR_4_URL", "https://thestreameast.top"),
            [],
            ["/stream/", "/match/", "/live/"],
        )
class FootybiteScraper(HtmlAggregatorScraper):
    def __init__(self):
        super().__init__(
            "Footybite",
            os.getenv("AGGREGATOR_9_URL", "https://footybite.im"),
            [],
            ["/watch/", "/stream/", "/live/", "/match/"],
        )


class OneStreamScraper(HtmlAggregatorScraper):
    def __init__(self):
        super().__init__(
            "1Stream",
            os.getenv("AGGREGATOR_10_URL", "https://1stream.ws"),
            [],
            ["/match/", "/stream/", "/live/"],
        )


class StreamedSuScraper(HtmlAggregatorScraper):
    def __init__(self):
        super().__init__(
            "Streamed",
            os.getenv("AGGREGATOR_11_URL", "https://streamed.su"),
            ["/category/football", "/category/american-football", "/category/basketball", "/category/baseball", "/category/hockey"],
            ["/watch/", "/live/"],
        )


class TopStreamsScraper(HtmlAggregatorScraper):
    def __init__(self):
        super().__init__(
            "TopStreams",
            os.getenv("AGGREGATOR_12_URL", "https://topstreams.info"),
            ["/nfl", "/nba", "/nhl", "/mlb", "/soccer"],
            ["/watch/", "/match/", "/live/"],
        )


_M3U_ATTR_RE = re.compile(r'([a-zA-Z0-9_-]+)="([^"]*)"')


def _parse_m3u_playlist(text: str) -> List[dict]:
    """Parse a #EXTM3U playlist into entries with tvg_id/tvg_name/group_title/url.
    Deliberately minimal (no external M3U library) since only a handful of
    #EXTINF attributes are needed here."""
    entries: List[dict] = []
    pending: Optional[dict] = None
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#EXTINF"):
            attrs = dict(_M3U_ATTR_RE.findall(line))
            display_name = line.rsplit(",", 1)[-1].strip() if "," in line else ""
            pending = {
                "tvg_id": attrs.get("tvg-id", ""),
                "tvg_name": attrs.get("tvg-name", "") or display_name,
                "group_title": attrs.get("group-title", ""),
                "display_name": display_name,
            }
        elif line.startswith("#"):
            continue
        elif pending is not None:
            pending["url"] = line
            entries.append(pending)
            pending = None
    return entries


IPTV_ORG_PLAYLIST_URL_DEFAULT = "https://iptv-org.github.io/iptv/categories/sports.m3u"
IPTV_ORG_REFRESH_SECONDS = bounded_float(os.getenv("IPTV_ORG_REFRESH_SECONDS", "21600"), 21600.0, 300.0, 86400.0)
_IPTV_ORG_CACHE: List[dict] = []
_IPTV_ORG_CACHE_LOADED_AT = 0.0
_IPTV_ORG_LOCK = asyncio.Lock()


class IptvOrgScraper(BaseProvider):
    """Curated, static free-to-air playlist (iptv-org) used as a structurally
    independent backup for always-live special channels: a plain cached HTTP
    fetch with no scraping and no Cloudflare exposure, so it survives failure
    modes (anti-bot changes, mass site outages) that could take out every
    HTML-scraping provider in ACTIVE_PROVIDERS at once. Only useful for 24/7
    linear channels (ESPN, FS1, NFL Network, ...) — it carries no per-game team
    broadcasts, so it belongs in LINEAR_PROVIDERS, not the general team search."""

    name = "IPTV-Org"

    def __init__(self):
        self.base_url = os.getenv("IPTV_ORG_PLAYLIST_URL", IPTV_ORG_PLAYLIST_URL_DEFAULT)

    async def _get_entries(self, http_client: Optional[httpx.AsyncClient]) -> List[dict]:
        global _IPTV_ORG_CACHE, _IPTV_ORG_CACHE_LOADED_AT
        now = time.monotonic()
        if _IPTV_ORG_CACHE and now - _IPTV_ORG_CACHE_LOADED_AT < IPTV_ORG_REFRESH_SECONDS:
            return _IPTV_ORG_CACHE

        async with _IPTV_ORG_LOCK:
            now = time.monotonic()
            if _IPTV_ORG_CACHE and now - _IPTV_ORG_CACHE_LOADED_AT < IPTV_ORG_REFRESH_SECONDS:
                return _IPTV_ORG_CACHE

            url = _validate_upstream_url(self.base_url)
            if not url:
                return _IPTV_ORG_CACHE

            owns_client = http_client is None
            client = http_client or httpx.AsyncClient(timeout=15.0, follow_redirects=True)
            try:
                resp = await client.get(url, headers={"User-Agent": DEFAULT_USER_AGENT})
                if resp.status_code == 200:
                    _IPTV_ORG_CACHE = _parse_m3u_playlist(resp.text)
                    _IPTV_ORG_CACHE_LOADED_AT = now
            except Exception as exc:
                _log_failure("fetch iptv-org playlist", exc)
            finally:
                if owns_client:
                    await client.aclose()
            return _IPTV_ORG_CACHE

    async def search(self, query_or_terms, browser: Optional[Browser] = None, http_client: Optional[httpx.AsyncClient] = None) -> List[dict]:
        search_terms = list(query_or_terms) if isinstance(query_or_terms, list) else [str(query_or_terms)]
        entries = await self._get_entries(http_client)
        if not entries:
            return []

        candidates = []
        for entry in entries:
            name_text = f"{entry.get('tvg_name', '')} {entry.get('display_name', '')}"
            if not _channel_term_matches(name_text, search_terms):
                continue
            url = _validate_upstream_url(entry.get("url", ""))
            if not url:
                continue
            candidates.append({
                "url": url,
                "referer": "",
                "origin": "",
                "provider": self.name,
                # match_team() scores a confident exact-word match 95-100 (110 only for
                # multi-word phrases); _channel_term_matches() above is at least that
                # precise — it's a word-boundary match against curated official channel
                # names with explicit ESPN/ESPN2/ESPN+ disambiguation, not noisy fuzzy
                # anchor text — so this shouldn't be scored as a weaker match than that.
                "match_score": 100,
                "match_title": entry.get("tvg_name") or entry.get("display_name") or "",
                "discovery_method": "http",
            })

        if not candidates:
            return []
        return await _verify_provider_streams(candidates, http_client)


class TheTVAppScraper(HtmlAggregatorScraper):
    def __init__(self):
        super().__init__(
            "TheTVApp", 
            os.getenv("AGGREGATOR_5_URL", "https://thetvapp67.com"), 
            ["/tv/"], 
            ["/tv/", "/watch/", "/channel/", "/sports-channels/"]
        )

    async def search(self, query_or_terms, browser: Optional[Browser] = None, http_client: Optional[httpx.AsyncClient] = None) -> List[dict]:
        if not browser or not browser.is_connected():
            return []

        search_terms = get_team_search_terms(query_or_terms, query_or_terms) if isinstance(query_or_terms, str) else list(query_or_terms or [])
        raw_terms_lower = [t.lower() for t in (query_or_terms if isinstance(query_or_terms, list) else [query_or_terms])]
        streams = []

        try:
            context = await browser.new_context(user_agent=DEFAULT_USER_AGENT)
            page = await context.new_page()

            target_url = f"{self.base_url}/tv/"
            try:
                await page.goto(target_url, wait_until="domcontentloaded", timeout=25000)
                try:
                    await page.wait_for_load_state("networkidle", timeout=8000)
                except Exception:
                    pass
            except Exception:
                pass

            html = await page.content()
            if "just a moment" in html.lower() or "cf-browser-verification" in html.lower():
                await context.close()
                return []

            soup = BeautifulSoup(html, "html.parser")

            best_url, best_score, best_title = None, 0, ""

            for anchor in soup.find_all("a", href=True):
                href = str(anchor.get("href") or "")
                text = self._anchor_context(anchor) or anchor.get_text(" ", strip=True)
                text_lower = text.lower()

                # Direct linear network match override (e.g., ESPN, RedZone, FS1)
                matched = False
                score = 0
                for rt in raw_terms_lower:
                    if _channel_term_matches(text_lower, [rt]) or _channel_term_matches(href, [rt]):
                        matched = True
                        score = 120
                        break

                # Fallback to standard team matcher if direct string isn't found
                if not matched:
                    matched, score, _ = match_team(search_terms, text, href=href)

                if matched and not _is_non_english_channel(text):
                    # Prefer the most specific matching channel when a page
                    # contains both a base network and numbered variants.
                    specificity = max(
                        (len(str(term).split()) * 10 + len(str(term)) for term in raw_terms_lower if _channel_term_matches(text, [term])),
                        default=0,
                    )
                    ranked_score = score + specificity
                else:
                    ranked_score = 0

                if ranked_score > best_score:
                    best_url = urllib.parse.urljoin(self.base_url, href)
                    best_score = ranked_score
                    best_title = text

            if best_url:
                def handle_request(request):
                    req_url = request.url
                    if _is_playlist_request_url(req_url) and validate_http_url(req_url):
                        streams.append({
                            "url": req_url,
                            "referer": request.headers.get("referer", best_url),
                            "origin": request.headers.get("origin", ""),
                            "provider": self.name,
                            "match_score": best_score,
                            "match_title": best_title,
                            "discovery_method": "playwright"
                        })

                page.on("request", handle_request)

                try:
                    await page.goto(best_url, wait_until="domcontentloaded", timeout=25000)
                    try:
                        await page.wait_for_load_state("networkidle", timeout=8000)
                    except Exception:
                        pass
                    await page.mouse.click(400, 300)
                    try:
                        await page.wait_for_load_state("networkidle", timeout=5000)
                    except Exception:
                        pass
                except Exception:
                    pass

            await context.close()
        except Exception as exc:
            _log_failure("TheTVApp custom extraction", exc)
            if 'context' in locals(): await context.close()

        seen = set()
        deduped = [s for s in streams if s["url"] not in seen and not seen.add(s["url"])]
        return await _verify_provider_streams(deduped[:MAX_STREAM_CANDIDATES], http_client)


# Region tags that mark a DaddyLive channel listing as non-English. English
# feeds are tagged "USA"; anything carrying one of these suffixes is excluded.
_NON_ENGLISH_CHANNEL_TAGS = frozenset({
    "de", "fr", "es", "it", "pt", "brasil", "brazil", "argentina", "mexico",
    "colombia", "chile", "peru", "ecuador", "uruguay", "paraguay", "bolivia",
    "venezuela", "deportes", "latino", "latin", "espanol", "spanish", "arab",
    "arabic", "turkiye", "turkish", "poland", "polska", "greece", "greek",
    "romania", "hungary", "czech", "slovakia", "bulgaria", "croatia", "serbia",
    "russia", "ukraine", "india", "hindi", "pakistan", "indonesia", "malaysia",
    "thailand", "vietnam", "philippines", "china", "korea", "japan", "australia",
    "canada", "ireland", "belgium", "switzerland", "austria", "sweden", "norway",
    "denmark", "finland", "iceland", "israel", "hebrew", "africa", "nigeria",
    "egypt", "saudi", "uae", "qatar", "caribbean",
})
_NON_ENGLISH_CHANNEL_CODES = frozenset({
    "nl", "de", "fr", "es", "it", "pt", "br", "tr", "pl", "gr", "ro", "hu",
    "cz", "sk", "bg", "hr", "rs", "ru", "ua", "pk", "id", "my", "th", "vn",
    "ph", "cn", "hk", "tw", "kr", "jp", "au", "nz", "ie", "be", "ch", "at",
    "se", "no", "dk", "fi", "is", "il", "za", "ng", "eg", "sa", "ae", "qa",
    "mx", "ar", "cl", "co", "pe", "ve", "uy",
})


def _is_non_english_channel(text: str) -> bool:
    """Return True when a channel listing carries an explicit non-English tag."""
    value = str(text or "").casefold()
    tokens = [token for token in re.split(r"[^a-z]+", value) if token]
    if any(token in _NON_ENGLISH_CHANNEL_TAGS for token in tokens):
        return True
    # Two-letter region codes are accepted only as explicit suffixes or inside
    # delimiters. This avoids treating ordinary words such as "in" as regions.
    code_pattern = r"(?:^|[\s(\[/_|-])(" + "|".join(sorted(_NON_ENGLISH_CHANNEL_CODES, key=len, reverse=True)) + r")(?:$|[\s)\]/_|-])"
    return re.search(code_pattern, value) is not None


_DISALLOWED_CHANNEL_SUBSTRINGS = {
    "fox": ("sports", "news", "business", "weather", "cricket", "soccer", "deportes", "league", "hd bulgaria"),
    "cbs": ("sports", "golazo", "news"),
    "nbc": ("sports", "news", "universo"),
    "abc": ("news",),
}


def _channel_term_matches(text: str, terms: List[str]) -> bool:
    """Match a channel name without allowing ESPN to match ESPN2/ESPN+, or broadcast nets to match sports/news spin-offs."""
    value = str(text or "").casefold()
    for term in terms:
        if not term:
            continue
        term_fold = str(term).casefold()
        if re.search(rf"(?<![a-z0-9]){re.escape(term_fold)}(?![a-z0-9])", value):
            # If searching for ESPN/ESPN2/ESPNU/FS1/FS2, do not falsely match ESPN+ or RedZone+
            if term_fold in {"espn", "espn2", "espnu"} and re.search(rf"(?<![a-z0-9]){re.escape(term_fold)}\s*\+", value):
                continue
            disallowed = _DISALLOWED_CHANNEL_SUBSTRINGS.get(term_fold)
            if disallowed and any(re.search(rf"(?<![a-z0-9]){re.escape(sub)}(?![a-z0-9])", value) for sub in disallowed):
                continue
            return True
    return False


class DaddyLiveScraper(HtmlAggregatorScraper):
    def __init__(self):
        super().__init__("DaddyLive", os.getenv("AGGREGATOR_6_URL", "https://dlhd.pk"), ["/24-7-channels.php"], ["watch.php"])

    async def _find_matches(self, client: httpx.AsyncClient, search_terms: List[str], browser: Optional[Browser] = None) -> List[tuple[str, int, str]]:
        """Match individual directory cards without inheriting the grid's text."""
        matches: List[tuple[str, int, str]] = []
        seen_matches: Set[str] = set()
        for page_url in self.get_scan_urls():
            page_html = await fetch_bounded_text(
                client,
                page_url,
                headers={"User-Agent": DEFAULT_USER_AGENT, "Referer": self.base_url},
                timeout=8.0,
            )
            if not page_html and browser and browser.is_connected():
                context = None
                try:
                    context = await browser.new_context(user_agent=DEFAULT_USER_AGENT)
                    page = await context.new_page()
                    await page.goto(page_url, wait_until="domcontentloaded", timeout=25000)
                    await asyncio.sleep(2)
                    page_html = await page.content()
                except Exception as exc:
                    _log_failure("scan DaddyLive directory with browser", exc)
                finally:
                    if context:
                        await context.close()
            if not page_html:
                continue

            soup = BeautifulSoup(page_html, "html.parser")
            for anchor in soup.find_all("a", href=True):
                href = str(anchor.get("href") or "")
                if not self._is_event_link(href, page_url):
                    continue
                match_url = urllib.parse.urljoin(page_url, href)
                if match_url in seen_matches:
                    continue
                title = str(anchor.get("title") or anchor.get("data-title") or "")
                raw_text = anchor.get_text(" ", strip=True)
                card_text = " ".join(part for part in (raw_text, title, str(anchor.get("aria-label") or "")) if part)
                if _is_non_english_channel(card_text):
                    continue
                matched = _channel_term_matches(card_text, search_terms) or _channel_term_matches(href, search_terms)
                score = max(
                    (len(str(term).split()) * 10 + len(str(term)) for term in search_terms if _channel_term_matches(card_text, [term])),
                    default=0,
                )
                if matched:
                    seen_matches.add(match_url)
                    matches.append((match_url, score, raw_text or title or match_url))
                    if len(matches) >= MAX_PROVIDER_EVENTS:
                        return matches
        return matches

    async def _extract_player_streams(self, browser: Browser, watch_url: str, score: int, match_title: str) -> List[dict]:
        """Follow the embedded player iframe chain and capture the real HLS manifest.

        DaddyLive watch pages embed the actual player one or two iframes deep
        (watch.php -> dlive.sx/stream/stream-<id>.php -> <player-host>). Listening
        for playlist requests across every frame in the page reliably surfaces the
        genuine manifest, which is requested from the player iframe's origin.
        """
        streams: List[dict] = []
        if not browser or not browser.is_connected():
            return streams

        context = None
        try:
            context = await browser.new_context(user_agent=DEFAULT_USER_AGENT)
            page = await context.new_page()

            def on_request(request) -> None:
                if not _is_playlist_request_url(request.url):
                    return
                url = request.url
                if any(s["url"] == url for s in streams):
                    return
                try:
                    frame_url = request.frame.url if request.frame else ""
                except Exception:
                    frame_url = ""
                referer = request.headers.get("referer") or frame_url or watch_url
                streams.append({
                    "url": url,
                    "provider": self.name,
                    "match_score": score,
                    "match_title": match_title,
                    "referer": referer,
                    "origin": request.headers.get("origin", ""),
                    "discovery_method": "playwright",
                })

            page.on("request", on_request)
            try:
                await page.goto(watch_url, wait_until="domcontentloaded", timeout=30000)
            except Exception as exc:
                _log_failure("DaddyLive watch page load", exc)

            # Let nested iframes attach, then nudge the player to start.
            await asyncio.sleep(5)
            for frame in list(page.frames):
                try:
                    await frame.mouse.click(400, 300)
                except Exception:
                    pass
            try:
                await page.mouse.click(400, 300)
            except Exception:
                pass
            await asyncio.sleep(5)
        except Exception as exc:
            _log_failure("DaddyLive player extraction", exc)
        finally:
            if context:
                try:
                    await context.close()
                except Exception:
                    pass

        seen = set()
        return [s for s in streams if s["url"] not in seen and not seen.add(s["url"])]

    async def search(self, query_or_terms, browser: Optional[Browser] = None, http_client: Optional[httpx.AsyncClient] = None) -> List[dict]:
        if not browser or not browser.is_connected():
            return []

        search_terms = get_team_search_terms(query_or_terms, query_or_terms) if isinstance(query_or_terms, str) else list(query_or_terms or [])
        owns_client = http_client is None
        client = http_client or httpx.AsyncClient(follow_redirects=True, timeout=12.0)

        try:
            matches = await self._find_matches(client, search_terms, browser=browser)
            if not matches:
                return []

            def match_specificity(event: tuple[str, int, str]) -> tuple[int, int, int]:
                _, score, title = event
                clean_title = clean_sports_text(title)
                exact = 0
                for term in search_terms:
                    clean_term = clean_sports_text(term)
                    if clean_term and re.search(rf"\b{re.escape(clean_term)}\b", clean_title):
                        exact = max(exact, len(clean_term.split()) * 10 + len(clean_term))
                # Strongly prioritize US English feeds (e.g. "USA" in card title)
                us_priority = 100 if re.search(r"\b(?:usa|us)\b", title, re.IGNORECASE) else 0
                return us_priority, exact, score

            specific_matches = [match for match in matches if match_specificity(match)[1] > 0 or match_specificity(match)[0] > 0]
            matches = sorted(specific_matches or matches, key=match_specificity, reverse=True)

            verified_streams: List[dict] = []
            for match_url, score, match_title in matches[:8]:
                streams = await self._extract_player_streams(browser, match_url, score, match_title)
                verified = await _verify_provider_streams(streams, client)
                if verified:
                    verified_streams.extend(verified)

            seen_urls: Set[str] = set()
            return [
                stream for stream in verified_streams
                if stream.get("url") and not (stream["url"] in seen_urls or seen_urls.add(stream["url"]))
            ]
        finally:
            if owns_client:
                await client.aclose()


ACTIVE_PROVIDERS = [
    TheTVAppScraper(),
    DaddyLiveScraper(),
    ISportSurgeScraper(),
    MyBuffStreamsScraper(),
    MethStreamsScraper(),
    StreamEastScraper(),
    FootybiteScraper(),
    OneStreamScraper(),
    StreamedSuScraper(),
    TopStreamsScraper(),
    IptvOrgScraper(),
]
LINEAR_PROVIDERS = tuple(provider for provider in ACTIVE_PROVIDERS if provider.name in {"TheTVApp", "DaddyLive", "IPTV-Org"})
stream_state: Dict[str, dict] = {}
_SCRAPE_IN_FLIGHT: Set[str] = set()

CATALOG_REFRESH_SECONDS = bounded_float(os.getenv("CATALOG_REFRESH_SECONDS", "3600"), 3600.0, 60.0, 86400.0)
_CATALOG_CACHE: Dict[str, Tuple[TeamSlug, ...]] = {}
_CATALOG_CACHE_LOADED_AT = 0.0
_CATALOG_REMOTE_LOADED = False
_CATALOG_LOCK = asyncio.Lock()
CATALOG_FAILURE_RETRY_SECONDS = bounded_float(os.getenv("CATALOG_FAILURE_RETRY_SECONDS", "120"), 120.0, 30.0, 1800.0)

SHARED_HTTP_CLIENT: Optional[httpx.AsyncClient] = None
PLAYWRIGHT_CLIENT: Optional[Playwright] = None
SHARED_BROWSER: Optional[Browser] = None
_PLAYWRIGHT_LOCK = asyncio.Lock()
_BACKGROUND_TASKS: Set[asyncio.Task] = set()
_TEAM_SCRAPE_TASKS: Dict[str, asyncio.Task] = {}
_TEAM_SCRAPE_WAKE_EVENTS: Dict[str, asyncio.Event] = {}
_TEAM_STATE_LOCKS: Dict[str, asyncio.Lock] = {}


def _team_state_lock(team_id: str) -> asyncio.Lock:
    lock = _TEAM_STATE_LOCKS.get(team_id)
    if lock is None:
        lock = asyncio.Lock()
        _TEAM_STATE_LOCKS[team_id] = lock
    return lock


def _static_catalog_records(category: str) -> List[TeamSlug]:
    records: List[TeamSlug] = []
    for record in STATIC_TEAM_RECORDS:
        if category in {"ncaaf", "ncaam"}:
            if record.is_college:
                records.append(record.for_category(category))
        elif record.category == category:
            records.append(record)
    return records


def _catalog_record_key(record: TeamSlug) -> Tuple[str, str, str]:
    return record.category, record.slug, record.canonical


async def _fetch_espn_directory(category: str, client: httpx.AsyncClient) -> Tuple[TeamSlug, ...]:
    endpoint = ESPN_DIRECTORY_ENDPOINTS.get(category)
    if not endpoint:
        return ()
    sport, league = endpoint
    url = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/teams"
    try:
        response = await client.get(url, params={"limit": "1000"})
        if response.status_code != 200:
            LOGGER.warning("ESPN catalog request category=%s status=%s", category, response.status_code)
            return ()
        records = parse_espn_team_directory(response.json(), category)
        return tuple(record.for_category(category) for record in records)
    except Exception as exc:
        _log_failure(f"fetch ESPN team directory category={category}", exc)
        return ()


async def get_team_catalog() -> Dict[str, Tuple[TeamSlug, ...]]:
    """Return grouped catalog records, refreshing college directories periodically."""
    global _CATALOG_CACHE, _CATALOG_CACHE_LOADED_AT, _CATALOG_REMOTE_LOADED

    now = time.monotonic()
    if _CATALOG_CACHE and now - _CATALOG_CACHE_LOADED_AT < CATALOG_REFRESH_SECONDS:
        return dict(_CATALOG_CACHE)

    async with _CATALOG_LOCK:
        now = time.monotonic()
        if _CATALOG_CACHE and now - _CATALOG_CACHE_LOADED_AT < CATALOG_REFRESH_SECONDS:
            return dict(_CATALOG_CACHE)

        categories = ("ncaaf", "ncaam", "nfl", "mlb", "nhl", "nba")
        grouped: Dict[str, Dict[Tuple[str, str, str], TeamSlug]] = {
            category: {
                _catalog_record_key(record): record
                for record in _static_catalog_records(category)
            }
            for category in categories
        }
        previous_catalog = dict(_CATALOG_CACHE)
        was_remote_loaded = _CATALOG_REMOTE_LOADED

        owns_client = SHARED_HTTP_CLIENT is None
        client = SHARED_HTTP_CLIENT or httpx.AsyncClient(
            timeout=10.0,
            follow_redirects=True,
            http2=True,
        )
        try:
            remote_results = await asyncio.gather(
                *(_fetch_espn_directory(category, client) for category in ("ncaaf", "ncaam")),
                return_exceptions=True,
            )
            remote_success = False
            for category, result in zip(("ncaaf", "ncaam"), remote_results):
                if isinstance(result, tuple) and result:
                    remote_success = True
                    grouped[category] = {
                        _catalog_record_key(record): record
                        for record in result
                    }
            _CATALOG_REMOTE_LOADED = remote_success or was_remote_loaded
        finally:
            if owns_client:
                await client.aclose()

        _CATALOG_CACHE = {
            category: tuple(sorted(records.values(), key=lambda record: record.display_name.casefold()))
            for category, records in grouped.items()
        }
        # A failed remote refresh should not suppress the next retry for the
        # full interval. Static records remain usable while ESPN recovers.
        if remote_success:
            _CATALOG_CACHE_LOADED_AT = time.monotonic()
        elif not previous_catalog:
            _CATALOG_CACHE_LOADED_AT = time.monotonic() - max(
                0.0,
                CATALOG_REFRESH_SECONDS - CATALOG_FAILURE_RETRY_SECONDS,
            )
        return dict(_CATALOG_CACHE)


def _catalog_team_id(category: str, source_id: str, name: str) -> str:
    return _safe_team_id(f"{category}_{source_id or name}")


_COLLEGE_CATEGORY_LABELS = {
    "ncaaf": "Football",
    "ncaam": "Men's Basketball",
}


def _sport_labeled_name(name: str, category: str) -> str:
    base_name = str(name or "").strip()
    sport_label = _COLLEGE_CATEGORY_LABELS.get(category, "")
    if not base_name or not sport_label:
        return base_name
    suffix = f" ({sport_label})"
    if base_name.casefold().endswith(suffix.casefold()):
        return base_name
    return f"{base_name}{suffix}"


def _catalog_display_name(category: str, record: TeamSlug) -> str:
    return _sport_labeled_name(record.display_name, category)


_SPECIAL_CHANNELS_BY_KEY = {channel.key: channel for channel in SPECIAL_CHANNELS}


def _normalize_channel_label(value: object) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", str(value or "").casefold())).strip()


_SPECIAL_CHANNELS_BY_LABEL = {
    _normalize_channel_label(label): channel
    for channel in SPECIAL_CHANNELS
    for label in (channel.name, *channel.search_terms)
    if _normalize_channel_label(label)
}
_CATEGORY_GROUP_LABELS = {
    "ncaaf": "College Football",
    "ncaam": "College Basketball",
    "nfl": "NFL",
    "mlb": "MLB",
    "nhl": "NHL",
    "nba": "NBA",
}


def _special_channel_for(data: dict):
    catalog_key = str(data.get("catalog_key") or "")
    if catalog_key.startswith("special:"):
        channel = _SPECIAL_CHANNELS_BY_KEY.get(catalog_key.removeprefix("special:"))
        if channel:
            return channel
    for field in ("name", "query"):
        channel = _SPECIAL_CHANNELS_BY_LABEL.get(_normalize_channel_label(data.get(field)))
        if channel:
            return channel
    return None


def _channel_is_always_live(data: dict) -> bool:
    return bool(data.get("always_live") or _special_channel_for(data))


def _channel_is_off_season(data: dict) -> bool:
    return data.get("schedule_status") == "off_season"


def _channel_tvg_id(team_id: str, data: dict) -> str:
    special_channel = _special_channel_for(data)
    return str(data.get("tvg_id") or (special_channel.tvg_id if special_channel else "") or team_id)


def _channel_group_title(data: dict) -> str:
    special_channel = _special_channel_for(data)
    return str(data.get("group_title") or (special_channel.group_title if special_channel else "") or _CATEGORY_GROUP_LABELS.get(data.get("category"), "Team Trackers"))


def _channel_logo_url(data: dict) -> str:
    special_channel = _special_channel_for(data)
    return str(
        data.get("logo_url")
        or (special_channel.logo_url if special_channel else "")
        or resolve_espn_logo(data.get("name", ""), data.get("category", ""), data.get("source_id", ""))
    )


def _m3u_attribute(value: str) -> str:
    return str(value or "").replace('"', "&quot;")


def _catalog_search_terms(record: TeamSlug, source_id: str) -> List[str]:
    values = [record.canonical, *record.aliases, source_id, record.slug.replace("-", " ")]
    terms = {str(value).strip() for value in values if str(value).strip()}
    return sorted(terms, key=lambda value: (len(value.split()), len(value)), reverse=True)


def _team_catalog_entry(category: str, record: TeamSlug) -> dict:
    source_id = record.team_id or record.slug
    name = _catalog_display_name(category, record)
    return {
        "catalog_key": f"team:{category}:{source_id}",
        "team_id": _catalog_team_id(category, source_id, name),
        "name": name,
        "query": record.canonical,
        "category": category,
        "source_id": source_id,
        "content_type": "team",
        "search_terms": _catalog_search_terms(record, source_id),
        "always_live": False,
        "logo_url": resolve_espn_logo(name, category, source_id),
    }


def _special_catalog_entry(channel) -> dict:
    return {
        "catalog_key": f"special:{channel.key}",
        "team_id": _safe_team_id(f"special_{channel.key}"),
        "name": channel.name,
        "query": channel.name,
        "category": "special",
        "source_id": "",
        "content_type": "channel",
        "search_terms": list(channel.search_terms),
        "always_live": True,
        "logo_url": channel.logo_url,
        "tvg_id": channel.tvg_id or channel.key,
        "group_title": channel.group_title,
    }


async def get_catalog_entries() -> List[dict]:
    grouped = await get_team_catalog()
    entries: List[dict] = []
    for category in ("ncaaf", "ncaam", "nfl", "mlb", "nhl", "nba"):
        entries.extend(_team_catalog_entry(category, record) for record in grouped.get(category, ()))
    entries.extend(_special_catalog_entry(channel) for channel in SPECIAL_CHANNELS)
    return entries


async def get_healthy_browser() -> Optional[Browser]:
    """Retrieve the shared browser, or relaunch it if it crashed or disconnected."""
    global SHARED_BROWSER, PLAYWRIGHT_CLIENT

    async with _PLAYWRIGHT_LOCK:
        if SHARED_BROWSER and SHARED_BROWSER.is_connected():
            return SHARED_BROWSER

        LOGGER.warning("Playwright browser disconnected or missing. Relaunching...")
        try:
            if SHARED_BROWSER:
                await SHARED_BROWSER.close()
        except Exception:
            pass

        try:
            if not PLAYWRIGHT_CLIENT:
                PLAYWRIGHT_CLIENT = await async_playwright().start()
            browser = await PLAYWRIGHT_CLIENT.chromium.launch(headless=True)
            if not browser.is_connected():
                await browser.close()
                return None
            SHARED_BROWSER = browser
            return browser
        except Exception as exc:
            _log_failure("relaunch Playwright browser", exc, logging.ERROR)
            return None


def _spawn_background_task(coroutine, operation: str) -> asyncio.Task:
    """Track fire-and-forget work so failures are logged and shutdown is clean."""
    task = asyncio.create_task(coroutine, name=operation)
    _BACKGROUND_TASKS.add(task)

    def _task_finished(done: asyncio.Task) -> None:
        _BACKGROUND_TASKS.discard(done)
        if done.cancelled():
            return
        try:
            exception = done.exception()
        except asyncio.CancelledError:
            return
        if exception:
            _log_failure(operation, exception, logging.ERROR)

    task.add_done_callback(_task_finished)
    return task


async def _cancel_background_tasks() -> None:
    tasks = list(_BACKGROUND_TASKS)
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def _start_team_scrape_loop(team_id: str) -> asyncio.Task:
    existing = _TEAM_SCRAPE_TASKS.get(team_id)
    if existing and not existing.done():
        return existing

    _TEAM_SCRAPE_WAKE_EVENTS.setdefault(team_id, asyncio.Event())
    task = asyncio.create_task(team_scrape_loop(team_id), name=f"scrape team={team_id}")
    _TEAM_SCRAPE_TASKS[team_id] = task

    def _scrape_finished(done: asyncio.Task) -> None:
        if _TEAM_SCRAPE_TASKS.get(team_id) is done:
            _TEAM_SCRAPE_TASKS.pop(team_id, None)
        if done.cancelled():
            return
        try:
            exception = done.exception()
        except asyncio.CancelledError:
            return
        if exception:
            _log_failure(f"team scrape loop team={team_id}", exception, logging.ERROR)

    task.add_done_callback(_scrape_finished)
    return task


async def _stop_team_scrape_loop(team_id: str) -> None:
    wake_event = _TEAM_SCRAPE_WAKE_EVENTS.pop(team_id, None)
    if wake_event:
        wake_event.set()
    task = _TEAM_SCRAPE_TASKS.pop(team_id, None)
    if task and not task.done():
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    _TEAM_STATE_LOCKS.pop(team_id, None)


CACHE_TTL_SECONDS = 120.0


def _positive_env_number(name: str, default: float) -> float:
    return bounded_float(os.getenv(name, str(default)), default, 0.001, 86400.0)


ACTIVE_HEALTH_INTERVAL = _positive_env_number("ACTIVE_HEALTH_INTERVAL", 3.0)
STANDBY_HEALTH_INTERVAL = _positive_env_number("STANDBY_HEALTH_INTERVAL", 45.0)
STANDBY_HEALTH_CONCURRENCY = bounded_int(os.getenv("STANDBY_HEALTH_CONCURRENCY", "4"), 4, 1, 32)
ACTIVE_HEALTH_CONCURRENCY = bounded_int(os.getenv("ACTIVE_HEALTH_CONCURRENCY", "8"), 8, 1, 64)
EMERGENCY_SCRAPE_COOLDOWN = _positive_env_number("EMERGENCY_SCRAPE_COOLDOWN", 60.0)
STREAM_CHUNK_CACHE_TTL = _positive_env_number("STREAM_CHUNK_CACHE_TTL", 15.0)
STREAM_STARTUP_BUFFER_SECONDS = bounded_float(
    os.getenv("STREAM_STARTUP_BUFFER_SECONDS", "15"), 15.0, 0.0, 120.0
)
STREAM_CHUNK_CACHE_CAPACITY = bounded_int(os.getenv("STREAM_CHUNK_CACHE_CAPACITY", "60"), 60, 1, 2000)
STREAM_CHUNK_CACHE_MAX_BYTES = bounded_int(
    os.getenv("STREAM_CHUNK_CACHE_MAX_BYTES", str(128 * 1024 * 1024)), 128 * 1024 * 1024, 1024 * 1024, 1024 * 1024 * 1024
)
PREFETCH_CHUNK_COUNT = bounded_int(os.getenv("PREFETCH_CHUNK_COUNT", "5"), 5, 0, 32)
PREFETCH_CONCURRENCY = bounded_int(os.getenv("PREFETCH_CONCURRENCY", "2"), 2, 1, 32)
MAX_MANIFEST_BYTES = bounded_int(os.getenv("MAX_MANIFEST_BYTES", str(2 * 1024 * 1024)), 2 * 1024 * 1024, 64 * 1024, 16 * 1024 * 1024)
MAX_RESOURCE_BYTES = bounded_int(os.getenv("MAX_RESOURCE_BYTES", str(8 * 1024 * 1024)), 8 * 1024 * 1024, 1024, 64 * 1024 * 1024)
MAX_CACHEABLE_CHUNK_BYTES = bounded_int(os.getenv("MAX_CACHEABLE_CHUNK_BYTES", str(4 * 1024 * 1024)), 4 * 1024 * 1024, 1024, 32 * 1024 * 1024)
MAX_UPSTREAM_REDIRECTS = bounded_int(os.getenv("MAX_UPSTREAM_REDIRECTS", "3"), 3, 0, 5)
STREAM_REQUEST_TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0)

class LRUChunkCache:
    """LRU + TTL cache, bounded by both entry count and total bytes held. Entry-count
    alone let capacity*MAX_CACHEABLE_CHUNK_BYTES (60*4MB=240MB) sit as a worst case;
    max_bytes gives a real memory ceiling regardless of individual chunk sizes."""

    def __init__(self, capacity: int = 60, max_bytes: int = 0):
        self.cache = OrderedDict()
        self.capacity = capacity
        self.max_bytes = max_bytes
        self.total_bytes = 0
        self.hits = 0
        self.misses = 0
        self.lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[bytes]:
        async with self.lock:
            if key not in self.cache:
                self.misses += 1
                return None
            val, exp = self.cache[key]
            if time.monotonic() > exp:
                self.total_bytes -= len(val)
                del self.cache[key]
                self.misses += 1
                return None
            self.cache.move_to_end(key)
            self.hits += 1
            return val

    async def stats(self) -> dict:
        """In-memory hit/miss counters since process start — avoids the DB write on
        every chunk fetch that a per-request cache_metrics INSERT would require on
        this hot path, at the cost of resetting on restart rather than persisting."""
        async with self.lock:
            total = self.hits + self.misses
            hit_rate = (self.hits / total * 100) if total > 0 else 0.0
            return {"hit_rate": round(hit_rate, 1), "total_hits": self.hits, "total_misses": self.misses}

    async def put(self, key: str, value: bytes, ttl: float) -> None:
        async with self.lock:
            now = time.monotonic()
            for expired_key, (expired_value, expires_at) in list(self.cache.items()):
                if now > expires_at:
                    self.total_bytes -= len(expired_value)
                    del self.cache[expired_key]
            existing = self.cache.get(key)
            if existing is not None:
                self.total_bytes -= len(existing[0])
            self.cache[key] = (value, time.monotonic() + ttl)
            self.total_bytes += len(value)
            self.cache.move_to_end(key)
            while len(self.cache) > self.capacity or (self.max_bytes and self.total_bytes > self.max_bytes):
                if not self.cache:
                    break
                _, (evicted_val, _) = self.cache.popitem(last=False)
                self.total_bytes -= len(evicted_val)

    async def purge_expired(self) -> None:
        async with self.lock:
            now = time.monotonic()
            for key, (value, expires_at) in list(self.cache.items()):
                if now > expires_at:
                    self.total_bytes -= len(value)
                    del self.cache[key]

CHUNK_CACHE = LRUChunkCache(capacity=STREAM_CHUNK_CACHE_CAPACITY, max_bytes=STREAM_CHUNK_CACHE_MAX_BYTES)
# Jellyfin re-polls a channel's manifest every few seconds during live playback, so
# recording a playback_events row on every proxy_stream() call would count one viewing
# session as dozens of rows. De-dupe to at most one row per team per this window.
PLAYBACK_EVENT_DEDUPE_SECONDS = 300.0
_LAST_PLAYBACK_EVENT: Dict[str, float] = {}
_PREFETCH_IN_FLIGHT: Set[str] = set()
_PREFETCH_LOCK = threading.Lock()
_PREFETCH_SEMAPHORE: Optional[asyncio.Semaphore] = None
_STARTUP_BUFFER_TASKS: Dict[str, asyncio.Task] = {}
_STARTUP_BUFFER_LOCK = asyncio.Lock()


def _remove_startup_buffer_task(done: asyncio.Task) -> None:
    for key, task in list(_STARTUP_BUFFER_TASKS.items()):
        if task is done:
            _STARTUP_BUFFER_TASKS.pop(key, None)

_ESPN_LOGO_CDN = "https://a.espncdn.com/i/teamlogos"
_SPORT_SLUG_MAP: Dict[str, tuple[str, str]] = {
    "florida state seminoles": ("ncaa", "52"),
    "florida state": ("ncaa", "52"),
    "seminoles": ("ncaa", "52"),
    "miami hurricanes": ("ncaa", "2390"),
    "inter miami": ("soccer", "10739"),
    "miami heat": ("nba", "mia"),
    "miami dolphins": ("nfl", "mia"),
    "miami marlins": ("mlb", "mia"),
    "florida panthers": ("nhl", "fla"),
    "florida gators": ("ncaa", "57"),
    "gators": ("ncaa", "57"),
    "florida": ("ncaa", "57"),
    "miami": ("ncaa", "2390"),
    "new york jets": ("nfl", "nyj"),
    "jets": ("nfl", "nyj"),
    "tampa bay buccaneers": ("nfl", "tb"),
    "buccaneers": ("nfl", "tb"),
    "jacksonville jaguars": ("nfl", "jax"),
    "jaguars": ("nfl", "jax"),
    "ucf knights": ("ncaa", "2116"),
    "ucf": ("ncaa", "2116"),
    "tampa bay lightning": ("nhl", "tb"),
    "lightning": ("nhl", "tb"),
    "michigan wolverines": ("ncaa", "130"),
    "michigan": ("ncaa", "130"),
    "michigan state spartans": ("ncaa", "127"),
    "ohio state buckeyes": ("ncaa", "194"),
    "new york mets": ("mlb", "nym"),
    "new york yankees": ("mlb", "nyy"),
    "tampa bay rays": ("mlb", "tb"),
}

def _resolve_espn_team(team_name: str) -> tuple[Optional[str], Optional[str], str]:
    identity = canonical_team_name(team_name)
    if not identity:
        return None, None, ""

    if identity in _SPORT_SLUG_MAP:
        sport, slug = _SPORT_SLUG_MAP[identity]
        return sport, slug, identity

    # Fallback only for a complete mapped phrase, never a loose substring such
    # as "michigan" inside "michigan state".
    words = set(identity.split())
    matches = [
        (key, value) for key, value in _SPORT_SLUG_MAP.items()
        if set(key.split()).issubset(words)
    ]
    if len(matches) == 1:
        key, (sport, slug) = matches[0]
        return sport, slug, key
    return None, None, identity


def resolve_espn_logo(team_name: str, category: str = "", source_id: str = "") -> str:
    category_logo_sports = {
        "nfl": "nfl",
        "ncaaf": "ncaa",
        "ncaam": "ncaa",
        "nba": "nba",
        "mlb": "mlb",
        "nhl": "nhl",
    }
    if category in category_logo_sports and source_id:
        return f"{_ESPN_LOGO_CDN}/{category_logo_sports[category]}/500/{source_id}.png?v=titan2"
    sport, slug, _ = _resolve_espn_team(team_name)
    if sport and slug:
        return f"{_ESPN_LOGO_CDN}/{sport}/500/{slug}.png?v=titan2"
    return ""


async def fetch_espn_team_schedule(
    team_name: str,
    query: str = "",
    category: str = "",
    source_id: str = "",
) -> tuple[Optional[datetime], Optional[datetime], bool]:
    api_map = {
        "nfl": ("football", "nfl"),
        "ncaaf": ("football", "college-football"),
        "ncaam": ("basketball", "mens-college-basketball"),
        "ncaa": ("football", "college-football"),
        "nba": ("basketball", "nba"),
        "mlb": ("baseball", "mlb"),
        "nhl": ("hockey", "nhl"),
        "soccer": ("soccer", "usa.1")
    }

    matched_sport = category
    matched_slug = source_id
    if not matched_sport or not matched_slug:
        matched_sport, matched_slug, _ = _resolve_espn_team(team_name or query)
    if not matched_sport or not matched_slug:
        return None, None, False
    
    if matched_sport not in api_map:
        return None, None, False
        
    sport, league = api_map[matched_sport]
    url = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/teams/{matched_slug}/schedule"
    
    now_utc = datetime.now(timezone.utc)
    duration_by_sport = {
        "football": timedelta(hours=4),
        "basketball": timedelta(hours=3),
        "baseball": timedelta(hours=4),
        "hockey": timedelta(hours=3),
        "soccer": timedelta(hours=2.5),
    }
    upcoming = []
    owns_client = SHARED_HTTP_CLIENT is None
    client = SHARED_HTTP_CLIENT or httpx.AsyncClient(timeout=8.0, follow_redirects=True, http2=True)
    try:
        resp = await client.get(url)
        if resp.status_code != 200:
            LOGGER.warning("ESPN schedule request team=%s status=%s", team_name or query, resp.status_code)
            return None, None, False
        data = resp.json()
        events = data.get("events", [])
        for ev in events:
            date_str = ev.get("date")
            if date_str:
                dt_start = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                if dt_start.tzinfo is None:
                    dt_start = dt_start.replace(tzinfo=timezone.utc)
                dt_start = dt_start.astimezone(timezone.utc)
                dt_stop = dt_start + duration_by_sport.get(sport, timedelta(hours=3))
                if dt_stop >= now_utc - timedelta(hours=1) and dt_start <= now_utc + timedelta(days=14):
                    upcoming.append((dt_start, dt_stop))
        if upcoming:
            start, stop = min(upcoming, key=lambda event: event[0])
            return start, stop, True
        return None, None, True
    except Exception as exc:
        _log_failure(f"fetch ESPN schedule team={team_name or query}", exc)
        return None, None, False
    finally:
        if owns_client:
            await client.aclose()
    return None, None, False

def xmltv_ts(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M%S +0000")


STREAM_LEAD_TIME = timedelta(hours=1)
STREAM_TRAIL_TIME = timedelta(hours=1)
SCRAPE_REFRESH_SECONDS = 300


def parse_team_schedule(data: dict) -> tuple[Optional[datetime], Optional[datetime]]:
    try:
        start_str = data.get("start_time", "")
        stop_str = data.get("stop_time", "")
        if not start_str or not stop_str:
            return None, None
        start = datetime.strptime(start_str, "%Y%m%d%H%M%S +0000").replace(tzinfo=timezone.utc)
        stop = datetime.strptime(stop_str, "%Y%m%d%H%M%S +0000").replace(tzinfo=timezone.utc)
        return start, stop
    except (TypeError, ValueError):
        return None, None


def is_in_season(category: str, today: Optional[date] = None) -> bool:
    """Return whether `category`'s sport is inside its preseason-to-championship window.

    Categories with no defined window (manually added "custom" teams, and
    non-sport special channels) are always considered in season.
    """
    window = SEASON_WINDOWS.get(category)
    if not window:
        return True
    start_month, start_day, end_month, end_day = window
    current = today or datetime.now(timezone.utc).date()
    start = (start_month, start_day)
    end = (end_month, end_day)
    here = (current.month, current.day)
    if start <= end:
        return start <= here <= end
    # The window wraps across the new year (e.g. NFL: Aug 1 -> Feb 15).
    return here >= start or here <= end


def _season_resume_label(category: str) -> str:
    """Return a short "Mon D" label for when `category`'s season window reopens."""
    window = SEASON_WINDOWS.get(category)
    if not window:
        return ""
    start_month, start_day, _, _ = window
    return f"{date(2000, start_month, 1).strftime('%b')} {start_day}"


def is_stream_window_active(data: dict, now: Optional[datetime] = None) -> bool:
    if _channel_is_always_live(data):
        return True
    schedule_status = data.get("schedule_status", "unknown")
    if schedule_status in ("no_event", "off_season"):
        return False
    if schedule_status == "lookup_failed":
        # A failed schedule lookup must not prevent discovery of a live stream.
        return True
    start, stop = parse_team_schedule(data)
    if not start or not stop:
        # Unknown schedules retain the old behavior instead of silently
        # removing a channel that may still have a valid live event.
        return True
    current = now or datetime.now(timezone.utc)
    return start - STREAM_LEAD_TIME <= current <= stop + STREAM_TRAIL_TIME


def _providers_for_search(always_live: bool = False):
    """Keep 24/7 channel discovery on dedicated linear-channel providers."""
    return LINEAR_PROVIDERS if always_live else ACTIVE_PROVIDERS


def _get_active_provider_priority() -> Dict[str, int]:
    """Return the provider tie-break priority used by rank_streams().

    When provider rotation mode is enabled, the preferred provider rotates
    hourly so no single aggregator is hammered with every search, spreading
    load across sources instead of always preferring the same one.
    """
    if get_setting("provider_rotation_mode", "0") != "1":
        return _provider_priority
    names = [provider.name for provider in ACTIVE_PROVIDERS]
    if not names:
        return _provider_priority
    offset = int(time.time() // 3600) % len(names)
    rotated = names[offset:] + names[:offset]
    return {name: index for index, name in enumerate(rotated)}


def _stream_matches_requested_event(stream: dict, search_terms: List[str]) -> bool:
    """Reject a provider stream whose source page identifies another event."""
    match_title = str(stream.get("match_title") or "")
    match_url = str(stream.get("match_url") or "")
    if not match_title and not match_url:
        return True
    matched, _, _ = match_team(search_terms, match_title, href=match_url, title=match_title)
    return matched

async def master_scrape(
    query: str,
    team_name: str = "",
    team_id: str = "",
    search_terms: Optional[List[str]] = None,
    always_live: bool = False,
) -> List[dict]:
    identity = canonical_team_name(team_name or query)
    if search_terms:
        search_terms = list(dict.fromkeys(term.strip() for term in search_terms if term and term.strip()))
    else:
        search_terms = get_team_search_terms(identity or team_name or query, query, team_id)
    display_title = team_name or query
    LOGGER.info("Starting stream search for team=%s terms=%s", display_title, search_terms[:4])
    providers = _providers_for_search(always_live)

    owns_client = SHARED_HTTP_CLIENT is None
    client = SHARED_HTTP_CLIENT or httpx.AsyncClient(
        limits=httpx.Limits(max_keepalive_connections=50, max_connections=150),
        timeout=12.0,
        follow_redirects=True,
        http2=True
    )

    # Per-team semaphore caps how many of this team's own providers run at once,
    # AND how many of the global PROVIDER_SEARCH_CONCURRENCY slots this team can hold
    # simultaneously — so a team stuck on slow providers can't starve other teams'
    # concurrently running scrapes of every global slot.
    team_semaphore = asyncio.Semaphore(min(PER_TEAM_PROVIDER_CONCURRENCY, PROVIDER_SEARCH_CONCURRENCY))

    async def _jittered_search(provider):
        if _provider_breaker_open(provider.name):
            LOGGER.info("Skipping provider=%s while circuit breaker is open", provider.name)
            return []
        dynamic_timeout = 45.0
        try:
            async with team_semaphore, _provider_search_semaphore():
                LOGGER.info("Querying provider=%s team=%s", provider.name, display_title)
                browser = await get_healthy_browser()
                dynamic_timeout = await _get_dynamic_provider_timeout(provider.name)
                start_time = time.time()
                res = await asyncio.wait_for(
                    provider.search(search_terms, browser=browser, http_client=client),
                    timeout=dynamic_timeout,
                )
                elapsed_ms = int((time.time() - start_time) * 1000)
                await _track_provider_response_time(provider.name, elapsed_ms, success=True)
                _provider_breaker_success(provider.name)
                LOGGER.info("Provider returned provider=%s team=%s streams=%d", provider.name, display_title, len(res))
                return res
        except asyncio.TimeoutError:
            _provider_breaker_failure(provider.name)
            await _track_provider_response_time(provider.name, int(dynamic_timeout * 1000), success=False)
            LOGGER.warning("Provider search timed out provider=%s team=%s", provider.name, display_title)
            return []
        except Exception as e:
            _provider_breaker_failure(provider.name)
            await _track_provider_response_time(provider.name, 0, success=False)
            _log_failure(f"provider search {provider.name} for {display_title}", e)
            return []

    try:
        tasks = [_jittered_search(provider) for provider in providers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_streams = [
            stream
            for sublist in results if isinstance(sublist, list)
            for stream in sublist
            if always_live or _stream_matches_requested_event(stream, search_terms)
        ]
    finally:
        if owns_client:
            await client.aclose()

    seen_urls = set()
    deduped = []
    all_streams = rank_streams(all_streams, _get_active_provider_priority())
    for s in all_streams:
        u = s.get("url")
        if u and u not in seen_urls:
            seen_urls.add(u)
            deduped.append(s)

    return deduped

def _scrape_lifecycle_defaults() -> dict:
    return {
        "scrape_in_progress": False,
        "last_scrape_started": 0.0,
        "last_scrape_completed": 0.0,
        "scrape_result": "pending",
        "scrape_error": "",
    }


def _mark_scrape_started(data: dict) -> None:
    data["scrape_in_progress"] = True
    data["last_scrape_started"] = time.time()
    data["scrape_error"] = ""
    data["scrape_result"] = "running"


def _mark_scrape_finished(data: dict, result: str, error: str = "") -> None:
    data["scrape_in_progress"] = False
    data["last_scrape_completed"] = time.time()
    data["scrape_result"] = result
    data["scrape_error"] = error


async def trigger_scrape(team_id: str):
    if team_id in _SCRAPE_IN_FLIGHT:
        LOGGER.debug("Skipping duplicate scrape team=%s", team_id)
        return
    data = stream_state.get(team_id)
    if not data:
        return
    _SCRAPE_IN_FLIGHT.add(team_id)
    _mark_scrape_started(data)
    try:
        await _trigger_scrape(team_id)
    except asyncio.CancelledError:
        _mark_scrape_finished(data, "cancelled")
        raise
    except Exception as exc:
        _mark_scrape_finished(data, "failed", type(exc).__name__)
        raise
    else:
        _mark_scrape_finished(data, "healthy" if data.get("candidates") else "empty")
    finally:
        _SCRAPE_IN_FLIGHT.discard(team_id)


def _resolve_schedule_status(
    start_utc: Optional[datetime],
    stop_utc: Optional[datetime],
    *,
    in_season: bool,
    always_live: bool,
    schedule_ok: bool,
    previous_start: str,
    previous_stop: str,
) -> Tuple[str, str, str]:
    """Pure decision logic extracted from _trigger_scrape: which stream-window state
    wins when a freshly-found event, an off-season league, a confirmed no-event
    response, and a failed/stale lookup can all be true in different combinations.
    Kept standalone (no stream_state, no I/O) so this branching — previously only
    ever exercised end-to-end via a live ESPN fetch — can be unit tested directly."""
    if start_utc and stop_utc:
        return xmltv_ts(start_utc), xmltv_ts(stop_utc), "scheduled"
    if not in_season and not always_live:
        return "", "", "off_season"
    if schedule_ok and not always_live:
        # A successful empty response is authoritative. Do not retain a past
        # event, because it can make a current event appear out of window.
        return "", "", "no_event"
    return previous_start, previous_stop, ("always_live" if always_live else "lookup_failed")


async def _trigger_scrape_unlocked(team_id: str):
    if team_id not in stream_state:
        return
    query = stream_state[team_id]["query"]
    team_name = stream_state[team_id]["name"]
    category = stream_state[team_id].get("category", "")
    source_id = stream_state[team_id].get("source_id", "")
    always_live = bool(stream_state[team_id].get("always_live"))
    in_season = is_in_season(category)
    logo_url = resolve_espn_logo(team_name, category, source_id)
    previous_start = stream_state[team_id].get("start_time", "")
    previous_stop = stream_state[team_id].get("stop_time", "")
    if always_live:
        start_utc, stop_utc = None, None
        schedule_ok = True
    elif not in_season:
        # Off-season: skip the ESPN schedule lookup entirely rather than
        # polling a league that has no games to find.
        start_utc, stop_utc = None, None
        schedule_ok = True
    else:
        start_utc, stop_utc, schedule_ok = await fetch_espn_team_schedule(team_name, query, category, source_id)

    current = stream_state.get(team_id)
    if current is None:
        return

    start_str, stop_str, schedule_status = _resolve_schedule_status(
        start_utc, stop_utc,
        in_season=in_season, always_live=always_live, schedule_ok=schedule_ok,
        previous_start=previous_start, previous_stop=previous_stop,
    )
    current["schedule_status"] = schedule_status

    current["logo_url"] = logo_url or current.get("logo_url", "")
    current["start_time"] = start_str
    current["stop_time"] = stop_str
    await update_team_meta_async(team_id, current["logo_url"], start_str, stop_str)
    current = stream_state.get(team_id)
    if current is None:
        return

    if not is_stream_window_active(current):
        current["candidates"] = []
        current["active_index"] = 0
        current["is_healthy"] = False
        return

    previous_candidates = current.get("candidates", [])
    new_candidates = await master_scrape(
        query,
        team_name=team_name,
        team_id=team_id,
        search_terms=current.get("search_terms") or None,
        always_live=always_live,
    )
    current = stream_state.get(team_id)
    if current is None:
        return
    was_healthy = current.get("is_healthy", False)
    if new_candidates:
        current["candidates"] = new_candidates
        current["active_index"] = 0
        is_healthy = True
        current["is_healthy"] = True
    elif previous_candidates:
        LOGGER.warning(
            "Keeping existing stream candidates after empty refresh team=%s count=%d",
            team_id,
            len(previous_candidates),
        )
        is_healthy = current.get("is_healthy", False)
    else:
        current["candidates"] = []
        current["active_index"] = 0
        is_healthy = False
        current["is_healthy"] = False

    if not current.get("logo_url"):
        for c in new_candidates:
            if c.get("logo_url"):
                current["logo_url"] = c["logo_url"]
                break

    if is_healthy and not was_healthy and len(new_candidates) > 0:
        _spawn_background_task(
            send_alert("✅ Stream Available", f"Stream active for **{team_name}**.", "success"),
            f"send stream-available alert team={team_id}",
        )

    if new_candidates:
        _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide")


async def _trigger_scrape(team_id: str):
    """Serialize all scrape-driven updates for one channel."""
    async with _team_state_lock(team_id):
        await _trigger_scrape_unlocked(team_id)


async def team_scrape_loop(team_id: str):
    while team_id in stream_state:
        scrape_failed = False
        try:
            await trigger_scrape(team_id)
        except Exception as exc:
            scrape_failed = True
            _log_failure(f"scheduled scrape team={team_id}", exc, logging.ERROR)

        data = stream_state.get(team_id)
        if not data:
            return
        start, stop = parse_team_schedule(data)
        delay = SCRAPE_REFRESH_SECONDS
        if data.get("schedule_status") == "off_season":
            # No point polling ESPN every few minutes for a sport that won't
            # have games for months; a daily check-in is plenty, and a
            # manual rescrape or the season starting still wakes it early.
            delay = 86400
        elif start and stop:
            now = datetime.now(timezone.utc)
            window_start = start - STREAM_LEAD_TIME
            if now < window_start:
                delay = min(SCRAPE_REFRESH_SECONDS, max(30, int((window_start - now).total_seconds())))
            elif now > stop + STREAM_TRAIL_TIME:
                delay = min(SCRAPE_REFRESH_SECONDS, 900)
        if scrape_failed:
            delay = min(delay, 60)
        wake_event = _TEAM_SCRAPE_WAKE_EVENTS.get(team_id)
        if wake_event is None:
            return
        try:
            await asyncio.wait_for(wake_event.wait(), timeout=delay)
            wake_event.clear()
        except asyncio.TimeoutError:
            pass

def _dynamic_provider_timeout_sync(provider: str) -> float:
    with _db_session() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT AVG(response_time_ms) FROM provider_performance WHERE provider=? AND timestamp > datetime('now', '-1 hour')",
            (provider,)
        )
        row = cursor.fetchone()
        if row and row[0]:
            avg_ms = row[0]
            return float(max(20, min(90, (avg_ms / 1000) * 3)))
    return 45.0


async def _get_dynamic_provider_timeout(provider: str) -> float:
    """Calculate dynamic timeout based on provider's historical response times."""
    try:
        return await asyncio.to_thread(_dynamic_provider_timeout_sync, provider)
    except Exception as exc:
        _log_failure("calculate dynamic provider timeout", exc)
        return 45.0


def _track_provider_response_time_sync(provider: str, response_time_ms: int, success: bool) -> None:
    with _db_session() as conn:
        conn.execute(
            "INSERT INTO provider_performance (provider, response_time_ms, success) VALUES (?, ?, ?)",
            (provider, response_time_ms, 1 if success else 0)
        )
        conn.commit()


async def _track_provider_response_time(provider: str, response_time_ms: int, success: bool = True) -> None:
    """Track provider response time for timeout optimization."""
    try:
        await asyncio.to_thread(_track_provider_response_time_sync, provider, response_time_ms, success)
    except Exception as exc:
        _log_failure("track provider response time", exc)

async def _trigger_adaptive_bitrate_fallback(team_id: str, current_candidate_idx: int) -> bool:
    """Attempt to switch to next candidate if current stream quality is degrading."""
    data = stream_state.get(team_id)
    if not data or current_candidate_idx >= len(data.get("candidates", [])) - 1:
        return False

    next_idx = current_candidate_idx + 1
    data["active_index"] = next_idx
    new_provider = data["candidates"][next_idx].get("provider", "Unknown")
    LOGGER.info("Adaptive fallback team=%s new_index=%d reason=quality_degradation", team_id, next_idx)
    await log_metric_event_async(team_id, new_provider, "failover", "Adaptive bitrate fallback due to quality degradation")
    _spawn_background_task(
        send_alert("⚠️ Stream Failover", f"Failed over to {new_provider} for **{data.get('name', team_id)}**.", "warning"),
        f"send failover alert team={team_id}",
    )
    return True

async def check_stream_health(url: str, referer: str) -> bool:
    owns_client = SHARED_HTTP_CLIENT is None
    client = SHARED_HTTP_CLIENT or httpx.AsyncClient(timeout=5.0, follow_redirects=True, http2=True)
    try:
        return await verify_stream_live(client, url, referer)
    except Exception as exc:
        _log_failure("stream health check", exc)
        return False
    finally:
        if owns_client:
            await client.aclose()

async def failover_monitor():
    last_standby_check = 0.0

    async def emergency_rescrape(team_id: str, data: dict, team_name: str) -> None:
        if team_id not in stream_state:
            return
        if team_id in _SCRAPE_IN_FLIGHT:
            LOGGER.debug("Skipping emergency scrape already in progress team=%s", team_id)
            return
        _SCRAPE_IN_FLIGHT.add(team_id)
        _mark_scrape_started(data)
        try:
            candidates = await master_scrape(
                data["query"],
                team_name=team_name,
                team_id=team_id,
                search_terms=data.get("search_terms") or None,
                always_live=bool(data.get("always_live")),
            )
            current = stream_state.get(team_id)
            if current is not None:
                if candidates:
                    current["candidates"] = candidates
                    current["active_index"] = 0
                    current["is_healthy"] = True
                else:
                    LOGGER.warning(
                        "Keeping existing stream candidates after empty emergency refresh team=%s count=%d",
                        team_id,
                        len(current.get("candidates", [])),
                    )
            _mark_scrape_finished(data, "healthy" if candidates else "empty")
        except asyncio.CancelledError:
            _mark_scrape_finished(data, "cancelled")
            raise
        except Exception as exc:
            _mark_scrape_finished(data, "failed", type(exc).__name__)
            _log_failure(f"emergency rescrape team={team_id}", exc, logging.ERROR)
        finally:
            _SCRAPE_IN_FLIGHT.discard(team_id)

    while True:
        try:
            now = time.monotonic()
            check_standby = now - last_standby_check >= STANDBY_HEALTH_INTERVAL
            standby_tasks = []
            standby_semaphore = asyncio.Semaphore(STANDBY_HEALTH_CONCURRENCY)

            async def probe_standby(team_id: str, candidate: dict) -> None:
                async with standby_semaphore:
                    is_alive = await check_stream_health(candidate["url"], candidate.get("referer", ""))
                candidate["last_health_check"] = time.time()
                candidate["last_health_ok"] = is_alive

            active_semaphore = asyncio.Semaphore(ACTIVE_HEALTH_CONCURRENCY)

            async def probe_active(team_id: str, active_stream: dict) -> Tuple[str, bool]:
                async with active_semaphore:
                    try:
                        is_alive = await asyncio.wait_for(
                            check_stream_health(active_stream["url"], active_stream.get("referer", "")),
                            timeout=8.0,
                        )
                    except asyncio.TimeoutError:
                        is_alive = False
                        LOGGER.warning("Active stream health check timed out team=%s", team_id)
                return team_id, is_alive

            # Collect the eligible teams first (cheap, synchronous), then run every
            # team's active-stream health check concurrently instead of one at a time —
            # with many channels, a slow/timing-out check used to stall every other
            # team's failover detection behind it.
            eligible = []
            active_checks = []
            for team_id, data in list(stream_state.items()):
                if data.get("type") == "multiview":
                    continue
                if not is_stream_window_active(data):
                    data["candidates"] = []
                    data["active_index"] = 0
                    data["is_healthy"] = False
                    continue
                if not data.get("candidates"):
                    continue
                active_idx = data.get("active_index", 0)
                if active_idx >= len(data["candidates"]):
                    active_idx = 0
                    stream_state[team_id]["active_index"] = 0

                active_stream = data["candidates"][active_idx]
                eligible.append((team_id, data, active_idx, active_stream))
                active_checks.append(probe_active(team_id, active_stream))

            active_results = dict(await asyncio.gather(*active_checks)) if active_checks else {}

            for team_id, data, active_idx, active_stream in eligible:
                is_alive = active_results.get(team_id, False)
                if is_alive:
                    stream_state[team_id]["is_healthy"] = True
                    active_stream["last_health_check"] = time.time()
                    active_stream["last_health_ok"] = True
                else:
                    stream_state[team_id]["is_healthy"] = False
                    active_stream["last_health_check"] = time.time()
                    active_stream["last_health_ok"] = False
                    team_name = data.get("name", team_id)
                    next_idx = active_idx + 1

                    if next_idx >= len(data["candidates"]):
                        current_time = time.monotonic()
                        last_emergency = data.get("last_emergency_scrape", 0.0)
                        if current_time - data.get("last_exhausted_alert", 0.0) >= EMERGENCY_SCRAPE_COOLDOWN:
                            data["last_exhausted_alert"] = current_time
                            _spawn_background_task(
                                send_alert("🚨 All Stream Candidates Exhausted", f"All candidates for **{team_name}** failed.", "danger"),
                                f"send exhausted-stream alert team={team_id}",
                            )
                        if current_time - last_emergency >= EMERGENCY_SCRAPE_COOLDOWN:
                            data["last_emergency_scrape"] = current_time
                            _spawn_background_task(
                                emergency_rescrape(team_id, data, team_name),
                                f"emergency rescrape team={team_id}",
                            )
                    else:
                        await _trigger_adaptive_bitrate_fallback(team_id, active_idx)

                if check_standby and len(data["candidates"]) > 1:
                    for idx, candidate in enumerate(data["candidates"]):
                        if idx != stream_state[team_id].get("active_index", 0):
                            standby_tasks.append(probe_standby(team_id, candidate))

            if standby_tasks:
                await asyncio.gather(*standby_tasks, return_exceptions=True)
            if check_standby:
                last_standby_check = now
        except Exception as exc:
            _log_failure("failover monitor iteration", exc, logging.ERROR)
        await asyncio.sleep(ACTIVE_HEALTH_INTERVAL)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global SHARED_HTTP_CLIENT, PLAYWRIGHT_CLIENT, SHARED_BROWSER, _PREFETCH_SEMAPHORE
    global STREAM_STARTUP_BUFFER_SECONDS, PREFETCH_CHUNK_COUNT, STREAM_CHUNK_CACHE_TTL

    init_db()
    _METRIC_WRITER.start()
    shutil.rmtree(MULTIVIEW_OUTPUT_ROOT, ignore_errors=True)
    MULTIVIEW_OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    shutil.rmtree(PLACEHOLDER_OUTPUT_DIR, ignore_errors=True)
    # The Playback Settings tab persists these three to app_settings, but they were
    # previously only ever read from the env var at import time — saving the sliders
    # updated the DB and nothing else. Restore any saved values now so they actually
    # take effect (update_playback_settings() below updates these same globals live).
    STREAM_STARTUP_BUFFER_SECONDS = bounded_float(
        get_setting("startup_buffer_seconds", str(STREAM_STARTUP_BUFFER_SECONDS)),
        STREAM_STARTUP_BUFFER_SECONDS, 0.0, 120.0,
    )
    PREFETCH_CHUNK_COUNT = bounded_int(
        get_setting("prefetch_chunk_count", str(PREFETCH_CHUNK_COUNT)), PREFETCH_CHUNK_COUNT, 0, 32
    )
    STREAM_CHUNK_CACHE_TTL = bounded_float(
        get_setting("stream_chunk_cache_ttl", str(STREAM_CHUNK_CACHE_TTL)), STREAM_CHUNK_CACHE_TTL, 0.001, 86400.0
    )
    stream_state.clear()
    _TEAM_SCRAPE_TASKS.clear()
    _SCRAPE_IN_FLIGHT.clear()
    _TEAM_STATE_LOCKS.clear()
    _STARTUP_BUFFER_TASKS.clear()
    _PREFETCH_SEMAPHORE = asyncio.Semaphore(PREFETCH_CONCURRENCY)
    try:
        PLAYWRIGHT_CLIENT = await async_playwright().start()
        SHARED_BROWSER = await PLAYWRIGHT_CLIENT.chromium.launch(headless=True)
    except Exception as exc:
        # HTTP scraping remains available when Chromium is not installed or cannot start.
        _log_failure("start Playwright; using HTTP extraction only", exc)
        if PLAYWRIGHT_CLIENT:
            await PLAYWRIGHT_CLIENT.stop()
        PLAYWRIGHT_CLIENT = None
        SHARED_BROWSER = None
    
    SHARED_HTTP_CLIENT = httpx.AsyncClient(
        limits=httpx.Limits(max_keepalive_connections=50, max_connections=150),
        timeout=12.0,
        follow_redirects=True,
        http2=True
    )
    await _check_ffmpeg_available()
    if FFMPEG_AVAILABLE:
        _spawn_background_task(_ensure_placeholder_running(), "start shared placeholder stream")
    for (
        team_id,
        name,
        query,
        logo_url,
        start_time,
        stop_time,
        category,
        source_id,
        content_type,
        encoded_search_terms,
        always_live,
        catalog_key,
        auto_disable_after,
    ) in load_teams():
        try:
            stored_search_terms = json.loads(encoded_search_terms or "[]")
        except (TypeError, ValueError, json.JSONDecodeError):
            stored_search_terms = []
        if not isinstance(stored_search_terms, list):
            stored_search_terms = []
        search_terms = [str(term).strip() for term in stored_search_terms if str(term).strip()]
        if not search_terms:
            search_terms = get_team_search_terms(name, query, team_id)
        display_name = _sport_labeled_name(name, category or "")
        special_channel = _special_channel_for({"catalog_key": catalog_key})
        stream_state[team_id] = {
            "name": display_name, "query": query, "candidates": [], "active_index": 0, "is_healthy": False,
            "logo_url": logo_url or resolve_espn_logo(name, category, source_id),
            "start_time": start_time or "",
            "stop_time": stop_time or "",
            "category": category or "custom",
            "source_id": source_id or "",
            "content_type": content_type or "team",
            "search_terms": search_terms,
            "always_live": bool(always_live) or bool(special_channel),
            "catalog_key": catalog_key or "",
            "tvg_id": special_channel.tvg_id if special_channel else "",
            "group_title": special_channel.group_title if special_channel else "",
            "auto_disable_after": auto_disable_after or "",
            **_scrape_lifecycle_defaults(),
        }
        _start_team_scrape_loop(team_id)

    for (
        channel_id,
        mv_name,
        layout,
        encoded_member_ids,
        active_audio_team_id,
        tvg_id,
        group_title,
        logo_url,
    ) in load_multiview_channels():
        try:
            member_team_ids = json.loads(encoded_member_ids or "[]")
        except (TypeError, ValueError, json.JSONDecodeError):
            member_team_ids = []
        member_team_ids = [t for t in member_team_ids if t in stream_state]
        if len(member_team_ids) not in (2, 4):
            LOGGER.warning("Skipping multiview channel with invalid member count: %s", channel_id)
            continue
        stream_state[channel_id] = {
            "name": mv_name, "query": "", "type": "multiview",
            "candidates": [{"synthetic": True}],  # non-empty sentinel only to satisfy generic health-dot/404 checks; not a real stream candidate
            "active_index": 0, "is_healthy": False,
            "logo_url": logo_url, "start_time": "", "stop_time": "",
            "category": "multiview", "source_id": "", "content_type": "multiview",
            "search_terms": [], "always_live": True, "catalog_key": "",
            "tvg_id": tvg_id, "group_title": group_title or "Multi-View",
            "layout": layout, "member_team_ids": member_team_ids,
            "active_audio_team_id": active_audio_team_id if active_audio_team_id in member_team_ids else member_team_ids[0],
            **_scrape_lifecycle_defaults(),
        }

    monitor_task = asyncio.create_task(failover_monitor(), name="failover monitor")
    prune_task = asyncio.create_task(prune_database_logs(), name="database log pruning")
    schedule_task = asyncio.create_task(enforce_scheduled_disables(), name="scheduled disable enforcement")
    multiview_idle_task = asyncio.create_task(multiview_idle_monitor(), name="multiview idle monitor")
    yield

    monitor_task.cancel()
    prune_task.cancel()
    schedule_task.cancel()
    multiview_idle_task.cancel()
    scrape_tasks = list(_TEAM_SCRAPE_TASKS.values())
    for t in scrape_tasks:
        t.cancel()
    # Wait for tasks to safely wind down before closing shared clients.
    await asyncio.gather(monitor_task, prune_task, schedule_task, multiview_idle_task, *scrape_tasks, return_exceptions=True)
    _TEAM_SCRAPE_TASKS.clear()
    _TEAM_SCRAPE_WAKE_EVENTS.clear()
    _TEAM_STATE_LOCKS.clear()
    for channel_id in list(_MULTIVIEW_PROCESSES):
        await _stop_multiview_process(channel_id)
    await _stop_placeholder_process()
    # Ensure tracked prefetchers and other background work are stopped too.
    await _cancel_background_tasks()
    await _METRIC_WRITER.stop()
    _STARTUP_BUFFER_TASKS.clear()
    shutil.rmtree(MULTIVIEW_OUTPUT_ROOT, ignore_errors=True)
    shutil.rmtree(PLACEHOLDER_OUTPUT_DIR, ignore_errors=True)
        
    if SHARED_BROWSER:
        try:
            await SHARED_BROWSER.close()
        except Exception as exc:
            _log_failure("close Playwright browser", exc)
    if PLAYWRIGHT_CLIENT:
        try:
            await PLAYWRIGHT_CLIENT.stop()
        except Exception as exc:
            _log_failure("stop Playwright", exc)
    if SHARED_HTTP_CLIENT:
        try:
            await SHARED_HTTP_CLIENT.aclose()
        except Exception as exc:
            _log_failure("close shared HTTP client", exc)
        SHARED_HTTP_CLIENT = None
    SHARED_BROWSER = None
    PLAYWRIGHT_CLIENT = None
    _PREFETCH_SEMAPHORE = None
    stream_state.clear()

app = FastAPI(title="Jellyfin Sports Proxy - Titan Engine", lifespan=lifespan)


@app.middleware("http")
async def request_diagnostics(request: Request, call_next):
    started = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - started) * 1000
        _log_failure(
            f"HTTP {request.method} {request.url.path} ({elapsed_ms:.0f}ms)",
            exc,
            logging.ERROR,
        )
        return JSONResponse(status_code=500, content={"detail": "Internal server error"})

    elapsed_ms = (time.perf_counter() - started) * 1000
    if response.status_code >= 500:
        LOGGER.warning(
            "HTTP failure method=%s path=%s status=%d duration_ms=%.0f",
            request.method,
            request.url.path,
            response.status_code,
            elapsed_ms,
        )
    elif elapsed_ms >= 5000:
        LOGGER.warning(
            "Slow request method=%s path=%s status=%d duration_ms=%.0f",
            request.method,
            request.url.path,
            response.status_code,
            elapsed_ms,
        )
    return response


async def _fetch_upstream_body(
    client: httpx.AsyncClient,
    url: str,
    headers: Dict[str, str],
    max_bytes: int,
) -> Optional[tuple[int, str, str, bytes, Dict[str, str]]]:
    current_url = url
    for _ in range(MAX_UPSTREAM_REDIRECTS + 1):
        safe_url = await validate_http_url_async(current_url)
        if not safe_url:
            return None
        try:
            async with client.stream(
                "GET",
                safe_url,
                headers=headers,
                timeout=STREAM_REQUEST_TIMEOUT,
                follow_redirects=False,
            ) as response:
                if 300 <= response.status_code < 400:
                    location = response.headers.get("location")
                    if not location:
                        return None
                    current_url = urllib.parse.urljoin(str(response.url), location)
                    continue
                if response.status_code >= 400:
                    LOGGER.warning(
                        "Upstream proxy resource rejected status=%s host=%s path=%s",
                        response.status_code,
                        urllib.parse.urlsplit(str(response.url)).netloc,
                        urllib.parse.urlsplit(str(response.url)).path,
                    )
                content_length = response.headers.get("content-length")
                try:
                    if content_length and int(content_length) > max_bytes:
                        return None
                except ValueError:
                    pass
                body = bytearray()
                async for block in response.aiter_bytes():
                    if len(body) + len(block) > max_bytes:
                        return None
                    body.extend(block)
                return (
                    response.status_code,
                    str(response.url),
                    response.headers.get("content-type", ""),
                    bytes(body),
                    dict(response.headers),
                )
        except Exception as exc:
            _log_failure("fetch upstream proxy resource", exc)
            return None
    return None


def _manifest_uri_is_playlist(url: str) -> bool:
    parsed = urllib.parse.urlsplit(str(url or ""))
    path = parsed.path.lower()
    query = parsed.query.lower()
    return (
        path.endswith(".m3u8")
        or "/playlist/" in path
        or "/manifest" in path
        or "/load-playlist" in path
        or ".m3u8" in query
    )


def _chunk_route_for_url(url: str) -> str:
    path = urllib.parse.urlsplit(str(url or "")).path.lower()
    if path.endswith((".m4s", ".mp4", ".m4v")):
        return "/chunk.mp4"
    if path.endswith((".aac", ".m4a")):
        return "/chunk.aac"
    if path.endswith((".vtt", ".webvtt")):
        return "/chunk.vtt"
    return "/chunk.ts"


_MAX_STORED_MANIFESTS = 50
_MAX_STORED_SEGMENTS = 5000
_MANIFEST_SEGMENTS_LOCK = threading.Lock()
_MANIFEST_MEDIA_SEGMENTS: "OrderedDict[str, List[str]]" = OrderedDict()
_SEGMENT_TO_MANIFEST: "OrderedDict[str, str]" = OrderedDict()


def _reorder_hls_variants(manifest_lines: List[str]) -> List[str]:
    """Reorder HLS variants by BANDWIDTH (highest first) for quality preference.

    Non-variant lines (#EXTM3U, #EXT-X-VERSION, #EXT-X-MEDIA groups, etc.) are
    preserved in place; only the STREAM-INF/URI pairs are moved, as sorted, to
    where the first variant originally appeared.
    """
    variants = []
    other_lines = []
    first_variant_pos = None
    i = 0
    while i < len(manifest_lines):
        line = manifest_lines[i].strip()
        if line.startswith("#EXT-X-STREAM-INF:") and i + 1 < len(manifest_lines):
            if first_variant_pos is None:
                first_variant_pos = len(other_lines)
            bandwidth_match = re.search(r'BANDWIDTH=(\d+)', line)
            bandwidth = int(bandwidth_match.group(1)) if bandwidth_match else 0
            variants.append((bandwidth, manifest_lines[i], manifest_lines[i + 1]))
            i += 2
            continue
        other_lines.append(manifest_lines[i])
        i += 1

    if not variants:
        return manifest_lines

    variants.sort(key=lambda x: x[0], reverse=True)
    variant_lines = []
    for _, inf_line, url_line in variants:
        variant_lines.append(inf_line)
        variant_lines.append(url_line)

    return other_lines[:first_variant_pos] + variant_lines + other_lines[first_variant_pos:]

def extract_manifest_media_urls(manifest_text: str, target_url: str) -> List[str]:
    """Parse all playable media chunk URLs from an HLS manifest in playlist order."""
    urls: List[str] = []
    seen: Set[str] = set()
    expect_variant_playlist = False
    for line in manifest_text.splitlines():
        value = line.strip()
        if not value:
            continue
        if value.startswith("#"):
            expect_variant_playlist = value.startswith("#EXT-X-STREAM-INF:")
            continue
        if expect_variant_playlist:
            expect_variant_playlist = False
            continue
        resolved = urllib.parse.urljoin(target_url, value)
        if _manifest_uri_is_playlist(resolved):
            continue
        if _validate_upstream_url(resolved) and resolved not in seen:
            seen.add(resolved)
            urls.append(resolved)
    return urls


def _register_manifest_segments(manifest_text: str, manifest_url: str) -> List[str]:
    """Store manifest segment URLs for upcoming chunk prefetch lookups."""
    segments = extract_manifest_media_urls(manifest_text, manifest_url)
    if not segments:
        return []
    with _MANIFEST_SEGMENTS_LOCK:
        _MANIFEST_MEDIA_SEGMENTS[manifest_url] = segments
        if len(_MANIFEST_MEDIA_SEGMENTS) > _MAX_STORED_MANIFESTS:
            _MANIFEST_MEDIA_SEGMENTS.popitem(last=False)
        for seg in segments:
            _SEGMENT_TO_MANIFEST[seg] = manifest_url
            if len(_SEGMENT_TO_MANIFEST) > _MAX_STORED_SEGMENTS:
                _SEGMENT_TO_MANIFEST.popitem(last=False)
    return segments


def _get_next_manifest_chunks(current_chunk_url: str, count: int = 5) -> List[str]:
    """Return upcoming media segment URLs following current_chunk_url in the manifest."""
    if count <= 0:
        return []
    with _MANIFEST_SEGMENTS_LOCK:
        manifest_url = _SEGMENT_TO_MANIFEST.get(current_chunk_url)
        segments = _MANIFEST_MEDIA_SEGMENTS.get(manifest_url) if manifest_url else None

        if not segments:
            current_path = urllib.parse.urlsplit(current_chunk_url).path
            for m_url, seg_list in reversed(_MANIFEST_MEDIA_SEGMENTS.items()):
                for seg in seg_list:
                    if seg == current_chunk_url or urllib.parse.urlsplit(seg).path == current_path:
                        manifest_url = m_url
                        segments = seg_list
                        break
                if segments:
                    break

        if not segments:
            return []

        idx = -1
        try:
            idx = segments.index(current_chunk_url)
        except ValueError:
            current_path = urllib.parse.urlsplit(current_chunk_url).path
            for i, seg in enumerate(segments):
                if urllib.parse.urlsplit(seg).path == current_path:
                    idx = i
                    break

        if idx < 0:
            return []

        return segments[idx + 1 : idx + 1 + count]


def rewrite_m3u8(
    manifest_text: str,
    target_url: str,
    referer: str,
    host: str,
    origin: str = "",
) -> str:
    _register_manifest_segments(manifest_text, target_url)
    rewritten_manifest = []
    enc_ref = urllib.parse.quote(referer or "")
    enc_origin = urllib.parse.quote(origin or "")
    expect_variant_playlist = False
    has_variants = "#EXT-X-STREAM-INF:" in manifest_text

    def _proxy_resource_uri(
        uri: str,
        resource_path: Optional[str] = None,
        force_playlist: bool = False,
    ) -> str:
        resolved_url = urllib.parse.urljoin(target_url, uri)
        if not _validate_upstream_url(resolved_url):
            return uri
        enc_url = urllib.parse.quote(resolved_url)
        if force_playlist or _manifest_uri_is_playlist(resolved_url):
            route = f"{host}/substream.m3u8?url={enc_url}&ref={enc_ref}"
        else:
            route = f"{host}{resource_path or _chunk_route_for_url(resolved_url)}?url={enc_url}&ref={enc_ref}"
        return f"{route}&org={enc_origin}" if enc_origin else route

    manifest_lines = manifest_text.splitlines()
    if has_variants:
        manifest_lines = _reorder_hls_variants(manifest_lines)

    for line in manifest_lines:
        line_clean = line.strip()
        if not line_clean:
            rewritten_manifest.append(line)
        elif line_clean.startswith("#"):
            if 'URI="' in line:
                is_playlist_attribute = line_clean.startswith((
                    "#EXT-X-MEDIA:",
                    "#EXT-X-I-FRAME-STREAM-INF:",
                ))
                line = re.sub(
                    r'URI="([^"]+)"',
                    lambda match: f'URI="{_proxy_resource_uri(
                        match.group(1),
                        "/resource" if line_clean.startswith(("#EXT-X-KEY", "#EXT-X-MAP", "#EXT-X-SESSION-KEY")) else None,
                        force_playlist=is_playlist_attribute,
                    )}"',
                    line,
                )
            rewritten_manifest.append(line)
            expect_variant_playlist = line_clean.startswith("#EXT-X-STREAM-INF:")
        else:
            rewritten_manifest.append(_proxy_resource_uri(line_clean, force_playlist=expect_variant_playlist))
            expect_variant_playlist = False

    return "\n".join(rewritten_manifest)


def _chunk_media_type(url: str) -> str:
    path = urllib.parse.urlsplit(str(url or "")).path.lower()
    if path.endswith((".m4s", ".mp4", ".m4v")):
        return "video/mp4"
    if path.endswith((".aac", ".m4a")):
        return "audio/aac"
    if path.endswith((".vtt", ".webvtt")):
        return "text/vtt"
    return "video/mp2t"


def _startup_media_urls(manifest_text: str, target_url: str) -> List[str]:
    urls = extract_manifest_media_urls(manifest_text, target_url)
    limit = PREFETCH_CHUNK_COUNT if PREFETCH_CHUNK_COUNT > 0 else 5
    return urls[:limit]


async def _warm_startup_buffer(
    client: httpx.AsyncClient,
    media_urls: List[str],
    referer: str,
    origin: str = "",
) -> None:
    global _PREFETCH_SEMAPHORE
    if not media_urls:
        return
    if _PREFETCH_SEMAPHORE is None:
        _PREFETCH_SEMAPHORE = asyncio.Semaphore(PREFETCH_CONCURRENCY)
    cache_ttl = max(STREAM_CHUNK_CACHE_TTL, STREAM_STARTUP_BUFFER_SECONDS + 30.0)

    async def warm_one(media_url: str) -> None:
        cache_key = f"{media_url}\0{referer}\0{origin}"
        if await CHUNK_CACHE.get(cache_key):
            return
        async with _PREFETCH_SEMAPHORE:
            result = await _fetch_upstream_body(
                client,
                media_url,
                _upstream_media_headers(referer, origin),
                MAX_CACHEABLE_CHUNK_BYTES,
            )
        if result:
            status_code, _, _, body, _ = result
            if status_code in (200, 206) and body:
                await CHUNK_CACHE.put(cache_key, body, cache_ttl)

    await asyncio.gather(*(warm_one(url) for url in media_urls), return_exceptions=True)


async def _ensure_startup_buffer(
    cache_key: str,
    client: httpx.AsyncClient,
    media_urls: List[str],
    referer: str,
    origin: str = "",
) -> None:
    if not media_urls or STREAM_STARTUP_BUFFER_SECONDS <= 0:
        return
    async with _STARTUP_BUFFER_LOCK:
        task = _STARTUP_BUFFER_TASKS.get(cache_key)
        if task is None:
            task = _spawn_background_task(
                _warm_startup_buffer(client, media_urls, referer, origin),
                "warm stream startup buffer",
            )
            _STARTUP_BUFFER_TASKS[cache_key] = task
            task.add_done_callback(_remove_startup_buffer_task)

# --- MULTI-VIEW (FFmpeg grid compositing) ---

def _default_ffmpeg_path() -> str:
    """Prefer a bundled ffmpeg binary in the packaged executable; otherwise
    fall back to a plain "ffmpeg" lookup on PATH (dev runs, Docker, or a
    build that didn't have one available to bundle)."""
    if getattr(sys, "frozen", False):
        bundled_name = "ffmpeg.exe" if sys.platform == "win32" else "ffmpeg"
        bundled_path = BUNDLE_DIR / "ffmpeg_bin" / bundled_name
        if bundled_path.is_file():
            return str(bundled_path)
    return "ffmpeg"


FFMPEG_PATH = os.getenv("FFMPEG_PATH") or _default_ffmpeg_path()
MULTIVIEW_HWACCEL = os.getenv("MULTIVIEW_HWACCEL", "nvenc").strip().lower()
MULTIVIEW_BITRATE = os.getenv("MULTIVIEW_BITRATE", "6M")
MULTIVIEW_SEGMENT_SECONDS = bounded_int(os.getenv("MULTIVIEW_SEGMENT_SECONDS", "4"), 4, 1, 15)
MULTIVIEW_IDLE_TIMEOUT_SECONDS = bounded_float(os.getenv("MULTIVIEW_IDLE_TIMEOUT_SECONDS", "180"), 180.0, 30.0, 3600.0)
MULTIVIEW_IDLE_CHECK_INTERVAL = bounded_float(os.getenv("MULTIVIEW_IDLE_CHECK_INTERVAL", "30"), 30.0, 5.0, 300.0)
MULTIVIEW_STARTUP_TIMEOUT_SECONDS = bounded_float(os.getenv("MULTIVIEW_STARTUP_TIMEOUT_SECONDS", "30"), 30.0, 5.0, 120.0)
# Each running Multi-View channel is its own ffmpeg transcode (GPU encoder session +
# CPU/network for every member stream it composites); an unbounded number of them can
# exhaust NVENC/QSV session limits or the host's CPU. Cap concurrent transcodes.
MAX_CONCURRENT_MULTIVIEW = bounded_int(os.getenv("MAX_CONCURRENT_MULTIVIEW", "3"), 3, 1, 32)
MULTIVIEW_OUTPUT_ROOT = DATA_DIR / "multiview"
MULTIVIEW_FPS = 30

MULTIVIEW_LAYOUTS = {
    "side_by_side_2": {"count": 2, "pane_w": 960, "pane_h": 1080, "xstack": "0_0|w0_0"},
    "grid_2x2": {"count": 4, "pane_w": 960, "pane_h": 540, "xstack": "0_0|w0_0|0_h0|w0_h0"},
}

FFMPEG_AVAILABLE = False
FFMPEG_VERSION_INFO = ""

_MULTIVIEW_PROCESSES: Dict[str, dict] = {}
_MULTIVIEW_LOCKS: Dict[str, asyncio.Lock] = {}
# Tracks recent spawn failures per channel so a client that keeps retrying a
# permanently-broken Multi-View (e.g. one member currently has no live
# candidates, so ffmpeg's -i for it 404s and the whole process aborts) can't
# force a new ffmpeg spawn attempt on every single request. Real players
# (Jellyfin's tuner among them) retry a failed live-TV stream on their own
# schedule regardless of what we return, so this backoff is the only thing
# standing between "one dead input" and continuous CPU/GPU churn.
_MULTIVIEW_FAILURES: Dict[str, dict] = {}
MULTIVIEW_BACKOFF_BASE_SECONDS = 15.0
MULTIVIEW_BACKOFF_MAX_SECONDS = 300.0


def _multiview_backoff_seconds(failure_count: int) -> float:
    return min(MULTIVIEW_BACKOFF_MAX_SECONDS, MULTIVIEW_BACKOFF_BASE_SECONDS * (2 ** max(0, failure_count - 1)))


def _multiview_cooldown_remaining(channel_id: str) -> float:
    record = _MULTIVIEW_FAILURES.get(channel_id)
    if not record:
        return 0.0
    elapsed = time.monotonic() - record["last_failure"]
    return max(0.0, _multiview_backoff_seconds(record["count"]) - elapsed)


def _record_multiview_failure(channel_id: str, error: str) -> None:
    record = _MULTIVIEW_FAILURES.setdefault(channel_id, {"count": 0, "last_failure": 0.0, "last_error": ""})
    record["count"] += 1
    record["last_failure"] = time.monotonic()
    record["last_error"] = error


def _clear_multiview_failure(channel_id: str) -> None:
    _MULTIVIEW_FAILURES.pop(channel_id, None)


def _multiview_error_from_log(log_lines) -> str:
    """Pull the most useful line out of ffmpeg's log for a human-readable failure reason."""
    lines = list(log_lines)
    for line in reversed(lines):
        if "Error opening input" in line or "error while opening" in line.lower():
            return line.strip()
    for line in reversed(lines):
        if line.strip():
            return line.strip()
    return "ffmpeg exited unexpectedly"


def _multiview_lock(channel_id: str) -> asyncio.Lock:
    lock = _MULTIVIEW_LOCKS.get(channel_id)
    if lock is None:
        lock = asyncio.Lock()
        _MULTIVIEW_LOCKS[channel_id] = lock
    return lock


async def _check_ffmpeg_available() -> None:
    """Probe FFMPEG_PATH once at startup so failures surface as a clear log/dashboard warning."""
    global FFMPEG_AVAILABLE, FFMPEG_VERSION_INFO
    try:
        process = await asyncio.create_subprocess_exec(
            FFMPEG_PATH, "-version",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await asyncio.wait_for(process.communicate(), timeout=5.0)
        FFMPEG_AVAILABLE = process.returncode == 0
        FFMPEG_VERSION_INFO = stdout.decode(errors="replace").splitlines()[0] if stdout else ""
    except (FileNotFoundError, OSError, asyncio.TimeoutError) as exc:
        FFMPEG_AVAILABLE = False
        FFMPEG_VERSION_INFO = ""
        _log_failure("locate ffmpeg for multiview", exc, logging.WARNING)
    if not FFMPEG_AVAILABLE:
        LOGGER.warning("ffmpeg not found at FFMPEG_PATH=%r; Multi-View channels unavailable", FFMPEG_PATH)


def _multiview_bufsize(bitrate: str) -> str:
    """Double a ffmpeg bitrate string (e.g. "6M" -> "12M", "6000k" -> "12000k") for -bufsize."""
    match = re.match(r"^(\d+(?:\.\d+)?)([A-Za-z]*)$", bitrate.strip())
    if not match:
        return bitrate
    value, unit = match.groups()
    doubled = float(value) * 2
    doubled_str = str(int(doubled)) if doubled == int(doubled) else str(doubled)
    return f"{doubled_str}{unit}"


def _multiview_video_encoder_args() -> List[str]:
    """Encoder + rate-control flags. Kept per-backend because several of these
    (e.g. -forced-idr, -sc_threshold) are private AVOptions that only exist on
    some encoders and make ffmpeg fail outright with "Unrecognized option" on
    others (notably h264_qsv), so they must not be shared unconditionally."""
    gop = MULTIVIEW_FPS * MULTIVIEW_SEGMENT_SECONDS
    common_rate_args = [
        "-b:v", MULTIVIEW_BITRATE,
        "-maxrate", MULTIVIEW_BITRATE,
        "-bufsize", _multiview_bufsize(MULTIVIEW_BITRATE),
        "-g", str(gop),
        "-keyint_min", str(gop),
        "-bf", "0",
        "-pix_fmt", "yuv420p",
    ]
    if MULTIVIEW_HWACCEL == "nvenc":
        return [
            "-c:v", "h264_nvenc", "-preset", "p4", "-tune", "ll", "-rc", "cbr",
            *common_rate_args,
            "-sc_threshold", "0",
            "-forced-idr", "1",
        ]
    if MULTIVIEW_HWACCEL == "qsv":
        return [
            "-c:v", "h264_qsv", "-preset", "veryfast", "-look_ahead", "0",
            *common_rate_args,
        ]
    return [
        "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
        *common_rate_args,
        "-sc_threshold", "0",
    ]


def _build_xstack_filter(layout: str, member_count: int) -> str:
    """Return the -filter_complex string that scales/pads each input and stacks it into one grid."""
    spec = MULTIVIEW_LAYOUTS[layout]
    pane_w, pane_h = spec["pane_w"], spec["pane_h"]
    parts = []
    labels = []
    for idx in range(member_count):
        label = f"v{idx}"
        labels.append(label)
        parts.append(
            f"[{idx}:v]scale={pane_w}:{pane_h}:force_original_aspect_ratio=decrease,"
            f"pad={pane_w}:{pane_h}:(ow-iw)/2:(oh-ih)/2,setsar=1[{label}]"
        )
    stack_inputs = "".join(f"[{label}]" for label in labels)
    parts.append(f"{stack_inputs}xstack=inputs={member_count}:layout={spec['xstack']}[vout]")
    return ";".join(parts)


def _build_multiview_ffmpeg_args(channel_id: str, data: dict, out_dir: Path) -> List[str]:
    """Pure command-builder (no I/O) so it can be unit tested without spawning ffmpeg."""
    member_team_ids: List[str] = data["member_team_ids"]
    layout = data.get("layout", "grid_2x2")
    active_audio_team_id = data.get("active_audio_team_id") or member_team_ids[0]
    audio_index = member_team_ids.index(active_audio_team_id) if active_audio_team_id in member_team_ids else 0

    args: List[str] = ["-y"]
    for team_id in member_team_ids:
        args += [
            "-reconnect", "1",
            "-reconnect_streamed", "1",
            "-reconnect_on_network_error", "1",
            "-reconnect_delay_max", "5",
            "-rw_timeout", "15000000",
            "-i", f"http://127.0.0.1:{PORT}/stream/{team_id}",
        ]

    args += ["-filter_complex", _build_xstack_filter(layout, len(member_team_ids))]
    args += ["-map", "[vout]", "-map", f"{audio_index}:a:0"]
    args += _multiview_video_encoder_args()
    args += ["-c:a", "aac", "-b:a", "160k", "-ac", "2"]
    args += [
        "-f", "hls",
        "-hls_time", str(MULTIVIEW_SEGMENT_SECONDS),
        "-hls_list_size", "6",
        "-hls_flags", "delete_segments+independent_segments",
        "-hls_segment_type", "mpegts",
        # Relative filenames only: ffmpeg writes these verbatim into the .m3u8
        # segment list, and the process is spawned with cwd=out_dir so both the
        # written files and the playlist references line up.
        "-hls_segment_filename", "seg_%05d.ts",
        "index.m3u8",
    ]
    return args


def _multiview_popen_kwargs(out_dir: Path) -> dict:
    return {
        "cwd": str(out_dir),
        "stdin": asyncio.subprocess.DEVNULL,
        "stdout": asyncio.subprocess.PIPE,
        "stderr": asyncio.subprocess.STDOUT,
    }


async def _drain_multiview_log(channel_id: str, process: "asyncio.subprocess.Process", log_lines: "deque[str]") -> None:
    """Continuously read ffmpeg's combined stdout/stderr pipe and discard it into a
    rolling buffer. This is not optional: ffmpeg writes a steady stream of progress
    output, and an unread PIPE fills its OS buffer (~64KB) and blocks the writing
    process indefinitely — an unread ffmpeg hangs forever without ever producing
    a single output segment."""
    try:
        while True:
            line = await process.stdout.readline()
            if not line:
                break
            log_lines.append(line.decode(errors="replace").rstrip())
    except (asyncio.CancelledError, ValueError):
        raise
    except Exception as exc:
        _log_failure(f"drain ffmpeg log multiview={channel_id}", exc)


async def _watch_multiview_process(channel_id: str, process: "asyncio.subprocess.Process") -> None:
    returncode = await process.wait()
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if entry and entry["process"] is process:
        entry["exited"] = True
        entry["exit_code"] = returncode
        if returncode != 0:
            log_tail = "\n".join(entry.get("log_lines", []))
            LOGGER.warning("multiview ffmpeg exited unexpectedly channel=%s code=%s\n%s", channel_id, returncode, log_tail)


_WINDOWS_CLEANUP_JOB_HANDLE = None


def _get_windows_cleanup_job():
    """Lazily create one Windows Job Object with KILL_ON_JOB_CLOSE for the lifetime
    of this process. Any ffmpeg child assigned to it is terminated by Windows itself
    if this process dies — including an ungraceful crash or Task Manager 'End Task'
    where our own lifespan shutdown / _stop_*_process cleanup never gets to run."""
    global _WINDOWS_CLEANUP_JOB_HANDLE
    if sys.platform != "win32":
        return None
    if _WINDOWS_CLEANUP_JOB_HANDLE is not None:
        return _WINDOWS_CLEANUP_JOB_HANDLE
    try:
        import ctypes
        from ctypes import wintypes

        class _IO_COUNTERS(ctypes.Structure):
            _fields_ = [(name, ctypes.c_ulonglong) for name in (
                "ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
                "ReadTransferCount", "WriteTransferCount", "OtherTransferCount",
            )]

        class _JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_int64),
                ("PerJobUserTimeLimit", ctypes.c_int64),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class _JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", _JOBOBJECT_BASIC_LIMIT_INFORMATION),
                ("IoInfo", _IO_COUNTERS),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000
        JobObjectExtendedLimitInformation = 9

        kernel32 = ctypes.windll.kernel32
        h_job = kernel32.CreateJobObjectW(None, None)
        if not h_job:
            return None
        info = _JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if not kernel32.SetInformationJobObject(
            h_job, JobObjectExtendedLimitInformation, ctypes.byref(info), ctypes.sizeof(info)
        ):
            kernel32.CloseHandle(h_job)
            return None
        _WINDOWS_CLEANUP_JOB_HANDLE = h_job
        return h_job
    except Exception as exc:
        _log_failure("create Windows job object for child-process cleanup", exc)
        return None


def _assign_child_to_cleanup_job(pid: int) -> None:
    """Best-effort; a failure here just means we fall back to explicit stop-on-shutdown
    cleanup (already in place) instead of Windows guaranteeing it on an ungraceful exit."""
    if sys.platform != "win32":
        return
    h_job = _get_windows_cleanup_job()
    if not h_job:
        return
    try:
        import ctypes
        PROCESS_TERMINATE = 0x0001
        PROCESS_SET_QUOTA = 0x0100
        kernel32 = ctypes.windll.kernel32
        h_process = kernel32.OpenProcess(PROCESS_TERMINATE | PROCESS_SET_QUOTA, False, pid)
        if not h_process:
            return
        try:
            kernel32.AssignProcessToJobObject(h_job, h_process)
        finally:
            kernel32.CloseHandle(h_process)
    except Exception as exc:
        _log_failure(f"assign pid={pid} to cleanup job object", exc)


async def _stop_multiview_process(channel_id: str, *, term_timeout: float = 5.0, kill_timeout: float = 3.0) -> None:
    entry = _MULTIVIEW_PROCESSES.pop(channel_id, None)
    if not entry:
        return
    process: asyncio.subprocess.Process = entry["process"]
    if sys.platform == "win32":
        # Do not trust process.returncode here: under a heavy piped-stdout
        # ffmpeg process, asyncio's ProactorEventLoop has been observed to
        # report a process as already exited while the real ffmpeg.exe is
        # still alive and encoding, which would silently skip termination
        # entirely and leak the process. taskkill acts on the OS process
        # table directly regardless of what asyncio believes, and killing an
        # already-dead PID is a harmless no-op.
        try:
            kill_proc = await asyncio.create_subprocess_exec(
                "taskkill", "/F", "/T", "/PID", str(process.pid),
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            )
            await kill_proc.wait()
        except OSError as exc:
            _log_failure(f"taskkill ffmpeg multiview={channel_id}", exc, logging.ERROR)
        try:
            await asyncio.wait_for(process.wait(), timeout=kill_timeout)
        except asyncio.TimeoutError:
            pass
    elif process.returncode is None:
        try:
            process.terminate()
            await asyncio.wait_for(process.wait(), timeout=term_timeout)
        except (asyncio.TimeoutError, ProcessLookupError):
            try:
                process.kill()
                await asyncio.wait_for(process.wait(), timeout=kill_timeout)
            except (asyncio.TimeoutError, ProcessLookupError) as exc:
                _log_failure(f"kill ffmpeg multiview={channel_id}", exc, logging.ERROR)
    for task_key in ("watch_task", "log_task"):
        task = entry.get(task_key)
        if task and not task.done():
            task.cancel()
    tasks = [entry.get(key) for key in ("watch_task", "log_task") if entry.get(key)]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    shutil.rmtree(entry["output_dir"], ignore_errors=True)


async def _read_hls_playlist_snapshot(path: Path) -> Optional[bytes]:
    """Read an HLS .m3u8 playlist as one atomic in-memory snapshot instead of via
    FileResponse. FileResponse stats the file for Content-Length and then streams it
    from disk in a separate step; ffmpeg rewrites this exact file in place on every
    segment rotation, so if it truncates/rewrites between the stat and the stream,
    the declared Content-Length no longer matches what's actually sent and h11 aborts
    the connection ("Too little data for declared Content-Length"). A single
    read_bytes() call can't observe a torn/partial state that way — segment files
    aren't affected since ffmpeg writes each one once as a discrete completed file."""
    try:
        return await asyncio.to_thread(path.read_bytes)
    except OSError:
        return None


async def _wait_for_first_segment(
    out_dir: Path,
    process: "asyncio.subprocess.Process",
    timeout: float,
    poll_interval: float = 0.25,
) -> bool:
    playlist = out_dir / "index.m3u8"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.returncode is not None:
            return False
        if playlist.exists():
            try:
                text = playlist.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                text = ""
            if "#EXTINF" in text:
                first_segment = next((line for line in text.splitlines() if line.endswith(".ts")), None)
                if first_segment and (out_dir / first_segment).exists():
                    return True
        await asyncio.sleep(poll_interval)
    return False


def _running_multiview_count(exclude: str = "") -> int:
    return sum(
        1
        for cid, entry in _MULTIVIEW_PROCESSES.items()
        if cid != exclude and entry["process"].returncode is None and not entry.get("exited")
    )


async def _spawn_multiview(channel_id: str, data: dict) -> Tuple[bool, str]:
    """Start ffmpeg for a multiview channel if it isn't already running.
    Returns (True, "") once the first segment is ready, or (False, detail) on refusal/failure —
    `detail` is only meaningful when no entry has been recorded in _MULTIVIEW_FAILURES (the
    caller prefers that dict's message when one exists, e.g. after a real ffmpeg failure)."""
    async with _multiview_lock(channel_id):
        entry = _MULTIVIEW_PROCESSES.get(channel_id)
        if entry and entry["process"].returncode is None and not entry.get("exited"):
            entry["last_access"] = time.monotonic()
            return True, ""

        if not FFMPEG_AVAILABLE:
            LOGGER.warning("Refusing to start multiview channel=%s: ffmpeg unavailable", channel_id)
            return False, "Multi-View stream failed to start: ffmpeg is unavailable"

        cooldown = _multiview_cooldown_remaining(channel_id)
        if cooldown > 0:
            LOGGER.info("Skipping multiview spawn channel=%s: %.0fs left in failure backoff", channel_id, cooldown)
            return False, ""

        running_count = _running_multiview_count(exclude=channel_id)
        if running_count >= MAX_CONCURRENT_MULTIVIEW:
            LOGGER.warning(
                "Refusing to start multiview channel=%s: %d/%d concurrent Multi-View streams already running",
                channel_id, running_count, MAX_CONCURRENT_MULTIVIEW,
            )
            return False, (
                f"Multi-View stream failed to start: {MAX_CONCURRENT_MULTIVIEW} concurrent Multi-View "
                "channel(s) are already running. Stop one first, or raise MAX_CONCURRENT_MULTIVIEW."
            )

        out_dir = MULTIVIEW_OUTPUT_ROOT / channel_id
        shutil.rmtree(out_dir, ignore_errors=True)
        out_dir.mkdir(parents=True, exist_ok=True)

        args = _build_multiview_ffmpeg_args(channel_id, data, out_dir)
        try:
            process = await asyncio.create_subprocess_exec(FFMPEG_PATH, *args, **_multiview_popen_kwargs(out_dir))
        except (FileNotFoundError, OSError) as exc:
            _log_failure(f"spawn ffmpeg multiview={channel_id}", exc, logging.ERROR)
            return False, "Multi-View stream failed to start: could not launch ffmpeg"
        _assign_child_to_cleanup_job(process.pid)

        log_lines: "deque[str]" = deque(maxlen=200)
        watch_task = _spawn_background_task(
            _watch_multiview_process(channel_id, process),
            f"watch multiview ffmpeg={channel_id}",
        )
        log_task = _spawn_background_task(
            _drain_multiview_log(channel_id, process, log_lines),
            f"drain multiview ffmpeg log={channel_id}",
        )
        _MULTIVIEW_PROCESSES[channel_id] = {
            "process": process,
            "output_dir": out_dir,
            "started_at": time.monotonic(),
            "last_access": time.monotonic(),
            "exited": False,
            "exit_code": None,
            "watch_task": watch_task,
            "log_task": log_task,
            "log_lines": log_lines,
        }

        ready = await _wait_for_first_segment(out_dir, process, MULTIVIEW_STARTUP_TIMEOUT_SECONDS)
        if not ready:
            failed_entry = _MULTIVIEW_PROCESSES.get(channel_id)
            error = _multiview_error_from_log(failed_entry.get("log_lines", [])) if failed_entry else "ffmpeg exited unexpectedly"
            _record_multiview_failure(channel_id, error)
            LOGGER.error("multiview channel=%s failed to produce a first segment in time: %s", channel_id, error)
            await _stop_multiview_process(channel_id)
            return False, ""
        _clear_multiview_failure(channel_id)
        return True, ""


async def _serve_multiview_stream(channel_id: str, data: dict, request: Request):
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if entry:
        entry["last_access"] = time.monotonic()
    if not entry or entry["process"].returncode is not None or entry.get("exited"):
        started, refusal_detail = await _spawn_multiview(channel_id, data)
        if not started:
            failure = _MULTIVIEW_FAILURES.get(channel_id)
            if failure:
                retry_in = round(_multiview_cooldown_remaining(channel_id))
                detail = f"Multi-View stream failed to start: {failure['last_error']} (retrying in {retry_in}s)"
            else:
                detail = refusal_detail or "Multi-View stream failed to start: ffmpeg is unavailable"
            return Response(status_code=502, content=detail)
        entry = _MULTIVIEW_PROCESSES.get(channel_id)
    host = request.headers.get("host") or f"127.0.0.1:{PORT}"
    return RedirectResponse(url=f"http://{host}/multiview/{channel_id}/index.m3u8")


async def multiview_idle_monitor() -> None:
    while True:
        try:
            now = time.monotonic()
            for channel_id, entry in list(_MULTIVIEW_PROCESSES.items()):
                if entry["process"].returncode is not None or entry.get("exited"):
                    LOGGER.warning("Reaping dead multiview ffmpeg channel=%s code=%s", channel_id, entry.get("exit_code"))
                    await _stop_multiview_process(channel_id)
                elif now - entry.get("last_access", now) > MULTIVIEW_IDLE_TIMEOUT_SECONDS:
                    LOGGER.info("Stopping idle multiview ffmpeg channel=%s", channel_id)
                    await _stop_multiview_process(channel_id)

            # The shared placeholder is meant to always be available; if it died
            # unexpectedly, respawn it eagerly rather than waiting for the next
            # dead-channel request to notice (this call is a cheap no-op when
            # it's already running fine).
            if FFMPEG_AVAILABLE:
                await _ensure_placeholder_running()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_failure("multiview idle monitor", exc)
        await asyncio.sleep(MULTIVIEW_IDLE_CHECK_INTERVAL)


def _multiview_member_validation(member_team_ids: List[str]) -> Optional[str]:
    """Return an error message if the requested member list is invalid, else None."""
    if len(member_team_ids) not in (2, 4):
        return "Select exactly 2 or 4 channels for a Multi-View."
    seen = set()
    for team_id in member_team_ids:
        if team_id in seen:
            return "Duplicate channel selected."
        seen.add(team_id)
        member_data = stream_state.get(team_id)
        if not member_data:
            return f"Unknown channel: {team_id}"
        if member_data.get("type") == "multiview":
            return "A Multi-View channel cannot include another Multi-View channel."
    return None


_MULTIVIEW_SEGMENT_NAME_RE = re.compile(r"^seg_\d{5}\.ts$")


@app.get("/multiview/{channel_id}/index.m3u8")
async def serve_multiview_playlist(channel_id: str):
    data = stream_state.get(channel_id)
    if not data or data.get("type") != "multiview":
        raise HTTPException(status_code=404, detail="Multi-View channel not found")
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if not entry or entry["process"].returncode is not None or entry.get("exited"):
        raise HTTPException(status_code=503, detail="Multi-View stream is not running")
    entry["last_access"] = time.monotonic()
    playlist_path = entry["output_dir"] / "index.m3u8"
    playlist_bytes = await _read_hls_playlist_snapshot(playlist_path)
    if playlist_bytes is None:
        raise HTTPException(status_code=503, detail="Multi-View stream is starting")
    return Response(
        content=playlist_bytes,
        media_type="application/vnd.apple.mpegurl",
        headers={"Cache-Control": "no-cache", "Access-Control-Allow-Origin": "*"},
    )


@app.get("/multiview/{channel_id}/{segment_name}")
async def serve_multiview_segment(channel_id: str, segment_name: str):
    data = stream_state.get(channel_id)
    if not data or data.get("type") != "multiview":
        raise HTTPException(status_code=404, detail="Multi-View channel not found")
    if not _MULTIVIEW_SEGMENT_NAME_RE.match(segment_name):
        raise HTTPException(status_code=404, detail="Segment not found")
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Segment not found")
    entry["last_access"] = time.monotonic()
    segment_path = entry["output_dir"] / segment_name
    if not segment_path.is_file():
        raise HTTPException(status_code=404, detail="Segment not found")
    return FileResponse(
        segment_path,
        media_type="video/mp2t",
        headers={"Cache-Control": "no-cache", "Access-Control-Allow-Origin": "*"},
    )


# --- SHARED "NO SIGNAL" PLACEHOLDER ---
# A single, always-on synthetic HLS stream shown for ANY channel (regular team,
# always-live special channel, or Multi-View member) that currently has no
# live candidates - instead of a bare 404. This turns Jellyfin's ugly "fatal
# player error" into a clean "no signal" screen, and - since Multi-View's
# ffmpeg pulls every member through this exact same /stream/{team_id} route -
# also transparently fixes a Multi-View channel refusing to start entirely
# just because one of its members isn't currently live: that member's input
# simply resolves to this placeholder instead of a 404, so ffmpeg's -i for it
# always succeeds.
PLACEHOLDER_OUTPUT_DIR = DATA_DIR / "placeholder"
PLACEHOLDER_STARTUP_TIMEOUT_SECONDS = 20.0
_PLACEHOLDER_STATE: Optional[dict] = None
_PLACEHOLDER_LOCK = asyncio.Lock()


def _build_placeholder_ffmpeg_args(out_dir: Path) -> List[str]:
    """Pure command-builder for the shared "No Signal" loop (no I/O, unit-testable)."""
    logo_path = _resource_path("assets/jellyball-logo.png")
    return [
        "-y",
        "-loop", "1", "-i", str(logo_path),
        "-f", "lavfi", "-i", "anullsrc=r=48000:cl=stereo",
        "-vf", (
            "scale=1280:720:force_original_aspect_ratio=decrease,"
            "pad=1280:720:(ow-iw)/2:(oh-ih)/2,"
            "drawtext=text='No Signal':font='Sans':fontcolor=white:fontsize=48:"
            "box=1:boxcolor=black@0.5:boxborderw=12:x=(w-text_w)/2:y=h-140"
        ),
        "-c:v", "libx264", "-preset", "ultrafast", "-tune", "stillimage",
        "-b:v", "800k", "-maxrate", "800k", "-bufsize", "1600k",
        "-g", "60", "-keyint_min", "60", "-sc_threshold", "0", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "64k", "-ac", "2",
        "-f", "hls",
        "-hls_time", "4",
        "-hls_list_size", "6",
        "-hls_flags", "delete_segments+independent_segments",
        "-hls_segment_type", "mpegts",
        "-hls_segment_filename", "seg_%05d.ts",
        "index.m3u8",
    ]


async def _drain_placeholder_log(process: "asyncio.subprocess.Process", log_lines: "deque[str]") -> None:
    try:
        while True:
            line = await process.stdout.readline()
            if not line:
                break
            log_lines.append(line.decode(errors="replace").rstrip())
    except (asyncio.CancelledError, ValueError):
        raise
    except Exception as exc:
        _log_failure("drain placeholder ffmpeg log", exc)


async def _watch_placeholder_process(process: "asyncio.subprocess.Process") -> None:
    returncode = await process.wait()
    if _PLACEHOLDER_STATE and _PLACEHOLDER_STATE["process"] is process:
        _PLACEHOLDER_STATE["exited"] = True
        if returncode != 0:
            LOGGER.warning("placeholder ffmpeg exited unexpectedly code=%s", returncode)


async def _stop_placeholder_process() -> None:
    global _PLACEHOLDER_STATE
    state, _PLACEHOLDER_STATE = _PLACEHOLDER_STATE, None
    if not state:
        return
    process: asyncio.subprocess.Process = state["process"]
    if sys.platform == "win32":
        try:
            kill_proc = await asyncio.create_subprocess_exec(
                "taskkill", "/F", "/T", "/PID", str(process.pid),
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            )
            await kill_proc.wait()
        except OSError as exc:
            _log_failure("taskkill placeholder ffmpeg", exc, logging.ERROR)
        try:
            await asyncio.wait_for(process.wait(), timeout=3.0)
        except asyncio.TimeoutError:
            pass
    elif process.returncode is None:
        try:
            process.terminate()
            await asyncio.wait_for(process.wait(), timeout=5.0)
        except (asyncio.TimeoutError, ProcessLookupError):
            try:
                process.kill()
                await asyncio.wait_for(process.wait(), timeout=3.0)
            except (asyncio.TimeoutError, ProcessLookupError) as exc:
                _log_failure("kill placeholder ffmpeg", exc, logging.ERROR)
    for task_key in ("watch_task", "log_task"):
        task = state.get(task_key)
        if task and not task.done():
            task.cancel()
    shutil.rmtree(state["output_dir"], ignore_errors=True)


async def _ensure_placeholder_running() -> bool:
    """Start the shared placeholder stream if it isn't already running. Idempotent."""
    global _PLACEHOLDER_STATE
    async with _PLACEHOLDER_LOCK:
        if _PLACEHOLDER_STATE and _PLACEHOLDER_STATE["process"].returncode is None and not _PLACEHOLDER_STATE.get("exited"):
            return True
        if not FFMPEG_AVAILABLE:
            return False

        out_dir = PLACEHOLDER_OUTPUT_DIR
        shutil.rmtree(out_dir, ignore_errors=True)
        out_dir.mkdir(parents=True, exist_ok=True)

        args = _build_placeholder_ffmpeg_args(out_dir)
        try:
            process = await asyncio.create_subprocess_exec(
                FFMPEG_PATH, *args,
                cwd=str(out_dir),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except (FileNotFoundError, OSError) as exc:
            _log_failure("spawn placeholder ffmpeg", exc, logging.ERROR)
            return False
        _assign_child_to_cleanup_job(process.pid)

        log_lines: "deque[str]" = deque(maxlen=200)
        watch_task = _spawn_background_task(_watch_placeholder_process(process), "watch placeholder ffmpeg")
        log_task = _spawn_background_task(_drain_placeholder_log(process, log_lines), "drain placeholder ffmpeg log")
        _PLACEHOLDER_STATE = {
            "process": process,
            "output_dir": out_dir,
            "exited": False,
            "watch_task": watch_task,
            "log_task": log_task,
            "log_lines": log_lines,
        }

        ready = await _wait_for_first_segment(out_dir, process, PLACEHOLDER_STARTUP_TIMEOUT_SECONDS)
        if not ready:
            error = _multiview_error_from_log(log_lines)
            LOGGER.error("placeholder ffmpeg failed to produce a first segment: %s", error)
            await _stop_placeholder_process()
            return False
        return True


async def _serve_placeholder_stream(request: Request):
    ready = await _ensure_placeholder_running()
    if not ready:
        return Response(status_code=404, content="Stream unavailable")
    if _PLACEHOLDER_STATE:
        _PLACEHOLDER_STATE["last_access"] = time.monotonic()
    host = request.headers.get("host") or f"127.0.0.1:{PORT}"
    return RedirectResponse(url=f"http://{host}/placeholder/index.m3u8")


_PLACEHOLDER_SEGMENT_NAME_RE = re.compile(r"^seg_\d{5}\.ts$")


@app.get("/placeholder/index.m3u8")
async def serve_placeholder_playlist():
    if not _PLACEHOLDER_STATE or _PLACEHOLDER_STATE["process"].returncode is not None or _PLACEHOLDER_STATE.get("exited"):
        raise HTTPException(status_code=503, detail="Placeholder stream is not running")
    playlist_path = _PLACEHOLDER_STATE["output_dir"] / "index.m3u8"
    playlist_bytes = await _read_hls_playlist_snapshot(playlist_path)
    if playlist_bytes is None:
        raise HTTPException(status_code=503, detail="Placeholder stream is starting")
    return Response(
        content=playlist_bytes,
        media_type="application/vnd.apple.mpegurl",
        headers={"Cache-Control": "no-cache", "Access-Control-Allow-Origin": "*"},
    )


@app.get("/placeholder/{segment_name}")
async def serve_placeholder_segment(segment_name: str):
    if not _PLACEHOLDER_SEGMENT_NAME_RE.match(segment_name):
        raise HTTPException(status_code=404, detail="Segment not found")
    if not _PLACEHOLDER_STATE:
        raise HTTPException(status_code=404, detail="Segment not found")
    segment_path = _PLACEHOLDER_STATE["output_dir"] / segment_name
    if not segment_path.is_file():
        raise HTTPException(status_code=404, detail="Segment not found")
    return FileResponse(
        segment_path,
        media_type="video/mp2t",
        headers={"Cache-Control": "no-cache", "Access-Control-Allow-Origin": "*"},
    )


@app.get("/stream/{team_id}")
async def proxy_stream(team_id: str, request: Request, provider: str = ""):
    data = stream_state.get(team_id)
    if not data:
        return Response(status_code=404, content="Stream unavailable")
    if data.get("type") == "multiview":
        return await _serve_multiview_stream(team_id, data, request)
    if not data.get("candidates"):
        return await _serve_placeholder_stream(request)
    active_idx = data.get("active_index", 0)
    if active_idx >= len(data["candidates"]):
        active_idx = 0

    if provider and len(data["candidates"]) > 0:
        for idx, candidate in enumerate(data["candidates"]):
            if candidate.get("provider", "").lower() == provider.lower():
                active_idx = idx
                break

    active_stream = data["candidates"][active_idx]
    target_url = active_stream["url"]
    if not _validate_upstream_url(target_url):
        LOGGER.warning("Rejected invalid active stream URL team=%s", team_id)
        return Response(status_code=502, content="Invalid upstream stream URL")
    referer = active_stream.get("referer", "")
    origin = active_stream.get("origin", "")
    if referer and not _validate_upstream_url(referer):
        referer = ""
    headers = _upstream_media_headers(referer, origin)
    host = request.headers.get("host") or f"127.0.0.1:{PORT}"
    proxy_origin = f"{request.url.scheme}://{host}"

    owns_client = SHARED_HTTP_CLIENT is None
    client = SHARED_HTTP_CLIENT or httpx.AsyncClient(follow_redirects=False, timeout=12.0, http2=True)
    try:
        result = await _fetch_upstream_body(client, target_url, headers, MAX_MANIFEST_BYTES)
        if result is None:
            return Response(status_code=502, content="Upstream manifest unavailable")
        status_code, effective_url, _, body, _ = result
        if status_code != 200:
            return Response(status_code=status_code)
        _maybe_record_playback_event(team_id)
        manifest_text = body.decode("utf-8", errors="replace")
        await _ensure_startup_buffer(
            f"{effective_url}\0{referer}\0{origin}",
            client,
            _startup_media_urls(manifest_text, effective_url),
            referer,
            origin,
        )
        rewritten = rewrite_m3u8(manifest_text, effective_url, referer, proxy_origin, origin)
        return Response(
            content=rewritten,
            media_type="application/vnd.apple.mpegurl",
            headers={"Cache-Control": "no-cache", "Access-Control-Allow-Origin": "*"},
        )
    except Exception as exc:
        _log_failure(f"proxy manifest team={team_id}", exc, logging.ERROR)
        return Response(status_code=502, content="Upstream manifest unavailable")
    finally:
        if owns_client:
            await client.aclose()

@app.get("/substream.m3u8")
async def proxy_substream(request: Request, url: str, ref: str = "", org: str = ""):
    decoded_url = url
    decoded_ref = ref or ""
    decoded_origin = org or ""
    if not _validate_upstream_url(decoded_url):
        return Response(status_code=400, content="Invalid upstream manifest URL")
    if decoded_ref and not _validate_upstream_url(decoded_ref):
        return Response(status_code=400, content="Invalid upstream referer URL")
    headers = _upstream_media_headers(decoded_ref, decoded_origin)
    host = request.headers.get("host") or f"127.0.0.1:{PORT}"
    proxy_origin = f"{request.url.scheme}://{host}"
    
    owns_client = SHARED_HTTP_CLIENT is None
    client = SHARED_HTTP_CLIENT or httpx.AsyncClient(follow_redirects=False, timeout=12.0, http2=True)
    try:
        result = await _fetch_upstream_body(client, decoded_url, headers, MAX_MANIFEST_BYTES)
        if result is None:
            return Response(status_code=502, content="Upstream manifest unavailable")
        status_code, effective_url, _, body, _ = result
        if status_code != 200:
            return Response(status_code=status_code)
        manifest_text = body.decode("utf-8", errors="replace")
        await _ensure_startup_buffer(
            f"{effective_url}\0{decoded_ref}\0{decoded_origin}",
            client,
            _startup_media_urls(manifest_text, effective_url),
            decoded_ref,
            decoded_origin,
        )
        rewritten = rewrite_m3u8(manifest_text, effective_url, decoded_ref, proxy_origin, decoded_origin)
        return Response(
            content=rewritten,
            media_type="application/vnd.apple.mpegurl",
            headers={"Cache-Control": "no-cache", "Access-Control-Allow-Origin": "*"},
        )
    except Exception as exc:
        _log_failure("proxy submanifest", exc, logging.ERROR)
        return Response(status_code=502, content="Upstream manifest unavailable")
    finally:
        if owns_client:
            await client.aclose()


@app.get("/resource")
async def proxy_resource(request: Request, url: str, ref: str = "", org: str = ""):
    """Proxy HLS key and initialization resources with the original media type."""
    decoded_url = url
    decoded_ref = ref or ""
    decoded_origin = org or ""
    if not _validate_upstream_url(decoded_url):
        return Response(status_code=400, content="Invalid upstream resource URL")
    if decoded_ref and not _validate_upstream_url(decoded_ref):
        return Response(status_code=400, content="Invalid upstream referer URL")

    owns_client = SHARED_HTTP_CLIENT is None
    client = SHARED_HTTP_CLIENT or httpx.AsyncClient(
        follow_redirects=False,
        timeout=STREAM_REQUEST_TIMEOUT,
        http2=True,
    )
    try:
        range_header = request.headers.get("range", "").strip()
        if range_header and not re.fullmatch(r"bytes=\d*-\d*", range_header):
            range_header = ""
        upstream_headers = _upstream_media_headers(decoded_ref, decoded_origin)
        if range_header:
            upstream_headers["Range"] = range_header
        result = await _fetch_upstream_body(
            client,
            decoded_url,
            upstream_headers,
            MAX_RESOURCE_BYTES,
        )
        if result is None:
            return Response(status_code=502, content="Upstream resource unavailable")
        status_code, _, content_type, body, response_headers = result
        if status_code not in (200, 206):
            headers = {
                key: response_headers[key]
                for key in ("content-range", "accept-ranges")
                if response_headers.get(key)
            }
            return Response(content=body, status_code=status_code, headers=headers)
        media_type = content_type.split(";", 1)[0] or "application/octet-stream"
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-cache",
        }
        for key in ("content-range", "accept-ranges"):
            if response_headers.get(key):
                headers[key] = response_headers[key]
        return Response(content=body, status_code=status_code, media_type=media_type, headers=headers)
    except Exception as exc:
        _log_failure("proxy HLS resource", exc, logging.ERROR)
        return Response(status_code=502, content="Upstream resource unavailable")
    finally:
        if owns_client:
            await client.aclose()


async def prefetch_next_chunks(current_chunk_url: str, referer: str = "", origin: str = "") -> None:
    global _PREFETCH_SEMAPHORE
    if PREFETCH_CHUNK_COUNT <= 0:
        return

    client = SHARED_HTTP_CLIENT
    if client is None:
        return

    next_urls = _get_next_manifest_chunks(current_chunk_url, PREFETCH_CHUNK_COUNT)
    if not next_urls:
        return

    if _PREFETCH_SEMAPHORE is None:
        _PREFETCH_SEMAPHORE = asyncio.Semaphore(PREFETCH_CONCURRENCY)

    async def prefetch_one(next_url: str) -> None:
        cache_key = f"{next_url}\0{referer}\0{origin}"
        if await CHUNK_CACHE.get(cache_key):
            return
        with _PREFETCH_LOCK:
            if cache_key in _PREFETCH_IN_FLIGHT:
                return
            _PREFETCH_IN_FLIGHT.add(cache_key)

        try:
            async with _PREFETCH_SEMAPHORE:
                result = await _fetch_upstream_body(
                    client,
                    next_url,
                    _upstream_media_headers(referer, origin),
                    MAX_CACHEABLE_CHUNK_BYTES,
                )
            if result:
                status_code, _, _, body, _ = result
                if status_code in (200, 206) and body:
                    await CHUNK_CACHE.put(cache_key, body, STREAM_CHUNK_CACHE_TTL)
        except Exception as exc:
            LOGGER.debug("Chunk prefetch failed error=%s", type(exc).__name__)
        finally:
            with _PREFETCH_LOCK:
                _PREFETCH_IN_FLIGHT.discard(cache_key)

    await asyncio.gather(
        *(prefetch_one(url) for url in next_urls),
        return_exceptions=True,
    )


@app.get("/chunk.mp4")
@app.get("/chunk.aac")
@app.get("/chunk.vtt")
@app.get("/chunk.ts")
@app.get("/chunk")
async def proxy_chunk(request: Request, url: str, ref: str = "", org: str = ""):
    decoded_url = url
    decoded_ref = ref or ""
    decoded_origin = org or ""
    if not _validate_upstream_url(decoded_url):
        return Response(status_code=400, content="Invalid upstream chunk URL")
    if decoded_ref and not _validate_upstream_url(decoded_ref):
        return Response(status_code=400, content="Invalid upstream referer URL")

    range_header = request.headers.get("range", "").strip()
    if range_header and not re.fullmatch(r"bytes=\d*-\d*", range_header):
        range_header = ""
    cache_key = f"{decoded_url}\0{decoded_ref}\0{decoded_origin}\0{range_header}"
    cached_bytes = await CHUNK_CACHE.get(cache_key) if not range_header else None
    if cached_bytes is not None:
        if PREFETCH_CHUNK_COUNT > 0:
            _spawn_background_task(prefetch_next_chunks(decoded_url, decoded_ref, decoded_origin), "prefetch cached stream chunks")
        return Response(
            content=cached_bytes,
            media_type=_chunk_media_type(decoded_url),
            headers={"Access-Control-Allow-Origin": "*", "Cache-Control": "public, max-age=15"},
        )

    owns_singleflight = False
    if not range_header:
        for _ in range(20):
            with _PREFETCH_LOCK:
                in_flight = cache_key in _PREFETCH_IN_FLIGHT
            if not in_flight:
                with _PREFETCH_LOCK:
                    if cache_key not in _PREFETCH_IN_FLIGHT:
                        _PREFETCH_IN_FLIGHT.add(cache_key)
                        owns_singleflight = True
                        break
            await asyncio.sleep(0.05)
            cached_bytes = await CHUNK_CACHE.get(cache_key)
            if cached_bytes is not None:
                return Response(
                    content=cached_bytes,
                    media_type=_chunk_media_type(decoded_url),
                    headers={"Access-Control-Allow-Origin": "*", "Cache-Control": "public, max-age=15"},
                )

    headers = _upstream_media_headers(decoded_ref, decoded_origin)
    if range_header:
        headers["Range"] = range_header
    if not range_header and PREFETCH_CHUNK_COUNT > 0:
        _spawn_background_task(prefetch_next_chunks(decoded_url, decoded_ref, decoded_origin), "prefetch stream chunks")

    if range_header:
        # A Range request must be streamed, not buffered: clients that read
        # byte-range HLS segments (multiple EXT-X-BYTERANGE cuts of one shared
        # file, as real providers and ffmpeg's own hls demuxer both use) commonly
        # ask for "bytes=X-<end-of-file>" and only read as many bytes as they
        # actually need before closing the connection. Buffering the full
        # declared range into memory first (as _fetch_upstream_body does, for
        # the small bounded manifests/resources it's meant for) would reject
        # that as exceeding MAX_RESOURCE_BYTES even though the real transfer is
        # small - the fix is to relay bytes as they arrive instead.
        owns_client = SHARED_HTTP_CLIENT is None
        client = SHARED_HTTP_CLIENT or httpx.AsyncClient(
            timeout=STREAM_REQUEST_TIMEOUT, follow_redirects=False, http2=True
        )
        current_url = decoded_url
        stream_ctx = None
        response = None
        try:
            for _ in range(MAX_UPSTREAM_REDIRECTS + 1):
                safe_url = await validate_http_url_async(current_url)
                if not safe_url:
                    return Response(status_code=502, content="Invalid upstream chunk redirect")
                stream_ctx = client.stream("GET", safe_url, headers=headers, timeout=STREAM_REQUEST_TIMEOUT)
                response = await stream_ctx.__aenter__()
                if 300 <= response.status_code < 400:
                    location = response.headers.get("location")
                    await stream_ctx.__aexit__(None, None, None)
                    if not location:
                        return Response(status_code=502, content="Upstream ranged chunk unavailable")
                    current_url = urllib.parse.urljoin(str(response.url), location)
                    stream_ctx, response = None, None
                    continue
                break
            if response is None:
                return Response(status_code=502, content="Upstream ranged chunk unavailable")

            if response.status_code not in (200, 206):
                body = await response.aread()
                await stream_ctx.__aexit__(None, None, None)
                if owns_client:
                    await client.aclose()
                return Response(status_code=response.status_code, content=body)

            response_headers_out = {
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "no-cache",
            }
            for key in ("content-range", "accept-ranges"):
                if response.headers.get(key):
                    response_headers_out[key] = response.headers[key]
            status_code = response.status_code
            opened_response, opened_ctx = response, stream_ctx

            async def ranged_stream_generator():
                try:
                    async for block in opened_response.aiter_bytes(chunk_size=131072):
                        yield block
                except (httpx.TimeoutException, httpx.NetworkError) as exc:
                    _log_failure("stream ranged chunk", exc)
                finally:
                    await opened_ctx.__aexit__(None, None, None)
                    if owns_client:
                        await client.aclose()

            return StreamingResponse(
                ranged_stream_generator(),
                status_code=status_code,
                media_type=_chunk_media_type(decoded_url),
                headers=response_headers_out,
            )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            _log_failure("open ranged chunk stream", exc)
            if stream_ctx is not None:
                await stream_ctx.__aexit__(type(exc), exc, exc.__traceback__)
            if owns_client:
                await client.aclose()
            return Response(status_code=502, content="Upstream ranged chunk unavailable")

    async def stream_generator():
        chunk_buffer = bytearray()
        cacheable = True
        owns_client = SHARED_HTTP_CLIENT is None
        client = SHARED_HTTP_CLIENT or httpx.AsyncClient(
            timeout=STREAM_REQUEST_TIMEOUT, follow_redirects=False, http2=True
        )
        try:
            for attempt in range(3):
                try:
                    async with client.stream(
                        "GET", decoded_url, headers=headers, timeout=STREAM_REQUEST_TIMEOUT
                    ) as resp:
                        if resp.status_code not in (200, 206):
                            if resp.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                                await asyncio.sleep(0.25 * (attempt + 1))
                                continue
                            LOGGER.warning("Chunk request returned status=%s", resp.status_code)
                            return

                        async for block in resp.aiter_bytes(chunk_size=131072):
                            if cacheable and not range_header:
                                remaining = MAX_CACHEABLE_CHUNK_BYTES - len(chunk_buffer)
                                if len(block) <= remaining:
                                    chunk_buffer.extend(block)
                                else:
                                    cacheable = False
                                    chunk_buffer.clear()
                            yield block
                        if cacheable and not range_header and chunk_buffer:
                            await CHUNK_CACHE.put(cache_key, bytes(chunk_buffer), STREAM_CHUNK_CACHE_TTL)
                        return
                except (httpx.TimeoutException, httpx.NetworkError) as exc:
                    LOGGER.debug("Chunk request attempt failed attempt=%d error=%s", attempt + 1, type(exc).__name__)
                    if attempt < 2:
                        await asyncio.sleep(0.25 * (attempt + 1))
                except Exception as exc:
                    _log_failure("proxy chunk stream", exc)
                    return
        finally:
            if owns_singleflight:
                with _PREFETCH_LOCK:
                    _PREFETCH_IN_FLIGHT.discard(cache_key)
            if owns_client:
                await client.aclose()

    return StreamingResponse(
        stream_generator(),
        media_type=_chunk_media_type(decoded_url),
        headers={"Access-Control-Allow-Origin": "*", "Cache-Control": "public, max-age=15"},
    )

@app.get("/playlist.m3u", response_class=PlainTextResponse)
async def generate_m3u(request: Request):
    host = request.headers.get("host") or f"127.0.0.1:{PORT}"
    scheme = getattr(getattr(request, "url", None), "scheme", "http")
    base_url = f"{scheme}://{host}"
    lines = ["#EXTM3U"]
    for team_id, data in stream_state.items():
        if _channel_is_off_season(data):
            continue
        local_proxy_url = f"http://{host}/stream/{team_id}"
        name = str(data.get("name") or team_id)
        tvg_id = _channel_tvg_id(team_id, data)
        logo = _channel_logo_url(data)
        logo_attr = f' tvg-logo="{_m3u_attribute(logo)}"' if logo else ""

        quality_hint = ""
        if data.get("candidates"):
            healthy = data.get("is_healthy", False)
            status = "🟢" if healthy else "🔴"
            quality_hint = f" {status}"

        display_name = f"{name}{quality_hint}"
        lines.append(
            f'#EXTINF:-1 tvg-id="{_m3u_attribute(tvg_id)}" '
            f'tvg-name="{_m3u_attribute(name)}"{logo_attr} '
            f'group-title="{_m3u_attribute(_channel_group_title(data))}",{display_name}'
        )
        lines.append(local_proxy_url)
    return "\n".join(lines)


TVGUIDE_SPECIAL_CHANNEL_IDS = {
    "9200004533": "BigTenNetwork.us",
    "9200006937": "ESPN.us",
    "9200012351": "ESPN2.us",
    "9233011350": "ESPNU.us",
    "9233008440": "FoxSports1.us",
    "9200009884": "FoxSports2.us",
    "9233013235": "CBSSportsNetwork.us",
    "9233011830": "TNT.us",
    "9200017734": "ACCNetwork.us",
    "9233008517": "SECNetwork.us",
    "9200009223": "MLBNetwork.us",
    "9200000070": "NBATV.us",
    "9200004330": "NFLNetwork.us",
    "9233009455": "NHLNetwork.us",
    "9233011874": "ABC.us",
    "9233002271": "FOX.us",
    "9200018514": "CBS.us",
    "9233009876": "NBC.us",
    "9233011398": "CW.us",
    "9233000403": "TBS.us",
    "9233004106": "USANetwork.us",
    "9200009547": "TruTV.us",
}
GUIDE_HORIZON_DAYS = 10
_TVGUIDE_EPG_CACHE: Dict[str, List[dict]] = {}
_TVGUIDE_EPG_CACHED_AT: float = 0.0


async def _fetch_tvguide_epg() -> Dict[str, List[dict]]:
    """Fetch linear schedule blocks for always-live sports channels from TVGuide."""
    global _TVGUIDE_EPG_CACHE, _TVGUIDE_EPG_CACHED_AT
    now = time.monotonic()
    if _TVGUIDE_EPG_CACHE and (now - _TVGUIDE_EPG_CACHED_AT < 1800.0):
        return _TVGUIDE_EPG_CACHE

    url = f"https://backend.tvguide.com/tvschedules/tvguide/9100001138/web?start={int(time.time())}&duration={GUIDE_HORIZON_DAYS * 1440}"
    headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Referer": "https://www.tvguide.com/",
    }
    client = SHARED_HTTP_CLIENT or httpx.AsyncClient(timeout=10.0, follow_redirects=True)
    owns_client = SHARED_HTTP_CLIENT is None
    try:
        resp = await client.get(url, headers=headers)
        if resp.status_code == 200:
            items = resp.json().get("data", {}).get("items", [])
            schedules: Dict[str, List[dict]] = {}
            for item in items:
                ch_id = str(item.get("channel", {}).get("sourceId", ""))
                tvg_id = TVGUIDE_SPECIAL_CHANNEL_IDS.get(ch_id)
                if tvg_id:
                    schedules[tvg_id] = item.get("programSchedules", [])
            if schedules:
                _TVGUIDE_EPG_CACHE = schedules
                _TVGUIDE_EPG_CACHED_AT = now
    except Exception as exc:
        LOGGER.debug("TVGuide EPG fetch failed: %s", type(exc).__name__)
    finally:
        if owns_client:
            await client.aclose()
    return _TVGUIDE_EPG_CACHE


@app.get("/epg.xml", response_class=PlainTextResponse)
async def generate_xmltv(request: Request = None):
    base_url = ""
    if request is not None:
        host = request.headers.get("host") or f"127.0.0.1:{PORT}"
        scheme = getattr(getattr(request, "url", None), "scheme", "http")
        base_url = f"{scheme}://{host}"
    now_utc = datetime.now(timezone.utc)
    xml = ['<?xml version="1.0" encoding="UTF-8"?>', '<tv>']
    
    for team_id, data in stream_state.items():
        if _channel_is_off_season(data):
            continue
        channel_id = _channel_tvg_id(team_id, data)
        name_esc = xml_escape(data["name"])
        logo = _channel_logo_url(data)
        icon_tag = f'\n    <icon src="{xml_escape(logo)}" />' if logo else ""
        xml.append(f'  <channel id="{xml_escape(channel_id)}">')
        xml.append(f'    <display-name>{name_esc}</display-name>{icon_tag}')
        xml.append('  </channel>')

    guide_start = now_utc - timedelta(hours=1)
    guide_end = now_utc + timedelta(days=GUIDE_HORIZON_DAYS)
    tvguide_schedules = await _fetch_tvguide_epg()

    for team_id, data in stream_state.items():
        if _channel_is_off_season(data):
            continue
        channel_id = _channel_tvg_id(team_id, data)
        name_esc = xml_escape(data["name"])
        logo = _channel_logo_url(data)
        icon_tag = f'\n    <icon src="{xml_escape(logo)}" />' if logo else ""
        
        dt_start, dt_stop = parse_team_schedule(data)
        has_specific_game = bool(
            dt_start and dt_stop and dt_stop >= guide_start and dt_start <= guide_end
        )

        if has_specific_game:
            if dt_start > guide_start:
                xml.append(f'  <programme channel="{xml_escape(channel_id)}" start="{xmltv_ts(guide_start)}" stop="{xmltv_ts(min(dt_start, guide_end))}">')
                xml.append(f'    <title>{name_esc} Standby</title>')
                xml.append(f'    <desc>Waiting for the scheduled {name_esc} broadcast.</desc>')
                if icon_tag: xml.append(f'    <icon src="{xml_escape(logo)}" />')
                xml.append('  </programme>')

            live_start = max(dt_start, guide_start)
            live_stop = min(dt_stop, guide_end)
            xml.append(f'  <programme channel="{xml_escape(channel_id)}" start="{xmltv_ts(live_start)}" stop="{xmltv_ts(live_stop)}">')
            xml.append(f'    <title>{name_esc} Scheduled Event</title>')
            xml.append(f'    <desc>Scheduled live stream window for {name_esc}. Searching starts one hour before the event and continues one hour after the scheduled end.</desc>')
            if icon_tag: xml.append(f'    <icon src="{xml_escape(logo)}" />')
            xml.append('  </programme>')

            if dt_stop < guide_end:
                xml.append(f'  <programme channel="{xml_escape(channel_id)}" start="{xmltv_ts(max(dt_stop, guide_start))}" stop="{xmltv_ts(guide_end)}">')
                xml.append(f'    <title>{name_esc} Standby</title>')
                xml.append(f'    <desc>Post-event standby for {name_esc}; the next scheduled event will refresh this guide.</desc>')
                if icon_tag: xml.append(f'    <icon src="{xml_escape(logo)}" />')
                xml.append('  </programme>')
        else:
            programs = tvguide_schedules.get(channel_id, [])
            if _channel_is_always_live(data) and programs:
                for prog in programs:
                    p_start = datetime.fromtimestamp(prog.get("startTime", 0), tz=timezone.utc)
                    p_stop = datetime.fromtimestamp(prog.get("endTime", 0), tz=timezone.utc)
                    if p_stop <= guide_start or p_start >= guide_end:
                        continue
                    p_start = max(p_start, guide_start)
                    p_stop = min(p_stop, guide_end)
                    p_title = xml_escape(prog.get("title") or f"{name_esc} Live")
                    p_desc = xml_escape(prog.get("description") or f"Live broadcast on {name_esc}.")
                    xml.append(f'  <programme channel="{xml_escape(channel_id)}" start="{xmltv_ts(p_start)}" stop="{xmltv_ts(p_stop)}">')
                    xml.append(f'    <title>{p_title}</title>')
                    xml.append('    <category>Sports</category>')
                    xml.append(f'    <desc>{p_desc}</desc>')
                    if icon_tag: xml.append(f'    <icon src="{xml_escape(logo)}" />')
                    xml.append('  </programme>')
            else:
                xml.append(f'  <programme channel="{xml_escape(channel_id)}" start="{xmltv_ts(guide_start)}" stop="{xmltv_ts(guide_end)}">')
                if _channel_is_always_live(data):
                    xml.append(f'    <title>{name_esc} Live</title>')
                    xml.append('    <category>Sports</category>')
                    xml.append(f'    <desc>Always-live sports channel for {name_esc}; the linear stream is monitored continuously.</desc>')
                else:
                    xml.append(f'    <title>{name_esc} Standby</title>')
                    xml.append(f'    <desc>No verified scheduled event is available for {name_esc} in the next {GUIDE_HORIZON_DAYS} days.</desc>')
                if icon_tag: xml.append(f'    <icon src="{xml_escape(logo)}" />')
                xml.append('  </programme>')

    xml.append('</tv>')
    return "\n".join(xml)


@app.get("/api/status")
async def api_status(auth: bool = Depends(verify_dashboard_auth)):
    """Return a compact status snapshot for local dashboards and health checks."""
    channels = []
    for team_id, data in stream_state.items():
        if data.get("type") == "multiview":
            # Multi-View cards are rendered separately (no .channel-card class,
            # a different status model - running/stopped rather than
            # healthy/searching) and are polled via /api/ffmpeg-status instead.
            # Including them here would make applyStatusSnapshot()'s channel
            # count permanently mismatch the .channel-card DOM count, which
            # forces a reload-loop on every 5s poll for as long as any
            # Multi-View channel exists.
            continue
        candidates = data.get("candidates", [])
        active_index = data.get("active_index", 0)
        active = candidates[active_index] if 0 <= active_index < len(candidates) else None
        channels.append({
            "team_id": team_id,
            "name": data.get("name", team_id),
            "healthy": bool(data.get("is_healthy")),
            "candidate_count": len(candidates),
            "active_provider": active.get("provider") if active else None,
            "stream_window_active": is_stream_window_active(data),
            "start_time": data.get("start_time", ""),
            "stop_time": data.get("stop_time", ""),
            "schedule_status": data.get("schedule_status", ""),
            "season_resume_label": _season_resume_label(data.get("category", "")),
            "category": data.get("category", "custom"),
            "content_type": data.get("content_type", "team"),
            "always_live": bool(data.get("always_live")),
            "catalog_key": data.get("catalog_key", ""),
            "scrape_in_progress": bool(data.get("scrape_in_progress", False)),
            "last_scrape_started": data.get("last_scrape_started", 0.0),
            "last_scrape_completed": data.get("last_scrape_completed", 0.0),
            "scrape_result": data.get("scrape_result", "pending"),
            "scrape_error": data.get("scrape_error", ""),
        })

    return {
        "status": "ok",
        "port": PORT,
        "channels": channels,
        "channel_count": len(channels),
    }


@app.get("/api/logs")
async def api_logs(limit: int = 200, auth: bool = Depends(verify_dashboard_auth)):
    """Return recent application log lines for the local dashboard."""
    limit = max(1, min(limit, 1000))
    try:
        with LOG_FILE.open("r", encoding="utf-8", errors="replace") as log_file:
            lines = list(deque(log_file, maxlen=limit))
        return {"logs": [line.rstrip("\r\n") for line in lines], "count": len(lines)}
    except OSError:
        return {"logs": [], "count": 0}


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, tab: str = "channels", status: str = "", auth: bool = Depends(verify_dashboard_auth)):
    base_url = f"{request.url.scheme}://{request.headers.get('host', f'127.0.0.1:{PORT}')}"
    base_url_html = _html(base_url)
    dashboard_metrics = await _load_dashboard_metrics_async()
    metrics = dashboard_metrics
    favorites = dashboard_metrics["favorites"]
    provider_totals = dashboard_metrics["provider_totals"]
    notif_cfg = await get_notification_config()
    jellyfin_cfg = await get_jellyfin_config()
    playback_cfg = await get_playback_settings()
    provider_rotation_enabled = await get_setting_async("provider_rotation_mode", "0") == "1"
    catalog_entries = await get_catalog_entries()
    active_catalog_keys = {
        data.get("catalog_key")
        for data in stream_state.values()
        if data.get("catalog_key")
    }

    provider_health_html = ""
    if metrics["failovers_by_provider"]:
        provider_stats = {}
        for prov, count in metrics["failovers_by_provider"].items():
            total = provider_totals.get(prov, 0) or 1
            success_rate = max(0, 100 - (count * 100 / total)) if total > 0 else 100
            provider_stats[prov] = (success_rate, count, total)

        for prov in sorted(provider_stats.keys(), key=lambda p: provider_stats[p][0], reverse=True):
            rate, fails, total = provider_stats[prov]
            rate_color = "var(--success)" if rate > 90 else ("var(--warning)" if rate > 70 else "var(--danger)")
            provider_health_html += f'<div style="margin-bottom: 0.75rem;"><div style="display: flex; justify-content: space-between; margin-bottom: 0.25rem;"><span>{_html(prov)}</span><span style="color: {rate_color}; font-weight: 700;">{rate:.1f}%</span></div><div style="background: var(--surface-3); height: 6px; border-radius: 3px; overflow: hidden;"><div style="background: {rate_color}; height: 100%; width: {rate:.1f}%; transition: width 0.4s var(--ease);"></div></div><span class="hint-text">{total} total checks</span></div>'
    else:
        provider_health_html = '<span style="color: var(--text-muted); font-size: 0.85rem;">No provider data yet.</span>'

    catalog_groups = (
        ("ncaaf", "College Football"),
        ("ncaam", "College Basketball"),
        ("nfl", "NFL"),
        ("mlb", "MLB"),
        ("nhl", "NHL"),
        ("nba", "NBA"),
        ("special", "Always-Live Sports Channels"),
    )
    catalog_html = ""
    for group_key, group_label in catalog_groups:
        group_entries = [
            entry for entry in catalog_entries
            if ("special" if entry["content_type"] == "channel" else entry["category"]) == group_key
        ]
        if not group_entries:
            continue
        item_html = ""
        for entry in group_entries:
            checked = " checked" if entry["catalog_key"] in active_catalog_keys else ""
            live_badge = '<span class="badge catalog-live-badge">Always live</span>' if entry["always_live"] else ""
            item_html += f'''
                <div class="catalog-item" data-catalog-name="{_html(entry["name"])}">
                    <label class="catalog-toggle">
                        <input type="checkbox" name="catalog_keys" value="{_html(entry["catalog_key"])}"{checked} onchange="updateCatalogSelectionCount()" aria-label="Enable {_html(entry["name"])}">
                        <span class="catalog-name">{_html(entry["name"])}</span>
                        {live_badge}
                    </label>
                </div>'''
        catalog_html += f'''
            <details class="catalog-group">
                <summary><span>{_html(group_label)}</span><span class="badge">{len(group_entries)} available</span></summary>
                <div class="catalog-grid">{item_html}</div>
            </details>'''
    catalog_source_text = (
        "College directories refreshed from ESPN."
        if _CATALOG_REMOTE_LOADED
        else "Using bundled teams; ESPN college directories will be retried on the next refresh."
    )
    
    failover_stats_html = ""
    if metrics["failovers_by_provider"]:
        for prov, count in metrics["failovers_by_provider"].items():
            failover_stats_html += f'<span class="badge" style="margin-right: 0.5rem; background: var(--border-strong);">{_html(prov)}: {_html(count)} failover(s)</span>'
    else:
        failover_stats_html = '<span style="color: var(--text-muted); font-size: 0.85rem;">No failover incidents recorded yet.</span>'

    events_html = ""
    if metrics["recent_events"]:
        for ts, team_id, prov, ev_type, details in metrics["recent_events"]:
            badge_color = "var(--danger)" if ev_type in ["exhausted", "danger"] else ("var(--warning)" if ev_type == "failover" else "var(--success)")
            events_html += f'<tr style="border-bottom: 1px solid var(--border); font-size: 0.85rem;"><td style="padding: 0.5rem; color: var(--text-muted);">{_html(ts)}</td><td style="padding: 0.5rem; font-weight: 600;">{_html(team_id)}</td><td style="padding: 0.5rem;"><span style="background: {badge_color}; color: white; padding: 0.15rem 0.4rem; border-radius: 4px;">{_html(ev_type)}</span></td><td style="padding: 0.5rem; color: var(--text-soft);">{_html(prov)}</td><td style="padding: 0.5rem; color: var(--text-muted);">{_html(details)}</td></tr>'
    else:
        events_html = '<tr><td colspan="5" style="padding: 1rem; text-align: center; color: var(--text-muted); font-size: 0.85rem;">No historical events recorded yet.</td></tr>'

    auth_badge = '<span class="badge" style="background: var(--surface-3); color: var(--text-muted);">🔓 Local Open Access</span>'
    webhook_discord_badge = '<span class="badge" style="background: #5865F2; color: white;">Discord Alert On</span>' if notif_cfg["discord_webhook_url"] else '<span class="badge" style="background: var(--surface-3); color: var(--text-dim);">Discord Off</span>'
    webhook_telegram_badge = '<span class="badge" style="background: #229ED9; color: white;">Telegram Alert On</span>' if (notif_cfg["telegram_bot_token"] and notif_cfg["telegram_chat_id"]) else '<span class="badge" style="background: var(--surface-3); color: var(--text-dim);">Telegram Off</span>'
    jellyfin_badge = '<span class="badge" style="background: var(--purple); color: white;">🍇 Jellyfin Auto-Refresh On</span>' if jellyfin_cfg["jellyfin_api_key"] else '<span class="badge" style="background: var(--surface-3); color: var(--text-dim);">🍇 Jellyfin Manual</span>'

    channels_html = ""
    favorites_html = ""
    other_teams_html = ""

    if not stream_state:
        channels_html = '<div class="card" style="grid-column: 1 / -1; text-align: center; color: var(--text-muted); padding: 3rem;">No channels enabled. Select entries from the catalog above.</div>'
    else:
        bulk_form_start = '<form id="bulk-actions-form" style="display: none;"><input type="hidden" id="bulk-team-ids" name="team_ids" value=""></form>'

        for t_id, data in stream_state.items():
            if data.get('type') == "multiview":
                continue
            dot_class = "online" if data.get('is_healthy') else "offline"
            if data.get('is_healthy'):
                status_text = "Stream Stable & Active"
            elif data.get('schedule_status') == "off_season":
                resume_label = _season_resume_label(data.get('category', ''))
                status_text = f"Off-season (resumes {resume_label})" if resume_label else "Off-season"
            else:
                status_text = "Searching / Re-evaluating"
            candidates_list = data.get('candidates', [])
            candidates_len = len(candidates_list)
            is_favorite = t_id in favorites

            candidates_options_html = ""
            for idx, cand in enumerate(candidates_list):
                is_active = "★ " if idx == data.get('active_index', 0) else ""
                prov = cand.get('provider', 'Unknown')
                title_trunc = cand.get("match_title", "Stream")[:25]
                candidates_options_html += f'<option value="{idx}">{_html(is_active)}{_html(prov)} - {_html(title_trunc)}</option>'

            override_form = f'''
            <form action="/override/{_html(t_id)}" method="post" style="margin-top:0.75rem; display:flex; gap:0.5rem; align-items:center;">
                <select name="candidate_index" style="flex:1; padding:0.55rem; border-radius:6px; background:var(--surface-2); color:#fff; border:1px solid var(--border-strong); font-size:0.85rem;">
                    {candidates_options_html}
                </select>
                <button type="submit" style="width:auto; padding:0.55rem 0.85rem; font-size:0.8rem; background:var(--border-strong); border:1px solid var(--border-soft);">Override</button>
            </form>
            ''' if candidates_list else '<p style="font-size:0.85rem; color:var(--text-dim); margin-top:0.75rem;">No candidates available</p>'

            channel_badge = "Always live" if data.get("always_live") else (
                str(data.get("category") or "manual").upper()
            )
            remove_label = "Disable" if data.get("catalog_key") else "Remove"

            active_provider = ""
            if candidates_list:
                active_index = data.get("active_index", 0)
                if 0 <= active_index < len(candidates_list):
                    active_provider = candidates_list[active_index].get("provider", "")

            card_html = f'''
            <div class="card channel-card" data-team-id="{_html(t_id)}" data-candidate-count="{candidates_len}" data-active-provider="{_html(active_provider)}" style="margin-bottom: 0;">
                <div style="display: flex; gap: 0.75rem; align-items: flex-start;">
                    <input type="checkbox" class="team-bulk-select" data-team-id="{_html(t_id)}" style="margin-top: 0.5rem; cursor: pointer;">
                    <div style="flex: 1;">
                        <div class="team-header">
                            <h4 class="team-name">{_html(data["name"])}</h4>
                            <span class="badge">{_html(channel_badge)}</span>
                        </div>
                        <div class="status-row" data-role="status-row">
                            <div class="dot {dot_class}" data-role="status-dot"></div>
                            <span data-role="status-text">{_html(status_text)}</span>
                        </div>
                        <p class="meta-text" data-role="candidate-count">Backups: {candidates_len}</p>
                        {override_form}
                        <div style="display: flex; gap: 0.5rem; margin-top: 0.75rem; flex-wrap: wrap;">
                            <form action="/favorite/{_html(t_id)}" method="post" style="display: inline;">
                                <button type="submit" style="width: auto; padding: 0.5rem 0.75rem; font-size: 0.8rem; background: {'var(--warning)' if is_favorite else 'var(--surface-3)'}; border: 1px solid var(--border-strong);">{'⭐ Favorited' if is_favorite else '☆ Favorite'}</button>
                            </form>
                            <form action="/rescrape/{_html(t_id)}" method="post" style="display: inline;">
                                <button type="submit" style="width: auto; padding: 0.5rem 0.75rem; font-size: 0.8rem; background: var(--surface-3); border: 1px solid var(--border-strong);">🔄 Rescrape</button>
                            </form>
                            <button type="button" onclick="testTeamStream('{_html(t_id)}')" style="width: auto; padding: 0.5rem 0.75rem; font-size: 0.8rem; background: var(--surface-3); border: 1px solid var(--border-strong); color: var(--text-soft); border-radius: 6px; cursor: pointer;">🧪 Test Stream</button>
                            <form action="/remove_team/{_html(t_id)}" method="post" style="display: inline;">
                                <button class="btn-danger" type="submit" style="width: auto; padding: 0.5rem 0.75rem; margin-top: 0; font-size: 0.8rem;">{_html(remove_label)}</button>
                            </form>
                        </div>
                        <p data-role="test-result" style="font-size: 0.8rem; margin: 0.5rem 0 0; min-height: 1.1em;"></p>
                        <details style="margin-top: 0.5rem;">
                            <summary style="font-size: 0.8rem; color: var(--text-muted); cursor: pointer;">⏰ Schedule auto-disable</summary>
                            <form action="/team/{_html(t_id)}/schedule-disable" method="post" style="display: flex; gap: 0.5rem; align-items: center; margin-top: 0.5rem;">
                                <input type="date" name="disable_date" value="{_html(data.get('auto_disable_after', ''))}" style="flex: 1; padding: 0.5rem; border-radius: 6px; background: var(--surface-2); color: #fff; border: 1px solid var(--border-strong); font-size: 0.8rem;">
                                <button type="submit" style="width: auto; padding: 0.5rem 0.75rem; font-size: 0.8rem; background: var(--surface-3); border: 1px solid var(--border-strong);">Save</button>
                            </form>
                        </details>
                    </div>
                </div>
            </div>'''

            if is_favorite:
                favorites_html += card_html
            else:
                other_teams_html += card_html

        channels_html = bulk_form_start

        if stream_state:
            channels_html += '''
            <div class="card" style="background: var(--accent-card-bg); border-color: var(--accent);">
                <h3 style="margin-top: 0;">🎛️ Bulk Team Management</h3>
                <p class="hint-text">Select multiple teams to perform actions on them at once.</p>
                <div style="display: flex; gap: 0.75rem; flex-wrap: wrap;">
                    <button type="button" onclick="selectAllTeams()" style="width: auto; padding: 0.7rem 1rem; background: var(--surface-3); border: 1px solid var(--border-strong); color: var(--text-soft); border-radius: 6px; cursor: pointer; font-weight: 600;">☑️ Select All</button>
                    <button type="button" onclick="deselectAllTeams()" style="width: auto; padding: 0.7rem 1rem; background: var(--surface-3); border: 1px solid var(--border-strong); color: var(--text-soft); border-radius: 6px; cursor: pointer; font-weight: 600;">☐ Deselect All</button>
                    <button type="button" onclick="bulkFavorite()" style="width: auto; padding: 0.7rem 1rem; background: var(--warning); border: 1px solid var(--warning-border); color: #000; border-radius: 6px; cursor: pointer; font-weight: 600;">⭐ Favorite</button>
                    <button type="button" onclick="bulkUnfavorite()" style="width: auto; padding: 0.7rem 1rem; background: var(--surface-3); border: 1px solid var(--border-strong); color: var(--text-soft); border-radius: 6px; cursor: pointer; font-weight: 600;">☆ Unfavorite</button>
                    <button type="button" onclick="bulkRemove()" style="width: auto; padding: 0.7rem 1rem; background: var(--danger); border: 1px solid var(--danger-border); color: #fff; border-radius: 6px; cursor: pointer; font-weight: 600;">🗑️ Remove</button>
                </div>
            </div>
            '''

        multiview_checkbox_html = "".join(
            f'<label style="display:flex; align-items:center; gap:0.5rem; padding:0.25rem 0; font-size:0.85rem;">'
            f'<input type="checkbox" class="multiview-member-select" data-team-id="{_html(mv_t_id)}">{_html(mv_data.get("name", mv_t_id))}</label>'
            for mv_t_id, mv_data in stream_state.items()
            if mv_data.get("type") != "multiview"
        )

        multiview_rows_html = ""
        for mv_id, mv_data in stream_state.items():
            if mv_data.get("type") != "multiview":
                continue
            mv_entry = _MULTIVIEW_PROCESSES.get(mv_id)
            mv_running = bool(mv_entry and mv_entry["process"].returncode is None and not mv_entry.get("exited"))
            mv_status = "🟢 Running" if mv_running else "⚪ Stopped"
            member_ids = mv_data.get("member_team_ids", [])
            member_names = ", ".join(_html(stream_state.get(m, {}).get("name", m)) for m in member_ids)
            audio_options = "".join(
                f'<option value="{_html(m)}"{" selected" if m == mv_data.get("active_audio_team_id") else ""}>{_html(stream_state.get(m, {}).get("name", m))}</option>'
                for m in member_ids
            )
            mv_failure = _MULTIVIEW_FAILURES.get(mv_id)
            mv_failure_html = ""
            if not mv_running and mv_failure:
                retry_in = round(_multiview_cooldown_remaining(mv_id))
                mv_failure_html = (
                    f'<p class="meta-text" style="color:var(--danger-text);">⚠️ {_html(mv_failure["last_error"])} '
                    f'(failed {mv_failure["count"]}x, retrying in {retry_in}s)</p>'
                )
            multiview_rows_html += f'''
            <div class="card multiview-card" data-team-id="{_html(mv_id)}" style="margin-bottom: 0.75rem;">
                <div class="team-header">
                    <h4 class="team-name">{_html(mv_data.get("name", mv_id))}</h4>
                    <span class="badge">{_html(mv_data.get("layout", ""))}</span>
                </div>
                <p class="meta-text"><span data-role="mv-status">{mv_status}</span> &middot; Members: {member_names}</p>
                <div data-role="mv-failure">{mv_failure_html}</div>
                <div style="display: flex; gap: 0.5rem; flex-wrap: wrap; margin-top: 0.5rem; align-items: center;">
                    <form action="/multiview/{_html(mv_id)}/set-audio" method="post" style="display:flex; gap:0.5rem; align-items:center;">
                        <select name="active_audio_team_id" style="padding:0.5rem; border-radius:6px; background:var(--surface-2); color:#fff; border:1px solid var(--border-strong); font-size:0.8rem;">
                            {audio_options}
                        </select>
                        <button type="submit" style="width:auto; padding:0.5rem 0.75rem; font-size:0.8rem; background:var(--surface-3); border:1px solid var(--border-strong);">🔊 Set Audio</button>
                    </form>
                    <form action="/multiview/{_html(mv_id)}/stop" method="post" style="display:inline;">
                        <button type="submit" style="width:auto; padding:0.5rem 0.75rem; font-size:0.8rem; background:var(--surface-3); border:1px solid var(--border-strong);">⏹️ Stop</button>
                    </form>
                    <form action="/multiview/{_html(mv_id)}/remove" method="post" style="display:inline;">
                        <button class="btn-danger" type="submit" style="width:auto; padding:0.5rem 0.75rem; margin-top:0; font-size:0.8rem;">Remove</button>
                    </form>
                </div>
            </div>'''

        if not FFMPEG_AVAILABLE:
            multiview_creation_html = (
                f'<p class="hint-text" style="color:var(--danger-text);">⚠️ ffmpeg was not found at FFMPEG_PATH='
                f'"{_html(FFMPEG_PATH)}". Install ffmpeg and restart Jellyball to enable Multi-View channels.</p>'
            )
        elif not multiview_checkbox_html:
            multiview_creation_html = '<p class="hint-text">Add at least two channels above before creating a Multi-View.</p>'
        else:
            multiview_creation_html = f'''
            <form action="/multiview/create" method="post" onsubmit="return updateMultiviewTeamIds()">
                <div style="max-height: 180px; overflow-y: auto; border: 1px solid var(--border-strong); border-radius: 6px; padding: 0.5rem; margin-bottom: 0.75rem;">
                    {multiview_checkbox_html}
                </div>
                <input type="hidden" id="multiview-team-ids" name="member_team_ids" value="">
                <div style="display: flex; gap: 0.75rem; flex-wrap: wrap; align-items: center;">
                    <input type="text" name="name" placeholder="Channel name (e.g. NFL Sunday Quad-Box)" required style="flex: 1; min-width: 220px;">
                    <select name="layout" style="padding: 0.6rem; border-radius: 6px; background:var(--surface-2); color:#fff; border:1px solid var(--border-strong);">
                        <option value="grid_2x2">2x2 Grid (4 channels)</option>
                        <option value="side_by_side_2">Side-by-Side (2 channels)</option>
                    </select>
                    <button type="submit" style="width: auto; padding: 0.6rem 1rem;">➕ Create Multi-View</button>
                </div>
            </form>
            '''

        channels_html += f'''
        <div class="card" style="background: var(--accent-card-bg); border-color: var(--accent);">
            <h3 style="margin-top: 0;">🖼️ Multi-View Channels</h3>
            <p class="hint-text">Composite 2 or 4 existing channels into one grid feed using server-side ffmpeg transcoding.</p>
            {multiview_creation_html}
        </div>
        {multiview_rows_html}
        '''

        if favorites_html:
            channels_html += f'<h3 style="margin-top: 1.5rem; margin-bottom: 0.75rem;">⭐ Your Favorites</h3>{favorites_html}'
        if other_teams_html:
            channels_html += f'<h3 style="margin-top: 1.5rem; margin-bottom: 0.75rem;">📺 All Channels</h3>{other_teams_html}'

    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><meta name="color-scheme" content="dark light"><title>Jellyball Sports Manager</title>
        <script>
            (function() {{
                try {{
                    var t = localStorage.getItem('theme');
                    if (!t) {{ t = window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark'; }}
                    document.documentElement.setAttribute('data-theme', t);
                }} catch (e) {{}}
            }})();
        </script>
        <style>
            :root {{
                --bg: #0a0e17; --bg-translucent: rgba(10,14,23,0.72);
                --card-bg: #121a2c; --surface-1: #0d1526; --surface-2: #0f172a; --surface-3: #1a2436;
                --border: #1e293b; --border-strong: #30425c; --border-soft: #415367;
                --text: #f1f5f9; --text-soft: #cbd5e1; --text-muted: #94a3b8; --text-dim: #64748b;
                --accent: #3b82f6; --accent-hover: #2563eb; --accent-text: #60a5fa; --accent-rgb: 59,130,246; --accent-surface: rgba(59,130,246,0.1);
                --accent-card-bg: #182544; --purple-card-to: #171026;
                --success: #10b981; --danger: #ef4444; --danger-text: #f87171; --danger-border: #7f1d1d;
                --warning: #eab308; --warning-border: #92400e;
                --purple: #a855f7; --purple-dark: #9333ea; --purple-text: #c084fc;
                --live-bg: #14532d; --live-border: #166534; --live-text: #86efac;
                --code-bg: #080b12; --code-text: #cbd5e1;
                --shadow: rgba(0,0,0,0.45); --shadow-soft: rgba(0,0,0,0.25);
                --ease: cubic-bezier(0.16, 1, 0.3, 1);
                color-scheme: dark;
            }}
            :root[data-theme="light"] {{
                --bg: #f2f4f9; --bg-translucent: rgba(242,244,249,0.75);
                --card-bg: #ffffff; --surface-1: #eef1f7; --surface-2: #eef1f7; --surface-3: #e7ebf3;
                --border: #e2e8f0; --border-strong: #cbd5e1; --border-soft: #cbd5e1;
                --text: #0f172a; --text-soft: #334155; --text-muted: #64748b; --text-dim: #94a3b8;
                --accent: #2563eb; --accent-hover: #1d4ed8; --accent-text: #2563eb; --accent-rgb: 37,99,235; --accent-surface: rgba(37,99,235,0.07);
                --accent-card-bg: #eaf1ff; --purple-card-to: #f6effe;
                --success: #059669; --danger: #dc2626; --danger-text: #dc2626; --danger-border: #fecaca;
                --warning: #b45309; --warning-border: #fde68a;
                --purple: #9333ea; --purple-dark: #7e22ce; --purple-text: #7e22ce;
                --live-bg: #dcfce7; --live-border: #86efac; --live-text: #166534;
                --code-bg: #0f172a; --code-text: #e2e8f0;
                --shadow: rgba(15,23,42,0.12); --shadow-soft: rgba(15,23,42,0.06);
                color-scheme: light;
            }}
            * {{ box-sizing: border-box; }}
            @media (prefers-reduced-motion: reduce) {{ *, *::before, *::after {{ animation-duration: 0.001ms !important; animation-iteration-count: 1 !important; transition-duration: 0.001ms !important; scroll-behavior: auto !important; }} }}
            body {{
                font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "Segoe UI", system-ui, sans-serif;
                background: radial-gradient(circle at 15% -10%, var(--accent-surface), transparent 45%), var(--bg);
                background-attachment: fixed;
                color: var(--text); margin: 0; padding: 2.5rem 1.5rem;
                transition: background-color 0.4s var(--ease), color 0.3s var(--ease);
            }}
            @keyframes fadeInUp {{ from {{ opacity: 0; transform: translateY(14px); }} to {{ opacity: 1; transform: translateY(0); }} }}
            @keyframes fadeIn {{ from {{ opacity: 0; }} to {{ opacity: 1; }} }}
            @keyframes gradientPan {{ 0% {{ background-position: 0% 50%; }} 100% {{ background-position: 200% 50%; }} }}
            @keyframes pulseGlow {{ 0%, 100% {{ box-shadow: 0 0 0 0 rgba(16,185,129,0.55); }} 50% {{ box-shadow: 0 0 0 5px rgba(16,185,129,0); }} }}
            @keyframes spinIn {{ from {{ transform: rotate(-90deg) scale(0.6); opacity: 0; }} to {{ transform: rotate(0) scale(1); opacity: 1; }} }}
            .toast {{ position: fixed; top: 20px; right: 20px; background: var(--success); color: white; padding: 1rem 1.5rem; border-radius: 10px; box-shadow: 0 10px 30px var(--shadow); z-index: 9999; animation: slideIn 0.35s var(--ease); backdrop-filter: blur(6px); }}
            .toast.error {{ background: var(--danger); }}
            @keyframes slideIn {{ from {{ transform: translateX(420px); opacity: 0; }} to {{ transform: translateX(0); opacity: 1; }} }}
            @keyframes slideOut {{ from {{ transform: translateX(0); opacity: 1; }} to {{ transform: translateX(420px); opacity: 0; }} }}
            .container {{ max-width: 920px; margin: 0 auto; }}
            header {{ text-align: center; margin-bottom: 1.75rem; position: relative; animation: fadeInUp 0.5s var(--ease) both; }}
            header h1 {{
                font-size: 2.3rem; font-weight: 800; margin: 0 0 0.5rem 0; letter-spacing: -0.02em;
                background: linear-gradient(90deg, var(--text) 0%, var(--accent-text) 35%, var(--purple-text) 60%, var(--text) 100%);
                background-size: 220% auto; -webkit-background-clip: text; background-clip: text; color: transparent;
                animation: gradientPan 10s linear infinite;
            }}
            header p {{ color: var(--text-muted); font-size: 0.95rem; margin: 0; }}
            .header-controls {{ position: absolute; top: 0; right: 0; display: flex; gap: 0.5rem; }}
            .theme-toggle {{ background: var(--card-bg); border: 1px solid var(--border); color: var(--text); padding: 0.6rem 0.8rem; border-radius: 8px; cursor: pointer; font-size: 1.2rem; transition: transform 0.2s var(--ease), border-color 0.2s var(--ease), background-color 0.3s var(--ease); line-height: 1; }}
            .theme-toggle:hover {{ border-color: var(--accent); transform: translateY(-2px) scale(1.05); }}
            .theme-toggle:active {{ transform: scale(0.92); }}
            .theme-toggle.spin span, .theme-toggle.spin {{ animation: spinIn 0.4s var(--ease); }}
            .status-badges {{ display: flex; justify-content: center; gap: 0.5rem; margin-top: 0.75rem; flex-wrap: wrap; }}
            .status-badges .badge {{ transition: transform 0.2s var(--ease); }}
            .status-badges .badge:hover {{ transform: translateY(-1px); }}
            .tabs-nav {{
                display: flex; gap: 0.5rem; margin: 0 -1.5rem 1.5rem; padding: 0.75rem 1.5rem; border-bottom: 1px solid var(--border);
                position: sticky; top: 0; z-index: 50; background: var(--bg-translucent); backdrop-filter: blur(14px); -webkit-backdrop-filter: blur(14px);
                overflow-x: auto; scrollbar-width: none;
            }}
            .tabs-nav::-webkit-scrollbar {{ display: none; }}
            .tab-btn {{ background: var(--card-bg); color: var(--text-muted); border: 1px solid var(--border); padding: 0.7rem 1.25rem; border-radius: 8px; font-weight: 600; font-size: 0.9rem; cursor: pointer; transition: all 0.2s var(--ease); width: auto; white-space: nowrap; }}
            .tab-btn:hover {{ color: var(--text); border-color: var(--border-strong); transform: translateY(-1px); }}
            .tab-btn.active {{ background: var(--accent); color: #fff; border-color: var(--accent); box-shadow: 0 4px 14px rgba(var(--accent-rgb),0.35); }}
            .tab-pane {{ display: none; }} .tab-pane.active {{ display: block; animation: fadeInUp 0.45s var(--ease) both; }}
            .log-viewer {{ max-height: 34rem; overflow: auto; margin: 0; padding: 1rem; background: var(--code-bg); border: 1px solid var(--border); border-radius: 10px; color: var(--code-text); font: 0.78rem/1.5 ui-monospace, SFMono-Regular, Consolas, monospace; white-space: pre-wrap; word-break: break-word; }}
            .card {{
                background: var(--card-bg); border: 1px solid var(--border); border-radius: 16px; padding: 1.75rem; margin-bottom: 1.5rem;
                box-shadow: 0 10px 24px -8px var(--shadow); transition: transform 0.25s var(--ease), box-shadow 0.25s var(--ease), background-color 0.3s var(--ease), border-color 0.3s var(--ease);
                animation: fadeInUp 0.5s var(--ease) both;
            }}
            .card:hover {{ transform: translateY(-2px); box-shadow: 0 16px 32px -10px var(--shadow); }}
            .card h3 {{ margin-top: 0; margin-bottom: 1.25rem; font-size: 1.25rem; font-weight: 600; }}
            .form-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-bottom: 1rem; }}
            input[type="text"], input[type="date"], select {{ width: 100%; padding: 0.85rem 1rem; background: var(--surface-2); border: 1px solid var(--border); border-radius: 8px; color: var(--text); font-size: 0.95rem; box-sizing: border-box; transition: border-color 0.2s var(--ease), box-shadow 0.2s var(--ease); }}
            input[type="text"]:focus, input[type="date"]:focus, select:focus {{ outline: none; border-color: var(--accent); box-shadow: 0 0 0 3px rgba(var(--accent-rgb),0.2); }}
            button {{ width: 100%; padding: 0.85rem; background: var(--accent); color: white; border: none; border-radius: 8px; font-weight: 600; font-size: 0.95rem; cursor: pointer; transition: background-color 0.2s var(--ease), transform 0.15s var(--ease), box-shadow 0.2s var(--ease); }}
            button:hover {{ background: var(--accent-hover); transform: translateY(-1px); box-shadow: 0 6px 16px -4px rgba(var(--accent-rgb),0.45); }}
            button:active {{ transform: translateY(0) scale(0.98); }}
            .btn-secondary {{ background: var(--surface-3); color: var(--text-soft); border: 1px solid var(--border-strong); }} .btn-secondary:hover {{ background: var(--border-strong); color: var(--text); }}
            .btn-danger {{ background: var(--danger); margin-top: 1rem; }} .btn-danger:hover {{ background: #b91c1c; }}
            .team-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 1.25rem; }}
            .team-header {{ display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 0.75rem; }}
            .team-name {{ font-size: 1.15rem; font-weight: 700; margin: 0; }}
            .badge {{ background: var(--surface-3); color: var(--text-soft); padding: 0.25rem 0.6rem; border-radius: 6px; font-size: 0.8rem; border: 1px solid var(--border-strong); display: inline-block; }}
            .status-row {{ display: flex; align-items: center; gap: 0.6rem; font-size: 0.9rem; font-weight: 500; margin-bottom: 0.5rem; }}
            .dot {{ width: 10px; height: 10px; border-radius: 50%; flex: 0 0 auto; }}
            .dot.online {{ background: var(--success); box-shadow: 0 0 10px var(--success); animation: pulseGlow 2.2s ease-in-out infinite; }}
            .dot.offline {{ background: var(--danger); box-shadow: 0 0 10px var(--danger); }}
            .meta-text {{ color: var(--text-muted); font-size: 0.85rem; margin: 0; }}
            .feed-box {{ display: flex; flex-direction: column; gap: 0.75rem; }} .feed-item label {{ font-size: 0.8rem; color: var(--text-muted); font-weight: 600; text-transform: uppercase; display: block; margin-bottom: 0.25rem; }}
            .feed-item input {{ font-family: monospace; font-size: 0.9rem; cursor: pointer; background: var(--surface-2); }}
            table {{ width: 100%; border-collapse: collapse; }} .hint-text {{ font-size: 0.8rem; color: var(--text-muted); margin-top: 0.35rem; line-height: 1.4; }}
             .catalog-filter {{ margin: 1rem 0 1.25rem; }}
             .catalog-list {{ display: flex; flex-direction: column; gap: 0.75rem; max-height: 48rem; overflow-y: auto; padding-right: 0.25rem; }}
             .catalog-list::-webkit-scrollbar, .log-viewer::-webkit-scrollbar {{ width: 8px; }}
             .catalog-list::-webkit-scrollbar-track, .log-viewer::-webkit-scrollbar-track {{ background: transparent; }}
             .catalog-list::-webkit-scrollbar-thumb, .log-viewer::-webkit-scrollbar-thumb {{ background: var(--border-strong); border-radius: 8px; }}
             .catalog-group {{ border: 1px solid var(--border-strong); border-radius: 10px; background: var(--surface-1); transition: border-color 0.2s var(--ease); }}
             .catalog-group summary {{ display: flex; justify-content: space-between; align-items: center; cursor: pointer; padding: 0.85rem 1rem; font-weight: 700; list-style: none; }}
             .catalog-group summary::-webkit-details-marker {{ display: none; }}
             .catalog-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 0.5rem; padding: 0 0.75rem 0.75rem; }}
             .catalog-item {{ margin: 0; }}
             .catalog-toggle {{ display: flex; align-items: center; gap: 0.6rem; min-height: 2.6rem; padding: 0.45rem 0.6rem; border: 1px solid var(--border); border-radius: 8px; cursor: pointer; background: var(--surface-2); transition: border-color 0.2s var(--ease), background-color 0.2s var(--ease), transform 0.15s var(--ease); }}
             .catalog-toggle:hover {{ border-color: var(--accent); background: var(--accent-surface); transform: translateY(-1px); }}
             .catalog-toggle input {{ width: 1rem; height: 1rem; accent-color: var(--accent); flex: 0 0 auto; }}
             .catalog-name {{ font-size: 0.88rem; line-height: 1.2; flex: 1; }}
              .catalog-live-badge {{ color: var(--live-text); border-color: var(--live-border); background: var(--live-bg); font-size: 0.68rem; white-space: nowrap; }}
              .catalog-actions {{ display: flex; align-items: center; gap: 0.75rem; margin-top: 1rem; flex-wrap: wrap; }}
              .catalog-actions button {{ width: auto; min-width: 13rem; }}
              .catalog-selection-count {{ color: var(--text-muted); font-size: 0.85rem; }}
             @media (max-width: 640px) {{ .form-grid {{ grid-template-columns: 1fr; }} .catalog-grid {{ grid-template-columns: 1fr; }} .card {{ padding: 1.25rem; }} .tabs-nav {{ margin: 0 -1rem 1.25rem; padding: 0.75rem 1rem; }} }}
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <div class="header-controls">
                    <button type="button" class="theme-toggle" id="theme-toggle" onclick="toggleTheme()" title="Toggle Dark/Light Mode">🌙</button>
                </div>
                <h1>Jellyball Sports Manager</h1>
                <p>Multi-Aggregator Scraper & Jellyfin Live TV Gateway</p>
                <div class="status-badges">{auth_badge} {jellyfin_badge} {webhook_discord_badge} {webhook_telegram_badge}</div>
            </header>
            <div class="tabs-nav">
                <button type="button" class="tab-btn {'active' if tab == 'channels' else ''}" id="btn-channels" onclick="switchTab('channels')">📺 Channels & Streams</button>
                <button type="button" class="tab-btn {'active' if tab == 'metrics' else ''}" id="btn-metrics" onclick="switchTab('metrics')">📊 Stability Metrics</button>
                <button type="button" class="tab-btn {'active' if tab == 'performance' else ''}" id="btn-performance" onclick="switchTab('performance')">⚡ Performance</button>
                <button type="button" class="tab-btn {'active' if tab == 'playback' else ''}" id="btn-playback" onclick="switchTab('playback')">⚙️ Playback Settings</button>
                <button type="button" class="tab-btn {'active' if tab == 'alerts' else ''}" id="btn-alerts" onclick="switchTab('alerts')">🔔 Alerts & Integrations</button>
                <button type="button" class="tab-btn {'active' if tab == 'logs' else ''}" id="btn-logs" onclick="switchTab('logs')">📜 Logs</button>
            </div>
            <div id="tab-channels" class="tab-pane {'active' if tab == 'channels' else ''}">
                <div class="card" style="border-color: var(--accent); background: linear-gradient(180deg, var(--card-bg) 0%, var(--surface-1) 100%);">
                    <h3 style="color: var(--accent-text); margin-bottom: 0.5rem;">Jellyfin Integration Endpoints</h3>
                    <div class="feed-box">
                        <div class="feed-item"><label>M3U Tuner Playlist URL (Click to copy)</label><input type="text" readonly value="{base_url_html}/playlist.m3u" onclick="copyToClipboard(this.value, 'Playlist URL copied!')"></div>
                        <div class="feed-item"><label>XMLTV EPG Guide URL (Click to copy)</label><input type="text" readonly value="{base_url_html}/epg.xml" onclick="copyToClipboard(this.value, 'EPG URL copied!')"></div>
                    </div>
                </div>
                <div class="card catalog-card">
                    <h3>Sports Catalog</h3>
                    <p class="hint-text">Turn on exact teams or always-live sports channels. Disabled entries are not scraped and do not appear in the Jellyfin playlist.</p>
                    <p class="hint-text">{_html(catalog_source_text)} Available entries: {_html(len(catalog_entries))}. Select as many entries as needed, then apply them together.</p>
                    <form action="/catalog/apply" method="post" id="catalog-form">
                        <input id="catalog-filter" class="catalog-filter" type="text" placeholder="Filter teams and channels..." oninput="filterCatalog(this.value)">
                        <div class="catalog-list">{catalog_html}</div>
                        <div class="catalog-actions">
                            <button type="submit">Apply Selected Teams</button>
                            <span class="catalog-selection-count" id="catalog-selection-count">Selected: {_html(len(active_catalog_keys))}</span>
                        </div>
                    </form>
                </div>
                <div class="card" style="background: var(--accent-card-bg); border-color: var(--accent);">
                    <div style="display: flex; justify-content: space-between; align-items: center; gap: 1rem;">
                        <div>
                            <h3 style="margin-top: 0; margin-bottom: 0.25rem;">🔄 Bulk Provider Override</h3>
                            <p class="hint-text" style="margin: 0;">Temporarily force all active streams to use a specific provider.</p>
                        </div>
                        <select id="global-provider-override" style="padding: 0.65rem; border-radius: 6px; background: var(--surface-2); color: #fff; border: 1px solid var(--border-strong); font-size: 0.9rem; min-width: 180px; cursor: pointer;">
                            <option value="">No Override (Auto)</option>
                            <option value="iSportSurge">iSportSurge</option>
                            <option value="MyBuffStreams">MyBuffStreams</option>
                            <option value="MethStreams">MethStreams</option>
                            <option value="StreamEast">StreamEast</option>
                            <option value="Footybite">Footybite</option>
                            <option value="1Stream">1Stream</option>
                            <option value="Streamed">Streamed</option>
                            <option value="TopStreams">TopStreams</option>
                            <option value="TheTVApp">TheTVApp</option>
                            <option value="DaddyLive">DaddyLive</option>
                        </select>
                    </div>
                </div>
                <div class="team-grid">{channels_html}</div>
            </div>
            <div id="tab-metrics" class="tab-pane {'active' if tab == 'metrics' else ''}">
                <div class="card">
                    <h3>Provider Health Leaderboard</h3>
                    <p class="hint-text">Success rates based on failover history. Higher is better.</p>
                    <div style="background: var(--surface-2); padding: 1rem; border-radius: 8px; border: 1px solid var(--border);">{provider_health_html}</div>
                </div>
                <div class="card">
                    <h3>Stream Stability & Provider Metrics</h3>
                    <div style="margin-bottom: 1.25rem;"><div>{failover_stats_html}</div></div>
                    <div style="overflow-x: auto;">
                        <table>
                            <thead><tr style="border-bottom: 1px solid var(--border-strong); text-align: left; font-size: 0.8rem; color: var(--text-muted); text-transform: uppercase;"><th style="padding: 0.5rem;">Time (UTC)</th><th style="padding: 0.5rem;">Team</th><th style="padding: 0.5rem;">Event</th><th style="padding: 0.5rem;">Source</th><th style="padding: 0.5rem;">Details</th></tr></thead>
                            <tbody>{events_html}</tbody>
                        </table>
                    </div>
                </div>
            </div>
            <div id="tab-performance" class="tab-pane {'active' if tab == 'performance' else ''}">
                <div class="card">
                    <h3>⚡ System Performance</h3>
                    <p class="hint-text">Real-time performance metrics and diagnostics.</p>
                    <div id="performance-metrics" style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem;">
                        <div style="background: var(--surface-2); padding: 1rem; border-radius: 8px; border: 1px solid var(--border);">
                            <div style="color: var(--text-muted); font-size: 0.85rem; margin-bottom: 0.5rem;">Cache Hit Rate</div>
                            <div style="font-size: 1.5rem; font-weight: 700; color: var(--success);" id="cache-hit-rate">--</div>
                        </div>
                        <div style="background: var(--surface-2); padding: 1rem; border-radius: 8px; border: 1px solid var(--border);">
                            <div style="color: var(--text-muted); font-size: 0.85rem; margin-bottom: 0.5rem;">Playback Sessions (1h)</div>
                            <div style="font-size: 1.5rem; font-weight: 700; color: var(--accent);" id="playback-sessions">--</div>
                        </div>
                        <div style="background: var(--surface-2); padding: 1rem; border-radius: 8px; border: 1px solid var(--border);">
                            <div style="color: var(--text-muted); font-size: 0.85rem; margin-bottom: 0.5rem;">Failovers (1h)</div>
                            <div style="font-size: 1.5rem; font-weight: 700; color: var(--danger);" id="failover-count">--</div>
                        </div>
                        <div style="background: var(--surface-2); padding: 1rem; border-radius: 8px; border: 1px solid var(--border);">
                            <div style="color: var(--text-muted); font-size: 0.85rem; margin-bottom: 0.5rem;">Database Size</div>
                            <div style="font-size: 1.5rem; font-weight: 700; color: var(--warning);" id="db-size">--</div>
                        </div>
                    </div>
                </div>
                <div class="card">
                    <h3>📊 Top Watched Teams (Last 7 Days)</h3>
                    <div id="top-teams-list" style="display: flex; flex-direction: column; gap: 0.75rem;">
                        <p style="color: var(--text-muted);">Loading...</p>
                    </div>
                </div>
                <div class="card">
                    <h3>⚠️ Rate Limiting Protection</h3>
                    <p class="hint-text">Success rate of each provider's search attempts in the last hour. A provider stuck below 50% may be throttling or banning us.</p>
                    <div id="rate-limiting-status" style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 1rem;">
                        <p style="color: var(--text-muted);">Loading...</p>
                    </div>
                </div>
            </div>
            <div id="tab-logs" class="tab-pane {'active' if tab == 'logs' else ''}">
                <div class="card">
                    <div style="display:flex; justify-content:space-between; align-items:center; gap:1rem; margin-bottom:1rem;">
                        <div><h3 style="margin-bottom:0.35rem;">Recent Application Logs</h3><p class="hint-text" style="margin:0;">The most recent 500 Jellyball log entries.</p></div>
                        <button type="button" class="btn-secondary" style="width:auto; white-space:nowrap;" onclick="loadLogs()">Refresh Logs</button>
                    </div>
                    <pre id="log-viewer" class="log-viewer">Loading logs...</pre>
                </div>
            </div>
            <div id="tab-playback" class="tab-pane {'active' if tab == 'playback' else ''}">
                <div class="card">
                    <h3>Playback & Stream Settings</h3>
                    <form action="/settings/playback" method="post" style="display: flex; flex-direction: column; gap: 1.5rem;">
                        <div>
                            <label style="font-size: 0.9rem; color: var(--text-soft); font-weight: 600; display: block; margin-bottom: 0.5rem;">Startup Buffer Duration: <span id="startup-value">{playback_cfg['startup_buffer_seconds']}</span>s</label>
                            <input type="range" name="startup_buffer" min="5" max="30" value="{playback_cfg['startup_buffer_seconds']}" style="width: 100%; cursor: pointer;" oninput="document.getElementById('startup-value').textContent = this.value">
                            <p class="hint-text">Higher values reduce buffering but increase startup delay. Range: 5-30 seconds.</p>
                        </div>
                        <div>
                            <label style="font-size: 0.9rem; color: var(--text-soft); font-weight: 600; display: block; margin-bottom: 0.5rem;">Prefetch Chunk Count: <span id="prefetch-value">{playback_cfg['prefetch_chunk_count']}</span></label>
                            <input type="range" name="prefetch_count" min="1" max="10" value="{playback_cfg['prefetch_chunk_count']}" style="width: 100%; cursor: pointer;" oninput="document.getElementById('prefetch-value').textContent = this.value">
                            <p class="hint-text">Number of upcoming chunks to download ahead. Higher values reduce buffering at cost of bandwidth. Range: 1-10.</p>
                        </div>
                        <div>
                            <label style="font-size: 0.9rem; color: var(--text-soft); font-weight: 600; display: block; margin-bottom: 0.5rem;">Chunk Cache TTL: <span id="cache-value">{playback_cfg['stream_chunk_cache_ttl']}</span>s</label>
                            <input type="range" name="cache_ttl" min="5" max="60" value="{playback_cfg['stream_chunk_cache_ttl']}" style="width: 100%; cursor: pointer;" oninput="document.getElementById('cache-value').textContent = this.value">
                            <p class="hint-text">How long cached chunks are kept in memory. Range: 5-60 seconds.</p>
                        </div>
                        <button type="submit" style="width: auto; align-self: flex-start;">💾 Save Playback Settings</button>
                    </form>
                </div>
                <div class="card">
                    <h3>Provider Rotation</h3>
                    <p class="hint-text">When enabled, the preferred provider for tie-broken stream matches rotates hourly instead of always favoring the same aggregator, spreading load across sources.</p>
                    <form action="/settings/provider-rotation" method="post" style="display: flex; align-items: center; gap: 0.75rem;">
                        <input type="checkbox" name="enabled" value="true" id="rotation-toggle" {'checked' if provider_rotation_enabled else ''} style="width: auto; cursor: pointer;">
                        <label for="rotation-toggle" style="cursor: pointer;">Enable provider rotation mode</label>
                        <button type="submit" style="width: auto; margin-left: auto;">💾 Save</button>
                    </form>
                </div>
                <div class="card">
                    <h3>Import/Export Team Configuration</h3>
                    <p class="hint-text">Backup or transfer your team selections to another Jellyball instance.</p>
                    <div style="display: flex; gap: 1rem; flex-wrap: wrap;">
                        <a href="/api/export-config" download="jellyball-config.json" style="display: inline-block; padding: 0.85rem 1.5rem; background: var(--accent); color: white; border-radius: 8px; text-decoration: none; font-weight: 600; cursor: pointer;">📥 Export Configuration</a>
                        <button type="button" onclick="document.getElementById('import-file').click()" style="width: auto; padding: 0.85rem 1.5rem;">📤 Import Configuration</button>
                        <input type="file" id="import-file" accept=".json" style="display: none;" onchange="importConfig(this.files[0])">
                    </div>
                </div>
            </div>
            <div id="tab-alerts" class="tab-pane {'active' if tab == 'alerts' else ''}">
                <div class="card" style="border-color: var(--purple); background: linear-gradient(180deg, var(--card-bg) 0%, var(--purple-card-to) 100%);">
                    <h3 style="color: var(--purple-text);">🍇 Jellyfin Automatic Guide Refresh API</h3>
                    <form action="/settings/jellyfin" method="post" style="display: flex; flex-direction: column; gap: 1.25rem;">
                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;">
                            <div><label style="font-size: 0.85rem; color: var(--text-soft); font-weight: 600; display: block; margin-bottom: 0.35rem;">🌐 Jellyfin Server URL</label><input type="text" name="jellyfin_url" value="{_html(jellyfin_cfg['jellyfin_url'])}"></div>
                            <div><label style="font-size: 0.85rem; color: var(--text-soft); font-weight: 600; display: block; margin-bottom: 0.35rem;">🔑 Jellyfin API Key</label><input type="text" name="jellyfin_api_key" value="{_html(jellyfin_cfg['jellyfin_api_key'])}"></div>
                        </div>
                        <div><label style="font-size: 0.85rem; color: var(--text-soft); font-weight: 600; display: block; margin-bottom: 0.35rem;">⚙️ Refresh Guide Scheduled Task ID</label><input type="text" name="jellyfin_task_id" value="{_html(jellyfin_cfg['jellyfin_task_id'])}"></div>
                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-top: 0.25rem;"><button type="submit" style="background: var(--purple-dark);">💾 Save Jellyfin API Settings</button></div>
                    </form>
                    <form action="/settings/test_jellyfin" method="post" style="margin-top: 1rem; display: flex; gap: 0.75rem; align-items: center;">
                        <button type="submit" class="btn-secondary" style="border-color: var(--purple); color: var(--purple-text); flex: 1;">🍇 Test Jellyfin Connection</button>
                        <span id="jellyfin-status" style="font-size: 0.9rem; min-width: 120px;"></span>
                    </form>
                    <p class="hint-text">Tests the API connection and shows whether the refresh task will work correctly.</p>
                </div>
                <div class="card">
                    <h3>Webhook Notification Settings</h3>
                    <form action="/settings/notifications" method="post" style="display: flex; flex-direction: column; gap: 1.25rem;">
                        <div><label style="font-size: 0.85rem; color: var(--text-soft); font-weight: 600; display: block; margin-bottom: 0.35rem;">👾 Discord Incoming Webhook URL</label><input type="text" name="discord_webhook_url" value="{_html(notif_cfg['discord_webhook_url'])}"></div>
                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;">
                            <div><label style="font-size: 0.85rem; color: var(--text-soft); font-weight: 600; display: block; margin-bottom: 0.35rem;">✈️ Telegram Bot Token</label><input type="text" name="telegram_bot_token" value="{_html(notif_cfg['telegram_bot_token'])}"></div>
                            <div><label style="font-size: 0.85rem; color: var(--text-soft); font-weight: 600; display: block; margin-bottom: 0.35rem;">💬 Telegram Chat ID</label><input type="text" name="telegram_chat_id" value="{_html(notif_cfg['telegram_chat_id'])}"></div>
                        </div>
                        <button type="submit" style="margin-top: 0.5rem;">💾 Save Notification Settings</button>
                    </form>
                </div>
            </div>
        </div>
        <script>
            function showToast(message, duration = 3000, isError = false) {{
                const toast = document.createElement('div');
                toast.className = 'toast' + (isError ? ' error' : '');
                toast.textContent = message;
                document.body.appendChild(toast);
                setTimeout(() => {{
                    toast.style.animation = 'slideOut 0.3s ease-out forwards';
                    setTimeout(() => toast.remove(), 300);
                }}, duration);
            }}
            function copyToClipboard(text, message = 'Copied!') {{
                navigator.clipboard.writeText(text).then(() => {{
                    showToast(message);
                }}).catch(() => {{
                    showToast('Failed to copy', 3000, true);
                }});
            }}
            function toggleTheme() {{
                const html = document.documentElement;
                const isDark = html.getAttribute('data-theme') !== 'light';
                const newTheme = isDark ? 'light' : 'dark';
                html.setAttribute('data-theme', newTheme);
                localStorage.setItem('theme', newTheme);
                const btn = document.getElementById('theme-toggle');
                btn.textContent = isDark ? '☀️' : '🌙';
                btn.classList.remove('spin');
                void btn.offsetWidth;
                btn.classList.add('spin');
            }}
            function switchTab(tabName) {{
                document.querySelectorAll('.tab-pane').forEach(el => el.classList.remove('active'));
                document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
                const pane = document.getElementById('tab-' + tabName);
                const btn = document.getElementById('btn-' + tabName);
                if (pane) pane.classList.add('active');
                if (btn) btn.classList.add('active');
                if (tabName === 'logs') loadLogs();
                const url = new URL(window.location);
                url.searchParams.set('tab', tabName);
                window.history.replaceState({{}}, '', url);
            }}
                async function loadLogs() {{
                    const viewer = document.getElementById('log-viewer');
                    if (!viewer) return;
                    viewer.textContent = 'Loading logs...';
                    try {{
                        const response = await fetch('/api/logs?limit=500', {{ cache: 'no-store' }});
                        if (!response.ok) throw new Error('Unable to load logs');
                        const payload = await response.json();
                        viewer.textContent = (payload.logs || []).join('\\n') || 'No log entries available.';
                        viewer.scrollTop = viewer.scrollHeight;
                    }} catch (error) {{
                        viewer.textContent = 'Unable to load logs. Check the application log file directly.';
                    }}
                }}
             function filterCatalog(value) {{
                 const query = (value || '').toLowerCase().trim();
                 document.querySelectorAll('.catalog-group').forEach(group => {{
                     let visible = 0;
                     group.querySelectorAll('.catalog-item').forEach(item => {{
                         const matches = !query || (item.dataset.catalogName || '').toLowerCase().includes(query);
                         item.style.display = matches ? '' : 'none';
                         if (matches) visible += 1;
                     }});
                     group.style.display = visible ? '' : 'none';
                     if (query && visible) group.open = true;
                 }});
             }}
              function updateCatalogSelectionCount() {{
                  const selected = document.querySelectorAll('#catalog-form input[name="catalog_keys"]:checked').length;
                  const label = document.getElementById('catalog-selection-count');
                  if (label) label.textContent = 'Selected: ' + selected;
              }}
               function applyStatusSnapshot(snapshot) {{
                   // Structural changes (a channel was added/removed, e.g. from another
                   // tab or a scheduled auto-disable) still need a reload since we don't
                   // have the HTML to insert a brand-new card client-side. Everything
                   // else — candidate count, active provider, health — is common during
                   // normal failover/rescrape churn and is patched into the existing
                   // card in place instead, so the page doesn't jump/reload every ~5s.
                   const channels = snapshot.channels || [];
                   const byId = new Map(channels.map(channel => [channel.team_id, channel]));
                   let shouldReload = channels.length !== document.querySelectorAll('.channel-card').length;
                   document.querySelectorAll('.channel-card').forEach(card => {{
                       const channel = byId.get(card.dataset.teamId);
                       if (!channel) {{
                           shouldReload = true;
                           return;
                       }}
                       const candidateCount = Number(channel.candidate_count || 0);
                       const activeProvider = channel.active_provider || '';
                       card.dataset.candidateCount = String(candidateCount);
                       card.dataset.activeProvider = activeProvider;
                       const dot = card.querySelector('[data-role="status-dot"]');
                       const statusText = card.querySelector('[data-role="status-text"]');
                       const candidateText = card.querySelector('[data-role="candidate-count"]');
                       if (dot) {{
                           dot.classList.toggle('online', Boolean(channel.healthy));
                           dot.classList.toggle('offline', !channel.healthy);
                       }}
                       if (statusText) {{
                           if (channel.healthy) {{
                               statusText.textContent = 'Stream Stable & Active';
                           }} else if (channel.schedule_status === 'off_season') {{
                               statusText.textContent = channel.season_resume_label
                                   ? `Off-season (resumes ${{channel.season_resume_label}})`
                                   : 'Off-season';
                           }} else {{
                               statusText.textContent = 'Searching / Re-evaluating';
                           }}
                       }}
                       if (candidateText) candidateText.textContent = 'Available Backups: ' + candidateCount;
                   }});
                   if (shouldReload) window.location.reload();
               }}
               async function pollChannelStatus() {{
                   try {{
                       const response = await fetch('/api/status', {{ cache: 'no-store' }});
                       if (response.ok) applyStatusSnapshot(await response.json());
                   }} catch (error) {{
                       // The next poll retries transient server or network failures.
                   }} finally {{
                       window.setTimeout(pollChannelStatus, 5000);
                   }}
               }}
               function applyMultiviewStatusSnapshot(snapshot) {{
                   const byId = new Map((snapshot.channels || []).map(ch => [ch.channel_id, ch]));
                   document.querySelectorAll('.multiview-card').forEach(card => {{
                       const ch = byId.get(card.dataset.teamId);
                       if (!ch) return;
                       const statusEl = card.querySelector('[data-role="mv-status"]');
                       const failureEl = card.querySelector('[data-role="mv-failure"]');
                       if (statusEl) statusEl.textContent = ch.running ? '🟢 Running' : '⚪ Stopped';
                       if (failureEl) {{
                           if (!ch.running && ch.last_error) {{
                               failureEl.innerHTML = `<p class="meta-text" style="color:var(--danger-text);">⚠️ ${{ch.last_error}} (failed ${{ch.failure_count}}x, retrying in ${{ch.retry_in_seconds}}s)</p>`;
                           }} else {{
                               failureEl.innerHTML = '';
                           }}
                       }}
                   }});
               }}
               async function pollMultiviewStatus() {{
                   if (!document.querySelector('.multiview-card')) {{
                       window.setTimeout(pollMultiviewStatus, 5000);
                       return;
                   }}
                   try {{
                       const response = await fetch('/api/ffmpeg-status', {{ cache: 'no-store' }});
                       if (response.ok) applyMultiviewStatusSnapshot(await response.json());
                   }} catch (error) {{
                       // The next poll retries transient server or network failures.
                   }} finally {{
                       window.setTimeout(pollMultiviewStatus, 5000);
                   }}
               }}
            function updateBulkTeamIds() {{
                const checked = document.querySelectorAll('.team-bulk-select:checked');
                const ids = Array.from(checked).map(c => c.dataset.teamId).join(',');
                document.getElementById('bulk-team-ids').value = ids;
                return ids;
            }}
            function updateMultiviewTeamIds() {{
                const checked = document.querySelectorAll('.multiview-member-select:checked');
                if (checked.length !== 2 && checked.length !== 4) {{
                    showToast('Select exactly 2 or 4 channels for a Multi-View', 3000, true);
                    return false;
                }}
                document.getElementById('multiview-team-ids').value = Array.from(checked).map(c => c.dataset.teamId).join(',');
                return true;
            }}
            function selectAllTeams() {{
                document.querySelectorAll('.team-bulk-select').forEach(c => c.checked = true);
                updateBulkTeamIds();
                showToast(`Selected ${{document.querySelectorAll('.team-bulk-select:checked').length}} teams`);
            }}
            function deselectAllTeams() {{
                document.querySelectorAll('.team-bulk-select').forEach(c => c.checked = false);
                updateBulkTeamIds();
                showToast('Deselected all teams');
            }}
            function bulkFavorite() {{
                const ids = updateBulkTeamIds();
                if (!ids) {{
                    showToast('Select teams first', 3000, true);
                    return;
                }}
                const form = document.createElement('form');
                form.method = 'POST';
                form.action = '/bulk-favorite';
                form.innerHTML = `<input type="hidden" name="team_ids" value="${{ids}}">`;
                document.body.appendChild(form);
                form.submit();
            }}
            function bulkUnfavorite() {{
                const ids = updateBulkTeamIds();
                if (!ids) {{
                    showToast('Select teams first', 3000, true);
                    return;
                }}
                const form = document.createElement('form');
                form.method = 'POST';
                form.action = '/bulk-unfavorite';
                form.innerHTML = `<input type="hidden" name="team_ids" value="${{ids}}">`;
                document.body.appendChild(form);
                form.submit();
            }}
            function bulkRemove() {{
                const ids = updateBulkTeamIds();
                if (!ids) {{
                    showToast('Select teams first', 3000, true);
                    return;
                }}
                if (!confirm(`Remove ${{document.querySelectorAll('.team-bulk-select:checked').length}} teams?`)) return;
                const form = document.createElement('form');
                form.method = 'POST';
                form.action = '/bulk-remove';
                form.innerHTML = `<input type="hidden" name="team_ids" value="${{ids}}">`;
                document.body.appendChild(form);
                form.submit();
            }}
            async function testTeamStream(teamId) {{
                const card = document.querySelector(`.channel-card[data-team-id="${{teamId}}"]`);
                const resultEl = card ? card.querySelector('[data-role="test-result"]') : null;
                if (resultEl) {{
                    resultEl.textContent = 'Testing...';
                    resultEl.style.color = 'var(--text-muted)';
                }}
                try {{
                    const resp = await fetch(`/api/test-stream/${{teamId}}`, {{ method: 'POST' }});
                    const result = await resp.json();
                    if (resultEl) {{
                        const count = result.candidate_count || 0;
                        resultEl.textContent = `${{result.status}} (${{count}} candidate${{count === 1 ? '' : 's'}})`;
                        resultEl.style.color = result.is_live ? '#22c55e' : 'var(--danger-text)';
                    }}
                }} catch (err) {{
                    if (resultEl) {{
                        resultEl.textContent = '❌ Test failed';
                        resultEl.style.color = 'var(--danger-text)';
                    }}
                }}
            }}
            function importConfig(file) {{
                if (!file) return;
                const reader = new FileReader();
                reader.onload = async (e) => {{
                    try {{
                        const config = JSON.parse(e.target.result);
                        const resp = await fetch('/api/import-config', {{
                            method: 'POST',
                            headers: {{'Content-Type': 'application/json'}},
                            body: JSON.stringify(config)
                        }});
                        if (resp.ok) {{
                            const result = await resp.json();
                            showToast(`✅ Imported ${{result.count}} teams`);
                            setTimeout(() => window.location.reload(), 2000);
                        }} else {{
                            showToast('Import failed', 3000, true);
                        }}
                    }} catch (error) {{
                        showToast('Invalid config file', 3000, true);
                    }}
                }};
                reader.readAsText(file);
            }}

            window.addEventListener('DOMContentLoaded', () => {{
                const params = new URLSearchParams(window.location.search);
                const activeTab = params.get('tab');
                const status = params.get('status');

                if (activeTab && document.getElementById('tab-' + activeTab)) switchTab(activeTab);
                if (activeTab === 'logs') loadLogs();
                updateCatalogSelectionCount();
                pollChannelStatus();
                pollMultiviewStatus();

                const statusMessages = {{
                    'jellyfin_success': '✅ Jellyfin connection successful!',
                    'jellyfin_failed': '❌ Jellyfin connection failed',
                    'saved': '✅ Settings saved',
                    'test_sent': '✅ Test alert sent'
                }};
                if (status && statusMessages[status]) {{
                    showToast(statusMessages[status], 4000, status.includes('failed'));
                    const statusEl = document.getElementById('jellyfin-status');
                    if (statusEl && status.includes('jellyfin')) {{
                        statusEl.textContent = statusMessages[status];
                        statusEl.style.color = status.includes('success') ? 'var(--success)' : 'var(--danger)';
                    }}
                    const newUrl = new URL(window.location);
                    newUrl.searchParams.delete('status');
                    window.history.replaceState({{}}, '', newUrl);
                }}

                const currentTheme = document.documentElement.getAttribute('data-theme') || 'dark';
                document.getElementById('theme-toggle').textContent = currentTheme === 'light' ? '☀️' : '🌙';

                const overrideSelect = document.getElementById('global-provider-override');
                if (overrideSelect) {{
                    overrideSelect.addEventListener('change', (e) => {{
                        sessionStorage.setItem('provider-override', e.target.value);
                        showToast(e.target.value ? `Provider override set to: ${{e.target.value}}` : 'Provider override cleared');
                        if (window.Jellyfin) {{
                            window.Jellyfin.mediaManager?.seekTo?.(0);
                        }}
                    }});
                    const saved = sessionStorage.getItem('provider-override');
                    if (saved) overrideSelect.value = saved;
                }}

                if (typeof MediaSession !== 'undefined') {{
                    navigator.mediaSession.setActionHandler('play', () => {{}});
                    navigator.mediaSession.setActionHandler('pause', () => {{}});
                }}

                document.querySelectorAll('.team-bulk-select').forEach(checkbox => {{
                    checkbox.addEventListener('change', updateBulkTeamIds);
                }});

                statusMessages['playback_saved'] = '✅ Playback settings saved';

                if (activeTab === 'performance') {{
                    async function refreshPerformanceTab(withTopTeams) {{
                        try {{
                            const cache = await fetch('/api/cache-metrics').then(r => r.json());
                            const perf = await fetch('/api/performance-stats').then(r => r.json());

                            const cacheEl = document.getElementById('cache-hit-rate');
                            const playEl = document.getElementById('playback-sessions');
                            const failEl = document.getElementById('failover-count');
                            const dbEl = document.getElementById('db-size');
                            const rateLimitEl = document.getElementById('rate-limiting-status');

                            if (cacheEl) cacheEl.textContent = (cache.hit_rate || 0).toFixed(1) + '%';
                            if (playEl) playEl.textContent = perf.playback_sessions_hour || 0;
                            if (failEl) failEl.textContent = perf.failovers_hour || 0;
                            if (dbEl) dbEl.textContent = (perf.db_size_mb || 0).toFixed(1) + ' MB';

                            if (rateLimitEl) {{
                                const health = perf.provider_health || [];
                                if (!health.length) {{
                                    rateLimitEl.innerHTML = '<div style="background: var(--surface-2); padding: 0.75rem; border-radius: 6px; border: 1px solid var(--border); color: var(--text-muted);">Not enough recent provider activity yet.</div>';
                                }} else {{
                                    rateLimitEl.innerHTML = health.map(p => {{
                                        if (p.sustained_failure) {{
                                            return `<div style="background: var(--surface-2); padding: 0.75rem; border-radius: 6px; border: 1px solid var(--danger);"><div style="font-weight:600;">${{p.provider}}</div><div style="color: var(--danger);">⛔ Likely dead — 0% over ${{p.samples_5d}} attempts/5d</div></div>`;
                                        }}
                                        const color = p.at_risk ? 'var(--danger)' : 'var(--success)';
                                        const icon = p.at_risk ? '⚠️' : '✅';
                                        const rateText = p.success_rate === null ? 'no data this hour' : `${{p.success_rate}}% (${{p.samples_hour}} samples/hr)`;
                                        return `<div style="background: var(--surface-2); padding: 0.75rem; border-radius: 6px; border: 1px solid var(--border);"><div style="font-weight:600;">${{p.provider}}</div><div style="color:${{color}};">${{icon}} ${{rateText}}</div></div>`;
                                    }}).join('');
                                }}
                            }}

                            if (withTopTeams) {{
                                const playback = await fetch('/api/playback-stats').then(r => r.json());
                                const topEl = document.getElementById('top-teams-list');
                                if (topEl && playback.top_watched_teams) {{
                                    topEl.innerHTML = playback.top_watched_teams.map(t => `<div style="padding: 0.5rem; background: var(--surface-2); border-radius: 6px; display: flex; justify-content: space-between;"><span>${{t.team_id}}</span><span style="color: var(--success);">${{t.plays}} plays</span></div>`).join('');
                                }}
                            }}
                        }} catch (e) {{
                            console.error('Performance load failed', e);
                        }}
                    }}
                    refreshPerformanceTab(true);
                    setInterval(() => refreshPerformanceTab(false), 5000);
                }}
            }});
        </script>
    </body>
    </html>
    """
    return html

@app.post("/settings/notifications")
async def update_notifications(discord_webhook_url: str = Form(""), telegram_bot_token: str = Form(""), telegram_chat_id: str = Form(""), auth: bool = Depends(verify_dashboard_auth)):
    await set_setting_async("discord_webhook_url", discord_webhook_url.strip())
    await set_setting_async("telegram_bot_token", telegram_bot_token.strip())
    await set_setting_async("telegram_chat_id", telegram_chat_id.strip())
    return RedirectResponse(url="/?tab=alerts&status=saved", status_code=303)

@app.post("/settings/jellyfin")
async def update_jellyfin_settings(jellyfin_url: str = Form("http://localhost:8096"), jellyfin_api_key: str = Form(""), jellyfin_task_id: str = Form(""), auth: bool = Depends(verify_dashboard_auth)):
    jellyfin_url = jellyfin_url.strip().rstrip("/")
    if not _validate_upstream_url(jellyfin_url, allow_private=True):
        raise HTTPException(status_code=400, detail="Jellyfin URL must be an absolute HTTP(S) URL")
    await set_setting_async("jellyfin_url", jellyfin_url.strip())
    await set_setting_async("jellyfin_api_key", jellyfin_api_key.strip())
    await set_setting_async("jellyfin_task_id", jellyfin_task_id.strip())
    return RedirectResponse(url="/?tab=alerts&status=jellyfin_saved", status_code=303)

@app.post("/settings/test_jellyfin")
async def test_jellyfin_refresh_endpoint(auth: bool = Depends(verify_dashboard_auth)):
    success = await trigger_jellyfin_refresh()
    status_code = "jellyfin_success" if success else "jellyfin_failed"
    return RedirectResponse(url=f"/?tab=alerts&status={status_code}", status_code=303)

@app.post("/settings/test_alert")
async def test_alert(auth: bool = Depends(verify_dashboard_auth)):
    await send_alert("🧪 Jellyball Notification Test", "Proactive monitoring alerts are functioning correctly.", "info")
    return RedirectResponse(url="/?tab=alerts&status=test_sent", status_code=303)


async def _set_catalog_entry_enabled(entry: dict, enabled: bool) -> bool:
    team_id = entry["team_id"]
    if enabled:
        if team_id not in stream_state:
            await save_team_async(
                team_id,
                entry["name"],
                entry["query"],
                entry["logo_url"],
                "",
                "",
                entry["category"],
                entry["source_id"],
                entry["content_type"],
                entry["search_terms"],
                entry["always_live"],
                entry["catalog_key"],
            )
            stream_state[team_id] = {
                "name": entry["name"],
                "query": entry["query"],
                "candidates": [],
                "active_index": 0,
                "is_healthy": False,
                "logo_url": entry["logo_url"],
                "start_time": "",
                "stop_time": "",
                "category": entry["category"],
                "source_id": entry["source_id"],
                "content_type": entry["content_type"],
                "search_terms": entry["search_terms"],
                "always_live": entry["always_live"],
                "catalog_key": entry["catalog_key"],
                "tvg_id": entry.get("tvg_id", ""),
                "group_title": entry.get("group_title", ""),
            **_scrape_lifecycle_defaults(),
            }
            _start_team_scrape_loop(team_id)
            return True
    else:
        if team_id in stream_state:
            await _stop_team_scrape_loop(team_id)
            del stream_state[team_id]
            await delete_team_async(team_id)
            return True
        await delete_team_async(team_id)
    return False


def _catalog_selection_changes(
    entries: List[dict],
    selected_keys: Set[str],
    active_catalog_keys: Set[str],
) -> tuple[Dict[str, dict], Set[str], Set[str]]:
    entries_by_key = {entry["catalog_key"]: entry for entry in entries}
    unknown_keys = selected_keys - set(entries_by_key)
    if unknown_keys:
        raise ValueError("Invalid catalog selection")
    return (
        entries_by_key,
        selected_keys - active_catalog_keys,
        (active_catalog_keys & set(entries_by_key)) - selected_keys,
    )


@app.post("/catalog/apply")
async def apply_catalog(
    catalog_keys: Optional[List[str]] = Form(None),
    auth: bool = Depends(verify_dashboard_auth),
):
    entries = await get_catalog_entries()
    selected_keys = {key.strip() for key in (catalog_keys or []) if key.strip()}
    active_catalog_keys = {
        data.get("catalog_key")
        for data in stream_state.values()
        if data.get("catalog_key")
    }
    try:
        entries_by_key, keys_to_enable, keys_to_disable = _catalog_selection_changes(
            entries,
            selected_keys,
            active_catalog_keys,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    changed_count = 0
    for catalog_key in keys_to_enable:
        changed_count += int(await _set_catalog_entry_enabled(entries_by_key[catalog_key], True))
    for catalog_key in keys_to_disable:
        changed_count += int(await _set_catalog_entry_enabled(entries_by_key[catalog_key], False))

    if changed_count:
        _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after catalog apply")
    return RedirectResponse(url=f"/?tab=channels&status=catalog_applied&changed={changed_count}", status_code=303)


@app.post("/catalog/toggle")
async def toggle_catalog(
    catalog_key: str = Form(...),
    enabled: bool = Form(False),
    auth: bool = Depends(verify_dashboard_auth),
):
    entries = await get_catalog_entries()
    entry = next((item for item in entries if item["catalog_key"] == catalog_key), None)
    if not entry:
        raise HTTPException(status_code=404, detail="Catalog entry not found")

    await _set_catalog_entry_enabled(entry, enabled)
    _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after catalog toggle")
    return RedirectResponse(url="/?tab=channels", status_code=303)


@app.post("/add_team")
async def add_team(team_name: str = Form(...), search_query: str = Form(...), auth: bool = Depends(verify_dashboard_auth)):
    team_name = team_name.strip()
    search_query = search_query.strip()
    if not team_name or not search_query:
        raise HTTPException(status_code=400, detail="Team name and search query are required")
    team_id = _safe_team_id(team_name)
    search_terms = get_team_search_terms(team_name, search_query, team_id)
    await save_team_async(team_id, team_name, search_query, search_terms=search_terms, content_type="manual")
    stream_state[team_id] = {
        "name": team_name,
        "query": search_query,
        "candidates": [],
        "active_index": 0,
        "is_healthy": False,
        "logo_url": "",
        "start_time": "",
        "stop_time": "",
        "category": "custom",
        "source_id": "",
        "content_type": "manual",
        "search_terms": search_terms,
        "always_live": False,
        "catalog_key": "",
        **_scrape_lifecycle_defaults(),
    }
    _start_team_scrape_loop(team_id)
    return RedirectResponse(url="/?tab=channels", status_code=303)

@app.post("/override/{team_id}")
async def override_stream(team_id: str, candidate_index: int = Form(...), auth: bool = Depends(verify_dashboard_auth)):
    if team_id in stream_state and stream_state[team_id].get("candidates"):
        max_idx = len(stream_state[team_id]["candidates"]) - 1
        if 0 <= candidate_index <= max_idx:
            stream_state[team_id]["active_index"] = candidate_index
            stream_state[team_id]["is_healthy"] = True
            
            team_name = stream_state[team_id]["name"]
            prov = stream_state[team_id]["candidates"][candidate_index].get("provider", "Unknown")
            _spawn_background_task(
                send_alert("🛠️ Manual Override Activated", f"Stream for **{team_name}** forced to {prov}.", "info"),
                f"send manual-override alert team={team_id}",
            )
            
    return RedirectResponse(url="/?tab=channels", status_code=303)

@app.post("/remove_team/{team_id}")
async def remove_team(team_id: str, auth: bool = Depends(verify_dashboard_auth)):
    if team_id in stream_state:
        await _stop_team_scrape_loop(team_id)
        del stream_state[team_id]
        await delete_team_async(team_id)
        _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after team removal")
    return RedirectResponse(url="/?tab=channels", status_code=303)


@app.post("/multiview/create")
async def create_multiview(
    name: str = Form(...),
    layout: str = Form("grid_2x2"),
    member_team_ids: str = Form(...),
    auth: bool = Depends(verify_dashboard_auth),
):
    name = name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Name is required")
    if layout not in MULTIVIEW_LAYOUTS:
        raise HTTPException(status_code=400, detail="Invalid layout")
    members = [m.strip() for m in member_team_ids.split(",") if m.strip()]
    error = _multiview_member_validation(members)
    if error:
        raise HTTPException(status_code=400, detail=error)
    if MULTIVIEW_LAYOUTS[layout]["count"] != len(members):
        raise HTTPException(status_code=400, detail=f"Layout {layout} requires exactly {MULTIVIEW_LAYOUTS[layout]['count']} channels")

    channel_id = _safe_team_id(f"multiview_{name}")
    active_audio_team_id = members[0]
    _clear_multiview_failure(channel_id)
    await save_multiview_channel_async(channel_id, name, layout, members, active_audio_team_id)
    stream_state[channel_id] = {
        "name": name, "query": "", "type": "multiview",
        "candidates": [{"synthetic": True}],
        "active_index": 0, "is_healthy": False,
        "logo_url": "", "start_time": "", "stop_time": "",
        "category": "multiview", "source_id": "", "content_type": "multiview",
        "search_terms": [], "always_live": True, "catalog_key": "",
        "tvg_id": "", "group_title": "Multi-View",
        "layout": layout, "member_team_ids": members,
        "active_audio_team_id": active_audio_team_id,
        **_scrape_lifecycle_defaults(),
    }
    _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after multiview creation")
    return RedirectResponse(url="/?tab=channels", status_code=303)


@app.post("/multiview/{channel_id}/remove")
async def remove_multiview(channel_id: str, auth: bool = Depends(verify_dashboard_auth)):
    data = stream_state.get(channel_id)
    if data and data.get("type") == "multiview":
        await _stop_multiview_process(channel_id)
        _clear_multiview_failure(channel_id)
        _MULTIVIEW_LOCKS.pop(channel_id, None)
        del stream_state[channel_id]
        await delete_multiview_channel_async(channel_id)
        _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after multiview removal")
    return RedirectResponse(url="/?tab=channels", status_code=303)


@app.post("/multiview/{channel_id}/stop")
async def stop_multiview(channel_id: str, auth: bool = Depends(verify_dashboard_auth)):
    data = stream_state.get(channel_id)
    if data and data.get("type") == "multiview":
        await _stop_multiview_process(channel_id)
    return RedirectResponse(url="/?tab=channels", status_code=303)


@app.post("/multiview/{channel_id}/set-audio")
async def set_multiview_audio(
    channel_id: str,
    active_audio_team_id: str = Form(...),
    auth: bool = Depends(verify_dashboard_auth),
):
    data = stream_state.get(channel_id)
    if not data or data.get("type") != "multiview":
        raise HTTPException(status_code=404, detail="Multi-View channel not found")
    if active_audio_team_id not in data.get("member_team_ids", []):
        raise HTTPException(status_code=400, detail="Not a member of this Multi-View channel")
    data["active_audio_team_id"] = active_audio_team_id
    await save_multiview_channel_async(
        channel_id, data["name"], data["layout"], data["member_team_ids"], active_audio_team_id,
        data.get("tvg_id", ""), data.get("group_title", ""), data.get("logo_url", ""),
    )
    # Audio mapping is baked in at spawn time; restart so the change takes effect.
    await _stop_multiview_process(channel_id)
    _clear_multiview_failure(channel_id)  # explicit user action: retry immediately, ignore any backoff
    # Start warming up the replacement process immediately instead of leaving it to
    # the next viewer request — that request would otherwise block for up to
    # MULTIVIEW_STARTUP_TIMEOUT_SECONDS behind a synchronous spawn (observed ~20-30s
    # in practice, dominated by re-fetching each member's stream).
    _spawn_background_task(
        _spawn_multiview(channel_id, data),
        f"prewarm multiview after audio change channel={channel_id}",
    )
    return RedirectResponse(url="/?tab=channels", status_code=303)


@app.get("/api/ffmpeg-status", response_class=JSONResponse)
async def ffmpeg_status(auth: bool = Depends(verify_dashboard_auth)):
    channels = []
    for channel_id, data in stream_state.items():
        if data.get("type") != "multiview":
            continue
        entry = _MULTIVIEW_PROCESSES.get(channel_id)
        running = bool(entry and entry["process"].returncode is None and not entry.get("exited"))
        failure = _MULTIVIEW_FAILURES.get(channel_id)
        channels.append({
            "channel_id": channel_id,
            "name": data.get("name", channel_id),
            "running": running,
            "exit_code": entry.get("exit_code") if entry else None,
            "uptime_seconds": (time.monotonic() - entry["started_at"]) if entry and running else 0,
            "recent_log_lines": list(entry.get("log_lines", []))[-20:] if entry else [],
            "last_error": failure["last_error"] if failure else None,
            "failure_count": failure["count"] if failure else 0,
            "retry_in_seconds": round(_multiview_cooldown_remaining(channel_id)) if failure else 0,
        })
    return {"available": FFMPEG_AVAILABLE, "version": FFMPEG_VERSION_INFO, "channels": channels}


def _toggle_favorite_sync(team_id: str) -> None:
    with _db_session() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT is_favorite FROM teams WHERE team_id=?", (team_id,))
        row = cursor.fetchone()
        is_fav = row[0] if row else 0
        new_fav = 1 - is_fav
        conn.execute("UPDATE teams SET is_favorite=? WHERE team_id=?", (new_fav, team_id))
        conn.commit()


@app.post("/favorite/{team_id}")
async def toggle_favorite(team_id: str, auth: bool = Depends(verify_dashboard_auth)):
    try:
        await asyncio.to_thread(_toggle_favorite_sync, team_id)
    except Exception as exc:
        _log_failure(f"toggle favorite for {team_id}", exc)
    return RedirectResponse(url="/?tab=channels", status_code=303)

@app.post("/rescrape/{team_id}")
async def manual_rescrape(team_id: str, auth: bool = Depends(verify_dashboard_auth)):
    _spawn_background_task(trigger_scrape(team_id), f"manual rescrape {team_id}")
    return RedirectResponse(url="/?tab=channels", status_code=303)


def _bulk_set_favorite_sync(ids: List[str], is_favorite: bool) -> None:
    with _db_session() as conn:
        for team_id in ids:
            conn.execute("UPDATE teams SET is_favorite=? WHERE team_id=?", (1 if is_favorite else 0, team_id))
        conn.commit()


@app.post("/bulk-favorite")
async def bulk_favorite(team_ids: str = Form(""), auth: bool = Depends(verify_dashboard_auth)):
    ids = [t.strip() for t in team_ids.split(",") if t.strip()]
    try:
        await asyncio.to_thread(_bulk_set_favorite_sync, ids, True)
    except Exception as exc:
        _log_failure("bulk favorite", exc)
    return RedirectResponse(url="/?tab=channels", status_code=303)

@app.post("/bulk-unfavorite")
async def bulk_unfavorite(team_ids: str = Form(""), auth: bool = Depends(verify_dashboard_auth)):
    ids = [t.strip() for t in team_ids.split(",") if t.strip()]
    try:
        await asyncio.to_thread(_bulk_set_favorite_sync, ids, False)
    except Exception as exc:
        _log_failure("bulk unfavorite", exc)
    return RedirectResponse(url="/?tab=channels", status_code=303)

@app.post("/bulk-remove")
async def bulk_remove(team_ids: str = Form(""), auth: bool = Depends(verify_dashboard_auth)):
    ids = [t.strip() for t in team_ids.split(",") if t.strip()]
    try:
        for team_id in ids:
            if team_id in stream_state:
                await _stop_team_scrape_loop(team_id)
                del stream_state[team_id]
            await delete_team_async(team_id)
    except Exception as exc:
        _log_failure("bulk remove", exc)
    _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after bulk removal")
    return RedirectResponse(url="/?tab=channels", status_code=303)

def _export_teams_sync() -> list:
    with _db_session() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT team_id, name, query, logo_url, category, is_favorite FROM teams ORDER BY is_favorite DESC, name")
        return [{"team_id": row[0], "name": row[1], "query": row[2], "logo_url": row[3], "category": row[4], "is_favorite": bool(row[5])} for row in cursor.fetchall()]


@app.get("/api/export-config", response_class=JSONResponse)
async def export_config(auth: bool = Depends(verify_dashboard_auth)):
    try:
        teams = await asyncio.to_thread(_export_teams_sync)
        config = {
            "version": "1.0",
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "teams": teams
        }
        return JSONResponse(config)
    except Exception as exc:
        _log_failure("export config", exc)
        raise HTTPException(status_code=500, detail="Export failed")


def _import_teams_sync(teams: list) -> None:
    with _db_session() as conn:
        for team in teams:
            team_id = team.get("team_id", "")
            name = team.get("name", "")
            query = team.get("query", "")
            logo_url = team.get("logo_url", "")
            is_fav = 1 if team.get("is_favorite") else 0

            if team_id and name and query:
                conn.execute(
                    "INSERT OR REPLACE INTO teams (team_id, name, query, logo_url, category, is_favorite) VALUES (?, ?, ?, ?, ?, ?)",
                    (team_id, name, query, logo_url, "imported", is_fav)
                )
        conn.commit()


@app.post("/api/import-config")
async def import_config(file: Request, auth: bool = Depends(verify_dashboard_auth)):
    try:
        body = await file.json()
        teams = body.get("teams", [])
        await asyncio.to_thread(_import_teams_sync, teams)
        _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after import")
        return JSONResponse({"status": "imported", "count": len(teams)})
    except Exception as exc:
        _log_failure("import config", exc)
        raise HTTPException(status_code=400, detail="Import failed")

@app.post("/settings/playback")
async def update_playback_settings(
    startup_buffer: int = Form(15),
    prefetch_count: int = Form(5),
    cache_ttl: int = Form(15),
    auth: bool = Depends(verify_dashboard_auth)
):
    global STREAM_STARTUP_BUFFER_SECONDS, PREFETCH_CHUNK_COUNT, STREAM_CHUNK_CACHE_TTL

    startup_buffer = max(5, min(30, startup_buffer))
    prefetch_count = max(1, min(10, prefetch_count))
    cache_ttl = max(5, min(60, cache_ttl))

    await set_setting_async("startup_buffer_seconds", str(startup_buffer))
    await set_setting_async("prefetch_chunk_count", str(prefetch_count))
    await set_setting_async("stream_chunk_cache_ttl", str(cache_ttl))

    # Take effect immediately instead of only on the next restart — these three
    # module-level globals are what the proxy/prefetch code actually reads on the
    # hot path, so saving the sliders previously changed the DB and nothing else.
    STREAM_STARTUP_BUFFER_SECONDS = float(startup_buffer)
    PREFETCH_CHUNK_COUNT = prefetch_count
    STREAM_CHUNK_CACHE_TTL = float(cache_ttl)

    return RedirectResponse(url="/?tab=channels&status=playback_saved", status_code=303)

@app.get("/api/playback-settings", response_class=JSONResponse)
async def get_playback_settings(auth: bool = Depends(verify_dashboard_auth)):
    startup = await get_setting_async("startup_buffer_seconds", "15")
    prefetch = await get_setting_async("prefetch_chunk_count", "5")
    ttl = await get_setting_async("stream_chunk_cache_ttl", "15")

    return {
        "startup_buffer_seconds": int(startup),
        "prefetch_chunk_count": int(prefetch),
        "stream_chunk_cache_ttl": int(ttl)
    }

@app.get("/api/cache-metrics", response_class=JSONResponse)
async def get_cache_metrics(auth: bool = Depends(verify_dashboard_auth)):
    try:
        return await CHUNK_CACHE.stats()
    except Exception as exc:
        _log_failure("get cache metrics", exc)
        return {"hit_rate": 0, "total_hits": 0, "total_misses": 0}


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


@app.get("/api/performance-stats", response_class=JSONResponse)
async def get_performance_stats(auth: bool = Depends(verify_dashboard_auth)):
    try:
        return await asyncio.to_thread(_performance_stats_sync)
    except Exception as exc:
        _log_failure("get performance stats", exc)
        return {"playback_sessions_hour": 0, "failovers_hour": 0, "db_size_mb": 0, "provider_health": []}


def _playback_stats_sync() -> dict:
    with _db_session() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT team_id, COUNT(*) as plays FROM playback_events WHERE timestamp > datetime('now', '-7 days') GROUP BY team_id ORDER BY plays DESC LIMIT 5")
        top_teams = [{"team_id": row[0], "plays": row[1]} for row in cursor.fetchall()]
        cursor.execute("SELECT COUNT(*) FROM playback_events WHERE timestamp > datetime('now', '-7 days')")
        total_plays = cursor.fetchone()[0] or 0
        return {"top_watched_teams": top_teams, "total_playbacks_week": total_plays}


@app.get("/api/playback-stats", response_class=JSONResponse)
async def get_playback_stats(auth: bool = Depends(verify_dashboard_auth)):
    try:
        return await asyncio.to_thread(_playback_stats_sync)
    except Exception as exc:
        _log_failure("get playback stats", exc)
        return {"top_watched_teams": [], "total_playbacks_week": 0}


def _record_stream_test_sync(team_id: str, is_live: bool, candidate_count: int) -> None:
    with _db_session() as conn:
        conn.execute("INSERT INTO stream_test_results (team_id, is_live, candidate_count) VALUES (?, ?, ?)", (team_id, 1 if is_live else 0, candidate_count))
        conn.commit()


@app.post("/api/test-stream/{team_id}")
async def test_stream(team_id: str, auth: bool = Depends(verify_dashboard_auth)):
    try:
        data = stream_state.get(team_id)
        candidates = data.get("candidates", []) if data else []
        is_live = len(candidates) > 0 and data.get("is_healthy", False)
        await asyncio.to_thread(_record_stream_test_sync, team_id, is_live, len(candidates))
        return {"team_id": team_id, "is_live": is_live, "candidate_count": len(candidates), "status": "✅ Live" if is_live else "❌ Offline"}
    except Exception as exc:
        _log_failure(f"test stream {team_id}", exc)
        return {"status": "error"}

@app.post("/settings/provider-rotation")
async def set_provider_rotation(enabled: bool = Form(False), auth: bool = Depends(verify_dashboard_auth)):
    await set_setting_async("provider_rotation_mode", "1" if enabled else "0")
    return RedirectResponse(url="/?tab=channels&status=saved", status_code=303)


def _schedule_team_disable_sync(team_id: str, disable_date: str) -> None:
    with _db_session() as conn:
        conn.execute("UPDATE teams SET auto_disable_after=? WHERE team_id=?", (disable_date, team_id))
        conn.commit()


@app.post("/team/{team_id}/schedule-disable")
async def schedule_team_disable(team_id: str, disable_date: str = Form(""), auth: bool = Depends(verify_dashboard_auth)):
    try:
        await asyncio.to_thread(_schedule_team_disable_sync, team_id, disable_date)
        if team_id in stream_state:
            stream_state[team_id]["auto_disable_after"] = disable_date
    except Exception as exc:
        _log_failure(f"schedule disable {team_id}", exc)
    return RedirectResponse(url="/?tab=channels", status_code=303)

def _create_tray_image() -> Image.Image:
    logo_path = _resource_path("assets/jellyball-icon.png")
    try:
        return Image.open(logo_path).convert("RGBA").resize((64, 64), Image.Resampling.LANCZOS)
    except (OSError, ValueError) as exc:
        LOGGER.warning("Could not load JellyBall tray icon error=%s", type(exc).__name__)
    image = Image.new("RGBA", (64, 64), (15, 23, 42, 255))
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((8, 8, 56, 56), radius=12, fill=(37, 99, 235, 255))
    draw.ellipse((20, 20, 44, 44), fill=(255, 255, 255, 255))
    return image


class TrayApplication:
    def __init__(self):
        self.server = None
        self.server_thread = None
        self.icon = pystray.Icon(
            "jellyball",
            _create_tray_image(),
            "Jellyball Sports Proxy",
            pystray.Menu(
                pystray.MenuItem("View", self.open_gui, default=True),
                pystray.MenuItem("Restart", self.restart),
                pystray.MenuItem("Quit", self.quit),
            ),
        )

    def open_gui(self, icon, item):
        webbrowser.open(f"http://127.0.0.1:{PORT}/")

    def quit(self, icon, item):
        if self.server:
            self.server.should_exit = True
        icon.stop()

    def restart(self, icon, item):
        if self.server:
            self.server.should_exit = True

        def relaunch():
            if self.server_thread:
                self.server_thread.join(timeout=10)
            if getattr(sys, "frozen", False):
                command = [sys.executable, *sys.argv[1:]]
            else:
                command = [sys.executable, str(Path(sys.argv[0]).resolve()), *sys.argv[1:]]
            subprocess.Popen(command, close_fds=True)
            self.icon.stop()

        threading.Thread(target=relaunch, name="jellyball-restart", daemon=True).start()

    def run(self):
        import uvicorn

        global PORT
        selected_port = _find_available_port(PORT)
        if selected_port != PORT:
            LOGGER.warning("Configured port %s is unavailable; using local port %s", PORT, selected_port)
            PORT = selected_port

        config = uvicorn.Config(
            app,
            host="127.0.0.1",
            port=PORT,
            reload=False,
            log_config=None,
        )
        self.server = uvicorn.Server(config)
        self.server_thread = threading.Thread(
            target=self.server.run,
            name="jellyball-server",
            daemon=True,
        )
        self.server_thread.start()
        LOGGER.info("Jellyball web GUI available at http://127.0.0.1:%s", PORT)
        threading.Timer(1.0, lambda: webbrowser.open(f"http://127.0.0.1:{PORT}/")).start()
        try:
            self.icon.run()
        finally:
            self.server.should_exit = True
            self.server_thread.join(timeout=10)


def _run_headless() -> None:
    """No system tray on Linux/Docker (pystray needs a display we don't have there),
    and 127.0.0.1 isn't reachable from outside a container. Used automatically on
    non-Windows platforms, or anywhere via JELLYBALL_HEADLESS=1."""
    import uvicorn

    global PORT
    if os.getenv("WEB_CONCURRENCY", "1") not in {"", "1"}:
        raise RuntimeError("Jellyball must run with one worker")
    selected_port = _find_available_port(PORT)
    if selected_port != PORT:
        LOGGER.warning("Configured port %s is unavailable; using local port %s", PORT, selected_port)
        PORT = selected_port
    host = os.getenv("JELLYBALL_HOST", "0.0.0.0" if sys.platform != "win32" else "127.0.0.1")
    LOGGER.info("Jellyball running headless at http://%s:%s", host, PORT)
    uvicorn.run(app, host=host, port=PORT, log_config=None)


if __name__ == "__main__":
    _wants_headless = sys.platform != "win32" or os.getenv("JELLYBALL_HEADLESS", "").strip().lower() in {"1", "true", "yes"}
    if _wants_headless:
        _run_headless()
    else:
        TrayApplication().run()

