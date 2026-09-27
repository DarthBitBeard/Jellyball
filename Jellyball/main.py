import os
import sys
import logging
import re
import queue
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
import subprocess
import webbrowser
import socket
import shutil
from pathlib import Path
from html import escape as html_escape
from typing import Optional

# Resolve configuration and writable data independently of the current directory.
# This is important when the application is launched from a Jellyfin service or a shortcut.
APP_DIR = Path(sys.executable if getattr(sys, "frozen", False) else __file__).resolve().parent
BUNDLE_DIR = Path(getattr(sys, "_MEIPASS", APP_DIR))


def _get_writable_data_dir() -> Path:
    # Explicit override first: the Windows service uses %ProgramData%\Jellyball,
    # Docker uses the mounted volume, tests use a temp dir.
    override = os.getenv("JELLYBALL_DATA_DIR", "").strip()
    if override:
        candidate = Path(override).expanduser()
        candidate.mkdir(parents=True, exist_ok=True)
        return candidate
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

# Size-capped rotation (5 x 10 MB) so a noisy day can't fill the disk of a box
# that runs for months. Records go through a queue so the event loop never
# blocks on file I/O; a background thread does the writing.
_log_handler = RotatingFileHandler(
    filename=str(LOG_FILE),
    maxBytes=10 * 1024 * 1024,
    backupCount=5,
    encoding="utf-8",
    delay=True,
)
_log_handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"))
_sink_handlers: list = [_log_handler]
# The windowed exe and the Windows service have no stdout (sys.stdout is None);
# a StreamHandler there fails on every record.
if sys.stdout is not None:
    _console_handler = logging.StreamHandler(sys.stdout)
    _console_handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    _sink_handlers.append(_console_handler)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
_LOG_LISTENER: Optional["QueueListener"] = None
if not root_logger.handlers:
    _log_queue: "queue.SimpleQueue" = queue.SimpleQueue()
    root_logger.addHandler(QueueHandler(_log_queue))
    _LOG_LISTENER = QueueListener(_log_queue, *_sink_handlers, respect_handler_level=True)
    _LOG_LISTENER.start()
LOGGER.setLevel(logging.INFO)

# httpx/httpcore log one INFO line per HTTP request by default — with many active
# channels that's every provider probe and every HLS segment fetch, drowning real
# diagnostics. uvicorn.access would log every segment request (with the full
# tokenized upstream URL in the query string for legacy /chunk requests).
for _noisy_logger in ("httpx", "httpcore", "uvicorn.access", "hpack", "h2"):
    logging.getLogger(_noisy_logger).setLevel(logging.WARNING)


_URL_IN_TEXT_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9+.-]*://[^\s'\"<>]+")


def _safe_exception_detail(exc: BaseException, limit: int = 160) -> str:
    """Exception text with URLs (tokens, webhook secrets) cut down to their host."""
    def _host_only(match) -> str:
        text = match.group(0)
        scheme, _, rest = text.partition("://")
        host = rest.split("/", 1)[0].split("?", 1)[0].rsplit("@", 1)[-1]
        return f"{scheme}://{host}/..."

    detail = _URL_IN_TEXT_RE.sub(_host_only, str(exc)).replace("\n", " ").strip()
    return detail[:limit]


def _log_failure(operation: str, exc: BaseException, level: int = logging.WARNING) -> None:
    """Log a failure with its type and a URL-scrubbed detail (URLs carry tokens)."""
    detail = _safe_exception_detail(exc)
    if detail:
        LOGGER.log(level, "%s failed (%s: %s)", operation, type(exc).__name__, detail)
    else:
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

_PLAYWRIGHT_PATH_SET_BY_APP = not os.getenv("PLAYWRIGHT_BROWSERS_PATH")
if _PLAYWRIGHT_PATH_SET_BY_APP:
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
import math
import hashlib
import hmac
import ipaddress
import secrets
import time
import random
import threading
from collections import OrderedDict, deque
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from xml.sax.saxutils import escape as xml_escape
from dotenv import load_dotenv
from fastapi import FastAPI, Form, Request, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import PlainTextResponse, Response, HTMLResponse, RedirectResponse, StreamingResponse, JSONResponse, FileResponse
from typing import Callable, Dict, List, Optional, Tuple, Set
from playwright.async_api import async_playwright, Browser, Playwright, Page
# pystray/PIL are imported lazily in tray mode only: on a headless Linux host
# `import pystray` tries to open an X display at import time and crashes.

from version import __version__

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
    playwright_page,
    playwright_pages_in_use,
    make_soup,
    verify_stream_live,
    DEFAULT_USER_AGENT,
    rank_streams,
    is_ignored_url,
)
from network_safety import bounded_float, bounded_int, validate_http_url, validate_http_url_async
from hls_session import FetchResult, SessionConfig, SessionHooks, SessionRegistry, SourceSpec

# Precedence: real environment variables > the data directory's .env (per-user
# for the tray app, %ProgramData%\Jellyball\.env for the service, written by the
# installer) > a .env beside the executable (package-wide defaults). Previously
# the file beside the exe overrode everything, including the environment a
# service manager or Docker passed in.
load_dotenv(dotenv_path=USER_ENV_FILE)
load_dotenv(dotenv_path=APP_DIR / ".env")
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

# bcrypt is used directly: passlib 1.7.4 is unmaintained and its bcrypt backend
# self-test fails against bcrypt>=4.1/5.x, which made hashed DASHBOARD_PASSWORD
# values silently fall back to a plain-text comparison that could never match.
try:
    import bcrypt as _bcrypt
except ImportError:
    _bcrypt = None
    if os.getenv("DASHBOARD_PASSWORD", "").startswith(("$2a$", "$2b$", "$2y$")):
        LOGGER.error("bcrypt is not installed; hashed dashboard passwords cannot be verified")

_BCRYPT_HASH_PREFIXES = ("$2a$", "$2b$", "$2y$")
# bcrypt verification deliberately costs ~100-300ms, and the dashboard polls
# several authenticated API routes; remember recent successful logins briefly.
_VERIFIED_CREDENTIALS: "OrderedDict[str, float]" = OrderedDict()
_VERIFIED_CREDENTIALS_TTL = 600.0
_VERIFIED_CREDENTIALS_MAX = 32


def _dashboard_password_matches(candidate: str, configured: str) -> bool:
    if configured.startswith(_BCRYPT_HASH_PREFIXES):
        if _bcrypt is None:
            return False
        try:
            # bcrypt only uses the first 72 bytes; bcrypt>=5 raises instead of truncating.
            return _bcrypt.checkpw(candidate.encode("utf-8")[:72], configured.encode("utf-8"))
        except ValueError:
            return False
    return secrets.compare_digest(candidate.encode("utf-8"), configured.encode("utf-8"))

DASHBOARD_USERNAME = os.getenv("DASHBOARD_USERNAME", "admin")
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "")
DASHBOARD_PASSWORD_FILE = DATA_DIR / "dashboard-password.txt"
# "configured" (DASHBOARD_PASSWORD), "generated" (network bind without one), or
# "open" (no password; only allowed while listening on loopback).
DASHBOARD_AUTH_MODE = "configured" if DASHBOARD_PASSWORD else "open"
security = HTTPBasic(auto_error=False)

_LOOPBACK_HOSTNAMES = {"localhost", "127.0.0.1", "::1", "[::1]"}


def _is_loopback_host(host: str) -> bool:
    host = (host or "").strip().lower().strip("[]")
    if host in _LOOPBACK_HOSTNAMES:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _configure_dashboard_auth(bind_host: str) -> None:
    """A dashboard with no password may only listen on loopback. Listening on the
    network (Docker's 0.0.0.0, a LAN bind) without DASHBOARD_PASSWORD gets a
    random password, generated once and kept in the data directory."""
    global DASHBOARD_PASSWORD, DASHBOARD_AUTH_MODE
    if DASHBOARD_PASSWORD or _is_loopback_host(bind_host):
        return
    generated = ""
    try:
        generated = DASHBOARD_PASSWORD_FILE.read_text(encoding="utf-8").strip()
    except OSError:
        pass
    if not generated:
        generated = secrets.token_urlsafe(12)
        try:
            DASHBOARD_PASSWORD_FILE.write_text(generated + "\n", encoding="utf-8")
        except OSError as exc:
            _log_failure("save generated dashboard password", exc)
        # Logged once, on the run that creates it (docker logs / the console);
        # later runs only point at the file.
        LOGGER.warning(
            "Dashboard listens on %s with no DASHBOARD_PASSWORD: generated one. user=%s password=%s (saved to %s)",
            bind_host, DASHBOARD_USERNAME, generated, DASHBOARD_PASSWORD_FILE,
        )
    else:
        LOGGER.warning(
            "Dashboard listens on %s with no DASHBOARD_PASSWORD: using the generated password in %s (user=%s)",
            bind_host, DASHBOARD_PASSWORD_FILE, DASHBOARD_USERNAME,
        )
    DASHBOARD_PASSWORD = generated
    DASHBOARD_AUTH_MODE = "generated"


def _request_host_name(request: Request) -> str:
    host = request.headers.get("host", "")
    if host.startswith("["):
        return host[1:].split("]", 1)[0]
    return host.rsplit(":", 1)[0] if host.count(":") == 1 else host


# Failed Basic-auth attempts per client address: (failures, window start, locked until).
_AUTH_FAILURES: "OrderedDict[str, List[float]]" = OrderedDict()
AUTH_FAILURE_LIMIT = 8
AUTH_FAILURE_WINDOW = 300.0
AUTH_LOCKOUT_SECONDS = 300.0


def _auth_client_key(request: Optional[Request]) -> str:
    client = getattr(request, "client", None) if request is not None else None
    return getattr(client, "host", "") or "unknown"


def _auth_locked_out(client_key: str, now: float) -> bool:
    entry = _AUTH_FAILURES.get(client_key)
    return bool(entry and entry[2] > now)


def _record_auth_failure(client_key: str, now: float) -> None:
    entry = _AUTH_FAILURES.get(client_key)
    if entry is None or now - entry[1] > AUTH_FAILURE_WINDOW:
        entry = [0.0, now, 0.0]
    entry[0] += 1
    if entry[0] >= AUTH_FAILURE_LIMIT:
        entry[2] = now + AUTH_LOCKOUT_SECONDS
        LOGGER.warning("Dashboard login locked for %.0fs after %d failures client=%s",
                       AUTH_LOCKOUT_SECONDS, int(entry[0]), client_key)
    _AUTH_FAILURES[client_key] = entry
    _AUTH_FAILURES.move_to_end(client_key)
    while len(_AUTH_FAILURES) > 256:
        _AUTH_FAILURES.popitem(last=False)


def verify_dashboard_auth(request: Request = None, credentials: Optional[HTTPBasicCredentials] = Depends(security)):
    if not DASHBOARD_PASSWORD:
        # Open access is only ever served on a loopback bind. Also require a
        # loopback Host header, so a DNS-rebinding page (evil.example resolving
        # to 127.0.0.1) can't drive the dashboard from the user's browser.
        if request is not None and not _is_loopback_host(_request_host_name(request)) \
                and _request_host_name(request).lower() != socket.gethostname().lower():
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Dashboard is local-only")
        return True
    now = time.monotonic()
    client_key = _auth_client_key(request)
    if _auth_locked_out(client_key, now):
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Too many failed logins")
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Basic"},
        )

    # compare_digest on str raises TypeError for non-ASCII input; compare bytes.
    is_user_ok = secrets.compare_digest(credentials.username.encode("utf-8"), DASHBOARD_USERNAME.encode("utf-8"))
    cache_key = hashlib.sha256(
        f"{credentials.username}\0{credentials.password}\0{DASHBOARD_PASSWORD}".encode("utf-8")
    ).hexdigest()
    verified_at = _VERIFIED_CREDENTIALS.get(cache_key)
    if verified_at is not None and now - verified_at < _VERIFIED_CREDENTIALS_TTL:
        is_pass_ok = True
    else:
        is_pass_ok = _dashboard_password_matches(credentials.password, DASHBOARD_PASSWORD)
        if is_pass_ok and is_user_ok:
            _VERIFIED_CREDENTIALS[cache_key] = now
            _VERIFIED_CREDENTIALS.move_to_end(cache_key)
            while len(_VERIFIED_CREDENTIALS) > _VERIFIED_CREDENTIALS_MAX:
                _VERIFIED_CREDENTIALS.popitem(last=False)

    if not (is_user_ok and is_pass_ok):
        _record_auth_failure(client_key, now)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    _AUTH_FAILURES.pop(client_key, None)
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
            await _remove_channel(team_id)
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


JELLYFIN_AUTO_REFRESH_MIN_INTERVAL = bounded_float(
    os.getenv("JELLYFIN_AUTO_REFRESH_MIN_INTERVAL", "600"), 600.0, 60.0, 86400.0
)
_JELLYFIN_REFRESH_STATE = {"signature": None, "last_run": 0.0, "pending": None}


def _guide_signature() -> str:
    """Hash of everything Jellyfin's guide shows for our channels. Scrapes only
    trigger a guide refresh when this changes - previously every successful
    5-minute rescrape of every channel queued a full Jellyfin guide refresh."""
    parts = [f"show_offseason={SHOW_OFFSEASON_CHANNELS}"]
    for team_id, data in sorted(stream_state.items()):
        parts.append("\0".join(str(value) for value in (
            team_id,
            data.get("name", ""),
            data.get("logo_url", ""),
            data.get("start_time", ""),
            data.get("stop_time", ""),
            data.get("schedule_status", ""),
            data.get("tvg_id", ""),
            data.get("group_title", ""),
            bool(data.get("candidates")),
        )))
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()


async def _debounced_jellyfin_refresh(delay: float) -> None:
    try:
        await asyncio.sleep(delay)
        signature = _guide_signature()
        if signature == _JELLYFIN_REFRESH_STATE["signature"]:
            return
        _JELLYFIN_REFRESH_STATE["signature"] = signature
        _JELLYFIN_REFRESH_STATE["last_run"] = time.monotonic()
        await trigger_jellyfin_refresh()
    finally:
        _JELLYFIN_REFRESH_STATE["pending"] = None


def request_jellyfin_guide_refresh_if_changed() -> None:
    """Refresh Jellyfin's guide for automatic (scrape/schedule-driven) changes only
    when the guide contents actually changed, at most once per
    JELLYFIN_AUTO_REFRESH_MIN_INTERVAL. User-initiated changes still call
    trigger_jellyfin_refresh() directly for an immediate refresh."""
    if _JELLYFIN_REFRESH_STATE["pending"] is not None:
        return
    if _guide_signature() == _JELLYFIN_REFRESH_STATE["signature"]:
        return
    if _JELLYFIN_REFRESH_STATE["last_run"] == 0.0:
        # First automatic refresh after startup: wait a minute so the initial
        # burst of channel scrapes lands in one refresh instead of the first one.
        delay = 60.0
    else:
        elapsed = time.monotonic() - _JELLYFIN_REFRESH_STATE["last_run"]
        delay = max(0.0, JELLYFIN_AUTO_REFRESH_MIN_INTERVAL - elapsed)
    _JELLYFIN_REFRESH_STATE["pending"] = _spawn_background_task(
        _debounced_jellyfin_refresh(delay), "debounced Jellyfin guide refresh"
    )

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


# --- Shared TTL cache + single-flight for aggregator index/category pages -----
# Every team's scrape cycle calls the same handful of ACTIVE_PROVIDERS, each of
# which re-fetches (and, on a Cloudflare block, re-renders via Playwright) the
# exact same index/category URLs. With dozens of teams configured this repeats
# the same network fetch (and possibly a full Chromium render) many times a
# minute for content that changes at most once a minute. Caching the raw HTML
# per URL, with single-flight de-duplication for concurrent callers, collapses
# all of that down to one fetch per URL per TTL window.
SCRAPE_INDEX_CACHE_SECONDS = bounded_float(os.getenv("SCRAPE_INDEX_CACHE_SECONDS", "60"), 60.0, 0.0, 3600.0)
_SCRAPE_INDEX_CACHE_MAX_ENTRIES = 64
# Failures (including "not found") are cached only briefly, so a transient
# outage doesn't leave every team blind for a full cache window.
_SCRAPE_INDEX_FAILURE_CACHE_SECONDS = min(5.0, SCRAPE_INDEX_CACHE_SECONDS) if SCRAPE_INDEX_CACHE_SECONDS > 0 else 0.0
_SCRAPE_INDEX_CACHE: "OrderedDict[str, tuple[float, Optional[str]]]" = OrderedDict()
_SCRAPE_INDEX_INFLIGHT: Dict[str, asyncio.Future] = {}
_SCRAPE_INDEX_INFLIGHT_LOOP = None


async def _get_cached_index_html(url: str, fetcher) -> Optional[str]:
    """Return `url`'s index-page HTML from the shared TTL cache, or run
    `fetcher()` (an async, argument-less callable that performs the actual
    fetch, HTTP and/or Playwright fallback) once per TTL window/failure window,
    sharing the in-flight call across any other team requesting the same URL
    at the same time.
    """
    global _SCRAPE_INDEX_INFLIGHT_LOOP

    now = time.monotonic()
    cached = _SCRAPE_INDEX_CACHE.get(url)
    if cached is not None:
        expires_at, cached_html = cached
        if now < expires_at:
            _SCRAPE_INDEX_CACHE.move_to_end(url)
            return cached_html
        _SCRAPE_INDEX_CACHE.pop(url, None)

    loop = asyncio.get_running_loop()
    if _SCRAPE_INDEX_INFLIGHT_LOOP is not loop:
        # A fresh loop (e.g. a new test run) can't share futures created on a
        # prior, now-closed loop.
        _SCRAPE_INDEX_INFLIGHT.clear()
        _SCRAPE_INDEX_INFLIGHT_LOOP = loop

    existing = _SCRAPE_INDEX_INFLIGHT.get(url)
    if existing is not None:
        return await asyncio.shield(existing)

    future: asyncio.Future = loop.create_future()
    _SCRAPE_INDEX_INFLIGHT[url] = future
    html_text: Optional[str] = None
    try:
        try:
            html_text = await fetcher()
        except Exception as exc:
            _log_failure(f"fetch index page {url}", exc)
            html_text = None
        ttl = SCRAPE_INDEX_CACHE_SECONDS if html_text is not None else _SCRAPE_INDEX_FAILURE_CACHE_SECONDS
        if ttl > 0:
            _SCRAPE_INDEX_CACHE[url] = (time.monotonic() + ttl, html_text)
            _SCRAPE_INDEX_CACHE.move_to_end(url)
            while len(_SCRAPE_INDEX_CACHE) > _SCRAPE_INDEX_CACHE_MAX_ENTRIES:
                _SCRAPE_INDEX_CACHE.popitem(last=False)
        return html_text
    finally:
        # Resolve the future even on cancellation so any other team waiting on
        # this URL isn't left hanging until its own provider-level timeout.
        if not future.done():
            future.set_result(html_text)
        if _SCRAPE_INDEX_INFLIGHT.get(url) is future:
            _SCRAPE_INDEX_INFLIGHT.pop(url, None)


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

    async def _fetch_index_page(self, client: httpx.AsyncClient, page_url: str, browser: Optional[Browser]) -> Optional[str]:
        """Fetch one index/category page: fast HTTP first, falling back to a
        Playwright-rendered page when the site is behind a Cloudflare check."""
        page_html = await fetch_bounded_text(
            client,
            page_url,
            headers={"User-Agent": DEFAULT_USER_AGENT, "Referer": self.base_url},
            timeout=8.0,
        )
        if not page_html and browser and browser.is_connected():
            try:
                async with playwright_page(browser, user_agent=DEFAULT_USER_AGENT) as page:
                    await page.goto(page_url, wait_until="domcontentloaded", timeout=35000)
                    try:
                        await page.wait_for_load_state("networkidle", timeout=8000)
                    except Exception:
                        pass
                    page_html = await page.content()
            except Exception as exc:
                _log_failure(f"{self.name} Cloudflare bypass for {page_url}", exc)
        return page_html

    def _parse_matches_from_html(
        self,
        page_html: str,
        page_url: str,
        search_terms: List[str],
        matches: List[tuple[str, int, str]],
        seen_matches: Set[str],
    ) -> None:
        """Pure CPU work (BeautifulSoup parse + per-anchor fuzzy matching),
        split out so it can run in a worker thread via asyncio.to_thread instead
        of blocking the shared event loop that also serves live video segments.
        Mutates `matches`/`seen_matches` in place to preserve the original
        cross-page, cumulative MAX_PROVIDER_EVENTS cutoff.
        """
        soup = make_soup(page_html)
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
                    return

    async def _find_matches(self, client: httpx.AsyncClient, search_terms: List[str], browser: Optional[Browser] = None) -> List[tuple[str, int, str]]:
        matches: List[tuple[str, int, str]] = []
        seen_matches: Set[str] = set()
        for page_url in self.get_scan_urls():
            try:
                page_html = await _get_cached_index_html(
                    page_url, lambda pu=page_url: self._fetch_index_page(client, pu, browser)
                )
                if not page_html:
                    continue

                await asyncio.to_thread(
                    self._parse_matches_from_html, page_html, page_url, search_terms, matches, seen_matches
                )
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

    def _find_best_channel_match(
        self, html_text: str, raw_terms_lower: List[str], search_terms: List[str]
    ) -> tuple[Optional[str], int, str]:
        """Pure CPU work (BeautifulSoup parse + per-anchor fuzzy matching over
        every listed channel), split out so it can run via asyncio.to_thread
        instead of blocking the shared event loop."""
        soup = make_soup(html_text)
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

        return best_url, best_score, best_title

    async def search(self, query_or_terms, browser: Optional[Browser] = None, http_client: Optional[httpx.AsyncClient] = None) -> List[dict]:
        if not browser or not browser.is_connected():
            return []

        search_terms = get_team_search_terms(query_or_terms, query_or_terms) if isinstance(query_or_terms, str) else list(query_or_terms or [])
        raw_terms_lower = [t.lower() for t in (query_or_terms if isinstance(query_or_terms, list) else [query_or_terms])]
        streams = []

        try:
            async with playwright_page(browser, user_agent=DEFAULT_USER_AGENT) as page:
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
                    return []

                best_url, best_score, best_title = await asyncio.to_thread(
                    self._find_best_channel_match, html, raw_terms_lower, search_terms
                )

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
        except Exception as exc:
            _log_failure("TheTVApp custom extraction", exc)

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

    async def _fetch_directory_page(self, client: httpx.AsyncClient, page_url: str, browser: Optional[Browser]) -> Optional[str]:
        page_html = await fetch_bounded_text(
            client,
            page_url,
            headers={"User-Agent": DEFAULT_USER_AGENT, "Referer": self.base_url},
            timeout=8.0,
        )
        if not page_html and browser and browser.is_connected():
            try:
                async with playwright_page(browser, user_agent=DEFAULT_USER_AGENT) as page:
                    await page.goto(page_url, wait_until="domcontentloaded", timeout=25000)
                    await asyncio.sleep(2)
                    page_html = await page.content()
            except Exception as exc:
                _log_failure("scan DaddyLive directory with browser", exc)
        return page_html

    def _parse_channel_matches_from_html(
        self,
        page_html: str,
        page_url: str,
        search_terms: List[str],
        matches: List[tuple[str, int, str]],
        seen_matches: Set[str],
    ) -> None:
        """Pure CPU work (BeautifulSoup parse + per-card channel-term matching),
        split out so it can run via asyncio.to_thread instead of blocking the
        shared event loop. Mutates `matches`/`seen_matches` in place to preserve
        the original cross-page, cumulative MAX_PROVIDER_EVENTS cutoff."""
        soup = make_soup(page_html)
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
                    return

    async def _find_matches(self, client: httpx.AsyncClient, search_terms: List[str], browser: Optional[Browser] = None) -> List[tuple[str, int, str]]:
        """Match individual directory cards without inheriting the grid's text."""
        matches: List[tuple[str, int, str]] = []
        seen_matches: Set[str] = set()
        for page_url in self.get_scan_urls():
            page_html = await _get_cached_index_html(
                page_url, lambda pu=page_url: self._fetch_directory_page(client, pu, browser)
            )
            if not page_html:
                continue

            await asyncio.to_thread(
                self._parse_channel_matches_from_html, page_html, page_url, search_terms, matches, seen_matches
            )
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

        try:
            async with playwright_page(browser, user_agent=DEFAULT_USER_AGENT) as page:
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
MEDIA_HTTP_CLIENT: Optional[httpx.AsyncClient] = None
PLAYWRIGHT_CLIENT: Optional[Playwright] = None
SHARED_BROWSER: Optional[Browser] = None
_PLAYWRIGHT_LOCK = asyncio.Lock()
# Periodically recycle the shared Chromium instance so a slow memory/handle leak
# inside the browser itself can't accumulate for the life of the process. 0
# disables recycling. Tracked as (id(SHARED_BROWSER), monotonic launch time) so
# a browser launched by lifespan() (outside get_healthy_browser) still gets a
# correct clock the first time it's observed here, and so the clock resets
# whenever the browser object actually changes instead of trusting stale state.
PLAYWRIGHT_RECYCLE_HOURS = bounded_float(os.getenv("PLAYWRIGHT_RECYCLE_HOURS", "6"), 6.0, 0.0, 168.0)
_BROWSER_LAUNCH_INFO: Optional[Tuple[int, float]] = None
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


# Off-season channels leave the M3U/guide by default; with this on they stay
# listed with an "Off-season - resumes <date>" guide block (Settings tab).
SHOW_OFFSEASON_CHANNELS = False


def _channel_listed(data: dict) -> bool:
    """Whether a channel appears in the M3U playlist and XMLTV guide."""
    return SHOW_OFFSEASON_CHANNELS or not _channel_is_off_season(data)


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


_CONTROL_CHARS_RE = re.compile(r"[\x00-\x1f\x7f-\x9f  ]")
# Characters XML 1.0 forbids outright (xml_escape leaves them in and the guide
# then fails to parse).
_XML_INVALID_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f￾￿]")


def _clean_label(value: object, max_length: int = 120) -> str:
    """Single-line, bounded text for names/queries that end up in the M3U, EPG and UI."""
    text = _CONTROL_CHARS_RE.sub(" ", str(value or ""))
    return re.sub(r"\s+", " ", text).strip()[:max_length].strip()


def _m3u_attribute(value: str) -> str:
    # A newline here would start a new playlist line (a planted channel/URL).
    return _CONTROL_CHARS_RE.sub(" ", str(value or "")).replace('"', "&quot;")


def _m3u_title(value: str) -> str:
    return _CONTROL_CHARS_RE.sub(" ", str(value or "")).strip()


def _xml_text(value: object) -> str:
    return xml_escape(_XML_INVALID_RE.sub("", str(value or "")))


def _xml_attr(value: object) -> str:
    return xml_escape(_XML_INVALID_RE.sub("", str(value or "")), {'"': "&quot;"})


_SAFE_HOST_HEADER_RE = re.compile(r"^[A-Za-z0-9.\-]+(:\d{1,5})?$|^\[[0-9A-Fa-f:.]+\](:\d{1,5})?$")


def _public_base_url(request: Optional[Request]) -> str:
    """Base URL for links we hand out (M3U entries, dashboard copy boxes).
    Falls back to loopback when the Host header is missing or malformed."""
    host = (request.headers.get("host") or "").strip() if request is not None else ""
    if not _SAFE_HOST_HEADER_RE.match(host):
        host = f"127.0.0.1:{PORT}"
    scheme = getattr(getattr(request, "url", None), "scheme", "http")
    scheme = scheme if scheme in ("http", "https") else "http"
    return f"{scheme}://{host}"


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
    """Retrieve the shared browser, relaunching it if it crashed, disconnected,
    or is due for a periodic recycle (PLAYWRIGHT_RECYCLE_HOURS). The recycle
    check runs opportunistically every time a caller acquires the browser, and
    is skipped whenever a page/context is still open against it, so an
    in-flight scrape is never torn down mid-navigation.
    """
    global SHARED_BROWSER, PLAYWRIGHT_CLIENT, _BROWSER_LAUNCH_INFO

    async with _PLAYWRIGHT_LOCK:
        if SHARED_BROWSER and SHARED_BROWSER.is_connected():
            if _BROWSER_LAUNCH_INFO is None or _BROWSER_LAUNCH_INFO[0] != id(SHARED_BROWSER):
                _BROWSER_LAUNCH_INFO = (id(SHARED_BROWSER), time.monotonic())
            launched_at = _BROWSER_LAUNCH_INFO[1]
            recycle_due = (
                PLAYWRIGHT_RECYCLE_HOURS > 0
                and time.monotonic() - launched_at >= PLAYWRIGHT_RECYCLE_HOURS * 3600
            )
            if not recycle_due or playwright_pages_in_use() > 0:
                return SHARED_BROWSER
            LOGGER.info(
                "Recycling Playwright browser after %.1f hour(s) in service",
                PLAYWRIGHT_RECYCLE_HOURS,
            )
        else:
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
            _BROWSER_LAUNCH_INFO = (id(browser), time.monotonic())
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


def _start_team_scrape_loop(team_id: str, initial_delay: float = 0.0) -> asyncio.Task:
    existing = _TEAM_SCRAPE_TASKS.get(team_id)
    if existing and not existing.done():
        return existing

    _TEAM_SCRAPE_WAKE_EVENTS.setdefault(team_id, asyncio.Event())
    task = asyncio.create_task(team_scrape_loop(team_id, initial_delay), name=f"scrape team={team_id}")
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


async def _get_active_provider_priority() -> Dict[str, int]:
    """Return the provider tie-break priority used by rank_streams().

    When provider rotation mode is enabled, the preferred provider rotates
    hourly so no single aggregator is hammered with every search, spreading
    load across sources instead of always preferring the same one.

    Uses get_setting_async() (a thread-offloaded SQLite read) instead of the
    synchronous get_setting(), since this runs on the same event loop that
    also serves live video segments for every other channel.
    """
    if await get_setting_async("provider_rotation_mode", "0") != "1":
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
    on_partial: Optional[Callable[[List[dict]], None]] = None,
) -> List[dict]:
    """Search every provider for the team's streams. `on_partial`, if given, is
    called with each provider's playable results as they arrive (ranked), so an
    emergency rescrape can put a channel back on air without waiting for the
    slowest provider's timeout."""
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

    async def _search_and_report(provider):
        res = await _jittered_search(provider)
        if on_partial is not None and res:
            playable = [s for s in res if always_live or _stream_matches_requested_event(s, search_terms)]
            if playable:
                try:
                    ranked = rank_streams(playable, await _get_active_provider_priority())
                    on_partial(ranked[:MAX_STREAM_CANDIDATES])
                except Exception as exc:
                    _log_failure("apply partial scrape results", exc)
        return res

    try:
        tasks = [_search_and_report(provider) for provider in providers]
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
    all_streams = rank_streams(all_streams, await _get_active_provider_priority())
    for s in all_streams:
        u = s.get("url")
        if u and u not in seen_urls:
            seen_urls.add(u)
            deduped.append(s)

    # Cap the returned candidate list: downstream merge steps already cap
    # stream_state[...]['candidates'] to MAX_STREAM_CANDIDATES, but capping
    # here too bounds how many streams every caller of master_scrape (and any
    # future one) has to carry around and log.
    return deduped[:MAX_STREAM_CANDIDATES]

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


_CANDIDATE_HEALTH_FIELDS = (
    "last_health_check", "last_health_ok", "consecutive_failures", "session_compatible", "incompatible_at",
    "probe_state", "has_audio", "codec_signature",
)


_TOKEN_PATH_SEGMENT_RE = re.compile(r"^[A-Za-z0-9_\-.~=]{24,}$")


def _is_token_path_segment(segment: str) -> bool:
    """Long mixed letters+digits segments (or signed 'exp=...~hmac=...' ones) are
    rotating tokens, not stream identity. Channel names/ids are short."""
    if "=" in segment and ("exp" in segment.lower() or "hmac" in segment.lower() or "token" in segment.lower()):
        return True
    return bool(
        _TOKEN_PATH_SEGMENT_RE.match(segment)
        and any(c.isdigit() for c in segment)
        and any(c.isalpha() for c in segment)
    )


def candidate_source_key(candidate: dict) -> Tuple[str, str, str]:
    """Identity of a stream source ignoring its query string and token-like path
    segments: aggregator URLs carry rotating tokens, so the same CDN stream gets
    a new URL on every rescrape (a new key lost its health history and made a
    token refresh look like a source switch)."""
    parts = urllib.parse.urlsplit(str(candidate.get("url") or ""))
    path = "/".join("*" if _is_token_path_segment(seg) else seg for seg in parts.path.split("/"))
    return (str(candidate.get("provider") or "").lower(), parts.netloc.lower(), path)


def _merge_stream_candidates(
    previous: List[dict],
    active_index: int,
    fresh: List[dict],
    keep_active: bool,
) -> Tuple[List[dict], int]:
    """Merge a rescrape's candidates into the current list without disturbing playback.

    Previously every rescrape (every 5 minutes, forever, for 24/7 channels) replaced
    the list and reset active_index to 0 - an unsignaled mid-playback source swap
    even when the current stream was perfectly healthy. Now a healthy active
    candidate stays first (and stays the object the channel session is playing;
    if the rescrape found the same source with a fresh token, only its URL is
    refreshed), fresh candidates become standbys, and known standbys keep their
    health history. Returns (candidates, active_index).
    """
    previous_by_key = {candidate_source_key(c): c for c in previous if c.get("url")}
    merged: List[dict] = []
    seen: Set[Tuple[str, str, str]] = set()

    if keep_active and 0 <= active_index < len(previous):
        active = previous[active_index]
        if active.get("url"):
            active_key = candidate_source_key(active)
            for candidate in fresh:
                if candidate.get("url") and candidate_source_key(candidate) == active_key:
                    for field in ("url", "referer", "origin"):
                        if candidate.get(field):
                            active[field] = candidate[field]
                    break
            merged.append(active)
            seen.add(active_key)

    for candidate in fresh:
        if not candidate.get("url"):
            continue
        key = candidate_source_key(candidate)
        if key in seen:
            continue
        old = previous_by_key.get(key)
        if old is not None:
            for field in _CANDIDATE_HEALTH_FIELDS:
                if field in old and field not in candidate:
                    candidate[field] = old[field]
        merged.append(candidate)
        seen.add(key)

    merged = merged[:MAX_STREAM_CANDIDATES]
    kept_active = (
        keep_active and bool(merged) and 0 <= active_index < len(previous)
        and merged[0] is previous[active_index]
    )
    if kept_active:
        return merged, 0
    # Nothing to keep: start at the best-health source, not blindly at [0]
    # (which may have failed seconds ago).
    return merged, _best_candidate_index(merged)


async def trigger_scrape(team_id: str, force: bool = False):
    if team_id in _SCRAPE_IN_FLIGHT:
        LOGGER.debug("Skipping duplicate scrape team=%s", team_id)
        return
    data = stream_state.get(team_id)
    if not data:
        return
    _SCRAPE_IN_FLIGHT.add(team_id)
    _mark_scrape_started(data)
    try:
        await _trigger_scrape(team_id, force=force)
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


async def _trigger_scrape_unlocked(team_id: str, force: bool = False):
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

    if _stream_window_should_close(team_id, current):
        current["candidates"] = []
        current["active_index"] = 0
        current["is_healthy"] = False
        current["exhausted"] = False
        return

    if (
        not force
        and current.get("candidates")
        and current.get("is_healthy")
        and not current.get("exhausted")
        and time.time() - current.get("last_candidate_refresh", 0.0) < HEALTHY_RESCRAPE_SECONDS
    ):
        # A healthy channel doesn't need its providers re-searched every 5 minutes
        # (that was constant Playwright load for every 24/7 channel); refresh the
        # standby list at HEALTHY_RESCRAPE_SECONDS, or sooner once it degrades.
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
        merged, merged_index = _merge_stream_candidates(
            current.get("candidates", []),
            current.get("active_index", 0),
            new_candidates,
            keep_active=bool(was_healthy),
        )
        current["candidates"] = merged
        current["active_index"] = merged_index
        current["exhausted"] = False
        current["last_candidate_refresh"] = time.time()
        is_healthy = True
        current["is_healthy"] = True
        SESSIONS.poke(team_id)
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
        request_jellyfin_guide_refresh_if_changed()


async def _trigger_scrape(team_id: str, force: bool = False):
    """Serialize all scrape-driven updates for one channel."""
    async with _team_state_lock(team_id):
        await _trigger_scrape_unlocked(team_id, force=force)


async def team_scrape_loop(team_id: str, initial_delay: float = 0.0):
    if initial_delay > 0:
        # Spread startup scrapes out instead of launching every channel's provider
        # searches (and Playwright pages) in the same instant.
        await asyncio.sleep(initial_delay)
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
        # +/-10% jitter keeps channels that started together from re-scraping in
        # lockstep (a pool/CPU/Chromium spike every SCRAPE_REFRESH_SECONDS).
        delay = max(15.0, delay * random.uniform(0.9, 1.1))
        wake_event = _TEAM_SCRAPE_WAKE_EVENTS.get(team_id)
        if wake_event is None:
            return
        try:
            await asyncio.wait_for(wake_event.wait(), timeout=delay)
            wake_event.clear()
        except asyncio.TimeoutError:
            pass

PROVIDER_TIMEOUT_MIN = bounded_float(os.getenv("PROVIDER_TIMEOUT_MIN", "20"), 20.0, 5.0, 300.0)
PROVIDER_TIMEOUT_MAX = bounded_float(os.getenv("PROVIDER_TIMEOUT_MAX", "90"), 90.0, 10.0, 600.0)
PROVIDER_TIMEOUT_DEFAULT = bounded_float(os.getenv("PROVIDER_TIMEOUT_DEFAULT", "45"), 45.0, 5.0, 600.0)


def _dynamic_provider_timeout_sync(provider: str) -> float:
    # Only successful searches: failures were recorded as the timeout itself
    # (ratcheting a slow provider up to the maximum for good) or as 0 ms
    # (dragging a failing one down to the minimum).
    with _db_session() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT AVG(response_time_ms) FROM provider_performance "
            "WHERE provider=? AND success=1 AND timestamp > datetime('now', '-1 hour')",
            (provider,)
        )
        row = cursor.fetchone()
        if row and row[0]:
            avg_ms = row[0]
            return float(max(PROVIDER_TIMEOUT_MIN, min(PROVIDER_TIMEOUT_MAX, (avg_ms / 1000) * 3)))
    return PROVIDER_TIMEOUT_DEFAULT


async def _get_dynamic_provider_timeout(provider: str) -> float:
    """Calculate dynamic timeout based on provider's historical response times."""
    try:
        return await asyncio.to_thread(_dynamic_provider_timeout_sync, provider)
    except Exception as exc:
        _log_failure("calculate dynamic provider timeout", exc)
        return PROVIDER_TIMEOUT_DEFAULT


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

IDLE_HEALTH_INTERVAL = _positive_env_number("IDLE_HEALTH_INTERVAL", 30.0)
HEALTH_FAILURE_THRESHOLD = bounded_int(os.getenv("HEALTH_FAILURE_THRESHOLD", "2"), 2, 1, 10)
HEALTH_PROBE_TIMEOUT = _positive_env_number("HEALTH_PROBE_TIMEOUT", 12.0)
STANDBY_PROBES_PER_CHANNEL = bounded_int(os.getenv("STANDBY_PROBES_PER_CHANNEL", "5"), 5, 1, 50)
UNWATCHED_STANDBY_INTERVAL = _positive_env_number("UNWATCHED_STANDBY_INTERVAL", 300.0)
HEALTHY_RESCRAPE_SECONDS = _positive_env_number("HEALTHY_RESCRAPE_SECONDS", 1800.0)
STARTUP_SCRAPE_SPREAD_SECONDS = bounded_float(os.getenv("STARTUP_SCRAPE_SPREAD_SECONDS", "45"), 45.0, 0.0, 600.0)
# While every candidate is marked failed, keep probing them all (watched or
# not) and recover on the first that plays again.
EXHAUSTED_PROBE_INTERVAL = _positive_env_number("EXHAUSTED_PROBE_INTERVAL", 20.0)
# Outside the scheduled window a channel is only closed after it has not been
# flowing for this long (one missed sample used to end overtime games).
WINDOW_CLOSE_GRACE_SECONDS = _positive_env_number("WINDOW_CLOSE_GRACE_SECONDS", 180.0)
# A channel's only source stalls: retry it this many times before No Signal.
SELF_RETRY_LIMIT = bounded_int(os.getenv("SELF_RETRY_LIMIT", "2"), 2, 0, 10)
SELF_RETRY_WINDOW = _positive_env_number("SELF_RETRY_WINDOW", 120.0)
# Session-incompatible sources (fMP4, separate audio) get another chance later.
INCOMPATIBLE_RETRY_SECONDS = _positive_env_number("INCOMPATIBLE_RETRY_SECONDS", 3600.0)
FAILOVER_ALERT_COOLDOWN = _positive_env_number("FAILOVER_ALERT_COOLDOWN", 300.0)
TOKEN_REFRESH_COOLDOWN = _positive_env_number("TOKEN_REFRESH_COOLDOWN", 120.0)
# Failover candidate tiers: known-good within this long; failed longer ago than
# FAILED_RETRY_AFTER is worth another try.
CANDIDATE_GOOD_FOR_SECONDS = _positive_env_number("CANDIDATE_GOOD_FOR_SECONDS", 180.0)
CANDIDATE_FAILED_RETRY_AFTER = _positive_env_number("CANDIDATE_FAILED_RETRY_AFTER", 60.0)
_ACTIVE_PROBE_SEMAPHORE: Optional[asyncio.Semaphore] = None


def _candidate_tier(candidate: dict, now: float) -> int:
    """0 known-good recently, 1 unknown, 2 failed a while ago, 3 failed moments ago."""
    ok = candidate.get("last_health_ok")
    checked = float(candidate.get("last_health_check") or 0.0)
    if ok is True and now - checked < CANDIDATE_GOOD_FOR_SECONDS:
        return 0
    if ok is None or ok is True:
        return 1
    if now - checked > CANDIDATE_FAILED_RETRY_AFTER:
        return 2
    return 3


def _candidate_session_compatible(candidate: dict, now: float) -> bool:
    if candidate.get("session_compatible", True) is not False:
        return True
    return now - float(candidate.get("incompatible_at") or 0.0) > INCOMPATIBLE_RETRY_SECONDS


def _best_candidate_index(candidates: List[dict]) -> int:
    """Where a (re)built candidate list should start: the best-health usable
    source, list order breaking ties (the list is already ranked by quality)."""
    if not candidates:
        return 0
    now = time.time()
    ranked = sorted(
        range(len(candidates)),
        key=lambda i: (
            0 if _candidate_session_compatible(candidates[i], now) else 1,
            _candidate_tier(candidates[i], now),
            i,
        ),
    )
    return ranked[0]


def _pick_next_candidate(
    candidates: List[dict],
    current_index: int,
    prefer_signature: Optional[tuple] = None,
) -> Optional[int]:
    """Choose the failover target: the freshest known-good standby first, then
    never-checked ones, then ones that failed a while ago - wrapping around the
    list instead of giving up at the end (the old code only ever tried
    active_index + 1). Candidates the channel session found unplayable
    (fMP4 / separate audio) are skipped, and a same-codec source is preferred
    so Jellyfin's decoder doesn't have to switch codecs mid-stream."""
    count = len(candidates)
    if count < 2:
        return None
    now = time.time()
    order = [(current_index + step) % count for step in range(1, count)]

    def rank(index: int) -> Tuple[int, int, int]:
        candidate = candidates[index]
        tier = _candidate_tier(candidate, now)  # 3 = failed moments ago; not worth switching to
        signature = candidate.get("codec_signature")
        codec_penalty = 0 if prefer_signature is None or signature in (None, prefer_signature) else 1
        return tier, codec_penalty, order.index(index)

    eligible = [
        index for index in order
        if _candidate_session_compatible(candidates[index], now) and rank(index)[0] < 3
    ]
    return min(eligible, key=rank) if eligible else None


async def request_failover(team_id: str, source_key: tuple, reason: str, incompatible: bool = False) -> bool:
    """Single failover entry point for channel sessions (real playback) and
    health probes (unwatched channels). A no-op if the active candidate already
    changed, so concurrent reporters can't double-advance."""
    data = stream_state.get(team_id)
    if not data or data.get("type") == "multiview" or tuple(source_key) == PLACEHOLDER_SOURCE_KEY:
        return False
    candidates = data.get("candidates") or []
    if not candidates:
        return False
    active_index = data.get("active_index", 0)
    if active_index >= len(candidates) or candidate_source_key(candidates[active_index]) != tuple(source_key):
        return False

    active = candidates[active_index]
    active["last_health_ok"] = False
    active["last_health_check"] = time.time()
    if incompatible:
        active["session_compatible"] = False
        active["incompatible_at"] = time.time()
    team_name = data.get("name", team_id)
    if reason == "playlist forbidden":
        _request_token_refresh(team_id, data)
    next_index = _pick_next_candidate(candidates, active_index, active.get("codec_signature"))
    if next_index is None:
        if not incompatible and _allow_self_retry(data):
            # The only (usable) source stalled. A stall is often brief, so give
            # it another stale window instead of going straight to No Signal.
            LOGGER.info("Retrying the same source team=%s reason=%s", team_id, reason)
            return False
        data["is_healthy"] = False
        data["exhausted"] = True
        data["exhausted_since"] = time.monotonic()
        SESSIONS.poke(team_id)
        _handle_candidates_exhausted(team_id, data, team_name)
        return False

    data["active_index"] = next_index
    data["exhausted"] = False
    data["self_retries"] = 0
    # The new source starts with a clean failure count (a stale count left
    # from an earlier stint made its first failed probe fail over at once).
    candidates[next_index]["consecutive_failures"] = 0
    new_provider = candidates[next_index].get("provider", "Unknown")
    SESSIONS.poke(team_id)
    LOGGER.info(
        "Failover team=%s from=%s to=%s reason=%s",
        team_id, active.get("provider", "Unknown"), new_provider, reason,
    )
    await log_metric_event_async(team_id, new_provider, "failover", reason)
    data["failover_count"] = int(data.get("failover_count", 0)) + 1
    data["last_failover"] = {"at": time.time(), "reason": reason, "to": new_provider}
    _send_failover_alert(team_id, data, team_name, new_provider)
    return True


def _allow_self_retry(data: dict) -> bool:
    now = time.monotonic()
    if now - float(data.get("self_retry_window_start") or 0.0) > SELF_RETRY_WINDOW:
        data["self_retry_window_start"] = now
        data["self_retries"] = 0
    if int(data.get("self_retries", 0)) >= SELF_RETRY_LIMIT:
        return False
    data["self_retries"] = int(data.get("self_retries", 0)) + 1
    return True


def _send_failover_alert(team_id: str, data: dict, team_name: str, new_provider: str) -> None:
    """At most one failover alert per channel per FAILOVER_ALERT_COOLDOWN; a
    flapping channel used to send one every few seconds (and got the webhook
    rate-limited right before the more important 'exhausted' alert)."""
    now = time.monotonic()
    last = data.get("last_failover_alert")
    if last is not None and now - last < FAILOVER_ALERT_COOLDOWN:
        data["suppressed_failover_alerts"] = int(data.get("suppressed_failover_alerts", 0)) + 1
        return
    suppressed = int(data.get("suppressed_failover_alerts", 0))
    data["last_failover_alert"] = now
    data["suppressed_failover_alerts"] = 0
    extra = f" ({suppressed} more failover(s) since the last alert)" if suppressed else ""
    _spawn_background_task(
        send_alert("⚠️ Stream Failover", f"Failed over to {new_provider} for **{team_name}**.{extra}", "warning"),
        f"send failover alert team={team_id}",
    )


def _request_token_refresh(team_id: str, data: dict) -> None:
    """The active playlist answered 401/403: its token probably expired, and
    standbys from the same scrape carry tokens just as old. Rescrape now (the
    merge keeps health history) rather than waiting for the next cycle."""
    now = time.monotonic()
    if now - float(data.get("last_token_refresh") or 0.0) < TOKEN_REFRESH_COOLDOWN:
        return
    data["last_token_refresh"] = now
    LOGGER.info("Refreshing stream tokens team=%s", team_id)
    _spawn_background_task(_trigger_scrape(team_id, force=True), f"token refresh rescrape team={team_id}")


async def _probe_exhausted_candidates(team_id: str, data: dict) -> None:
    """Recover an exhausted channel as soon as any known source plays again,
    instead of waiting for a rescrape to return candidates."""
    try:
        candidates = list(data.get("candidates") or [])
        if not candidates:
            return
        semaphore = asyncio.Semaphore(STANDBY_HEALTH_CONCURRENCY)
        await asyncio.gather(
            *(_probe_standby_candidate(candidate, semaphore) for candidate in candidates),
            return_exceptions=True,
        )
        current = stream_state.get(team_id)
        if current is not data or not data.get("exhausted") or data.get("candidates") is None:
            return
        now = time.time()
        for index, candidate in enumerate(data["candidates"]):
            if candidate.get("last_health_ok") is True and _candidate_session_compatible(candidate, now):
                data["active_index"] = index
                data["exhausted"] = False
                data["is_healthy"] = True
                data["self_retries"] = 0
                candidate["consecutive_failures"] = 0
                SESSIONS.poke(team_id)
                LOGGER.info(
                    "Recovered exhausted channel team=%s provider=%s",
                    team_id, candidate.get("provider", "Unknown"),
                )
                return
    finally:
        data["exhausted_probe_in_flight"] = False


def _handle_candidates_exhausted(team_id: str, data: dict, team_name: str) -> None:
    current_time = time.monotonic()
    if current_time - data.get("last_exhausted_alert", 0.0) >= EMERGENCY_SCRAPE_COOLDOWN:
        data["last_exhausted_alert"] = current_time
        _spawn_background_task(
            send_alert("🚨 All Stream Candidates Exhausted", f"All candidates for **{team_name}** failed.", "danger"),
            f"send exhausted-stream alert team={team_id}",
        )
    if current_time - data.get("last_emergency_scrape", 0.0) >= EMERGENCY_SCRAPE_COOLDOWN:
        data["last_emergency_scrape"] = current_time
        _spawn_background_task(emergency_rescrape(team_id), f"emergency rescrape team={team_id}")


async def emergency_rescrape(team_id: str) -> None:
    data = stream_state.get(team_id)
    if data is None:
        return
    if team_id in _SCRAPE_IN_FLIGHT:
        LOGGER.debug("Skipping emergency scrape already in progress team=%s", team_id)
        return
    _SCRAPE_IN_FLIGHT.add(team_id)
    _mark_scrape_started(data)

    def _install_partial(streams: List[dict]) -> None:
        # First playable provider result while still off the air: use it now.
        current = stream_state.get(team_id)
        if current is None or not current.get("exhausted"):
            return
        merged, merged_index = _merge_stream_candidates(
            current.get("candidates", []), 0, streams, keep_active=False
        )
        current["candidates"] = merged
        current["active_index"] = merged_index
        current["exhausted"] = False
        current["is_healthy"] = True
        SESSIONS.poke(team_id)
        LOGGER.info("Emergency rescrape found streams early team=%s count=%d", team_id, len(streams))

    try:
        candidates = await master_scrape(
            data["query"],
            team_name=data.get("name", team_id),
            team_id=team_id,
            search_terms=data.get("search_terms") or None,
            always_live=bool(data.get("always_live")),
            on_partial=_install_partial,
        )
        current = stream_state.get(team_id)
        if current is not None:
            if candidates:
                # Every known candidate just failed, so nothing is kept active, but
                # standbys found again keep their health history. If an early
                # partial result is already playing, keep playing it.
                playing_early = not current.get("exhausted") and bool(current.get("candidates"))
                merged, merged_index = _merge_stream_candidates(
                    current.get("candidates", []), current.get("active_index", 0) if playing_early else 0,
                    candidates, keep_active=playing_early,
                )
                current["candidates"] = merged
                current["active_index"] = merged_index
                current["is_healthy"] = True
                current["exhausted"] = False
                current["last_candidate_refresh"] = time.time()
                SESSIONS.poke(team_id)
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


async def check_stream_health(url: str, referer: str, origin: str = "", probe_state: Optional[dict] = None) -> bool:
    owns_client = SHARED_HTTP_CLIENT is None
    client = SHARED_HTTP_CLIENT or httpx.AsyncClient(timeout=5.0, follow_redirects=True, http2=True)
    try:
        # Origin must be forwarded: streams captured with one (Playwright-intercepted
        # providers) play through the proxy, which sends it, but failed every probe
        # without it - causing endless false failovers.
        # probe_state (kept on the candidate) lets the probe notice a playlist
        # that stopped advancing between probes, not just one that is reachable.
        return await verify_stream_live(client, url, referer, origin=origin, probe_state=probe_state)
    except Exception as exc:
        _log_failure("stream health check", exc)
        return False
    finally:
        if owns_client:
            await client.aclose()


def _session_on_placeholder(session) -> bool:
    source = getattr(session, "source", None)
    return source is not None and bool(source.key) and source.key[0] == "placeholder"


def _session_playing_real_source(session) -> bool:
    """Flowing with real content: the No Signal placeholder also produces
    segments, but a channel showing it is not healthy."""
    return session is not None and session.is_flowing() and not _session_on_placeholder(session)


def _session_flowing(team_id: str) -> bool:
    session = SESSIONS.peek(team_id)
    return session is not None and session.is_watched() and _session_playing_real_source(session)


def _stream_window_should_close(team_id: str, data: dict) -> bool:
    """Outside the scheduled window, but keep a game that runs long (overtime,
    rain delay) while people are watching it and it is still flowing. Closing
    needs WINDOW_CLOSE_GRACE_SECONDS without real playback, not one sample."""
    if is_stream_window_active(data) or _session_flowing(team_id):
        data.pop("window_close_pending_since", None)
        return False
    now = time.monotonic()
    pending_since = data.setdefault("window_close_pending_since", now)
    return now - pending_since >= WINDOW_CLOSE_GRACE_SECONDS


async def _probe_active_candidate(team_id: str, data: dict) -> None:
    global _ACTIVE_PROBE_SEMAPHORE
    if _ACTIVE_PROBE_SEMAPHORE is None:
        _ACTIVE_PROBE_SEMAPHORE = asyncio.Semaphore(ACTIVE_HEALTH_CONCURRENCY)
    candidates = data.get("candidates") or []
    active_index = data.get("active_index", 0)
    if active_index >= len(candidates):
        return
    candidate = candidates[active_index]
    try:
        async with _ACTIVE_PROBE_SEMAPHORE:
            try:
                is_alive = await asyncio.wait_for(
                    check_stream_health(
                        candidate["url"], candidate.get("referer", ""), candidate.get("origin", ""),
                        probe_state=candidate.setdefault("probe_state", {}),
                    ),
                    timeout=HEALTH_PROBE_TIMEOUT,
                )
            except asyncio.TimeoutError:
                is_alive = False
        # The channel may have failed over (or started being watched) meanwhile.
        if data.get("candidates") is not candidates or data.get("active_index", 0) != active_index:
            return
        candidate["last_health_check"] = time.time()
        if is_alive:
            candidate["last_health_ok"] = True
            candidate["consecutive_failures"] = 0
            data["is_healthy"] = True
            return
        candidate["consecutive_failures"] = candidate.get("consecutive_failures", 0) + 1
        if candidate["consecutive_failures"] < HEALTH_FAILURE_THRESHOLD:
            # One failed probe is often a transient blip; re-check soon before acting.
            data["next_active_probe"] = time.monotonic() + max(ACTIVE_HEALTH_INTERVAL * 2, 5.0)
            return
        candidate["last_health_ok"] = False
        data["is_healthy"] = False
        await request_failover(team_id, candidate_source_key(candidate), "health probe failed")
    finally:
        data["probe_in_flight"] = False


async def _probe_standby_candidate(candidate: dict, semaphore: asyncio.Semaphore) -> None:
    async with semaphore:
        try:
            is_alive = await asyncio.wait_for(
                check_stream_health(
                    candidate["url"], candidate.get("referer", ""), candidate.get("origin", ""),
                    probe_state=candidate.setdefault("probe_state", {}),
                ),
                timeout=HEALTH_PROBE_TIMEOUT,
            )
        except asyncio.TimeoutError:
            is_alive = False
    candidate["last_health_check"] = time.time()
    candidate["last_health_ok"] = is_alive
    if is_alive:
        candidate["consecutive_failures"] = 0


async def standby_health_loop() -> None:
    """Keep standby health fresh so failover picks a working source first. Runs
    on its own so a slow sweep never delays active-stream failure detection.
    Watched channels' standbys are probed every STANDBY_HEALTH_INTERVAL, others
    every UNWATCHED_STANDBY_INTERVAL, at most STANDBY_PROBES_PER_CHANNEL each."""
    last_unwatched_sweep = 0.0
    while True:
        await asyncio.sleep(STANDBY_HEALTH_INTERVAL)
        try:
            now = time.monotonic()
            include_unwatched = now - last_unwatched_sweep >= UNWATCHED_STANDBY_INTERVAL
            if include_unwatched:
                last_unwatched_sweep = now
            semaphore = asyncio.Semaphore(STANDBY_HEALTH_CONCURRENCY)
            probes = []
            for team_id, data in list(stream_state.items()):
                if data.get("type") == "multiview" or not data.get("candidates"):
                    continue
                if not include_unwatched and not SESSIONS.is_watched(team_id):
                    continue
                active_index = data.get("active_index", 0)
                standbys = [c for i, c in enumerate(data["candidates"]) if i != active_index]
                for candidate in standbys[:STANDBY_PROBES_PER_CHANNEL]:
                    probes.append(_probe_standby_candidate(candidate, semaphore))
            if probes:
                await asyncio.gather(*probes, return_exceptions=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_failure("standby health sweep", exc, logging.ERROR)


def _failover_monitor_tick() -> None:
    now = time.monotonic()
    for team_id, data in list(stream_state.items()):
        if data.get("type") == "multiview":
            entry = _MULTIVIEW_PROCESSES.get(team_id)
            data["is_healthy"] = bool(entry and entry.get("ready") and not entry.get("exited"))
            continue
        if _stream_window_should_close(team_id, data):
            if data.get("candidates"):
                data["candidates"] = []
                data["active_index"] = 0
                data["is_healthy"] = False
                data["exhausted"] = False
            continue
        candidates = data.get("candidates")
        if not candidates:
            continue
        if data.get("active_index", 0) >= len(candidates):
            data["active_index"] = 0

        if data.get("exhausted"):
            data["is_healthy"] = False
            if not data.get("exhausted_probe_in_flight") and now >= data.get("next_exhausted_probe", 0.0):
                data["exhausted_probe_in_flight"] = True
                data["next_exhausted_probe"] = now + EXHAUSTED_PROBE_INTERVAL
                _spawn_background_task(_probe_exhausted_candidates(team_id, data), f"exhausted probe team={team_id}")
            continue

        session = SESSIONS.peek(team_id)
        if session is not None and session.is_watched():
            # Real playback is the health signal for watched channels: the
            # session reports stale playlists / failing segments itself, so no
            # synthetic probes (which used to fail over on a single blip).
            data["is_healthy"] = _session_playing_real_source(session)
            continue

        if data.get("probe_in_flight") or now < data.get("next_active_probe", 0.0):
            continue
        data["probe_in_flight"] = True
        data["next_active_probe"] = now + IDLE_HEALTH_INTERVAL
        _spawn_background_task(_probe_active_candidate(team_id, data), f"health probe team={team_id}")


async def failover_monitor():
    standby_task = asyncio.create_task(standby_health_loop(), name="standby health")
    try:
        while True:
            try:
                _failover_monitor_tick()
            except Exception as exc:
                _log_failure("failover monitor iteration", exc, logging.ERROR)
            await asyncio.sleep(ACTIVE_HEALTH_INTERVAL)
    finally:
        standby_task.cancel()
        await asyncio.gather(standby_task, return_exceptions=True)


def _install_loop_exception_filter() -> None:
    """Windows' Proactor event loop reports every client that drops its TCP
    connection abruptly (Jellyfin stopping a stream, ffmpeg reconnecting) as an
    unhandled ConnectionResetError traceback from _call_connection_lost. On a
    24/7 server that is pure log noise; keep every other loop error."""
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()

    def handler(event_loop, context):
        exc = context.get("exception")
        if isinstance(exc, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
            return
        if previous is not None:
            previous(event_loop, context)
        else:
            event_loop.default_exception_handler(context)

    loop.set_exception_handler(handler)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global SHARED_HTTP_CLIENT, MEDIA_HTTP_CLIENT, PLAYWRIGHT_CLIENT, SHARED_BROWSER, _PREFETCH_SEMAPHORE

    _install_loop_exception_filter()
    init_db()
    _METRIC_WRITER.start()
    # Before wiping the run dirs: their pid files identify ffmpeg left running
    # by a crashed previous instance (and those would keep the files locked).
    _kill_orphaned_ffmpeg()
    shutil.rmtree(MULTIVIEW_OUTPUT_ROOT, ignore_errors=True)
    MULTIVIEW_OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    shutil.rmtree(PLACEHOLDER_OUTPUT_DIR, ignore_errors=True)
    # Advanced settings saved from the dashboard override the env defaults.
    _load_tunable_overrides()
    global SHOW_OFFSEASON_CHANNELS
    SHOW_OFFSEASON_CHANNELS = get_setting("show_offseason_channels", "0") == "1"
    _spawn_background_task(update_check_loop(), "update check")
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
    # Separate pool for playback (playlists + segments): scrape bursts used to
    # exhaust the shared pool and make segment fetches hit PoolTimeout. Long
    # keepalive because segment polls are 2-6s apart (the 5s default expired
    # between polls, paying a new TCP+TLS handshake on most requests). Redirects
    # are followed manually so every hop is SSRF-validated.
    MEDIA_HTTP_CLIENT = httpx.AsyncClient(
        limits=httpx.Limits(max_keepalive_connections=64, max_connections=200, keepalive_expiry=60.0),
        timeout=httpx.Timeout(connect=5.0, read=10.0, write=10.0, pool=3.0),
        follow_redirects=False,
        http2=True,
    )
    SESSIONS.sessions.clear()
    await _check_ffmpeg_available()
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
        _start_team_scrape_loop(team_id, initial_delay=random.uniform(0.0, STARTUP_SCRAPE_SPREAD_SECONDS))

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
        member_team_ids = [str(t) for t in member_team_ids if str(t).strip()]
        if len(member_team_ids) not in (2, 4):
            LOGGER.warning("Skipping multiview channel with invalid member count: %s", channel_id)
            continue
        missing = [t for t in member_team_ids if t not in stream_state]
        if missing:
            # Keep the Multi-View: removed members render as a "No Signal" pane.
            LOGGER.warning("Multi-View %s references removed channels %s; showing No Signal for them", channel_id, missing)
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
    multiview_watchdog_task = asyncio.create_task(multiview_watchdog(), name="multiview watchdog")
    # Tracked background task: _cancel_background_tasks() stops it at shutdown.
    _spawn_background_task(multiview_output_sweeper(), "multiview output sweep")
    yield

    monitor_task.cancel()
    prune_task.cancel()
    schedule_task.cancel()
    multiview_idle_task.cancel()
    multiview_watchdog_task.cancel()
    scrape_tasks = list(_TEAM_SCRAPE_TASKS.values())
    for t in scrape_tasks:
        t.cancel()
    # Wait for tasks to safely wind down before closing shared clients.
    await asyncio.gather(
        monitor_task, prune_task, schedule_task, multiview_idle_task, multiview_watchdog_task, *scrape_tasks,
        return_exceptions=True,
    )
    _TEAM_SCRAPE_TASKS.clear()
    _TEAM_SCRAPE_WAKE_EVENTS.clear()
    _TEAM_STATE_LOCKS.clear()
    await SESSIONS.close_all()
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
    if MEDIA_HTTP_CLIENT:
        try:
            await MEDIA_HTTP_CLIENT.aclose()
        except Exception as exc:
            _log_failure("close media HTTP client", exc)
        MEDIA_HTTP_CLIENT = None
    SHARED_BROWSER = None
    PLAYWRIGHT_CLIENT = None
    _PREFETCH_SEMAPHORE = None
    stream_state.clear()

def _is_expected_slow_request(scope) -> bool:
    """A channel playlist's first request waits for the session to start (up to
    STREAM_STARTUP_TIMEOUT); that's normal, not worth a warning each time."""
    path = scope.get("path") or ""
    return path.endswith(".m3u8") and (path.startswith("/stream/") or path.startswith("/multiview/"))


class RequestDiagnosticsMiddleware:
    """Pure ASGI middleware: logs 5xx and slow responses, turns unhandled errors
    into a 500. Replaces @app.middleware("http") (BaseHTTPMiddleware), which
    pushed every streamed body chunk through an extra memory stream and task
    hop - measurable overhead on the segment relay path."""

    def __init__(self, asgi_app) -> None:
        self.app = asgi_app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        started = time.perf_counter()
        status_holder = {"status": None}

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                status_holder["status"] = message["status"]
                elapsed_ms = (time.perf_counter() - started) * 1000
                if message["status"] >= 500:
                    LOGGER.warning(
                        "HTTP failure method=%s path=%s status=%d duration_ms=%.0f",
                        scope.get("method"), scope.get("path"), message["status"], elapsed_ms,
                    )
                elif elapsed_ms >= 5000 and not _is_expected_slow_request(scope):
                    LOGGER.warning(
                        "Slow request method=%s path=%s status=%d duration_ms=%.0f",
                        scope.get("method"), scope.get("path"), message["status"], elapsed_ms,
                    )
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception as exc:
            if status_holder["status"] is not None:
                # Response already started (e.g. an upstream segment died
                # mid-body): re-raise so the server aborts the connection and
                # the client retries, instead of seeing a clean truncated body.
                raise
            elapsed_ms = (time.perf_counter() - started) * 1000
            _log_failure(f"HTTP {scope.get('method')} {scope.get('path')} ({elapsed_ms:.0f}ms)", exc, logging.ERROR)
            response = JSONResponse(status_code=500, content={"detail": "Internal server error"})
            await response(scope, receive, send)


_UNSAFE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
_DEFAULT_PORTS = {"http": "80", "https": "443"}


def _normalized_netloc(scheme: str, netloc: str) -> str:
    netloc = (netloc or "").strip().lower()
    default_port = _DEFAULT_PORTS.get((scheme or "").lower())
    if default_port and netloc.endswith(f":{default_port}") and not netloc.endswith("]"):
        netloc = netloc[: -(len(default_port) + 1)]
    return netloc


def _is_cross_site_write(method: str, headers: Dict[str, str], scheme: str) -> bool:
    """True for a state-changing request a browser sent from another site.

    Basic-auth credentials are replayed by the browser on cross-site form POSTs,
    so every dashboard write would otherwise be forgeable from any web page.
    Browsers always send Origin (or at least Referer) on such requests;
    non-browser clients (curl, scripts) send neither and aren't a CSRF vector."""
    if method not in _UNSAFE_METHODS:
        return False
    allowed = {
        _normalized_netloc(scheme, headers.get("host", "")),
        _normalized_netloc(scheme, headers.get("x-forwarded-host", "").split(",")[0]),
    } - {""}
    origin = headers.get("origin")
    source = origin if origin is not None else headers.get("referer")
    if source is None:
        return False
    if source.strip().lower() == "null":
        return True
    parsed = urllib.parse.urlsplit(source)
    if not parsed.netloc:
        return True
    return _normalized_netloc(parsed.scheme, parsed.netloc) not in allowed


class CsrfOriginMiddleware:
    """Pure ASGI: reject cross-site state-changing requests (see _is_cross_site_write)."""

    def __init__(self, asgi_app) -> None:
        self.app = asgi_app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope.get("method") in _UNSAFE_METHODS:
            headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers", [])}
            if _is_cross_site_write(scope["method"], headers, scope.get("scheme", "http")):
                LOGGER.warning("Rejected cross-site request method=%s path=%s", scope.get("method"), scope.get("path"))
                response = PlainTextResponse("Cross-site request rejected", status_code=403)
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)


app = FastAPI(title="Jellyfin Sports Proxy - Titan Engine", version=__version__, lifespan=lifespan)
app.add_middleware(CsrfOriginMiddleware)
app.add_middleware(RequestDiagnosticsMiddleware)


@app.get("/healthz")
async def healthz():
    """Unauthenticated liveness probe (Docker healthcheck, installer, tray)."""
    return {"status": "ok", "app": "jellyball", "version": __version__}


_UPSTREAM_REJECTION_LOGGED: "OrderedDict[Tuple[str, int], float]" = OrderedDict()


def _log_upstream_rejection(url: str, status_code: int) -> None:
    """Rate-limited: a dead source is polled every few seconds by its channel
    session and would otherwise write a warning line per poll."""
    host = urllib.parse.urlsplit(url).netloc
    key = (host, status_code)
    now = time.monotonic()
    if now - _UPSTREAM_REJECTION_LOGGED.get(key, 0.0) < 60.0:
        return
    _UPSTREAM_REJECTION_LOGGED[key] = now
    _UPSTREAM_REJECTION_LOGGED.move_to_end(key)
    while len(_UPSTREAM_REJECTION_LOGGED) > 256:
        _UPSTREAM_REJECTION_LOGGED.popitem(last=False)
    LOGGER.warning(
        "Upstream proxy resource rejected status=%s host=%s path=%s",
        status_code, host, urllib.parse.urlsplit(url).path,
    )


async def _fetch_upstream_body(
    client: httpx.AsyncClient,
    url: str,
    headers: Dict[str, str],
    max_bytes: int,
    timeout: Optional[httpx.Timeout] = None,
) -> Optional[tuple[int, str, str, bytes, Dict[str, str]]]:
    """GET a bounded upstream body, validating every redirect hop (SSRF guard)."""
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
                timeout=timeout or STREAM_REQUEST_TIMEOUT,
                follow_redirects=False,
            ) as response:
                if 300 <= response.status_code < 400:
                    location = response.headers.get("location")
                    if not location:
                        return None
                    current_url = urllib.parse.urljoin(str(response.url), location)
                    continue
                if response.status_code >= 400:
                    _log_upstream_rejection(str(response.url), response.status_code)
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
            _log_upstream_exception(safe_url, exc)
            return None
    return None


_UPSTREAM_EXCEPTION_LOGGED: "OrderedDict[Tuple[str, str], float]" = OrderedDict()


def _log_upstream_exception(url: str, exc: BaseException) -> None:
    """Once per (host, error type) per minute: a dead CDN is polled every few
    seconds by every session using it."""
    host = urllib.parse.urlsplit(url).netloc.lower()
    key = (host, type(exc).__name__)
    now = time.monotonic()
    if now - _UPSTREAM_EXCEPTION_LOGGED.get(key, -1e9) < 60.0:
        return
    _UPSTREAM_EXCEPTION_LOGGED[key] = now
    _UPSTREAM_EXCEPTION_LOGGED.move_to_end(key)
    while len(_UPSTREAM_EXCEPTION_LOGGED) > 256:
        _UPSTREAM_EXCEPTION_LOGGED.popitem(last=False)
    LOGGER.warning("Upstream fetch failed host=%s error=%s", host, type(exc).__name__)


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


def _load_relay_signing_key() -> bytes:
    """Key for signing legacy relay URLs, kept in the data dir so URLs handed to
    a player before a restart stay valid after it."""
    key_file = DATA_DIR / "relay-signing.key"
    try:
        key = key_file.read_bytes()
        if len(key) >= 32:
            return key[:32]
    except OSError:
        pass
    key = secrets.token_bytes(32)
    try:
        key_file.write_bytes(key)
    except OSError as exc:
        _log_failure("save relay signing key", exc)
    return key


_RELAY_SIGNING_KEY = _load_relay_signing_key()


def _relay_signature(url: str, ref: str = "", org: str = "") -> str:
    """/substream.m3u8, /chunk* and /resource fetch whatever URL they are given
    and must stay unauthenticated (Jellyfin's ffmpeg calls them), so they only
    serve URLs this server wrote into a playlist itself: without a signature
    they were an open fetch relay for anyone who could reach the port."""
    message = f"{url}\n{ref or ''}\n{org or ''}".encode("utf-8")
    return hmac.new(_RELAY_SIGNING_KEY, message, hashlib.sha256).hexdigest()[:32]


def _relay_signature_ok(url: str, ref: str, org: str, sig: str) -> bool:
    return bool(sig) and hmac.compare_digest(str(sig), _relay_signature(url, ref, org))


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
        if enc_origin:
            route = f"{route}&org={enc_origin}"
        return f"{route}&sig={_relay_signature(resolved_url, referer, origin)}"

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
    """The newest segments: a live player starts ~3 from the end, so warming
    the oldest ones (as this used to) fetched segments nobody asked for."""
    urls = extract_manifest_media_urls(manifest_text, target_url)
    limit = PREFETCH_CHUNK_COUNT if PREFETCH_CHUNK_COUNT > 0 else 5
    return urls[-limit:]


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


def _jellyfin_ffmpeg_path() -> Optional[str]:
    """Jellyfin's own ffmpeg build, when Jellyball runs on the Jellyfin server."""
    candidates: List[Path] = []
    if sys.platform == "win32":
        for root in (os.getenv("ProgramW6432"), os.getenv("ProgramFiles"), r"C:\Program Files"):
            if root:
                candidates.append(Path(root) / "Jellyfin" / "Server" / "ffmpeg.exe")
    else:
        candidates += [Path("/usr/lib/jellyfin-ffmpeg/ffmpeg"), Path("/usr/share/jellyfin-ffmpeg/ffmpeg")]
    for candidate in candidates:
        if candidate.is_file():
            return str(candidate)
    return None


# Multi-View's encoder must match the GPU driver. The bundled ffmpeg is a very
# recent build (its NVENC needs NVIDIA driver 610+); on a Jellyfin server the
# Jellyfin ffmpeg is built for whatever driver Jellyfin's own hardware
# transcoding already uses, so prefer it unless a path is configured.
MULTIVIEW_FFMPEG_PATH = (
    os.getenv("MULTIVIEW_FFMPEG_PATH")
    or (None if os.getenv("FFMPEG_PATH") else _jellyfin_ffmpeg_path())
    or FFMPEG_PATH
)
MULTIVIEW_FFMPEG_VERSION_INFO = ""
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


def _env_choice(name: str, default: str, allowed) -> str:
    """A string setting limited to known values: an unknown ffmpeg preset or
    tune would make every single Multi-View spawn fail."""
    value = (os.getenv(name) or "").strip().lower()
    if not value:
        return default
    if value not in allowed:
        LOGGER.warning("Ignoring %s=%r (expected one of %s); using %r", name, value, ", ".join(sorted(allowed)), default)
        return default
    return value


# Encoder tuning. The GOP is FPS x segment length and keyframes are forced on
# the segment grid (see _multiview_video_encoder_args), so any combination
# keeps every per-audio output cutting at the same instants.
MULTIVIEW_FPS = bounded_int(os.getenv("MULTIVIEW_FPS", "30"), 30, 10, 60)
MULTIVIEW_HLS_LIST_SIZE = bounded_int(os.getenv("MULTIVIEW_HLS_LIST_SIZE", "8"), 8, 3, 30)
MULTIVIEW_NVENC_PRESET = _env_choice(
    "MULTIVIEW_NVENC_PRESET", "p4",
    {"p1", "p2", "p3", "p4", "p5", "p6", "p7", "default", "slow", "medium", "fast", "hp", "hq", "bd", "ll", "llhq", "llhp"},
)
MULTIVIEW_NVENC_TUNE = _env_choice("MULTIVIEW_NVENC_TUNE", "ll", {"hq", "ll", "ull", "lossless"})
# After a GPU encoder failure new runs are software-encoded for this long,
# then the GPU is tried again (the failure is often transient: every NVENC
# session taken by Jellyfin's own transcodes, a driver update, ...).
NVENC_FALLBACK_SECONDS = bounded_float(os.getenv("NVENC_FALLBACK_SECONDS", "600"), 600.0, 30.0, 86400.0)
# Spawn-failure backoff (and the step/cap of the restart backoff below).
MULTIVIEW_BACKOFF_BASE_SECONDS = bounded_float(os.getenv("MULTIVIEW_BACKOFF_BASE_SECONDS", "15"), 15.0, 1.0, 600.0)
MULTIVIEW_BACKOFF_MAX_SECONDS = max(
    MULTIVIEW_BACKOFF_BASE_SECONDS,
    bounded_float(os.getenv("MULTIVIEW_BACKOFF_MAX_SECONDS", "300"), 300.0, 1.0, 3600.0),
)
# Watchdog/view-failure restarts: the first MULTIVIEW_RESTART_BURST inside a
# rolling window are immediate; each further one waits exponentially longer.
MULTIVIEW_RESTART_WINDOW_SECONDS = bounded_float(os.getenv("MULTIVIEW_RESTART_WINDOW_SECONDS", "600"), 600.0, 60.0, 86400.0)
MULTIVIEW_RESTART_BURST = bounded_int(os.getenv("MULTIVIEW_RESTART_BURST", "2"), 2, 0, 20)
# A refused start (concurrency cap reached / ffmpeg unavailable) is retried
# after a short delay that doubles per consecutive refusal, up to a minute.
MULTIVIEW_REFUSAL_BACKOFF_SECONDS = bounded_float(os.getenv("MULTIVIEW_REFUSAL_BACKOFF_SECONDS", "5"), 5.0, 1.0, 300.0)
MULTIVIEW_REFUSAL_BACKOFF_MAX_SECONDS = max(60.0, MULTIVIEW_REFUSAL_BACKOFF_SECONDS)
# How often leftover run directories (~100MB of segments each) are swept.
MULTIVIEW_SWEEP_INTERVAL = bounded_float(os.getenv("MULTIVIEW_SWEEP_INTERVAL", "600"), 600.0, 60.0, 86400.0)

MULTIVIEW_LAYOUTS = {
    "side_by_side_2": {"count": 2, "pane_w": 960, "pane_h": 1080, "xstack": "0_0|w0_0"},
    "grid_2x2": {"count": 4, "pane_w": 960, "pane_h": 540, "xstack": "0_0|w0_0|0_h0|w0_h0"},
}

FFMPEG_AVAILABLE = False
FFMPEG_VERSION_INFO = ""

_MULTIVIEW_PROCESSES: Dict[str, dict] = {}
# Tracks recent spawn failures per channel so a client that keeps retrying a
# permanently-broken Multi-View (e.g. one member currently has no live
# candidates, so ffmpeg's -i for it 404s and the whole process aborts) can't
# force a new ffmpeg spawn attempt on every single request. Real players
# (Jellyfin's tuner among them) retry a failed live-TV stream on their own
# schedule regardless of what we return, so this backoff is the only thing
# standing between "one dead input" and continuous CPU/GPU churn.
_MULTIVIEW_FAILURES: Dict[str, dict] = {}
# Short "don't start before" holds on top of the failure backoff, for refused
# starts and backed-off restarts: {"until": monotonic, "kind": str, "reason": str}.
_MULTIVIEW_HOLDS: Dict[str, dict] = {}
# Consecutive refused starts per channel (sizes the refusal hold).
_MULTIVIEW_REFUSALS: Dict[str, int] = {}
# Rolling restart history: {"times": deque of monotonic, "last_alert": Optional[float]}.
# Unlike _MULTIVIEW_FAILURES it survives successful spawns, so a run that
# starts fine and then stalls every minute still backs off.
_MULTIVIEW_RESTARTS: Dict[str, dict] = {}
# Multi-Views an admin stopped (monotonic time of the Stop). While set, nothing
# auto-starts the grid and its viewers get the "No Signal" placeholder.
# Cleared by an explicit play: the next NEW viewer session (a playlist or
# segment request for this Multi-View while none of its view sessions is
# running, i.e. a fresh tune) or POST /multiview/{id}/start. A viewer who was
# already watching when Stop was pressed keeps getting No Signal until their
# session idles out (SESSION_IDLE_SECONDS after they stop pulling), so the
# player that happened to be open can't silently undo the Stop.
_MULTIVIEW_MANUAL_STOPS: Dict[str, float] = {}
# Channels holding a concurrency slot while their spawn warms members and waits
# for first segments. Reserved before the (up to 20s) warm-up, so simultaneous
# starts can't all pass MAX_CONCURRENT_MULTIVIEW; released when the spawn ends
# (a successful run keeps the slot through its _MULTIVIEW_PROCESSES entry).
_MULTIVIEW_SLOT_RESERVATIONS: Set[str] = set()
# Run directories created by a spawn but not registered yet (the sweep skips them).
_MULTIVIEW_PENDING_RUN_DIRS: Set[Path] = set()
_MULTIVIEW_RUN_COUNTER = 0
# At most one start task per channel; this (not a lock) serializes spawns.
_MULTIVIEW_START_TASKS: Dict[str, asyncio.Task] = {}
_MULTIVIEW_LAST_VIEWER: Dict[str, float] = {}


def _multiview_backoff_seconds(failure_count: int) -> float:
    return min(MULTIVIEW_BACKOFF_MAX_SECONDS, MULTIVIEW_BACKOFF_BASE_SECONDS * (2 ** max(0, failure_count - 1)))


def _multiview_hold_remaining(channel_id: str) -> float:
    hold = _MULTIVIEW_HOLDS.get(channel_id)
    if not hold:
        return 0.0
    remaining = hold["until"] - time.monotonic()
    if remaining <= 0:
        _MULTIVIEW_HOLDS.pop(channel_id, None)
        return 0.0
    return remaining


def _multiview_cooldown_remaining(channel_id: str) -> float:
    """Seconds before this Multi-View may be started again (failure backoff,
    refusal hold or restart backoff, whichever ends last)."""
    record = _MULTIVIEW_FAILURES.get(channel_id)
    failure_remaining = 0.0
    if record:
        elapsed = time.monotonic() - record["last_failure"]
        failure_remaining = max(0.0, _multiview_backoff_seconds(record["count"]) - elapsed)
    return max(failure_remaining, _multiview_hold_remaining(channel_id))


def _set_multiview_hold(channel_id: str, seconds: float, kind: str, reason: str) -> None:
    until = time.monotonic() + seconds
    current = _MULTIVIEW_HOLDS.get(channel_id)
    if current is None or current["until"] < until:
        _MULTIVIEW_HOLDS[channel_id] = {"until": until, "kind": kind, "reason": reason}


def _record_multiview_failure(channel_id: str, error: str) -> None:
    record = _MULTIVIEW_FAILURES.setdefault(channel_id, {"count": 0, "last_failure": 0.0, "last_error": ""})
    record["count"] += 1
    record["last_failure"] = time.monotonic()
    record["last_error"] = error


def _clear_multiview_failure(channel_id: str) -> None:
    _MULTIVIEW_FAILURES.pop(channel_id, None)


def _record_multiview_refusal(channel_id: str, reason: str) -> float:
    """A start refused before anything was launched. Viewers get the
    placeholder meanwhile; nothing retries the spawn until the hold expires,
    so this logs once per hold instead of on every 0.5s session poll."""
    count = _MULTIVIEW_REFUSALS.get(channel_id, 0) + 1
    _MULTIVIEW_REFUSALS[channel_id] = count
    delay = min(MULTIVIEW_REFUSAL_BACKOFF_MAX_SECONDS, MULTIVIEW_REFUSAL_BACKOFF_SECONDS * (2 ** (count - 1)))
    _set_multiview_hold(channel_id, delay, "refused", reason)
    LOGGER.warning(
        "Refusing to start multiview channel=%s: %s; next attempt in %.0fs (viewers see No Signal)",
        channel_id, reason, delay,
    )
    return delay


def _multiview_restart_delay(restart_count: int) -> float:
    """Hold before the Nth restart inside the rolling window may start."""
    excess = restart_count - MULTIVIEW_RESTART_BURST
    if excess <= 0:
        return 0.0
    return min(MULTIVIEW_BACKOFF_MAX_SECONDS, MULTIVIEW_BACKOFF_BASE_SECONDS * (2 ** (excess - 1)))


def _recent_multiview_restarts(channel_id: str, now: Optional[float] = None) -> int:
    record = _MULTIVIEW_RESTARTS.get(channel_id)
    if not record:
        return 0
    now = time.monotonic() if now is None else now
    times = record["times"]
    while times and now - times[0] > MULTIVIEW_RESTART_WINDOW_SECONDS:
        times.popleft()
    return len(times)


def _note_multiview_restart(channel_id: str, reason: str) -> Tuple[int, float]:
    """Count a restart in the rolling window; returns (restarts in window, hold
    before the next start). Sends one alert when the backoff kicks in, then at
    most one per window while it stays engaged."""
    now = time.monotonic()
    record = _MULTIVIEW_RESTARTS.setdefault(channel_id, {"times": deque(), "last_alert": None})
    _recent_multiview_restarts(channel_id, now)
    record["times"].append(now)
    count = len(record["times"])
    delay = _multiview_restart_delay(count)
    if delay > 0:
        _set_multiview_hold(channel_id, delay, "restart", reason)
        last_alert = record["last_alert"]
        if last_alert is None or now - last_alert >= MULTIVIEW_RESTART_WINDOW_SECONDS:
            record["last_alert"] = now
            name = (stream_state.get(channel_id) or {}).get("name", channel_id)
            _spawn_background_task(
                send_alert(
                    "⚠️ Multi-View Unstable",
                    f"**{name}** restarted {count} times in {MULTIVIEW_RESTART_WINDOW_SECONDS / 60:.0f} min "
                    f"(last: {reason}). Restarts now back off; next start in {delay:.0f}s.",
                    "warning",
                ),
                f"send multiview restart alert channel={channel_id}",
            )
    return count, delay


def _forget_multiview_channel(channel_id: str) -> None:
    """Drop every per-channel Multi-View bookkeeping entry (channel removed)."""
    for mapping in (
        _MULTIVIEW_FAILURES, _MULTIVIEW_HOLDS, _MULTIVIEW_REFUSALS, _MULTIVIEW_RESTARTS,
        _MULTIVIEW_MANUAL_STOPS, _MULTIVIEW_LAST_VIEWER, _MULTIVIEW_START_TASKS,
    ):
        mapping.pop(channel_id, None)
    _MULTIVIEW_SLOT_RESERVATIONS.discard(channel_id)


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


async def _probe_ffmpeg_version(path: str, timeout: float = 5.0) -> Tuple[Optional[int], str]:
    """Run `<path> -version`; returns (exit code, first output line). A binary
    that hangs (e.g. on an unreachable network path) is killed on timeout
    instead of being left running for the life of the service."""
    process = await asyncio.create_subprocess_exec(
        path, "-version",
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        creationflags=_child_process_creationflags(),
    )
    try:
        stdout, _ = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        if process.returncode is None:
            try:
                process.kill()
            except (ProcessLookupError, OSError):
                pass
            try:
                await asyncio.wait_for(process.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
        raise
    first_line = stdout.decode(errors="replace").splitlines()[0] if stdout else ""
    return process.returncode, first_line


async def _check_ffmpeg_available() -> None:
    """Probe FFMPEG_PATH once at startup so failures surface as a clear log/dashboard warning."""
    global FFMPEG_AVAILABLE, FFMPEG_VERSION_INFO
    try:
        returncode, FFMPEG_VERSION_INFO = await _probe_ffmpeg_version(FFMPEG_PATH)
        FFMPEG_AVAILABLE = returncode == 0
    except (FileNotFoundError, OSError, asyncio.TimeoutError) as exc:
        FFMPEG_AVAILABLE = False
        FFMPEG_VERSION_INFO = ""
        _log_failure("locate ffmpeg for multiview", exc, logging.WARNING)
    if not FFMPEG_AVAILABLE:
        LOGGER.warning("ffmpeg not found at FFMPEG_PATH=%r; Multi-View channels unavailable", FFMPEG_PATH)
    await _check_multiview_ffmpeg()


async def _check_multiview_ffmpeg() -> None:
    global MULTIVIEW_FFMPEG_PATH, MULTIVIEW_FFMPEG_VERSION_INFO
    if MULTIVIEW_FFMPEG_PATH != FFMPEG_PATH:
        try:
            returncode, first_line = await _probe_ffmpeg_version(MULTIVIEW_FFMPEG_PATH)
            if returncode == 0 and first_line:
                MULTIVIEW_FFMPEG_VERSION_INFO = first_line
            else:
                raise OSError(f"exit code {returncode}")
        except (FileNotFoundError, OSError, asyncio.TimeoutError) as exc:
            _log_failure(f"probe Multi-View ffmpeg {MULTIVIEW_FFMPEG_PATH!r}; using {FFMPEG_PATH!r}", exc)
            MULTIVIEW_FFMPEG_PATH = FFMPEG_PATH
    if MULTIVIEW_FFMPEG_PATH == FFMPEG_PATH:
        MULTIVIEW_FFMPEG_VERSION_INFO = FFMPEG_VERSION_INFO
    if MULTIVIEW_FFMPEG_VERSION_INFO:
        LOGGER.info("Multi-View ffmpeg: %s (%s)", MULTIVIEW_FFMPEG_VERSION_INFO, MULTIVIEW_FFMPEG_PATH)


def _multiview_bufsize(bitrate: str) -> str:
    """Double a ffmpeg bitrate string (e.g. "6M" -> "12M", "6000k" -> "12000k") for -bufsize."""
    match = re.match(r"^(\d+(?:\.\d+)?)([A-Za-z]*)$", bitrate.strip())
    if not match:
        return bitrate
    value, unit = match.groups()
    doubled = float(value) * 2
    doubled_str = str(int(doubled)) if doubled == int(doubled) else str(doubled)
    return f"{doubled_str}{unit}"


def _multiview_video_encoder_args(encoder: Optional[str] = None) -> List[str]:
    """Encoder + rate-control flags. Kept per-backend because several of these
    (e.g. -forced-idr, -sc_threshold) are private AVOptions that only exist on
    some encoders and make ffmpeg fail outright with "Unrecognized option" on
    others (notably h264_qsv), so they must not be shared unconditionally.

    Keyframes are forced on the segment grid (fps is pinned to MULTIVIEW_FPS in
    the filter graph), so every HLS output of the tee cuts at the same instants
    and segments line up across the per-audio outputs."""
    encoder = (encoder or MULTIVIEW_HWACCEL).lower()
    gop = MULTIVIEW_FPS * MULTIVIEW_SEGMENT_SECONDS
    common_rate_args = [
        "-b:v", MULTIVIEW_BITRATE,
        "-maxrate", MULTIVIEW_BITRATE,
        "-bufsize", _multiview_bufsize(MULTIVIEW_BITRATE),
        "-g", str(gop),
        "-keyint_min", str(gop),
        "-bf", "0",
        "-pix_fmt", "yuv420p",
        "-force_key_frames", f"expr:gte(t,n_forced*{MULTIVIEW_SEGMENT_SECONDS})",
    ]
    if encoder == "nvenc":
        return [
            "-c:v", "h264_nvenc", "-preset", MULTIVIEW_NVENC_PRESET, "-tune", MULTIVIEW_NVENC_TUNE, "-rc", "cbr",
            *common_rate_args,
            "-forced-idr", "1",
        ]
    if encoder == "qsv":
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
    """Return the video part of -filter_complex: scale/pad each input and stack
    them into one grid, pinned to MULTIVIEW_FPS (xstack otherwise emits a frame
    whenever *any* input has one, so mixed 30/60fps members made the output rate
    irregular and broke the GOP-to-segment alignment)."""
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
    parts.append(f"{stack_inputs}xstack=inputs={member_count}:layout={spec['xstack']},fps={MULTIVIEW_FPS}[vout]")
    return ";".join(parts)


def _build_multiview_audio_filter(audio_presence: List[bool]) -> str:
    """Silent stand-in tracks [aN] for members without audio (so every
    per-audio output always has a track). Members that do have audio are
    stream-copied, not filtered: decoding + re-encoding the members' live audio
    made ffmpeg's scheduler throttle every input (measured ~0.27x real time
    with two live inputs, vs 1.2x when the audio is copied)."""
    return ";".join(
        f"anullsrc=r=48000:cl=stereo[a{idx}]"
        for idx, has_audio in enumerate(audio_presence)
        if not has_audio
    )


def _multiview_tee_outputs(member_count: int) -> str:
    """One muxed HLS output per member audio, all fed by the same encoded video.

    Escaping is exact on purpose: inside the tee spec, the select value must be
    written as select=\\'v:0,a:N\\' (a literal backslash before each quote in the
    argv element) or ffmpeg rejects it. onfail=ignore keeps one broken output
    from killing the others (the watchdog restarts the run if one stalls). No
    temp_file flag: on Windows its rename-over fails whenever we have the
    playlist open for reading, which would drop that output."""
    slaves = []
    for idx in range(member_count):
        options = ":".join([
            "f=hls",
            f"select=\\'v:0,a:{idx}\\'",
            "onfail=ignore",
            f"hls_time={MULTIVIEW_SEGMENT_SECONDS}",
            f"hls_list_size={MULTIVIEW_HLS_LIST_SIZE}",
            "hls_flags=delete_segments+independent_segments",
            "hls_segment_type=mpegts",
            f"hls_segment_filename=a{idx}/seg_%06d.ts",
        ])
        slaves.append(f"[{options}]a{idx}/index.m3u8")
    return "|".join(slaves)


def _internal_base_url() -> str:
    """Base URL ffmpeg uses to read our own member sessions."""
    host = (os.getenv("JELLYBALL_HOST") or "127.0.0.1").strip()
    if host in ("", "0.0.0.0", "::", "[::]", "localhost"):
        host = "127.0.0.1"
    return f"http://{host}:{PORT}"


def _multiview_input_url(team_id: str) -> str:
    if team_id not in stream_state:
        # A member that was deleted shows the "No Signal" pane instead of
        # breaking the whole Multi-View.
        team_id = PLACEHOLDER_SESSION_ID
    return f"{_internal_base_url()}/stream/{team_id}.m3u8"


def _build_multiview_ffmpeg_args(
    channel_id: str,
    data: dict,
    out_dir: Path,
    audio_presence: Optional[List[Optional[bool]]] = None,
    encoder: Optional[str] = None,
    *,
    hw_decode: Optional[bool] = None,
    input_ids: Optional[List[str]] = None,
) -> List[str]:
    """Pure command-builder (no I/O) so it can be unit tested without spawning ffmpeg.

    Inputs are the members' channel sessions, which never 502, follow failover,
    and are normalized (fixed PIDs, continuous timestamps), so one member's
    provider switching no longer freezes its pane or the whole grid. The run
    writes one HLS output per member audio under a{N}/ (relative to cwd=out_dir).

    `audio_presence[i]` must be True for member i's audio to be mapped: None
    (unknown) gets the silent track like False, because mapping a missing
    N:a:0 makes ffmpeg fail the whole run. `input_ids[i]` is the channel
    session that feeds pane i (default: the member itself; the placeholder for
    a member that wasn't ready in time). `hw_decode` (default: encoder is
    nvenc) adds -hwaccel cuda per input."""
    member_team_ids: List[str] = data["member_team_ids"]
    layout = data.get("layout", "grid_2x2")
    encoder = (encoder or MULTIVIEW_HWACCEL).lower()
    if hw_decode is None:
        hw_decode = encoder == "nvenc"
    audio_presence = (
        [has_audio is True for has_audio in audio_presence] if audio_presence else [True] * len(member_team_ids)
    )
    sources = list(input_ids) if input_ids else list(member_team_ids)

    loglevel = os.getenv("MULTIVIEW_FFMPEG_LOGLEVEL", "warning").strip() or "warning"
    args: List[str] = ["-y", "-hide_banner", "-loglevel", loglevel]
    # Progress lines only when debugging with a verbose level.
    args += ["-stats", "-stats_period", "5"] if loglevel in ("info", "verbose", "debug") else ["-nostats"]
    for source_id in sources:
        if hw_decode:
            # Decode on the GPU as well (frames are downloaded for the CPU
            # scale/pad/xstack filters). ffmpeg falls back to software per
            # stream when NVDEC can't handle a codec/profile, but a CUDA
            # *device* failure (no/broken driver) is fatal for the whole run;
            # _start_multiview_run then retries without -hwaccel.
            args += ["-hwaccel", "cuda"]
        args += [
            # Our own normalized sessions (one program, fixed PIDs): a short
            # probe is plenty, and it opens each input much faster than the
            # 5MB/5s default when four members open one after another.
            "-probesize", "1000000",
            "-analyzeduration", "1000000",
            "-reconnect", "1",
            "-reconnect_streamed", "1",
            "-reconnect_on_network_error", "1",
            "-reconnect_delay_max", "5",
            "-rw_timeout", "15000000",
            "-thread_queue_size", "1024",
            "-i", _multiview_input_url(source_id),
        ]

    filter_graph = ";".join(part for part in (
        _build_xstack_filter(layout, len(member_team_ids)),
        _build_multiview_audio_filter(audio_presence),
    ) if part)
    args += ["-filter_complex", filter_graph]
    args += ["-map", "[vout]"]
    for idx, has_audio in enumerate(audio_presence):
        args += ["-map", f"{idx}:a:0" if has_audio else f"[a{idx}]"]
    args += _multiview_video_encoder_args(encoder)
    # Copy member audio as-is; only the generated silent tracks are encoded.
    args += ["-c:a", "copy"]
    for idx, has_audio in enumerate(audio_presence):
        if not has_audio:
            args += [f"-c:a:{idx}", "aac", f"-b:a:{idx}", "96k"]
    # Don't let one briefly starved audio member hold every output for the
    # default 10s interleave window.
    args += ["-max_interleave_delta", "2000000"]
    args += ["-f", "tee", _multiview_tee_outputs(len(member_team_ids))]
    return args


def _child_process_creationflags() -> int:
    """No console window per ffmpeg/taskkill child in the windowed tray exe."""
    if sys.platform == "win32":
        return getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)
    return 0


def _ffmpeg_creationflags() -> int:
    """Multi-View / placeholder ffmpeg: no console window, and below-normal
    priority so a CPU (libx264) fallback encode can't starve Jellyfin's own
    transcodes or this server's request handling."""
    flags = _child_process_creationflags()
    if sys.platform == "win32":
        flags |= getattr(subprocess, "BELOW_NORMAL_PRIORITY_CLASS", 0x00004000)
    return flags


def _multiview_popen_kwargs(out_dir: Path) -> dict:
    kwargs = {
        "cwd": str(out_dir),
        "stdin": asyncio.subprocess.DEVNULL,
        "stdout": asyncio.subprocess.PIPE,
        "stderr": asyncio.subprocess.STDOUT,
    }
    if sys.platform == "win32":
        kwargs["creationflags"] = _ffmpeg_creationflags()
    return kwargs


async def _drain_multiview_log(channel_id: str, process: "asyncio.subprocess.Process", log_lines: "deque[str]") -> None:
    """Continuously read ffmpeg's combined stdout/stderr pipe into a rolling
    buffer. This is not optional: an unread PIPE fills its OS buffer (~64KB) and
    blocks ffmpeg forever. Read in chunks rather than readline(): ffmpeg's
    progress output ends in '\\r', and readline() raises once 64KB arrive with no
    '\\n', which used to kill this task and then hang ffmpeg."""
    pending = b""
    try:
        while True:
            chunk = await process.stdout.read(8192)
            if not chunk:
                break
            pending += chunk
            *lines, pending = re.split(rb"[\r\n]+", pending)
            for line in lines:
                if line.strip():
                    log_lines.append(line.decode(errors="replace").rstrip())
            if len(pending) > 16384:
                log_lines.append(pending[-1024:].decode(errors="replace"))
                pending = b""
        if pending.strip():
            log_lines.append(pending.decode(errors="replace").rstrip())
    except asyncio.CancelledError:
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
            log_tail = "\n".join(list(entry.get("log_lines", []))[-25:])
            LOGGER.warning("multiview ffmpeg exited unexpectedly channel=%s code=%s\n%s", channel_id, returncode, log_tail)


class _Win32ProcessApi:
    """The kernel32 calls used for ffmpeg process control, with explicit
    prototypes. Without restype=HANDLE ctypes returns a C int, which truncates
    64-bit handles; a private WinDLL instance keeps these prototypes from
    clashing with any other ctypes user of kernel32."""

    PROCESS_TERMINATE = 0x0001
    PROCESS_SET_QUOTA = 0x0100
    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    SYNCHRONIZE = 0x00100000
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000
    JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS = 9

    def __init__(self) -> None:
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

        self.ctypes = ctypes
        self.wintypes = wintypes
        self.extended_limit_info = _JOBOBJECT_EXTENDED_LIMIT_INFORMATION
        k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        HANDLE, BOOL, DWORD, UINT = wintypes.HANDLE, wintypes.BOOL, wintypes.DWORD, wintypes.UINT
        PFILETIME = ctypes.POINTER(wintypes.FILETIME)
        prototypes = {
            "CreateJobObjectW": (HANDLE, [wintypes.LPVOID, wintypes.LPCWSTR]),
            "SetInformationJobObject": (BOOL, [HANDLE, ctypes.c_int, wintypes.LPVOID, DWORD]),
            "AssignProcessToJobObject": (BOOL, [HANDLE, HANDLE]),
            "TerminateJobObject": (BOOL, [HANDLE, UINT]),
            "OpenProcess": (HANDLE, [DWORD, BOOL, DWORD]),
            "TerminateProcess": (BOOL, [HANDLE, UINT]),
            "GetProcessTimes": (BOOL, [HANDLE, PFILETIME, PFILETIME, PFILETIME, PFILETIME]),
            "WaitForSingleObject": (DWORD, [HANDLE, DWORD]),
            "CloseHandle": (BOOL, [HANDLE]),
        }
        for name, (restype, argtypes) in prototypes.items():
            function = getattr(k32, name)
            function.restype = restype
            function.argtypes = argtypes
        self.k32 = k32

    def close(self, handle) -> None:
        if handle:
            self.k32.CloseHandle(handle)

    def create_kill_on_close_job(self):
        h_job = self.k32.CreateJobObjectW(None, None)
        if not h_job:
            return None
        info = self.extended_limit_info()
        info.BasicLimitInformation.LimitFlags = self.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if not self.k32.SetInformationJobObject(
            h_job, self.JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS,
            self.ctypes.byref(info), self.ctypes.sizeof(info),
        ):
            self.close(h_job)
            return None
        return h_job

    def assign_pid(self, h_job, pid: int) -> bool:
        h_process = self.k32.OpenProcess(self.PROCESS_TERMINATE | self.PROCESS_SET_QUOTA, False, pid)
        if not h_process:
            return False
        try:
            return bool(self.k32.AssignProcessToJobObject(h_job, h_process))
        finally:
            self.close(h_process)

    def terminate_job(self, h_job) -> bool:
        return bool(self.k32.TerminateJobObject(h_job, 1))

    def _creation_time(self, h_process) -> Optional[int]:
        times = [self.wintypes.FILETIME() for _ in range(4)]
        if not self.k32.GetProcessTimes(h_process, *(self.ctypes.byref(t) for t in times)):
            return None
        return (times[0].dwHighDateTime << 32) | times[0].dwLowDateTime

    def process_creation_time(self, pid: int) -> Optional[int]:
        h_process = self.k32.OpenProcess(self.PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not h_process:
            return None
        try:
            return self._creation_time(h_process)
        finally:
            self.close(h_process)

    def terminate_pid_if_created_at(self, pid: int, created: int, wait_ms: int = 3000) -> bool:
        """Kill `pid` only if it is still the process created at `created`
        (a FILETIME): a bare pid may have been reused by anything since."""
        access = self.PROCESS_TERMINATE | self.PROCESS_QUERY_LIMITED_INFORMATION | self.SYNCHRONIZE
        h_process = self.k32.OpenProcess(access, False, pid)
        if not h_process:
            return False
        try:
            if self._creation_time(h_process) != created:
                return False
            if not self.k32.TerminateProcess(h_process, 1):
                return False
            self.k32.WaitForSingleObject(h_process, wait_ms)
            return True
        finally:
            self.close(h_process)


_WIN32_PROCESS_API = None  # None = not loaded yet, False = unavailable


def _win32_process_api() -> Optional[_Win32ProcessApi]:
    global _WIN32_PROCESS_API
    if sys.platform != "win32":
        return None
    if _WIN32_PROCESS_API is None:
        try:
            _WIN32_PROCESS_API = _Win32ProcessApi()
        except Exception as exc:
            _log_failure("load kernel32 process API", exc)
            _WIN32_PROCESS_API = False
    return _WIN32_PROCESS_API or None


_WINDOWS_CLEANUP_JOB_HANDLE = None


def _get_windows_cleanup_job():
    """Lazily create one Windows Job Object with KILL_ON_JOB_CLOSE for the lifetime
    of this process. Any ffmpeg child assigned to it is terminated by Windows itself
    if this process dies — including an ungraceful crash or Task Manager 'End Task'
    where our own lifespan shutdown / _stop_*_process cleanup never gets to run.
    Only a fallback now: each run normally gets its own job (_create_run_job)."""
    global _WINDOWS_CLEANUP_JOB_HANDLE
    if _WINDOWS_CLEANUP_JOB_HANDLE is not None:
        return _WINDOWS_CLEANUP_JOB_HANDLE
    api = _win32_process_api()
    if api is None:
        return None
    try:
        _WINDOWS_CLEANUP_JOB_HANDLE = api.create_kill_on_close_job()
    except Exception as exc:
        _log_failure("create Windows job object for child-process cleanup", exc)
        return None
    return _WINDOWS_CLEANUP_JOB_HANDLE


def _assign_child_to_cleanup_job(pid: int) -> None:
    """Best-effort; a failure here just means we fall back to explicit stop-on-shutdown
    cleanup (already in place) instead of Windows guaranteeing it on an ungraceful exit."""
    api = _win32_process_api()
    h_job = _get_windows_cleanup_job()
    if api is None or not h_job:
        return
    try:
        api.assign_pid(h_job, pid)
    except Exception as exc:
        _log_failure(f"assign pid={pid} to cleanup job object", exc)


def _create_run_job(pid: int):
    """Put one ffmpeg run in its own Job Object (KILL_ON_JOB_CLOSE). Stopping
    the run is then TerminateJobObject, which acts on the process objects in
    the job and can never hit an unrelated process that reused the pid; and
    Windows still kills the run if Jellyball dies without cleaning up (the job
    handle closes with us). Returns the job handle, or None off Windows or if
    no job could be made (then the shared cleanup job gives crash safety)."""
    api = _win32_process_api()
    if api is None:
        return None
    h_job = None
    try:
        h_job = api.create_kill_on_close_job()
        if h_job and api.assign_pid(h_job, pid):
            return h_job
    except Exception as exc:
        _log_failure(f"create job object for ffmpeg pid={pid}", exc)
    if h_job:
        api.close(h_job)
    _assign_child_to_cleanup_job(pid)
    return None


RUN_PID_FILE = "ffmpeg.pid"


def _write_run_pid_file(run_dir: Path, pid: int) -> None:
    """Record pid + creation time in the run dir so the next startup can kill
    this ffmpeg if Jellyball died without stopping it (and its job didn't take
    it down). Windows only: the creation time is what proves a live pid is
    still our ffmpeg rather than a reused pid."""
    api = _win32_process_api()
    if api is None:
        return
    try:
        created = api.process_creation_time(pid)
        if created is not None:
            (run_dir / RUN_PID_FILE).write_text(json.dumps({"pid": pid, "created": created}), encoding="utf-8")
    except Exception as exc:
        _log_failure(f"write ffmpeg pid file pid={pid}", exc)


def _kill_orphaned_ffmpeg() -> int:
    """Startup: kill ffmpeg runs a crashed previous instance left behind (pid
    and creation time must both match its pid file). Returns how many were
    killed. No-op off Windows."""
    api = _win32_process_api()
    if api is None:
        return 0
    pid_files: List[Path] = []
    for root, pattern in ((MULTIVIEW_OUTPUT_ROOT, f"*/run*/{RUN_PID_FILE}"), (PLACEHOLDER_OUTPUT_DIR, f"run*/{RUN_PID_FILE}")):
        try:
            pid_files += list(root.glob(pattern))
        except OSError:
            continue
    killed = 0
    for pid_file in pid_files:
        try:
            record = json.loads(pid_file.read_text(encoding="utf-8"))
            pid, created = int(record["pid"]), int(record["created"])
        except (OSError, ValueError, KeyError, TypeError):
            continue
        try:
            if api.terminate_pid_if_created_at(pid, created):
                killed += 1
                LOGGER.warning("Killed orphaned ffmpeg pid=%d left by a previous run (%s)", pid, pid_file.parent)
        except Exception as exc:
            _log_failure(f"kill orphaned ffmpeg pid={pid}", exc)
    return killed


async def _taskkill_tree(pid: int, label: str) -> None:
    try:
        kill_proc = await asyncio.create_subprocess_exec(
            "taskkill", "/F", "/T", "/PID", str(pid),
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            creationflags=_child_process_creationflags(),
        )
        await asyncio.wait_for(kill_proc.wait(), timeout=10.0)
    except (OSError, asyncio.TimeoutError) as exc:
        _log_failure(f"taskkill ffmpeg {label}", exc, logging.ERROR)


async def _terminate_ffmpeg(entry: dict, label: str, *, term_timeout: float = 5.0, kill_timeout: float = 3.0) -> None:
    """Stop one Multi-View/placeholder ffmpeg run.

    Windows: TerminateJobObject on the run's own job. It doesn't depend on
    process.returncode (asyncio's Proactor loop has been seen reporting a
    heavily-piped ffmpeg as exited while it was still encoding) and, unlike
    `taskkill /PID`, can't kill an unrelated process that reused the pid.
    Without a job: TerminateProcess through our own process handle (also
    immune to pid reuse), and taskkill only as a last resort while asyncio
    still considers the process alive."""
    process: asyncio.subprocess.Process = entry["process"]
    job = entry.pop("job", None)
    try:
        if sys.platform == "win32":
            api = _win32_process_api()
            terminated = False
            if job and api is not None:
                try:
                    terminated = api.terminate_job(job)
                except Exception as exc:
                    _log_failure(f"terminate job ffmpeg {label}", exc)
            if not terminated and process.returncode is None:
                try:
                    process.kill()
                except (ProcessLookupError, OSError):
                    pass
            try:
                await asyncio.wait_for(process.wait(), timeout=kill_timeout)
            except asyncio.TimeoutError:
                if process.returncode is None:
                    await _taskkill_tree(process.pid, label)
                    try:
                        await asyncio.wait_for(process.wait(), timeout=kill_timeout)
                    except asyncio.TimeoutError:
                        LOGGER.error("ffmpeg %s pid=%s did not exit after kill", label, process.pid)
        elif process.returncode is None:
            try:
                process.terminate()
                await asyncio.wait_for(process.wait(), timeout=term_timeout)
            except (asyncio.TimeoutError, ProcessLookupError):
                try:
                    process.kill()
                    await asyncio.wait_for(process.wait(), timeout=kill_timeout)
                except (asyncio.TimeoutError, ProcessLookupError) as exc:
                    _log_failure(f"kill ffmpeg {label}", exc, logging.ERROR)
    finally:
        if job:
            api = _win32_process_api()
            if api is not None:
                api.close(job)


def _rmtree_with_retries(path: Path, attempts: int = 5, delay: float = 1.0) -> bool:
    """Blocking (run it in a thread). Windows keeps a just-killed process's
    files locked for a moment, so one rmtree right after the kill used to
    leave the whole run directory (~100MB of segments) behind."""
    for attempt in range(attempts):
        try:
            shutil.rmtree(path)
            return True
        except OSError:
            if not path.exists():
                return True
            if attempt + 1 < attempts:
                time.sleep(delay)
    return not path.exists()


def _remove_tree_later(path: Path, label: str) -> None:
    """Delete a run directory off the event loop, retrying while files are locked."""
    _spawn_background_task(asyncio.to_thread(_rmtree_with_retries, path), f"remove {label} output dir")


def _prepare_run_dir(run_dir: Path, audio_outputs: int) -> None:
    """Blocking: a fresh, empty run directory (plus a{N}/ per Multi-View audio output)."""
    if run_dir.exists():
        shutil.rmtree(run_dir, ignore_errors=True)
    run_dir.mkdir(parents=True, exist_ok=True)
    for idx in range(audio_outputs):
        (run_dir / f"a{idx}").mkdir(parents=True, exist_ok=True)


async def _stop_multiview_process(
    channel_id: str,
    *,
    run_id: Optional[int] = None,
    term_timeout: float = 5.0,
    kill_timeout: float = 3.0,
) -> bool:
    """Stop the channel's current run (only if it is `run_id`, when given, so a
    caller holding a stale snapshot can't kill a newer run). True if stopped."""
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if not entry or (run_id is not None and entry.get("run_id") != run_id):
        return False
    _MULTIVIEW_PROCESSES.pop(channel_id, None)
    await _terminate_ffmpeg(entry, f"multiview={channel_id}", term_timeout=term_timeout, kill_timeout=kill_timeout)
    for task_key in ("watch_task", "log_task"):
        task = entry.get(task_key)
        if task and not task.done():
            task.cancel()
    tasks = [entry.get(key) for key in ("watch_task", "log_task") if entry.get(key)]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    _remove_tree_later(entry["output_dir"], f"multiview={channel_id}")
    return True


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


def _multiview_entry_alive(entry: dict) -> bool:
    return entry["process"].returncode is None and not entry.get("exited")


def _running_multiview_count(exclude: str = "") -> int:
    """Concurrency slots in use: live runs plus spawns that reserved a slot
    and are still warming up or waiting for first segments."""
    in_use = {cid for cid, entry in _MULTIVIEW_PROCESSES.items() if _multiview_entry_alive(entry)}
    in_use |= _MULTIVIEW_SLOT_RESERVATIONS
    in_use.discard(exclude)
    return len(in_use)


MULTIVIEW_MEMBER_WARM_TIMEOUT = bounded_float(os.getenv("MULTIVIEW_MEMBER_WARM_TIMEOUT", "20"), 20.0, 3.0, 120.0)
MULTIVIEW_WATCHDOG_INTERVAL = bounded_float(os.getenv("MULTIVIEW_WATCHDOG_INTERVAL", "3"), 3.0, 1.0, 60.0)
MULTIVIEW_AUDIO_CHANNELS = os.getenv("MULTIVIEW_AUDIO_CHANNELS", "1").strip().lower() not in ("0", "false", "no")
# Extra wait for the placeholder session when a member has to be replaced by it.
MULTIVIEW_STANDIN_WAIT_SECONDS = 10.0
PLACEHOLDER_SESSION_ID = "__placeholder__"
# MPEG-TS stream_type ffmpeg writes for AAC (ADTS): the generated silent track.
_MULTIVIEW_SILENCE_AUDIO_TYPE = 0x0F
# Monotonic time of the last GPU encoder / CUDA device failure (None = none yet).
_HW_ENCODER_FAILED_AT: Optional[float] = None
_CUDA_DECODE_FAILED_AT: Optional[float] = None
# Encoder-specific failure wording, matched against the message part of a
# failed run's log lines. Deliberately no bare "cuda": NVDEC's per-stream
# software fallback ("Failed setup for format cuda") mentions it too, and
# leaves the GPU encoder perfectly usable.
_HW_ENCODER_MARKERS = {
    "nvenc": (
        "h264_nvenc", "hevc_nvenc", "openencodesessionex", "nvenc", "no capable devices found",
        "cannot load nvcuda", "cannot load libcuda", "nvenc api version",
        "driver does not support the required nvenc api version",
    ),
    "qsv": (
        "h264_qsv", "hevc_qsv", "qsv", "mfx session", "libmfx", "libvpl", "device creation failed",
    ),
}
_HW_ENCODER_NAMES = {"nvenc": ("h264_nvenc", "hevc_nvenc"), "qsv": ("h264_qsv", "hevc_qsv")}
# CUDA *device* setup failures (no/broken NVIDIA driver). Unlike the per-stream
# NVDEC fallback these are fatal for every input opened with -hwaccel cuda
# (ffmpeg 9: "Hardware device setup failed for decoder" aborts the run), so a
# retry must drop -hwaccel.
_CUDA_INIT_FAILURE_MARKERS = (
    "cannot load nvcuda", "cannot load libcuda", "could not dynamically load cuda",
    "device creation failed", "no device available for decoder", "hardware device setup failed",
    "cuinit",
)
_FFMPEG_LOG_CONTEXT_RE = re.compile(r"^\s*\[([^\]]*)\]\s*")


def _ffmpeg_log_parts(line: str) -> Tuple[List[str], str]:
    """'[vost#0:0/h264_nvenc @ 0x1] [enc:h264_nvenc @ 0x2] Error ...' ->
    (['vost#0:0/h264_nvenc', 'enc:h264_nvenc'], 'error ...'), lowercased."""
    contexts: List[str] = []
    rest = line
    while True:
        match = _FFMPEG_LOG_CONTEXT_RE.match(rest)
        if not match:
            break
        contexts.append(match.group(1).split(" @ ")[0].strip().lower())
        rest = rest[match.end():]
    return contexts, rest.strip().lower()


def _is_ffmpeg_stream_info(message: str) -> bool:
    # Stream mapping / description / metadata lines name the encoder even when
    # it works ("Stream #0:0 -> #0:0 (h264 (native) -> h264 (h264_nvenc))").
    return message.startswith(("stream #", "stream mapping", "encoder")) or " -> " in message


def _looks_like_hw_encoder_failure(log_lines, encoder: str = "nvenc") -> bool:
    """True when a failed run's log blames the GPU *encoder* (NVENC or QSV)."""
    encoder = (encoder or "").lower()
    markers = _HW_ENCODER_MARKERS.get(encoder)
    if not markers:
        return False
    names = _HW_ENCODER_NAMES[encoder]
    for line in log_lines:
        contexts, message = _ffmpeg_log_parts(str(line))
        if not message or _is_ffmpeg_stream_info(message):
            continue
        if any(marker in message for marker in markers):
            return True
        # Anything the encoder itself logged on a failed run ("[h264_nvenc @ ..] InitializeEncoder failed").
        if any(context in names for context in contexts):
            return True
        # fftools' wrapper around it ("[enc:h264_nvenc @ ..] Error while opening encoder").
        if ("error while opening encoder" in message or "could not open encoder" in message) and any(
            context.endswith(names) for context in contexts
        ):
            return True
    return False


def _looks_like_cuda_init_failure(log_lines) -> bool:
    for line in log_lines:
        _, message = _ffmpeg_log_parts(str(line))
        if any(marker in message for marker in _CUDA_INIT_FAILURE_MARKERS):
            return True
    return False


def _multiview_encoder_plan(now: Optional[float] = None) -> Tuple[str, bool]:
    """(encoder, use -hwaccel cuda) for the next run. Within
    NVENC_FALLBACK_SECONDS of a GPU encoder failure new runs use libx264, then
    the GPU is tried again. CUDA decoding stays on in that fallback unless the
    CUDA device itself failed."""
    now = time.monotonic() if now is None else now
    encoder = MULTIVIEW_HWACCEL
    if (
        encoder in _HW_ENCODER_MARKERS
        and _HW_ENCODER_FAILED_AT is not None
        and now - _HW_ENCODER_FAILED_AT < NVENC_FALLBACK_SECONDS
    ):
        encoder = "none"
    cuda_broken = _CUDA_DECODE_FAILED_AT is not None and now - _CUDA_DECODE_FAILED_AT < NVENC_FALLBACK_SECONDS
    return encoder, MULTIVIEW_HWACCEL == "nvenc" and not cuda_broken


def _multiview_audio_index(data: dict) -> int:
    members = data.get("member_team_ids") or []
    active = data.get("active_audio_team_id") or (members[0] if members else "")
    return members.index(active) if active in members else 0


def _multiview_member_label(team_id: str) -> str:
    member = stream_state.get(team_id)
    return str(member.get("name") or team_id) if member else f"{team_id} (removed)"


from typing import NamedTuple  # noqa: E402 (kept local to the Multi-View section)


class _MultiviewInput(NamedTuple):
    team_id: str                # the configured member
    session_id: str             # channel session ffmpeg reads for this pane
    has_audio: bool             # known to carry audio (else: silent track)
    audio_type: Optional[int]   # MPEG-TS stream_type of that audio, if known
    standin: bool               # an existing member replaced by the placeholder for this run


async def _warm_multiview_members(member_team_ids: List[str]) -> List[_MultiviewInput]:
    """Start every member's channel session in parallel and wait for each to
    have a playable window before ffmpeg opens them one after another (four
    cold members used to blow the startup timeout).

    A member still not ready after MULTIVIEW_MEMBER_WARM_TIMEOUT is fed from
    the "No Signal" placeholder session for this run instead: its own
    /stream/x.m3u8 would answer 503, ffmpeg's -i would fail, and one slow
    member would push the whole grid into failure backoff. The placeholder is
    only warmed once some member is still pending halfway through the wait,
    and the watchdog swaps the real member in once it is live."""
    session_ids = [team_id if team_id in stream_state else PLACEHOLDER_SESSION_ID for team_id in member_team_ids]
    waits: Dict[str, asyncio.Future] = {}
    for session_id in session_ids:
        if session_id not in waits:
            session = SESSIONS.get(session_id)
            session.touch()
            waits[session_id] = asyncio.ensure_future(session.wait_ready(MULTIVIEW_MEMBER_WARM_TIMEOUT))
    try:
        _, pending = await asyncio.wait(list(waits.values()), timeout=MULTIVIEW_MEMBER_WARM_TIMEOUT / 2)
        if pending and PLACEHOLDER_SESSION_ID not in waits:
            SESSIONS.get(PLACEHOLDER_SESSION_ID).touch()
        await asyncio.gather(*waits.values(), return_exceptions=True)
    finally:
        for wait in waits.values():
            if not wait.done():
                wait.cancel()

    def is_ready(session_id: str) -> bool:
        wait = waits.get(session_id)
        return bool(wait and wait.done() and not wait.cancelled() and wait.exception() is None and wait.result())

    standins = {
        index for index, session_id in enumerate(session_ids)
        if session_id != PLACEHOLDER_SESSION_ID and not is_ready(session_id)
    }
    placeholder_ready = is_ready(PLACEHOLDER_SESSION_ID)
    if (standins or PLACEHOLDER_SESSION_ID in waits) and not placeholder_ready:
        placeholder = SESSIONS.get(PLACEHOLDER_SESSION_ID)
        placeholder.touch()
        placeholder_ready = await placeholder.wait_ready(MULTIVIEW_STANDIN_WAIT_SECONDS)

    inputs: List[_MultiviewInput] = []
    for index, (team_id, session_id) in enumerate(zip(member_team_ids, session_ids)):
        standin = index in standins
        if standin:
            LOGGER.warning(
                "Multi-View member %s not ready after %.0fs; its pane shows No Signal for this run",
                team_id, MULTIVIEW_MEMBER_WARM_TIMEOUT,
            )
            session_id = PLACEHOLDER_SESSION_ID
        ready = placeholder_ready if session_id == PLACEHOLDER_SESSION_ID else True
        session = SESSIONS.peek(session_id)
        # Unknown audio (None) counts as none: -map N:a:0 on an input without
        # audio fails the whole run, the silent track never does.
        has_audio = bool(ready and session is not None and session.has_audio is True)
        signature = session.codec_signature if has_audio and session is not None else None
        audio_type = signature[1] if signature and len(signature) > 1 else None
        inputs.append(_MultiviewInput(team_id, session_id, has_audio, audio_type, standin))
    return inputs


def _multiview_spawn_wanted(channel_id: str, data: dict) -> bool:
    """Re-checked after every await of a spawn: the channel may have been
    removed/replaced, or stopped from the dashboard, in the meantime."""
    return stream_state.get(channel_id) is data and channel_id not in _MULTIVIEW_MANUAL_STOPS


async def _spawn_multiview(channel_id: str, data: dict) -> None:
    """Start ffmpeg for a Multi-View channel unless it's already running.

    Only ever runs as the channel's _MULTIVIEW_START_TASKS entry (see
    _request_multiview_start), which serializes starts per channel; Stop and
    Remove cancel that task. Outcomes are recorded, not returned: failures in
    _MULTIVIEW_FAILURES (backoff), refusals as a short hold, success as a
    ready _MULTIVIEW_PROCESSES entry."""
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if entry and _multiview_entry_alive(entry):
        return
    if not _multiview_spawn_wanted(channel_id, data) or _multiview_cooldown_remaining(channel_id) > 0:
        return
    if not FFMPEG_AVAILABLE:
        _record_multiview_refusal(channel_id, "ffmpeg is unavailable")
        return
    in_use = _running_multiview_count(exclude=channel_id)
    if in_use >= MAX_CONCURRENT_MULTIVIEW:
        _record_multiview_refusal(
            channel_id,
            f"{in_use}/{MAX_CONCURRENT_MULTIVIEW} concurrent Multi-View streams already running "
            "(stop one, or raise MAX_CONCURRENT_MULTIVIEW)",
        )
        return
    _MULTIVIEW_REFUSALS.pop(channel_id, None)
    _MULTIVIEW_SLOT_RESERVATIONS.add(channel_id)
    try:
        await _start_multiview_run(channel_id, data)
    finally:
        _MULTIVIEW_SLOT_RESERVATIONS.discard(channel_id)


async def _start_multiview_run(channel_id: str, data: dict) -> None:
    global _MULTIVIEW_RUN_COUNTER, _HW_ENCODER_FAILED_AT, _CUDA_DECODE_FAILED_AT
    inputs = await _warm_multiview_members(list(data["member_team_ids"]))
    if not _multiview_spawn_wanted(channel_id, data):
        LOGGER.info("Multi-View channel=%s was removed or stopped while warming up; not starting", channel_id)
        return
    stale = _MULTIVIEW_PROCESSES.get(channel_id)
    if stale is not None:  # a dead run the idle monitor hasn't reaped yet
        await _stop_multiview_process(channel_id, run_id=stale.get("run_id"))

    encoder, hw_decode = _multiview_encoder_plan()
    error = "ffmpeg exited unexpectedly"
    for _attempt in range(3):
        _MULTIVIEW_RUN_COUNTER += 1
        outcome, lines = await _launch_multiview_run(channel_id, data, _MULTIVIEW_RUN_COUNTER, inputs, encoder, hw_decode)
        if outcome != "failed":
            return
        error = _multiview_error_from_log(lines) if lines else "ffmpeg exited unexpectedly"
        if encoder in _HW_ENCODER_MARKERS and _looks_like_hw_encoder_failure(lines, encoder):
            # GPU encoder unavailable (driver, session limit shared with
            # Jellyfin's own transcodes, ...): retry this same spawn with
            # libx264 right away, and keep new runs on it for a while.
            cuda_broken = hw_decode and _looks_like_cuda_init_failure(lines)
            _HW_ENCODER_FAILED_AT = time.monotonic()
            if cuda_broken:
                _CUDA_DECODE_FAILED_AT = _HW_ENCODER_FAILED_AT
            hw_decode = hw_decode and not cuda_broken
            LOGGER.warning(
                "%s unavailable for multiview channel=%s (%s); retrying with libx264%s, GPU re-tried in %.0fs",
                encoder.upper(), channel_id, error, " + CUDA decoding" if hw_decode else "", NVENC_FALLBACK_SECONDS,
            )
            encoder = "none"
            continue
        if hw_decode and _looks_like_cuda_init_failure(lines):
            _CUDA_DECODE_FAILED_AT = time.monotonic()
            hw_decode = False
            LOGGER.warning("CUDA decoding unavailable for multiview channel=%s (%s); retrying with software decoding", channel_id, error)
            continue
        break
    _record_multiview_failure(channel_id, error)
    LOGGER.error("multiview channel=%s failed to produce first segments in time: %s", channel_id, error)


async def _launch_multiview_run(
    channel_id: str,
    data: dict,
    run_id: int,
    inputs: List[_MultiviewInput],
    encoder: str,
    hw_decode: bool,
) -> Tuple[str, List[str]]:
    """One ffmpeg attempt: ("ready" | "failed" | "aborted", log lines of a failed run)."""
    run_dir = MULTIVIEW_OUTPUT_ROOT / channel_id / f"run{run_id}"
    _MULTIVIEW_PENDING_RUN_DIRS.add(run_dir)
    try:
        await asyncio.to_thread(_prepare_run_dir, run_dir, len(inputs))
        if not _multiview_spawn_wanted(channel_id, data):
            _remove_tree_later(run_dir, f"multiview={channel_id}")
            return "aborted", []
        args = _build_multiview_ffmpeg_args(
            channel_id, data, run_dir, [item.has_audio for item in inputs], encoder,
            hw_decode=hw_decode, input_ids=[item.session_id for item in inputs],
        )
        try:
            process = await asyncio.create_subprocess_exec(MULTIVIEW_FFMPEG_PATH, *args, **_multiview_popen_kwargs(run_dir))
        except (FileNotFoundError, OSError) as exc:
            _log_failure(f"spawn ffmpeg multiview={channel_id}", exc, logging.ERROR)
            _remove_tree_later(run_dir, f"multiview={channel_id}")
            return "failed", ["could not launch ffmpeg"]
        job = _create_run_job(process.pid)
        _write_run_pid_file(run_dir, process.pid)

        log_lines: "deque[str]" = deque(maxlen=200)
        watch_task = _spawn_background_task(
            _watch_multiview_process(channel_id, process),
            f"watch multiview ffmpeg={channel_id}",
        )
        log_task = _spawn_background_task(
            _drain_multiview_log(channel_id, process, log_lines),
            f"drain multiview ffmpeg log={channel_id}",
        )
        now = time.monotonic()
        entry = {
            "process": process,
            "job": job,
            "output_dir": run_dir,
            "run_id": run_id,
            "audio_count": len(inputs),
            # Output a{N} carries member N's own (stream-copied) audio codec,
            # or the generated AAC silence; part of the views' source key.
            "audio_types": [item.audio_type if item.has_audio else _MULTIVIEW_SILENCE_AUDIO_TYPE for item in inputs],
            "standins": [item.team_id for item in inputs if item.standin],
            "encoder": encoder,
            "hw_decode": hw_decode,
            "ready": False,
            "started_at": now,
            "last_access": max(now, _MULTIVIEW_LAST_VIEWER.get(channel_id, 0.0)),
            "exited": False,
            "exit_code": None,
            "watch_task": watch_task,
            "log_task": log_task,
            "log_lines": log_lines,
        }
        _MULTIVIEW_PROCESSES[channel_id] = entry
        try:
            results = await asyncio.gather(*(
                _wait_for_first_segment(run_dir / f"a{idx}", process, MULTIVIEW_STARTUP_TIMEOUT_SECONDS)
                for idx in range(len(inputs))
            ))
        except asyncio.CancelledError:
            # Stop/Remove cancelled this spawn: never leave its ffmpeg behind.
            await _stop_multiview_process(channel_id, run_id=run_id)
            raise
        if not _multiview_spawn_wanted(channel_id, data) or _MULTIVIEW_PROCESSES.get(channel_id) is not entry:
            # Stopped/removed meanwhile, or the run was already taken down
            # elsewhere (then this stop is a no-op): not a failure to back off.
            await _stop_multiview_process(channel_id, run_id=run_id)
            return "aborted", []
        if all(results):
            entry["ready"] = True
            _clear_multiview_failure(channel_id)
            LOGGER.info(
                "Multi-View running channel=%s run=%d encoder=%s%s",
                channel_id, run_id, encoder, " (CUDA decoding)" if hw_decode else "",
            )
            for session_id in _multiview_view_session_ids(channel_id):
                SESSIONS.poke(session_id)
            return "ready", []
        lines = list(log_lines)
        await _stop_multiview_process(channel_id, run_id=run_id)
        return "failed", lines
    finally:
        _MULTIVIEW_PENDING_RUN_DIRS.discard(run_dir)


def _request_multiview_start(channel_id: str) -> None:
    data = stream_state.get(channel_id)
    if not data or data.get("type") != "multiview":
        return
    if channel_id in _MULTIVIEW_MANUAL_STOPS:
        return
    task = _MULTIVIEW_START_TASKS.get(channel_id)
    if task is not None and not task.done():
        return
    if _multiview_cooldown_remaining(channel_id) > 0:
        return
    task = _spawn_background_task(_spawn_multiview(channel_id, data), f"start multiview channel={channel_id}")
    _MULTIVIEW_START_TASKS[channel_id] = task

    def _forget(done: asyncio.Task, cid: str = channel_id) -> None:
        if _MULTIVIEW_START_TASKS.get(cid) is done:
            _MULTIVIEW_START_TASKS.pop(cid, None)

    task.add_done_callback(_forget)


def _multiview_start_in_progress(channel_id: str) -> bool:
    task = _MULTIVIEW_START_TASKS.get(channel_id)
    return task is not None and not task.done()


async def _cancel_multiview_start(channel_id: str) -> bool:
    """Cancel and await an in-progress spawn (which stops any ffmpeg it had
    already launched). True if one was running."""
    task = _MULTIVIEW_START_TASKS.pop(channel_id, None)
    if task is None or task.done():
        return False
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    return True


async def _stop_multiview_manually(channel_id: str) -> None:
    """Dashboard Stop: stop the grid and keep it stopped (see _MULTIVIEW_MANUAL_STOPS)."""
    _MULTIVIEW_MANUAL_STOPS[channel_id] = time.monotonic()
    await _cancel_multiview_start(channel_id)
    await _stop_multiview_process(channel_id)
    LOGGER.info("Multi-View channel=%s stopped from the dashboard; it stays stopped until a new viewer tunes in", channel_id)
    for session_id in _multiview_view_session_ids(channel_id):
        SESSIONS.poke(session_id)


def _clear_multiview_manual_stop(channel_id: str, why: str) -> None:
    if _MULTIVIEW_MANUAL_STOPS.pop(channel_id, None) is not None:
        LOGGER.info("Multi-View channel=%s may start again (%s)", channel_id, why)


def _multiview_view_session_ids(channel_id: str) -> List[str]:
    data = stream_state.get(channel_id) or {}
    return [channel_id] + [f"{channel_id}#a{idx}" for idx in range(len(data.get("member_team_ids") or []))]


def _multiview_view_for_session(session_id: str) -> Optional[Tuple[str, Optional[int]]]:
    """Map a session id to (multiview channel, audio index or None for 'active audio')."""
    if "#a" in session_id:
        channel_id, _, index_text = session_id.rpartition("#a")
        data = stream_state.get(channel_id)
        if data and data.get("type") == "multiview" and index_text.isdigit():
            return channel_id, int(index_text)
        return None
    data = stream_state.get(session_id)
    if data and data.get("type") == "multiview":
        return session_id, None
    return None


def _multiview_output_audio_tag(entry: dict, index: int) -> str:
    """Source-key component for Multi-View output a{index}: its audio codec
    (MPEG-TS stream_type) when known, else the output index itself."""
    audio_types = entry.get("audio_types") or []
    audio_type = audio_types[index] if 0 <= index < len(audio_types) else None
    return f"audio:{audio_type:#04x}" if isinstance(audio_type, int) else f"audio:a{index}"


def _resolve_multiview_view_source(channel_id: str, audio_index: Optional[int]) -> Optional[SourceSpec]:
    data = stream_state.get(channel_id)
    if not data or data.get("type") != "multiview":
        return None
    if channel_id in _MULTIVIEW_MANUAL_STOPS:
        return _placeholder_source()
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if entry and entry.get("ready") and _multiview_entry_alive(entry):
        members = data.get("member_team_ids") or []
        index = _multiview_audio_index(data) if audio_index is None else audio_index
        index = max(0, min(index, entry["audio_count"] - 1))
        label = _multiview_member_label(members[index]) if index < len(members) else f"audio {index}"
        # One key per (run, audio codec). All outputs of a run share segment
        # numbers and video timestamps, so switching the main channel's audio
        # to an output whose audio has the same codec is just a URL change
        # and continues seamlessly. Member audio is stream-copied, though, so
        # each output carries its own member's codec: switching to a different
        # one (say AAC -> AC-3) gets a new key, i.e. a new normalizer epoch and
        # a discontinuity, so the player re-probes instead of hitting a PMT
        # change mid-stream. When a codec isn't known the key uses the output
        # index, making every switch to/from it a clean discontinuity. A new
        # run (restart) is always a new key.
        return SourceSpec(
            key=("multiview", channel_id, entry["run_id"], _multiview_output_audio_tag(entry, index)),
            url=str(entry["output_dir"] / f"a{index}" / "index.m3u8"),
            local=True,
            label=f"Multi-View {label}",
        )
    _request_multiview_start(channel_id)
    if _multiview_cooldown_remaining(channel_id) > 0 or not FFMPEG_AVAILABLE:
        # Backoff, refusal (concurrency cap) or no ffmpeg: No Signal, not a 503.
        return _placeholder_source()
    return None  # starting: the session waits (and keeps its current window)


def _multiview_view_sessions_running(channel_id: str) -> bool:
    for session_id in _multiview_view_session_ids(channel_id):
        session = SESSIONS.peek(session_id)
        if session is not None and session.is_running:
            return True
    return False


def _touch_multiview_viewer(channel_id: str) -> None:
    now = time.monotonic()
    if channel_id in _MULTIVIEW_MANUAL_STOPS and not _multiview_view_sessions_running(channel_id):
        # No view session is running, so this request starts a new one: a
        # fresh tune after the Stop counts as an explicit play.
        _clear_multiview_manual_stop(channel_id, "new viewer tuned in")
    _MULTIVIEW_LAST_VIEWER[channel_id] = now
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if entry:
        entry["last_access"] = now


def _on_multiview_view_failure(session_id: str, source_key: tuple, reason: str) -> None:
    view = _multiview_view_for_session(session_id)
    if view is None or len(source_key) < 3 or source_key[0] != "multiview":
        return
    channel_id = view[0]
    run_id = source_key[2]
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if entry and entry.get("run_id") == run_id:
        _spawn_background_task(
            _restart_multiview(channel_id, f"view reported {reason}", run_id=run_id),
            f"restart multiview channel={channel_id}",
        )


async def _restart_multiview(
    channel_id: str,
    reason: str,
    run_id: Optional[int] = None,
    *,
    count_toward_backoff: bool = True,
) -> bool:
    """Restart a Multi-View. With `run_id` it only acts if that run is still
    the current one: the watchdog and the view sessions work from snapshots,
    and a restart that already replaced run N with N+1 must not be followed
    by a second one killing N+1. Without `run_id` (a config change, e.g. a
    member was removed) it also cancels a spawn in progress so the new run
    picks up the change. Restarts are counted in a rolling window and backed
    off (see _note_multiview_restart). True if a restart happened."""
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if run_id is not None:
        if entry is None or entry.get("run_id") != run_id:
            LOGGER.info(
                "Ignoring restart of stale multiview run channel=%s run=%s current=%s reason=%s",
                channel_id, run_id, entry.get("run_id") if entry else None, reason,
            )
            return False
    else:
        cancelled = await _cancel_multiview_start(channel_id)
        entry = _MULTIVIEW_PROCESSES.get(channel_id)
        if entry is None and not cancelled:
            return False
    if entry is not None and entry.get("restarting"):
        return False
    if entry is not None:
        entry["restarting"] = True
    count, delay = _note_multiview_restart(channel_id, reason) if count_toward_backoff else (0, 0.0)
    if delay > 0:
        LOGGER.warning(
            "Restarting multiview channel=%s run=%s reason=%s; %d restarts in %.0f min, next start held %.0fs",
            channel_id, entry.get("run_id") if entry else None, reason, count,
            MULTIVIEW_RESTART_WINDOW_SECONDS / 60, delay,
        )
    else:
        LOGGER.warning(
            "Restarting multiview channel=%s run=%s reason=%s",
            channel_id, entry.get("run_id") if entry else None, reason,
        )
    if entry is not None:
        await _stop_multiview_process(channel_id, run_id=entry["run_id"])
    _request_multiview_start(channel_id)  # no-op while held; the next viewer poll starts it
    return True


def _newest_segment_age(output_dir: Path) -> Optional[float]:
    """Seconds since this output last produced a segment, or None if it has
    no playlist either (broken output).

    ffmpeg's delete_segments removes old files constantly, so a file can
    vanish between the glob and its stat: that one file is skipped rather
    than the whole output being reported stalled (which restarted healthy
    grids). An output with no segment file visible falls back to its
    playlist's age, so it only counts as stalled once the playlist is stale."""
    newest = None
    try:
        paths = list(output_dir.glob("seg_*.ts"))
    except OSError:
        paths = []
    for path in paths:
        try:
            mtime = path.stat().st_mtime
        except OSError:
            continue
        newest = mtime if newest is None or mtime > newest else newest
    if newest is None:
        try:
            newest = (output_dir / "index.m3u8").stat().st_mtime
        except FileNotFoundError:
            return None
        except OSError:
            return 0.0  # can't tell right now (e.g. a sharing violation): not evidence of a stall
    return max(0.0, time.time() - newest)


async def _swap_in_ready_members(channel_id: str, entry: dict) -> None:
    """A pane showing the stand-in placeholder gets its real member back once
    that member is live: keep its session warm meanwhile and restart the run
    when it flows, as long as that restart wouldn't be backed off."""
    live = []
    for team_id in entry.get("standins") or []:
        if team_id not in stream_state:
            continue
        session = SESSIONS.get(team_id)
        session.touch()
        source = session.source
        if session.is_flowing() and source is not None and not _is_placeholder_key(source.key):
            live.append(team_id)
    if live and _multiview_restart_delay(_recent_multiview_restarts(channel_id) + 1) == 0:
        await _restart_multiview(channel_id, f"member(s) {live} live now", run_id=entry["run_id"])


async def _multiview_watchdog_pass(stall_after: float) -> None:
    now = time.monotonic()
    for channel_id, entry in list(_MULTIVIEW_PROCESSES.items()):
        if not entry.get("ready") or entry.get("restarting"):
            continue
        run_id = entry["run_id"]
        watched = now - entry.get("last_access", 0.0) < MULTIVIEW_IDLE_TIMEOUT_SECONDS
        if not _multiview_entry_alive(entry):
            if watched:
                await _restart_multiview(channel_id, f"ffmpeg exited code={entry.get('exit_code')}", run_id=run_id)
            continue
        if now - entry["started_at"] < MULTIVIEW_STARTUP_TIMEOUT_SECONDS:
            continue
        ages = await asyncio.to_thread(
            lambda e=entry: [_newest_segment_age(e["output_dir"] / f"a{i}") for i in range(e["audio_count"])]
        )
        stalled = [i for i, age in enumerate(ages) if age is None or age > stall_after]
        if stalled:
            await _restart_multiview(channel_id, f"output stalled audio={stalled}", run_id=run_id)
        elif watched and entry.get("standins"):
            await _swap_in_ready_members(channel_id, entry)


async def multiview_watchdog() -> None:
    """Restart a Multi-View run whose output stalls (one frozen input can hold
    xstack) or whose ffmpeg died while people are watching. The channel
    sessions hide the restart behind a discontinuity."""
    stall_after = max(3.0 * MULTIVIEW_SEGMENT_SECONDS, 12.0)
    while True:
        await asyncio.sleep(MULTIVIEW_WATCHDOG_INTERVAL)
        try:
            await _multiview_watchdog_pass(stall_after)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_failure("multiview watchdog", exc)


async def multiview_idle_monitor() -> None:
    while True:
        try:
            now = time.monotonic()
            for channel_id, entry in list(_MULTIVIEW_PROCESSES.items()):
                if entry.get("restarting"):
                    continue
                if not _multiview_entry_alive(entry):
                    if now - entry.get("last_access", 0.0) >= MULTIVIEW_IDLE_TIMEOUT_SECONDS:
                        LOGGER.warning("Reaping dead multiview ffmpeg channel=%s code=%s", channel_id, entry.get("exit_code"))
                        await _stop_multiview_process(channel_id, run_id=entry.get("run_id"))
                elif now - entry.get("last_access", now) > MULTIVIEW_IDLE_TIMEOUT_SECONDS:
                    LOGGER.info("Stopping idle multiview ffmpeg channel=%s", channel_id)
                    await _stop_multiview_process(channel_id, run_id=entry.get("run_id"))

            # The placeholder runs on demand: channel sessions (re)start it when a
            # channel has nothing live, and it stops after a stretch of disuse.
            state = _PLACEHOLDER_STATE
            if state and now - state.get("last_access", now) > PLACEHOLDER_IDLE_SECONDS:
                LOGGER.info("Stopping idle placeholder ffmpeg")
                await _stop_placeholder_process()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_failure("multiview idle monitor", exc)
        await asyncio.sleep(MULTIVIEW_IDLE_CHECK_INTERVAL)


def _sweep_output_dirs_sync(live_dirs: Set[Path], min_age: float) -> List[Path]:
    """Blocking. Delete Multi-View run dirs (<root>/<channel>/runN) and
    placeholder run dirs that no live run owns, plus channel dirs left empty.
    Dirs younger than `min_age` seconds are kept: a spawn may have just
    created one it hasn't registered yet."""
    removed: List[Path] = []
    now = time.time()
    live_parents = {path.parent for path in live_dirs}

    def stale(path: Path) -> bool:
        if path in live_dirs:
            return False
        try:
            return path.is_dir() and now - path.stat().st_mtime >= min_age
        except OSError:
            return False

    def children(path: Path) -> List[Path]:
        try:
            return list(path.iterdir())
        except OSError:
            return []

    for channel_dir in children(MULTIVIEW_OUTPUT_ROOT):
        if not channel_dir.is_dir():
            continue
        for run_dir in children(channel_dir):
            if stale(run_dir) and _rmtree_with_retries(run_dir, attempts=2, delay=0.5):
                removed.append(run_dir)
        if channel_dir not in live_parents and not children(channel_dir):
            try:
                channel_dir.rmdir()
                removed.append(channel_dir)
            except OSError:
                pass
    for run_dir in children(PLACEHOLDER_OUTPUT_DIR):
        if stale(run_dir) and _rmtree_with_retries(run_dir, attempts=2, delay=0.5):
            removed.append(run_dir)
    return removed


async def _sweep_output_dirs(min_age: float = 60.0) -> List[Path]:
    live: Set[Path] = {entry["output_dir"] for entry in _MULTIVIEW_PROCESSES.values()}
    live |= _MULTIVIEW_PENDING_RUN_DIRS | _PLACEHOLDER_PENDING_DIRS
    if _PLACEHOLDER_STATE:
        live.add(_PLACEHOLDER_STATE["output_dir"])
    removed = await asyncio.to_thread(_sweep_output_dirs_sync, live, min_age)
    if removed:
        LOGGER.info("Removed %d leftover Multi-View/placeholder output dir(s)", len(removed))
    return removed


async def multiview_output_sweeper() -> None:
    """Periodic safety net for run directories a stop couldn't delete (files
    still locked after all retries, a crash mid-stop, ...)."""
    while True:
        await asyncio.sleep(MULTIVIEW_SWEEP_INTERVAL)
        try:
            await _sweep_output_dirs()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_failure("sweep multiview output dirs", exc)


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


def _multiview_audio_view(channel_id: str, audio_index: int) -> Optional[dict]:
    data = stream_state.get(channel_id)
    if not data or data.get("type") != "multiview":
        return None
    if not 0 <= audio_index < len(data.get("member_team_ids") or []):
        return None
    return data


async def _serve_multiview_audio_playlist(channel_id: str, audio_index: int, request: Request, segment_prefix: str):
    """Per-audio Multi-View channel ("🔊 Team · Name"): the same composited video
    with one member's audio. Switching audio inside Jellyfin = changing channel,
    which works on every client."""
    if _multiview_audio_view(channel_id, audio_index) is None:
        raise HTTPException(status_code=404, detail="Multi-View channel not found")
    if request.method == "HEAD":
        return Response(status_code=200, media_type=HLS_MEDIA_TYPE, headers={"Cache-Control": "no-cache"})
    _touch_multiview_viewer(channel_id)
    _maybe_record_playback_event(channel_id)
    return await _serve_session_playlist(f"{channel_id}#a{audio_index}", segment_prefix)


# The playlist filename is "audio-N", not "N": when an M3U entry has no
# tvg-chno, Jellyfin falls back to a purely numeric URL filename as the channel
# number, which turned the audio channels into stray channels 1, 2, 3.
@app.api_route("/multiview/{channel_id}/audio-{audio_index}.m3u8", methods=["GET", "HEAD"])
async def serve_multiview_audio_playlist(channel_id: str, audio_index: int, request: Request):
    return await _serve_multiview_audio_playlist(channel_id, audio_index, request, f"audio-{audio_index}/seg/")


@app.api_route("/multiview/{channel_id}/audio/{audio_index}.m3u8", methods=["GET", "HEAD"])
async def serve_multiview_audio_playlist_legacy(channel_id: str, audio_index: int, request: Request):
    # Old URL form, kept until Jellyfin's next guide refresh picks up the new one.
    return await _serve_multiview_audio_playlist(channel_id, audio_index, request, f"{audio_index}/seg/")


@app.get("/multiview/{channel_id}/audio-{audio_index}/seg/{seq}.ts")
@app.get("/multiview/{channel_id}/audio/{audio_index}/seg/{seq}.ts")
async def serve_multiview_audio_segment(channel_id: str, audio_index: int, seq: int):
    if _multiview_audio_view(channel_id, audio_index) is None:
        raise HTTPException(status_code=404, detail="Multi-View channel not found")
    _touch_multiview_viewer(channel_id)
    return _serve_session_segment(f"{channel_id}#a{audio_index}", seq)


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
PLACEHOLDER_IDLE_SECONDS = bounded_float(os.getenv("PLACEHOLDER_IDLE_SECONDS", "900"), 900.0, 60.0, 86400.0)
_PLACEHOLDER_STATE: Optional[dict] = None
_PLACEHOLDER_LOCK = asyncio.Lock()
# Bumped per placeholder ffmpeg start: names its run dir and is part of its
# source key, so channel sessions treat a restarted placeholder (segment
# numbers back at 0) as a new source instead of a lagging edge.
_PLACEHOLDER_RUN_COUNTER = 0
# Last spawn failure(s): {"count", "last_failure", "last_error"}. The next start
# waits _multiview_backoff_seconds(count), so a broken placeholder ffmpeg is
# no longer respawned (and logged as an ERROR) on every session poll.
_PLACEHOLDER_FAILURE: Optional[dict] = None
# Turned off once drawtext failed with this ffmpeg build (no libfreetype, no
# usable font): later starts render the logo without the caption.
_PLACEHOLDER_DRAWTEXT_OK = True
_PLACEHOLDER_PENDING_DIRS: Set[Path] = set()
_DRAWTEXT_FAILURE_MARKERS = (
    "drawtext", "fontconfig", "freetype", "fontfile", "font file", "could not load font", "cannot find a valid font",
)


def _ffmpeg_filter_path(path: Path) -> str:
    """A path for a single-quoted filter option value: forward slashes, and
    the drive colon escaped (ffmpeg splits filter options on ':')."""
    return str(path).replace("\\", "/").replace(":", "\\:").replace("'", "'\\''")


def _placeholder_font_option() -> str:
    """drawtext font selection. Bundled Windows ffmpeg builds usually have no
    fontconfig configuration, so font='Sans' fails or falls back unpredictably
    there: point at a real Windows font file instead."""
    if sys.platform == "win32":
        fonts_dir = Path(os.environ.get("WINDIR") or os.environ.get("SystemRoot") or r"C:\Windows") / "Fonts"
        for name in ("segoeui.ttf", "arial.ttf", "tahoma.ttf", "verdana.ttf"):
            candidate = fonts_dir / name
            if candidate.is_file():
                return f"fontfile='{_ffmpeg_filter_path(candidate)}':"
        return ""
    return "font='Sans':"


def _build_placeholder_ffmpeg_args(out_dir: Path, drawtext: bool = True) -> List[str]:
    """Pure command-builder for the shared "No Signal" loop (no I/O besides
    locating a font, unit-testable)."""
    logo_path = _resource_path("assets/jellyball-logo.png")
    video_filter = (
        "scale=1920:1080:force_original_aspect_ratio=decrease,"
        "pad=1920:1080:(ow-iw)/2:(oh-ih)/2"
    )
    if drawtext:
        video_filter += (
            f",drawtext={_placeholder_font_option()}text='No Signal':fontcolor=white:fontsize=64:"
            "box=1:boxcolor=black@0.5:boxborderw=16:x=(w-text_w)/2:y=h-200"
        )
    # -re paces both inputs at real time: without it ffmpeg encoded this loop as
    # fast as the CPU allowed (pinning a core and racing segments far ahead of
    # the wall clock). 1080p30 with 2s segments so a channel that starts on the
    # placeholder and then goes live doesn't make Jellyfin lock in a low
    # resolution/frame rate from its initial probe, and so a cold start has a
    # playable buffer within a few seconds.
    return [
        "-y",
        "-hide_banner", "-nostats", "-loglevel", "warning",
        "-re", "-loop", "1", "-framerate", "30", "-i", str(logo_path),
        "-re", "-f", "lavfi", "-i", "anullsrc=r=48000:cl=stereo",
        "-vf", video_filter,
        "-c:v", "libx264", "-preset", "ultrafast", "-tune", "stillimage",
        "-b:v", "1M", "-maxrate", "1M", "-bufsize", "2M",
        "-g", "60", "-keyint_min", "60", "-sc_threshold", "0", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "64k", "-ac", "2", "-ar", "48000",
        "-f", "hls",
        "-hls_time", "2",
        "-hls_list_size", "8",
        "-hls_flags", "delete_segments+independent_segments",
        "-hls_segment_type", "mpegts",
        "-hls_segment_filename", "seg_%05d.ts",
        "index.m3u8",
    ]


def _looks_like_drawtext_failure(log_lines) -> bool:
    text = "\n".join(str(line) for line in log_lines).lower()
    return any(marker in text for marker in _DRAWTEXT_FAILURE_MARKERS)


def _placeholder_cooldown_remaining() -> float:
    record = _PLACEHOLDER_FAILURE
    if not record:
        return 0.0
    elapsed = time.monotonic() - record["last_failure"]
    return max(0.0, _multiview_backoff_seconds(record["count"]) - elapsed)


def _record_placeholder_failure(error: str) -> float:
    global _PLACEHOLDER_FAILURE
    record = _PLACEHOLDER_FAILURE or {"count": 0, "last_failure": 0.0, "last_error": ""}
    record["count"] += 1
    record["last_failure"] = time.monotonic()
    record["last_error"] = error
    _PLACEHOLDER_FAILURE = record
    return _multiview_backoff_seconds(record["count"])


async def _drain_placeholder_log(process: "asyncio.subprocess.Process", log_lines: "deque[str]") -> None:
    # Same chunked reader as Multi-View (readline() can die on CR-only progress output).
    await _drain_multiview_log("placeholder", process, log_lines)


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
    await _terminate_ffmpeg(state, "placeholder", term_timeout=5.0, kill_timeout=3.0)
    tasks = [state.get(key) for key in ("watch_task", "log_task") if state.get(key)]
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    _remove_tree_later(state["output_dir"], "placeholder")


async def _launch_placeholder_run(drawtext: bool) -> Tuple[bool, List[str]]:
    """One placeholder ffmpeg attempt (caller holds _PLACEHOLDER_LOCK):
    (ready, log lines of a failed attempt)."""
    global _PLACEHOLDER_STATE, _PLACEHOLDER_RUN_COUNTER
    _PLACEHOLDER_RUN_COUNTER += 1
    run_id = _PLACEHOLDER_RUN_COUNTER
    out_dir = PLACEHOLDER_OUTPUT_DIR / f"run{run_id}"
    _PLACEHOLDER_PENDING_DIRS.add(out_dir)
    try:
        await asyncio.to_thread(_prepare_run_dir, out_dir, 0)
        args = _build_placeholder_ffmpeg_args(out_dir, drawtext=drawtext)
        try:
            process = await asyncio.create_subprocess_exec(
                FFMPEG_PATH, *args,
                cwd=str(out_dir),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                creationflags=_ffmpeg_creationflags(),
            )
        except (FileNotFoundError, OSError) as exc:
            _log_failure("spawn placeholder ffmpeg", exc, logging.ERROR)
            _remove_tree_later(out_dir, "placeholder")
            return False, ["could not launch ffmpeg"]
        job = _create_run_job(process.pid)
        _write_run_pid_file(out_dir, process.pid)

        log_lines: "deque[str]" = deque(maxlen=200)
        watch_task = _spawn_background_task(_watch_placeholder_process(process), "watch placeholder ffmpeg")
        log_task = _spawn_background_task(_drain_placeholder_log(process, log_lines), "drain placeholder ffmpeg log")
        state = {
            "process": process,
            "job": job,
            "run_id": run_id,
            "output_dir": out_dir,
            "exited": False,
            "watch_task": watch_task,
            "log_task": log_task,
            "log_lines": log_lines,
            "ready": False,
            "last_access": time.monotonic(),
        }
        _PLACEHOLDER_STATE = state
        try:
            ready = await _wait_for_first_segment(out_dir, process, PLACEHOLDER_STARTUP_TIMEOUT_SECONDS)
        except asyncio.CancelledError:
            if _PLACEHOLDER_STATE is state:
                await _stop_placeholder_process()
            raise
        if ready and _PLACEHOLDER_STATE is state:
            state["ready"] = True
            return True, []
        lines = list(log_lines)
        if _PLACEHOLDER_STATE is state:
            await _stop_placeholder_process()
        return False, lines
    finally:
        _PLACEHOLDER_PENDING_DIRS.discard(out_dir)


async def _ensure_placeholder_running() -> bool:
    """Start the shared placeholder stream if it isn't already running. Idempotent;
    after a failed start it declines (returns False) until the backoff expires."""
    global _PLACEHOLDER_FAILURE, _PLACEHOLDER_DRAWTEXT_OK
    async with _PLACEHOLDER_LOCK:
        if _PLACEHOLDER_STATE and _PLACEHOLDER_STATE["process"].returncode is None and not _PLACEHOLDER_STATE.get("exited"):
            return True
        if not FFMPEG_AVAILABLE or _placeholder_cooldown_remaining() > 0:
            return False
        if _PLACEHOLDER_STATE is not None:  # a dead run: reap it before starting over
            await _stop_placeholder_process()

        lines: List[str] = []
        for _attempt in range(2):
            ready, lines = await _launch_placeholder_run(_PLACEHOLDER_DRAWTEXT_OK)
            if ready:
                _PLACEHOLDER_FAILURE = None
                return True
            if _PLACEHOLDER_DRAWTEXT_OK and _looks_like_drawtext_failure(lines):
                _PLACEHOLDER_DRAWTEXT_OK = False
                LOGGER.warning(
                    "placeholder ffmpeg: drawtext failed (%s); retrying without the 'No Signal' caption",
                    _multiview_error_from_log(lines),
                )
                continue
            break
        error = _multiview_error_from_log(lines)
        retry_in = _record_placeholder_failure(error)
        LOGGER.error("placeholder ffmpeg failed to produce a first segment: %s (retrying in %.0fs)", error, retry_in)
        return False


async def _serve_placeholder_stream(request: Request):
    ready = await _ensure_placeholder_running()
    if not ready:
        return Response(status_code=404, content="Stream unavailable")
    if _PLACEHOLDER_STATE:
        _PLACEHOLDER_STATE["last_access"] = time.monotonic()
    host = request.headers.get("host") or f"127.0.0.1:{PORT}"
    return RedirectResponse(url=f"http://{host}/placeholder/index.m3u8")


_PLACEHOLDER_SEGMENT_NAME_RE = re.compile(r"^seg_\d{5,}\.ts$")


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


# --- CHANNEL SESSIONS (proxy-built continuous playlists; see hls_session.py) ---

SESSION_IDLE_SECONDS = bounded_float(os.getenv("SESSION_IDLE_SECONDS", "60"), 60.0, 10.0, 3600.0)
STREAM_STARTUP_TIMEOUT = bounded_float(os.getenv("STREAM_STARTUP_TIMEOUT", "20"), 20.0, 3.0, 120.0)
# A channel whose source hasn't produced anything by STREAM_STARTUP_TIMEOUT plays
# No Signal for this long (then retries the real source) instead of answering
# 503: Jellyfin's ffmpeg does not retry a failed first open.
STARTUP_PLACEHOLDER_SECONDS = bounded_float(os.getenv("STARTUP_PLACEHOLDER_SECONDS", "30"), 30.0, 5.0, 600.0)
STREAM_MAX_BANDWIDTH = bounded_int(os.getenv("STREAM_MAX_BANDWIDTH", "0"), 0, 0, 1_000_000_000)
# Prefix of every placeholder source key: the actual key is
# PLACEHOLDER_SOURCE_KEY + (placeholder run id,). Test with _is_placeholder_key().
PLACEHOLDER_SOURCE_KEY = ("placeholder",)
HLS_MEDIA_TYPE = "application/vnd.apple.mpegurl"
_PLACEHOLDER_START_TASK: Optional[asyncio.Task] = None


def _media_client() -> Optional[httpx.AsyncClient]:
    """Dedicated pool for playlists/segments so scrape bursts can't starve playback."""
    return MEDIA_HTTP_CLIENT or SHARED_HTTP_CLIENT


async def _session_fetch(url: str, headers: Dict[str, str], max_bytes: int, timeout_seconds: float) -> Optional[FetchResult]:
    client = _media_client()
    if client is None:
        return None
    timeout = httpx.Timeout(connect=5.0, read=timeout_seconds, write=10.0, pool=3.0)
    try:
        # httpx read timeouts are per-read; also cap the whole transfer so a
        # trickling upstream can't hold a live segment for minutes.
        result = await asyncio.wait_for(
            _fetch_upstream_body(client, url, headers, max_bytes, timeout),
            timeout_seconds + 5.0,
        )
    except asyncio.TimeoutError:
        return None
    if result is None:
        return None
    status_code, effective_url, content_type, body, _ = result
    return FetchResult(status_code, effective_url, content_type, body)


def _request_placeholder_start() -> None:
    global _PLACEHOLDER_START_TASK
    if not FFMPEG_AVAILABLE or _placeholder_cooldown_remaining() > 0:
        return
    if _PLACEHOLDER_START_TASK is None or _PLACEHOLDER_START_TASK.done():
        _PLACEHOLDER_START_TASK = _spawn_background_task(_ensure_placeholder_running(), "start placeholder stream")


def _placeholder_source_key(run_id: int) -> tuple:
    return PLACEHOLDER_SOURCE_KEY + (run_id,)


def _is_placeholder_key(key) -> bool:
    """True for any placeholder source key, whatever placeholder run it names."""
    return bool(key) and tuple(key)[:len(PLACEHOLDER_SOURCE_KEY)] == PLACEHOLDER_SOURCE_KEY


def _placeholder_source() -> Optional[SourceSpec]:
    state = _PLACEHOLDER_STATE
    if state and state.get("ready") and state["process"].returncode is None and not state.get("exited"):
        state["last_access"] = time.monotonic()
        return SourceSpec(
            # Per placeholder run: a restarted placeholder ffmpeg numbers its
            # segments from 0 again, which under one constant key looked like
            # a lagging edge; a new key is a clean source switch instead.
            key=_placeholder_source_key(state.get("run_id", 0)),
            url=str(state["output_dir"] / "index.m3u8"),
            local=True,
            label="No Signal",
        )
    _request_placeholder_start()
    return None


def _candidate_source(candidate: dict) -> Optional[SourceSpec]:
    url = _validate_upstream_url(str(candidate.get("url") or ""))
    if not url:
        return None
    referer = str(candidate.get("referer") or "")
    if referer and not _validate_upstream_url(referer):
        referer = ""
    return SourceSpec(
        key=candidate_source_key(candidate),
        url=url,
        referer=referer,
        origin=str(candidate.get("origin") or ""),
        label=str(candidate.get("provider") or ""),
    )


def _resolve_session_source(channel_id: str) -> Optional[SourceSpec]:
    """What should this channel's session play right now? (pull model)"""
    if channel_id == PLACEHOLDER_SESSION_ID:
        return _placeholder_source()
    view = _multiview_view_for_session(channel_id)
    if view is not None:
        return _resolve_multiview_view_source(*view)
    data = stream_state.get(channel_id)
    if data is None:
        return None
    candidates = data.get("candidates") or []
    if not candidates or data.get("exhausted"):
        return _placeholder_source()
    if data.get("startup_placeholder_until", 0.0) > time.monotonic():
        return _placeholder_source()
    active_index = data.get("active_index", 0)
    if active_index >= len(candidates):
        active_index = 0
    return _candidate_source(candidates[active_index]) or _placeholder_source()


def _on_session_failure(channel_id: str, source_key: tuple, reason: str) -> None:
    if tuple(source_key) == PLACEHOLDER_SOURCE_KEY:
        return
    if _multiview_view_for_session(channel_id) is not None:
        _on_multiview_view_failure(channel_id, source_key, reason)
        return
    _spawn_background_task(request_failover(channel_id, source_key, reason), f"session failover {channel_id}")


def _on_session_incompatible(channel_id: str, source_key: tuple, reason: str) -> bool:
    """Returns True when a compatible standby exists (the session keeps going
    and picks it up), False to let a cold-starting session use the legacy proxy."""
    if _multiview_view_for_session(channel_id) is not None:
        return False
    data = stream_state.get(channel_id) or {}
    candidates = data.get("candidates") or []
    active_index = data.get("active_index", 0)
    has_alternative = (
        0 <= active_index < len(candidates)
        and candidate_source_key(candidates[active_index]) == tuple(source_key)
        and _pick_next_candidate(candidates, active_index) is not None
    )
    if has_alternative:
        # Mark it now so the next resolve can't hand the session the same source.
        candidates[active_index]["session_compatible"] = False
        candidates[active_index]["incompatible_at"] = time.time()
    _spawn_background_task(
        request_failover(channel_id, source_key, reason, incompatible=True),
        f"session incompatible failover {channel_id}",
    )
    return has_alternative


def _on_session_media_info(channel_id: str, source_key: tuple, has_audio: bool, signature: tuple) -> None:
    data = stream_state.get(channel_id)
    if not data:
        return
    for candidate in data.get("candidates") or []:
        if candidate_source_key(candidate) == tuple(source_key):
            candidate["has_audio"] = has_audio
            candidate["codec_signature"] = tuple(signature)
            break


SESSIONS = SessionRegistry(
    SessionHooks(
        fetch=lambda url, headers, max_bytes, timeout: _session_fetch(url, headers, max_bytes, timeout),
        headers_for=lambda referer, origin: _upstream_media_headers(referer, origin),
        resolve_source=lambda channel_id: _resolve_session_source(channel_id),
        report_failure=lambda channel_id, key, reason: _on_session_failure(channel_id, key, reason),
        report_incompatible=lambda channel_id, key, reason: _on_session_incompatible(channel_id, key, reason),
        on_media_info=lambda channel_id, key, has_audio, sig: _on_session_media_info(channel_id, key, has_audio, sig),
    ),
    SessionConfig(idle_timeout=SESSION_IDLE_SECONDS, bandwidth_cap=STREAM_MAX_BANDWIDTH),
)


def _hls_response(text: str) -> Response:
    return Response(
        content=text,
        media_type=HLS_MEDIA_TYPE,
        headers={"Cache-Control": "no-cache", "Access-Control-Allow-Origin": "*"},
    )


def _start_on_placeholder(channel_id: str) -> bool:
    data = stream_state.get(channel_id)
    if not data or data.get("type") == "multiview" or not FFMPEG_AVAILABLE:
        return False
    data["startup_placeholder_until"] = time.monotonic() + STARTUP_PLACEHOLDER_SECONDS
    LOGGER.info("Stream slow to start; showing No Signal meanwhile channel=%s", channel_id)
    _request_placeholder_start()
    SESSIONS.poke(channel_id)
    return True


async def _serve_session_playlist(session_id: str, segment_prefix: str, legacy=None) -> Response:
    session = SESSIONS.get(session_id)
    session.touch()
    if session.state == "legacy" and legacy is not None:
        return await legacy()
    await session.wait_ready(STREAM_STARTUP_TIMEOUT)
    if session.state == "legacy" and legacy is not None:
        return await legacy()
    if not session.window and _start_on_placeholder(session_id):
        await session.wait_ready(10.0)
    if not session.window:
        return Response(
            status_code=503,
            content="Stream is starting",
            headers={"Retry-After": "2", "Cache-Control": "no-cache"},
        )
    return _hls_response(session.render_playlist(segment_prefix))


def _serve_session_segment(session_id: str, seq: int) -> Response:
    session = SESSIONS.peek(session_id)
    if session is None:
        return Response(status_code=404, content="Segment not found")
    session.touch()
    segment = session.get_segment(seq)
    if segment is None:
        return Response(status_code=404, content="Segment not found")
    return Response(
        content=segment.data,
        media_type="video/mp2t",
        headers={"Cache-Control": "public, max-age=120", "Access-Control-Allow-Origin": "*"},
    )


async def _serve_channel_playlist(team_id: str, request: Request) -> Response:
    if team_id == PLACEHOLDER_SESSION_ID and FFMPEG_AVAILABLE:
        # Stand-in input for a Multi-View member that no longer exists.
        return await _serve_session_playlist(team_id, f"{team_id}/seg/")
    data = stream_state.get(team_id)
    if not data:
        return Response(status_code=404, content="Stream unavailable")
    if request.method == "HEAD":
        # Jellyfin's M3U tuner HEADs extensionless URLs to decide between
        # raw-TS sharing and ffmpeg HLS input; always say HLS.
        return Response(status_code=200, media_type=HLS_MEDIA_TYPE, headers={"Cache-Control": "no-cache"})
    if data.get("type") == "multiview":
        _touch_multiview_viewer(team_id)
    _maybe_record_playback_event(team_id)

    async def legacy():
        return await _legacy_proxy_stream(team_id, request)

    return await _serve_session_playlist(team_id, f"{team_id}/seg/", legacy=None if data.get("type") == "multiview" else legacy)


@app.api_route("/stream/{team_id}.m3u8", methods=["GET", "HEAD"])
async def stream_playlist(team_id: str, request: Request):
    return await _serve_channel_playlist(team_id, request)


@app.api_route("/stream/{team_id}/seg/{seq}.ts", methods=["GET", "HEAD"])
async def stream_segment(team_id: str, seq: int):
    if team_id in stream_state and stream_state[team_id].get("type") == "multiview":
        _touch_multiview_viewer(team_id)
    return _serve_session_segment(team_id, seq)


@app.api_route("/stream/{team_id}", methods=["GET", "HEAD"])
async def proxy_stream(team_id: str, request: Request, provider: str = ""):
    """Extensionless alias kept for existing Jellyfin tuner configs. `?provider=`
    pins one provider for debugging via the legacy passthrough proxy."""
    if provider and request.method == "GET" and team_id in stream_state:
        return await _legacy_proxy_stream(team_id, request, provider)
    return await _serve_channel_playlist(team_id, request)


async def _legacy_proxy_stream(team_id: str, request: Request, provider: str = ""):
    """Pre-session passthrough proxy: rewrites the upstream playlist's URIs to
    /substream.m3u8 and /chunk*. Only used for sources a channel session can't
    normalize (fMP4, separate audio renditions, SAMPLE-AES) and for ?provider=."""
    data = stream_state.get(team_id)
    if not data:
        return Response(status_code=404, content="Stream unavailable")
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
    proxy_origin = _public_base_url(request)

    owns_client = _media_client() is None
    client = _media_client() or httpx.AsyncClient(follow_redirects=False, timeout=12.0, http2=True)
    try:
        result = await _fetch_upstream_body(client, target_url, headers, MAX_MANIFEST_BYTES)
        if result is None:
            return Response(status_code=502, content="Upstream manifest unavailable")
        status_code, effective_url, _, body, _ = result
        if status_code != 200:
            return Response(status_code=status_code)
        manifest_text = body.decode("utf-8", errors="replace")
        await _ensure_startup_buffer(
            f"{effective_url}\0{referer}\0{origin}",
            client,
            _startup_media_urls(manifest_text, effective_url),
            referer,
            origin,
        )
        rewritten = rewrite_m3u8(manifest_text, effective_url, referer, proxy_origin, origin)
        return _hls_response(rewritten)
    except Exception as exc:
        _log_failure(f"proxy manifest team={team_id}", exc, logging.ERROR)
        return Response(status_code=502, content="Upstream manifest unavailable")
    finally:
        if owns_client:
            await client.aclose()


@app.get("/substream.m3u8")
async def proxy_substream(request: Request, url: str, ref: str = "", org: str = "", sig: str = ""):
    decoded_url = url
    decoded_ref = ref or ""
    decoded_origin = org or ""
    if not _relay_signature_ok(decoded_url, decoded_ref, decoded_origin, sig):
        return Response(status_code=403, content="Unsigned relay URL")
    if not _validate_upstream_url(decoded_url):
        return Response(status_code=400, content="Invalid upstream manifest URL")
    if decoded_ref and not _validate_upstream_url(decoded_ref):
        return Response(status_code=400, content="Invalid upstream referer URL")
    headers = _upstream_media_headers(decoded_ref, decoded_origin)
    proxy_origin = _public_base_url(request)
    
    owns_client = _media_client() is None
    client = _media_client() or httpx.AsyncClient(follow_redirects=False, timeout=12.0, http2=True)
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
async def proxy_resource(request: Request, url: str, ref: str = "", org: str = "", sig: str = ""):
    """Proxy HLS key and initialization resources with the original media type."""
    decoded_url = url
    decoded_ref = ref or ""
    decoded_origin = org or ""
    if not _relay_signature_ok(decoded_url, decoded_ref, decoded_origin, sig):
        return Response(status_code=403, content="Unsigned relay URL")
    if not _validate_upstream_url(decoded_url):
        return Response(status_code=400, content="Invalid upstream resource URL")
    if decoded_ref and not _validate_upstream_url(decoded_ref):
        return Response(status_code=400, content="Invalid upstream referer URL")

    owns_client = _media_client() is None
    client = _media_client() or httpx.AsyncClient(
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

    client = _media_client()
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


async def _open_upstream_media(
    client: httpx.AsyncClient,
    url: str,
    headers: Dict[str, str],
) -> Tuple[Optional[object], Optional[httpx.Response]]:
    """Open a streaming GET, following redirects manually so every hop is
    SSRF-validated. Returns (stream context, response) or (None, None)."""
    current_url = url
    for _ in range(MAX_UPSTREAM_REDIRECTS + 1):
        safe_url = await validate_http_url_async(current_url)
        if not safe_url:
            return None, None
        stream_ctx = client.stream("GET", safe_url, headers=headers, timeout=STREAM_REQUEST_TIMEOUT, follow_redirects=False)
        response = await stream_ctx.__aenter__()
        if 300 <= response.status_code < 400:
            location = response.headers.get("location")
            await stream_ctx.__aexit__(None, None, None)
            if not location:
                return None, None
            current_url = urllib.parse.urljoin(str(response.url), location)
            continue
        return stream_ctx, response
    return None, None


class _UpstreamBodyInterrupted(Exception):
    """Raised mid-body so the server aborts the connection instead of ending a
    chunked response cleanly: a cleanly-ended truncated segment looks complete
    to ffmpeg, while an aborted one makes it retry."""


class _ClosingStreamingResponse(StreamingResponse):
    """StreamingResponse that always runs `on_close`, even when the client
    disconnects before the body generator starts (its own `finally` then never
    runs, which leaked the upstream stream and its pool slot)."""

    def __init__(self, *args, on_close=None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._on_close = on_close

    async def __call__(self, scope, receive, send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            if self._on_close is not None:
                await self._on_close()


@app.get("/chunk.mp4")
@app.get("/chunk.aac")
@app.get("/chunk.vtt")
@app.get("/chunk.ts")
@app.get("/chunk")
async def proxy_chunk(request: Request, url: str, ref: str = "", org: str = "", sig: str = ""):
    """Legacy segment relay (for sources channel sessions can't normalize)."""
    decoded_url = url
    decoded_ref = ref or ""
    decoded_origin = org or ""
    if not _relay_signature_ok(decoded_url, decoded_ref, decoded_origin, sig):
        return Response(status_code=403, content="Unsigned relay URL")
    if not _validate_upstream_url(decoded_url):
        return Response(status_code=400, content="Invalid upstream chunk URL")
    if decoded_ref and not _validate_upstream_url(decoded_ref):
        return Response(status_code=400, content="Invalid upstream referer URL")

    range_header = request.headers.get("range", "").strip()
    if range_header and not re.fullmatch(r"bytes=\d*-\d*", range_header):
        range_header = ""
    # Same key format as prefetch/startup warm (previously this had an extra
    # trailing field, so prefetched chunks could never be served from cache).
    cache_key = f"{decoded_url}\0{decoded_ref}\0{decoded_origin}"
    media_type = _chunk_media_type(decoded_url)
    cache_headers = {"Access-Control-Allow-Origin": "*", "Cache-Control": "public, max-age=15"}

    if not range_header:
        cached_bytes = await CHUNK_CACHE.get(cache_key)
        if cached_bytes is not None:
            if PREFETCH_CHUNK_COUNT > 0:
                _spawn_background_task(prefetch_next_chunks(decoded_url, decoded_ref, decoded_origin), "prefetch cached stream chunks")
            return Response(content=cached_bytes, media_type=media_type, headers=cache_headers)

    owns_singleflight = False
    if not range_header:
        for _ in range(20):
            with _PREFETCH_LOCK:
                if cache_key not in _PREFETCH_IN_FLIGHT:
                    _PREFETCH_IN_FLIGHT.add(cache_key)
                    owns_singleflight = True
                    break
            await asyncio.sleep(0.05)
            cached_bytes = await CHUNK_CACHE.get(cache_key)
            if cached_bytes is not None:
                return Response(content=cached_bytes, media_type=media_type, headers=cache_headers)

    def release_singleflight() -> None:
        if owns_singleflight:
            with _PREFETCH_LOCK:
                _PREFETCH_IN_FLIGHT.discard(cache_key)

    headers = _upstream_media_headers(decoded_ref, decoded_origin)
    if range_header:
        headers["Range"] = range_header
    elif PREFETCH_CHUNK_COUNT > 0:
        _spawn_background_task(prefetch_next_chunks(decoded_url, decoded_ref, decoded_origin), "prefetch stream chunks")

    owns_client = _media_client() is None
    client = _media_client() or httpx.AsyncClient(timeout=STREAM_REQUEST_TIMEOUT, follow_redirects=False, http2=True)

    # Open upstream *before* committing a response status: previously the 200
    # went out first and any upstream error became an empty "successful"
    # segment that ffmpeg never retried. Retry only here, before any byte.
    stream_ctx = None
    response = None
    status_code = 502
    try:
        for attempt in range(3):
            try:
                stream_ctx, response = await _open_upstream_media(client, decoded_url, headers)
            except httpx.HTTPError as exc:
                LOGGER.debug("Chunk open attempt failed attempt=%d error=%s", attempt + 1, type(exc).__name__)
                stream_ctx, response = None, None
            if response is None:
                status_code = 502
            elif response.status_code in (200, 206):
                break
            else:
                status_code = response.status_code
                await stream_ctx.__aexit__(None, None, None)
                stream_ctx, response = None, None
                if status_code not in (429, 500, 502, 503, 504):
                    break
            if attempt < 2:
                await asyncio.sleep(0.25 * (attempt + 1))
    except BaseException:
        # Anything else (client gone mid-retry, an unexpected error): never
        # leave the single-flight key or an open upstream stream behind.
        if stream_ctx is not None:
            await stream_ctx.__aexit__(None, None, None)
        release_singleflight()
        if owns_client:
            await client.aclose()
        raise

    if response is None:
        release_singleflight()
        if owns_client:
            await client.aclose()
        return Response(status_code=status_code, content="Upstream chunk unavailable")

    out_headers = dict(cache_headers) if not range_header else {"Access-Control-Allow-Origin": "*", "Cache-Control": "no-cache"}
    for key in ("content-range", "accept-ranges"):
        if response.headers.get(key):
            out_headers[key] = response.headers[key]
    opened_ctx, opened_response = stream_ctx, response
    closed = False

    async def close_upstream() -> None:
        nonlocal closed
        if closed:
            return
        closed = True
        try:
            await opened_ctx.__aexit__(None, None, None)
        finally:
            release_singleflight()
            if owns_client:
                await client.aclose()

    async def relay():
        chunk_buffer = bytearray()
        cacheable = not range_header
        try:
            async for block in opened_response.aiter_bytes(chunk_size=131072):
                if cacheable:
                    if len(chunk_buffer) + len(block) <= MAX_CACHEABLE_CHUNK_BYTES:
                        chunk_buffer.extend(block)
                    else:
                        cacheable = False
                        chunk_buffer.clear()
                yield block
            if cacheable and chunk_buffer:
                await CHUNK_CACHE.put(cache_key, bytes(chunk_buffer), STREAM_CHUNK_CACHE_TTL)
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
            _log_failure("relay upstream chunk", exc)
            raise _UpstreamBodyInterrupted() from exc
        finally:
            await close_upstream()

    return _ClosingStreamingResponse(
        relay(), status_code=opened_response.status_code, media_type=media_type, headers=out_headers,
        on_close=close_upstream,
    )


@app.api_route("/playlist.m3u", methods=["GET", "HEAD"], response_class=PlainTextResponse)
async def generate_m3u(request: Request):
    base_url = _public_base_url(request)
    lines = ["#EXTM3U"]
    for team_id, data in stream_state.items():
        if not _channel_listed(data):
            continue
        # The .m3u8 extension makes Jellyfin treat the URL as an HLS manifest
        # (always through its own ffmpeg, never "direct play" of our URL or
        # raw-TS sharing). Channel names stay stable: health is shown on the
        # dashboard, not appended to the name (which renamed channels in
        # Jellyfin on every guide refresh).
        name = str(data.get("name") or team_id)
        tvg_id = _channel_tvg_id(team_id, data)
        logo = _channel_logo_url(data)
        logo_attr = f' tvg-logo="{_m3u_attribute(logo)}"' if logo else ""
        group = _m3u_attribute(_channel_group_title(data))
        lines.append(
            f'#EXTINF:-1 tvg-id="{_m3u_attribute(tvg_id)}" '
            f'tvg-name="{_m3u_attribute(name)}"{logo_attr} '
            f'group-title="{group}",{_m3u_title(name)}'
        )
        lines.append(f"{base_url}/stream/{urllib.parse.quote(team_id, safe='')}.m3u8")
        for audio_id, audio_name, path in _multiview_audio_channels(team_id, data):
            lines.append(
                f'#EXTINF:-1 tvg-id="{_m3u_attribute(audio_id)}" '
                f'tvg-name="{_m3u_attribute(audio_name)}"{logo_attr} '
                f'group-title="{group}",{_m3u_title(audio_name)}'
            )
            lines.append(f"{base_url}{path}")
    return "\n".join(lines)


def _multiview_audio_tvg_id(base_tvg_id: str, member_team_id: str) -> str:
    """tvg-id for one Multi-View per-audio channel.

    Keyed by the member's own channel id (stable even if the member list is
    reordered) rather than its positional index, and always ending in the
    fixed, non-numeric literal ".audio" - regardless of what the member id
    itself looks like. Jellyfin's M3U tuner derives a channel number from
    tvg-id (or channel-id) when tvg-chno is absent - see
    M3uParser.GetChannelNumber upstream - so ending in a constant word
    instead of a bare digit means it can never be mistaken for one.
    """
    return f"{base_tvg_id}.{member_team_id}.audio"


def _multiview_audio_channels(channel_id: str, data: dict) -> List[Tuple[str, str, str]]:
    """(tvg-id, display name, URL path) for each per-audio Multi-View channel."""
    if data.get("type") != "multiview" or not MULTIVIEW_AUDIO_CHANNELS:
        return []
    base_tvg_id = _channel_tvg_id(channel_id, data)
    mv_name = str(data.get("name") or channel_id)
    return [
        (
            _multiview_audio_tvg_id(base_tvg_id, member),
            f"🔊 {_multiview_member_label(member)} · {mv_name}",
            f"/multiview/{channel_id}/audio-{index}.m3u8",
        )
        for index, member in enumerate(data.get("member_team_ids") or [])
    ]


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


def _channel_programmes(
    team_id: str,
    data: dict,
    guide_start: datetime,
    guide_end: datetime,
    tvguide_schedules: Dict[str, List[dict]],
) -> List[dict]:
    """Raw (unescaped) programme blocks for one ordinary (non-Multi-View) channel.

    Each entry is {start, stop, title, desc, category, icon}; `category` and
    `icon` may be "" meaning the XML renderer omits that tag. This mirrors the
    guide logic that used to live inline in generate_xmltv() - kept as its own
    helper so a Multi-View channel can pull a member's own guide data (see
    _multiview_member_programmes) to build its composite guide.
    """
    name = str(data.get("name") or team_id)
    logo = _channel_logo_url(data)
    channel_id = _channel_tvg_id(team_id, data)
    programmes: List[dict] = []

    def block(start: datetime, stop: datetime, title: str, desc: str, category: str = "") -> None:
        programmes.append({"start": start, "stop": stop, "title": title, "desc": desc,
                           "category": category, "icon": logo})

    if _channel_is_off_season(data):
        resumes = _season_resume_label(data.get("category", ""))
        block(guide_start, guide_end,
              f"{name}: Off-season" + (f" (resumes {resumes})" if resumes else ""),
              f"{name} is between seasons. The channel returns to the guide automatically"
              + (f" when the season resumes around {resumes}." if resumes else " when the season resumes."))
        return programmes

    dt_start, dt_stop = parse_team_schedule(data)
    has_specific_game = bool(
        dt_start and dt_stop and dt_stop >= guide_start and dt_start <= guide_end
    )

    if has_specific_game:
        if dt_start > guide_start:
            block(guide_start, min(dt_start, guide_end), f"{name} Standby",
                  f"Waiting for the scheduled {name} broadcast.")
        block(max(dt_start, guide_start), min(dt_stop, guide_end), f"{name} Scheduled Event",
              f"Scheduled live stream window for {name}. Searching starts one hour before the event "
              "and continues one hour after the scheduled end.")
        if dt_stop < guide_end:
            block(max(dt_stop, guide_start), guide_end, f"{name} Standby",
                  f"Post-event standby for {name}; the next scheduled event will refresh this guide.")
    else:
        programs = tvguide_schedules.get(channel_id, [])
        if _channel_is_always_live(data) and programs:
            for prog in programs:
                p_start = datetime.fromtimestamp(prog.get("startTime", 0), tz=timezone.utc)
                p_stop = datetime.fromtimestamp(prog.get("endTime", 0), tz=timezone.utc)
                if p_stop <= guide_start or p_start >= guide_end:
                    continue
                block(max(p_start, guide_start), min(p_stop, guide_end),
                      prog.get("title") or f"{name} Live",
                      prog.get("description") or f"Live broadcast on {name}.", "Sports")
        elif _channel_is_always_live(data):
            block(guide_start, guide_end, f"{name} Live",
                  f"Always-live sports channel for {name}; the linear stream is monitored continuously.", "Sports")
        else:
            block(guide_start, guide_end, f"{name} Standby",
                  f"No verified scheduled event is available for {name} in the next {GUIDE_HORIZON_DAYS} days.")
    return programmes


def _programme_xml_lines(channel_id: str, programmes: List[dict]) -> List[str]:
    """Render _channel_programmes()-shaped dicts as <programme> XML lines."""
    lines: List[str] = []
    channel_id_esc = _xml_attr(channel_id)
    for p in programmes:
        lines.append(
            f'  <programme channel="{channel_id_esc}" start="{xmltv_ts(p["start"])}" stop="{xmltv_ts(p["stop"])}">'
        )
        lines.append(f'    <title>{_xml_text(p["title"])}</title>')
        if p.get("category"):
            lines.append(f'    <category>{_xml_text(p["category"])}</category>')
        lines.append(f'    <desc>{_xml_text(p["desc"])}</desc>')
        if p.get("icon"):
            lines.append(f'    <icon src="{_xml_attr(p["icon"])}" />')
        lines.append('  </programme>')
    return lines


MULTIVIEW_PANE_LABELS = {
    "side_by_side_2": ["Left", "Right"],
    "grid_2x2": ["Top-left", "Top-right", "Bottom-left", "Bottom-right"],
}
MULTIVIEW_MAX_PROGRAMMES = 500
MULTIVIEW_MIN_INTERVAL_SECONDS = 300.0
_MULTIVIEW_FILLER_TITLE_MARKERS = ("Standby", "No verified scheduled event")


def _multiview_pane_labels(layout: str, count: int) -> List[str]:
    labels = MULTIVIEW_PANE_LABELS.get(layout)
    if labels and len(labels) == count:
        return labels
    return [f"Pane {i + 1}" for i in range(count)]


def _is_filler_programme_title(title: str) -> bool:
    return any(marker in title for marker in _MULTIVIEW_FILLER_TITLE_MARKERS)


def _multiview_member_programmes(
    member_team_ids: List[str],
    guide_start: datetime,
    guide_end: datetime,
    tvguide_schedules: Dict[str, List[dict]],
) -> Dict[str, List[dict]]:
    """Each member's own programme list, or a single "No Signal" block for a
    member that has since been removed (member_team_ids may reference a
    channel id no longer in stream_state)."""
    result: Dict[str, List[dict]] = {}
    for member_id in member_team_ids:
        member_data = stream_state.get(member_id)
        if member_data is None:
            result[member_id] = [{
                "start": guide_start, "stop": guide_end,
                "title": "No Signal", "desc": "", "category": "", "icon": "",
            }]
        else:
            result[member_id] = _channel_programmes(
                member_id, member_data, guide_start, guide_end, tvguide_schedules
            )
    return result


def _programme_at(programmes: List[dict], instant: datetime) -> Optional[dict]:
    for p in programmes:
        if p["start"] <= instant < p["stop"]:
            return p
    return programmes[-1] if programmes else None


def _multiview_boundaries(
    member_programmes: Dict[str, List[dict]], guide_start: datetime, guide_end: datetime
) -> List[datetime]:
    """Every member programme start/stop within the guide window, plus the
    window's own edges - the points at which the composite guide can change."""
    points = {guide_start, guide_end}
    for programmes in member_programmes.values():
        for p in programmes:
            if guide_start <= p["start"] <= guide_end:
                points.add(p["start"])
            if guide_start <= p["stop"] <= guide_end:
                points.add(p["stop"])
    return sorted(points)


def _merge_short_intervals(
    boundaries: List[datetime], min_seconds: float = MULTIVIEW_MIN_INTERVAL_SECONDS
) -> List[Tuple[datetime, datetime]]:
    """Turn sorted boundary points into (start, stop) intervals, folding any
    interval shorter than `min_seconds` into its neighbor so the guide doesn't
    fill up with sliver-length programmes."""
    if len(boundaries) < 2:
        return []
    intervals: List[Tuple[datetime, datetime]] = []
    start = boundaries[0]
    for i in range(1, len(boundaries)):
        stop = boundaries[i]
        is_last = i == len(boundaries) - 1
        if (stop - start).total_seconds() < min_seconds and not is_last:
            continue  # keep accumulating rather than committing a short interval
        intervals.append((start, stop))
        start = stop
    if len(intervals) >= 2 and (intervals[-1][1] - intervals[-1][0]).total_seconds() < min_seconds:
        prev_start, _ = intervals[-2]
        _, last_stop = intervals[-1]
        intervals[-2] = (prev_start, last_stop)
        intervals.pop()
    return intervals


def _cap_intervals(
    intervals: List[Tuple[datetime, datetime]], cap: int = MULTIVIEW_MAX_PROGRAMMES
) -> List[Tuple[datetime, datetime]]:
    """Repeatedly merge the shortest interval into its shorter neighbor until
    at most `cap` remain, so a channel can never emit an unbounded number of
    programmes."""
    intervals = list(intervals)
    while len(intervals) > cap:
        durations = [(b - a).total_seconds() for a, b in intervals]
        idx = min(range(len(durations)), key=lambda i: durations[i])
        if idx == 0:
            merge_idx = 0
        elif idx == len(intervals) - 1:
            merge_idx = idx - 1
        else:
            merge_idx = idx - 1 if durations[idx - 1] <= durations[idx + 1] else idx
        a_start, _ = intervals[merge_idx]
        _, b_stop = intervals[merge_idx + 1]
        intervals[merge_idx] = (a_start, b_stop)
        intervals.pop(merge_idx + 1)
    return intervals


def _cap_programme_dicts(entries: List[dict], cap: int = MULTIVIEW_MAX_PROGRAMMES) -> List[dict]:
    if len(entries) <= cap:
        return entries
    capped = list(entries[:cap])
    capped[-1] = dict(capped[-1])
    capped[-1]["stop"] = entries[-1]["stop"]
    return capped


def _multiview_pane_title(member_id: str, programme: Optional[dict], present: bool) -> str:
    """The text to show for one pane: the real programme title, or the
    member's own display name when there's nothing but filler ("... Standby"
    / "No verified scheduled event"), or "No Signal" for a removed member."""
    if not present:
        return "No Signal"
    title = str((programme or {}).get("title") or "")
    if not title or _is_filler_programme_title(title):
        return _multiview_member_label(member_id)
    return title


def _multiview_pane_info(
    member_team_ids: List[str],
    layout: str,
    member_programmes: Dict[str, List[dict]],
    instant: datetime,
) -> List[Tuple[str, str, str]]:
    """(position label, current pane title, member display name) for every
    pane, sampled at `instant`. `member display name` is "" for a removed
    member (nothing to show in parentheses)."""
    labels = _multiview_pane_labels(layout, len(member_team_ids))
    info: List[Tuple[str, str, str]] = []
    for label, member_id in zip(labels, member_team_ids):
        present = member_id in stream_state
        programme = _programme_at(member_programmes.get(member_id) or [], instant)
        pane_title = _multiview_pane_title(member_id, programme, present)
        member_name = _multiview_member_label(member_id) if present else ""
        info.append((label, pane_title, member_name))
    return info


def _multiview_pane_desc_lines(pane_info: List[Tuple[str, str, str]]) -> List[str]:
    lines = []
    for label, pane_title, member_name in pane_info:
        if member_name and pane_title != member_name:
            lines.append(f"{label}: {pane_title} ({member_name})")
        else:
            lines.append(f"{label}: {pane_title}")
    return lines


def _multiview_programmes(
    channel_id: str,
    data: dict,
    guide_start: datetime,
    guide_end: datetime,
    tvguide_schedules: Dict[str, List[dict]],
) -> Tuple[List[dict], Dict[str, List[dict]]]:
    """Programme dicts for a Multi-View main channel, plus one programme list
    per per-audio channel (keyed by the audio channel's own tvg-id, see
    _multiview_audio_tvg_id / _multiview_audio_channels).

    The main channel's guide window is split at every member's own programme
    boundary (so the composite title/desc only changes when a pane's content
    actually changes), short slivers are merged into a neighbor, and the
    result is capped so a Multi-View channel can never flood the guide.
    """
    member_team_ids = list(data.get("member_team_ids") or [])
    layout = data.get("layout") or ""
    logo = _channel_logo_url(data)
    mv_name = str(data.get("name") or channel_id)
    base_tvg_id = _channel_tvg_id(channel_id, data)

    if not member_team_ids:
        # Malformed/legacy entry with no members recorded - fall back to a
        # single placeholder block rather than emitting an empty title.
        return (
            [{
                "start": guide_start, "stop": guide_end,
                "title": f"{mv_name} Live", "desc": mv_name,
                "category": "Sports", "icon": logo,
            }],
            {},
        )

    member_programmes = _multiview_member_programmes(
        member_team_ids, guide_start, guide_end, tvguide_schedules
    )
    boundaries = _multiview_boundaries(member_programmes, guide_start, guide_end)
    intervals = _cap_intervals(_merge_short_intervals(boundaries))

    active_audio_id = data.get("active_audio_team_id") or member_team_ids[0]
    active_audio_name = _multiview_member_label(active_audio_id)

    main_programmes: List[dict] = []
    for start, stop in intervals:
        pane_info = _multiview_pane_info(member_team_ids, layout, member_programmes, start)
        title = " | ".join(pane_title for _, pane_title, _ in pane_info)
        desc_lines = _multiview_pane_desc_lines(pane_info)
        desc_lines.append(f"Audio: {active_audio_name} — change channel to the 🔊 channels to switch audio")
        main_programmes.append({
            "start": start, "stop": stop,
            "title": title, "desc": "\n".join(desc_lines),
            "category": "Sports", "icon": logo,
        })

    audio_programmes: Dict[str, List[dict]] = {}
    for member_id in member_team_ids:
        audio_id = _multiview_audio_tvg_id(base_tvg_id, member_id)
        present = member_id in stream_state
        member_name = _multiview_member_label(member_id)
        entries: List[dict] = []
        for p in member_programmes.get(member_id) or []:
            pane_info = _multiview_pane_info(member_team_ids, layout, member_programmes, p["start"])
            pane_title = _multiview_pane_title(member_id, p, present)
            desc_lines = [f"Audio from {member_name} in {mv_name}."]
            desc_lines.extend(_multiview_pane_desc_lines(pane_info))
            entries.append({
                "start": p["start"], "stop": p["stop"],
                "title": f"🔊 {pane_title}", "desc": "\n".join(desc_lines),
                "category": "Sports", "icon": logo,
            })
        audio_programmes[audio_id] = _cap_programme_dicts(entries)

    return main_programmes, audio_programmes


@app.api_route("/epg.xml", methods=["GET", "HEAD"], response_class=PlainTextResponse)
async def generate_xmltv(request: Request = None):
    now_utc = datetime.now(timezone.utc)
    xml = ['<?xml version="1.0" encoding="UTF-8"?>', '<tv>']

    for team_id, data in stream_state.items():
        if not _channel_listed(data):
            continue
        channel_id = _channel_tvg_id(team_id, data)
        name_esc = _xml_text(data["name"])
        logo = _channel_logo_url(data)
        icon_tag = f'\n    <icon src="{_xml_attr(logo)}" />' if logo else ""
        xml.append(f'  <channel id="{_xml_attr(channel_id)}">')
        xml.append(f'    <display-name>{name_esc}</display-name>{icon_tag}')
        xml.append('  </channel>')
        for audio_id, audio_name, _ in _multiview_audio_channels(team_id, data):
            xml.append(f'  <channel id="{_xml_attr(audio_id)}">')
            xml.append(f'    <display-name>{_xml_text(audio_name)}</display-name>{icon_tag}')
            xml.append('  </channel>')

    guide_start = now_utc - timedelta(hours=1)
    guide_end = now_utc + timedelta(days=GUIDE_HORIZON_DAYS)
    tvguide_schedules = await _fetch_tvguide_epg()

    for team_id, data in stream_state.items():
        if not _channel_listed(data):
            continue
        channel_id = _channel_tvg_id(team_id, data)

        if data.get("type") == "multiview":
            main_programmes, audio_programmes = _multiview_programmes(
                team_id, data, guide_start, guide_end, tvguide_schedules
            )
            xml.extend(_programme_xml_lines(channel_id, main_programmes))
            for audio_id, programmes in audio_programmes.items():
                xml.extend(_programme_xml_lines(audio_id, programmes))
        else:
            programmes = _channel_programmes(team_id, data, guide_start, guide_end, tvguide_schedules)
            xml.extend(_programme_xml_lines(channel_id, programmes))

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
        session = SESSIONS.peek(team_id)
        channels.append({
            "team_id": team_id,
            "name": data.get("name", team_id),
            "healthy": bool(data.get("is_healthy")),
            "watching": bool(session is not None and session.is_watched()),
            "on_placeholder": bool(session is not None and session.is_watched() and _session_on_placeholder(session)),
            "exhausted": bool(data.get("exhausted")),
            "failover_count": int(data.get("failover_count", 0)),
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


def _session_status(channel_id: str, snapshot: dict) -> dict:
    data = stream_state.get(channel_id) or {}
    candidates = data.get("candidates") or []
    active_index = data.get("active_index", 0)
    active = candidates[active_index] if 0 <= active_index < len(candidates) else {}
    source_key = snapshot.get("source_key") or []
    return {
        **snapshot,
        "name": data.get("name", channel_id),
        "on_placeholder": bool(source_key and source_key[0] == "placeholder"),
        "exhausted": bool(data.get("exhausted")),
        "active_provider": active.get("provider"),
        "codec_signature": list(active.get("codec_signature") or []) or None,
        "failover_count": int(data.get("failover_count", 0)),
        "last_failover": data.get("last_failover"),
        "candidate_count": len(candidates),
    }


@app.get("/api/sessions")
async def api_sessions(auth: bool = Depends(verify_dashboard_auth)):
    """Live per-channel playback state: what each running channel session is
    playing, its throughput/latency, and its failover history."""
    return {
        "sessions": {cid: _session_status(cid, snap) for cid, snap in SESSIONS.snapshot().items()},
    }


def _prometheus_label(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", " ").replace('"', '\\"')


@app.get("/metrics", response_class=PlainTextResponse)
async def prometheus_metrics(auth: bool = Depends(verify_dashboard_auth)):
    """Prometheus text exposition of the main health numbers (dashboard auth applies)."""
    lines = [
        "# HELP jellyball_channels Configured channels.",
        "# TYPE jellyball_channels gauge",
        f"jellyball_channels {len(stream_state)}",
        "# HELP jellyball_channel_healthy 1 when the channel's active source is healthy.",
        "# TYPE jellyball_channel_healthy gauge",
    ]
    for team_id, data in stream_state.items():
        lines.append(f'jellyball_channel_healthy{{channel="{_prometheus_label(team_id)}"}} {1 if data.get("is_healthy") else 0}')
    lines += [
        "# HELP jellyball_channel_failovers_total Failovers since start.",
        "# TYPE jellyball_channel_failovers_total counter",
    ]
    for team_id, data in stream_state.items():
        lines.append(f'jellyball_channel_failovers_total{{channel="{_prometheus_label(team_id)}"}} {int(data.get("failover_count", 0))}')
    snapshots = SESSIONS.snapshot()
    lines += [
        "# HELP jellyball_session_watched 1 while someone is watching the channel.",
        "# TYPE jellyball_session_watched gauge",
    ]
    for cid, snap in snapshots.items():
        lines.append(f'jellyball_session_watched{{channel="{_prometheus_label(cid)}"}} {1 if snap.get("watched") else 0}')
    lines += [
        "# HELP jellyball_session_bitrate_kbps Recent segment bitrate.",
        "# TYPE jellyball_session_bitrate_kbps gauge",
    ]
    for cid, snap in snapshots.items():
        if snap.get("bitrate_kbps") is not None:
            lines.append(f'jellyball_session_bitrate_kbps{{channel="{_prometheus_label(cid)}"}} {snap["bitrate_kbps"]}')
    lines += [
        "# HELP jellyball_session_segment_seconds_p95 95th percentile segment download time.",
        "# TYPE jellyball_session_segment_seconds_p95 gauge",
    ]
    for cid, snap in snapshots.items():
        if snap.get("segment_ms_p95") is not None:
            lines.append(
                f'jellyball_session_segment_seconds_p95{{channel="{_prometheus_label(cid)}"}} {snap["segment_ms_p95"] / 1000:.3f}'
            )
    return "\n".join(lines) + "\n"


@app.get("/api/logs")
async def api_logs(limit: int = 200, auth: bool = Depends(verify_dashboard_auth)):
    """Return recent application log lines for the local dashboard."""
    limit = max(1, min(limit, 1000))
    lines = await asyncio.to_thread(_tail_log_file, limit)
    return {"logs": lines, "count": len(lines)}


def _tail_log_file(limit: int) -> List[str]:
    """Read only the end of the log (off the event loop) instead of the whole file."""
    try:
        with LOG_FILE.open("rb") as log_file:
            log_file.seek(0, os.SEEK_END)
            size = log_file.tell()
            chunk = min(size, max(64 * 1024, limit * 400))
            log_file.seek(size - chunk)
            data = log_file.read(chunk)
    except OSError:
        return []
    lines = data.decode("utf-8", errors="replace").splitlines()
    if chunk < size and lines:
        lines = lines[1:]  # first line is probably partial
    return lines[-limit:]


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, tab: str = "channels", status: str = "", auth: bool = Depends(verify_dashboard_auth)):
    base_url = _public_base_url(request)
    base_url_html = _html(base_url)
    dashboard_metrics = await _load_dashboard_metrics_async()
    metrics = dashboard_metrics
    favorites = dashboard_metrics["favorites"]
    provider_totals = dashboard_metrics["provider_totals"]
    notif_cfg = await get_notification_config()
    jellyfin_cfg = await get_jellyfin_config()
    provider_rotation_enabled = await get_setting_async("provider_rotation_mode", "0") == "1"
    update_check_enabled = await get_setting_async("update_check_enabled", "0") == "1"
    update_banner = ""
    if update_check_enabled and _update_available():
        release_link = (
            f' <a href="{_html(_UPDATE_STATE["url"])}" target="_blank" rel="noopener noreferrer">Release notes</a>'
            if _UPDATE_STATE.get("url") else ""
        )
        update_banner = (
            f'<div class="card" role="status" style="border-color: var(--success);">'
            f'⬆️ Jellyball {_html(_UPDATE_STATE["latest"])} is available (you are running {_html(__version__)}).'
            f'{release_link}</div>'
        )
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

    if DASHBOARD_AUTH_MODE == "open":
        auth_badge = '<span class="badge" style="background: var(--surface-3); color: var(--text-muted);" title="No DASHBOARD_PASSWORD; only reachable from this computer">🔓 Local Open Access</span>'
    elif DASHBOARD_AUTH_MODE == "generated":
        auth_badge = f'<span class="badge" style="background: var(--surface-3); color: var(--text-muted);" title="Generated password in {_html(DASHBOARD_PASSWORD_FILE)}">🔒 Generated Password</span>'
    else:
        auth_badge = '<span class="badge" style="background: var(--surface-3); color: var(--text-muted);">🔒 Password Protected</span>'
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
                    <button type="button" class="theme-toggle" id="theme-toggle" onclick="toggleTheme()" title="Toggle Dark/Light Mode" aria-label="Toggle dark or light mode">🌙</button>
                </div>
                <h1>Jellyball Sports Manager</h1>
                <p>Multi-Aggregator Scraper & Jellyfin Live TV Gateway</p>
                <div class="status-badges">{auth_badge} {jellyfin_badge} {webhook_discord_badge} {webhook_telegram_badge}</div>
            </header>
            {update_banner}
            <div class="tabs-nav">
                <button type="button" class="tab-btn {'active' if tab == 'channels' else ''}" id="btn-channels" onclick="switchTab('channels')">📺 Channels & Streams</button>
                <button type="button" class="tab-btn {'active' if tab == 'metrics' else ''}" id="btn-metrics" onclick="switchTab('metrics')">📊 Stability Metrics</button>
                <button type="button" class="tab-btn {'active' if tab == 'performance' else ''}" id="btn-performance" onclick="switchTab('performance')">⚡ Performance</button>
                <button type="button" class="tab-btn {'active' if tab == 'playback' else ''}" id="btn-playback" onclick="switchTab('playback')">⚙️ Settings</button>
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
                    <h3>Advanced Settings</h3>
                    <p class="hint-text">Leave a field blank to use its default (the .env value, or the built-in default). Changes apply immediately.</p>
                    <form action="/settings/advanced" method="post" style="display: flex; flex-direction: column; gap: 1.25rem;">
                        {_advanced_settings_html()}
                        <button type="submit" style="width: auto; align-self: flex-start;">💾 Save Advanced Settings</button>
                    </form>
                </div>
                <div class="card">
                    <h3>Update Check</h3>
                    <p class="hint-text">Running Jellyball {_html(__version__)}. When enabled, Jellyball asks GitHub twice a day whether a newer release exists and shows a notice here. Nothing else is sent.</p>
                    <form action="/settings/update-check" method="post" style="display: flex; align-items: center; gap: 0.75rem;">
                        <input type="checkbox" name="enabled" value="true" id="update-toggle" {'checked' if update_check_enabled else ''} style="width: auto; cursor: pointer;">
                        <label for="update-toggle" style="cursor: pointer;">Check for new versions</label>
                        <button type="submit" style="width: auto; margin-left: auto;">💾 Save</button>
                    </form>
                </div>
                <div class="card">
                    <h3>Off-season Channels</h3>
                    <p class="hint-text">By default a team's channel leaves Jellyfin's channel list and guide between seasons. Keep them listed instead, with an "Off-season" guide entry showing when the season resumes.</p>
                    <form action="/settings/offseason" method="post" style="display: flex; align-items: center; gap: 0.75rem;">
                        <input type="checkbox" name="enabled" value="true" id="offseason-toggle" {'checked' if SHOW_OFFSEASON_CHANNELS else ''} style="width: auto; cursor: pointer;">
                        <label for="offseason-toggle" style="cursor: pointer;">Keep off-season channels in Jellyfin</label>
                        <button type="submit" style="width: auto; margin-left: auto;">💾 Save</button>
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
                            <div><label style="font-size: 0.85rem; color: var(--text-soft); font-weight: 600; display: block; margin-bottom: 0.35rem;">🔑 Jellyfin API Key</label>{_secret_input("jellyfin_api_key", jellyfin_cfg['jellyfin_api_key'])}</div>
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
                        <div><label style="font-size: 0.85rem; color: var(--text-soft); font-weight: 600; display: block; margin-bottom: 0.35rem;">👾 Discord Incoming Webhook URL</label>{_secret_input("discord_webhook_url", notif_cfg['discord_webhook_url'])}</div>
                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;">
                            <div><label style="font-size: 0.85rem; color: var(--text-soft); font-weight: 600; display: block; margin-bottom: 0.35rem;">✈️ Telegram Bot Token</label>{_secret_input("telegram_bot_token", notif_cfg['telegram_bot_token'])}</div>
                            <div><label style="font-size: 0.85rem; color: var(--text-soft); font-weight: 600; display: block; margin-bottom: 0.35rem;">💬 Telegram Chat ID</label><input type="text" name="telegram_chat_id" value="{_html(notif_cfg['telegram_chat_id'])}"></div>
                        </div>
                        <button type="submit" style="margin-top: 0.5rem;">💾 Save Notification Settings</button>
                    </form>
                </div>
            </div>
        </div>
        <script>
            function escapeHtml(value) {{
                return String(value ?? '').replace(/[&<>"']/g, c => ({{'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}})[c]);
            }}
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
                               failureEl.innerHTML = `<p class="meta-text" style="color:var(--danger-text);">⚠️ ${{escapeHtml(ch.last_error)}} (failed ${{escapeHtml(ch.failure_count)}}x, retrying in ${{escapeHtml(ch.retry_in_seconds)}}s)</p>`;
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
                form.innerHTML = `<input type="hidden" name="team_ids" value="${{escapeHtml(ids)}}">`;
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
                form.innerHTML = `<input type="hidden" name="team_ids" value="${{escapeHtml(ids)}}">`;
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
                form.innerHTML = `<input type="hidden" name="team_ids" value="${{escapeHtml(ids)}}">`;
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
                    'jellyfin_key_required': '❌ Re-enter the API key when changing the Jellyfin server',
                    'team_added': '✅ Channel added',
                    'schedule_saved': '✅ Auto-disable date saved',
                    'schedule_invalid': '❌ Enter a valid date (YYYY-MM-DD)',
                    'schedule_failed': '❌ Could not save the auto-disable date',
                    'jellyfin_url_invalid': '❌ Jellyfin URL must be an absolute http(s) URL',
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

                statusMessages['advanced_saved'] = '✅ Advanced settings saved';
                statusMessages['advanced_invalid'] = '❌ A value was not a number; nothing after it was saved';

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
                                            return `<div style="background: var(--surface-2); padding: 0.75rem; border-radius: 6px; border: 1px solid var(--danger);"><div style="font-weight:600;">${{escapeHtml(p.provider)}}</div><div style="color: var(--danger);">⛔ Likely dead — 0% over ${{p.samples_5d}} attempts/5d</div></div>`;
                                        }}
                                        const color = p.at_risk ? 'var(--danger)' : 'var(--success)';
                                        const icon = p.at_risk ? '⚠️' : '✅';
                                        const rateText = p.success_rate === null ? 'no data this hour' : `${{p.success_rate}}% (${{p.samples_hour}} samples/hr)`;
                                        return `<div style="background: var(--surface-2); padding: 0.75rem; border-radius: 6px; border: 1px solid var(--border);"><div style="font-weight:600;">${{escapeHtml(p.provider)}}</div><div style="color:${{color}};">${{icon}} ${{escapeHtml(rateText)}}</div></div>`;
                                    }}).join('');
                                }}
                            }}

                            if (withTopTeams) {{
                                const playback = await fetch('/api/playback-stats').then(r => r.json());
                                const topEl = document.getElementById('top-teams-list');
                                if (topEl && playback.top_watched_teams) {{
                                    topEl.innerHTML = playback.top_watched_teams.map(t => `<div style="padding: 0.5rem; background: var(--surface-2); border-radius: 6px; display: flex; justify-content: space-between;"><span>${{escapeHtml(t.team_id)}}</span><span style="color: var(--success);">${{escapeHtml(t.plays)}} plays</span></div>`).join('');
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

def _secret_input(name: str, saved_value: str) -> str:
    """Password-type input that never echoes a saved secret back into the page.
    Blank on submit keeps the saved value; the checkbox clears it."""
    if not saved_value:
        return f'<input type="password" name="{name}" value="" autocomplete="off">'
    return (
        f'<input type="password" name="{name}" value="" autocomplete="off" '
        f'placeholder="Saved (ends {_html(saved_value[-4:])}) - leave blank to keep">'
        f'<label style="font-size: 0.8rem; color: var(--text-muted); display: flex; gap: 0.35rem; align-items: center; margin-top: 0.35rem;">'
        f'<input type="checkbox" name="clear_{name}" value="1" style="width: auto;"> Remove saved value</label>'
    )


def _submitted_secret(submitted: str, clear: str, current: str) -> str:
    """Resolve a _secret_input submission against the current value."""
    if clear:
        return ""
    return (submitted or "").strip() or current


@app.post("/settings/notifications")
async def update_notifications(
    discord_webhook_url: str = Form(""),
    telegram_bot_token: str = Form(""),
    telegram_chat_id: str = Form(""),
    clear_discord_webhook_url: str = Form(""),
    clear_telegram_bot_token: str = Form(""),
    auth: bool = Depends(verify_dashboard_auth),
):
    current = await get_notification_config()
    discord = _submitted_secret(discord_webhook_url, clear_discord_webhook_url, current["discord_webhook_url"])
    token = _submitted_secret(telegram_bot_token, clear_telegram_bot_token, current["telegram_bot_token"])
    await set_setting_async("discord_webhook_url", discord)
    await set_setting_async("telegram_bot_token", token)
    await set_setting_async("telegram_chat_id", telegram_chat_id.strip())
    return RedirectResponse(url="/?tab=alerts&status=saved", status_code=303)

@app.post("/settings/jellyfin")
async def update_jellyfin_settings(
    jellyfin_url: str = Form("http://localhost:8096"),
    jellyfin_api_key: str = Form(""),
    jellyfin_task_id: str = Form(""),
    clear_jellyfin_api_key: str = Form(""),
    auth: bool = Depends(verify_dashboard_auth),
):
    jellyfin_url = jellyfin_url.strip().rstrip("/")
    if not _validate_upstream_url(jellyfin_url, allow_private=True):
        return RedirectResponse(url="/?tab=alerts&status=jellyfin_url_invalid", status_code=303)
    current = await get_jellyfin_config()
    new_key = (jellyfin_api_key or "").strip()
    old_host = urllib.parse.urlsplit(current["jellyfin_url"]).netloc.lower()
    new_host = urllib.parse.urlsplit(jellyfin_url).netloc.lower()
    # The saved key must never follow a URL change to another host (that would
    # hand the key to whatever server the new URL names): require it again.
    if new_host != old_host and current["jellyfin_api_key"] and not new_key and not clear_jellyfin_api_key:
        return RedirectResponse(url="/?tab=alerts&status=jellyfin_key_required", status_code=303)
    api_key = "" if clear_jellyfin_api_key else (new_key or current["jellyfin_api_key"])
    await set_setting_async("jellyfin_url", jellyfin_url)
    await set_setting_async("jellyfin_api_key", api_key)
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
            await _remove_channel(team_id)
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


TEAM_NAME_MAX_LENGTH = 120
TEAM_QUERY_MAX_LENGTH = 200


async def _add_manual_team(
    team_name: str,
    search_query: str,
    *,
    team_id: str = "",
    logo_url: str = "",
    category: str = "custom",
) -> Optional[str]:
    """Sanitize, persist and start one manually defined channel. Shared by the
    dashboard form and config import so both apply the same rules. Returns the
    team id, or None when the input is unusable."""
    team_name = _clean_label(team_name, TEAM_NAME_MAX_LENGTH)
    search_query = _clean_label(search_query, TEAM_QUERY_MAX_LENGTH)
    if not team_name or not search_query:
        return None
    team_id = _safe_team_id(team_id or team_name)
    # Jellyfin fetches channel logos server-side: only public http(s) URLs.
    logo_url = (_validate_upstream_url(logo_url.strip()) or "") if logo_url else ""
    category = _clean_label(category, 40) or "custom"
    search_terms = get_team_search_terms(team_name, search_query, team_id)
    await save_team_async(team_id, team_name, search_query, logo_url, category=category,
                          search_terms=search_terms, content_type="manual")
    stream_state[team_id] = {
        "name": team_name,
        "query": search_query,
        "candidates": [],
        "active_index": 0,
        "is_healthy": False,
        "logo_url": logo_url,
        "start_time": "",
        "stop_time": "",
        "category": category,
        "source_id": "",
        "content_type": "manual",
        "search_terms": search_terms,
        "always_live": False,
        "catalog_key": "",
        **_scrape_lifecycle_defaults(),
    }
    _start_team_scrape_loop(team_id)
    return team_id


@app.post("/add_team")
async def add_team(team_name: str = Form(...), search_query: str = Form(...), auth: bool = Depends(verify_dashboard_auth)):
    team_id = await _add_manual_team(team_name, search_query)
    if team_id is None:
        raise HTTPException(status_code=400, detail="Team name and search query are required")
    return RedirectResponse(url="/?tab=channels&status=team_added", status_code=303)

@app.post("/override/{team_id}")
async def override_stream(team_id: str, candidate_index: int = Form(...), auth: bool = Depends(verify_dashboard_auth)):
    if team_id in stream_state and stream_state[team_id].get("candidates"):
        max_idx = len(stream_state[team_id]["candidates"]) - 1
        if 0 <= candidate_index <= max_idx:
            stream_state[team_id]["active_index"] = candidate_index
            stream_state[team_id]["is_healthy"] = True
            stream_state[team_id]["exhausted"] = False
            stream_state[team_id]["self_retries"] = 0
            candidate = stream_state[team_id]["candidates"][candidate_index]
            candidate["consecutive_failures"] = 0
            candidate.pop("session_compatible", None)
            SESSIONS.poke(team_id)
            
            team_name = stream_state[team_id]["name"]
            prov = stream_state[team_id]["candidates"][candidate_index].get("provider", "Unknown")
            _spawn_background_task(
                send_alert("🛠️ Manual Override Activated", f"Stream for **{team_name}** forced to {prov}.", "info"),
                f"send manual-override alert team={team_id}",
            )
            
    return RedirectResponse(url="/?tab=channels&status=override_saved", status_code=303)

async def _remove_channel(channel_id: str) -> None:
    """Single removal path for every kind of channel: stops its scrape loop or
    Multi-View ffmpeg, closes its channel sessions, and deletes its DB row."""
    data = stream_state.get(channel_id)
    _LAST_PLAYBACK_EVENT.pop(channel_id, None)
    if data is not None and data.get("type") == "multiview":
        view_sessions = _multiview_view_session_ids(channel_id)
        # Out of stream_state first: a spawn already past its warm-up re-checks
        # this after every await and gives up instead of launching ffmpeg for
        # a deleted channel; then cancel it (it stops anything it launched).
        stream_state.pop(channel_id, None)
        await _cancel_multiview_start(channel_id)
        await _stop_multiview_process(channel_id)
        _forget_multiview_channel(channel_id)
        _remove_tree_later(MULTIVIEW_OUTPUT_ROOT / channel_id, f"multiview={channel_id}")
        await delete_multiview_channel_async(channel_id)
        for session_id in view_sessions:
            await SESSIONS.close(session_id)
        return
    if data is not None:
        await _stop_team_scrape_loop(channel_id)
        stream_state.pop(channel_id, None)
    await delete_team_async(channel_id)
    await SESSIONS.close(channel_id)
    dependents = [
        cid for cid, other in stream_state.items()
        if other.get("type") == "multiview" and channel_id in (other.get("member_team_ids") or [])
    ]
    for multiview_id in dependents:
        # The removed member's pane now shows "No Signal"; restart a running
        # (or starting) run so it picks up the placeholder input. A config
        # change, so it doesn't count toward the restart backoff.
        LOGGER.warning("Channel %s removed but used by Multi-View %s; its pane will show No Signal", channel_id, multiview_id)
        if multiview_id in _MULTIVIEW_PROCESSES or _multiview_start_in_progress(multiview_id):
            _spawn_background_task(
                _restart_multiview(multiview_id, f"member {channel_id} removed", count_toward_backoff=False),
                f"restart multiview channel={multiview_id}",
            )


@app.post("/remove_team/{team_id}")
async def remove_team(team_id: str, auth: bool = Depends(verify_dashboard_auth)):
    if team_id in stream_state:
        await _remove_channel(team_id)
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
    if channel_id in stream_state:
        # INSERT OR REPLACE used to silently overwrite an existing Multi-View
        # (leaving its old ffmpeg running against the old members).
        raise HTTPException(status_code=409, detail="A channel with that name already exists")
    active_audio_team_id = members[0]
    _forget_multiview_channel(channel_id)
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
        await _remove_channel(channel_id)
        _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after multiview removal")
    return RedirectResponse(url="/?tab=channels", status_code=303)


@app.post("/multiview/{channel_id}/stop")
async def stop_multiview(channel_id: str, auth: bool = Depends(verify_dashboard_auth)):
    data = stream_state.get(channel_id)
    if data and data.get("type") == "multiview":
        # Cancels a spawn in progress too, and keeps the grid stopped (viewers
        # get No Signal) until a new viewer tunes in or /start is posted.
        await _stop_multiview_manually(channel_id)
    return RedirectResponse(url="/?tab=channels", status_code=303)


@app.post("/multiview/{channel_id}/start")
async def start_multiview(channel_id: str, auth: bool = Depends(verify_dashboard_auth)):
    """Explicit start: clears a manual stop and any backoff, then starts the
    grid now (the idle monitor stops it again if nobody watches)."""
    data = stream_state.get(channel_id)
    if data and data.get("type") == "multiview":
        _clear_multiview_manual_stop(channel_id, "explicit start")
        for mapping in (_MULTIVIEW_FAILURES, _MULTIVIEW_HOLDS, _MULTIVIEW_REFUSALS):
            mapping.pop(channel_id, None)
        _MULTIVIEW_LAST_VIEWER[channel_id] = time.monotonic()
        _request_multiview_start(channel_id)
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
    # Every member's audio is already in its own output (stream-copied), so
    # switching the main channel's audio is just pointing its session at
    # another output: no ffmpeg restart. The view's source key carries the
    # output's audio codec (see _resolve_multiview_view_source): same codec
    # continues seamlessly, a different codec becomes a clean discontinuity.
    SESSIONS.poke(channel_id)
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
    _spawn_background_task(trigger_scrape(team_id, force=True), f"manual rescrape {team_id}")
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
            await _remove_channel(team_id)
    except Exception as exc:
        _log_failure("bulk remove", exc)
    _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after bulk removal")
    return RedirectResponse(url="/?tab=channels", status_code=303)

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


@app.get("/api/export-config", response_class=JSONResponse)
async def export_config(auth: bool = Depends(verify_dashboard_auth)):
    try:
        teams = await asyncio.to_thread(_export_teams_sync)
        config = {
            "version": "2.0",
            "app_version": __version__,
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "teams": teams
        }
        return JSONResponse(config)
    except Exception as exc:
        _log_failure("export config", exc)
        raise HTTPException(status_code=500, detail="Export failed")


IMPORT_MAX_TEAMS = 500
IMPORT_MAX_BYTES = 2 * 1024 * 1024


@app.post("/api/import-config")
async def import_config(file: Request, auth: bool = Depends(verify_dashboard_auth)):
    """Import channels from an export. Goes through the same sanitizing path as
    the dashboard form and starts each new channel right away (imports used to
    sit inert in the DB until a restart). Existing channels are left alone."""
    try:
        raw = await file.body()
        if len(raw) > IMPORT_MAX_BYTES:
            raise HTTPException(status_code=413, detail="Import file too large")
        body = json.loads(raw.decode("utf-8"))
        teams = body.get("teams", []) if isinstance(body, dict) else None
        if not isinstance(teams, list):
            raise ValueError("teams must be a list")
    except HTTPException:
        raise
    except Exception as exc:
        _log_failure("import config", exc)
        raise HTTPException(status_code=400, detail="Import failed: not a Jellyball export")

    catalog_by_key = {entry["catalog_key"]: entry for entry in await get_catalog_entries()}
    imported, skipped, favorites = 0, 0, []
    for team in teams[:IMPORT_MAX_TEAMS]:
        if not isinstance(team, dict):
            skipped += 1
            continue
        requested_id = _safe_team_id(str(team.get("team_id") or team.get("name") or ""))
        if requested_id in stream_state:
            skipped += 1
            continue
        catalog_entry = catalog_by_key.get(str(team.get("catalog_key") or ""))
        if catalog_entry is not None:
            # A catalog channel: enable the real catalog entry (keeps its
            # search terms, schedule and guide metadata) instead of a copy.
            if catalog_entry["team_id"] in stream_state:
                skipped += 1
                continue
            await _set_catalog_entry_enabled(catalog_entry, True)
            imported += 1
            if team.get("is_favorite"):
                favorites.append(catalog_entry["team_id"])
            continue
        team_id = await _add_manual_team(
            str(team.get("name") or ""),
            str(team.get("query") or ""),
            team_id=requested_id,
            logo_url=str(team.get("logo_url") or ""),
            category=str(team.get("category") or "imported"),
        )
        if team_id is None:
            skipped += 1
            continue
        imported += 1
        if team.get("is_favorite"):
            favorites.append(team_id)
    skipped += max(0, len(teams) - IMPORT_MAX_TEAMS)
    if favorites:
        await asyncio.to_thread(_bulk_set_favorite_sync, favorites, True)
    if imported:
        request_jellyfin_guide_refresh_if_changed()
    return JSONResponse({"status": "imported", "count": imported, "skipped": skipped})

@dataclass(frozen=True)
class Tunable:
    """A setting the dashboard can change at runtime. The env var (or built-in
    default) is the default; a value saved in app_settings overrides it and is
    applied live - no restart."""

    name: str  # env var name; also the form field
    label: str
    group: str
    kind: type  # int or float
    minimum: float
    maximum: float
    target: str  # module global, or "session.<SessionConfig field>"
    help: str = ""
    db_key: str = ""  # keys saved by 1.x's Playback Settings sliders

    @property
    def key(self) -> str:
        return self.db_key or f"tunable:{self.name}"


TUNABLES: Tuple[Tunable, ...] = (
    Tunable("IDLE_HEALTH_INTERVAL", "Unwatched channel probe interval (s)", "Health & failover", float, 5, 600,
            "IDLE_HEALTH_INTERVAL", "How often the active source of a channel nobody is watching is checked."),
    Tunable("HEALTH_FAILURE_THRESHOLD", "Failed probes before failover", "Health & failover", int, 1, 10,
            "HEALTH_FAILURE_THRESHOLD"),
    Tunable("HEALTH_PROBE_TIMEOUT", "Probe timeout (s)", "Health & failover", float, 3, 60, "HEALTH_PROBE_TIMEOUT"),
    Tunable("STANDBY_HEALTH_INTERVAL", "Standby check interval, watched channels (s)", "Health & failover", float,
            10, 3600, "STANDBY_HEALTH_INTERVAL"),
    Tunable("UNWATCHED_STANDBY_INTERVAL", "Standby check interval, unwatched channels (s)", "Health & failover",
            float, 30, 7200, "UNWATCHED_STANDBY_INTERVAL"),
    Tunable("EXHAUSTED_PROBE_INTERVAL", "Recovery probe interval when all sources failed (s)", "Health & failover",
            float, 5, 600, "EXHAUSTED_PROBE_INTERVAL"),
    Tunable("WINDOW_CLOSE_GRACE_SECONDS", "Keep a finished game on air without playback for (s)", "Health & failover",
            float, 0, 3600, "WINDOW_CLOSE_GRACE_SECONDS"),
    Tunable("SELF_RETRY_LIMIT", "Retries of a channel's only source before No Signal", "Health & failover", int,
            0, 10, "SELF_RETRY_LIMIT"),
    Tunable("EMERGENCY_SCRAPE_COOLDOWN", "Min seconds between emergency rescans", "Health & failover", float,
            10, 3600, "EMERGENCY_SCRAPE_COOLDOWN"),
    Tunable("FAILOVER_ALERT_COOLDOWN", "Min seconds between failover alerts per channel", "Health & failover",
            float, 0, 86400, "FAILOVER_ALERT_COOLDOWN"),
    Tunable("HEALTHY_RESCRAPE_SECONDS", "Refresh standbys of healthy channels every (s)", "Scraping", float,
            300, 86400, "HEALTHY_RESCRAPE_SECONDS"),
    Tunable("MAX_STREAM_CANDIDATES", "Max stream candidates per channel", "Scraping", int, 1, 200,
            "MAX_STREAM_CANDIDATES"),
    Tunable("PROVIDER_BREAKER_FAILURES", "Provider failures before its breaker opens", "Scraping", int, 1, 20,
            "PROVIDER_BREAKER_FAILURES"),
    Tunable("PROVIDER_BREAKER_COOLDOWN", "Provider breaker cooldown (s)", "Scraping", float, 5, 3600,
            "PROVIDER_BREAKER_COOLDOWN"),
    Tunable("PROVIDER_TIMEOUT_MIN", "Provider search timeout, minimum (s)", "Scraping", float, 5, 300,
            "PROVIDER_TIMEOUT_MIN"),
    Tunable("PROVIDER_TIMEOUT_MAX", "Provider search timeout, maximum (s)", "Scraping", float, 10, 600,
            "PROVIDER_TIMEOUT_MAX"),
    Tunable("SESSION_IDLE_SECONDS", "Stop a channel session after idle (s)", "Channel sessions", float, 10, 3600,
            "session.idle_timeout"),
    Tunable("SESSION_LIVE_EDGE_SEGMENTS", "Segments of buffer at tune-in", "Channel sessions", int, 1, 10,
            "session.live_edge_segments"),
    Tunable("SESSION_WINDOW_SECONDS", "Playlist window (s)", "Channel sessions", float, 12, 600,
            "session.window_min_seconds"),
    Tunable("SESSION_STALE_SECONDS", "Fail over when no new segment for at least (s)", "Channel sessions", float,
            5, 300, "session.stale_min_seconds"),
    Tunable("SESSION_FAIL_THRESHOLD", "Consecutive fetch failures before failover", "Channel sessions", int, 1, 20,
            "session.fail_threshold"),
    Tunable("SESSION_SEGMENT_TIMEOUT", "Segment download timeout (s)", "Channel sessions", float, 3, 120,
            "session.segment_timeout"),
    Tunable("STREAM_STARTUP_TIMEOUT", "Wait for a channel to start before No Signal (s)", "Channel sessions", float,
            3, 120, "STREAM_STARTUP_TIMEOUT"),
    Tunable("STARTUP_PLACEHOLDER_SECONDS", "No Signal shown for a slow start (s)", "Channel sessions", float,
            5, 600, "STARTUP_PLACEHOLDER_SECONDS"),
    Tunable("MULTIVIEW_IDLE_TIMEOUT_SECONDS", "Stop an unwatched Multi-View after (s)", "Multi-View", float, 30, 3600,
            "MULTIVIEW_IDLE_TIMEOUT_SECONDS"),
    Tunable("STREAM_STARTUP_BUFFER_SECONDS", "Warm-up cache time (s)", "Legacy proxy (fMP4 / separate-audio sources)",
            float, 0, 120, "STREAM_STARTUP_BUFFER_SECONDS", db_key="startup_buffer_seconds"),
    Tunable("PREFETCH_CHUNK_COUNT", "Read-ahead chunks", "Legacy proxy (fMP4 / separate-audio sources)", int, 0, 32,
            "PREFETCH_CHUNK_COUNT", db_key="prefetch_chunk_count"),
    Tunable("STREAM_CHUNK_CACHE_TTL", "Chunk cache TTL (s)", "Legacy proxy (fMP4 / separate-audio sources)", float,
            1, 600, "STREAM_CHUNK_CACHE_TTL", db_key="stream_chunk_cache_ttl"),
)
_TUNABLES_BY_NAME = {t.name: t for t in TUNABLES}


def _tunable_value(tunable: Tunable):
    if tunable.target.startswith("session."):
        return getattr(SESSIONS.config, tunable.target.split(".", 1)[1])
    return globals()[tunable.target]


_TUNABLE_DEFAULTS = {t.name: _tunable_value(t) for t in TUNABLES}


def _coerce_tunable(tunable: Tunable, raw: object):
    """Parse and clamp; None when the value is unusable."""
    try:
        value = float(str(raw).strip())
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value):
        return None
    value = min(max(value, tunable.minimum), tunable.maximum)
    return int(round(value)) if tunable.kind is int else float(value)


def _apply_tunable(tunable: Tunable, value) -> None:
    if tunable.target.startswith("session."):
        # Every channel session shares this config object, so running
        # sessions pick the change up on their next poll.
        setattr(SESSIONS.config, tunable.target.split(".", 1)[1], value)
    else:
        globals()[tunable.target] = value


def _load_tunable_overrides() -> None:
    for tunable in TUNABLES:
        raw = get_setting(tunable.key, "")
        if raw == "":
            continue
        value = _coerce_tunable(tunable, raw)
        if value is not None:
            _apply_tunable(tunable, value)


def _advanced_settings_snapshot() -> List[dict]:
    return [
        {
            "name": t.name, "label": t.label, "group": t.group, "help": t.help,
            "value": _tunable_value(t), "default": _TUNABLE_DEFAULTS[t.name],
            "min": t.minimum, "max": t.maximum, "type": t.kind.__name__,
        }
        for t in TUNABLES
    ]


def _advanced_settings_html() -> str:
    groups: Dict[str, List[str]] = {}
    for item in _advanced_settings_snapshot():
        overridden = item["value"] != item["default"]
        step = "1" if item["type"] == "int" else "any"
        hint = f"Default {item['default']:g}" if isinstance(item["default"], (int, float)) else ""
        if item["help"]:
            hint = f"{hint}. {item['help']}" if hint else item["help"]
        groups.setdefault(item["group"], []).append(
            f'<div><label style="font-size: 0.85rem; color: var(--text-soft); font-weight: 600; display: block; '
            f'margin-bottom: 0.35rem;" for="adv-{_html(item["name"])}">{_html(item["label"])}'
            f'{" <span class=\"badge\">changed</span>" if overridden else ""}</label>'
            f'<input type="number" id="adv-{_html(item["name"])}" name="{_html(item["name"])}" '
            f'step="{step}" min="{item["min"]:g}" max="{item["max"]:g}" '
            f'value="{_html(item["value"] if overridden else "")}" placeholder="{_html(item["default"])}">'
            f'<p class="hint-text" style="margin: 0.25rem 0 0;">{_html(hint)}</p></div>'
        )
    sections = []
    for group, fields in groups.items():
        sections.append(
            f'<fieldset style="border: 1px solid var(--border); border-radius: 8px; padding: 1rem;">'
            f'<legend style="padding: 0 0.5rem; font-weight: 700;">{_html(group)}</legend>'
            f'<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 1rem;">'
            f'{"".join(fields)}</div></fieldset>'
        )
    return "".join(sections)


@app.get("/api/settings/advanced", response_class=JSONResponse)
async def get_advanced_settings(auth: bool = Depends(verify_dashboard_auth)):
    return {"settings": _advanced_settings_snapshot()}


@app.post("/settings/advanced")
async def update_advanced_settings(request: Request, auth: bool = Depends(verify_dashboard_auth)):
    """Blank field = back to the default (env var or built-in). Applied live."""
    form = await request.form()
    for tunable in TUNABLES:
        if tunable.name not in form:
            continue
        raw = str(form.get(tunable.name) or "").strip()
        if raw == "":
            await set_setting_async(tunable.key, "")
            _apply_tunable(tunable, _TUNABLE_DEFAULTS[tunable.name])
            continue
        value = _coerce_tunable(tunable, raw)
        if value is None:
            return RedirectResponse(url="/?tab=playback&status=advanced_invalid", status_code=303)
        await set_setting_async(tunable.key, str(value))
        _apply_tunable(tunable, value)
    return RedirectResponse(url="/?tab=playback&status=advanced_saved", status_code=303)

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
    return RedirectResponse(url="/?tab=playback&status=saved", status_code=303)


UPDATE_CHECK_URL = os.getenv(
    "UPDATE_CHECK_URL", "https://api.github.com/repos/DarthBitBeard/Jellyball/releases/latest"
)
UPDATE_CHECK_INTERVAL = 12 * 3600.0
_UPDATE_STATE: Dict[str, object] = {"latest": "", "url": "", "checked_at": 0.0}


def _version_tuple(value: str) -> Tuple[int, ...]:
    numbers = re.findall(r"\d+", str(value or "").split("-", 1)[0])
    return tuple(int(n) for n in numbers[:3]) or (0,)


def _update_available() -> bool:
    latest = str(_UPDATE_STATE.get("latest") or "")
    return bool(latest) and _version_tuple(latest) > _version_tuple(__version__)


async def check_for_update() -> None:
    """One request to GitHub Releases (opt-in: Settings > Update check)."""
    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            response = await client.get(
                UPDATE_CHECK_URL,
                headers={"Accept": "application/vnd.github+json", "User-Agent": f"Jellyball/{__version__}"},
            )
        if response.status_code != 200:
            return
        release = response.json()
        tag = str(release.get("tag_name") or "").lstrip("vV")
        html_url = str(release.get("html_url") or "")
        if tag and not release.get("draft") and not release.get("prerelease"):
            _UPDATE_STATE.update({
                "latest": tag,
                "url": html_url if html_url.startswith("https://github.com/") else "",
                "checked_at": time.time(),
            })
            if _update_available():
                LOGGER.info("Jellyball %s is available (running %s)", tag, __version__)
    except Exception as exc:
        _log_failure("check for updates", exc, logging.INFO)


async def update_check_loop() -> None:
    await asyncio.sleep(random.uniform(30.0, 120.0))
    while True:
        if await get_setting_async("update_check_enabled", "0") == "1":
            await check_for_update()
        await asyncio.sleep(UPDATE_CHECK_INTERVAL)


@app.get("/api/version")
async def api_version(auth: bool = Depends(verify_dashboard_auth)):
    return {
        "version": __version__,
        "latest": _UPDATE_STATE.get("latest") or None,
        "update_available": _update_available(),
        "release_url": _UPDATE_STATE.get("url") or None,
    }


@app.post("/settings/update-check")
async def set_update_check(enabled: bool = Form(False), auth: bool = Depends(verify_dashboard_auth)):
    await set_setting_async("update_check_enabled", "1" if enabled else "0")
    if enabled:
        _spawn_background_task(check_for_update(), "check for updates")
    return RedirectResponse(url="/?tab=playback&status=saved", status_code=303)


@app.post("/settings/offseason")
async def set_offseason_listing(enabled: bool = Form(False), auth: bool = Depends(verify_dashboard_auth)):
    global SHOW_OFFSEASON_CHANNELS
    await set_setting_async("show_offseason_channels", "1" if enabled else "0")
    if SHOW_OFFSEASON_CHANNELS != bool(enabled):
        SHOW_OFFSEASON_CHANNELS = bool(enabled)
        request_jellyfin_guide_refresh_if_changed()
    return RedirectResponse(url="/?tab=playback&status=saved", status_code=303)


def _parse_disable_date(value: str) -> Optional[str]:
    """'' clears the schedule; anything else must be a real YYYY-MM-DD date
    (it is compared as text against date('now'), so other formats never fire)."""
    value = (value or "").strip()
    if not value:
        return ""
    try:
        return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def _schedule_team_disable_sync(team_id: str, disable_date: str) -> None:
    with _db_session() as conn:
        conn.execute("UPDATE teams SET auto_disable_after=? WHERE team_id=?", (disable_date, team_id))
        conn.commit()


@app.post("/team/{team_id}/schedule-disable")
async def schedule_team_disable(team_id: str, disable_date: str = Form(""), auth: bool = Depends(verify_dashboard_auth)):
    parsed = _parse_disable_date(disable_date)
    if parsed is None or team_id not in stream_state:
        return RedirectResponse(url="/?tab=channels&status=schedule_invalid", status_code=303)
    try:
        await asyncio.to_thread(_schedule_team_disable_sync, team_id, parsed)
        stream_state[team_id]["auto_disable_after"] = parsed
    except Exception as exc:
        _log_failure(f"schedule disable {team_id}", exc)
        return RedirectResponse(url="/?tab=channels&status=schedule_failed", status_code=303)
    return RedirectResponse(url="/?tab=channels&status=schedule_saved", status_code=303)

def _create_tray_image():
    from PIL import Image, ImageDraw

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


PORT_BIND_WAIT_SECONDS = bounded_float(os.getenv("PORT_BIND_WAIT_SECONDS", "30"), 30.0, 0.0, 600.0)


def _port_is_free(host: str, port: int) -> bool:
    bind_host = "0.0.0.0" if host in ("", "0.0.0.0") else host
    family = socket.AF_INET6 if ":" in bind_host else socket.AF_INET
    try:
        with socket.socket(family, socket.SOCK_STREAM) as probe:
            probe.bind((bind_host, port))
        return True
    except OSError:
        return False


def _wait_for_port(host: str, port: int, timeout: float) -> bool:
    """A restart can race the previous instance releasing the port; wait for it
    instead of silently moving to another port (which broke Jellyfin's saved
    tuner and guide URLs)."""
    deadline = time.monotonic() + timeout
    while True:
        if _port_is_free(host, port):
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.5)


def _existing_jellyball_instance(port: int) -> bool:
    """True if a Jellyball (e.g. the installed Windows service) already answers
    on this port."""
    try:
        response = httpx.get(f"http://127.0.0.1:{port}/healthz", timeout=2.0)
        return response.status_code == 200 and response.json().get("app") == "jellyball"
    except Exception:
        return False


def build_server(host: str, port: int):
    import uvicorn

    _configure_dashboard_auth(host)

    try:
        import httptools  # noqa: F401
        http_impl = "httptools"
    except ImportError:
        http_impl = "auto"
    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        reload=False,
        log_config=None,
        access_log=False,
        http=http_impl,
        lifespan="on",
        # Jellyfin's ffmpeg re-polls playlists every 2-6s; keep its connection
        # open between polls instead of reconnecting each time.
        timeout_keep_alive=30,
        # Streaming responses would otherwise hold shutdown open indefinitely.
        timeout_graceful_shutdown=10,
    )
    return uvicorn.Server(config)


def _headless_host() -> str:
    return os.getenv("JELLYBALL_HOST", "0.0.0.0" if sys.platform != "win32" else "127.0.0.1").strip() or "127.0.0.1"


def run_headless(stop_event: Optional[threading.Event] = None) -> int:
    """Run the server in the foreground (console mode, Docker, Windows service).
    Returns a process exit code; non-zero lets a service manager restart us."""
    if os.getenv("WEB_CONCURRENCY", "1") not in {"", "1"}:
        LOGGER.error("Jellyball must run with a single worker (WEB_CONCURRENCY=1)")
        return 2
    host = _headless_host()
    if not _wait_for_port(host, PORT, PORT_BIND_WAIT_SECONDS):
        LOGGER.error("Port %s on %s is in use; refusing to start on a different port", PORT, host)
        return 3
    server = build_server(host, PORT)
    if stop_event is not None:
        def _watch_stop() -> None:
            stop_event.wait()
            server.should_exit = True

        threading.Thread(target=_watch_stop, name="jellyball-stop-watch", daemon=True).start()
    LOGGER.info("Jellyball %s running headless at http://%s:%s", __version__, host, PORT)
    try:
        server.run()
    finally:
        if _LOG_LISTENER is not None:
            _LOG_LISTENER.stop()
    return 0 if server.started else 1


class TrayApplication:
    def __init__(self):
        import pystray

        self.server = None
        self.server_thread = None
        self.host = "127.0.0.1"
        self._pending_notices: List[str] = []
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
                self.server_thread.join(timeout=15)
            if getattr(sys, "frozen", False):
                command = [sys.executable, *sys.argv[1:]]
            else:
                command = [sys.executable, str(Path(sys.argv[0]).resolve()), *sys.argv[1:]]
            env = dict(os.environ)
            # PyInstaller 6 treats a child launched with the parent's environment
            # as a worker sharing the parent's (about to be deleted) extraction
            # directory; reset it. Also drop the browser path we derived from it.
            env["PYINSTALLER_RESET_ENVIRONMENT"] = "1"
            if _PLAYWRIGHT_PATH_SET_BY_APP:
                env.pop("PLAYWRIGHT_BROWSERS_PATH", None)
            child = subprocess.Popen(command, close_fds=True, env=env, creationflags=_child_process_creationflags())
            # Only hand over once the new instance answers; if it dies (e.g. a
            # broken .env edit), keep this tray alive and bring the server back.
            deadline = time.monotonic() + 45.0
            while time.monotonic() < deadline:
                if _existing_jellyball_instance(PORT):
                    self.icon.stop()
                    return
                if child.poll() is not None:
                    break
                time.sleep(1.0)
            LOGGER.error("Restarted Jellyball did not come up (exit=%s); keeping this instance", child.poll())
            if child.poll() is None:
                child.terminate()
            self._notify("Restart failed - Jellyball kept running the previous instance. See jellyball.log.")
            self._start_server()

        threading.Thread(target=relaunch, name="jellyball-restart", daemon=True).start()

    def _notify(self, message: str) -> None:
        try:
            self.icon.notify(message, "Jellyball")
        except Exception as exc:  # not every tray backend supports notifications
            _log_failure("tray notification", exc, logging.DEBUG)

    def _on_icon_ready(self, icon) -> None:
        icon.visible = True
        for message in self._pending_notices:
            self._notify(message)
        self._pending_notices.clear()

    def _start_server(self) -> None:
        self.server = build_server(self.host, PORT)
        self.server_thread = threading.Thread(target=self.server.run, name="jellyball-server", daemon=True)
        self.server_thread.start()

    def run(self):
        global PORT
        host = os.getenv("JELLYBALL_HOST", "127.0.0.1").strip() or "127.0.0.1"
        self.host = host
        if not _port_is_free(host, PORT):
            if _existing_jellyball_instance(PORT):
                # The Windows service (or another tray instance) already runs
                # Jellyball here: just open its dashboard.
                LOGGER.info("Jellyball already running on port %s; opening its dashboard", PORT)
                webbrowser.open(f"http://127.0.0.1:{PORT}/")
                return
            if not _wait_for_port(host, PORT, 10.0):
                selected_port = _find_available_port(PORT)
                LOGGER.warning(
                    "Configured port %s is in use by another program; using %s for this desktop session "
                    "(Jellyfin tuner URLs pointing at %s will not work until it is free)",
                    PORT, selected_port, PORT,
                )
                self._pending_notices.append(
                    f"Port {PORT} is in use, so Jellyball is on port {selected_port} for now. "
                    f"Jellyfin URLs using port {PORT} won't work until it's free."
                )
                PORT = selected_port

        self._start_server()
        if DASHBOARD_AUTH_MODE == "generated":
            self._pending_notices.append(
                f"Dashboard password generated (user {DASHBOARD_USERNAME}); it is in {DASHBOARD_PASSWORD_FILE}."
            )
        LOGGER.info("Jellyball %s web GUI available at http://127.0.0.1:%s", __version__, PORT)
        threading.Timer(1.0, lambda: webbrowser.open(f"http://127.0.0.1:{PORT}/")).start()
        try:
            self.icon.run(setup=self._on_icon_ready)
        finally:
            self.server.should_exit = True
            self.server_thread.join(timeout=15)
            if _LOG_LISTENER is not None:
                _LOG_LISTENER.stop()


def _wants_headless() -> bool:
    return sys.platform != "win32" or os.getenv("JELLYBALL_HEADLESS", "").strip().lower() in {"1", "true", "yes"}


if __name__ == "__main__":
    if _wants_headless() or "--console" in sys.argv[1:]:
        sys.exit(run_headless())
    TrayApplication().run()
