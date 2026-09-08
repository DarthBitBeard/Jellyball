import os
import sys
import logging
from logging.handlers import RotatingFileHandler
import subprocess
import webbrowser
from pathlib import Path
from html import escape as html_escape

# Resolve configuration and writable data independently of the current directory.
# This is important when the application is launched from a Jellyfin service or a shortcut.
APP_DIR = Path(sys.executable if getattr(sys, "frozen", False) else __file__).resolve().parent


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
        )
        try:
            USER_ENV_FILE.write_text(template, encoding="utf-8")
        except OSError as exc:
            # Logging is configured immediately below; retain a safe fallback message.
            LOGGER.warning("Could not create user configuration file error=%s", type(exc).__name__)

    try:
        handler = RotatingFileHandler(
            LOG_FILE,
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        )
        logging.basicConfig(
            handlers=[handler],
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            force=True,
        )
    except OSError as exc:
        logging.basicConfig(level=logging.INFO)
        _log_failure("create rotating log file", exc)

    LOGGER.info("Jellyball starting; data directory initialized")


_bootstrap_runtime_files()

if not os.getenv("PLAYWRIGHT_BROWSERS_PATH"):
    if getattr(sys, "frozen", False):
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(DATA_DIR / "playwright_browsers")
    else:
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "0"

import asyncio

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import httpx
import re
import urllib.parse
import sqlite3
import secrets
import time
import random
import threading
from collections import OrderedDict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from xml.sax.saxutils import escape as xml_escape
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from fastapi import FastAPI, Form, Request, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import PlainTextResponse, Response, HTMLResponse, RedirectResponse, StreamingResponse, JSONResponse
from typing import Dict, List, Optional, Tuple, Set
from playwright.async_api import async_playwright, Browser, Playwright, Page
import pystray
from PIL import Image, ImageDraw

from sports_matcher import get_team_search_terms, match_team, clean_sports_text, canonical_team_name
from stream_extractor import (
    fetch_streams_from_page,
    playwright_intercept_streams,
    verify_stream_live,
    DEFAULT_USER_AGENT,
    rank_streams
)

# User settings are defaults; a .env beside the executable can override them.
load_dotenv(dotenv_path=USER_ENV_FILE)
load_dotenv(dotenv_path=APP_DIR / ".env")
try:
    PORT = max(1, min(65535, int(os.getenv("PORT", "8000"))))
except (TypeError, ValueError):
    PORT = 8000
    LOGGER.warning("Invalid PORT setting; using default port=8000")
_configured_db = Path(os.getenv("DB_FILE", "sports_proxy.db"))
DB_FILE = str(_configured_db if _configured_db.is_absolute() else DATA_DIR / _configured_db)
_provider_priority = {
    name.strip(): index
    for index, name in enumerate(os.getenv("STREAM_PROVIDER_PRIORITY", "").split(","))
    if name.strip()
}


def _connect_db() -> sqlite3.Connection:
    """Open a consistently configured SQLite connection for every code path."""
    conn = sqlite3.connect(DB_FILE, timeout=SQLITE_BUSY_TIMEOUT_MS / 1000)
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _validate_upstream_url(value: str) -> Optional[str]:
    """Allow only absolute HTTP(S) upstream URLs for proxy endpoints."""
    try:
        parsed = urllib.parse.urlparse(value)
    except ValueError:
        return None
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    if parsed.username or parsed.password:
        return None
    return value


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
    with _connect_db() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
        conn.execute('''CREATE TABLE IF NOT EXISTS teams 
                        (team_id TEXT PRIMARY KEY, name TEXT, query TEXT,
                         logo_url TEXT DEFAULT '', start_time TEXT DEFAULT '', stop_time TEXT DEFAULT '')''')
        conn.execute('''CREATE TABLE IF NOT EXISTS stream_events 
                        (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                         timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, 
                         team_id TEXT, 
                         provider TEXT, 
                         event_type TEXT, 
                         details TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS app_settings 
                        (key TEXT PRIMARY KEY, value TEXT)''')
        for col, default in [("logo_url", "''"), ("start_time", "''"), ("stop_time", "''")]:
            try:
                conn.execute(f"ALTER TABLE teams ADD COLUMN {col} TEXT DEFAULT {default}")
            except sqlite3.OperationalError as exc:
                if "duplicate column name" in str(exc).lower():
                    LOGGER.debug("Database column already exists: %s", col)
                else:
                    _log_failure(f"migrate database column {col}", exc, logging.ERROR)
                    raise
        conn.commit()
    LOGGER.info("Database initialized")

async def prune_database_logs():
    while True:
        try:
            with _connect_db() as conn:
                conn.execute("DELETE FROM stream_events WHERE timestamp < datetime('now', '-7 days')")
                conn.commit()
        except Exception as exc:
            _log_failure("prune database logs", exc)
        await asyncio.sleep(86400)

def get_setting(key: str, default: str = "") -> str:
    try:
        with _connect_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM app_settings WHERE key=?", (key,))
            row = cursor.fetchone()
            if row and row[0] is not None:
                return row[0]
    except Exception as exc:
        _log_failure(f"read setting {key}", exc)
    return default

def set_setting(key: str, value: str):
    with _connect_db() as conn:
        conn.execute("INSERT OR REPLACE INTO app_settings (key, value) VALUES (?, ?)", (key, value))
        conn.commit()

def get_notification_config() -> dict:
    return {
        "discord_webhook_url": get_setting("discord_webhook_url", os.getenv("DISCORD_WEBHOOK_URL", "")),
        "telegram_bot_token": get_setting("telegram_bot_token", os.getenv("TELEGRAM_BOT_TOKEN", "")),
        "telegram_chat_id": get_setting("telegram_chat_id", os.getenv("TELEGRAM_CHAT_ID", ""))
    }

async def send_alert(title: str, message: str, level: str = "warning"):
    config = get_notification_config()
    discord_url = config["discord_webhook_url"]
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
    _BACKGROUND_TASKS.clear()

def get_jellyfin_config() -> dict:
    return {
        "jellyfin_url": get_setting("jellyfin_url", os.getenv("JELLYFIN_URL", "http://localhost:8096")).rstrip("/"),
        "jellyfin_api_key": get_setting("jellyfin_api_key", os.getenv("JELLYFIN_API_KEY", "")),
        "jellyfin_task_id": get_setting("jellyfin_task_id", os.getenv("JELLYFIN_TASK_ID", ""))
    }

async def trigger_jellyfin_refresh() -> bool:
    cfg = get_jellyfin_config()
    jellyfin_url = cfg["jellyfin_url"]
    api_key = cfg["jellyfin_api_key"]
    task_id = cfg["jellyfin_task_id"]

    if not api_key:
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
                            set_setting("jellyfin_task_id", task_id)
                            break
            except Exception as exc:
                _log_failure("discover Jellyfin guide task", exc)

        if not task_id:
            return False

        url = f"{jellyfin_url}/ScheduledTasks/Running/{task_id}"
        try:
            response = await client.post(url, headers=headers)
            if response.status_code in [200, 204]:
                log_metric_event("Jellyfin", "API", "guide_refresh", "Triggered Live TV Guide Refresh task")
                return True
            return False
        except Exception as exc:
            _log_failure("trigger Jellyfin guide refresh", exc)
            return False

def save_team(team_id: str, name: str, query: str, logo_url: str = "", start_time: str = "", stop_time: str = ""):
    with _connect_db() as conn:
        conn.execute("INSERT OR REPLACE INTO teams (team_id, name, query, logo_url, start_time, stop_time) VALUES (?, ?, ?, ?, ?, ?)", (team_id, name, query, logo_url, start_time, stop_time))
        conn.commit()

def update_team_meta(team_id: str, logo_url: str = "", start_time: str = "", stop_time: str = ""):
    try:
        with _connect_db() as conn:
            conn.execute("UPDATE teams SET logo_url=?, start_time=?, stop_time=? WHERE team_id=?", (logo_url, start_time, stop_time, team_id))
            conn.commit()
    except Exception as exc:
        _log_failure("update team metadata", exc)

def delete_team(team_id: str):
    with _connect_db() as conn:
        conn.execute("DELETE FROM teams WHERE team_id=?", (team_id,))
        conn.commit()

def load_teams() -> list:
    with _connect_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT team_id, name, query, logo_url, start_time, stop_time FROM teams")
        return cursor.fetchall()

def log_metric_event(team_id: str, provider: str, event_type: str, details: str):
    try:
        with _connect_db() as conn:
            conn.execute("INSERT INTO stream_events (team_id, provider, event_type, details) VALUES (?, ?, ?, ?)", (team_id, provider, event_type, details))
            conn.commit()
    except Exception as exc:
        _log_failure("write metric event", exc)

def get_stability_metrics() -> dict:
    try:
        with _connect_db() as conn:
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

PROVIDER_EVENT_CONCURRENCY = max(1, min(16, int(os.getenv("PROVIDER_EVENT_CONCURRENCY", "6"))))
MAX_PROVIDER_EVENTS = max(1, min(200, int(os.getenv("MAX_PROVIDER_EVENTS", "60"))))


class HtmlAggregatorScraper(BaseProvider):
    """Configurable adapter for aggregators that expose linked event pages."""

    def __init__(self, name: str, base_url: str, categories: Optional[List[str]] = None, event_path_hints: Optional[List[str]] = None):
        self.name = name.strip() or "Aggregator"
        self.base_url = (base_url or "").strip().rstrip("/")
        self.categories = list(categories or [])
        self.event_path_hints = tuple(hint.lower() for hint in (event_path_hints or []) if hint)

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

    async def _find_matches(self, client: httpx.AsyncClient, search_terms: List[str]) -> List[tuple[str, int, str]]:
        matches: List[tuple[str, int, str]] = []
        seen_matches: Set[str] = set()
        for page_url in self.get_scan_urls():
            try:
                response = await client.get(
                    page_url,
                    headers={"User-Agent": DEFAULT_USER_AGENT, "Referer": self.base_url},
                    timeout=8.0,
                )
                if response.status_code != 200:
                    continue
                soup = BeautifulSoup(response.text, "html.parser")
                for anchor in soup.find_all("a", href=True):
                    href = str(anchor.get("href") or "")
                    if not self._is_event_link(href, page_url):
                        continue
                    match_url = urllib.parse.urljoin(page_url, href)
                    if match_url in seen_matches:
                        continue
                    title = str(anchor.get("title") or "")
                    raw_text = anchor.get_text(" ", strip=True)
                    candidate_text = raw_text or title or href
                    matched, score, _ = match_team(
                        search_terms,
                        candidate_text,
                        href=href,
                        title=title,
                    )
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
            matched_events = await self._find_matches(client, search_terms)
            event_semaphore = asyncio.Semaphore(PROVIDER_EVENT_CONCURRENCY)

            async def inspect_event(event: tuple[str, int, str]) -> List[dict]:
                match_url, score, match_title = event
                async with event_semaphore:
                    event_streams = await fetch_streams_from_page(
                        client, match_url, self.name, score, match_title
                    )
                    if not event_streams and browser and browser.is_connected():
                        event_streams = await playwright_intercept_streams(
                            browser, match_url, self.name, score, match_title
                        )
                    return event_streams

            results = await asyncio.gather(
                *(inspect_event(event) for event in matched_events),
                return_exceptions=True,
            )
            return [stream for result in results if isinstance(result, list) for stream in result]
        finally:
            if owns_client:
                await client.aclose()


# --- Hybrid fast-HTTP and Playwright scrapers ---
class ISportSurgeScraper(HtmlAggregatorScraper):
    name = "iSportSurge"
    base_url = os.getenv("AGGREGATOR_1_URL", "https://isportsurge.ws")
    categories = [
        "/cfb/livestreams2", "/nfl/livestreams3", "/mlb/livestreams2",
        "/nba/livestreams3", "/nhl/livestreams3", "/soccer/livestreams",
    ]

    def __init__(self):
        super().__init__(self.name, self.base_url, self.categories, ["/watch/", "/event/", "/title-game/"])

class MyBuffStreamsScraper(BaseProvider):
    name = "MyBuffStreams"
    base_url = os.getenv("AGGREGATOR_2_URL", "https://mybuffstreams.plus")
    categories = [
        "/cfbstreams2",
        "/nflstreams2",
        "/mlb-live-streams",
        "/nbastreams2",
        "/nhlstreams2",
        "/soccer-live-streams"
    ]

    async def search(
        self,
        query_or_terms,
        browser: Optional[Browser] = None,
        http_client: Optional[httpx.AsyncClient] = None
    ) -> List[dict]:
        search_terms = get_team_search_terms(query_or_terms, query_or_terms) if isinstance(query_or_terms, str) else query_or_terms
        streams = []
        client = http_client or httpx.AsyncClient(follow_redirects=True, timeout=12.0)
        
        scan_urls = self.get_scan_urls()
        matched_events = []
        seen_matches = set()

        for page_url in scan_urls:
            try:
                headers = {"User-Agent": DEFAULT_USER_AGENT, "Referer": self.base_url}
                resp = await client.get(page_url, headers=headers, timeout=8.0)
                if resp.status_code != 200:
                    continue
                soup = BeautifulSoup(resp.text, 'html.parser')
                for a_tag in soup.find_all('a', href=True):
                    href = a_tag.get('href', '')
                    if not any(k in href for k in ['/cfb/', '/mlb/', '/nfl/', '/nba/', '/nhl/', '/title-game/', '/watch/', '/soccer/']):
                        continue
                    raw_text = a_tag.get_text()
                    clean_text = clean_sports_text(raw_text)
                    if len(clean_text) < 3:
                        continue
                    m_url = urllib.parse.urljoin(self.base_url, href)
                    if m_url in seen_matches:
                        continue

                    matched, score, _ = match_team(search_terms, clean_text, href=href, title=a_tag.get('title', ''))
                    if matched:
                        seen_matches.add(m_url)
                        matched_events.append((m_url, score, raw_text.strip()))
            except Exception as exc:
                _log_failure(f"scan provider={self.name} page", exc)

        for m_url, score, match_title in matched_events:
            event_streams = await fetch_streams_from_page(client, m_url, self.name, score, match_title)
            if event_streams:
                streams.extend(event_streams)
            elif browser and browser.is_connected():
                pw_streams = await playwright_intercept_streams(browser, m_url, self.name, score, match_title)
                streams.extend(pw_streams)

        return streams

class MethStreamsScraper(BaseProvider):
    name = "MethStreams"
    base_url = os.getenv("AGGREGATOR_3_URL", "https://methstreams.click")
    categories = []

    async def search(
        self,
        query_or_terms,
        browser: Optional[Browser] = None,
        http_client: Optional[httpx.AsyncClient] = None
    ) -> List[dict]:
        search_terms = get_team_search_terms(query_or_terms, query_or_terms) if isinstance(query_or_terms, str) else query_or_terms
        streams = []
        client = http_client or httpx.AsyncClient(follow_redirects=True, timeout=12.0)
        seen_matches = set()
        matched_events = []

        try:
            headers = {"User-Agent": DEFAULT_USER_AGENT, "Referer": self.base_url}
            resp = await client.get(self.base_url, headers=headers, timeout=8.0)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                for a_tag in soup.find_all('a', href=True):
                    href = a_tag.get('href', '')
                    raw_text = a_tag.get_text()
                    clean_text = clean_sports_text(raw_text)
                    if len(clean_text) < 3: continue
                    m_url = urllib.parse.urljoin(self.base_url, href)
                    if m_url in seen_matches: continue

                    matched, score, _ = match_team(search_terms, clean_text, href=href, title=a_tag.get('title', ''))
                    if matched:
                        seen_matches.add(m_url)
                        matched_events.append((m_url, score, raw_text.strip()))
        except Exception as exc:
            _log_failure(f"scan provider={self.name}", exc, logging.ERROR)

        for m_url, score, match_title in matched_events:
            event_streams = await fetch_streams_from_page(client, m_url, self.name, score, match_title)
            if event_streams:
                streams.extend(event_streams)
            elif browser and browser.is_connected():
                pw_streams = await playwright_intercept_streams(browser, m_url, self.name, score, match_title)
                streams.extend(pw_streams)

        return streams

class StreamEastScraper(BaseProvider):
    name = "StreamEast"
    base_url = os.getenv("AGGREGATOR_4_URL", "https://thestreameast.top")
    categories = []

    async def search(
        self,
        query_or_terms,
        browser: Optional[Browser] = None,
        http_client: Optional[httpx.AsyncClient] = None
    ) -> List[dict]:
        search_terms = get_team_search_terms(query_or_terms, query_or_terms) if isinstance(query_or_terms, str) else query_or_terms
        streams = []
        client = http_client or httpx.AsyncClient(follow_redirects=True, timeout=12.0)
        seen_matches = set()
        matched_events = []

        try:
            headers = {"User-Agent": DEFAULT_USER_AGENT, "Referer": self.base_url}
            resp = await client.get(self.base_url, headers=headers, timeout=8.0)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                for a_tag in soup.find_all('a', href=True):
                    href = a_tag.get('href', '')
                    raw_text = a_tag.get_text()
                    clean_text = clean_sports_text(raw_text)
                    if len(clean_text) < 3: continue
                    m_url = urllib.parse.urljoin(self.base_url, href)
                    if m_url in seen_matches: continue

                    matched, score, _ = match_team(search_terms, clean_text, href=href, title=a_tag.get('title', ''))
                    if matched:
                        seen_matches.add(m_url)
                        matched_events.append((m_url, score, raw_text.strip()))
        except Exception as exc:
            _log_failure(f"scan provider={self.name}", exc, logging.ERROR)

        for m_url, score, match_title in matched_events:
            event_streams = await fetch_streams_from_page(client, m_url, self.name, score, match_title)
            if event_streams:
                streams.extend(event_streams)
            elif browser and browser.is_connected():
                pw_streams = await playwright_intercept_streams(browser, m_url, self.name, score, match_title)
                streams.extend(pw_streams)

        return streams

ACTIVE_PROVIDERS = [ISportSurgeScraper(), MyBuffStreamsScraper(), MethStreamsScraper(), StreamEastScraper()]
stream_state: Dict[str, dict] = {}
_SCRAPE_IN_FLIGHT: Set[str] = set()

SHARED_HTTP_CLIENT: Optional[httpx.AsyncClient] = None
PLAYWRIGHT_CLIENT: Optional[Playwright] = None
SHARED_BROWSER: Optional[Browser] = None
_BACKGROUND_TASKS: Set[asyncio.Task] = set()
_TEAM_SCRAPE_TASKS: Dict[str, asyncio.Task] = {}


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


def _stop_team_scrape_loop(team_id: str) -> None:
    task = _TEAM_SCRAPE_TASKS.pop(team_id, None)
    if task and not task.done():
        task.cancel()


CACHE_TTL_SECONDS = 120.0


def _positive_env_number(name: str, default: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
        return value if value > 0 else default
    except (TypeError, ValueError):
        return default


ACTIVE_HEALTH_INTERVAL = _positive_env_number("ACTIVE_HEALTH_INTERVAL", 3.0)
STANDBY_HEALTH_INTERVAL = _positive_env_number("STANDBY_HEALTH_INTERVAL", 45.0)
STANDBY_HEALTH_CONCURRENCY = max(1, int(_positive_env_number("STANDBY_HEALTH_CONCURRENCY", 4)))
EMERGENCY_SCRAPE_COOLDOWN = _positive_env_number("EMERGENCY_SCRAPE_COOLDOWN", 60.0)
STREAM_CHUNK_CACHE_TTL = _positive_env_number("STREAM_CHUNK_CACHE_TTL", 15.0)
STREAM_CHUNK_CACHE_CAPACITY = int(_positive_env_number("STREAM_CHUNK_CACHE_CAPACITY", 300))
PREFETCH_CHUNK_COUNT = max(1, min(5, int(_positive_env_number("PREFETCH_CHUNK_COUNT", 5))))
PREFETCH_CONCURRENCY = max(1, int(_positive_env_number("PREFETCH_CONCURRENCY", 2)))
STREAM_REQUEST_TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0)

class LRUChunkCache:
    def __init__(self, capacity: int = 150):
        self.cache = OrderedDict()
        self.capacity = capacity
        self.lock = threading.Lock()

    def get(self, key: str) -> Optional[bytes]:
        with self.lock:
            if key not in self.cache:
                return None
            val, exp = self.cache[key]
            if time.time() > exp:
                del self.cache[key]
                return None
            self.cache.move_to_end(key)
            return val

    def put(self, key: str, value: bytes, ttl: float):
        with self.lock:
            self.cache[key] = (value, time.time() + ttl)
            self.cache.move_to_end(key)
            if len(self.cache) > self.capacity:
                self.cache.popitem(last=False)

CHUNK_CACHE = LRUChunkCache(capacity=STREAM_CHUNK_CACHE_CAPACITY)
_PREFETCH_IN_FLIGHT: Set[str] = set()
_PREFETCH_LOCK = threading.Lock()
_PREFETCH_SEMAPHORE: Optional[asyncio.Semaphore] = None

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


def resolve_espn_logo(team_name: str) -> str:
    sport, slug, _ = _resolve_espn_team(team_name)
    if sport and slug:
        return f"{_ESPN_LOGO_CDN}/{sport}/500/{slug}.png?v=titan2"
    return ""


async def fetch_espn_team_schedule(
    team_name: str,
    query: str = "",
) -> tuple[Optional[datetime], Optional[datetime]]:
    matched_sport, matched_slug, _ = _resolve_espn_team(team_name or query)
    if not matched_sport or not matched_slug:
        return None, None

    api_map = {
        "nfl": ("football", "nfl"),
        "ncaa": ("football", "college-football"),
        "nba": ("basketball", "nba"),
        "mlb": ("baseball", "mlb"),
        "nhl": ("hockey", "nhl"),
        "soccer": ("soccer", "usa.1")
    }
    
    if matched_sport not in api_map:
        return None, None
        
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
        if resp.status_code == 200:
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
    except Exception as exc:
        _log_failure(f"fetch ESPN schedule team={team_name or query}", exc)
    finally:
        if owns_client:
            await client.aclose()
    if upcoming:
        return min(upcoming, key=lambda event: event[0])
    return None, None

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


def is_stream_window_active(data: dict, now: Optional[datetime] = None) -> bool:
    start, stop = parse_team_schedule(data)
    if not start or not stop:
        # Unknown schedules retain the old behavior instead of silently
        # removing a channel that may still have a valid live event.
        return True
    current = now or datetime.now(timezone.utc)
    return start - STREAM_LEAD_TIME <= current <= stop + STREAM_TRAIL_TIME

async def master_scrape(query: str, team_name: str = "", team_id: str = "") -> List[dict]:
    identity = canonical_team_name(team_name or query)
    search_terms = get_team_search_terms(identity or team_name or query, query, team_id)
    display_title = team_name or query
    LOGGER.info("Starting stream search for team=%s terms=%s", display_title, search_terms[:4])

    owns_client = SHARED_HTTP_CLIENT is None
    client = SHARED_HTTP_CLIENT or httpx.AsyncClient(
        limits=httpx.Limits(max_keepalive_connections=50, max_connections=150),
        timeout=12.0,
        follow_redirects=True,
        http2=True
    )

    async def _jittered_search(provider):
        await asyncio.sleep(random.uniform(0.5, 1.5))
        try:
            LOGGER.info("Querying provider=%s team=%s", provider.name, display_title)
            res = await provider.search(search_terms, browser=SHARED_BROWSER, http_client=client)
            LOGGER.info("Provider returned provider=%s team=%s streams=%d", provider.name, display_title, len(res))
            return res
        except Exception as e:
            _log_failure(f"provider search {provider.name} for {display_title}", e)
            return []

    try:
        tasks = [_jittered_search(provider) for provider in ACTIVE_PROVIDERS]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_streams = [stream for sublist in results if isinstance(sublist, list) for stream in sublist]
    finally:
        if owns_client:
            await client.aclose()

    seen_urls = set()
    deduped = []
    all_streams = rank_streams(all_streams, _provider_priority)
    for s in all_streams:
        u = s.get("url")
        if u and u not in seen_urls:
            seen_urls.add(u)
            deduped.append(s)

    return deduped

async def trigger_scrape(team_id: str):
    if team_id in _SCRAPE_IN_FLIGHT:
        LOGGER.debug("Skipping duplicate scrape team=%s", team_id)
        return
    _SCRAPE_IN_FLIGHT.add(team_id)
    try:
        await _trigger_scrape(team_id)
    finally:
        _SCRAPE_IN_FLIGHT.discard(team_id)


async def _trigger_scrape(team_id: str):
    if team_id not in stream_state:
        return
    query = stream_state[team_id]["query"]
    team_name = stream_state[team_id]["name"]
    logo_url = resolve_espn_logo(team_name)
    previous_start = stream_state[team_id].get("start_time", "")
    previous_stop = stream_state[team_id].get("stop_time", "")
    start_utc, stop_utc = await fetch_espn_team_schedule(team_name, query)
    
    if start_utc and stop_utc:
        start_str = xmltv_ts(start_utc)
        stop_str = xmltv_ts(stop_utc)
        stream_state[team_id]["start_time"] = start_str
        stream_state[team_id]["stop_time"] = stop_str
    else:
        start_str = previous_start
        stop_str = previous_stop

    stream_state[team_id]["logo_url"] = logo_url or stream_state[team_id].get("logo_url", "")
    stream_state[team_id]["start_time"] = start_str
    stream_state[team_id]["stop_time"] = stop_str
    update_team_meta(team_id, stream_state[team_id]["logo_url"], start_str, stop_str)

    if not is_stream_window_active(stream_state[team_id]):
        stream_state[team_id]["candidates"] = []
        stream_state[team_id]["active_index"] = 0
        stream_state[team_id]["is_healthy"] = False
        return

    previous_candidates = stream_state[team_id].get("candidates", [])
    new_candidates = await master_scrape(query, team_name=team_name, team_id=team_id)
    was_healthy = stream_state[team_id].get("is_healthy", False)
    if new_candidates:
        stream_state[team_id]["candidates"] = new_candidates
        stream_state[team_id]["active_index"] = 0
        is_healthy = True
        stream_state[team_id]["is_healthy"] = True
    elif previous_candidates:
        LOGGER.warning(
            "Keeping existing stream candidates after empty refresh team=%s count=%d",
            team_id,
            len(previous_candidates),
        )
        is_healthy = stream_state[team_id].get("is_healthy", False)
    else:
        stream_state[team_id]["candidates"] = []
        stream_state[team_id]["active_index"] = 0
        is_healthy = False
        stream_state[team_id]["is_healthy"] = False

    if not stream_state[team_id].get("logo_url"):
        for c in new_candidates:
            if c.get("logo_url"):
                stream_state[team_id]["logo_url"] = c["logo_url"]
                break

    if is_healthy and not was_healthy and len(new_candidates) > 0:
        _spawn_background_task(
            send_alert("✅ Stream Available", f"Stream active for **{team_name}**.", "success"),
            f"send stream-available alert team={team_id}",
        )

    if new_candidates:
        _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide")


async def team_scrape_loop(team_id: str):
    while team_id in stream_state:
        try:
            await trigger_scrape(team_id)
        except Exception as exc:
            _log_failure(f"scheduled scrape team={team_id}", exc, logging.ERROR)

        data = stream_state.get(team_id)
        if not data:
            return
        start, stop = parse_team_schedule(data)
        delay = SCRAPE_REFRESH_SECONDS
        if start and stop:
            now = datetime.now(timezone.utc)
            window_start = start - STREAM_LEAD_TIME
            if now < window_start:
                delay = min(SCRAPE_REFRESH_SECONDS, max(30, int((window_start - now).total_seconds())))
            elif now > stop + STREAM_TRAIL_TIME:
                delay = min(SCRAPE_REFRESH_SECONDS, 900)
        await asyncio.sleep(delay)

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
        try:
            candidates = await master_scrape(data["query"], team_name=team_name, team_id=team_id)
            current = stream_state.get(team_id)
            if current is not None:
                current["candidates"] = candidates
                current["active_index"] = 0
                current["is_healthy"] = bool(candidates)
        except Exception as exc:
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

            for team_id, data in list(stream_state.items()):
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
                is_alive = await check_stream_health(active_stream["url"], active_stream.get("referer", ""))
                
                if is_alive:
                    stream_state[team_id]["is_healthy"] = True
                    active_stream["last_health_check"] = time.time()
                    active_stream["last_health_ok"] = True
                else:
                    stream_state[team_id]["is_healthy"] = False
                    active_stream["last_health_check"] = time.time()
                    active_stream["last_health_ok"] = False
                    curr_provider = active_stream.get("provider", "Unknown")
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
                        new_provider = data["candidates"][next_idx].get("provider", "Unknown")
                        _spawn_background_task(
                            send_alert("⚠️ Stream Failover", f"Failed over to {new_provider} for **{team_name}**.", "warning"),
                            f"send failover alert team={team_id}",
                        )
                        stream_state[team_id]["active_index"] = next_idx

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
    
    init_db()
    stream_state.clear()
    _TEAM_SCRAPE_TASKS.clear()
    _SCRAPE_IN_FLIGHT.clear()
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
    for team_id, name, query, logo_url, start_time, stop_time in load_teams():
        stream_state[team_id] = {
            "name": name, "query": query, "candidates": [], "active_index": 0, "is_healthy": False,
            "logo_url": logo_url or resolve_espn_logo(name), "start_time": start_time or "", "stop_time": stop_time or "",
        }
        _start_team_scrape_loop(team_id)
        
    monitor_task = asyncio.create_task(failover_monitor(), name="failover monitor")
    prune_task = asyncio.create_task(prune_database_logs(), name="database log pruning")
    yield

    monitor_task.cancel()
    prune_task.cancel()
    scrape_tasks = list(_TEAM_SCRAPE_TASKS.values())
    for t in scrape_tasks:
        t.cancel()
    await asyncio.gather(monitor_task, prune_task, *scrape_tasks, return_exceptions=True)
    _TEAM_SCRAPE_TASKS.clear()
    await _cancel_background_tasks()
        
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

def rewrite_m3u8(manifest_text: str, target_url: str, referer: str, host: str) -> str:
    rewritten_manifest = []
    enc_ref = urllib.parse.quote(referer or "")

    def _proxy_resource_uri(uri: str, resource_path: str = "/chunk.ts") -> str:
        resolved_url = urllib.parse.urljoin(target_url, uri)
        if not _validate_upstream_url(resolved_url):
            return uri
        enc_url = urllib.parse.quote(resolved_url)
        if ".m3u8" in resolved_url.lower() or "/playlist/" in resolved_url.lower() or "manifest" in resolved_url.lower():
            return f"http://{host}/substream.m3u8?url={enc_url}&ref={enc_ref}"
        return f"http://{host}{resource_path}?url={enc_url}&ref={enc_ref}"

    for line in manifest_text.splitlines():
        line_clean = line.strip()
        if not line_clean:
            rewritten_manifest.append(line)
        elif line_clean.startswith("#"):
            if 'URI="' in line:
                line = re.sub(
                    r'URI="([^"]+)"',
                    lambda match: f'URI="{_proxy_resource_uri(match.group(1), "/resource" if line_clean.startswith(("#EXT-X-KEY", "#EXT-X-MAP")) else "/chunk.ts")}"',
                    line,
                )
            rewritten_manifest.append(line)
        else:
            rewritten_manifest.append(_proxy_resource_uri(line_clean))
    return "\n".join(rewritten_manifest)

@app.get("/stream/{team_id}")
async def proxy_stream(team_id: str, request: Request):
    data = stream_state.get(team_id)
    if not data or not data.get("candidates"):
        return Response(status_code=404, content="Stream unavailable")
    active_idx = data.get("active_index", 0)
    if active_idx >= len(data["candidates"]):
        active_idx = 0
    active_stream = data["candidates"][active_idx]
    target_url = active_stream["url"]
    if not _validate_upstream_url(target_url):
        LOGGER.warning("Rejected invalid active stream URL team=%s", team_id)
        return Response(status_code=502, content="Invalid upstream stream URL")
    referer = active_stream.get("referer", "")
    headers = {"User-Agent": "Mozilla/5.0", "Referer": referer}
    host = request.headers.get("host") or f"127.0.0.1:{PORT}"
    
    owns_client = SHARED_HTTP_CLIENT is None
    client = SHARED_HTTP_CLIENT or httpx.AsyncClient(follow_redirects=True, timeout=12.0, http2=True)
    try:
        resp = await client.get(target_url, headers=headers, timeout=STREAM_REQUEST_TIMEOUT)
        if resp.status_code != 200:
            return Response(status_code=resp.status_code)
        effective_url = str(resp.url)
        rewritten = rewrite_m3u8(resp.text, effective_url, referer, host)
        return Response(content=rewritten, media_type="application/vnd.apple.mpegurl")
    except Exception as exc:
        _log_failure(f"proxy manifest team={team_id}", exc, logging.ERROR)
        return Response(status_code=502, content="Upstream manifest unavailable")
    finally:
        if owns_client:
            await client.aclose()

@app.get("/substream.m3u8")
async def proxy_substream(request: Request, url: str, ref: str = ""):
    decoded_url = urllib.parse.unquote(url)
    decoded_ref = urllib.parse.unquote(ref) if ref else ""
    if not _validate_upstream_url(decoded_url):
        return Response(status_code=400, content="Invalid upstream manifest URL")
    if decoded_ref and not _validate_upstream_url(decoded_ref):
        return Response(status_code=400, content="Invalid upstream referer URL")
    headers = {"User-Agent": "Mozilla/5.0", "Referer": decoded_ref}
    host = request.headers.get("host") or f"127.0.0.1:{PORT}"
    
    owns_client = SHARED_HTTP_CLIENT is None
    client = SHARED_HTTP_CLIENT or httpx.AsyncClient(follow_redirects=True, timeout=12.0, http2=True)
    try:
        resp = await client.get(decoded_url, headers=headers, timeout=STREAM_REQUEST_TIMEOUT)
        if resp.status_code != 200:
            return Response(status_code=resp.status_code)
        effective_url = str(resp.url)
        rewritten = rewrite_m3u8(resp.text, effective_url, decoded_ref, host)
        return Response(content=rewritten, media_type="application/vnd.apple.mpegurl")
    except Exception as exc:
        _log_failure("proxy submanifest", exc, logging.ERROR)
        return Response(status_code=502, content="Upstream manifest unavailable")
    finally:
        if owns_client:
            await client.aclose()


@app.get("/resource")
async def proxy_resource(url: str, ref: str = ""):
    """Proxy HLS key and initialization resources with the original media type."""
    decoded_url = urllib.parse.unquote(url)
    decoded_ref = urllib.parse.unquote(ref) if ref else ""
    if not _validate_upstream_url(decoded_url):
        return Response(status_code=400, content="Invalid upstream resource URL")
    if decoded_ref and not _validate_upstream_url(decoded_ref):
        return Response(status_code=400, content="Invalid upstream referer URL")

    owns_client = SHARED_HTTP_CLIENT is None
    client = SHARED_HTTP_CLIENT or httpx.AsyncClient(
        follow_redirects=True,
        timeout=STREAM_REQUEST_TIMEOUT,
        http2=True,
    )
    try:
        response = await client.get(
            decoded_url,
            headers={"User-Agent": DEFAULT_USER_AGENT, "Referer": decoded_ref},
            timeout=STREAM_REQUEST_TIMEOUT,
        )
        if response.status_code != 200:
            return Response(status_code=response.status_code)
        media_type = response.headers.get("content-type", "application/octet-stream").split(";", 1)[0]
        return Response(content=response.content, media_type=media_type)
    except Exception as exc:
        _log_failure("proxy HLS resource", exc, logging.ERROR)
        return Response(status_code=502, content="Upstream resource unavailable")
    finally:
        if owns_client:
            await client.aclose()


async def prefetch_next_chunks(current_chunk_url: str, referer: str = "") -> None:
    global _PREFETCH_SEMAPHORE
    match = re.search(r"(\d+)(\.[^./?]+)(?=\?|$)", current_chunk_url)
    if not match:
        return

    number_text, extension = match.groups()
    current_number = int(number_text)
    client = SHARED_HTTP_CLIENT
    if client is None:
        return

    if _PREFETCH_SEMAPHORE is None:
        _PREFETCH_SEMAPHORE = asyncio.Semaphore(PREFETCH_CONCURRENCY)

    async def prefetch_one(next_number: int) -> None:
        next_text = f"{next_number:0{len(number_text)}d}"
        next_url = current_chunk_url[:match.start(1)] + next_text + current_chunk_url[match.end(1):]
        cache_key = f"{next_url}\0{referer}"
        if CHUNK_CACHE.get(cache_key):
            return
        with _PREFETCH_LOCK:
            if cache_key in _PREFETCH_IN_FLIGHT:
                return
            _PREFETCH_IN_FLIGHT.add(cache_key)

        try:
            async with _PREFETCH_SEMAPHORE:
                response = await client.get(
                    next_url,
                    headers={"User-Agent": DEFAULT_USER_AGENT, "Referer": referer},
                    timeout=STREAM_REQUEST_TIMEOUT,
                )
            if response.status_code in (200, 206) and response.content:
                CHUNK_CACHE.put(cache_key, response.content, STREAM_CHUNK_CACHE_TTL)
        except Exception as exc:
            LOGGER.debug("Chunk prefetch failed error=%s", type(exc).__name__)
        finally:
            with _PREFETCH_LOCK:
                _PREFETCH_IN_FLIGHT.discard(cache_key)

    await asyncio.gather(
        *(prefetch_one(current_number + offset) for offset in range(1, PREFETCH_CHUNK_COUNT + 1)),
        return_exceptions=True,
    )


@app.get("/chunk.ts")
@app.get("/chunk")
async def proxy_chunk(url: str, ref: str = ""):
    decoded_url = urllib.parse.unquote(url)
    decoded_ref = urllib.parse.unquote(ref) if ref else ""
    if not _validate_upstream_url(decoded_url):
        return Response(status_code=400, content="Invalid upstream chunk URL")
    if decoded_ref and not _validate_upstream_url(decoded_ref):
        return Response(status_code=400, content="Invalid upstream referer URL")

    cache_key = f"{decoded_url}\0{decoded_ref}"
    cached_bytes = CHUNK_CACHE.get(cache_key)
    if cached_bytes:
        _spawn_background_task(prefetch_next_chunks(decoded_url, decoded_ref), "prefetch cached stream chunks")
        return Response(content=cached_bytes, media_type="video/mp2t")

    headers = {"User-Agent": "Mozilla/5.0", "Referer": decoded_ref}
    _spawn_background_task(prefetch_next_chunks(decoded_url, decoded_ref), "prefetch stream chunks")

    async def stream_generator():
        chunk_buffer = bytearray()
        owns_client = SHARED_HTTP_CLIENT is None
        client = SHARED_HTTP_CLIENT or httpx.AsyncClient(
            timeout=STREAM_REQUEST_TIMEOUT, follow_redirects=True, http2=True
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
                            chunk_buffer.extend(block)
                            yield block
                        if chunk_buffer:
                            CHUNK_CACHE.put(cache_key, bytes(chunk_buffer), STREAM_CHUNK_CACHE_TTL)
                        return
                except (httpx.TimeoutException, httpx.NetworkError) as exc:
                    LOGGER.debug("Chunk request attempt failed attempt=%d error=%s", attempt + 1, type(exc).__name__)
                    if attempt < 2:
                        await asyncio.sleep(0.25 * (attempt + 1))
                except Exception as exc:
                    _log_failure("proxy chunk stream", exc)
                    return
        finally:
            if owns_client:
                await client.aclose()

    return StreamingResponse(stream_generator(), media_type="video/mp2t")

@app.get("/playlist.m3u", response_class=PlainTextResponse)
async def generate_m3u(request: Request):
    host = request.headers.get("host") or f"127.0.0.1:{PORT}"
    lines = ["#EXTM3U"]
    for team_id, data in stream_state.items():
        local_proxy_url = f"http://{host}/stream/{team_id}"
        logo = data.get("logo_url", "") or resolve_espn_logo(data["name"])
        logo_attr = f' tvg-logo="{logo}"' if logo else ""
        lines.append(f'#EXTINF:-1 tvg-id="{team_id}" tvg-name="{data["name"]}"{logo_attr},{data["name"]}')
        lines.append(local_proxy_url)
    return "\n".join(lines)

@app.get("/epg.xml", response_class=PlainTextResponse)
async def generate_xmltv():
    now_utc = datetime.now(timezone.utc)
    xml = ['<?xml version="1.0" encoding="UTF-8"?>', '<tv>']
    
    for team_id, data in stream_state.items():
        name_esc = xml_escape(data["name"])
        logo = data.get("logo_url", "") or resolve_espn_logo(data["name"])
        icon_tag = f'\n    <icon src="{xml_escape(logo)}" />' if logo else ""
        xml.append(f'  <channel id="{team_id}">')
        xml.append(f'    <display-name>{name_esc}</display-name>{icon_tag}')
        xml.append('  </channel>')

    guide_start = now_utc - timedelta(hours=1)
    guide_end = now_utc + timedelta(days=14)

    for team_id, data in stream_state.items():
        name_esc = xml_escape(data["name"])
        logo = data.get("logo_url", "") or resolve_espn_logo(data["name"])
        icon_tag = f'\n    <icon src="{xml_escape(logo)}" />' if logo else ""
        
        dt_start, dt_stop = parse_team_schedule(data)
        has_specific_game = bool(
            dt_start and dt_stop and dt_stop >= guide_start and dt_start <= guide_end
        )

        if has_specific_game:
            if dt_start > guide_start:
                xml.append(f'  <programme channel="{team_id}" start="{xmltv_ts(guide_start)}" stop="{xmltv_ts(min(dt_start, guide_end))}">')
                xml.append(f'    <title>{name_esc} Standby</title>')
                xml.append(f'    <desc>Waiting for the scheduled {name_esc} broadcast.</desc>')
                if icon_tag: xml.append(f'    <icon src="{xml_escape(logo)}" />')
                xml.append('  </programme>')

            live_start = max(dt_start, guide_start)
            live_stop = min(dt_stop, guide_end)
            xml.append(f'  <programme channel="{team_id}" start="{xmltv_ts(live_start)}" stop="{xmltv_ts(live_stop)}">')
            xml.append(f'    <title>{name_esc} Scheduled Event</title>')
            xml.append(f'    <desc>Scheduled live stream window for {name_esc}. Searching starts one hour before the event and continues one hour after the scheduled end.</desc>')
            if icon_tag: xml.append(f'    <icon src="{xml_escape(logo)}" />')
            xml.append('  </programme>')

            if dt_stop < guide_end:
                xml.append(f'  <programme channel="{team_id}" start="{xmltv_ts(max(dt_stop, guide_start))}" stop="{xmltv_ts(guide_end)}">')
                xml.append(f'    <title>{name_esc} Standby</title>')
                xml.append(f'    <desc>Post-event standby for {name_esc}; the next scheduled event will refresh this guide.</desc>')
                if icon_tag: xml.append(f'    <icon src="{xml_escape(logo)}" />')
                xml.append('  </programme>')
        else:
            xml.append(f'  <programme channel="{team_id}" start="{xmltv_ts(guide_start)}" stop="{xmltv_ts(guide_end)}">')
            xml.append(f'    <title>{name_esc} Standby</title>')
            xml.append(f'    <desc>No verified scheduled event is available for {name_esc} in the next 14 days.</desc>')
            if icon_tag: xml.append(f'    <icon src="{xml_escape(logo)}" />')
            xml.append('  </programme>')

    xml.append('</tv>')
    return "\n".join(xml)


@app.get("/api/status")
async def api_status(auth: bool = Depends(verify_dashboard_auth)):
    """Return a compact status snapshot for local dashboards and health checks."""
    channels = []
    for team_id, data in stream_state.items():
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
        lines = LOG_FILE.read_text(encoding="utf-8", errors="replace").splitlines()
        return {"logs": lines[-limit:], "count": min(len(lines), limit)}
    except OSError:
        return {"logs": [], "count": 0}


@app.get("/api/review-queue")
async def api_review_queue(auth: bool = Depends(verify_dashboard_auth)):
    """Return an empty queue until manual stream review is implemented."""
    return {"items": [], "count": 0}


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, tab: str = "channels", status: str = "", auth: bool = Depends(verify_dashboard_auth)):
    base_url = f"{request.url.scheme}://{request.headers.get('host', f'127.0.0.1:{PORT}')}"
    base_url_html = _html(base_url)
    metrics = get_stability_metrics()
    notif_cfg = get_notification_config()
    jellyfin_cfg = get_jellyfin_config()
    
    failover_stats_html = ""
    if metrics["failovers_by_provider"]:
        for prov, count in metrics["failovers_by_provider"].items():
            failover_stats_html += f'<span class="badge" style="margin-right: 0.5rem; background: #334155;">{_html(prov)}: {_html(count)} failover(s)</span>'
    else:
        failover_stats_html = '<span style="color: var(--text-muted); font-size: 0.85rem;">No failover incidents recorded yet.</span>'

    events_html = ""
    if metrics["recent_events"]:
        for ts, team_id, prov, ev_type, details in metrics["recent_events"]:
            badge_color = "#dc2626" if ev_type in ["exhausted", "danger"] else ("#eab308" if ev_type == "failover" else "#059669")
            events_html += f'<tr style="border-bottom: 1px solid #1e293b; font-size: 0.85rem;"><td style="padding: 0.5rem; color: var(--text-muted);">{_html(ts)}</td><td style="padding: 0.5rem; font-weight: 600;">{_html(team_id)}</td><td style="padding: 0.5rem;"><span style="background: {badge_color}; color: white; padding: 0.15rem 0.4rem; border-radius: 4px;">{_html(ev_type)}</span></td><td style="padding: 0.5rem; color: #cbd5e1;">{_html(prov)}</td><td style="padding: 0.5rem; color: var(--text-muted);">{_html(details)}</td></tr>'
    else:
        events_html = '<tr><td colspan="5" style="padding: 1rem; text-align: center; color: var(--text-muted); font-size: 0.85rem;">No historical events recorded yet.</td></tr>'

    auth_badge = '<span class="badge" style="background: #065f46; color: #6ee7b7; border-color: #047857;">🔒 Password Protected</span>' if DASHBOARD_PASSWORD else '<span class="badge" style="background: #1e293b; color: #94a3b8;">🔓 Open Access</span>'
    webhook_discord_badge = '<span class="badge" style="background: #5865F2; color: white;">Discord Alert On</span>' if notif_cfg["discord_webhook_url"] else '<span class="badge" style="background: #1e293b; color: #64748b;">Discord Off</span>'
    webhook_telegram_badge = '<span class="badge" style="background: #229ED9; color: white;">Telegram Alert On</span>' if (notif_cfg["telegram_bot_token"] and notif_cfg["telegram_chat_id"]) else '<span class="badge" style="background: #1e293b; color: #64748b;">Telegram Off</span>'
    jellyfin_badge = '<span class="badge" style="background: #a855f7; color: white;">🍇 Jellyfin Auto-Refresh On</span>' if jellyfin_cfg["jellyfin_api_key"] else '<span class="badge" style="background: #1e293b; color: #64748b;">🍇 Jellyfin Manual</span>'

    channels_html = ""
    if not stream_state:
        channels_html = '<div class="card" style="grid-column: 1 / -1; text-align: center; color: var(--text-muted); padding: 3rem;">No teams configured.</div>'
    else:
        for t_id, data in stream_state.items():
            dot_class = "online" if data.get('is_healthy') else "offline"
            status_text = "Stream Stable & Active" if data.get('is_healthy') else "Searching / Re-evaluating"
            candidates_list = data.get('candidates', [])
            candidates_len = len(candidates_list)
            
            candidates_options_html = ""
            for idx, cand in enumerate(candidates_list):
                is_active = "★ " if idx == data.get('active_index', 0) else ""
                prov = cand.get('provider', 'Unknown')
                title_trunc = cand.get("match_title", "Stream")[:25]
                candidates_options_html += f'<option value="{idx}">{_html(is_active)}{_html(prov)} - {_html(title_trunc)}</option>'
                
            override_form = f'''
            <form action="/override/{_html(t_id)}" method="post" style="margin-top:0.75rem; display:flex; gap:0.5rem; align-items:center;">
                <select name="candidate_index" style="flex:1; padding:0.55rem; border-radius:6px; background:#0f172a; color:#fff; border:1px solid #334155; font-size:0.85rem;">
                    {candidates_options_html}
                </select>
                <button type="submit" style="width:auto; padding:0.55rem 0.85rem; font-size:0.8rem; background:#334155; border:1px solid #475569;">Force Override</button>
            </form>
            ''' if candidates_list else '<p style="font-size:0.85rem; color:#64748b; margin-top:0.75rem;">No candidates available to override</p>'
            
            channels_html += f'''
            <div class="card" style="margin-bottom: 0;">
                <div class="team-header">
                    <h4 class="team-name">{_html(data["name"])}</h4>
                    <span class="badge">Query: {_html(data["query"])}</span>
                </div>
                <div class="status-row">
                    <div class="dot {dot_class}"></div> 
                    <span>{status_text}</span>
                </div>
                <p class="meta-text">Available Backups: {candidates_len}</p>
                {override_form}
                <form action="/remove_team/{_html(t_id)}" method="post">
                    <button class="btn-danger" type="submit">Remove Tracker</button>
                </form>
            </div>'''

    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Titan Sports Manager</title>
        <style>
            :root {{ --bg: #090d16; --card-bg: #131d31; --border: #1e293b; --text: #f1f5f9; --text-muted: #94a3b8; --accent: #2563eb; --accent-hover: #1d4ed8; --success: #059669; --danger: #dc2626; }}
            body {{ font-family: system-ui, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 2.5rem 1.5rem; }}
            .container {{ max-width: 920px; margin: 0 auto; }}
            header {{ text-align: center; margin-bottom: 1.75rem; }} header h1 {{ font-size: 2.2rem; font-weight: 800; margin: 0 0 0.5rem 0; color: #fff; }} header p {{ color: var(--text-muted); font-size: 0.95rem; margin: 0; }}
            .status-badges {{ display: flex; justify-content: center; gap: 0.5rem; margin-top: 0.75rem; flex-wrap: wrap; }}
            .tabs-nav {{ display: flex; gap: 0.5rem; margin-bottom: 1.5rem; border-bottom: 1px solid var(--border); padding-bottom: 0.75rem; }}
            .tab-btn {{ background: #131d31; color: var(--text-muted); border: 1px solid var(--border); padding: 0.7rem 1.25rem; border-radius: 8px; font-weight: 600; font-size: 0.9rem; cursor: pointer; transition: all 0.2s; width: auto; }}
            .tab-btn:hover {{ color: #fff; border-color: #334155; }} .tab-btn.active {{ background: var(--accent); color: #fff; border-color: var(--accent); box-shadow: 0 4px 12px rgba(37,99,235,0.3); }}
            .tab-pane {{ display: none; }} .tab-pane.active {{ display: block; }}
            .card {{ background: var(--card-bg); border: 1px solid var(--border); border-radius: 14px; padding: 1.75rem; margin-bottom: 1.5rem; box-shadow: 0 10px 15px -3px rgba(0,0,0,0.3); }}
            .card h3 {{ margin-top: 0; margin-bottom: 1.25rem; font-size: 1.25rem; font-weight: 600; }}
            .form-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-bottom: 1rem; }}
            input[type="text"] {{ width: 100%; padding: 0.85rem 1rem; background: #090d16; border: 1px solid var(--border); border-radius: 8px; color: var(--text); font-size: 0.95rem; box-sizing: border-box; }}
            input[type="text"]:focus {{ outline: none; border-color: var(--accent); }}
            button {{ width: 100%; padding: 0.85rem; background: var(--accent); color: white; border: none; border-radius: 8px; font-weight: 600; font-size: 0.95rem; cursor: pointer; transition: background 0.2s; }}
            button:hover {{ background: var(--accent-hover); }}
            .btn-secondary {{ background: #1e293b; color: #cbd5e1; border: 1px solid #334155; }} .btn-secondary:hover {{ background: #334155; color: #fff; }}
            .btn-danger {{ background: var(--danger); margin-top: 1rem; }} .btn-danger:hover {{ background: #b91c1c; }}
            .team-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 1.25rem; }}
            .team-header {{ display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 0.75rem; }}
            .team-name {{ font-size: 1.15rem; font-weight: 700; margin: 0; }}
            .badge {{ background: #1e293b; color: #cbd5e1; padding: 0.25rem 0.6rem; border-radius: 6px; font-size: 0.8rem; border: 1px solid #334155; }}
            .status-row {{ display: flex; align-items: center; gap: 0.6rem; font-size: 0.9rem; font-weight: 500; margin-bottom: 0.5rem; }}
            .dot {{ width: 10px; height: 10px; border-radius: 50%; }} .dot.online {{ background: var(--success); box-shadow: 0 0 10px var(--success); }} .dot.offline {{ background: var(--danger); box-shadow: 0 0 10px var(--danger); }}
            .meta-text {{ color: var(--text-muted); font-size: 0.85rem; margin: 0; }}
            .feed-box {{ display: flex; flex-direction: column; gap: 0.75rem; }} .feed-item label {{ font-size: 0.8rem; color: #94a3b8; font-weight: 600; text-transform: uppercase; display: block; margin-bottom: 0.25rem; }}
            .feed-item input {{ font-family: monospace; font-size: 0.9rem; cursor: pointer; background: #0c1220; }}
            table {{ width: 100%; border-collapse: collapse; }} .hint-text {{ font-size: 0.8rem; color: #94a3b8; margin-top: 0.35rem; line-height: 1.4; }}
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>Live Sports Proxy Engine</h1>
                <p>Multi-Aggregator Scraper & Jellyfin Live TV Gateway</p>
                <div class="status-badges">{auth_badge} {jellyfin_badge} {webhook_discord_badge} {webhook_telegram_badge}</div>
            </header>
            <div class="tabs-nav">
                <button type="button" class="tab-btn {'active' if tab == 'channels' else ''}" id="btn-channels" onclick="switchTab('channels')">📺 Channels & Streams</button>
                <button type="button" class="tab-btn {'active' if tab == 'metrics' else ''}" id="btn-metrics" onclick="switchTab('metrics')">📊 Stability Metrics</button>
                <button type="button" class="tab-btn {'active' if tab == 'alerts' else ''}" id="btn-alerts" onclick="switchTab('alerts')">🔔 Alerts & Integrations</button>
            </div>
            <div id="tab-channels" class="tab-pane {'active' if tab == 'channels' else ''}">
                <div class="card" style="border-color: #2563eb; background: linear-gradient(180deg, #131d31 0%, #0d1527 100%);">
                    <h3 style="color: #60a5fa; margin-bottom: 0.5rem;">Jellyfin Integration Endpoints</h3>
                    <div class="feed-box">
                        <div class="feed-item"><label>M3U Tuner Playlist URL (Click to copy)</label><input type="text" readonly value="{base_url_html}/playlist.m3u" onclick="this.select(); navigator.clipboard.writeText(this.value);"></div>
                        <div class="feed-item"><label>XMLTV EPG Guide URL (Click to copy)</label><input type="text" readonly value="{base_url_html}/epg.xml" onclick="this.select(); navigator.clipboard.writeText(this.value);"></div>
                    </div>
                </div>
                <div class="card">
                    <h3>Add New Team Tracker</h3>
                    <form action="/add_team" method="post">
                        <div class="form-grid"><input type="text" name="team_name" placeholder="Display Name (e.g., Florida Gators)" required><input type="text" name="search_query" placeholder="Search Term (e.g., Gators)" required></div>
                        <button type="submit">Initialize Multi-Site Scraper</button>
                    </form>
                </div>
                <div class="team-grid">{channels_html}</div>
            </div>
            <div id="tab-metrics" class="tab-pane {'active' if tab == 'metrics' else ''}">
                <div class="card">
                    <h3>Stream Stability & Provider Metrics</h3>
                    <div style="margin-bottom: 1.25rem;"><div>{failover_stats_html}</div></div>
                    <div style="overflow-x: auto;">
                        <table>
                            <thead><tr style="border-bottom: 1px solid #334155; text-align: left; font-size: 0.8rem; color: #94a3b8; text-transform: uppercase;"><th style="padding: 0.5rem;">Time (UTC)</th><th style="padding: 0.5rem;">Team</th><th style="padding: 0.5rem;">Event</th><th style="padding: 0.5rem;">Source</th><th style="padding: 0.5rem;">Details</th></tr></thead>
                            <tbody>{events_html}</tbody>
                        </table>
                    </div>
                </div>
            </div>
            <div id="tab-alerts" class="tab-pane {'active' if tab == 'alerts' else ''}">
                <div class="card" style="border-color: #a855f7; background: linear-gradient(180deg, #131d31 0%, #171026 100%);">
                    <h3 style="color: #c084fc;">🍇 Jellyfin Automatic Guide Refresh API</h3>
                    <form action="/settings/jellyfin" method="post" style="display: flex; flex-direction: column; gap: 1.25rem;">
                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;">
                            <div><label style="font-size: 0.85rem; color: #cbd5e1; font-weight: 600; display: block; margin-bottom: 0.35rem;">🌐 Jellyfin Server URL</label><input type="text" name="jellyfin_url" value="{_html(jellyfin_cfg['jellyfin_url'])}"></div>
                            <div><label style="font-size: 0.85rem; color: #cbd5e1; font-weight: 600; display: block; margin-bottom: 0.35rem;">🔑 Jellyfin API Key</label><input type="text" name="jellyfin_api_key" value="{_html(jellyfin_cfg['jellyfin_api_key'])}"></div>
                        </div>
                        <div><label style="font-size: 0.85rem; color: #cbd5e1; font-weight: 600; display: block; margin-bottom: 0.35rem;">⚙️ Refresh Guide Scheduled Task ID</label><input type="text" name="jellyfin_task_id" value="{_html(jellyfin_cfg['jellyfin_task_id'])}"></div>
                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-top: 0.25rem;"><button type="submit" style="background: #9333ea;">💾 Save Jellyfin API Settings</button></div>
                    </form>
                    <form action="/settings/test_jellyfin" method="post" style="margin-top: 1rem;"><button type="submit" class="btn-secondary" style="border-color: #a855f7; color: #e9d5ff;">🍇 Force Jellyfin Guide Refresh Now</button></form>
                </div>
                <div class="card">
                    <h3>Webhook Notification Settings</h3>
                    <form action="/settings/notifications" method="post" style="display: flex; flex-direction: column; gap: 1.25rem;">
                        <div><label style="font-size: 0.85rem; color: #cbd5e1; font-weight: 600; display: block; margin-bottom: 0.35rem;">👾 Discord Incoming Webhook URL</label><input type="text" name="discord_webhook_url" value="{_html(notif_cfg['discord_webhook_url'])}"></div>
                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;">
                            <div><label style="font-size: 0.85rem; color: #cbd5e1; font-weight: 600; display: block; margin-bottom: 0.35rem;">✈️ Telegram Bot Token</label><input type="text" name="telegram_bot_token" value="{_html(notif_cfg['telegram_bot_token'])}"></div>
                            <div><label style="font-size: 0.85rem; color: #cbd5e1; font-weight: 600; display: block; margin-bottom: 0.35rem;">💬 Telegram Chat ID</label><input type="text" name="telegram_chat_id" value="{_html(notif_cfg['telegram_chat_id'])}"></div>
                        </div>
                        <button type="submit" style="margin-top: 0.5rem;">💾 Save Notification Settings</button>
                    </form>
                </div>
            </div>
        </div>
        <script>
            function switchTab(tabName) {{
                document.querySelectorAll('.tab-pane').forEach(el => el.classList.remove('active'));
                document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
                const pane = document.getElementById('tab-' + tabName);
                const btn = document.getElementById('btn-' + tabName);
                if (pane) pane.classList.add('active');
                if (btn) btn.classList.add('active');
                const url = new URL(window.location);
                url.searchParams.set('tab', tabName);
                window.history.replaceState({{}}, '', url);
            }}
            window.addEventListener('DOMContentLoaded', () => {{
                const params = new URLSearchParams(window.location.search);
                const activeTab = params.get('tab');
                if (activeTab && document.getElementById('tab-' + activeTab)) switchTab(activeTab);
            }});
        </script>
    </body>
    </html>
    """
    return html

@app.post("/settings/notifications")
async def update_notifications(discord_webhook_url: str = Form(""), telegram_bot_token: str = Form(""), telegram_chat_id: str = Form(""), auth: bool = Depends(verify_dashboard_auth)):
    set_setting("discord_webhook_url", discord_webhook_url.strip())
    set_setting("telegram_bot_token", telegram_bot_token.strip())
    set_setting("telegram_chat_id", telegram_chat_id.strip())
    return RedirectResponse(url="/?tab=alerts&status=saved", status_code=303)

@app.post("/settings/jellyfin")
async def update_jellyfin_settings(jellyfin_url: str = Form("http://localhost:8096"), jellyfin_api_key: str = Form(""), jellyfin_task_id: str = Form(""), auth: bool = Depends(verify_dashboard_auth)):
    jellyfin_url = jellyfin_url.strip().rstrip("/")
    if not _validate_upstream_url(jellyfin_url):
        raise HTTPException(status_code=400, detail="Jellyfin URL must be an absolute HTTP(S) URL")
    set_setting("jellyfin_url", jellyfin_url.strip())
    set_setting("jellyfin_api_key", jellyfin_api_key.strip())
    set_setting("jellyfin_task_id", jellyfin_task_id.strip())
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

@app.post("/add_team")
async def add_team(team_name: str = Form(...), search_query: str = Form(...), auth: bool = Depends(verify_dashboard_auth)):
    team_name = team_name.strip()
    search_query = search_query.strip()
    if not team_name or not search_query:
        raise HTTPException(status_code=400, detail="Team name and search query are required")
    team_id = _safe_team_id(team_name)
    save_team(team_id, team_name, search_query)
    stream_state[team_id] = {
        "name": team_name,
        "query": search_query,
        "candidates": [],
        "active_index": 0,
        "is_healthy": False,
        "logo_url": "",
        "start_time": "",
        "stop_time": "",
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
        _stop_team_scrape_loop(team_id)
        del stream_state[team_id]
        delete_team(team_id)
        _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after team removal")
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

        config = uvicorn.Config(
            app,
            host="0.0.0.0",
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
        try:
            self.icon.run()
        finally:
            self.server.should_exit = True
            self.server_thread.join(timeout=10)


if __name__ == "__main__":
    TrayApplication().run()
