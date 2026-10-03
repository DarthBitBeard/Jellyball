"""Support bundle (X6): one zip a user can attach to a bug report.

What goes in: version/OS/ffmpeg/Jellyfin facts, app settings, provider health,
the live session and channel snapshots, and the tail of the log. What must
never go in: API keys, webhook URLs, bot tokens, the password hash, and the
tokens stream URLs carry. So every value passes through redact() on the way
out - secret-named settings are blanked, URLs are cut to scheme://host/...,
query strings and key=value secrets are dropped, and home-directory user names
and e-mail addresses are masked. The bundle says so in its README; the user
should still read it before posting it publicly.
"""

import asyncio
import io
import json
import os
import platform
import re
import sys
import time
import zipfile
from typing import Any, Dict, List

from config import DATA_DIR, LOG_FILE

REDACTED = "<redacted>"

# Settings whose value is a credential or identifies the user's accounts.
SECRET_SETTING_KEYS = frozenset({
    "jellyfin_api_key",
    "discord_webhook_url",
    "telegram_bot_token",
    "telegram_chat_id",
    "dashboard_password_hash",
    "dashboard_session_epoch",
})
_SECRET_NAME_RE = re.compile(r"(?i)(pass(word|wd)?|secret|token|api[_-]?key|apikey|webhook|cookie|auth|hash|chat_id|sig)")

_URL_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9+.-]*://[^\s'\"<>]+")
_ENCODED_URL_RE = re.compile(r"(?i)[a-z][a-z0-9+.-]*%3A%2F%2F[^\s'\"<>&]+")
_QUERY_RE = re.compile(r"\?[^\s'\"<>]*")
_KEY_VALUE_RE = re.compile(
    r"(?i)\b([\w.-]*(?:pass(?:word|wd)?|secret|token|api[_-]?key|apikey|auth|sig(?:nature)?|key|session)[\w.-]*)"
    r"(\s*[=:]\s*)(\"[^\"]*\"|'[^']*'|[^\s,;&'\"]+)"
)
_AUTH_HEADER_RE = re.compile(r"(?i)\b(authorization|x-emby-token|x-mediabrowser-token|cookie|set-cookie)(\s*[:=]\s*)([^\r\n]+)")
_BEARER_RE = re.compile(r"(?i)\b(bearer|basic|mediabrowser)\s+[A-Za-z0-9._~+/=-]{8,}")
_JWT_RE = re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]*")
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_WINDOWS_HOME_RE = re.compile(r"(?i)([a-z]:[\\/]+users[\\/]+)[^\\/\s'\"]+")
_POSIX_HOME_RE = re.compile(r"(/(?:home|Users)/)[^/\s'\"]+")
_LONG_SECRETISH_RE = re.compile(r"\b[A-Fa-f0-9]{32,}\b")


def _host_only(match: "re.Match[str]") -> str:
    scheme, _, rest = match.group(0).partition("://")
    host = rest.split("/", 1)[0].split("?", 1)[0].split("#", 1)[0].rsplit("@", 1)[-1]
    return f"{scheme}://{host}/..."


def redact_text(text: str) -> str:
    """Scrub one string. Order matters: URLs first (their paths and queries
    carry tokens), then whatever secret-looking pieces are left."""
    text = _URL_RE.sub(_host_only, str(text))
    text = _ENCODED_URL_RE.sub("<url>", text)
    text = _QUERY_RE.sub("?" + REDACTED, text)
    text = _AUTH_HEADER_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}{REDACTED}", text)
    text = _BEARER_RE.sub(lambda m: f"{m.group(1)} {REDACTED}", text)
    text = _KEY_VALUE_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}{REDACTED}", text)
    text = _JWT_RE.sub(REDACTED, text)
    text = _LONG_SECRETISH_RE.sub(REDACTED, text)
    text = _EMAIL_RE.sub("<email>", text)
    text = _WINDOWS_HOME_RE.sub(lambda m: m.group(1) + "<user>", text)
    text = _POSIX_HOME_RE.sub(lambda m: m.group(1) + "<user>", text)
    return text


def is_secret_name(name: str) -> bool:
    return name in SECRET_SETTING_KEYS or bool(_SECRET_NAME_RE.search(name or ""))


def redact(value: Any, name: str = "") -> Any:
    """Recursively scrub a JSON-able value; `name` is its key, if any."""
    if name and is_secret_name(name) and not isinstance(value, (dict, list, tuple)):
        return REDACTED if value not in (None, "", 0, False) else value
    if isinstance(value, dict):
        return {str(k): redact(v, str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact(v) for v in value]
    if isinstance(value, str):
        return redact_text(value)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return redact_text(str(value))


def _read_all_settings_sync() -> Dict[str, str]:
    import db  # late: db is heavy and this module is imported at startup

    try:
        with db._db_session() as conn:
            return {str(k): "" if v is None else str(v) for k, v in conn.execute("SELECT key, value FROM app_settings")}
    except Exception as exc:  # noqa: BLE001 - a broken DB is exactly what a bundle should report
        return {"<error>": f"{type(exc).__name__}"}


def redacted_settings(settings: Dict[str, str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, value in sorted(settings.items()):
        if is_secret_name(key):
            out[key] = REDACTED if value else ""
            continue
        try:
            decoded = json.loads(value) if value[:1] in "{[" else None
        except ValueError:
            decoded = None
        out[key] = redact(decoded) if decoded is not None else redact_text(value)
    return out


def tail_log_lines(limit: int = 2000) -> List[str]:
    try:
        with LOG_FILE.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            chunk = min(size, max(256 * 1024, limit * 400))
            handle.seek(size - chunk)
            data = handle.read(chunk)
    except OSError:
        return []
    lines = data.decode("utf-8", errors="replace").splitlines()
    if chunk < size and lines:
        lines = lines[1:]
    return [redact_text(line) for line in lines[-limit:]]


def _channel_summary(channel_id: str, data: dict) -> dict:
    candidates = data.get("candidates") or []
    return {
        "id": channel_id,
        "name": data.get("name", ""),
        "type": data.get("type", "team"),
        "category": data.get("category", ""),
        "healthy": bool(data.get("is_healthy")),
        "exhausted": bool(data.get("exhausted")),
        "candidate_count": len(candidates),
        "providers": sorted({str(c.get("provider", "")) for c in candidates if isinstance(c, dict) and c.get("provider")}),
        "active_index": data.get("active_index", 0),
        "failover_count": int(data.get("failover_count", 0) or 0),
        "schedule_status": data.get("schedule_status", ""),
        "scrape_result": data.get("scrape_result", ""),
        "scrape_error": data.get("scrape_error", ""),
        "start_time": data.get("start_time", ""),
        "stop_time": data.get("stop_time", ""),
    }


async def collect() -> Dict[str, Any]:
    """Everything the bundle holds, already redacted, keyed by file name."""
    import ffmpeg_proc
    import http_hardening
    import security
    from jellyfin_client import load_status as load_jellyfin_status
    from routes_api import _session_status
    from scrapers import provider_breaker_snapshot
    from sessions import SESSIONS
    from state import stream_state
    from tunables import _advanced_settings_snapshot
    from updates import _UPDATE_STATE
    from version import __version__

    settings = await asyncio.to_thread(_read_all_settings_sync)
    try:
        from db import _performance_stats_sync

        performance = await asyncio.to_thread(_performance_stats_sync)
    except Exception as exc:  # noqa: BLE001
        performance = {"error": type(exc).__name__}
    try:
        jellyfin = await load_jellyfin_status()
    except Exception as exc:  # noqa: BLE001
        jellyfin = {"error": type(exc).__name__}

    summary = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "version": __version__,
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "frozen": bool(getattr(sys, "frozen", False)),
        "data_dir": str(DATA_DIR),
        "dashboard_auth_mode": security.DASHBOARD_AUTH_MODE,
        "api_docs_enabled": http_hardening.API_DOCS_ENABLED,
        "trusted_proxies": len(http_hardening.TRUSTED_PROXIES),
        "playback_allow_list": (
            None if http_hardening.PLAYBACK_ALLOW_CIDRS is None else len(http_hardening.PLAYBACK_ALLOW_CIDRS)
        ),
        "ffmpeg": {
            "available": ffmpeg_proc.FFMPEG_AVAILABLE,
            "path": ffmpeg_proc.FFMPEG_PATH,
            "version": ffmpeg_proc.FFMPEG_VERSION_INFO,
            "multiview_path": getattr(ffmpeg_proc, "MULTIVIEW_FFMPEG_PATH", ""),
            "multiview_version": getattr(ffmpeg_proc, "MULTIVIEW_FFMPEG_VERSION_INFO", ""),
        },
        "jellyfin": jellyfin,
        "update_check": {"latest": _UPDATE_STATE.get("latest"), "checked": _UPDATE_STATE.get("checked_at")},
        "channel_count": sum(1 for d in stream_state.values() if d.get("type") != "multiview"),
        "multiview_count": sum(1 for d in stream_state.values() if d.get("type") == "multiview"),
    }
    sessions = {cid: _session_status(cid, snap) for cid, snap in SESSIONS.snapshot().items()}
    return {
        "summary.json": redact(summary),
        "settings.json": {
            "app_settings": redacted_settings(settings),
            "advanced": redact(_advanced_settings_snapshot()),
        },
        "providers.json": redact({"breakers": provider_breaker_snapshot(), "performance": performance}),
        "sessions.json": redact(sessions),
        "channels.json": redact([_channel_summary(cid, data) for cid, data in stream_state.items()]),
        "log-tail.txt": "\n".join(tail_log_lines()) + "\n",
    }


README_TEXT = """Jellyball diagnostics bundle

Made by the dashboard's "Download diagnostics" button for attaching to a bug
report. Secrets are removed before anything is written: API keys, webhook
URLs, bot tokens and the password hash are blanked; stream and other URLs
are cut to their host name; query strings, tokens and e-mail addresses are
masked; and the user name in home-directory paths is replaced.

Please still read the files before you post them publicly.

summary.json   version, OS, ffmpeg, Jellyfin, security settings in effect
settings.json  dashboard settings (secrets removed) and advanced settings
providers.json provider circuit breakers and recent success rates
sessions.json  what each running channel session is doing
channels.json  each channel's health and candidate counts (no stream URLs)
log-tail.txt   the end of jellyball.log, scrubbed
"""


def build_zip(contents: Dict[str, Any]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("README.txt", README_TEXT)
        for name, value in contents.items():
            text = value if isinstance(value, str) else json.dumps(value, indent=2, sort_keys=True, default=str)
            archive.writestr(name, text)
    return buffer.getvalue()


def bundle_filename() -> str:
    return f"jellyball-diagnostics-{time.strftime('%Y%m%d-%H%M%S')}.zip"
