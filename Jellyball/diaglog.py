"""Troubleshooting log lines: what a tune-in did, why a channel showed No Signal
or could not play at all, which playback requests arrived from Jellyfin, and
a one-block startup report of the environment.

Everything here only logs; nothing changes behaviour. Lines are written as
`key=value` so they are easy to grep. Secrets never reach the file: the log
filter in config redacts them, and URLs are reduced to their host here.
JELLYBALL_LOG_LEVEL=DEBUG adds one line per media segment request.
"""

import os
import platform
import sys
import time
import urllib.parse
from typing import Any, Dict, Optional, Tuple

import dns_override
from config import DATA_DIR, LOG_LEVEL_NAME, LOGGER, PORT
from state import stream_state
from version import __version__

# A playlist request from the same client for the same channel is logged at
# most this often at INFO (Jellyfin re-reads a live playlist every few seconds).
PLAYLIST_LOG_INTERVAL_SECONDS = 60.0
SLOW_SEGMENT_MS = 3000.0
_LAST_PLAYLIST_LOG: Dict[Tuple[str, str], float] = {}
_PLAYBACK_PREFIXES = ("/stream/", "/multiview/")
_GUIDE_PATHS = ("/playlist.m3u", "/epg.xml")


def _host(url: str) -> str:
    try:
        return urllib.parse.urlsplit(str(url or "")).netloc or "-"
    except ValueError:
        return "-"


def _client(scope: dict) -> str:
    client = scope.get("client")
    return str(client[0]) if client else "-"


def _user_agent(scope: dict) -> str:
    for name, value in scope.get("headers") or ():
        if name == b"user-agent":
            return value.decode("latin-1", "replace")[:60]
    return "-"


def _candidates(channel_id: str) -> Tuple[int, str]:
    data = stream_state.get(channel_id) or {}
    candidates = data.get("candidates") or []
    providers = sorted({str(c.get("provider", "?")) for c in candidates if isinstance(c, dict)})
    return len(candidates), ",".join(providers) or "-"


# -- requests -----------------------------------------------------------------

def log_request(scope: dict, status: int, elapsed_ms: float) -> None:
    """Called for every HTTP response start; logs the ones that matter for playback."""
    path = str(scope.get("path") or "")
    method = scope.get("method", "")
    if path in _GUIDE_PATHS:
        LOGGER.info("Guide request path=%s method=%s status=%d duration_ms=%.0f client=%s ua=%s",
                    path, method, status, elapsed_ms, _client(scope), _user_agent(scope))
        return
    if not path.startswith(_PLAYBACK_PREFIXES):
        return
    is_segment = path.endswith(".ts")
    if is_segment:
        if status >= 400:
            LOGGER.warning("Segment request failed path=%s status=%d duration_ms=%.0f client=%s",
                           path, status, elapsed_ms, _client(scope))
        elif elapsed_ms >= SLOW_SEGMENT_MS:
            LOGGER.warning("Slow segment request path=%s duration_ms=%.0f client=%s", path, elapsed_ms, _client(scope))
        else:
            LOGGER.debug("Segment request path=%s status=%d duration_ms=%.0f", path, status, elapsed_ms)
        return
    key = (path, _client(scope))
    now = time.monotonic()
    if status >= 400:
        LOGGER.warning("Playback request failed path=%s method=%s status=%d duration_ms=%.0f client=%s ua=%s",
                       path, method, status, elapsed_ms, _client(scope), _user_agent(scope))
    elif now - _LAST_PLAYLIST_LOG.get(key, 0.0) >= PLAYLIST_LOG_INTERVAL_SECONDS:
        LOGGER.info("Playback request path=%s method=%s status=%d duration_ms=%.0f client=%s ua=%s",
                    path, method, status, elapsed_ms, _client(scope), _user_agent(scope))
        _LAST_PLAYLIST_LOG[key] = now
        if len(_LAST_PLAYLIST_LOG) > 2000:
            _LAST_PLAYLIST_LOG.clear()
    else:
        LOGGER.debug("Playback request path=%s status=%d duration_ms=%.0f", path, status, elapsed_ms)


# -- tune-in --------------------------------------------------------------------

def log_tune_in(channel_id: str) -> None:
    data = stream_state.get(channel_id) or {}
    count, providers = _candidates(channel_id)
    LOGGER.info(
        "Tune-in channel=%s name=%r candidates=%d providers=%s healthy=%s schedule=%s scrape=%s",
        channel_id, data.get("name", ""), count, providers, bool(data.get("is_healthy")),
        data.get("schedule_status", "-"), data.get("scrape_result", "-"),
    )
    if count == 0 and data.get("type") != "multiview":
        LOGGER.warning(
            "Tune-in with no stream candidates channel=%s: no provider has a playable stream for it right now "
            "(see the 'Provider returned' and 'Stream candidate rejected' lines for this channel)",
            channel_id,
        )


def log_cold_race(channel_id: str, won: bool, elapsed: float) -> None:
    if won:
        LOGGER.info("Tune-in race won channel=%s elapsed_s=%.1f", channel_id, elapsed)
    else:
        count, providers = _candidates(channel_id)
        LOGGER.warning("Tune-in race lost channel=%s elapsed_s=%.1f candidates=%d providers=%s",
                       channel_id, elapsed, count, providers)


def log_placeholder(channel_id: str, reason: str, started: bool) -> None:
    import ffmpeg_proc  # late: ffmpeg_proc imports state, keep this module light

    if started:
        LOGGER.warning("No Signal shown channel=%s reason=%s", channel_id, reason)
    else:
        LOGGER.warning(
            "No Signal could not be shown channel=%s reason=%s ffmpeg_available=%s ffmpeg=%s",
            channel_id, reason, ffmpeg_proc.FFMPEG_AVAILABLE, ffmpeg_proc.FFMPEG_PATH,
        )


def log_unplayable(channel_id: str, session: Any) -> None:
    count, providers = _candidates(channel_id)
    LOGGER.error(
        "Channel unplayable, answering 503 channel=%s state=%s source=%s candidates=%d providers=%s",
        channel_id, getattr(session, "state", "?"), _session_source(session), count, providers,
    )


def log_tune_in_ready(channel_id: str, session: Any, elapsed: float) -> None:
    LOGGER.info("Tune-in ready channel=%s elapsed_s=%.1f state=%s source=%s",
                channel_id, elapsed, getattr(session, "state", "?"), _session_source(session))


def _session_source(session: Any) -> str:
    source = getattr(session, "source", None)
    if source is None:
        return "-"
    label = getattr(source, "label", "") or "?"
    key = getattr(source, "key", None)
    host = key[1] if isinstance(key, (list, tuple)) and len(key) > 1 else ""
    return f"{label}@{host}" if host else str(label)


# -- startup --------------------------------------------------------------------

def startup_report(extra: Optional[Dict[str, Any]] = None) -> None:
    """One block describing the environment, logged once the app has loaded its channels."""
    import ffmpeg_proc
    import scrapers

    channels = list(stream_state.values())
    multiview = sum(1 for c in channels if c.get("type") == "multiview")
    always_live = sum(1 for c in channels if c.get("always_live"))
    browser = scrapers.SHARED_BROWSER
    lines = [
        f"version={__version__} python={platform.python_version()} os={platform.platform()} "
        f"frozen={bool(getattr(sys, 'frozen', False))} pid={os.getpid()}",
        f"data_dir={DATA_DIR} log_level={LOG_LEVEL_NAME} port={PORT}",
        f"dns={dns_override.describe()}",
        f"ffmpeg={ffmpeg_proc.FFMPEG_PATH} available={ffmpeg_proc.FFMPEG_AVAILABLE} "
        f"version={(ffmpeg_proc.FFMPEG_VERSION_INFO or '-')[:60]}",
        f"multiview_ffmpeg={ffmpeg_proc.MULTIVIEW_FFMPEG_PATH} hwaccel={os.getenv('MULTIVIEW_HWACCEL', 'auto') or 'auto'}",
        f"chromium={'running ' + browser.version if browser is not None and browser.is_connected() else 'NOT running'} "
        f"browsers_path={os.environ.get('PLAYWRIGHT_BROWSERS_PATH', '(Playwright default)')}",
        f"channels={len(channels)} always_live={always_live} multiview={multiview} "
        f"teams={len(channels) - always_live - multiview}",
    ]
    for key, value in (extra or {}).items():
        lines.append(f"{key}={value}")
    LOGGER.info("Startup report:\n    " + "\n    ".join(lines))
