"""Advanced Settings registry: runtime-tunable settings, their defaults,
and live application.

Each Tunable.target is a module global owned by the module that reads it
(failover, scrapers, sessions, ...). _apply_tunable rebinds it on that owning
module (see _tunable_module), so the change is seen by the code that uses it.
"""

import math
from dataclasses import dataclass
from typing import Dict, List, Tuple

from db import get_setting
import scrapers
import legacy_proxy
import sessions
from sessions import SESSIONS
import multiview
import failover


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
# A non-session Tunable.target is a global of the module whose code reads it.
# It must be rebound on that module - a `from x import NAME` copy elsewhere
# would not see the change - so these are the modules that own the targets.
_TUNABLE_TARGET_MODULES = (scrapers, legacy_proxy, sessions, multiview, failover)


def _tunable_module(tunable: Tunable):
    """The module that owns `tunable.target` (a plain module global)."""
    for module in _TUNABLE_TARGET_MODULES:
        if hasattr(module, tunable.target):
            return module
    raise LookupError(f"no module owns tunable target {tunable.target}")


def _tunable_value(tunable: Tunable):
    if tunable.target.startswith("session."):
        return getattr(SESSIONS.config, tunable.target.split(".", 1)[1])
    return getattr(_tunable_module(tunable), tunable.target)


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
        setattr(_tunable_module(tunable), tunable.target, value)


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


def _advanced_settings_html() -> List[dict]:
    """Advanced-settings tunables grouped for the dashboard template, one
    dict per fieldset: {"name": group name, "items": [...]}."""
    groups: Dict[str, List[dict]] = {}
    for item in _advanced_settings_snapshot():
        overridden = item["value"] != item["default"]
        step = "1" if item["type"] == "int" else "any"
        hint = f"Default {item['default']:g}" if isinstance(item["default"], (int, float)) else ""
        if item["help"]:
            hint = f"{hint}. {item['help']}" if hint else item["help"]
        groups.setdefault(item["group"], []).append({
            "name": item["name"],
            "label": item["label"],
            "overridden": overridden,
            "step": step,
            "min": f'{item["min"]:g}',
            "max": f'{item["max"]:g}',
            "value": item["value"] if overridden else "",
            "default": item["default"],
            "hint": hint,
        })
    return [{"name": group, "fields": fields} for group, fields in groups.items()]
