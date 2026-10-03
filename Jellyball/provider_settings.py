"""Per-provider enable/disable and priority (R5).

ACTIVE_PROVIDERS stays the literal list in scrapers.py; this layer decides, at
search time, which of them run and how they rank against each other. With no
rows in provider_settings every provider is enabled and the priority is the
STREAM_PROVIDER_PRIORITY env order, exactly as before.

State lives in a module-level dict loaded once at startup and refreshed on save,
so a search never reads the database on the event loop.
"""

from typing import Dict, Iterable, List, Optional

from config import _log_failure
from db import _db_session

_SETTINGS: Dict[str, dict] = {}


def load() -> None:
    """Refresh the in-memory settings from the database (idempotent)."""
    loaded: Dict[str, dict] = {}
    try:
        with _db_session() as conn:
            for name, enabled, priority in conn.execute(
                "SELECT name, enabled, priority FROM provider_settings"
            ).fetchall():
                loaded[name] = {"enabled": bool(enabled), "priority": priority}
    except Exception as exc:  # noqa: BLE001 - a missing table must not stop startup
        _log_failure("load provider settings", exc)
    _SETTINGS.clear()
    _SETTINGS.update(loaded)


def is_enabled(name: str) -> bool:
    return _SETTINGS.get(name, {}).get("enabled", True)


def priority_override(name: str) -> Optional[int]:
    value = _SETTINGS.get(name, {}).get("priority")
    return None if value is None else int(value)


def filter_enabled(providers: Iterable) -> list:
    return [provider for provider in providers if is_enabled(provider.name)]


def apply_priority(base: Dict[str, int]) -> Dict[str, int]:
    """The env-derived priority map with any dashboard overrides applied."""
    if not any(entry.get("priority") is not None for entry in _SETTINGS.values()):
        return base
    merged = dict(base)
    for name, entry in _SETTINGS.items():
        if entry.get("priority") is not None:
            merged[name] = int(entry["priority"])
    return merged


def set_provider_sync(name: str, enabled: bool, priority: Optional[int]) -> None:
    with _db_session() as conn:
        conn.execute(
            "INSERT INTO provider_settings (name, enabled, priority) VALUES (?, ?, ?) "
            "ON CONFLICT(name) DO UPDATE SET enabled=excluded.enabled, priority=excluded.priority",
            (name, 1 if enabled else 0, priority),
        )
        conn.commit()
    _SETTINGS[name] = {"enabled": bool(enabled), "priority": priority}


def snapshot(names: List[str]) -> List[dict]:
    return [
        {"name": name, "enabled": is_enabled(name), "priority": priority_override(name)}
        for name in names
    ]
