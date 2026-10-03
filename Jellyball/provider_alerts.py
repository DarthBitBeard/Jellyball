"""'Provider silent' alerts (R3).

A provider can go dark without ever raising: the site changes and its index page
lists nothing, or the circuit breaker keeps tripping. After each provider search
check_soon() looks (at most once per CHECK_INTERVAL per provider) at three
signals and sends one alert per provider and kind per ALERT_COOLDOWN through the
normal send_alert channels:

  - breaker: the circuit breaker is open
  - silent: the index page listed no events in every recent run
  - dead: zero successes across many attempts over five days (the existing
    provider_health sustained_failure signal)

It never imports scrapers (scrapers calls it); the caller passes the breaker state.
"""

import asyncio
import time
from typing import Dict, List, Tuple

from config import _log_failure
import provider_telemetry

CHECK_INTERVAL = 600.0
ALERT_COOLDOWN = 6 * 3600.0

_last_check: Dict[str, float] = {}
_last_alert: Dict[Tuple[str, str], float] = {}


def reset_for_tests() -> None:
    _last_check.clear()
    _last_alert.clear()


def _due(provider: str, kind: str, now: float) -> bool:
    last = _last_alert.get((provider, kind))
    return last is None or now - last >= ALERT_COOLDOWN


def problems_sync(provider: str, breaker_open: bool) -> List[Tuple[str, str]]:
    """(kind, message) for each current problem with `provider`."""
    problems: List[Tuple[str, str]] = []
    if breaker_open:
        problems.append(("breaker", "its circuit breaker is open after repeated failures; searches are paused for a while"))
    if any(item["provider"] == provider for item in provider_telemetry.silent_providers_sync()):
        problems.append(("silent", "its index page has listed no events in any recent search, so the site may have changed or gone"))
    from db import _db_session
    try:
        with _db_session() as conn:
            row = conn.execute(
                "SELECT COUNT(*), COALESCE(SUM(success), 0) FROM provider_performance "
                "WHERE provider=? AND timestamp > datetime('now', '-5 days')",
                (provider,),
            ).fetchone()
        if row and row[0] >= 10 and row[1] == 0:
            problems.append(("dead", f"none of its last {row[0]} searches (5 days) succeeded"))
    except Exception as exc:  # noqa: BLE001
        _log_failure("read provider health for alerts", exc)
    return problems


async def check_and_alert(provider: str, breaker_open: bool, now: float = 0.0) -> List[str]:
    """Send any due alert for `provider`; returns the kinds sent."""
    from alerts import send_alert  # late: alerts pulls in the catalog

    now = now or time.monotonic()
    sent: List[str] = []
    for kind, message in await asyncio.to_thread(problems_sync, provider, breaker_open):
        if not _due(provider, kind, now):
            continue
        _last_alert[(provider, kind)] = now
        await send_alert("Provider silent", f"**{provider}**: {message}.", "warning")
        sent.append(kind)
    return sent


def check_soon(provider: str, breaker_open: bool) -> None:
    """Fire-and-forget, rate-limited health check; call from the search path."""
    now = time.monotonic()
    last = _last_check.get(provider)
    if last is not None and now - last < CHECK_INTERVAL:
        return
    _last_check[provider] = now
    from state import _spawn_background_task

    _spawn_background_task(check_and_alert(provider, breaker_open, now), f"provider alert check {provider}")
