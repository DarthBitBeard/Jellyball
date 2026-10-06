"""ESPN team schedules: the next game of one team, which sets its guide entry
and stream window.

Results are cached per (sport, team slug): successful lookups for
ESPN_SCHEDULE_CACHE_SECONDS (default 20 minutes), failed lookups briefly, so a
guide rebuild over many teams doesn't hammer ESPN's API. catalog.py
re-exports fetch_espn_team_schedule, so `from catalog import ...` keeps working.
"""

import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple

import httpx

from network_safety import bounded_float, safe_get

from config import _log_failure, LOGGER
from leagues import SCHEDULE_LEAGUES
import state


ESPN_SCHEDULE_CACHE_SECONDS = bounded_float(
    os.getenv("ESPN_SCHEDULE_CACHE_SECONDS", "1200"), 1200.0, 60.0, 7200.0
)
# Failed lookups are cached only briefly, so a transient ESPN outage doesn't
# pin every team to a stale "lookup failed" for the full cache window.
_ESPN_SCHEDULE_FAILURE_CACHE_SECONDS = 60.0
_ESPN_SCHEDULE_CACHE: Dict[Tuple[str, str], Tuple[float, tuple]] = {}


async def fetch_espn_team_schedule(
    team_name: str,
    query: str = "",
    category: str = "",
    source_id: str = "",
) -> tuple[Optional[datetime], Optional[datetime], bool]:
    """(start, stop, lookup_ok) of the team's earliest game that ended at most
    an hour ago and starts within 14 days; (None, None, True) when ESPN lists
    no such game, (None, None, False) when the lookup failed or the team or
    league is unknown. Successful results are cached per (sport, slug)."""
    # Late import: catalog imports this module, so it is reached at call time.
    from catalog import _resolve_espn_team

    matched_sport: Optional[str] = category
    matched_slug: Optional[str] = source_id
    if not matched_sport or not matched_slug:
        matched_sport, matched_slug, _ = _resolve_espn_team(team_name or query)
    if not matched_sport or not matched_slug:
        return None, None, False

    if matched_sport not in SCHEDULE_LEAGUES:
        return None, None, False

    cache_key = (matched_sport, matched_slug)
    now_mono = time.monotonic()
    cached = _ESPN_SCHEDULE_CACHE.get(cache_key)
    if cached is not None:
        expires_at, result = cached
        if now_mono < expires_at:
            return result
        _ESPN_SCHEDULE_CACHE.pop(cache_key, None)

    result = await _fetch_espn_team_schedule_uncached(
        team_name or query, matched_sport, matched_slug
    )
    ttl = ESPN_SCHEDULE_CACHE_SECONDS if result[2] else _ESPN_SCHEDULE_FAILURE_CACHE_SECONDS
    _ESPN_SCHEDULE_CACHE[cache_key] = (time.monotonic() + ttl, result)
    return result


async def _fetch_espn_team_schedule_uncached(
    team_name: str,
    matched_sport: str,
    matched_slug: str,
) -> tuple[Optional[datetime], Optional[datetime], bool]:
    """One live request to ESPN's team schedule API (no cache)."""

    schedule_league = SCHEDULE_LEAGUES[matched_sport]
    sport, league = schedule_league.espn_sport, schedule_league.espn_league
    url = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/teams/{matched_slug}/schedule"

    now_utc = datetime.now(timezone.utc)
    game_duration = schedule_league.game_duration
    upcoming = []
    owns_client = state.SHARED_HTTP_CLIENT is None
    client = state.SHARED_HTTP_CLIENT or httpx.AsyncClient(timeout=8.0, follow_redirects=False, http2=True)
    try:
        resp = await safe_get(client, url)
        if resp is None or resp.status_code != 200:
            LOGGER.warning("ESPN schedule request team=%s status=%s", team_name,
                           resp.status_code if resp is not None else "fetch-failed")
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
                dt_stop = dt_start + game_duration
                if dt_stop >= now_utc - timedelta(hours=1) and dt_start <= now_utc + timedelta(days=14):
                    upcoming.append((dt_start, dt_stop))
        if upcoming:
            start, stop = min(upcoming, key=lambda event: event[0])
            return start, stop, True
        return None, None, True
    except Exception as exc:
        _log_failure(f"fetch ESPN schedule team={team_name}", exc)
        return None, None, False
    finally:
        if owns_client:
            await client.aclose()
