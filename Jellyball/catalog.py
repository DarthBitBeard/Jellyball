"""Channel catalog: the ESPN team directory, special channels, logos,
season windows and per-channel listing/metadata helpers. Team schedules are
fetched by espn_schedule.py.
"""

import asyncio
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import httpx

from config import _log_failure, _safe_team_id, LOGGER
import state
from leagues import CATALOG_CATEGORIES, COLLEGE_CATEGORIES, LEAGUES
from network_safety import bounded_float, safe_get
from sports_catalog import (
    ESPN_DIRECTORY_ENDPOINTS,
    parse_espn_team_directory,
    SEASON_WINDOWS,
    SPECIAL_CHANNELS,
    STATIC_TEAM_RECORDS,
    TeamSlug,
)
from sports_matcher import canonical_team_name
# Defined in espn_schedule.py; re-exported here for `from catalog import ...`.
from espn_schedule import fetch_espn_team_schedule


CATALOG_REFRESH_SECONDS = bounded_float(os.getenv("CATALOG_REFRESH_SECONDS", "3600"), 3600.0, 60.0, 86400.0)
_CATALOG_CACHE: Dict[str, Tuple[TeamSlug, ...]] = {}
_CATALOG_CACHE_LOADED_AT = 0.0
_CATALOG_REMOTE_LOADED = False
_CATALOG_LOCK = asyncio.Lock()
CATALOG_FAILURE_RETRY_SECONDS = bounded_float(os.getenv("CATALOG_FAILURE_RETRY_SECONDS", "120"), 120.0, 30.0, 1800.0)


def _static_catalog_records(category: str) -> List[TeamSlug]:
    records: List[TeamSlug] = []
    for record in STATIC_TEAM_RECORDS:
        if category in COLLEGE_CATEGORIES:
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
        response = await safe_get(client, url, params={"limit": "1000"})
        if response is None or response.status_code != 200:
            LOGGER.warning("ESPN catalog request category=%s status=%s", category,
                           response.status_code if response is not None else "fetch-failed")
            return ()
        records = parse_espn_team_directory(response.json(), category)
        return tuple(record.for_category(category) for record in records)
    except Exception as exc:
        _log_failure(f"fetch ESPN team directory category={category}", exc)
        return ()


def _persist_team_catalog_sync(grouped: Dict[str, Tuple[TeamSlug, ...]]) -> None:
    """Replace the persisted catalog with the last good merged one (best-effort;
    failures are logged by the caller, never raised into the refresh path)."""
    import json

    from db import _db_session  # late: keep catalog importable without db

    rows = [
        (category, record.slug, record.canonical, record.logo_sport,
         json.dumps(list(record.aliases)), record.team_id)
        for category, records in grouped.items()
        for record in records
    ]
    with _db_session() as conn:
        conn.execute("DELETE FROM team_catalog_cache")
        conn.executemany(
            "INSERT INTO team_catalog_cache "
            "(category, slug, canonical, logo_sport, aliases, team_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()


def _load_persisted_team_catalog_sync() -> Dict[str, Tuple[TeamSlug, ...]]:
    """The last good merged catalog from a previous run, or {} when none was
    ever persisted (e.g. a fresh install or a pre-2.2.0 database)."""
    import json

    from db import _db_session  # late: keep catalog importable without db

    try:
        with _db_session() as conn:
            rows = conn.execute(
                "SELECT category, slug, canonical, logo_sport, aliases, team_id "
                "FROM team_catalog_cache"
            ).fetchall()
    except Exception as exc:  # noqa: BLE001 - a missing table must not break startup
        _log_failure("load persisted team catalog", exc)
        return {}
    grouped: Dict[str, List[TeamSlug]] = {}
    for category, slug, canonical, logo_sport, aliases_json, team_id in rows:
        try:
            aliases = tuple(json.loads(aliases_json or "[]"))
        except Exception:
            aliases = ()
        grouped.setdefault(category, []).append(
            TeamSlug(
                canonical=canonical,
                category=category,
                slug=slug,
                logo_sport=logo_sport or "",
                aliases=aliases,
                team_id=team_id or "",
            )
        )
    return {
        category: tuple(sorted(records, key=lambda record: record.display_name.casefold()))
        for category, records in grouped.items()
    }


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

        categories = CATALOG_CATEGORIES
        grouped: Dict[str, Dict[Tuple[str, str, str], TeamSlug]] = {
            category: {
                _catalog_record_key(record): record
                for record in _static_catalog_records(category)
            }
            for category in categories
        }
        previous_catalog = dict(_CATALOG_CACHE)
        was_remote_loaded = _CATALOG_REMOTE_LOADED

        owns_client = state.SHARED_HTTP_CLIENT is None
        client = state.SHARED_HTTP_CLIENT or httpx.AsyncClient(
            timeout=10.0,
            follow_redirects=False,
            http2=True,
        )
        try:
            remote_results = await asyncio.gather(
                *(_fetch_espn_directory(category, client) for category in COLLEGE_CATEGORIES),
                return_exceptions=True,
            )
            remote_success = False
            for category, result in zip(COLLEGE_CATEGORIES, remote_results):
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
            # Persist the last good merged catalog off the event loop, but only
            # when every college directory came back: a partial merge must not
            # overwrite a previously good full one.
            if all(
                isinstance(result, tuple) and result
                for result in remote_results
            ):
                try:
                    await asyncio.to_thread(_persist_team_catalog_sync, _CATALOG_CACHE)
                except Exception as exc:
                    _log_failure("persist team catalog", exc)
        elif not previous_catalog:
            # No usable in-memory catalog (e.g. a restart during an ESPN
            # outage): fall back to the last good merged catalog from the DB
            # instead of dropping college coverage to the static list.
            persisted = await asyncio.to_thread(_load_persisted_team_catalog_sync)
            if persisted:
                _CATALOG_CACHE = persisted
                _CATALOG_CACHE_LOADED_AT = time.monotonic()
            else:
                _CATALOG_CACHE_LOADED_AT = time.monotonic() - max(
                    0.0,
                    CATALOG_REFRESH_SECONDS - CATALOG_FAILURE_RETRY_SECONDS,
                )
        return dict(_CATALOG_CACHE)


def _catalog_team_id(category: str, source_id: str, name: str) -> str:
    return _safe_team_id(f"{category}_{source_id or name}")


_COLLEGE_CATEGORY_LABELS = {league.key: league.sport_label for league in LEAGUES if league.sport_label}


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
_CATEGORY_GROUP_LABELS = {league.key: league.group_title for league in LEAGUES}


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
    for category in CATALOG_CATEGORIES:
        entries.extend(_team_catalog_entry(category, record) for record in grouped.get(category, ()))
    entries.extend(_special_catalog_entry(channel) for channel in SPECIAL_CHANNELS)
    return entries


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


_CATEGORY_LOGO_SPORTS = {league.key: league.logo_sport for league in LEAGUES}


def resolve_espn_logo(team_name: str, category: str = "", source_id: str = "") -> str:
    if category in _CATEGORY_LOGO_SPORTS and source_id:
        return f"{_ESPN_LOGO_CDN}/{_CATEGORY_LOGO_SPORTS[category]}/500/{source_id}.png?v=titan2"
    sport, slug, _ = _resolve_espn_team(team_name)
    if sport and slug:
        return f"{_ESPN_LOGO_CDN}/{sport}/500/{slug}.png?v=titan2"
    return ""


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
