"""Motorsport series schedules (Formula 1): every session of the coming race
weekends, from ESPN's racing scoreboard.

A series channel follows all sessions (practice, sprint, qualifying, race), so
instead of one team's next game this module keeps the list of sessions in the
next SCHEDULE_LOOKAHEAD_DAYS, one request per series:

  /apis/site/v2/sports/<sport>/<league>/scoreboard?dates=YYYYMMDD-YYYYMMDD

Each event in the response is a race weekend; each of its competitions is one
session whose type.abbreviation is the session code (FP1, SS, SR, Qual, Race).

fetch_series_schedule() returns the same (start, stop, lookup_ok) triple as
espn_schedule.fetch_espn_team_schedule, for the current or next session, so the
scrape loop's stream-window logic works unchanged. The season (first and last
round) comes from the scoreboard's calendar, fetched separately because a
dated request only returns the calendar entries inside its date range.
"""

import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

from config import _log_failure, LOGGER
from leagues import SERIES_BY_KEY, Series
from network_safety import bounded_float, safe_get
import state


ESPN_SITE_API = "https://site.api.espn.com/apis/site/v2/sports"

SERIES_SCHEDULE_CACHE_SECONDS = bounded_float(
    os.getenv("SERIES_SCHEDULE_CACHE_SECONDS", "1200"), 1200.0, 60.0, 7200.0
)
SERIES_SEASON_CACHE_SECONDS = 6 * 3600.0
# Failed lookups are cached briefly, as in espn_schedule, so an ESPN outage
# neither hammers the API nor pins a channel to "lookup failed" for long.
_FAILURE_CACHE_SECONDS = 60.0

# Sessions are requested from yesterday (a session that ran past midnight UTC
# is still in its trailing window) up to this many days ahead: past the guide
# horizon (epg.GUIDE_HORIZON_DAYS) and the 14 days team schedules look ahead.
SCHEDULE_LOOKBACK_DAYS = 1
SCHEDULE_LOOKAHEAD_DAYS = 14
# A session still counts as current this long after its assumed end, matching
# espn_schedule ("ended at most an hour ago").
CURRENT_SESSION_GRACE = timedelta(hours=1)
# The season opens this many days before the first round's listed start
# (the calendar gives the weekend, sessions can start the day before).
SEASON_OPEN_MARGIN = timedelta(days=7)


@dataclass(frozen=True)
class Session:
    """One session of a race weekend."""

    series: str
    event_id: str
    event_name: str  # ESPN's full name, sponsor included
    round_name: str  # display name of the weekend, e.g. "São Paulo GP"
    code: str  # ESPN session code, e.g. "Qual"
    label: str  # e.g. "Qualifying"
    start: datetime
    stop: datetime
    circuit: str = ""
    city: str = ""
    country: str = ""

    @property
    def title(self) -> str:
        return f"{self.round_name} - {self.label}"


# Grand Prix names as Formula 1 uses them ("<place> Grand Prix"). ESPN prefixes
# the title sponsor ("MSC Cruises São Paulo Grand Prix", "Qatar Airways Qatar
# Grand Prix"), so the display name is the known place right before "Grand
# Prix". An unknown place keeps ESPN's full name.
F1_GRAND_PRIX_PLACES: Tuple[str, ...] = (
    "70th Anniversary", "Abu Dhabi", "Australian", "Austrian", "Azerbaijan", "Bahrain",
    "Barcelona-Catalunya", "Belgian", "Brazilian", "British", "Canadian", "Chinese", "Dutch",
    "Eifel", "Emilia Romagna", "French", "German", "Hungarian", "Italian", "Japanese",
    "Las Vegas", "Madrid", "Mexican", "Mexico City", "Miami", "Monaco", "Portuguese", "Qatar",
    "Russian", "Sakhir", "São Paulo", "Sao Paulo", "Saudi Arabian", "Singapore", "Spanish",
    "Styrian", "Turkish", "Tuscan", "United States",
)
_PLACE_RE = re.compile(
    r"\b(?P<place>" + "|".join(re.escape(p) for p in sorted(F1_GRAND_PRIX_PLACES, key=len, reverse=True))
    + r")\s+Grand\s+Prix\b(?P<rest>.*)$",
    re.IGNORECASE,
)


def round_display_name(event_name: str) -> str:
    """"MSC Cruises São Paulo Grand Prix" -> "São Paulo GP";
    "Gulf Air Bahrain Grand Prix in Malaysia" -> "Bahrain GP in Malaysia"."""
    name = re.sub(r"\s+", " ", str(event_name or "")).strip()
    match = _PLACE_RE.search(name)
    if match:
        return f"{match.group('place')} GP{match.group('rest')}".strip()
    return re.sub(r"\bGrand\s+Prix\b", "GP", name, flags=re.IGNORECASE)


def _parse_espn_time(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    try:
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


_LOWERCASE_PARTICLES = {"de", "do", "da", "di", "del", "la", "le", "of", "and"}


def _place_label(value: object) -> str:
    """ESPN writes some places partly in lower case ("Sao paulo"): capitalise
    each lower-case word except connecting particles ("Rio de Janeiro")."""
    words = re.sub(r"\s+", " ", str(value or "")).strip().split(" ")
    return " ".join(
        word[:1].upper() + word[1:]
        if word[:1].islower() and (index == 0 or word not in _LOWERCASE_PARTICLES) else word
        for index, word in enumerate(words)
    )


def _as_dict(value: object) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def parse_series_sessions(series: Series, data: object) -> List[Session]:
    """Every session in an ESPN racing scoreboard response, sorted by start.
    Competitions without a usable date are skipped; a malformed response
    yields []."""
    if not isinstance(data, dict):
        return []
    sessions: List[Session] = []
    for event in data.get("events") or []:
        if not isinstance(event, dict):
            continue
        event_name = str(event.get("name") or "").strip()
        round_name = round_display_name(event_name) or series.name
        circuit = _as_dict(event.get("circuit"))
        address = _as_dict(circuit.get("address"))
        for comp in event.get("competitions") or []:
            if not isinstance(comp, dict):
                continue
            start = _parse_espn_time(comp.get("date") or comp.get("startDate"))
            if start is None:
                continue
            code = str(_as_dict(comp.get("type")).get("abbreviation") or "").strip()
            sessions.append(Session(
                series=series.key,
                event_id=str(event.get("id") or ""),
                event_name=event_name,
                round_name=round_name,
                code=code,
                label=series.session_label(code) if code else "Session",
                start=start,
                stop=start + series.session_duration(code),
                circuit=str(circuit.get("fullName") or "").strip(),
                city=_place_label(address.get("city")),
                country=_place_label(address.get("country")),
            ))
    sessions.sort(key=lambda session: (session.start, session.code))
    return sessions


def parse_series_season(data: object) -> Optional[Tuple[datetime, datetime]]:
    """(first round start, last round end) from a scoreboard's calendar."""
    if not isinstance(data, dict):
        return None
    leagues = data.get("leagues")
    if not isinstance(leagues, list) or not leagues or not isinstance(leagues[0], dict):
        return None
    starts, ends = [], []
    for entry in leagues[0].get("calendar") or []:
        if not isinstance(entry, dict):
            continue
        start = _parse_espn_time(entry.get("startDate"))
        end = _parse_espn_time(entry.get("endDate")) or start
        if start and end:
            starts.append(start)
            ends.append(end)
    if not starts:
        return None
    return min(starts), max(ends)


_SESSIONS_CACHE: Dict[str, Tuple[float, Tuple[Tuple[Session, ...], bool]]] = {}
_SEASON_CACHE: Dict[str, Tuple[float, Optional[Tuple[datetime, datetime]]]] = {}


async def _get_json(url: str, what: str) -> Optional[object]:
    owns_client = state.SHARED_HTTP_CLIENT is None
    client = state.SHARED_HTTP_CLIENT or httpx.AsyncClient(timeout=8.0, follow_redirects=False, http2=True)
    try:
        resp = await safe_get(client, url)
        if resp is None or resp.status_code != 200:
            LOGGER.warning("ESPN %s request status=%s", what,
                           resp.status_code if resp is not None else "fetch-failed")
            return None
        return resp.json()
    except Exception as exc:
        _log_failure(f"fetch ESPN {what}", exc)
        return None
    finally:
        if owns_client:
            await client.aclose()


async def fetch_series_sessions(series_key: str) -> Tuple[Tuple[Session, ...], bool]:
    """(sessions, lookup_ok): every session from yesterday to
    SCHEDULE_LOOKAHEAD_DAYS ahead, sorted by start. ((), False) when the
    lookup failed or the series is unknown. Cached per series."""
    series = SERIES_BY_KEY.get(series_key)
    if series is None:
        return (), False
    cached = _SESSIONS_CACHE.get(series.key)
    if cached is not None and time.monotonic() < cached[0]:
        return cached[1]

    today = datetime.now(timezone.utc).date()
    first = today - timedelta(days=SCHEDULE_LOOKBACK_DAYS)
    last = today + timedelta(days=SCHEDULE_LOOKAHEAD_DAYS)
    url = (f"{ESPN_SITE_API}/{series.espn_sport}/{series.espn_league}/scoreboard"
           f"?dates={first:%Y%m%d}-{last:%Y%m%d}")
    data = await _get_json(url, f"{series.key} schedule")
    if isinstance(data, dict) and isinstance(data.get("events", []), list):
        result: Tuple[Tuple[Session, ...], bool] = (tuple(parse_series_sessions(series, data)), True)
    else:
        result = ((), False)
    ttl = SERIES_SCHEDULE_CACHE_SECONDS if result[1] else _FAILURE_CACHE_SECONDS
    _SESSIONS_CACHE[series.key] = (time.monotonic() + ttl, result)
    return result


async def fetch_series_season(series_key: str) -> Optional[Tuple[datetime, datetime]]:
    """(first round start, last round end) of the season ESPN currently lists,
    or None when unknown. Cached per series; read back by series_in_season()."""
    series = SERIES_BY_KEY.get(series_key)
    if series is None:
        return None
    cached = _SEASON_CACHE.get(series.key)
    if cached is not None and time.monotonic() < cached[0]:
        return cached[1]
    url = f"{ESPN_SITE_API}/{series.espn_sport}/{series.espn_league}/scoreboard"
    season = parse_series_season(await _get_json(url, f"{series.key} season"))
    ttl = SERIES_SEASON_CACHE_SECONDS if season else _FAILURE_CACHE_SECONDS
    _SEASON_CACHE[series.key] = (time.monotonic() + ttl, season)
    return season


def _cached_season(series_key: str) -> Optional[Tuple[datetime, datetime]]:
    cached = _SEASON_CACHE.get(series_key)
    return cached[1] if cached else None


def series_in_season(series_key: str, today: date) -> bool:
    """Whether `today` falls inside the cached season (from a week before the
    first round to the day after the last). True while the season is unknown,
    so a failed or not-yet-made lookup never hides the channel."""
    season = _cached_season(series_key)
    if season is None:
        return True
    first, last = season
    return (first - SEASON_OPEN_MARGIN).date() <= today <= last.date() + timedelta(days=1)


def series_resume_label(series_key: str, today: date) -> str:
    """"Mar 6" when the cached season starts after `today`, else ""
    (the next season is not listed yet)."""
    season = _cached_season(series_key)
    if season is None or season[0].date() <= today:
        return ""
    return f"{season[0]:%b} {season[0].day}"


def current_or_next_session(sessions: Tuple[Session, ...], now: datetime) -> Optional[Session]:
    """The earliest session that ended at most CURRENT_SESSION_GRACE ago and
    starts within SCHEDULE_LOOKAHEAD_DAYS."""
    horizon = now + timedelta(days=SCHEDULE_LOOKAHEAD_DAYS)
    upcoming = [s for s in sessions if s.stop >= now - CURRENT_SESSION_GRACE and s.start <= horizon]
    return min(upcoming, key=lambda s: s.start) if upcoming else None


async def fetch_series_schedule(series_key: str) -> Tuple[Optional[datetime], Optional[datetime], bool]:
    """(start, stop, lookup_ok) of the series' current or next session, with
    the contract of espn_schedule.fetch_espn_team_schedule: (None, None, True)
    when ESPN lists no session in the window, (None, None, False) when the
    lookup failed or the series is unknown."""
    sessions, ok = await fetch_series_sessions(series_key)
    if not ok:
        return None, None, False
    session = current_or_next_session(sessions, datetime.now(timezone.utc))
    if session is None:
        return None, None, True
    return session.start, session.stop, True


def is_series_category(category: str) -> bool:
    return category in SERIES_BY_KEY
