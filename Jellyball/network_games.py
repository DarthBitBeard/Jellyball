"""Network channels follow the game.

A 24/7 channel such as ABC, CBS, NBC or FOX is only worth watching in a sports
app while it carries a game, and there is no free national feed of those
networks: the affiliate web streams iptv-org lists carry the stations' own
local news, not network football. So while ESPN's scoreboard says a game is on
a network (in progress, or starting within GAME_LEAD_MINUTES), that network's
channel searches the event providers for the game itself, exactly as a team
channel does. Cable sports networks (ESPN, FS1, ...) do the same while they
carry a game and keep their own linear stream otherwise.

Over-the-air networks (BROADCAST_NETWORKS) without a game show No Signal
instead of a random local station, unless NETWORK_AFFILIATE_FALLBACK=1.

When several games are on one network at once (regional NFL on CBS and FOX),
the one involving a team the user has a channel for wins, then the one that
started first.
"""

import asyncio
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import httpx

from config import _log_failure, LOGGER
from leagues import LEAGUES, OPTIONAL_LEAGUES
from network_safety import bounded_float

# Special-channel key -> the names ESPN's scoreboard uses for that network.
NETWORK_BROADCAST_NAMES: Dict[str, Tuple[str, ...]] = {
    "abc": ("ABC",), "cbs": ("CBS",), "nbc": ("NBC",), "fox": ("FOX",), "the_cw": ("CW", "The CW"),
    "espn": ("ESPN",), "espn2": ("ESPN2",), "espnu": ("ESPNU",), "fs1": ("FS1",), "fs2": ("FS2",),
    "cbs_sports_network": ("CBSSN", "CBS Sports Network"), "tnt_sports": ("TNT", "TNT Sports"),
    "nbc_sports": ("NBCSN", "NBC Sports"), "big_ten_network": ("BTN", "Big Ten Network"),
    "acc_network": ("ACCN", "ACC Network"), "sec_network": ("SECN", "SEC Network"),
    "longhorn_network": ("LHN", "Longhorn Network"), "mlb_network": ("MLB Net", "MLB Network", "MLBN"),
    "nba_tv": ("NBA TV",), "nfl_network": ("NFL Net", "NFL Network"), "nhl_network": ("NHL Net", "NHL Network"),
    "tbs": ("TBS",), "usa_network": ("USA Net", "USA Network"), "trutv": ("truTV", "TRUTV"),
}
BROADCAST_NETWORKS = frozenset({"abc", "cbs", "nbc", "fox", "the_cw"})

GAME_LEAD_MINUTES = bounded_float(os.getenv("NETWORK_GAME_LEAD_MINUTES", "20"), 20.0, 0.0, 180.0)
SCOREBOARD_REFRESH_SECONDS = bounded_float(os.getenv("NETWORK_SCOREBOARD_REFRESH_SECONDS", "120"), 120.0, 30.0, 3600.0)
AFFILIATE_FALLBACK = os.getenv("NETWORK_AFFILIATE_FALLBACK", "0").strip().lower() in ("1", "true", "yes", "on")

# ESPN lists only featured games by default for college sports; these groups
# return every Division I game.
_SCOREBOARD_PARAMS = {
    ("football", "college-football"): {"groups": "80", "limit": "300"},
    ("basketball", "mens-college-basketball"): {"groups": "50", "limit": "300"},
}


@dataclass(frozen=True)
class Game:
    event_id: str
    league: str
    name: str  # "Alabama Crimson Tide at LSU Tigers"
    short_name: str  # "ALA @ LSU"
    home: str
    away: str
    start: datetime
    state: str  # "pre" | "in" | "post"
    networks: Tuple[str, ...] = field(default_factory=tuple)

    @property
    def search_query(self) -> str:
        return f"{self.away} vs {self.home}"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id, "league": self.league, "name": self.name, "short_name": self.short_name,
            "home": self.home, "away": self.away, "start": self.start.isoformat(), "state": self.state,
            "networks": list(self.networks),
        }


_GAMES: List[Game] = []
_LOADED_AT = 0.0
_LOCK = asyncio.Lock()


def network_key(data: Dict[str, Any]) -> Optional[str]:
    """The network a channel is ("special:abc" -> "abc"), if it is one we follow."""
    catalog_key = str(data.get("catalog_key") or "")
    key = catalog_key.removeprefix("special:") if catalog_key.startswith("special:") else ""
    return key if key in NETWORK_BROADCAST_NAMES else None


def is_broadcast_network(data: Dict[str, Any]) -> bool:
    return network_key(data) in BROADCAST_NETWORKS


def parse_scoreboard(payload: Dict[str, Any], league: str) -> List[Game]:
    games: List[Game] = []
    for event in payload.get("events") or []:
        try:
            competition = (event.get("competitions") or [{}])[0]
            teams = {c.get("homeAway"): c.get("team") or {} for c in competition.get("competitors") or []}
            home, away = teams.get("home") or {}, teams.get("away") or {}
            names = []
            for broadcast in competition.get("broadcasts") or []:
                names.extend(str(n) for n in broadcast.get("names") or [])
            for broadcast in competition.get("geoBroadcasts") or []:
                media = (broadcast.get("media") or {}).get("shortName")
                if media:
                    names.append(str(media))
            start = datetime.fromisoformat(str(event.get("date") or competition.get("date")).replace("Z", "+00:00"))
            games.append(Game(
                event_id=str(event.get("id") or ""),
                league=league,
                name=str(event.get("name") or ""),
                short_name=str(event.get("shortName") or ""),
                home=str(home.get("displayName") or home.get("name") or ""),
                away=str(away.get("displayName") or away.get("name") or ""),
                start=start if start.tzinfo else start.replace(tzinfo=timezone.utc),
                state=str(((event.get("status") or {}).get("type") or {}).get("state") or ""),
                networks=tuple(dict.fromkeys(names)),
            ))
        except (TypeError, ValueError, AttributeError):
            continue
    return [game for game in games if game.home and game.away and game.event_id]


def _scoreboard_leagues() -> Iterable[Tuple[str, str, str]]:
    seen = set()
    for league in (*LEAGUES, *OPTIONAL_LEAGUES):
        if not getattr(league, "has_schedule", True):
            continue
        path = (league.espn_sport, league.espn_league)
        if path in seen:
            continue
        seen.add(path)
        yield league.key, league.espn_sport, league.espn_league


async def refresh(client: Optional[httpx.AsyncClient] = None, force: bool = False) -> List[Game]:
    """Today's games on every league's ESPN scoreboard (cached for SCOREBOARD_REFRESH_SECONDS)."""
    global _GAMES, _LOADED_AT
    if not force and _GAMES and time.monotonic() - _LOADED_AT < SCOREBOARD_REFRESH_SECONDS:
        return _GAMES
    async with _LOCK:
        if not force and _GAMES and time.monotonic() - _LOADED_AT < SCOREBOARD_REFRESH_SECONDS:
            return _GAMES
        import state

        owns = client is None and state.SHARED_HTTP_CLIENT is None
        http = client or state.SHARED_HTTP_CLIENT or httpx.AsyncClient(timeout=15.0)
        try:
            async def one(key: str, sport: str, league: str) -> List[Game]:
                url = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/scoreboard"
                try:
                    response = await http.get(url, params=_SCOREBOARD_PARAMS.get((sport, league), {}), timeout=15.0)
                    if response.status_code != 200:
                        return []
                    return parse_scoreboard(response.json(), key)
                except Exception as exc:  # noqa: BLE001 - one league failing must not hide the others
                    _log_failure(f"fetch ESPN scoreboard {sport}/{league}", exc)
                    return []

            results = await asyncio.gather(*(one(*item) for item in _scoreboard_leagues()))
        finally:
            if owns:
                await http.aclose()
        games = [game for batch in results for game in batch]
        if games or not _GAMES:
            _GAMES, _LOADED_AT = games, time.monotonic()
        return _GAMES


def _on_network(game: Game, key: str) -> bool:
    wanted = {name.casefold() for name in NETWORK_BROADCAST_NAMES.get(key, ())}
    return any(name.casefold() in wanted for name in game.networks)


def game_for_network(
    key: str, games: Iterable[Game], now: Optional[datetime] = None, preferred_teams: Iterable[str] = (),
) -> Optional[Game]:
    """The game the network channel should show now, or None."""
    now = now or datetime.now(timezone.utc)
    lead = timedelta(minutes=GAME_LEAD_MINUTES)
    preferred = {team.casefold() for team in preferred_teams if team}
    live = [g for g in games if _on_network(g, key) and (g.state == "in" or (g.state == "pre" and g.start - lead <= now))]
    if not live:
        return None

    def rank(game: Game) -> tuple:
        involves_mine = game.home.casefold() in preferred or game.away.casefold() in preferred
        return (game.state != "in", not involves_mine, game.start, game.event_id)

    return sorted(live, key=rank)[0]


def preferred_team_names(stream_state: Dict[str, Dict[str, Any]]) -> List[str]:
    """Teams the user has channels for (their display names without the sport suffix)."""
    names = []
    for data in stream_state.values():
        if data.get("always_live") or data.get("type") == "multiview":
            continue
        name = str(data.get("name") or "")
        names.append(name.split(" (", 1)[0])
    return names


def current_game(data: Dict[str, Any], stream_state: Dict[str, Dict[str, Any]]) -> Optional[Game]:
    key = network_key(data)
    if key is None:
        return None
    return game_for_network(key, _GAMES, preferred_teams=preferred_team_names(stream_state))


async def watch_loop() -> None:
    """Refresh the scoreboards and re-search a network channel the moment the
    game it should show changes (kick-off, final whistle, next game)."""
    from failover import trigger_scrape
    from state import _spawn_background_task, stream_state

    while True:
        try:
            await refresh(force=True)
            for team_id, data in list(stream_state.items()):
                if network_key(data) is None:
                    continue
                game = current_game(data, stream_state)
                game_id = game.event_id if game else ""
                if game_id != str(data.get("network_game_id") or ""):
                    LOGGER.info(
                        "Network channel %s now follows %s",
                        team_id, f"{game.short_name} ({game.league})" if game else "no game",
                    )
                    _spawn_background_task(trigger_scrape(team_id, force=True), f"network game {team_id}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - keep watching
            _log_failure("network game watch", exc)
        await asyncio.sleep(SCOREBOARD_REFRESH_SECONDS)
