"""League registry: the one place that describes each sports league.

Catalog order, guide group titles, ESPN API and logo paths, game lengths,
season windows and college handling are read from here instead of being
repeated as literals across the catalog, the ESPN lookups and the dashboard.
test_leagues.py freezes every structure derived from it.

A league key is stored in the database (teams.category) and is part of channel
ids and catalog keys ("team:nfl:buf"), so an existing key must never change.
"""

from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, Optional, Tuple

# (start_month, start_day, end_month, end_day): preseason through the championship.
SeasonWindow = Tuple[int, int, int, int]

# sports_catalog.TeamSlug.category of the college team records, which football
# and men's basketball share (ESPN uses one team id for both).
COLLEGE_RECORD_CATEGORY = "college"
# ESPN logo folder of every college team (teamlogos/ncaa/500/<id>.png).
COLLEGE_LOGO_SPORT = "ncaa"


@dataclass(frozen=True)
class League:
    """key: category key (teams.category; catalog key "team:<key>:<id>").
    espn_sport/espn_league: ESPN site API path, sports/<espn_sport>/<espn_league>/...
    game_duration: assumed length of one game (guide block, stream window).
    group_title: M3U group-title and dashboard catalog section.
    logo_sport: ESPN logo folder of its catalog channels, teamlogos/<logo_sport>/500/<id>.png.
    season_window: in-season dates; None means always in season.
    is_college: teams come from the shared college records and the directory is refreshed from ESPN.
    sport_label: appended to college channel names, "Michigan Wolverines (Football)".
    schedule_params: query parameters of its ESPN team schedule request (ESPN's
    soccer schedule lists only played games unless asked for fixtures).
    has_schedule: False for an opt-in league ESPN does not cover: no schedule
    request is made, so its channels keep searching while in season.
    name_suffix: appended to its catalog channel names, "Arsenal (Champions League)",
    so a club in two opt-in leagues gets two distinguishable channels."""

    key: str
    espn_sport: str
    espn_league: str
    game_duration: timedelta
    group_title: str = ""
    logo_sport: str = ""
    season_window: Optional[SeasonWindow] = None
    is_college: bool = False
    sport_label: str = ""
    schedule_params: Tuple[Tuple[str, str], ...] = ()
    has_schedule: bool = True
    name_suffix: str = ""


NCAAF = League(
    key="ncaaf", espn_sport="football", espn_league="college-football", game_duration=timedelta(hours=4),
    group_title="College Football", logo_sport=COLLEGE_LOGO_SPORT,
    season_window=(8, 1, 1, 22),  # fall camp/preseason through the CFP national championship
    is_college=True, sport_label="Football",
)
NCAAM = League(
    key="ncaam", espn_sport="basketball", espn_league="mens-college-basketball", game_duration=timedelta(hours=3),
    group_title="College Basketball", logo_sport=COLLEGE_LOGO_SPORT,
    season_window=(11, 1, 4, 10),  # season tip-off through the men's national championship
    is_college=True, sport_label="Men's Basketball",
)
NFL = League(
    key="nfl", espn_sport="football", espn_league="nfl", game_duration=timedelta(hours=4),
    group_title="NFL", logo_sport="nfl",
    season_window=(8, 1, 2, 15),  # Hall of Fame Game/preseason through the Super Bowl
)
MLB = League(
    key="mlb", espn_sport="baseball", espn_league="mlb", game_duration=timedelta(hours=4),
    group_title="MLB", logo_sport="mlb",
    season_window=(2, 15, 11, 10),  # spring training through the World Series
)
NHL = League(
    key="nhl", espn_sport="hockey", espn_league="nhl", game_duration=timedelta(hours=3),
    group_title="NHL", logo_sport="nhl",
    season_window=(9, 15, 6, 30),  # preseason through the Stanley Cup Final
)
NBA = League(
    key="nba", espn_sport="basketball", espn_league="nba", game_duration=timedelta(hours=3),
    group_title="NBA", logo_sport="nba",
    season_window=(10, 1, 6, 30),  # preseason through the NBA Finals
)

# The catalog leagues, in catalog order (dashboard sections, catalog entries).
LEAGUES: Tuple[League, ...] = (NCAAF, NCAAM, NFL, MLB, NHL, NBA)
LEAGUES_BY_KEY: Dict[str, League] = {league.key: league for league in LEAGUES}
CATALOG_CATEGORIES: Tuple[str, ...] = tuple(league.key for league in LEAGUES)
COLLEGE_CATEGORIES: Tuple[str, ...] = tuple(league.key for league in LEAGUES if league.is_college)

# ESPN's soccer team schedule lists only played games unless asked for fixtures.
SOCCER_FIXTURES: Tuple[Tuple[str, str], ...] = (("fixture", "true"),)

# Not a catalog league: only the hand-mapped Inter Miami entry of
# catalog._SPORT_SLUG_MAP uses it, for its ESPN schedule (usa.1 is MLS).
SOCCER = League(
    key="soccer", espn_sport="soccer", espn_league="usa.1", game_duration=timedelta(hours=2.5),
    schedule_params=SOCCER_FIXTURES,
)

# Every key espn_schedule.fetch_espn_team_schedule accepts: the catalog
# leagues, SOCCER, and "ncaa", the sport catalog._SPORT_SLUG_MAP gives college
# teams, which looks up their college football schedule.
SCHEDULE_LEAGUES: Dict[str, League] = {**LEAGUES_BY_KEY, SOCCER.key: SOCCER, "ncaa": NCAAF}


# --- Opt-in leagues (issues #45, #46) ---------------------------------------
# Not part of LEAGUES: their teams appear in the catalog only after the user
# turns the league on (setting OPTIONAL_GROUPS_SETTING), they are seeded from
# the bundled league_seeds/<key>.json (offline, deterministic, frozen ids) and
# never refreshed from ESPN, and none of their names joins the matcher's
# shared alias table, so turning one on or off never changes how an existing
# team matches. Their keys are part of channel ids and catalog keys
# ("team:epl:359") like every league key, so they must never change either.
MLS = League(
    key="mls", espn_sport="soccer", espn_league="usa.1", game_duration=timedelta(hours=2.5),
    group_title="MLS", logo_sport="soccer",
    season_window=(2, 1, 12, 15),  # opening weekend through MLS Cup
    schedule_params=SOCCER_FIXTURES,
)
EPL = League(
    key="epl", espn_sport="soccer", espn_league="eng.1", game_duration=timedelta(hours=2.5),
    group_title="English Premier League", logo_sport="soccer",
    season_window=(8, 1, 5, 31),  # opening weekend through the final matchday
    schedule_params=SOCCER_FIXTURES,
)
UCL = League(
    key="ucl", espn_sport="soccer", espn_league="uefa.champions", game_duration=timedelta(hours=2.5),
    group_title="UEFA Champions League", logo_sport="soccer",
    season_window=(7, 1, 6, 10),  # qualifying rounds through the final
    schedule_params=SOCCER_FIXTURES, name_suffix="Champions League",
)
# ESPN still lists the CFL teams but stopped at the 2022 season (empty team
# schedules, no logos), and has no PWHL league at all: both are seeded with
# their teams only and searched throughout their season window.
CFL = League(
    key="cfl", espn_sport="football", espn_league="cfl", game_duration=timedelta(hours=3.5),
    group_title="CFL", season_window=(5, 15, 11, 30),  # preseason through the Grey Cup
    has_schedule=False,
)
PWHL = League(
    key="pwhl", espn_sport="hockey", espn_league="pwhl", game_duration=timedelta(hours=3),
    group_title="PWHL", season_window=(11, 1, 6, 15),  # preseason through the Walter Cup Final
    has_schedule=False,
)

OPTIONAL_LEAGUES: Tuple[League, ...] = (MLS, EPL, UCL, CFL, PWHL)
OPTIONAL_LEAGUES_BY_KEY: Dict[str, League] = {league.key: league for league in OPTIONAL_LEAGUES}

# Opt-in group of always-live channels (sports_catalog.OPTIONAL_SPECIAL_CHANNELS).
UK_SPORTS_GROUP_KEY = "uk_sports"
UK_SPORTS_GROUP_TITLE = "UK Sports Channels"

# Every opt-in catalog group, in catalog order: (key, dashboard/M3U group title).
OPTIONAL_CATALOG_GROUPS: Tuple[Tuple[str, str], ...] = (
    *((league.key, league.group_title) for league in OPTIONAL_LEAGUES),
    (UK_SPORTS_GROUP_KEY, UK_SPORTS_GROUP_TITLE),
)
OPTIONAL_GROUP_KEYS: Tuple[str, ...] = tuple(key for key, _ in OPTIONAL_CATALOG_GROUPS)
# Settings row holding the enabled opt-in groups, comma separated ("" = none).
OPTIONAL_GROUPS_SETTING = "optional_catalog_groups"


def parse_optional_groups(value: object) -> Tuple[str, ...]:
    """The known opt-in group keys in `value` (a comma-separated setting), in
    catalog order; unknown or repeated keys are dropped."""
    requested = {part.strip().casefold() for part in str(value or "").split(",")}
    return tuple(key for key in OPTIONAL_GROUP_KEYS if key in requested)


def schedule_league(key: str) -> Optional[League]:
    """The league espn_schedule looks up for a team of category `key`."""
    return SCHEDULE_LEAGUES.get(key) or OPTIONAL_LEAGUES_BY_KEY.get(key)


# (ESPN session code, guide label, assumed length). ESPN lists every session of
# a race weekend as one "competition" whose type.abbreviation is the code.
SessionKind = Tuple[str, str, timedelta]


@dataclass(frozen=True)
class Series:
    """A motorsport series: one channel follows every session of every round,
    instead of one team's games.

    key: category key (teams.category; catalog key "series:<key>"). Must not
        collide with a League key and must never change once stored.
    espn_sport/espn_league: ESPN site API path, sports/<espn_sport>/<espn_league>/scoreboard.
    name: display name of the channel.
    short_name: prefix of guide titles ("F1 Qualifying"): one title per
        session kind all season, so a Jellyfin "Record Series" on it records
        every qualifying; the round goes in the episode title.
    group_title: M3U group-title and dashboard catalog section.
    session_kinds: known session codes, in weekend order.
    default_session_duration: length of a session whose code is not listed."""

    key: str
    espn_sport: str
    espn_league: str
    name: str
    short_name: str
    group_title: str
    session_kinds: Tuple[SessionKind, ...]
    default_session_duration: timedelta = timedelta(hours=1)

    def session_label(self, code: str) -> str:
        for kind_code, label, _ in self.session_kinds:
            if kind_code == code:
                return label
        return code

    def session_duration(self, code: str) -> timedelta:
        for kind_code, _, duration in self.session_kinds:
            if kind_code == code:
                return duration
        return self.default_session_duration


F1 = Series(
    key="f1", espn_sport="racing", espn_league="f1",
    name="Formula 1", short_name="F1", group_title="Formula 1",
    session_kinds=(
        ("FP1", "Practice 1", timedelta(hours=1)),
        ("FP2", "Practice 2", timedelta(hours=1)),
        ("FP3", "Practice 3", timedelta(hours=1)),
        ("SS", "Sprint Qualifying", timedelta(hours=1)),  # ESPN: "sprint shootout"
        ("SR", "Sprint", timedelta(hours=1)),
        ("Qual", "Qualifying", timedelta(hours=1)),
        ("Race", "Race", timedelta(hours=2)),
    ),
)

SERIES: Tuple[Series, ...] = (F1,)
SERIES_BY_KEY: Dict[str, Series] = {series.key: series for series in SERIES}
