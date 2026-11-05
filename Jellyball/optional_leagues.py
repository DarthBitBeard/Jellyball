"""Team records of the opt-in leagues (leagues.OPTIONAL_LEAGUES).

Each league's teams come from the bundled league_seeds/<key>.json: no network,
the same records on every machine, and frozen team ids (they are part of the
channel ids and catalog keys, "team:epl:359").

These teams are deliberately kept out of the matcher's shared alias table
(sports_matcher._TEAM_ALIASES_DB) and out of sports_catalog.STATIC_TEAM_RECORDS:
adding them there would turn existing nicknames ambiguous ("Spurs" is San
Antonio's) and silently stop existing channels from matching. Instead every
seed alias is kept only when it is safe:

- it is at least 3 characters and not a generic club word ("united", "inter");
- no existing team uses it as a name, and it does not occur inside one
  ("lions" is the Detroit Lions'; "golden" is inside "golden state warriors");
- no other opt-in team's names contain it ("inter" is inside "inter miami cf").

The surviving multi-word names are also what sports_matcher uses to keep an
existing team's search ("Kansas City Chiefs" -> "kansas city") from matching a
title that names an opt-in team ("Sporting Kansas City vs LA Galaxy").
"""

import json
import logging
import re
import sys
from pathlib import Path
from typing import Dict, FrozenSet, Iterable, List, Mapping, Sequence, Set, Tuple

from leagues import OPTIONAL_LEAGUES, League
from sports_catalog import TeamSlug, normalize_team_label

_LOGGER = logging.getLogger("jellyball")

SEED_DIR_NAME = "league_seeds"
MIN_ALIAS_LENGTH = 3
# Words that name many clubs; never a team's alias on their own.
GENERIC_ALIASES: FrozenSet[str] = frozenset({
    "fc", "sc", "cf", "afc", "club", "united", "utd", "city", "real", "inter", "sporting", "athletic",
    "union", "galaxy", "fire", "crew", "dynamo", "revolution", "fleet", "frost", "charge", "torrent",
    "riders", "bombers", "red bulls", "red bull", "forest", "palace", "villa", "rovers", "wanderers",
})

Identity = Tuple[str, str]


def _seed_dir() -> Path:
    # The PyInstaller bundle unpacks data files under sys._MEIPASS; the source
    # tree and the Docker image keep them next to this module.
    return Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent)) / SEED_DIR_NAME


def load_seed(league: League, seed_dir: Path) -> List[dict]:
    """The seed's team rows ({"id", "name", "aliases"}), or [] (logged) when
    the file is missing or malformed: the league is then just empty."""
    path = seed_dir / f"{league.key}.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        rows = data["teams"]
        if data.get("league") != league.key or not isinstance(rows, list):
            raise ValueError("seed does not describe this league")
    except Exception as exc:  # noqa: BLE001 - a bad seed must not break startup
        _LOGGER.warning("League seed %s unusable (%s): %s", path.name, type(exc).__name__, exc)
        return []
    teams = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        team_id, name = str(row.get("id") or "").strip(), str(row.get("name") or "").strip()
        aliases = row.get("aliases") if isinstance(row.get("aliases"), list) else []
        if team_id and name:
            teams.append({"id": team_id, "name": name, "aliases": [str(alias) for alias in aliases]})
    return teams


def identity_of(record: TeamSlug) -> Identity:
    """One club in several opt-in leagues is one identity: ESPN uses the same
    soccer team id in the Premier League and the Champions League."""
    return (record.logo_sport or record.category, record.team_id)


def _contains_phrase(haystack: str, phrase: str) -> bool:
    if phrase not in haystack or haystack == phrase:
        return False
    return re.search(rf"(?<![a-z0-9]){re.escape(phrase)}(?![a-z0-9])", haystack) is not None


def _existing_team_names() -> FrozenSet[str]:
    # Late import: sports_matcher imports this module lazily for its guard.
    from sports_matcher import _TEAM_ALIASES_DB

    names = set()
    for canonical, aliases in _TEAM_ALIASES_DB.items():
        for name in (canonical, *aliases):
            normalized = normalize_team_label(name)
            if normalized:
                names.add(normalized)
    return frozenset(names)


def build_records(
    seeds: Sequence[Tuple[League, Sequence[dict]]],
    existing_names: Iterable[str],
) -> Tuple[TeamSlug, ...]:
    """Turn seed rows into catalog records, keeping only the safe aliases."""
    existing = frozenset(existing_names)
    drafts: List[Tuple[League, dict, str, List[str]]] = []
    for league, rows in seeds:
        for row in rows:
            canonical = normalize_team_label(row["name"])
            aliases = [normalize_team_label(alias) for alias in row["aliases"]]
            drafts.append((league, row, canonical, aliases))

    def draft_identity(league: League, row: dict) -> Identity:
        return (league.logo_sport or league.key, row["id"])

    names_by_identity: Dict[Identity, Set[str]] = {}
    for league, row, canonical, aliases in drafts:
        names_by_identity.setdefault(draft_identity(league, row), set()).update([canonical, *aliases])

    def safe(alias: str, identity: Identity) -> bool:
        if len(alias) < MIN_ALIAS_LENGTH or alias in GENERIC_ALIASES or alias in existing:
            return False
        if any(_contains_phrase(name, alias) for name in existing):
            return False
        return not any(
            alias == name or _contains_phrase(name, alias)
            for other, names in names_by_identity.items() if other != identity
            for name in names
        )

    records = []
    for league, row, canonical, aliases in drafts:
        identity = draft_identity(league, row)
        kept = tuple(dict.fromkeys(a for a in aliases if a and a != canonical and safe(a, identity)))
        records.append(TeamSlug(
            canonical=canonical,
            category=league.key,
            slug=row["id"],
            logo_sport=league.logo_sport,
            aliases=kept,
            team_id=row["id"],
            display=row["name"],
        ))
    return tuple(records)


def _load_records() -> Tuple[TeamSlug, ...]:
    seed_dir = _seed_dir()
    return build_records([(league, load_seed(league, seed_dir)) for league in OPTIONAL_LEAGUES], _existing_team_names())


OPTIONAL_TEAM_RECORDS: Tuple[TeamSlug, ...] = _load_records()


def optional_records(category: str) -> Tuple[TeamSlug, ...]:
    """The opt-in league's records, sorted for display."""
    return tuple(sorted(
        (record for record in OPTIONAL_TEAM_RECORDS if record.category == category),
        key=lambda record: record.display_name.casefold(),
    ))


def _names_by_identity(records: Iterable[TeamSlug]) -> Dict[Identity, FrozenSet[str]]:
    grouped: Dict[Identity, Set[str]] = {}
    for record in records:
        grouped.setdefault(identity_of(record), set()).update((record.canonical, *record.aliases))
    return {identity: frozenset(names) for identity, names in grouped.items()}


# Every opt-in identity's names (canonical + kept aliases).
OPTIONAL_TEAM_NAMES: Dict[Identity, FrozenSet[str]] = _names_by_identity(OPTIONAL_TEAM_RECORDS)
