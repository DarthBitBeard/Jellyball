"""Search terms and conflict rules for motorsport series channels (pure data
and functions, no I/O; imported by sports_matcher and motorsport_schedule).

A series channel searches with three kinds of terms:

* the series' own names ("formula 1", "formula one", "f1");
* the current round's names joined with "gp" / "grand prix" ("brazil gp",
  "sao paulo grand prix"), from F1_ROUNDS and ESPN's city/country;
* the round's venues on their own ("interlagos").

Stream sites list F1 next to other series that share its words ("Grand Prix",
"F1 Academy", MotoGP's "Grand Prix of Qatar") and list the next rounds before
the current one is over, so has_series_conflict() rejects a title that names
another series, or names another round of this series and not the current one.
"""

import re
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from sports_catalog import fold_accents


def fold(text: str) -> str:
    """Lower-case, accents dropped: "São Paulo" -> "sao paulo"."""
    return fold_accents(text)


# The series' own names, by series key (leagues.SERIES_BY_KEY).
SERIES_TERMS: Dict[str, Tuple[str, ...]] = {
    "f1": ("formula 1", "formula one", "f1"),
}

# Other series a stream title can name instead. Matched as whole words in the
# cleaned title; any of them rejects the title for the series.
SERIES_CONFLICTS: Dict[str, Tuple[str, ...]] = {
    "f1": (
        "academy", "f1 academy",
        "formula 2", "formula two", "f2",
        "formula 3", "formula three", "f3",
        "formula 4", "f4", "formula regional",
        "formula e",
        "motogp", "moto gp", "moto2", "moto3", "motoe",
        "superbike", "superbikes", "wsbk",
        "indycar", "indy car", "indy 500", "nascar",
        "wec", "world endurance", "le mans", "imsa",
        "porsche supercup", "porsche carrera cup", "supercars", "dtm", "w series",
    ),
}


# Formula 1 rounds: the "<place> Grand Prix" name -> (other names that take
# "GP"/"Grand Prix", venues that identify the round on their own). Names are
# matched after fold(). Keys are also the places motorsport_schedule strips a
# title sponsor up to ("MSC Cruises São Paulo Grand Prix" -> "São Paulo GP").
F1_ROUNDS: Dict[str, Tuple[Tuple[str, ...], Tuple[str, ...]]] = {
    "70th Anniversary": ((), ("silverstone",)),
    "Abu Dhabi": (("abu dhabi", "uae"), ("yas marina",)),
    "Australian": (("australia", "melbourne"), ("albert park",)),
    "Austrian": (("austria",), ("red bull ring", "spielberg")),
    "Azerbaijan": (("baku",), ()),
    "Bahrain": (("bahrain",), ()),
    "Barcelona-Catalunya": (("barcelona", "catalunya", "catalonia", "spain", "spanish"), ()),
    "Belgian": (("belgium", "spa"), ("spa francorchamps", "spa-francorchamps")),
    "Brazilian": (("brazil", "sao paulo"), ("interlagos",)),
    "British": (("britain", "great britain", "uk"), ("silverstone",)),
    "Canadian": (("canada", "montreal"), ("gilles villeneuve",)),
    "Chinese": (("china", "shanghai"), ()),
    "Dutch": (("netherlands", "holland"), ("zandvoort",)),
    "Eifel": ((), ("nurburgring",)),
    "Emilia Romagna": (("imola",), ()),
    "French": (("france",), ("paul ricard",)),
    "German": (("germany",), ("hockenheim", "hockenheimring")),
    "Hungarian": (("hungary", "budapest"), ("hungaroring",)),
    "Italian": (("italy", "monza"), ()),
    "Japanese": (("japan",), ("suzuka",)),
    "Las Vegas": (("vegas",), ()),
    "Madrid": (("spain", "spanish"), ("madring",)),
    "Mexican": (("mexico", "mexico city"), ("hermanos rodriguez",)),
    "Mexico City": (("mexico", "mexican"), ("hermanos rodriguez",)),
    "Miami": ((), ()),
    "Monaco": (("monte carlo",), ()),
    "Portuguese": (("portugal",), ("portimao",)),
    "Qatar": ((), ("lusail",)),
    "Russian": (("russia",), ("sochi",)),
    "Sakhir": ((), ()),
    "São Paulo": (("brazil", "brazilian"), ("interlagos",)),
    "Sao Paulo": (("brazil", "brazilian"), ("interlagos",)),
    "Saudi Arabian": (("saudi", "saudi arabia", "jeddah"), ()),
    "Singapore": ((), ("marina bay",)),
    "Spanish": (("spain", "madrid"), ("madring",)),
    "Styrian": (("styria",), ()),
    "Turkish": (("turkey",), ("istanbul park",)),
    "Tuscan": (("tuscany",), ("mugello",)),
    "United States": (("usa", "us", "united states", "austin", "usgp"), ("circuit of the americas", "cota")),
}
F1_GRAND_PRIX_PLACES: Tuple[str, ...] = tuple(F1_ROUNDS)

_GP_SUFFIXES = ("gp", "grand prix")


def _gp_forms(names: Iterable[str]) -> Set[str]:
    return {f"{fold(name).strip()} {suffix}" for name in names if fold(name).strip() for suffix in _GP_SUFFIXES}


def round_names(place: str, city: str = "", country: str = "") -> Tuple[Set[str], Set[str]]:
    """(names + "gp"/"grand prix", venues) of one round. `place` is the F1_ROUNDS
    key (or any place name), city/country come from ESPN."""
    other_names, venues = F1_ROUNDS.get(place, ((), ()))
    names = _gp_forms((place, *other_names, city, country))
    if "usgp" in other_names:
        names.add("usgp")
    return names, {fold(v) for v in venues}


def _all_round_names() -> Set[str]:
    every: Set[str] = set()
    for place in F1_ROUNDS:
        names, venues = round_names(place)
        every |= names | venues
    return every


_ALL_F1_ROUND_NAMES = frozenset(_all_round_names())


def series_search_terms(series_key: str, place: str = "", city: str = "", country: str = "") -> List[str]:
    """The series' names plus, when the current round is known, its names and
    venues; longest first like sports_matcher.get_team_search_terms."""
    terms = set(SERIES_TERMS.get(series_key, ()))
    if not terms:
        return []
    if place or city or country:
        names, venues = round_names(place, city, country)
        terms |= names | venues
    return sorted(terms, key=lambda t: (len(t.split()), len(t)), reverse=True)


def _mentions(term: str, text: str) -> bool:
    return re.search(rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])", text) is not None


def series_for_terms(search_terms: Sequence[str]) -> Optional[str]:
    """The series key whose own names are among `search_terms`, if any."""
    normalized = {fold(t).strip() for t in search_terms}
    for key, names in SERIES_TERMS.items():
        if normalized & set(names):
            return key
    return None


def has_series_conflict(search_terms: Sequence[str], text: str) -> bool:
    """True when `text` (cleaned, lower-case) names another series than the one
    searched for, or another round of it while not naming the current round.
    Always False for team searches (no series name among the terms)."""
    series_key = series_for_terms(search_terms)
    if series_key is None:
        return False
    target = fold(text)
    if any(_mentions(other, target) for other in SERIES_CONFLICTS.get(series_key, ())):
        return True
    if series_key != "f1":
        return False
    normalized = {fold(t).strip() for t in search_terms}
    current = (normalized - set(SERIES_TERMS[series_key]))
    if not current:
        return False  # current round unknown: any round may match
    if any(_mentions(name, target) for name in current):
        return False
    return any(_mentions(name, target) for name in _ALL_F1_ROUND_NAMES - current)
