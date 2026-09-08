"""
sports_matcher.py - Robust Sports Team Matching and Alias Engine
Provides sports aliases, acronym expansion, commentary noise stripping,
and precision token matching with state/short-word guardrails.
"""

import re
from typing import List, Tuple, Set, Dict, Optional

from sports_catalog import STATIC_TEAM_RECORDS, normalize_team_label

try:
    from thefuzz import fuzz
except ImportError:
    from difflib import SequenceMatcher
    class FuzzFallback:
        @staticmethod
        def partial_ratio(s1, s2):
            return int(SequenceMatcher(None, s1.lower(), s2.lower()).find_longest_match(0, len(s1), 0, len(s2)).size / max(len(s1), 1) * 100)
        @staticmethod
        def token_set_ratio(s1, s2):
            t1 = " ".join(sorted(s1.lower().split()))
            t2 = " ".join(sorted(s2.lower().split()))
            return int(SequenceMatcher(None, t1, t2).ratio() * 100)
        @staticmethod
        def ratio(s1, s2):
            return int(SequenceMatcher(None, s1.lower(), s2.lower()).ratio() * 100)
    fuzz = FuzzFallback()

# --- COMPREHENSIVE SPORTS ALIAS MAPPING ---
_TEAM_ALIASES_DB: Dict[str, List[str]] = {
    # Florida & Major Colleges
    "florida gators": ["florida", "gators", "uf", "univ of florida", "university of florida"],
    "florida state seminoles": ["florida state", "florida st", "seminoles", "noles", "fsu"],
    "central florida knights": ["ucf", "central florida", "knights", "ucf knights", "cfl"],
    "miami hurricanes": ["miami", "hurricanes", "canes", "um", "miami hurricanes", "miami fl", "miami (fl)"],
    "south florida bulls": ["usf", "south florida", "bulls"],
    "florida atlantic owls": ["fau", "florida atlantic", "owls"],
    "florida international panthers": ["fiu", "florida international", "fiu panthers"],
    # Big Ten
    "michigan wolverines": ["michigan", "wolverines", "um", "u of m"],
    "michigan state spartans": ["michigan state", "michigan st", "spartans", "msu"],
    "ohio state buckeyes": ["ohio state", "ohio st", "buckeyes", "osu"],
    "penn state nittany lions": ["penn state", "penn st", "nittany lions", "psu"],
    "wisconsin badgers": ["wisconsin", "badgers", "uw"],
    "nebraska cornhuskers": ["nebraska", "cornhuskers", "huskers"],
    "iowa hawkeyes": ["iowa", "hawkeyes"],
    "indiana hoosiers": ["indiana", "hoosiers", "iu"],
    "purdue boilermakers": ["purdue", "boilermakers"],
    "illinois fighting illini": ["illinois", "fighting illini", "illini", "uiuc"],
    "minnesota golden gophers": ["minnesota", "golden gophers", "gophers"],
    "rutgers scarlet knights": ["rutgers", "scarlet knights"],
    "maryland terrapins": ["maryland", "terrapins", "terps", "umd"],
    "northwestern wildcats": ["northwestern", "wildcats"],
    "oregon ducks": ["oregon", "ducks", "uo"],
    "washington huskies": ["washington", "huskies", "uw"],
    "usc trojans": ["usc", "southern california", "trojans"],
    "ucla bruins": ["ucla", "bruins"],
    # SEC
    "alabama crimson tide": ["alabama", "crimson tide", "bama", "bama tide"],
    "georgia bulldogs": ["georgia", "bulldogs", "uga", "dawgs"],
    "texas longhorns": ["texas", "longhorns", "horns", "ut"],
    "texas a&m aggies": ["texas a&m", "texas am", "aggies", "tamu", "a&m"],
    "lsu tigers": ["lsu", "louisiana state", "tigers", "bayou bengals"],
    "tennessee volunteers": ["tennessee", "volunteers", "vols", "ut"],
    "oklahoma sooners": ["oklahoma", "sooners", "ou"],
    "auburn tigers": ["auburn", "tigers", "war eagle"],
    "arkansas razorbacks": ["arkansas", "razorbacks", "hogs"],
    "south carolina gamecocks": ["south carolina", "gamecocks", "sc"],
    "kentucky wildcats": ["kentucky", "wildcats", "uk"],
    "missouri tigers": ["missouri", "mizzou", "tigers"],
    "ole miss rebels": ["ole miss", "mississippi", "rebels"],
    "mississippi state bulldogs": ["mississippi state", "mississippi st", "msu", "bulldogs"],
    "vanderbilt commodores": ["vanderbilt", "commodores", "vandy"],
    # ACC & Independents
    "clemson tigers": ["clemson", "tigers"],
    "notre dame fighting irish": ["notre dame", "fighting irish", "irish", "nd"],
    "north carolina tar heels": ["north carolina", "tar heels", "unc"],
    "duke blue devils": ["duke", "blue devils"],
    "virginia cavaliers": ["virginia", "cavaliers", "cavs", "uva"],
    "virginia tech hokies": ["virginia tech", "va tech", "hokies", "vt"],
    "georgia tech yellow jackets": ["georgia tech", "yellow jackets", "gt"],
    "louisville cardinals": ["louisville", "cardinals", "cards"],
    "pittsburgh panthers": ["pittsburgh", "pitt", "panthers"],
    "syracuse orange": ["syracuse", "orange", "cuse"],
    "boston college eagles": ["boston college", "eagles", "bc"],
    "wake forest demon deacons": ["wake forest", "demon deacons", "wake"],
    "nc state wolfpack": ["nc state", "wolfpack", "ncst"],
    "stanford cardinal": ["stanford", "cardinal"],
    "california golden bears": ["california", "cal", "golden bears"],
    "smu mustangs": ["smu", "mustangs"],
    # Big 12
    "colorado buffaloes": ["colorado", "buffaloes", "buffs", "cu"],
    "utah utes": ["utah", "utes"],
    "kansas jayhawks": ["kansas", "jayhawks", "ku"],
    "kansas state wildcats": ["kansas state", "kansas st", "wildcats", "ksu"],
    "iowa state cyclones": ["iowa state", "iowa st", "cyclones", "isu"],
    "arizona wildcats": ["arizona", "wildcats", "ua"],
    "arizona state sun devils": ["arizona state", "arizona st", "sun devils", "asu"],
    "west virginia mountaineers": ["west virginia", "mountaineers", "wvu"],
    "tcu horned frogs": ["tcu", "horned frogs"],
    "baylor bears": ["baylor", "bears"],
    "texas tech red raiders": ["texas tech", "red raiders", "ttu"],
    "houston cougars": ["houston", "cougars", "uh"],
    "cincinnati bearcats": ["cincinnati", "bearcats", "uc"],
    "byu cougars": ["byu", "cougars"],
    "oklahoma state cowboys": ["oklahoma state", "oklahoma st", "cowboys", "ok state"],
    # Other Notable Colleges
    "boise state broncos": ["boise state", "boise st", "broncos", "bsu"],
    "appalachian state mountaineers": ["appalachian state", "app state", "mountaineers"],
    "memphis tigers": ["memphis", "tigers"],
    "tulane green wave": ["tulane", "green wave"],
    "navy midshipmen": ["navy", "midshipmen"],
    "army black knights": ["army", "black knights"],
    "air force falcons": ["air force", "falcons"],

    # NFL Teams
    "miami dolphins": ["miami dolphins", "dolphins", "fins", "mia"],
    "tampa bay buccaneers": ["tampa bay buccaneers", "tampa bay", "buccaneers", "bucs", "tb"],
    "jacksonville jaguars": ["jacksonville jaguars", "jacksonville", "jaguars", "jags", "jax"],
    "new york jets": ["new york jets", "jets", "nyj"],
    "new york giants": ["new york giants", "giants", "nyg"],
    "kansas city chiefs": ["kansas city chiefs", "chiefs", "kc"],
    "buffalo bills": ["buffalo bills", "bills", "buf"],
    "new england patriots": ["new england patriots", "patriots", "pats", "ne"],
    "baltimore ravens": ["baltimore ravens", "ravens", "bal"],
    "pittsburgh steelers": ["pittsburgh steelers", "steelers", "pit"],
    "cleveland browns": ["cleveland browns", "browns", "cle"],
    "cincinnati bengals": ["cincinnati bengals", "bengals", "cin"],
    "houston texans": ["houston texans", "texans", "hou"],
    "indianapolis colts": ["indianapolis colts", "colts", "indy", "ind"],
    "tennessee titans": ["tennessee titans", "titans", "ten"],
    "denver broncos": ["denver broncos", "broncos", "den"],
    "las vegas raiders": ["las vegas raiders", "raiders", "lv"],
    "los angeles chargers": ["los angeles chargers", "chargers", "lac"],
    "dallas cowboys": ["dallas cowboys", "cowboys", "dal"],
    "philadelphia eagles": ["philadelphia eagles", "eagles", "phi"],
    "washington commanders": ["washington commanders", "commanders", "was"],
    "green bay packers": ["green bay packers", "packers", "gb"],
    "chicago bears": ["chicago bears", "bears", "chi"],
    "detroit lions": ["detroit lions", "lions", "det"],
    "minnesota vikings": ["minnesota vikings", "vikings", "min"],
    "atlanta falcons": ["atlanta falcons", "falcons", "atl"],
    "carolina panthers": ["carolina panthers", "panthers", "car"],
    "new orleans saints": ["new orleans saints", "saints", "no"],
    "san francisco 49ers": ["san francisco 49ers", "49ers", "niners", "sf"],
    "seattle seahawks": ["seattle seahawks", "seahawks", "sea"],
    "los angeles rams": ["los angeles rams", "rams", "lar"],
    "arizona cardinals": ["arizona cardinals", "cardinals", "az", "ari"],

    # MLB Teams
    "miami marlins": ["miami marlins", "marlins", "mia"],
    "tampa bay rays": ["tampa bay rays", "rays", "tb"],
    "new york yankees": ["new york yankees", "yankees", "nyy", "bronx bombers"],
    "new york mets": ["new york mets", "mets", "nym"],
    "boston red sox": ["boston red sox", "red sox", "bos"],
    "los angeles dodgers": ["los angeles dodgers", "dodgers", "lad"],
    "chicago cubs": ["chicago cubs", "cubs", "chc"],
    "chicago white sox": ["chicago white sox", "white sox", "chw"],
    "atlanta braves": ["atlanta braves", "braves", "atl"],
    "philadelphia phillies": ["philadelphia phillies", "phillies", "phi"],
    "houston astros": ["houston astros", "astros", "hou"],
    "texas rangers": ["texas rangers", "rangers", "tex"],
    "st louis cardinals": ["st louis cardinals", "cardinals", "stl"],
    "baltimore orioles": ["baltimore orioles", "orioles", "os", "bal"],
    "kansas city royals": ["kansas city royals", "royals", "kc"],
    "san diego padres": ["san diego padres", "padres", "sd"],
    "san francisco giants": ["san francisco giants", "giants", "sf"],
    "seattle mariners": ["seattle mariners", "mariners", "sea"],
    "cleveland guardians": ["cleveland guardians", "guardians", "cle"],
    "milwaukee brewers": ["milwaukee brewers", "brewers", "mil"],
    "toronto blue jays": ["toronto blue jays", "blue jays", "jays", "tor"],
    "detroit tigers": ["detroit tigers", "tigers", "det"],
    "minnesota twins": ["minnesota twins", "twins", "min"],
    "athletics": ["athletics", "a's", "as", "oakland athletics"],

    # NBA & NHL Teams (Key Florida / Major)
    "miami heat": ["miami heat", "heat", "mia"],
    "orlando magic": ["orlando magic", "magic", "orl"],
    "florida panthers": ["florida panthers", "panthers", "fla"],
    "tampa bay lightning": ["tampa bay lightning", "lightning", "bolts", "tb"],
}

# Keep the legacy aliases above for backward compatibility, but merge the
# shared catalog so the complete NFL/NBA fallback set and common NCAA teams
# use the same identity data as logo and schedule resolution.
for _catalog_record in STATIC_TEAM_RECORDS:
    _TEAM_ALIASES_DB.setdefault(_catalog_record.canonical, [])
    for _catalog_alias in (_catalog_record.canonical, *_catalog_record.aliases):
        if _catalog_alias and _catalog_alias not in _TEAM_ALIASES_DB[_catalog_record.canonical]:
            _TEAM_ALIASES_DB[_catalog_record.canonical].append(_catalog_alias)


def _canonical_matches(value: str) -> Set[str]:
    normalized = normalize_team_label(value)
    if not normalized:
        return set()

    matches: Set[str] = set()
    for canonical, aliases in _TEAM_ALIASES_DB.items():
        known_names = {normalize_team_label(canonical), *(normalize_team_label(alias) for alias in aliases)}
        if normalized in known_names:
            matches.add(canonical)
            continue
        if len(normalized.split()) > 1 and any(
            len(alias) >= 4 and re.search(rf"\b{re.escape(alias)}\b", normalized)
            for alias in known_names
            if alias
        ):
            matches.add(canonical)
    return matches


def _exact_canonical_matches(value: str) -> Set[str]:
    normalized = normalize_team_label(value)
    if not normalized:
        return set()
    matches: Set[str] = set()
    for canonical, aliases in _TEAM_ALIASES_DB.items():
        known_names = {normalize_team_label(canonical), *(normalize_team_label(alias) for alias in aliases)}
        if normalized in known_names:
            matches.add(canonical)
    return matches


def _is_ambiguous_term(term: str) -> bool:
    """Return True for nicknames/codes shared by multiple team identities."""
    return len(_exact_canonical_matches(term)) > 1

# Compound nickname conflicts (e.g. Rutgers Scarlet Knights != UCF Knights)
_COMPOUND_CONFLICTS: Dict[str, List[str]] = {
    "knights": ["scarlet knights", "black knights", "golden knights", "southern virginia knights"],
    "raiders": ["red raiders"],
    "sox": ["red sox", "white sox"],
    "panthers": ["florida panthers", "carolina panthers", "pittsburgh panthers", "fiu panthers"],
}

# Words to strip out as stream metadata noise
_NOISE_PATTERNS = [
    r'\bin\s+progress\b',
    r'\bcfb\s+live\b',
    r'\bnfl\s+live\b',
    r'\bmlb\s+live\b',
    r'\bnba\s+live\b',
    r'\bnhl\s+live\b',
    r'\blive\s+streams?\b',
    r'\blivestreams?\b',
    r'\blive\s+broadcast\b',
    r'\blive\s+stream\b',
    r'\blive\b',
    r'\bdelayed\b',
    r'\bstream\b',
    r'\bstreams\b',
    r'\bwatch\b',
    r'\bfree\b',
    r'\bhd\b',
    r'\bfhd\b',
    r'\bhq\b',
    r'\bcfb\b',
    r'\bncaaf\b',
    r'\bncaab\b',
    r'\bnfl\b',
    r'\bmlb\b',
    r'\bnba\b',
    r'\bnhl\b',
    r'\bquarter\b',
    r'\b\d{1,2}(?:st|nd|rd|th)\s*quarter\b',
    r'\b\d{1,2}:\d{2}\s*-\s*\d{1,2}(?:st|nd|rd|th)\s*quarter\b',
    r'\b\d{1,2}:\d{2}\s*(?:am|pm)?(?:\s*et)?\b',
    r'\b\d+\s*hours?\s*(?:from\s*now|ago)\b',
    r'\b\d+\s*minutes?\s*(?:from\s*now|ago)\b',
    r'\bsportsurge\b',
    r'\bbuffstreams\b',
    r'\bstreameast\b',
    r'\bmethstreams\b',
]

def clean_sports_text(text: str) -> str:
    """Removes live match metadata noise, timestamps, and quarter info."""
    clean = text.lower()
    for pattern in _NOISE_PATTERNS:
        clean = re.sub(pattern, ' ', clean, flags=re.IGNORECASE)
    # Replace non-alphanumeric with spaces except keep vs, @, -
    clean = re.sub(r'[^a-z0-9\s@\-]', ' ', clean)
    return re.sub(r'\s+', ' ', clean).strip()

def get_team_search_terms(team_name: str, query: str = "", team_id: str = "") -> List[str]:
    """
    Builds a deduplicated list of search terms, aliases, and acronyms for a team.
    Combines the team display name, user query, team_id, and any known database aliases.
    """
    terms: Set[str] = set()
    raw_values = [team_name, query, team_id.replace("_", " ")]

    # Always retain the caller's exact terms, but only expand an alias when it
    # identifies one canonical team. This prevents a query such as "Miami"
    # from collecting Dolphins, Heat, Hurricanes, and Marlins aliases together.
    for raw in raw_values:
        cleaned = normalize_team_label(raw)
        if not cleaned:
            continue
        terms.add(cleaned)

        matches = _canonical_matches(cleaned)
        explicit = {
            canonical for canonical in matches
            if re.search(rf"\b{re.escape(normalize_team_label(canonical))}\b", cleaned)
        }
        if len(explicit) == 1:
            matches = explicit
        if len(matches) != 1:
            continue

        canonical_name = next(iter(matches))
        terms.add(canonical_name)
        for alias in _TEAM_ALIASES_DB.get(canonical_name, []):
            alias = normalize_team_label(alias)
            if alias and _exact_canonical_matches(alias) == {canonical_name}:
                terms.add(alias)

    # Return ordered terms, prioritizing longer / more specific terms first
    result = sorted(list(terms), key=lambda x: (len(x.split()), len(x)), reverse=True)
    return result


def canonical_team_name(team_name: str) -> str:
    """Resolve an unambiguous full name or alias to the database canonical name."""
    normalized = normalize_team_label(team_name)
    if not normalized:
        return ""

    exact_matches = sorted(_exact_canonical_matches(normalized))
    if len(exact_matches) == 1:
        return exact_matches[0]

    embedded_matches = [
        canonical for canonical, aliases in _TEAM_ALIASES_DB.items()
        if re.search(rf"\b{re.escape(normalize_team_label(canonical))}\b", normalized)
    ]
    if len(set(embedded_matches)) == 1:
        return embedded_matches[0]

    # Only accept a fuzzy identity when it is clearly unique; generic names
    # such as Tigers, Giants, or Panthers must remain unresolved.
    scored = []
    for canonical in _TEAM_ALIASES_DB:
        score = fuzz.token_set_ratio(normalized, canonical)
        if score >= 90:
            scored.append((score, canonical))
    if scored:
        best_score = max(score for score, _ in scored)
        best = {canonical for score, canonical in scored if score == best_score}
        if len(best) == 1:
            return best.pop()
    return normalized

def is_state_school_conflict(query_term: str, candidate_text: str) -> bool:
    """
    Guards against 'Florida' matching 'Florida State',
    'Michigan' matching 'Michigan State', etc.
    """
    q_low = query_term.lower().strip()
    c_low = candidate_text.lower().strip()

    if "state" in q_low or " st" in q_low or q_low.endswith("st"):
        return False

    state_schools = [
        "florida", "michigan", "ohio", "penn", "washington", "oregon",
        "arizona", "kansas", "iowa", "mississippi", "oklahoma", "georgia",
        "arkansas", "colorado", "indiana", "illinois", "idaho", "montana",
        "utah", "new mexico", "texas", "california", "louisiana", "boise",
        "ball", "kent", "fresno", "san diego", "san jose"
    ]

    for school in state_schools:
        if school in q_low and (school + " state" not in q_low and school + " st" not in q_low):
            pattern = rf'\b{re.escape(school)}\s+(?:state|st)\b'
            if re.search(pattern, c_low):
                return True
    return False


_TEAM_IDENTITY_FAMILIES = (
    (
        {"michigan state", "spartans", "msu"},
        ({"eastern michigan", "eastern michigan eagles"},
         {"central michigan", "central michigan chippewas"},
         {"western michigan", "western michigan broncos"},
         {"michigan wolverines", "wolverines"}),
    ),
    (
        {"tampa bay buccaneers", "buccaneers", "bucs"},
        ({"tampa bay rays", "tampa bay devil rays", "rays"},
         {"tampa bay lightning", "lightning"}),
    ),
    (
        {"new york jets", "ny jets", "jets"},
        ({"new york mets", "ny mets", "mets"},
         {"new york giants", "ny giants", "giants"}),
    ),
)


def has_team_identity_conflict(search_terms: List[str], target: str) -> bool:
    """Reject a different team in a known same-name identity family."""
    target_low = target.lower()
    normalized_terms = {term.lower().strip() for term in search_terms}

    for requested_variants, competing_groups in _TEAM_IDENTITY_FAMILIES:
        requested = requested_variants & normalized_terms
        if not requested:
            continue
        if any(re.search(rf"\b{re.escape(variant)}\b", target_low) for variant in requested):
            continue
        if any(
            any(re.search(rf"\b{re.escape(variant)}\b", target_low) for variant in group)
            for group in competing_groups
        ):
            return True
    return False

def has_compound_conflict(term: str, target: str, search_terms: List[str]) -> bool:
    """
    Checks if a generic term like 'knights' conflicts with a compound modifier
    present in target (e.g. 'scarlet knights') when the search terms don't include it.
    """
    clean_target = ' '.join(target.replace('-', ' ').split())
    compounds = _COMPOUND_CONFLICTS.get(term)
    if compounds:
        for compound in compounds:
            if compound in clean_target:
                # If the target has 'scarlet knights', but none of our search terms have 'scarlet':
                if not any(compound in st for st in search_terms):
                    return True
    return False

def match_team(
    search_terms: List[str],
    text: str,
    href: str = "",
    title: str = "",
    threshold: int = 70
) -> Tuple[bool, int, str]:
    """
    Evaluates whether any search term matches the given event text, href URL slug, or title.
    
    Returns:
        (is_matched: bool, score: int, matched_term: str)
    """
    if not search_terms:
        return False, 0, ""

    clean_text = clean_sports_text(text)
    slug_text = re.sub(r'[^a-z0-9]', ' ', href.lower()).strip()
    clean_title = clean_sports_text(title)
    
    sides = re.split(r'\s+(?:vs\.?|at|@|-)\s+', clean_text)
    sides = [s.strip() for s in sides if s.strip()]
    
    all_targets = [clean_text] + sides
    if slug_text:
        all_targets.append(slug_text)
    if clean_title:
        all_targets.append(clean_title)

    best_score = 0
    best_term = ""

    for term in search_terms:
        term_low = term.strip().lower()
        if not term_low:
            continue
        if _is_ambiguous_term(term_low):
            continue

        term_is_short = len(term_low) <= 4

        for target in all_targets:
            if not target:
                continue

            if has_team_identity_conflict(search_terms, target):
                continue

            # Check state-school conflict guard
            if is_state_school_conflict(term_low, target):
                continue

            # Check compound modifier conflict (e.g. Scarlet Knights vs Knights)
            if has_compound_conflict(term_low, target, search_terms):
                continue

            # 1. Exact phrase / acronym with word boundaries
            pattern = rf'\b{re.escape(term_low)}\b'
            if re.search(pattern, target):
                score = 110 if len(term_low.split()) > 1 else (100 if len(term_low) > 2 else 95)
                if score > best_score:
                    best_score = score
                    best_term = term_low
                continue

            if term_is_short:
                continue

            # 2. Substring match for longer multi-word terms
            if len(term_low) > 4 and term_low in target:
                score = 95
                if score > best_score:
                    best_score = score
                    best_term = term_low
                continue

            # 3. Token set ratio for multi-word terms with high confidence threshold
            if len(term_low.split()) > 1:
                token_score = fuzz.token_set_ratio(term_low, target)
                if token_score >= 85 and token_score > best_score:
                    best_score = token_score
                    best_term = term_low

    return (best_score >= threshold), best_score, best_term
