"""
Shared team identity and ESPN slug catalog.

The catalog deliberately separates a team's display identity from its ESPN
league. NFL, MLB, NHL, and NBA records are complete static fallbacks. College
records cover common teams offline and can be refreshed from ESPN's team
directories for full NCAAF/NCAAM coverage at runtime.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from leagues import (
    COLLEGE_CATEGORIES,
    COLLEGE_LOGO_SPORT,
    COLLEGE_RECORD_CATEGORY,
    LEAGUES,
    MLB,
    NBA,
    NCAAF,
    NCAAM,
    NFL,
    NHL,
)


@dataclass(frozen=True)
class TeamSlug:
    """Normalized team identity used for search, logos, and schedules."""

    canonical: str
    category: str
    slug: str
    logo_sport: str
    aliases: Tuple[str, ...] = ()
    team_id: str = ""

    @property
    def is_college(self) -> bool:
        return self.category == COLLEGE_RECORD_CATEGORY or self.category in COLLEGE_CATEGORIES

    def for_category(self, category: str) -> "TeamSlug":
        if not self.is_college or category not in COLLEGE_CATEGORIES:
            return self
        return replace(self, category=category)

    @property
    def display_name(self) -> str:
        """Return a readable label for server-rendered catalog controls."""
        return self.canonical.title()


@dataclass(frozen=True)
class SpecialChannel:
    """A non-team channel that should remain eligible for continuous search."""

    key: str
    name: str
    search_terms: Tuple[str, ...]
    logo_url: str = ""
    tvg_id: str = ""
    group_title: str = "24/7 Sports"


def normalize_team_label(value: str) -> str:
    """Normalize a team label without making short aliases more permissive."""
    normalized = str(value or "").casefold().replace("&", " and ")
    normalized = normalized.replace("â€™", "'")
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def _record(
    canonical: str,
    slug: str,
    aliases: Sequence[str],
    category: str,
    logo_sport: Optional[str] = None,
) -> TeamSlug:
    return TeamSlug(
        canonical=normalize_team_label(canonical),
        category=category,
        slug=str(slug),
        logo_sport=logo_sport or (COLLEGE_LOGO_SPORT if category == COLLEGE_RECORD_CATEGORY else category),
        aliases=tuple(normalize_team_label(alias) for alias in aliases if normalize_team_label(alias)),
        team_id=str(slug),
    )


MLB_TEAMS: Tuple[TeamSlug, ...] = tuple(
    _record(canonical, slug, aliases, MLB.key)
    for canonical, slug, aliases in (
        ("arizona diamondbacks", "ari", ("arizona", "diamondbacks", "dbacks", "ari")),
        ("atlanta braves", "atl", ("atlanta", "braves", "atl")),
        ("baltimore orioles", "bal", ("baltimore", "orioles", "bal")),
        ("boston red sox", "bos", ("boston", "red sox", "bos")),
        ("chicago cubs", "chc", ("chicago cubs", "cubs", "chc")),
        ("chicago white sox", "cws", ("chicago white sox", "white sox", "cws")),
        ("cincinnati reds", "cin", ("cincinnati", "reds", "cin")),
        ("cleveland guardians", "cle", ("cleveland", "guardians", "cle")),
        ("colorado rockies", "col", ("colorado", "rockies", "col")),
        ("detroit tigers", "det", ("detroit", "tigers", "det")),
        ("houston astros", "hou", ("houston", "astros", "hou")),
        ("kansas city royals", "kc", ("kansas city", "royals", "kc")),
        ("los angeles angels", "laa", ("los angeles angels", "angels", "laa")),
        ("los angeles dodgers", "lad", ("los angeles dodgers", "dodgers", "lad")),
        ("miami marlins", "mia", ("miami", "marlins", "fish", "mia")),
        ("milwaukee brewers", "mil", ("milwaukee", "brewers", "mil")),
        ("minnesota twins", "min", ("minnesota", "twins", "min")),
        ("new york mets", "nym", ("new york mets", "mets", "nym")),
        ("new york yankees", "nyy", ("new york yankees", "yankees", "nyy")),
        ("athletics", "oak", ("oakland athletics", "oakland", "sacramento", "sacramento athletics", "a's", "as", "oak")),
        ("philadelphia phillies", "phi", ("philadelphia", "phillies", "phi")),
        ("pittsburgh pirates", "pit", ("pittsburgh", "pirates", "pit")),
        ("san diego padres", "sd", ("san diego", "padres", "sd")),
        ("san francisco giants", "sf", ("san francisco", "giants", "sf")),
        ("seattle mariners", "sea", ("seattle", "mariners", "sea")),
        ("st louis cardinals", "stl", ("st louis", "cardinals", "stl")),
        ("tampa bay rays", "tb", ("tampa bay", "rays", "tb")),
        ("texas rangers", "tex", ("texas", "rangers", "tex")),
        ("toronto blue jays", "tor", ("toronto", "blue jays", "jays", "tor")),
        ("washington nationals", "wsh", ("washington", "nationals", "nats", "wsh")),
    )
)


NHL_TEAMS: Tuple[TeamSlug, ...] = tuple(
    _record(canonical, slug, aliases, NHL.key)
    for canonical, slug, aliases in (
        ("anaheim ducks", "ana", ("anaheim", "ducks", "ana")),
        ("boston bruins", "bos", ("boston", "bruins", "bos")),
        ("buffalo sabres", "buf", ("buffalo", "sabres", "buf")),
        ("calgary flames", "cgy", ("calgary", "flames", "cgy")),
        ("carolina hurricanes", "car", ("carolina", "hurricanes", "canes", "car")),
        ("chicago blackhawks", "chi", ("chicago", "blackhawks", "hawks", "chi")),
        ("colorado avalanche", "col", ("colorado", "avalanche", "avs", "col")),
        ("columbus blue jackets", "cbj", ("columbus", "blue jackets", "cbj")),
        ("dallas stars", "dal", ("dallas", "stars", "dal")),
        ("detroit red wings", "det", ("detroit", "red wings", "wings", "det")),
        ("edmonton oilers", "edm", ("edmonton", "oilers", "edm")),
        ("florida panthers", "fla", ("florida", "panthers", "cats", "fla")),
        ("los angeles kings", "lak", ("los angeles", "kings", "lak")),
        ("minnesota wild", "min", ("minnesota", "wild", "min")),
        ("montreal canadiens", "mtl", ("montreal", "canadiens", "habs", "mtl")),
        ("nashville predators", "nsh", ("nashville", "predators", "preds", "nsh")),
        ("new jersey devils", "nj", ("new jersey", "devils", "nj")),
        ("new york islanders", "nyi", ("new york islanders", "islanders", "nyi")),
        ("new york rangers", "nyr", ("new york rangers", "rangers", "nyr")),
        ("ottawa senators", "ott", ("ottawa", "senators", "sens", "ott")),
        ("philadelphia flyers", "phi", ("philadelphia", "flyers", "phi")),
        ("pittsburgh penguins", "pit", ("pittsburgh", "penguins", "pens", "pit")),
        ("san jose sharks", "sj", ("san jose", "sharks", "sj")),
        ("seattle kraken", "sea", ("seattle", "kraken", "sea")),
        ("st louis blues", "stl", ("st louis", "blues", "stl")),
        ("tampa bay lightning", "tb", ("tampa bay", "lightning", "bolts", "tb")),
        ("toronto maple leafs", "tor", ("toronto", "maple leafs", "leafs", "tor")),
        ("utah mammoth", "uta", ("utah", "mammoth", "utah hockey club", "uta")),
        ("vancouver canucks", "van", ("vancouver", "canucks", "van")),
        ("vegas golden knights", "vgk", ("vegas", "golden knights", "knights", "vgk")),
        ("washington capitals", "wsh", ("washington", "capitals", "caps", "wsh")),
        ("winnipeg jets", "wpg", ("winnipeg", "jets", "wpg")),
    )
)


_TV_LOGOS_BASE = "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/united-states/"

SPECIAL_CHANNELS: Tuple[SpecialChannel, ...] = (
    SpecialChannel(
        "nfl_redzone",
        "NFL RedZone",
        ("nfl redzone", "nfl red zone", "redzone", "red zone"),
        _TV_LOGOS_BASE + "nfl-red-zone-us.png",
        "NFLRedZone.us",
    ),
    SpecialChannel(
        "espn",
        "ESPN",
        ("espn",),
        _TV_LOGOS_BASE + "espn-us.png",
        "ESPN.us",
    ),
    SpecialChannel(
        "espn2",
        "ESPN2",
        ("espn2", "espn 2"),
        _TV_LOGOS_BASE + "espn-2-us.png",
        "ESPN2.us",
    ),
    SpecialChannel(
        "espnu",
        "ESPNU",
        ("espnu", "espn u"),
        _TV_LOGOS_BASE + "espn-u-us.png",
        "ESPNU.us",
    ),
    SpecialChannel(
        "fs1",
        "FOX Sports 1",
        ("fs1", "fox sports 1"),
        _TV_LOGOS_BASE + "fox-sports-1-us.png",
        "FoxSports1.us",
    ),
    SpecialChannel(
        "fs2",
        "FOX Sports 2",
        ("fs2", "fox sports 2"),
        _TV_LOGOS_BASE + "fox-sports-2-us.png",
        "FoxSports2.us",
    ),
    SpecialChannel(
        "cbs_sports_network",
        "CBS Sports Network",
        ("cbs sports network", "cbssn"),
        _TV_LOGOS_BASE + "cbs-sports-network-us.png",
        "CBSSportsNetwork.us",
    ),
    SpecialChannel(
        "tnt_sports",
        "TNT Sports",
        ("tnt sports", "tnt sports network", "tnt network", "tnt"),
        _TV_LOGOS_BASE + "tnt-us.png",
        "TNT.us",
    ),
    SpecialChannel(
        "nbc_sports",
        "NBC Sports",
        ("nbc sports", "nbc sports network", "nbcsn", "nbc sn"),
        _TV_LOGOS_BASE + "nbc-sports-us.png",
        "NBCSports.us",
    ),
    SpecialChannel(
        "big_ten_network",
        "Big Ten Network",
        ("big ten network", "btn"),
        _TV_LOGOS_BASE + "big-ten-network-us.png",
        "BigTenNetwork.us",
    ),
    SpecialChannel(
        "acc_network",
        "ACC Network",
        ("acc network", "accn"),
        _TV_LOGOS_BASE + "acc-network-us.png",
        "ACCNetwork.us",
    ),
    SpecialChannel(
        "sec_network",
        "SEC Network",
        ("sec network", "secn"),
        _TV_LOGOS_BASE + "sec-network-us.png",
        "SECNetwork.us",
    ),
    SpecialChannel(
        "longhorn_network",
        "Longhorn Network",
        ("longhorn network", "longhorn"),
        _TV_LOGOS_BASE + "longhorn-network-us.png",
        "LonghornNetwork.us",
    ),
    SpecialChannel(
        "mlb_network",
        "MLB Network",
        ("mlb network", "mlb tv"),
        _TV_LOGOS_BASE + "mlb-network-us.png",
        "MLBNetwork.us",
    ),
    SpecialChannel(
        "nba_tv",
        "NBA TV",
        ("nba tv", "nbatv"),
        _TV_LOGOS_BASE + "nba-tv-us.png",
        "NBATV.us",
    ),
    SpecialChannel(
        "nfl_network",
        "NFL Network",
        ("nfl network", "nfl net"),
        _TV_LOGOS_BASE + "nfl-network-us.png",
        "NFLNetwork.us",
    ),
    SpecialChannel(
        "nhl_network",
        "NHL Network",
        ("nhl network", "nhl net"),
        _TV_LOGOS_BASE + "nhl-network-us.png",
        "NHLNetwork.us",
    ),
    SpecialChannel(
        "abc",
        "ABC",
        ("abc", "abc network", "abc usa", "abc ny"),
        _TV_LOGOS_BASE + "abc-us.png",
        "ABC.us",
    ),
    SpecialChannel(
        "fox",
        "FOX",
        ("fox", "fox network", "fox usa", "fox broadcast"),
        _TV_LOGOS_BASE + "fox-us.png",
        "FOX.us",
    ),
    SpecialChannel(
        "cbs",
        "CBS",
        ("cbs", "cbs network", "cbs usa", "cbs broadcast"),
        _TV_LOGOS_BASE + "cbs-logo-white-us.png",
        "CBS.us",
    ),
    SpecialChannel(
        "nbc",
        "NBC",
        ("nbc", "nbc network", "nbc usa", "nbc broadcast"),
        _TV_LOGOS_BASE + "nbc-us.png",
        "NBC.us",
    ),
    SpecialChannel(
        "the_cw",
        "The CW",
        ("the cw", "cw", "cw network", "cw usa"),
        _TV_LOGOS_BASE + "the-cw-us.png",
        "CW.us",
    ),
    SpecialChannel(
        "tbs",
        "TBS",
        ("tbs", "tbs network", "tbs usa"),
        _TV_LOGOS_BASE + "tbs-us.png",
        "TBS.us",
    ),
    SpecialChannel(
        "usa_network",
        "USA Network",
        ("usa network", "usa net"),
        _TV_LOGOS_BASE + "usa-us.png",
        "USANetwork.us",
    ),
    SpecialChannel(
        "trutv",
        "TruTV",
        ("trutv", "tru tv", "trutv usa"),
        _TV_LOGOS_BASE + "tru-tv-us.png",
        "TruTV.us",
    ),
)


# ESPN's current pro abbreviations are stable and are safer logo fallbacks than
# deriving a slug from a display name. Generic nicknames remain in aliases so
# the matcher can use them only when they are unambiguous.
NFL_TEAMS: Tuple[TeamSlug, ...] = tuple(
    _record(canonical, slug, aliases, NFL.key)
    for canonical, slug, aliases in (
        ("arizona cardinals", "ari", ("arizona", "cardinals", "ari", "az")),
        ("atlanta falcons", "atl", ("atlanta", "falcons", "atl")),
        (" baltimore ravens", "bal", ("baltimore", "ravens", "bal")),
        ("buffalo bills", "buf", ("buffalo", "bills", "buf")),
        ("carolina panthers", "car", ("carolina", "panthers", "car")),
        ("chicago bears", "chi", ("chicago", "bears", "chi")),
        ("cincinnati bengals", "cin", ("cincinnati", "bengals", "cin")),
        ("cleveland browns", "cle", ("cleveland", "browns", "cle")),
        ("dallas cowboys", "dal", ("dallas", "cowboys", "dal")),
        ("denver broncos", "den", ("denver", "broncos", "den")),
        ("detroit lions", "det", ("detroit", "lions", "det")),
        ("green bay packers", "gb", ("green bay", "packers", "gb")),
        ("houston texans", "hou", ("houston", "texans", "hou")),
        ("indianapolis colts", "ind", ("indianapolis", "colts", "indy", "ind")),
        ("jacksonville jaguars", "jax", ("jacksonville", "jaguars", "jags", "jax")),
        ("kansas city chiefs", "kc", ("kansas city", "chiefs", "kc")),
        ("las vegas raiders", "lv", ("las vegas", "raiders", "lv")),
        ("los angeles chargers", "lac", ("los angeles", "la chargers", "chargers", "lac")),
        ("los angeles rams", "lar", ("los angeles", "la rams", "rams", "lar")),
        ("miami dolphins", "mia", ("miami dolphins", "dolphins", "fins", "mia")),
        ("minnesota vikings", "min", ("minnesota", "vikings", "min")),
        ("new england patriots", "ne", ("new england", "patriots", "pats", "ne")),
        ("new orleans saints", "no", ("new orleans", "saints", "no")),
        ("new york giants", "nyg", ("new york giants", "ny giants", "giants", "nyg")),
        ("new york jets", "nyj", ("new york jets", "ny jets", "jets", "nyj")),
        ("philadelphia eagles", "phi", ("philadelphia", "eagles", "phi")),
        ("pittsburgh steelers", "pit", ("pittsburgh", "steelers", "pit")),
        ("san francisco 49ers", "sf", ("san francisco", "49ers", "niners", "sf")),
        ("seattle seahawks", "sea", ("seattle", "seahawks", "sea")),
        ("tampa bay buccaneers", "tb", ("tampa bay", "buccaneers", "bucs", "tb")),
        ("tennessee titans", "ten", ("tennessee", "titans", "ten")),
        ("washington commanders", "wsh", ("washington", "commanders", "was", "wsh")),
    )
)

NBA_TEAMS: Tuple[TeamSlug, ...] = tuple(
    _record(canonical, slug, aliases, NBA.key)
    for canonical, slug, aliases in (
        ("atlanta hawks", "atl", ("atlanta", "hawks", "atl")),
        ("boston celtics", "bos", ("boston", "celtics", "bos")),
        ("brooklyn nets", "bkn", ("brooklyn", "nets", "bkn", "brk")),
        ("charlotte hornets", "cha", ("charlotte", "hornets", "cha")),
        ("chicago bulls", "chi", ("chicago", "bulls", "chi")),
        ("cleveland cavaliers", "cle", ("cleveland", "cavaliers", "cavs", "cle")),
        ("dallas mavericks", "dal", ("dallas", "mavericks", "mavs", "dal")),
        ("denver nuggets", "den", ("denver", "nuggets", "den")),
        ("detroit pistons", "det", ("detroit", "pistons", "det")),
        ("golden state warriors", "gs", ("golden state", "warriors", "gsw", "gs")),
        ("houston rockets", "hou", ("houston", "rockets", "hou")),
        ("indiana pacers", "ind", ("indiana", "pacers", "ind")),
        ("la clippers", "lac", ("la clippers", "los angeles clippers", "clippers", "lac")),
        ("los angeles lakers", "lal", ("los angeles lakers", "la lakers", "lakers", "lal")),
        ("memphis grizzlies", "mem", ("memphis", "grizzlies", "mem")),
        ("miami heat", "mia", ("miami heat", "heat", "mia")),
        ("milwaukee bucks", "mil", ("milwaukee", "bucks", "mil")),
        ("minnesota timberwolves", "min", ("minnesota", "timberwolves", "wolves", "min")),
        ("new orleans pelicans", "no", ("new orleans", "pelicans", "nop", "no")),
        ("new york knicks", "ny", ("new york", "knicks", "nyk", "ny")),
        ("oklahoma city thunder", "okc", ("oklahoma city", "thunder", "okc")),
        ("orlando magic", "orl", ("orlando", "magic", "orl")),
        ("philadelphia 76ers", "phi", ("philadelphia", "76ers", "sixers", "phi")),
        ("phoenix suns", "phx", ("phoenix", "suns", "phx", "pho")),
        ("portland trail blazers", "por", ("portland", "trail blazers", "blazers", "por")),
        ("sacramento kings", "sac", ("sacramento", "kings", "sac")),
        ("san antonio spurs", "sa", ("san antonio", "spurs", "sas", "sa")),
        ("toronto raptors", "tor", ("toronto", "raptors", "tor")),
        ("utah jazz", "utah", ("utah", "jazz")),
        ("washington wizards", "wsh", ("washington wizards", "wizards", "was", "wsh")),
    )
)


# NCAA IDs are shared by football and men's basketball on ESPN. These records
# are an offline fallback; the runtime directory refresh adds the complete
# season-specific NCAAF and NCAAM team lists when ESPN is reachable.
COLLEGE_TEAMS: Tuple[TeamSlug, ...] = tuple(
    _record(canonical, team_id, aliases, COLLEGE_RECORD_CATEGORY, COLLEGE_LOGO_SPORT)
    for canonical, team_id, aliases in (
        ("alabama crimson tide", "333", ("alabama", "crimson tide", "bama", "roll tide")),
        ("appalachian state mountaineers", "2026", ("appalachian state", "app state", "mountaineers")),
        ("arizona wildcats", "12", ("arizona", "wildcats", "ua")),
        ("arizona state sun devils", "9", ("arizona state", "arizona st", "sun devils", "asu")),
        ("arkansas razorbacks", "8", ("arkansas", "razorbacks", "hogs")),
        ("army black knights", "349", ("army", "black knights")),
        ("auburn tigers", "2", ("auburn", "tigers", "war eagle")),
        ("air force falcons", "2005", ("air force", "falcons")),
        ("baylor bears", "239", ("baylor", "bears")),
        ("boise state broncos", "68", ("boise state", "boise st", "broncos", "bsu")),
        ("boston college eagles", "103", ("boston college", "eagles", "bc")),
        ("byu cougars", "252", ("byu", "cougars")),
        ("california golden bears", "25", ("california", "cal", "golden bears")),
        ("cincinnati bearcats", "2132", ("cincinnati", "bearcats", "uc")),
        ("clemson tigers", "228", ("clemson", "tigers")),
        ("colorado buffaloes", "38", ("colorado", "buffaloes", "buffs", "cu")),
        ("connecticut huskies", "41", ("connecticut", "uconn", "huskies")),
        ("duke blue devils", "150", ("duke", "blue devils")),
        ("florida atlantic owls", "2226", ("florida atlantic", "fau", "owls")),
        ("florida gators", "57", ("florida gators", "gators", "uf", "university of florida")),
        ("florida international panthers", "2229", ("florida international", "fiu", "fiu panthers")),
        ("florida state seminoles", "52", ("florida state", "florida st", "seminoles", "noles", "fsu")),
        ("georgia bulldogs", "61", ("georgia", "bulldogs", "uga", "dawgs")),
        ("georgia tech yellow jackets", "59", ("georgia tech", "yellow jackets", "gt")),
        ("gonzaga bulldogs", "2250", ("gonzaga", "bulldogs")),
        ("houston cougars", "248", ("houston", "cougars", "uh")),
        ("illinois fighting illini", "356", ("illinois", "fighting illini", "illini", "uiuc")),
        ("indiana hoosiers", "84", ("indiana", "hoosiers", "iu")),
        ("iowa hawkeyes", "2294", ("iowa", "hawkeyes")),
        ("iowa state cyclones", "66", ("iowa state", "iowa st", "cyclones", "isu")),
        ("kansas jayhawks", "2305", ("kansas", "jayhawks", "ku")),
        ("kansas state wildcats", "2306", ("kansas state", "kansas st", "wildcats", "ksu")),
        ("kentucky wildcats", "96", ("kentucky", "wildcats", "uk")),
        ("louisville cardinals", "97", ("louisville", "cardinals", "cards")),
        ("lsu tigers", "99", ("lsu", "louisiana state", "tigers", "bayou bengals")),
        ("maryland terrapins", "120", ("maryland", "terrapins", "terps", "umd")),
        ("memphis tigers", "235", ("memphis", "tigers")),
        ("miami hurricanes", "2390", ("miami hurricanes", "hurricanes", "canes", "um", "miami fl")),
        ("michigan wolverines", "130", ("michigan wolverines", "wolverines", "u of m")),
        ("michigan state spartans", "127", ("michigan state", "michigan st", "spartans", "msu")),
        ("minnesota golden gophers", "135", ("minnesota", "golden gophers", "gophers")),
        ("mississippi state bulldogs", "344", ("mississippi state", "mississippi st", "msu", "bulldogs")),
        ("missouri tigers", "142", ("missouri", "mizzou", "tigers")),
        ("navy midshipmen", "2426", ("navy", "midshipmen")),
        ("nc state wolfpack", "152", ("nc state", "ncst", "wolfpack")),
        ("nebraska cornhuskers", "158", ("nebraska", "cornhuskers", "huskers")),
        ("north carolina tar heels", "153", ("north carolina", "tar heels", "unc")),
        ("northwestern wildcats", "77", ("northwestern", "wildcats")),
        ("notre dame fighting irish", "87", ("notre dame", "fighting irish", "irish", "nd")),
        ("ohio state buckeyes", "194", ("ohio state", "ohio st", "buckeyes", "osu")),
        ("oklahoma sooners", "201", ("oklahoma", "sooners", "ou")),
        ("oklahoma state cowboys", "197", ("oklahoma state", "oklahoma st", "cowboys", "ok state")),
        ("ole miss rebels", "145", ("ole miss", "mississippi", "rebels")),
        ("oregon ducks", "2483", ("oregon", "ducks", "uo")),
        ("penn state nittany lions", "213", ("penn state", "penn st", "nittany lions", "psu")),
        ("pittsburgh panthers", "221", ("pittsburgh", "pitt", "panthers")),
        ("purdue boilermakers", "2509", ("purdue", "boilermakers")),
        ("rutgers scarlet knights", "164", ("rutgers", "scarlet knights")),
        ("smu mustangs", "256", ("smu", "mustangs")),
        ("south carolina gamecocks", "2579", ("south carolina", "gamecocks", "sc")),
        ("stanford cardinal", "24", ("stanford", "cardinal")),
        ("syracuse orange", "183", ("syracuse", "orange", "cuse")),
        ("tcu horned frogs", "2628", ("tcu", "horned frogs")),
        ("temple owls", "218", ("temple", "owls")),
        ("tennessee volunteers", "2633", ("tennessee", "volunteers", "vols")),
        ("texas longhorns", "251", ("texas", "longhorns", "horns", "ut")),
        ("texas a&m aggies", "245", ("texas a&m", "texas am", "aggies", "tamu")),
        ("texas tech red raiders", "2641", ("texas tech", "red raiders", "ttu")),
        ("tulane green wave", "2655", ("tulane", "green wave")),
        ("ucla bruins", "26", ("ucla", "bruins")),
        ("ucf knights", "2116", ("ucf", "central florida", "knights", "ucf knights")),
        ("south florida bulls", "58", ("usf", "south florida", "bulls")),
        ("usc trojans", "30", ("usc", "southern california", "trojans")),
        ("utah utes", "254", ("utah", "utes")),
        ("vanderbilt commodores", "238", ("vanderbilt", "commodores", "vandy")),
        ("virginia cavaliers", "258", ("virginia", "cavaliers", "cavs", "uva")),
        ("virginia tech hokies", "259", ("virginia tech", "va tech", "hokies", "vt")),
        ("wake forest demon deacons", "154", ("wake forest", "demon deacons", "wake")),
        ("washington huskies", "264", ("washington", "huskies")),
        ("west virginia mountaineers", "277", ("west virginia", "mountaineers", "wvu")),
        ("wisconsin badgers", "275", ("wisconsin", "badgers", "uw")),
        ("xavier musketeers", "2752", ("xavier", "musketeers")),
    )
)

STATIC_TEAM_RECORDS: Tuple[TeamSlug, ...] = NFL_TEAMS + MLB_TEAMS + NHL_TEAMS + NBA_TEAMS + COLLEGE_TEAMS


def _identity_key(record: TeamSlug) -> Tuple[str, str, str]:
    return record.logo_sport, record.slug, record.canonical


def _record_aliases(record: TeamSlug) -> Tuple[str, ...]:
    return tuple(dict.fromkeys((record.canonical, *record.aliases)))


def build_team_index(records: Iterable[TeamSlug] = STATIC_TEAM_RECORDS) -> Dict[str, Tuple[TeamSlug, ...]]:
    index: Dict[str, List[TeamSlug]] = {}
    for record in records:
        for alias in _record_aliases(record):
            normalized = normalize_team_label(alias)
            if normalized:
                index.setdefault(normalized, []).append(record)
    return {
        alias: tuple({item for item in values})
        for alias, values in index.items()
    }


def build_static_slug_map() -> Dict[str, Tuple[str, str]]:
    """Build a compatibility map containing only unambiguous aliases."""
    result: Dict[str, Tuple[str, str]] = {}
    for alias, records in build_team_index().items():
        identities = {_identity_key(record) for record in records if record.slug}
        if len(identities) == 1:
            record = records[0]
            result[alias] = record.logo_sport, record.slug
    return result


def category_hint(value: str) -> str:
    normalized = normalize_team_label(value)
    if re.search(r"\b(?:nfl|pro football)\b", normalized):
        return NFL.key
    if re.search(r"\b(?:nba|pro basketball)\b", normalized):
        return NBA.key
    if re.search(r"\b(?:mlb|major league baseball|baseball)\b", normalized):
        return MLB.key
    if re.search(r"\b(?:nhl|pro hockey|hockey)\b", normalized):
        return NHL.key
    if re.search(r"\b(?:ncaaf|cfb|college football)\b", normalized):
        return NCAAF.key
    if re.search(r"\b(?:ncaam|mens college basketball|men college basketball|college basketball)\b", normalized):
        return NCAAM.key
    return ""


def _filter_category(records: Iterable[TeamSlug], category: str) -> List[TeamSlug]:
    if not category:
        return list(records)
    if category in COLLEGE_CATEGORIES:
        return [record for record in records if record.is_college]
    return [record for record in records if record.category == category]


def find_team_identity(
    value: str,
    category: str = "",
    index: Optional[Mapping[str, Sequence[TeamSlug]]] = None,
) -> Optional[TeamSlug]:
    """Find an identity only when the label is exact and unambiguous."""
    normalized = normalize_team_label(value)
    if not normalized:
        return None

    selected_category = category or category_hint(value)
    search_index = index or build_team_index()
    exact = _filter_category(search_index.get(normalized, ()), selected_category)
    candidates: List[TeamSlug] = list(exact)

    if not candidates:
        # Allow a sport suffix or surrounding display text, but choose only a
        # unique longest identity. This prevents "miami" from winning over
        # Miami Heat/Hurricanes/Dolphins.
        for alias, records in search_index.items():
            if len(alias) < 4 or not re.search(rf"\b{re.escape(alias)}\b", normalized):
                continue
            for record in _filter_category(records, selected_category):
                candidates.append(record)

    unique: Dict[Tuple[str, str, str], TeamSlug] = {
        _identity_key(record): record for record in candidates if record.slug
    }
    if not unique:
        return None

    if len(unique) > 1:
        ranked = sorted(
            unique.values(),
            key=lambda record: len(record.canonical),
            reverse=True,
        )
        best_length = len(ranked[0].canonical)
        best = [record for record in ranked if len(record.canonical) == best_length]
        if len(best) != 1:
            return None
        selected = best[0]
    else:
        selected = next(iter(unique.values()))

    if selected.is_college and selected_category in COLLEGE_CATEGORIES:
        return selected.for_category(selected_category)
    return selected


def _directory_team_values(data: Mapping[str, Any]) -> List[Mapping[str, Any]]:
    values: List[Mapping[str, Any]] = []
    direct = data.get("teams")
    if isinstance(direct, list):
        values.extend(item if isinstance(item, Mapping) else {} for item in direct)

    for sport in data.get("sports", []) if isinstance(data.get("sports"), list) else []:
        if not isinstance(sport, Mapping):
            continue
        leagues = sport.get("leagues", [])
        for league in leagues if isinstance(leagues, list) else []:
            if not isinstance(league, Mapping):
                continue
            teams = league.get("teams", [])
            if isinstance(teams, list):
                values.extend(item if isinstance(item, Mapping) else {} for item in teams)
    return values


def parse_espn_team_directory(data: Mapping[str, Any], category: str) -> Tuple[TeamSlug, ...]:
    """Convert the flexible ESPN directory response into catalog records."""
    records: Dict[Tuple[str, str, str], TeamSlug] = {}
    for item in _directory_team_values(data):
        team = item.get("team", item)
        if not isinstance(team, Mapping):
            continue
        display_name = str(team.get("displayName") or team.get("name") or "").strip()
        location = str(team.get("location") or "").strip()
        nickname = str(team.get("name") or "").strip()
        canonical = display_name or " ".join(part for part in (location, nickname) if part)
        team_id = str(team.get("id") or "").strip()
        slug = str(team.get("slug") or team_id).strip()
        if not canonical or not slug:
            continue

        aliases = {
            display_name,
            str(team.get("shortDisplayName") or ""),
            location,
            nickname,
            str(team.get("abbreviation") or ""),
            slug.replace("-", " "),
        }
        is_college = category in COLLEGE_CATEGORIES
        record = TeamSlug(
            canonical=normalize_team_label(canonical),
            category=COLLEGE_RECORD_CATEGORY if is_college else category,
            slug=slug,
            logo_sport=COLLEGE_LOGO_SPORT if is_college else category,
            aliases=tuple(sorted({normalize_team_label(alias) for alias in aliases if normalize_team_label(alias)})),
            team_id=team_id or slug,
        )
        records[_identity_key(record)] = record
    return tuple(records.values())


def merge_team_indexes(*indexes: Mapping[str, Sequence[TeamSlug]]) -> Dict[str, Tuple[TeamSlug, ...]]:
    merged: Dict[str, List[TeamSlug]] = {}
    for index in indexes:
        for alias, records in index.items():
            bucket = merged.setdefault(normalize_team_label(alias), [])
            for record in records:
                if record not in bucket:
                    bucket.append(record)
    return {alias: tuple(records) for alias, records in merged.items()}


# Both derived from the league registry (leagues.py); test_leagues.py freezes them.
ESPN_DIRECTORY_ENDPOINTS: Dict[str, Tuple[str, str]] = {
    league.key: (league.espn_sport, league.espn_league) for league in LEAGUES
}


# (start_month, start_day, end_month, end_day) for each sport's active window,
# covering preseason through the championship. A category with no entry here
# (e.g. manually added "custom" teams) is treated as always in season.
SEASON_WINDOWS: Dict[str, Tuple[int, int, int, int]] = {
    league.key: league.season_window for league in LEAGUES if league.season_window is not None
}




