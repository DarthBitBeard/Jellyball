"""The league registry (leagues.py) must reproduce, exactly, the per-league
literals that were scattered across the code before it existed.

Every constant below is copied verbatim from the code as of 2.0.1 (catalog.py,
sports_catalog.py, routes_dashboard.py and catalog.fetch_espn_team_schedule).
Do not edit them to follow a registry change: a difference here is a behaviour
change (catalog order, M3U group titles, logo URLs, guide windows, ESPN
requests) and must be a deliberate decision.
"""

import asyncio
import unittest
from datetime import date, timedelta
from unittest.mock import AsyncMock, patch

import httpx

import catalog
import leagues
import main
import routes_dashboard
import security
import sports_catalog
import state
from sports_catalog import STATIC_TEAM_RECORDS, TeamSlug, category_hint


CATEGORY_ORDER = ("ncaaf", "ncaam", "nfl", "mlb", "nhl", "nba")

ESPN_DIRECTORY_ENDPOINTS = {
    "nfl": ("football", "nfl"),
    "ncaaf": ("football", "college-football"),
    "nba": ("basketball", "nba"),
    "ncaam": ("basketball", "mens-college-basketball"),
    "mlb": ("baseball", "mlb"),
    "nhl": ("hockey", "nhl"),
}

SEASON_WINDOWS = {
    "nfl": (8, 1, 2, 15),
    "ncaaf": (8, 1, 1, 22),
    "nba": (10, 1, 6, 30),
    "ncaam": (11, 1, 4, 10),
    "nhl": (9, 15, 6, 30),
    "mlb": (2, 15, 11, 10),
}

CATEGORY_GROUP_LABELS = {
    "ncaaf": "College Football",
    "ncaam": "College Basketball",
    "nfl": "NFL",
    "mlb": "MLB",
    "nhl": "NHL",
    "nba": "NBA",
}

COLLEGE_CATEGORY_LABELS = {
    "ncaaf": "Football",
    "ncaam": "Men's Basketball",
}

CATEGORY_LOGO_SPORTS = {
    "nfl": "nfl",
    "ncaaf": "ncaa",
    "ncaam": "ncaa",
    "nba": "nba",
    "mlb": "mlb",
    "nhl": "nhl",
}

# fetch_espn_team_schedule's api_map and duration_by_sport (default 3 h).
SCHEDULE_API_MAP = {
    "nfl": ("football", "nfl"),
    "ncaaf": ("football", "college-football"),
    "ncaam": ("basketball", "mens-college-basketball"),
    "ncaa": ("football", "college-football"),
    "nba": ("basketball", "nba"),
    "mlb": ("baseball", "mlb"),
    "nhl": ("hockey", "nhl"),
    "soccer": ("soccer", "usa.1")
}
DURATION_BY_SPORT = {
    "football": timedelta(hours=4),
    "basketball": timedelta(hours=3),
    "baseball": timedelta(hours=4),
    "hockey": timedelta(hours=3),
    "soccer": timedelta(hours=2.5),
}

DASHBOARD_CATALOG_GROUPS = (
    ("ncaaf", "College Football"),
    ("ncaam", "College Basketball"),
    ("nfl", "NFL"),
    ("mlb", "MLB"),
    ("nhl", "NHL"),
    ("nba", "NBA"),
    ("special", "Always-Live Sports Channels"),
)

# The `{"ncaaf", "ncaam"}` checks, and the categories whose ESPN directory is refreshed (in this order).
COLLEGE_CATEGORIES = ("ncaaf", "ncaam")


class RegistryShapeTests(unittest.TestCase):
    def test_exactly_the_six_catalog_leagues_in_catalog_order(self):
        self.assertEqual(tuple(league.key for league in leagues.LEAGUES), CATEGORY_ORDER)
        self.assertEqual(leagues.CATALOG_CATEGORIES, CATEGORY_ORDER)
        self.assertEqual(tuple(leagues.LEAGUES_BY_KEY), CATEGORY_ORDER)

    def test_college_categories(self):
        self.assertEqual(leagues.COLLEGE_CATEGORIES, COLLEGE_CATEGORIES)
        self.assertEqual(leagues.COLLEGE_RECORD_CATEGORY, "college")
        self.assertEqual(leagues.COLLEGE_LOGO_SPORT, "ncaa")

    def test_leagues_are_immutable(self):
        with self.assertRaises(AttributeError):
            leagues.NFL.group_title = "Pro Football"


class DerivedStructureTests(unittest.TestCase):
    def test_espn_directory_endpoints(self):
        self.assertEqual(sports_catalog.ESPN_DIRECTORY_ENDPOINTS, ESPN_DIRECTORY_ENDPOINTS)
        self.assertIs(catalog.ESPN_DIRECTORY_ENDPOINTS, sports_catalog.ESPN_DIRECTORY_ENDPOINTS)

    def test_season_windows(self):
        self.assertEqual(sports_catalog.SEASON_WINDOWS, SEASON_WINDOWS)
        self.assertIs(catalog.SEASON_WINDOWS, sports_catalog.SEASON_WINDOWS)
        self.assertTrue(catalog.is_in_season("nfl", date(2027, 2, 15)))
        self.assertFalse(catalog.is_in_season("nfl", date(2027, 2, 16)))
        self.assertTrue(catalog.is_in_season("custom", date(2027, 7, 1)))
        self.assertEqual(catalog._season_resume_label("nhl"), "Sep 15")

    def test_group_titles(self):
        self.assertEqual(catalog._CATEGORY_GROUP_LABELS, CATEGORY_GROUP_LABELS)
        for category, title in CATEGORY_GROUP_LABELS.items():
            self.assertEqual(catalog._channel_group_title({"category": category}), title)
        self.assertEqual(catalog._channel_group_title({"category": "custom"}), "Team Trackers")

    def test_college_name_labels(self):
        self.assertEqual(catalog._COLLEGE_CATEGORY_LABELS, COLLEGE_CATEGORY_LABELS)
        self.assertEqual(catalog._sport_labeled_name("Michigan Wolverines", "ncaaf"), "Michigan Wolverines (Football)")
        self.assertEqual(
            catalog._sport_labeled_name("Michigan Wolverines", "ncaam"), "Michigan Wolverines (Men's Basketball)",
        )
        self.assertEqual(catalog._sport_labeled_name("Buffalo Bills", "nfl"), "Buffalo Bills")

    def test_logo_sports(self):
        self.assertEqual(catalog._CATEGORY_LOGO_SPORTS, CATEGORY_LOGO_SPORTS)
        cdn = "https://a.espncdn.com/i/teamlogos"
        self.assertEqual(catalog.resolve_espn_logo("x", "ncaam", "130"), f"{cdn}/ncaa/500/130.png?v=titan2")
        self.assertEqual(catalog.resolve_espn_logo("x", "nhl", "bos"), f"{cdn}/nhl/500/bos.png?v=titan2")
        # Not a catalog category: falls back to the team-name map.
        self.assertEqual(catalog.resolve_espn_logo("Inter Miami", "soccer", "1"), f"{cdn}/soccer/500/10739.png?v=titan2")

    def test_schedule_endpoints_and_game_lengths(self):
        endpoints = {key: (lg.espn_sport, lg.espn_league) for key, lg in leagues.SCHEDULE_LEAGUES.items()}
        self.assertEqual(endpoints, SCHEDULE_API_MAP)
        for key, (sport, _) in SCHEDULE_API_MAP.items():
            with self.subTest(key=key):
                expected = DURATION_BY_SPORT.get(sport, timedelta(hours=3))
                self.assertEqual(leagues.SCHEDULE_LEAGUES[key].game_duration, expected)

    def test_non_catalog_schedule_keys_stay_out_of_the_catalog(self):
        for key in ("ncaa", "soccer"):
            with self.subTest(key=key):
                self.assertNotIn(key, leagues.CATALOG_CATEGORIES)
                self.assertNotIn(key, sports_catalog.ESPN_DIRECTORY_ENDPOINTS)
                self.assertNotIn(key, sports_catalog.SEASON_WINDOWS)
                self.assertNotIn(key, catalog._CATEGORY_GROUP_LABELS)
                self.assertNotIn(key, catalog._CATEGORY_LOGO_SPORTS)


class CollegeHandlingTests(unittest.TestCase):
    def test_which_team_categories_count_as_college(self):
        def slug(category):
            return TeamSlug(canonical="x", category=category, slug="1", logo_sport="x")

        for category in ("college", "ncaaf", "ncaam"):
            self.assertTrue(slug(category).is_college, category)
        for category in ("ncaa", "nfl", "nba", "mlb", "nhl", "soccer", "custom", ""):
            self.assertFalse(slug(category).is_college, category)
        self.assertEqual(slug("college").for_category("ncaam").category, "ncaam")
        self.assertEqual(slug("college").for_category("nfl").category, "college")
        self.assertEqual(slug("nfl").for_category("ncaaf").category, "nfl")

    def test_static_records(self):
        self.assertEqual({r.category for r in STATIC_TEAM_RECORDS}, {"college", "nfl", "mlb", "nhl", "nba"})
        for record in STATIC_TEAM_RECORDS:
            expected = "ncaa" if record.category == "college" else record.category
            self.assertEqual(record.logo_sport, expected, record.canonical)
        football = catalog._static_catalog_records("ncaaf")
        self.assertTrue(football and all(r.category == "ncaaf" for r in football))
        self.assertEqual(len(football), len(catalog._static_catalog_records("ncaam")))

    def test_espn_directory_parsing_files_college_teams_as_college(self):
        payload = {"sports": [{"leagues": [{"teams": [
            {"team": {"id": "130", "slug": "michigan-wolverines", "displayName": "Michigan Wolverines"}},
        ]}]}]}
        (college,) = sports_catalog.parse_espn_team_directory(payload, "ncaaf")
        self.assertEqual((college.category, college.logo_sport), ("college", "ncaa"))
        (pro,) = sports_catalog.parse_espn_team_directory(payload, "nfl")
        self.assertEqual((pro.category, pro.logo_sport), ("nfl", "nfl"))

    def test_category_hints(self):
        cases = {
            "NFL RedZone": "nfl", "pro basketball": "nba", "Major League Baseball": "mlb", "hockey night": "nhl",
            "college football": "ncaaf", "college basketball": "ncaam", "nfl and college football": "nfl",
            "soccer": "",
        }
        for text, expected in cases.items():
            self.assertEqual(category_hint(text), expected, text)


class CatalogOrderTests(unittest.TestCase):
    def test_catalog_entries_follow_the_category_order(self):
        fetch = AsyncMock(return_value=())
        with patch.object(catalog, "_CATALOG_CACHE", {}), \
                patch.object(catalog, "_CATALOG_CACHE_LOADED_AT", 0.0), \
                patch.object(catalog, "_CATALOG_REMOTE_LOADED", False), \
                patch.object(catalog, "_fetch_espn_directory", fetch), \
                patch.object(state, "SHARED_HTTP_CLIENT", None):
            entries = asyncio.run(catalog.get_catalog_entries())
        categories = [entry["category"] for entry in entries]
        self.assertEqual(tuple(dict.fromkeys(categories)), CATEGORY_ORDER + ("special",))
        # Specials come last, after every team entry.
        self.assertEqual(categories[-len(sports_catalog.SPECIAL_CHANNELS):], ["special"] * len(sports_catalog.SPECIAL_CHANNELS))
        # Only the college directories are fetched from ESPN, football first.
        self.assertEqual([call.args[0] for call in fetch.await_args_list], list(COLLEGE_CATEGORIES))

    def test_dashboard_catalog_sections_keep_their_titles_and_order(self):
        entries = [
            {"catalog_key": f"team:{category}:1", "name": f"Team {category}", "content_type": "team",
             "category": category, "always_live": False}
            for category in reversed(CATEGORY_ORDER)
        ]
        entries.append({"catalog_key": "special:espn", "name": "ESPN", "content_type": "channel",
                        "category": "special", "always_live": True})

        async def go():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=main.app), base_url="http://127.0.0.1:8000",
            ) as client:
                return await client.get("/")

        with patch.object(security, "DASHBOARD_PASSWORD", ""), \
                patch.object(routes_dashboard, "get_catalog_entries", AsyncMock(return_value=entries)):
            response = asyncio.run(go())
        self.assertEqual(response.status_code, 200)
        positions = [response.text.find(f"<summary><span>{title}</span>") for _, title in DASHBOARD_CATALOG_GROUPS]
        self.assertNotIn(-1, positions)
        self.assertEqual(positions, sorted(positions))


if __name__ == "__main__":
    unittest.main()
