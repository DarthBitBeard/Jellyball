"""Opt-in leagues (issues #45, #46): MLS, Premier League, Champions League, CFL,
PWHL and the UK always-live channels.

The contract: they are opt-in, they add nothing to the frozen catalog until a
group is turned on, and their names never change how an existing team or channel
matches ("Spurs", "Union", "Galaxy", "Inter", "TNT Sports").
"""

import asyncio
import functools
import json
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

import catalog
import db
import main
import optional_leagues
import routes_dashboard
import security
import scrapers
import sports_catalog
import sports_matcher
import state
from db import get_setting
from leagues import (
    CATALOG_CATEGORIES,
    OPTIONAL_GROUPS_SETTING,
    LEAGUES_BY_KEY,
    MOTORSPORT_GROUP_KEY,
    OPTIONAL_GROUP_KEYS,
    OPTIONAL_LEAGUES,
    UK_SPORTS_GROUP_KEY,
    parse_optional_groups,
    schedule_league,
)
from sports_catalog import OPTIONAL_SPECIAL_CHANNELS, SPECIAL_CHANNELS, STATIC_TEAM_RECORDS
from sports_matcher import get_team_search_terms, match_team

SEED_DIR = Path(__file__).resolve().parent / "league_seeds"
OPTIONAL_KEYS = {league.key for league in OPTIONAL_LEAGUES}


def _entries(groups):
    fetch = AsyncMock(return_value=())
    with patch.object(catalog, "_CATALOG_CACHE", {}), \
            patch.object(catalog, "_CATALOG_CACHE_LOADED_AT", 0.0), \
            patch.object(catalog, "_CATALOG_REMOTE_LOADED", False), \
            patch.object(catalog, "_fetch_espn_directory", fetch), \
            patch.object(state, "SHARED_HTTP_CLIENT", None):
        return asyncio.run(catalog.get_catalog_entries(groups))


class RegistryTests(unittest.TestCase):
    def test_keys_do_not_collide_with_the_frozen_leagues(self):
        keys = [league.key for league in OPTIONAL_LEAGUES]
        self.assertEqual(keys, ["mls", "epl", "ucl", "cfl", "pwhl"])
        self.assertFalse(set(keys) & set(LEAGUES_BY_KEY))
        self.assertFalse(set(keys) & set(CATALOG_CATEGORIES))
        self.assertEqual(OPTIONAL_GROUP_KEYS, (*keys, UK_SPORTS_GROUP_KEY, MOTORSPORT_GROUP_KEY))

    def test_parse_optional_groups_drops_unknown_and_orders(self):
        self.assertEqual(parse_optional_groups(" PWHL, nope ,epl,epl"), ("epl", "pwhl"))
        self.assertEqual(parse_optional_groups(""), ())
        self.assertEqual(parse_optional_groups(None), ())

    def test_seeds_are_well_formed_with_unique_frozen_ids(self):
        expected = {"mls": 30, "epl": 20, "ucl": 36, "cfl": 9, "pwhl": 12}
        for league in OPTIONAL_LEAGUES:
            with self.subTest(league=league.key):
                rows = json.loads((SEED_DIR / f"{league.key}.json").read_text(encoding="utf-8"))["teams"]
                ids = [row["id"] for row in rows]
                self.assertEqual(len(ids), expected[league.key])
                self.assertEqual(len(set(ids)), len(ids))
                self.assertEqual(len(optional_leagues.optional_records(league.key)), len(ids))
        # ESPN's own team ids: ids in a seed are part of channel ids forever.
        epl = {r.canonical: r.team_id for r in optional_leagues.optional_records("epl")}
        self.assertEqual(epl["arsenal"], "359")
        mls = {r.canonical: r.team_id for r in optional_leagues.optional_records("mls")}
        self.assertEqual(mls["inter miami cf"], "20232")
        self.assertEqual(mls["philadelphia union"], "10739")

    def test_schedule_support(self):
        for key in ("mls", "epl", "ucl"):
            league = schedule_league(key)
            self.assertTrue(league.has_schedule)
            self.assertEqual(dict(league.schedule_params), {"fixture": "true"})
        # ESPN has no PWHL and its CFL schedules stopped in 2022.
        for key in ("cfl", "pwhl"):
            self.assertFalse(schedule_league(key).has_schedule)

    def test_inter_miami_maps_to_inter_miami_cf(self):
        self.assertEqual(catalog._SPORT_SLUG_MAP["inter miami"], ("soccer", "20232"))


class CatalogTests(unittest.TestCase):
    def test_nothing_is_added_while_every_group_is_off(self):
        off = _entries(())
        self.assertEqual([e["catalog_key"] for e in off], [e["catalog_key"] for e in _entries(None)])
        self.assertFalse([
            e for e in off
            if e["category"] in OPTIONAL_KEYS or e["catalog_key"].startswith("special:uk_")
        ])

    def test_each_group_adds_only_its_own_entries_and_leaves_the_rest(self):
        base = {e["catalog_key"] for e in _entries(())}
        for group in OPTIONAL_GROUP_KEYS:
            with self.subTest(group=group):
                entries = _entries((group,))
                added = [e for e in entries if e["catalog_key"] not in base]
                self.assertTrue(added)
                self.assertTrue(base <= {e["catalog_key"] for e in entries})
                self.assertEqual({e["catalog_group"] for e in added}, {group})

    def test_enabled_groups_setting_controls_the_default_catalog(self):
        try:
            catalog.set_enabled_optional_groups("epl,uk_sports")
            groups = {e.get("catalog_group") for e in _entries(None)}
        finally:
            catalog.set_enabled_optional_groups("")
        self.assertEqual(groups - {None}, {"epl", "uk_sports"})

    def test_team_entries_and_ids(self):
        entries = {e["catalog_key"]: e for e in _entries(OPTIONAL_GROUP_KEYS)}
        epl, ucl = entries["team:epl:359"], entries["team:ucl:359"]
        self.assertEqual((epl["name"], ucl["name"]), ("Arsenal", "Arsenal (Champions League)"))
        self.assertNotEqual(epl["team_id"], ucl["team_id"])
        self.assertEqual(catalog._channel_group_title(epl), "English Premier League")
        self.assertEqual(catalog._channel_group_title(entries["team:cfl:79"]), "CFL")
        self.assertEqual(entries["team:pwhl:boston-fleet"]["logo_url"], "")  # no PWHL logo source
        ids = [e["team_id"] for e in entries.values()]
        self.assertEqual(len(ids), len(set(ids)))

    def test_season_windows_cover_the_new_leagues(self):
        self.assertTrue(catalog.is_in_season("epl", date(2026, 11, 5)))
        self.assertFalse(catalog.is_in_season("epl", date(2026, 7, 5)))
        self.assertFalse(catalog.is_in_season("cfl", date(2026, 2, 1)))
        self.assertTrue(catalog.is_in_season("pwhl", date(2026, 1, 15)))

    def test_default_catalog_has_exactly_the_frozen_leagues_and_special_channels(self):
        entries = _entries(())
        self.assertEqual(list(dict.fromkeys(e["category"] for e in entries)), [*CATALOG_CATEGORIES, "special"])
        self.assertEqual(
            [e["catalog_key"] for e in entries if e["category"] == "special"],
            [f"special:{c.key}" for c in SPECIAL_CHANNELS],
        )


class UkChannelTests(unittest.TestCase):
    def test_ids_and_names_are_unique_and_follow_the_conventions(self):
        us = {c.key for c in SPECIAL_CHANNELS} | {c.tvg_id for c in SPECIAL_CHANNELS} | {c.name for c in SPECIAL_CHANNELS}
        uk = list(OPTIONAL_SPECIAL_CHANNELS)
        for field in ("key", "tvg_id", "name"):
            values = [getattr(c, field) for c in uk]
            self.assertEqual(len(values), len(set(values)), field)
            self.assertFalse(set(values) & us, field)
        for channel in uk:
            self.assertEqual(channel.group_title, "UK Sports Channels")
            self.assertTrue(channel.key.startswith("uk_"))
            self.assertTrue(channel.tvg_id.endswith(".uk"))
        names = {c.name for c in uk}
        for required in ("Sky Sports Main Event", "Sky Sports Premier League", "Sky Sports Football",
                         "TNT Sports 1 UK", "TNT Sports 2 UK", "TNT Sports 3 UK", "TNT Sports 4 UK", "Premier Sports 1"):
            self.assertIn(required, names)

    def test_entries_are_always_live_special_channels_in_their_own_group(self):
        entries = [e for e in _entries((UK_SPORTS_GROUP_KEY,)) if e.get("catalog_group") == UK_SPORTS_GROUP_KEY]
        self.assertEqual(len(entries), len(OPTIONAL_SPECIAL_CHANNELS))
        for entry in entries:
            self.assertTrue(entry["always_live"])
            self.assertEqual(entry["content_type"], "channel")
            self.assertEqual(catalog._channel_group_title(entry), "UK Sports Channels")
            self.assertEqual(catalog._channel_tvg_id(entry["team_id"], entry), entry["tvg_id"])
            self.assertEqual(catalog.catalog_group_key(entry), UK_SPORTS_GROUP_KEY)

    def test_a_custom_channel_sharing_a_uk_name_keeps_its_own_metadata(self):
        custom = {"name": "Sky Sports Football", "query": "Sky Sports Football", "content_type": "channel"}
        self.assertIsNone(catalog._special_channel_for(custom))

    # Provider channel names as IPTV lists write them -> the one channel that may take them.
    PROVIDER_NAMES = {
        "Sky Sports Main Event": "uk_sky_sports_main_event",
        "UK: Sky Sports Main Event HD": "uk_sky_sports_main_event",
        "Sky Sports Premier League UK": "uk_sky_sports_premier_league",
        "Sky Sports Football": "uk_sky_sports_football",
        "Sky Sports F1 HD": "uk_sky_sports_f1",
        "Sky Sports News": "uk_sky_sports_news",
        "TNT Sports 1 UK": "uk_tnt_sports_1",
        "UK| TNT SPORTS 2 HD": "uk_tnt_sports_2",
        "TNT Sports 3": "uk_tnt_sports_3",
        "TNT Sports 4 UK": "uk_tnt_sports_4",
        "Premier Sports 1 HD": "uk_premier_sports_1",
        "Premier Sports 2": "uk_premier_sports_2",
    }

    def test_each_provider_name_matches_exactly_one_uk_channel(self):
        for name, key in self.PROVIDER_NAMES.items():
            with self.subTest(name=name):
                hits = [c.key for c in OPTIONAL_SPECIAL_CHANNELS
                        if scrapers._channel_term_matches(name, list(c.search_terms))]
                self.assertEqual(hits, [key])

    def test_pairwise_matrix_between_every_special_channel(self):
        every = [*SPECIAL_CHANNELS, *OPTIONAL_SPECIAL_CHANNELS]
        for owner in every:
            for other in every:
                if owner.key == other.key:
                    continue
                for name in (other.name, f"{other.name} HD", f"UK: {other.name}"):
                    with self.subTest(searching=owner.key, listed=name):
                        self.assertFalse(scrapers._channel_term_matches(name, list(owner.search_terms)))

    def test_every_channel_matches_its_own_name(self):
        for channel in (*SPECIAL_CHANNELS, *OPTIONAL_SPECIAL_CHANNELS):
            with self.subTest(channel=channel.key):
                self.assertTrue(scrapers._channel_term_matches(channel.name, list(channel.search_terms)))

    def test_us_tnt_still_matches_what_it_matched_before(self):
        tnt = next(c for c in SPECIAL_CHANNELS if c.key == "tnt_sports")
        for name in ("TNT", "TNT Sports", "TNT HD", "US: TNT Sports 1"):
            self.assertTrue(scrapers._channel_term_matches(name, list(tnt.search_terms)), name)
        for name in ("TNT Sports 1 UK", "UK: TNT Sports 2", "TNT Sports 4 UK HD"):
            self.assertFalse(scrapers._channel_term_matches(name, list(tnt.search_terms)), name)


class MatcherSafetyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # The matrix makes ~30k calls; the (static) ambiguity lookup dominates each.
        patcher = patch.object(sports_matcher, "_is_ambiguous_term", functools.lru_cache(maxsize=None)(sports_matcher._is_ambiguous_term))
        patcher.start()
        cls.addClassCleanup(patcher.stop)

        def row(record):
            return (record.display_name, record.category, record.team_id,
                    catalog._catalog_search_terms(record, record.team_id))

        cls.optional = [row(r) for league in OPTIONAL_LEAGUES for r in optional_leagues.optional_records(league.key)]
        # Every existing pro team that shares a word with an opt-in name (the
        # only ones that could collide), plus the nicknames the issues name.
        optional_words = {w for _, _, _, terms in cls.optional for t in terms for w in t.split() if len(w) >= 3}
        named = {"san antonio spurs", "philadelphia union", "los angeles galaxy"}
        cls.existing = [
            row(r) for r in STATIC_TEAM_RECORDS
            if not r.is_college and (r.canonical in named or optional_words & set(r.canonical.split()))
        ]

    def test_optional_names_never_join_the_shared_alias_table(self):
        names = {
            sports_catalog.normalize_team_label(n)
            for canonical, aliases in sports_matcher._TEAM_ALIASES_DB.items() for n in (canonical, *aliases)
        }
        for record in optional_leagues.OPTIONAL_TEAM_RECORDS:
            self.assertNotIn(record.canonical, names, record.canonical)
        self.assertEqual(sports_matcher._exact_canonical_matches("spurs"), {"san antonio spurs"})

    def test_pairwise_optional_teams_never_match_each_other(self):
        for name_a, cat_a, id_a, terms_a in self.optional:
            for name_b, cat_b, id_b, _ in self.optional:
                # One club is one team in the Premier League and the Champions League.
                if name_a == name_b or id_a == id_b and "mls" not in (cat_a, cat_b):
                    continue
                with self.subTest(searching=f"{cat_a}:{name_a}", title=f"{cat_b}:{name_b}"):
                    self.assertFalse(match_team(terms_a, f"{name_b} vs Wrexham AFC")[0])

    def test_optional_and_existing_teams_never_match_each_other(self):
        for first, second in ((self.optional, self.existing), (self.existing, self.optional)):
            for name_a, cat_a, _, terms_a in first:
                for name_b, cat_b, _, _ in second:
                    with self.subTest(searching=f"{cat_a}:{name_a}", title=f"{cat_b}:{name_b}"):
                        self.assertFalse(match_team(terms_a, f"{name_b} vs Wrexham AFC")[0])

    def test_each_optional_team_matches_its_own_titles(self):
        for name, category, _, terms in self.optional:
            for title in (f"{name} vs Wrexham AFC", f"Wrexham AFC at {name}", f"{name} - Live"):
                with self.subTest(team=f"{category}:{name}", title=title):
                    self.assertTrue(match_team(terms, title)[0])

    def test_accented_titles_match_plain_names(self):
        terms = next(t for _, c, i, t in self.optional if c == "pwhl" and i == "montreal-victoire")
        self.assertTrue(match_team(terms, "PWHL: Montréal Victoire vs Boston Fleet")[0])
        self.assertTrue(match_team(terms, "Montreal Victoire vs Boston Fleet")[0])

    def test_issue_named_ambiguities_stay_with_existing_teams(self):
        cases = [
            ("San Antonio Spurs", "San Antonio Spurs vs Boston Celtics", True),
            ("San Antonio Spurs", "Tottenham Hotspur vs Arsenal", False),
            ("Spurs", "Tottenham Hotspur vs Arsenal", False),
            ("Kansas City Chiefs", "Sporting Kansas City vs LA Galaxy", False),
            ("Kansas City Chiefs", "Kansas City Chiefs vs Buffalo Bills", True),
            ("Detroit Lions", "BC Lions at Edmonton Elks", False),
            ("Detroit Lions", "Detroit Lions vs Chicago Bears", True),
            ("Nashville Predators", "Nashville SC vs Orlando City SC", False),
            ("Nashville Predators", "Nashville Predators vs Dallas Stars", True),
        ]
        for query, title, expected in cases:
            with self.subTest(query=query, title=title):
                self.assertEqual(match_team(get_team_search_terms(query), title)[0], expected)

    def test_existing_matching_does_not_depend_on_the_optional_data(self):
        sample = [r for r in STATIC_TEAM_RECORDS if not r.is_college]

        def snapshot():
            return [
                (get_team_search_terms(r.canonical),
                 match_team(get_team_search_terms(r.canonical), f"{r.display_name} vs Wrexham AFC")[:1])
                for r in sample
            ]

        with_data = snapshot()
        with patch.object(sports_matcher, "_optional_team_name_data", return_value=(frozenset(), {}, None)):
            without = snapshot()
        self.assertEqual(with_data, without)
        self.assertTrue(all(hit == (True,) for _, hit in with_data))


class DashboardTests(unittest.TestCase):
    def setUp(self):
        db.init_db()

    def _call(self, method, path, **kwargs):
        async def go():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=main.app), base_url="http://127.0.0.1:8000", follow_redirects=False,
            ) as client:
                return await client.request(method, path, **kwargs)

        entries = AsyncMock(return_value=_entries(catalog.ENABLED_OPTIONAL_GROUPS))
        with patch.object(security, "DASHBOARD_PASSWORD", ""),                 patch.object(routes_dashboard, "get_catalog_entries", entries):
            return asyncio.run(go())

    def tearDown(self):
        catalog.set_enabled_optional_groups("")

    def test_toggling_groups_is_saved_and_changes_the_catalog_picker(self):
        page = self._call("GET", "/").text
        self.assertIn('id="optional-catalog-groups"', page)
        self.assertNotIn("<summary><span>UK Sports Channels</span>", page)
        self.assertNotIn("<summary><span>English Premier League</span>", page)

        response = self._call("POST", "/catalog/optional-groups", data={"groups": ["epl", "uk_sports", "bogus"]})
        self.assertEqual(response.status_code, 303)
        self.assertEqual(get_setting(OPTIONAL_GROUPS_SETTING, ""), "epl,uk_sports")
        page = self._call("GET", "/").text
        self.assertIn("<summary><span>UK Sports Channels</span>", page)
        self.assertIn("<summary><span>English Premier League</span>", page)
        self.assertNotIn("<summary><span>CFL</span>", page)

        self._call("POST", "/catalog/optional-groups", data={})
        self.assertEqual(get_setting(OPTIONAL_GROUPS_SETTING, ""), "")


if __name__ == "__main__":
    unittest.main()
