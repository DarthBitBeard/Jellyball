"""3.2.0: network channels (ABC, CBS, NBC, FOX, ESPN, ...) follow the game ESPN
says they carry, and over-the-air networks show No Signal between games instead
of a random local station."""

import asyncio
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import failover
import network_games
import scrapers
from sports_matcher import REQUIRED_TEAMS, get_team_search_terms
from network_games import Game, game_for_network, parse_scoreboard

NOW = datetime(2026, 11, 7, 21, 0, tzinfo=timezone.utc)


def event(event_id, away, home, start, state, networks):
    return {
        "id": event_id, "name": f"{away} at {home}", "shortName": f"{away[:3].upper()} @ {home[:3].upper()}",
        "date": start.strftime("%Y-%m-%dT%H:%MZ"),
        "status": {"type": {"state": state}},
        "competitions": [{
            "competitors": [
                {"homeAway": "home", "team": {"displayName": home}},
                {"homeAway": "away", "team": {"displayName": away}},
            ],
            "broadcasts": [{"names": list(networks)}],
        }],
    }


SCOREBOARD = {"events": [
    event("1", "Alabama Crimson Tide", "LSU Tigers", NOW - timedelta(hours=1), "in", ["ABC", "Disney+"]),
    event("2", "Oregon Ducks", "Ohio State Buckeyes", NOW - timedelta(minutes=30), "in", ["CBS"]),
    event("3", "Miami Hurricanes", "Notre Dame Fighting Irish", NOW + timedelta(hours=3), "pre", ["NBC"]),
    event("4", "Georgia Bulldogs", "Ole Miss Rebels", NOW + timedelta(minutes=10), "pre", ["ABC"]),
    event("5", "BYU Cougars", "Utah Utes", NOW - timedelta(hours=4), "post", ["FOX"]),
    {"id": "bad"},  # malformed rows are skipped
]}


class ParseTests(unittest.TestCase):
    def test_scoreboard_rows(self):
        games = parse_scoreboard(SCOREBOARD, "ncaaf")
        self.assertEqual([g.event_id for g in games], ["1", "2", "3", "4", "5"])
        first = games[0]
        self.assertEqual((first.away, first.home, first.state, first.league), ("Alabama Crimson Tide", "LSU Tigers", "in", "ncaaf"))
        self.assertEqual(first.networks, ("ABC", "Disney+"))
        self.assertEqual(first.search_query, "Alabama Crimson Tide vs LSU Tigers")


class SelectionTests(unittest.TestCase):
    games = parse_scoreboard(SCOREBOARD, "ncaaf")

    def test_in_progress_game_on_the_network(self):
        self.assertEqual(game_for_network("cbs", self.games, NOW).event_id, "2")

    def test_live_beats_starting_soon_and_preferred_team_wins_among_live(self):
        self.assertEqual(game_for_network("abc", self.games, NOW).event_id, "1")
        later = NOW + timedelta(hours=2)  # game 1 still "in", game 4 now within its lead window too
        self.assertEqual(game_for_network("abc", self.games, later, preferred_teams=["LSU Tigers"]).event_id, "1")

    def test_pregame_lead_window(self):
        self.assertIsNone(game_for_network("nbc", self.games, NOW))  # 3 hours away
        self.assertEqual(game_for_network("nbc", self.games, NOW + timedelta(hours=2, minutes=45)).event_id, "3")

    def test_finished_games_are_ignored(self):
        self.assertIsNone(game_for_network("fox", self.games, NOW))

    def test_regional_games_prefer_my_teams(self):
        a = Game("10", "nfl", "Jets at Chiefs", "NYJ @ KC", "Kansas City Chiefs", "New York Jets", NOW, "in", ("CBS",))
        b = Game("11", "nfl", "Browns at Saints", "CLE @ NO", "New Orleans Saints", "Cleveland Browns", NOW, "in", ("CBS",))
        self.assertEqual(game_for_network("cbs", [a, b], NOW).event_id, "10")  # same start: stable order
        self.assertEqual(game_for_network("cbs", [a, b], NOW, preferred_teams=["New Orleans Saints"]).event_id, "11")

    def test_network_key_and_broadcast_flag(self):
        self.assertEqual(network_games.network_key({"catalog_key": "special:abc"}), "abc")
        self.assertIsNone(network_games.network_key({"catalog_key": "special:nfl_redzone"}))
        self.assertIsNone(network_games.network_key({"catalog_key": "team:nfl:kc"}))
        self.assertTrue(network_games.is_broadcast_network({"catalog_key": "special:nbc"}))
        self.assertFalse(network_games.is_broadcast_network({"catalog_key": "special:espn"}))

    def test_preferred_teams_come_from_team_channels(self):
        state = {
            "nfl_kc": {"name": "Kansas City Chiefs"},
            "ncaaf_99": {"name": "LSU Tigers (Football)"},
            "special_abc": {"name": "ABC", "always_live": True},
        }
        self.assertEqual(network_games.preferred_team_names(state), ["Kansas City Chiefs", "LSU Tigers"])


class ScrapeIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.state = {}
        self._patches = [
            patch.object(failover, "stream_state", self.state),
            patch.object(network_games, "_GAMES", parse_scoreboard(SCOREBOARD, "ncaaf")),
            patch.object(failover, "update_team_meta_async", AsyncMock()),
            patch.object(failover, "resolve_espn_logo", return_value=""),
        ]
        for p in self._patches:
            p.start()

    def tearDown(self):
        for p in reversed(self._patches):
            p.stop()

    def channel(self, key):
        self.state[f"special_{key}"] = {
            "name": key.upper(), "query": key.upper(), "catalog_key": f"special:{key}", "always_live": True,
            "candidates": [{"url": "https://local-news.example.test/live.m3u8", "provider": "IPTV-Org"}],
            "active_index": 0, "is_healthy": True,
        }
        return self.state[f"special_{key}"]

    def run_scrape(self, team_id, scrape_result):
        calls = []

        async def fake_master_scrape(query, **kwargs):
            calls.append((query, {**kwargs, "required_teams": REQUIRED_TEAMS.get()}))
            return list(scrape_result)

        with patch.object(failover, "master_scrape", fake_master_scrape), \
                patch.object(network_games, "datetime", wraps=datetime) as fake_dt:
            fake_dt.now.return_value = NOW
            asyncio.run(failover._trigger_scrape_unlocked(team_id, force=True))
        return calls

    def test_broadcast_network_with_a_game_searches_the_game_only(self):
        data = self.channel("cbs")
        stream = {"url": "https://event.example.test/osu.m3u8", "provider": "iSportSurge", "match_title": "Oregon vs Ohio State"}
        calls = self.run_scrape("special_cbs", [stream])
        self.assertEqual(len(calls), 1)
        query, kwargs = calls[0]
        self.assertEqual(query, "Oregon Ducks vs Ohio State Buckeyes")
        self.assertFalse(kwargs["always_live"])
        self.assertIn("buckeyes", kwargs["search_terms"])
        self.assertEqual(len(kwargs["required_teams"]), 2)  # both teams required
        self.assertEqual(REQUIRED_TEAMS.get(), ())  # and only inside the game search
        self.assertEqual(data["network_game_id"], "2")
        self.assertEqual([c["url"] for c in data["candidates"]], ["https://event.example.test/osu.m3u8"])  # local news dropped

    def test_broadcast_network_without_a_game_shows_no_signal(self):
        data = self.channel("fox")  # FOX's only game is final
        calls = self.run_scrape("special_fox", [])
        self.assertEqual(calls, [])
        self.assertEqual(data["candidates"], [])
        self.assertFalse(data["is_healthy"])

    def test_affiliate_fallback_keeps_the_linear_search(self):
        self.channel("fox")
        with patch.object(network_games, "AFFILIATE_FALLBACK", True):
            calls = self.run_scrape("special_fox", [])
        self.assertEqual(len(calls), 1)
        self.assertTrue(calls[0][1]["always_live"])

    def test_cable_network_searches_the_game_and_its_linear_stream(self):
        self.state["special_espn"] = {
            "name": "ESPN", "query": "ESPN", "catalog_key": "special:espn", "always_live": True,
            "candidates": [], "active_index": 0,
        }
        espn_game = Game("20", "ncaaf", "Texas A&M Aggies at South Carolina Gamecocks", "TA&M @ SC",
                         "South Carolina Gamecocks", "Texas A&M Aggies", NOW, "in", ("ESPN",))
        network_games._GAMES.append(espn_game)
        calls = self.run_scrape("special_espn", [])
        self.assertEqual(sorted(kwargs["always_live"] for _, kwargs in calls), [False, True])
        linear = next(kwargs for _, kwargs in calls if kwargs["always_live"])
        self.assertEqual(linear["required_teams"], ())  # the linear stream has no game teams to match


class BothTeamsTests(unittest.TestCase):
    """A game search only opens listings that name both teams."""

    HTML = (
        "<div><a href='/watch/oregon-vs-ohio-state'>Oregon Ducks vs Ohio State Buckeyes</a></div>"
        "<div><a href='/watch/ohio-state-vs-purdue'>Ohio State vs Purdue</a></div>"
        "<div><a href='/watch/oregon-state-vs-utah'>Oregon Ducks Classic Replay</a></div>"
        "<div><span>Ohio State Buckeyes at Oregon Ducks</span><a href='/watch/event/991'>Watch</a></div>"
    )

    def setUp(self):
        self.home = get_team_search_terms("Ohio State Buckeyes", "Ohio State Buckeyes")
        self.away = get_team_search_terms("Oregon Ducks", "Oregon Ducks")
        self.terms = list(dict.fromkeys([*self.home, *self.away]))
        self.scraper = scrapers.HtmlAggregatorScraper("TestAgg", "https://agg.example.test", ["/cfb"], ["/watch/"])

    def parse(self):
        matches = []
        self.scraper._parse_matches_from_html(self.HTML, "https://agg.example.test/cfb", self.terms, matches, set())
        return sorted(m[0].rsplit("/", 1)[-1] for m in matches)

    def test_without_requirement_either_team_matches(self):
        self.assertIn("ohio-state-vs-purdue", self.parse())

    def test_game_search_keeps_only_listings_naming_both_teams(self):
        token = REQUIRED_TEAMS.set((tuple(self.home), tuple(self.away)))
        try:
            found = self.parse()
        finally:
            REQUIRED_TEAMS.reset(token)
        self.assertEqual(found, ["991", "oregon-vs-ohio-state"])

    def test_event_filter_drops_one_team_streams(self):
        one = {"match_title": "Ohio State vs Purdue", "match_url": "https://agg.example.test/watch/ohio-state-vs-purdue"}
        both = {"match_title": "Watch", "match_url": "https://agg.example.test/watch/event/991",
                "match_context": "Ohio State Buckeyes at Oregon Ducks"}
        self.assertTrue(scrapers._stream_matches_requested_event(one, self.terms))
        token = REQUIRED_TEAMS.set((tuple(self.home), tuple(self.away)))
        try:
            self.assertFalse(scrapers._stream_matches_requested_event(one, self.terms))
            self.assertTrue(scrapers._stream_matches_requested_event(both, self.terms))
        finally:
            REQUIRED_TEAMS.reset(token)


if __name__ == "__main__":
    unittest.main()
