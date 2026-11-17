"""Matching stream titles for a motorsport series channel (Formula 1):
motorsport_terms' search terms and conflict rules, applied by
sports_matcher.match_team, and the scrape loop's per-round terms.

The titles follow the formats stream sites used for the 2026 São Paulo GP
weekend (written by hand; no site names), where Formula 1 is listed next to
MotoGP, Moto2 and Moto3 rounds and the next F1 round."""

import json
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

import failover
import leagues
import motorsport_schedule as ms
import motorsport_terms as mt
import scrapers
import state
from sports_matcher import clean_sports_text, match_team


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "espn"
SESSIONS = tuple(ms.parse_series_sessions(
    leagues.F1, json.loads((FIXTURES / "f1_scoreboard_two_weekends.json").read_text(encoding="utf-8"))))
SAO_PAULO_TERMS = mt.series_search_terms("f1", "São Paulo", "Sao Paulo", "Brazil")
SERIES_ONLY_TERMS = mt.series_search_terms("f1")

# (title, href) as sites list the São Paulo weekend's F1 event.
SAO_PAULO_EVENTS = [
    ("Formula 1 2026 - Brazil GP 06:07 ET - Nov/06/2026", "/live-event/formula-1-2026-brazil-gp-live-stream"),
    ("Formula 1 2026 - Brazil GP", "/event/formula-1/formula-1-2026-brazil-gp-live-streaming-links"),
    ("F1 Sao Paulo Grand Prix", "/game/f1-sao-paulo-grand-prix-0511061530"),
    ("São Paulo Grand Prix - Practice 1", "/game/sao-paulo-grand-prix-practice-1"),
    ("Brazilian GP: Qualifying", ""),
    ("Formula One - Qualifying", ""),
    ("Live from Interlagos", ""),
]
# Listed on the same pages, never the F1 channel's event during São Paulo.
OTHER_EVENTS = [
    ("MotoGP 2026 - Grand Prix of Qatar", "/live-event/motogp-2026-grand-prix-of-qatar-live-stream"),
    ("Moto2 2026 - Grand Prix of Qatar", "/title-game/moto2/moto2-2026-grand-prix-of-qatar"),
    ("Moto3 2026 - Grand Prix of Qatar", ""),
    ("Portugese Grand Prix Moto2", "/game/portugese-grand-prix-moto2-4289776943"),
    ("F1 Academy - Las Vegas", ""),
    ("Formula 2 Feature Race - Qatar", ""),
    ("F3 Sprint Race", ""),
    ("Formula E: Sao Paulo E-Prix", ""),
    ("NASCAR Cup Series Championship", ""),
    ("IndyCar - Grand Prix of Long Beach", ""),
    # The next F1 round, already listed.
    ("Formula 1 2026 - Las Vegas GP", "/stream/formula-1/formula-1-2026-las-vegas-gp-live-stream"),
    ("Las Vegas Grand Prix Practice 1", "/game/las-vegas-grand-prix-practice-1-7587742573"),
    # Team sports.
    ("Buffalo Bills vs New York Jets", "/nfl/bills-vs-jets"),
]


def matched(terms, title, href=""):
    return match_team(terms, title, href=href, title=title)[0]


class SeriesTermsTests(unittest.TestCase):
    def test_series_only_terms(self):
        self.assertEqual(set(SERIES_ONLY_TERMS), {"formula 1", "formula one", "f1"})
        self.assertEqual(mt.series_search_terms("indycar"), [])

    def test_round_terms(self):
        for term in ("formula 1", "f1", "brazil gp", "brazilian grand prix", "sao paulo gp",
                     "sao paulo grand prix", "interlagos"):
            with self.subTest(term=term):
                self.assertIn(term, SAO_PAULO_TERMS)
        self.assertEqual(SAO_PAULO_TERMS[0], "sao paulo grand prix")  # longest first

    def test_united_states_round_includes_usgp_and_cota(self):
        terms = mt.series_search_terms("f1", "United States", "Austin", "United States")
        for term in ("usgp", "us gp", "austin gp", "cota", "circuit of the americas"):
            self.assertIn(term, terms)

    def test_every_round_has_names(self):
        for place in mt.F1_ROUNDS:
            with self.subTest(place=place):
                names, _ = mt.round_names(place)
                self.assertIn(f"{mt.fold(place)} gp", names)

    def test_fold(self):
        self.assertEqual(mt.fold("São Paulo"), "sao paulo")
        self.assertEqual(clean_sports_text("São Paulo Grand Prix"), "sao paulo grand prix")


class SeriesMatchingTests(unittest.TestCase):
    def test_the_current_round_matches(self):
        for title, href in SAO_PAULO_EVENTS:
            with self.subTest(title=title):
                self.assertTrue(matched(SAO_PAULO_TERMS, title, href))

    def test_other_series_rounds_and_sports_do_not(self):
        for title, href in OTHER_EVENTS:
            with self.subTest(title=title):
                self.assertFalse(matched(SAO_PAULO_TERMS, title, href))

    def test_a_split_title_cannot_hide_another_round(self):
        # "formula 1 2026" alone matches; the "las vegas gp" side must still reject it.
        self.assertTrue(matched(SAO_PAULO_TERMS, "Formula 1 2026"))
        self.assertFalse(matched(SAO_PAULO_TERMS, "Formula 1 2026 - Las Vegas GP"))

    def test_a_title_naming_both_rounds_matches(self):
        self.assertTrue(matched(SAO_PAULO_TERMS, "F1: Brazil GP highlights, Las Vegas GP preview"))

    def test_without_a_known_round_any_f1_round_matches(self):
        self.assertTrue(matched(SERIES_ONLY_TERMS, "Formula 1 2026 - Las Vegas GP"))
        self.assertTrue(matched(SERIES_ONLY_TERMS, "Formula 1 2026 - Brazil GP"))
        self.assertFalse(matched(SERIES_ONLY_TERMS, "MotoGP 2026 - Grand Prix of Qatar"))
        self.assertFalse(matched(SERIES_ONLY_TERMS, "F1 Academy - Las Vegas"))

    def test_the_final_event_check_rejects_another_round(self):
        right = {"match_title": "Formula 1 2026 - Brazil GP", "match_url": "/live-event/formula-1-2026-brazil-gp"}
        wrong = {"match_title": "Formula 1 2026 - Las Vegas GP", "match_url": "/live-event/formula-1-2026-las-vegas-gp"}
        self.assertTrue(scrapers._stream_matches_requested_event(right, SAO_PAULO_TERMS))
        self.assertFalse(scrapers._stream_matches_requested_event(wrong, SAO_PAULO_TERMS))

    def test_team_searches_are_unaffected(self):
        bills = ["buffalo bills", "bills"]
        self.assertFalse(mt.has_series_conflict(bills, "buffalo bills vs new york jets f2"))
        self.assertTrue(matched(bills, "Buffalo Bills vs New York Jets", "/nfl/bills-vs-jets"))
        self.assertFalse(matched(bills, "Formula 1 2026 - Brazil GP"))


class CurrentRoundTermsTests(unittest.TestCase):
    def setUp(self):
        ms._SESSIONS_CACHE.clear()
        ms._LAST_GOOD_SESSIONS.clear()
        self.addCleanup(ms._SESSIONS_CACHE.clear)
        self.addCleanup(ms._LAST_GOOD_SESSIONS.clear)

    def prime(self):
        ms._SESSIONS_CACHE["f1"] = (time.monotonic() + 3600, (SESSIONS, True))

    def test_sessions_know_their_round(self):
        self.assertEqual({s.place for s in SESSIONS}, {"São Paulo", "Las Vegas"})
        self.assertEqual(ms.round_place("Qatar Airways Qatar Grand Prix"), "Qatar")
        self.assertEqual(ms.round_place("Season Finale"), "")

    def test_terms_follow_the_current_round(self):
        self.prime()
        during = ms.current_search_terms("f1", datetime(2026, 11, 7, 18, 30, tzinfo=timezone.utc))
        after = ms.current_search_terms("f1", datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc))
        self.assertIn("brazil gp", during)
        self.assertNotIn("vegas gp", during)
        self.assertIn("vegas gp", after)
        self.assertNotIn("brazil gp", after)

    def test_before_any_lookup_only_the_series_names(self):
        self.assertEqual(set(ms.current_search_terms("f1")), {"formula 1", "formula one", "f1"})

    def test_scrape_loop_uses_round_terms_for_series_channels_only(self):
        self.prime()
        f1 = state.new_channel_state(name="Formula 1", query="Formula 1", category="f1",
                                     search_terms=["formula 1"])
        bills = state.new_channel_state(name="Buffalo Bills", query="Bills", category="nfl",
                                        search_terms=["buffalo bills", "bills"])
        f1_terms = failover._channel_search_terms(f1)
        self.assertIn("f1", f1_terms)
        self.assertGreater(len(f1_terms), 3)  # plus the current or next round's names
        self.assertEqual(failover._channel_search_terms(bills), ["buffalo bills", "bills"])
        self.assertIsNone(failover._channel_search_terms(state.new_channel_state(name="X", query="X")))


if __name__ == "__main__":
    unittest.main()
