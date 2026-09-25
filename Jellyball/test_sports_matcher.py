"""
Unit tests for sports_matcher.py.

sports_matcher.py had no dedicated tests before this file. It covers the
identity-aware team search the README advertises: "Rejects near-name
collisions such as Eastern Michigan for Michigan State, Rays for
Buccaneers, and Mets for Jets while retaining known aliases."

It also covers the D4 same-market conflict groups (Los Angeles Rams vs.
Los Angeles Lakers, New York Mets vs. New York Yankees, Chicago Bears vs.
Chicago Cubs, ...), derived once from the shared catalog instead of being
hand-maintained.
"""

import unittest

from sports_catalog import STATIC_TEAM_RECORDS, normalize_team_label
from sports_matcher import (
    canonical_team_name,
    clean_sports_text,
    get_team_search_terms,
    has_compound_conflict,
    has_team_identity_conflict,
    is_state_school_conflict,
    match_team,
    _is_ambiguous_term,
    _market_key,
    _ALL_IDENTITY_FAMILIES,
    _MARKET_IDENTITY_FAMILIES,
    _TEAM_IDENTITY_FAMILIES,
)


class CollisionGuardTests(unittest.TestCase):
    """The three README-advertised collisions, plus alias retention."""

    def test_eastern_michigan_does_not_match_michigan_state_query(self):
        terms = get_team_search_terms("Michigan State Spartans", "Michigan State Spartans")
        matched, score, term = match_team(terms, "Eastern Michigan Eagles vs Toledo")
        self.assertFalse(matched, f"unexpectedly matched with term={term!r} score={score}")

    def test_rays_does_not_match_buccaneers_query(self):
        terms = get_team_search_terms("Tampa Bay Buccaneers", "Tampa Bay Buccaneers")
        matched, score, term = match_team(terms, "Tampa Bay Rays vs New York Yankees")
        self.assertFalse(matched, f"unexpectedly matched with term={term!r} score={score}")

    def test_mets_does_not_match_jets_query(self):
        terms = get_team_search_terms("New York Jets", "New York Jets")
        matched, score, term = match_team(terms, "New York Mets vs Atlanta Braves")
        self.assertFalse(matched, f"unexpectedly matched with term={term!r} score={score}")

    def test_buccaneers_still_match_bucs_and_tampa_bay_text(self):
        terms = get_team_search_terms("Tampa Bay Buccaneers", "Tampa Bay Buccaneers")
        matched, _, _ = match_team(terms, "Bucs vs Falcons")
        self.assertTrue(matched)

    def test_jets_still_match_bare_jets_text(self):
        terms = get_team_search_terms("New York Jets", "New York Jets")
        matched, _, _ = match_team(terms, "Jets vs Dolphins")
        self.assertTrue(matched)

    def test_michigan_state_still_matches_spartans_alias(self):
        terms = get_team_search_terms("Michigan State Spartans", "Michigan State Spartans")
        matched, _, _ = match_team(terms, "Spartans vs Wolverines")
        # Michigan Wolverines is itself a competing group member of the
        # Michigan State family, so this title mentions two rival teams;
        # the guard only rejects the OTHER team's name, not the requested
        # team's own name appearing anywhere in the text.
        self.assertTrue(matched)


class KnownAliasMatchTests(unittest.TestCase):
    """Known aliases should keep resolving to their canonical identity."""

    def test_bucs_resolves_to_tampa_bay_buccaneers(self):
        self.assertEqual(canonical_team_name("Bucs"), "tampa bay buccaneers")

    def test_fins_resolves_to_miami_dolphins(self):
        self.assertEqual(canonical_team_name("Fins"), "miami dolphins")

    def test_niners_resolves_to_san_francisco_49ers(self):
        self.assertEqual(canonical_team_name("Niners"), "san francisco 49ers")

    def test_bronx_bombers_resolves_to_new_york_yankees(self):
        self.assertEqual(canonical_team_name("Bronx Bombers"), "new york yankees")

    def test_noles_resolves_to_florida_state_seminoles(self):
        self.assertEqual(canonical_team_name("Noles"), "florida state seminoles")

    def test_athletics_aliases_resolve_through_relocation_names(self):
        # D5: the franchise renamed itself "Athletics" in 2025 (Sacramento,
        # with a planned Las Vegas move); old and new city references must
        # both keep resolving to the same identity.
        for alias in ("Athletics", "Oakland Athletics", "Oakland", "Sacramento Athletics", "A's"):
            with self.subTest(alias=alias):
                self.assertEqual(canonical_team_name(alias), "athletics")

    def test_bare_sacramento_stays_ambiguous_with_the_kings(self):
        # Bare "Sacramento" is genuinely ambiguous: the Sacramento Kings
        # (NBA) have used it as a bare alias for decades, and the Athletics
        # only started playing there in 2025. canonical_team_name() must not
        # silently guess one over the other; it currently favors the
        # long-established, fully-formed "Sacramento Kings" match. Callers
        # that mean the ballclub should say "Sacramento Athletics" instead.
        self.assertEqual(canonical_team_name("Sacramento"), "sacramento kings")


class ConflictHelperTests(unittest.TestCase):
    """Direct tests of the conflict/guard helper functions."""

    def test_has_team_identity_conflict_true_for_hand_listed_family(self):
        terms = ["tampa bay buccaneers", "buccaneers", "bucs"]
        self.assertTrue(has_team_identity_conflict(terms, "tampa bay rays vs orioles"))

    def test_has_team_identity_conflict_false_when_target_is_own_team(self):
        terms = ["tampa bay buccaneers", "buccaneers", "bucs"]
        self.assertFalse(has_team_identity_conflict(terms, "tampa bay buccaneers vs saints"))

    def test_has_team_identity_conflict_false_for_unrelated_target(self):
        terms = ["tampa bay buccaneers", "buccaneers", "bucs"]
        self.assertFalse(has_team_identity_conflict(terms, "cowboys vs eagles"))

    def test_has_compound_conflict_rejects_scarlet_knights_for_bare_knights_query(self):
        search_terms = ["knights"]
        self.assertTrue(has_compound_conflict("knights", "rutgers scarlet knights vs temple", search_terms))

    def test_has_compound_conflict_allows_when_search_terms_name_the_compound(self):
        search_terms = ["scarlet knights", "rutgers"]
        self.assertFalse(has_compound_conflict("knights", "rutgers scarlet knights vs temple", search_terms))

    def test_has_compound_conflict_false_for_unrelated_term(self):
        self.assertFalse(has_compound_conflict("bears", "chicago bears vs packers", ["bears"]))

    def test_is_state_school_conflict_blocks_florida_matching_florida_state(self):
        self.assertTrue(is_state_school_conflict("florida", "florida state seminoles vs clemson"))

    def test_is_state_school_conflict_blocks_michigan_matching_michigan_state(self):
        self.assertTrue(is_state_school_conflict("michigan", "michigan state spartans vs iowa"))

    def test_is_state_school_conflict_allows_florida_state_query_itself(self):
        # The guard only exists to stop the *bare* state name from bleeding
        # into the "State" school; a query that already says "state" is exempt.
        self.assertFalse(is_state_school_conflict("florida state", "florida state seminoles vs clemson"))

    def test_is_state_school_conflict_false_for_unrelated_school(self):
        self.assertFalse(is_state_school_conflict("florida", "florida gators vs georgia"))


class AmbiguousTermTests(unittest.TestCase):
    """_is_ambiguous_term should flag nicknames shared by multiple identities."""

    def test_tigers_is_ambiguous(self):
        # Clemson, LSU, Auburn, Missouri, and Memphis all use "Tigers".
        self.assertTrue(_is_ambiguous_term("tigers"))

    def test_panthers_is_ambiguous(self):
        # Carolina Panthers, Florida Panthers, Pittsburgh Panthers, FIU Panthers.
        self.assertTrue(_is_ambiguous_term("panthers"))

    def test_gators_is_not_ambiguous(self):
        self.assertFalse(_is_ambiguous_term("gators"))

    def test_dolphins_is_not_ambiguous(self):
        self.assertFalse(_is_ambiguous_term("dolphins"))

    def test_ambiguous_term_is_excluded_from_search_matching(self):
        # An ambiguous alias must never be trusted as a search term on its
        # own; match_team() skips it entirely rather than guess a winner.
        terms = ["tigers"]
        matched, score, term = match_team(terms, "clemson tigers vs south carolina")
        self.assertFalse(matched)
        self.assertEqual(score, 0)


class MatchTeamAcrossLeaguesTests(unittest.TestCase):
    """Positive and negative match_team cases spanning every league."""

    def test_nfl_positive_and_negative(self):
        terms = get_team_search_terms("Miami Dolphins", "Miami Dolphins")
        matched, _, _ = match_team(terms, "Miami Dolphins vs Buffalo Bills")
        self.assertTrue(matched)
        matched, _, _ = match_team(terms, "Miami Marlins vs Atlanta Braves")
        self.assertFalse(matched)

    def test_mlb_positive_and_negative(self):
        terms = get_team_search_terms("New York Yankees", "New York Yankees")
        matched, _, _ = match_team(terms, "Yankees vs Red Sox")
        self.assertTrue(matched)
        matched, _, _ = match_team(terms, "New York Mets vs Atlanta Braves")
        self.assertFalse(matched)

    def test_nba_positive_and_negative(self):
        terms = get_team_search_terms("Los Angeles Lakers", "Los Angeles Lakers")
        matched, _, _ = match_team(terms, "Lakers vs Celtics")
        self.assertTrue(matched)
        matched, _, _ = match_team(terms, "LA Clippers vs Suns")
        self.assertFalse(matched)

    def test_nhl_positive_and_negative(self):
        terms = get_team_search_terms("Boston Bruins", "Boston Bruins")
        matched, _, _ = match_team(terms, "Boston Bruins vs New York Rangers")
        self.assertTrue(matched)
        matched, _, _ = match_team(terms, "Boston Celtics vs Miami Heat")
        self.assertFalse(matched)

    def test_college_positive_and_negative(self):
        terms = get_team_search_terms("Florida Gators", "Florida Gators")
        matched, _, _ = match_team(terms, "Florida Gators vs Georgia Bulldogs")
        self.assertTrue(matched)
        matched, _, _ = match_team(terms, "Florida State Seminoles vs Clemson")
        self.assertFalse(matched)


class MarketConflictGroupTests(unittest.TestCase):
    """D4: same-market identity-conflict groups derived from the catalog."""

    def test_market_key_recognizes_multi_word_and_single_word_cities(self):
        self.assertEqual(_market_key("los angeles rams"), "los angeles")
        self.assertEqual(_market_key("chicago bears"), "chicago")
        self.assertEqual(_market_key("rams"), None)

    def test_market_key_synonyms_unify_branding_mismatches(self):
        # Vegas Golden Knights and Las Vegas Raiders share one market...
        self.assertEqual(_market_key("vegas golden knights"), "las vegas")
        # ...and Brooklyn Nets join the rest of the New York teams.
        self.assertEqual(_market_key("brooklyn nets"), "new york")

    def test_derived_groups_are_computed_once_and_combined_with_hand_listed(self):
        # Computed once at import time into a plain (immutable) tuple, not
        # recomputed per call.
        self.assertIsInstance(_MARKET_IDENTITY_FAMILIES, tuple)
        self.assertGreater(len(_MARKET_IDENTITY_FAMILIES), 0)
        # _ALL_IDENTITY_FAMILIES combines the hand-listed exceptions (special
        # cases like Michigan vs Michigan State) with the derived groups.
        self.assertEqual(
            len(_ALL_IDENTITY_FAMILIES),
            len(_TEAM_IDENTITY_FAMILIES) + len(_MARKET_IDENTITY_FAMILIES),
        )

    def test_los_angeles_lakers_does_not_match_los_angeles_rams_query(self):
        terms = get_team_search_terms("Los Angeles Rams", "Los Angeles Rams")
        matched, score, term = match_team(terms, "Los Angeles Lakers vs Boston Celtics")
        self.assertFalse(matched, f"unexpectedly matched with term={term!r} score={score}")

    def test_new_york_mets_does_not_match_new_york_yankees_query(self):
        terms = get_team_search_terms("New York Yankees", "New York Yankees")
        matched, score, term = match_team(terms, "New York Mets vs Atlanta Braves")
        self.assertFalse(matched, f"unexpectedly matched with term={term!r} score={score}")

    def test_chicago_cubs_does_not_match_chicago_bears_query(self):
        terms = get_team_search_terms("Chicago Bears", "Chicago Bears")
        matched, score, term = match_team(terms, "Chicago Cubs vs St Louis Cardinals")
        self.assertFalse(matched, f"unexpectedly matched with term={term!r} score={score}")

    def test_pittsburgh_pirates_does_not_match_pittsburgh_steelers_query(self):
        terms = get_team_search_terms("Pittsburgh Steelers", "Pittsburgh Steelers")
        matched, score, term = match_team(terms, "Pittsburgh Pirates vs Cincinnati Reds")
        self.assertFalse(matched, f"unexpectedly matched with term={term!r} score={score}")

    def test_buffalo_bills_and_sabres_share_an_abbreviation_without_conflict(self):
        # Regression guard: Buffalo Bills and Buffalo Sabres both use "buf"
        # as a generic city abbreviation in the catalog. A shared value like
        # that must not falsely flag a team against *itself* -- it should be
        # dropped from both teams' distinguishing variants instead.
        terms = get_team_search_terms("Buffalo Bills", "Buffalo Bills")
        matched, score, term = match_team(terms, "Buffalo Bills vs Pittsburgh Steelers")
        self.assertTrue(matched, f"expected a self-match, got score={score} term={term!r}")
        # And the market guard should still catch the real conflict.
        matched, score, term = match_team(terms, "Buffalo Sabres vs Boston Bruins")
        self.assertFalse(matched, f"unexpectedly matched with term={term!r} score={score}")

    def test_la_rams_and_bare_rams_still_match_the_rams(self):
        terms = get_team_search_terms("Los Angeles Rams", "Los Angeles Rams")
        matched, _, _ = match_team(terms, "LA Rams vs San Francisco 49ers")
        self.assertTrue(matched)
        matched, _, _ = match_team(terms, "Rams vs 49ers")
        self.assertTrue(matched)

    def test_bare_market_city_alone_does_not_trigger_the_conflict_guard(self):
        # Naming only the city, with no team-distinguishing word on either
        # side, must not trigger OR block anything through this mechanism:
        # neither side's specific name variant is present in the text, so
        # has_team_identity_conflict() stays out of the decision either way.
        terms = ["los angeles rams", "la rams", "rams", "lar"]
        self.assertFalse(has_team_identity_conflict(terms, "los angeles vs boston"))

    def test_bare_market_city_title_keeps_todays_match_team_behavior(self):
        # match_team()'s outcome for a bare-city title (no mascot for either
        # side) is governed by the base fuzzy scorer, not by the identity
        # guard -- D4 must not change it. Confirm the full pipeline's result
        # is identical whether or not the derived market families are used.
        terms = get_team_search_terms("Los Angeles Rams", "Los Angeles Rams")
        text = "Los Angeles vs Boston"

        with_derived_groups = match_team(terms, text)

        import sports_matcher as sm
        original = sm._ALL_IDENTITY_FAMILIES
        try:
            sm._ALL_IDENTITY_FAMILIES = sm._TEAM_IDENTITY_FAMILIES
            without_derived_groups = match_team(terms, text)
        finally:
            sm._ALL_IDENTITY_FAMILIES = original

        self.assertEqual(with_derived_groups, without_derived_groups)


class CatalogCompletenessTests(unittest.TestCase):
    """Every catalog team must keep at least one usable search term."""

    def test_every_catalog_team_has_a_non_ambiguous_specific_term(self):
        unmatchable = []
        for record in STATIC_TEAM_RECORDS:
            terms = get_team_search_terms(record.canonical, record.canonical)
            has_specific_term = any(term and not _is_ambiguous_term(term) for term in terms)
            if not has_specific_term:
                unmatchable.append((record.category, record.canonical))

        self.assertEqual(
            unmatchable,
            [],
            f"catalog teams with no unambiguous search term (unmatchable): {unmatchable}",
        )

    def test_every_catalog_team_matches_its_own_full_name(self):
        # A softer, end-to-end version of the guarantee above: searching for
        # a team's own canonical name must find a same-named event.
        failures = []
        for record in STATIC_TEAM_RECORDS:
            terms = get_team_search_terms(record.canonical, record.canonical)
            matched, _, _ = match_team(terms, f"{record.canonical} vs somebody")
            if not matched:
                failures.append((record.category, record.canonical))

        self.assertEqual(failures, [], f"catalog teams that failed to self-match: {failures}")


if __name__ == "__main__":
    unittest.main()
