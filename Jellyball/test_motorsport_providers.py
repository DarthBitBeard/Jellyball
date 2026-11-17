"""Provider support for motorsport series searches: series sections are
scanned only for a series search, event links on a section's own (sister)
host are followed, and a Formula 1 section page yields only the current
round's F1 events. Served from fixtures/providers/aggregator/f1_section.html
through the fake site of test_fixtures_contract."""

import unittest
import urllib.parse

import motorsport_terms as mt
import scrapers
from plugins.sdk import HtmlAggregatorProvider, Provider
from test_fixtures_contract import AGG_BASE, AGG_HOST, FixtureSite, OfflineCase
from sports_matcher import get_team_search_terms


SAO_PAULO_TERMS = mt.series_search_terms("f1", "São Paulo", "Sao Paulo", "Brazil")
GATORS_TERMS = get_team_search_terms("Florida Gators", "Florida Gators")
SISTER_HOST = "sister.example.test"

# Built-in providers with an F1 section (verified against their 2026 sites,
# except Streamed's, which follows its /category/<sport> pattern).
F1_SECTIONS = {
    scrapers.ISportSurgeScraper: ["/f1/livestreams2"],
    scrapers.MyBuffStreamsScraper: ["/f1streams2"],
    scrapers.MethStreamsScraper: ["/formula1streams"],
    scrapers.StreamEastScraper: ["/f1streams4"],
    scrapers.FootybiteScraper: ["/f1-streams"],
    scrapers.OneStreamScraper: ["/f1streams"],
    scrapers.StreamedSuScraper: ["/category/motor-sports"],
}


def _provider(sections, hints=("/live-event/", "/game/", "/stream/")):
    provider = HtmlAggregatorProvider("F1Test", AGG_BASE, ["/nfl", "/nba"], list(hints),
                                      series_sections={"f1": sections})
    return provider


class ScanUrlTests(unittest.TestCase):
    def test_team_searches_scan_the_categories(self):
        provider = _provider(["/f1-streams"])
        self.assertEqual(provider.scan_urls_for(GATORS_TERMS), provider.get_scan_urls())
        self.assertEqual(provider.get_scan_urls(), [AGG_BASE, AGG_BASE + "/nfl", AGG_BASE + "/nba"])

    def test_series_searches_scan_the_home_page_and_the_sections(self):
        provider = _provider(["/f1-streams", "https://sister.example.test/f1streams4"])
        self.assertEqual(provider.scan_urls_for(SAO_PAULO_TERMS),
                         [AGG_BASE, AGG_BASE + "/f1-streams", "https://sister.example.test/f1streams4"])

    def test_a_provider_without_sections_scans_only_its_home_page_for_a_series(self):
        provider = HtmlAggregatorProvider("Plain", AGG_BASE, ["/nfl"], ["/watch/"])
        self.assertEqual(provider.scan_urls_for(SAO_PAULO_TERMS), [AGG_BASE])
        self.assertEqual(Provider.series_sections, {})

    def test_sections_follow_a_base_url_override(self):
        provider = _provider(["/f1-streams"])
        provider.base_url = "https://mirror.example.test"
        self.assertEqual(provider.get_series_scan_urls("f1"), ["https://mirror.example.test/f1-streams"])
        self.assertEqual(provider.get_series_scan_urls("indycar"), [])

    def test_builtin_f1_sections(self):
        for scraper_type, sections in F1_SECTIONS.items():
            with self.subTest(scraper=scraper_type.__name__):
                self.assertEqual(scraper_type().series_sections, {"f1": sections})
        self.assertEqual(scrapers.TopStreamsScraper().series_sections, {})

    def test_event_links_may_live_on_the_listing_pages_host(self):
        provider = _provider([])
        sister_page = f"https://{SISTER_HOST}/f1streams4"
        self.assertTrue(provider._is_event_link(f"https://{SISTER_HOST}/stream/formula-1/x", sister_page))
        self.assertTrue(provider._is_event_link("/stream/formula-1/x", sister_page))
        self.assertFalse(provider._is_event_link("https://other.example.test/stream/x", sister_page))
        self.assertFalse(provider._is_event_link(f"https://{SISTER_HOST}/stream/x", AGG_BASE + "/"))


class SectionMatchingTests(OfflineCase):
    def site(self):
        site = FixtureSite()
        site.add(AGG_HOST, "/", "providers", "aggregator", "index.html")
        site.add(AGG_HOST, "/f1-streams", "providers", "aggregator", "f1_section.html")
        site.add(SISTER_HOST, "/f1streams4", "providers", "aggregator", "f1_section.html")
        for path in ("/nfl", "/nba"):
            site.add(AGG_HOST, path, "providers", "aggregator", "category.html")
        return site

    async def test_only_the_current_rounds_f1_events_match(self):
        provider = _provider(["/f1-streams"])
        site = self.site()
        async with site.client() as client:
            matches = await provider._find_matches(client, SAO_PAULO_TERMS)
        self.assertEqual(
            sorted(urllib.parse.urlsplit(url).path for url, _, _ in matches),
            ["/game/f1-sao-paulo-grand-prix-0511061530", "/live-event/formula-1-2026-brazil-gp-live-stream"],
        )
        self.assertTrue(site.requested(AGG_HOST, "/f1-streams"))
        self.assertFalse(site.requested(AGG_HOST, "/nfl"))  # team categories are skipped

    async def test_a_sister_domain_section_yields_its_own_events(self):
        provider = _provider([f"https://{SISTER_HOST}/f1streams4"])
        async with self.site().client() as client:
            matches = await provider._find_matches(client, SAO_PAULO_TERMS)
        hosts = {urllib.parse.urlsplit(url).hostname for url, _, _ in matches}
        self.assertEqual(hosts, {SISTER_HOST})
        self.assertEqual(len(matches), 2)

    async def test_team_searches_never_fetch_the_section(self):
        provider = _provider(["/f1-streams"])
        site = self.site()
        async with site.client() as client:
            await provider._find_matches(client, GATORS_TERMS)
        self.assertFalse(site.requested(AGG_HOST, "/f1-streams"))
        self.assertTrue(site.requested(AGG_HOST, "/nfl"))


if __name__ == "__main__":
    unittest.main()
