"""The Formula 1 catalog entry: in the opt-in Motorsport group (like the
opt-in leagues, offered only once the group is turned on), listed under a
Motorsport section of the dashboard and setup-wizard pickers, and enabling it
creates a series channel (category "f1", content type "series") with the F1
group title and logo."""

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import httpx

import catalog
import channels
import leagues
import main
import routes_dashboard
import routes_setup
import security
import state


F1_KEY = "series:f1"


def _entries():
    team = {"catalog_key": "team:nfl:buf", "name": "Buffalo Bills", "content_type": "team",
            "category": "nfl", "always_live": False}
    special = {"catalog_key": "special:espn", "name": "ESPN", "content_type": "channel",
               "category": "special", "always_live": True}
    return [team, catalog._series_catalog_entry(leagues.F1), special]


class CatalogEntryTests(unittest.TestCase):
    def test_f1_entry(self):
        entry = catalog._series_catalog_entry(leagues.F1)
        self.assertEqual(entry["catalog_key"], F1_KEY)
        self.assertEqual(entry["team_id"], "series_f1")
        self.assertEqual((entry["name"], entry["query"], entry["category"]), ("Formula 1", "Formula 1", "f1"))
        self.assertEqual(entry["content_type"], "series")
        self.assertFalse(entry["always_live"])
        self.assertEqual(set(entry["search_terms"]), {"formula 1", "formula one", "f1"})
        self.assertEqual(entry["logo_url"], "https://a.espncdn.com/i/teamlogos/leagues/500/f1.png")
        self.assertEqual(entry["group_title"], "Formula 1")
        self.assertEqual(entry["catalog_group"], "motorsport")

    def _keys(self, groups=None):
        with patch.object(catalog, "get_team_catalog", AsyncMock(return_value={})):
            return [entry["catalog_key"] for entry in asyncio.run(catalog.get_catalog_entries(groups))]

    def test_f1_is_offered_only_once_motorsport_is_turned_on(self):
        self.assertNotIn(F1_KEY, self._keys(()))
        self.assertEqual(self._keys(("motorsport",)).count(F1_KEY), 1)
        # A config import resolves against every opt-in group.
        self.assertIn(F1_KEY, self._keys(leagues.OPTIONAL_GROUP_KEYS))
        saved = catalog.ENABLED_OPTIONAL_GROUPS
        self.addCleanup(setattr, catalog, "ENABLED_OPTIONAL_GROUPS", saved)
        catalog.set_enabled_optional_groups("epl")
        self.assertNotIn(F1_KEY, self._keys())
        catalog.set_enabled_optional_groups("epl,motorsport")
        self.assertIn(F1_KEY, self._keys())

    def test_motorsport_is_an_opt_in_group(self):
        self.assertEqual(leagues.OPTIONAL_CATALOG_GROUPS[-1], ("motorsport", "Motorsport"))
        self.assertEqual(leagues.parse_optional_groups("Motorsport"), ("motorsport",))
        # The group key is not a series or league key, so it never names a channel's category.
        self.assertNotIn("motorsport", leagues.SERIES_BY_KEY)
        self.assertNotIn("motorsport", leagues.LEAGUES_BY_KEY)
        self.assertEqual([catalog.catalog_group_key(e) for e in _entries()], ["nfl", "motorsport", "special"])

    def test_stored_f1_channel_keeps_its_group_title(self):
        # group_title is not stored in the DB: a reloaded channel derives it.
        self.assertEqual(catalog._channel_group_title({"category": "f1"}), "Formula 1")
        self.assertEqual(catalog._channel_group_title({"category": "custom"}), "Team Trackers")


class EnableTests(unittest.TestCase):
    def setUp(self):
        self._saved = dict(state.stream_state)
        self.addCleanup(self._restore)
        state.stream_state.clear()

    def _restore(self):
        state.stream_state.clear()
        state.stream_state.update(self._saved)

    def test_enabling_creates_a_series_channel(self):
        entry = catalog._series_catalog_entry(leagues.F1)
        save, start = AsyncMock(), unittest.mock.Mock()
        with patch.object(channels, "save_team_async", save), \
                patch.object(channels, "_start_team_scrape_loop", start):
            changed = asyncio.run(channels._set_catalog_entry_enabled(entry, True))
        self.assertTrue(changed)
        data = state.stream_state["series_f1"]
        self.assertEqual((data["category"], data["content_type"], data["catalog_key"]), ("f1", "series", F1_KEY))
        self.assertEqual(data["group_title"], "Formula 1")
        self.assertFalse(data["always_live"])
        start.assert_called_once_with("series_f1")
        self.assertEqual(save.await_args.args[6], "f1")  # category, as stored
        self.assertEqual(save.await_args.args[8], "series")  # content type, as stored


class PickerTests(unittest.TestCase):
    def test_dashboard_lists_formula_1_under_motorsport(self):
        async def go():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=main.app), base_url="http://127.0.0.1:8000",
            ) as client:
                return await client.get("/")

        with patch.object(security, "DASHBOARD_PASSWORD", ""), \
                patch.object(routes_dashboard, "get_catalog_entries", AsyncMock(return_value=_entries())):
            response = asyncio.run(go())
        self.assertEqual(response.status_code, 200)
        text = response.text
        # Opt-in groups come after the always-live channels.
        positions = [text.find(f"<summary><span>{title}</span>") for title in ("NFL", "Always-Live Sports Channels", "Motorsport")]
        self.assertNotIn(-1, positions)
        self.assertEqual(positions, sorted(positions))
        self.assertIn(F1_KEY, text[positions[2]:])
        self.assertNotIn(F1_KEY, text[:positions[2]])

    def test_setup_wizard_groups(self):
        groups = routes_setup._catalog_groups(_entries(), {F1_KEY})
        self.assertEqual([g["label"] for g in groups], ["NFL", "Always-Live Sports Channels", "Motorsport"])
        motorsport = groups[2]["entries"]
        self.assertEqual([(e["name"], e["checked"]) for e in motorsport], [("Formula 1", True)])


if __name__ == "__main__":
    unittest.main()
