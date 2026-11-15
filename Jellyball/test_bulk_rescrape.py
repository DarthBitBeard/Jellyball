"""The Bulk Team Management Rescrape button queues one rescrape per channel."""

import asyncio
import unittest
from unittest.mock import MagicMock, patch

import routes_dashboard


class BulkRescrapeTests(unittest.TestCase):
    def _run(self, team_ids: str):
        spawned = []

        def fake_spawn(coro, name):
            spawned.append(name)
            coro.close()

        trigger = MagicMock(side_effect=lambda team_id, force=False: asyncio.sleep(0))
        with patch.object(routes_dashboard, "_spawn_background_task", side_effect=fake_spawn), \
                patch.object(routes_dashboard, "trigger_scrape", trigger):
            response = asyncio.run(routes_dashboard.bulk_rescrape(team_ids=team_ids, auth=True))
        return response, spawned, trigger

    def test_queues_a_forced_rescrape_per_selected_channel(self):
        response, spawned, trigger = self._run("nfl_1, nba_2,nhl_3")
        self.assertEqual(spawned, ["bulk rescrape nfl_1", "bulk rescrape nba_2", "bulk rescrape nhl_3"])
        trigger.assert_any_call("nba_2", force=True)
        self.assertEqual(response.status_code, 303)
        self.assertIn("status=bulk_rescrape_started", response.headers["location"])

    def test_skips_blanks_and_duplicates(self):
        _, spawned, _ = self._run("nfl_1,,nfl_1, ")
        self.assertEqual(spawned, ["bulk rescrape nfl_1"])

    def test_empty_selection_queues_nothing(self):
        _, spawned, _ = self._run("")
        self.assertEqual(spawned, [])


if __name__ == "__main__":
    unittest.main()
