"""fetch_espn_team_schedule against a small inline ESPN team-schedule response
(httpx.MockTransport) at a fixed time: which game is chosen, which are ignored,
and when the lookup counts as failed."""

import asyncio
import socket
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import httpx

import catalog
import config
import espn_schedule
import failover
import state
from network_safety import clear_dns_cache


def _public_getaddrinfo(host, port, *args, **kwargs):
    """Stub DNS: every hostname resolves to a public address, so
    validate_http_url_async's DNS check doesn't depend on real network access."""
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", port))]


NOW = datetime(2026, 10, 4, 18, 0, tzinfo=timezone.utc)
ESPN_API = "https://site.api.espn.com/apis/site/v2/sports"


class _FrozenDatetime(datetime):
    """espn_schedule's `datetime`, with now() pinned to NOW."""

    @classmethod
    def now(cls, tz=None):
        return NOW.astimezone(tz) if tz is not None else NOW.replace(tzinfo=None)


def _espn_date(moment: datetime) -> str:
    """ESPN's format, e.g. 2026-10-05T17:00Z."""
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")


def _schedule(*starts: datetime) -> dict:
    return {
        "team": {"id": "2", "abbreviation": "BUF", "displayName": "Buffalo Bills"},
        "events": [
            {"id": str(index), "date": _espn_date(start), "name": f"Game {index}",
             "competitions": [{"status": {"type": {"completed": start < NOW}}}]}
            for index, start in enumerate(starts)
        ],
    }


def _fetch(handler, *args, **kwargs):
    requests = []

    def recording_handler(request):
        requests.append(request)
        return handler(request)

    async def go():
        clear_dns_cache()
        client = httpx.AsyncClient(transport=httpx.MockTransport(recording_handler))
        try:
            with patch.object(state, "SHARED_HTTP_CLIENT", client), \
                    patch.object(espn_schedule, "datetime", _FrozenDatetime), \
                    patch.object(config.LOGGER, "warning"), patch.object(config.LOGGER, "log"), \
                    patch.object(socket, "getaddrinfo", side_effect=_public_getaddrinfo):
                return await espn_schedule.fetch_espn_team_schedule(*args, **kwargs)
        finally:
            await client.aclose()
            clear_dns_cache()

    return asyncio.run(go()), [str(request.url) for request in requests]


def _json(payload: dict):
    return lambda request: httpx.Response(200, json=payload, request=request)


class EspnScheduleTests(unittest.TestCase):
    def test_earliest_upcoming_game_is_chosen(self):
        payload = _schedule(
            NOW + timedelta(days=7),
            NOW - timedelta(hours=6),  # over (ended 2 h ago)
            NOW + timedelta(days=1),
            NOW + timedelta(days=20),  # beyond the 14-day horizon
            NOW + timedelta(days=3),
        )
        (start, stop, ok), urls = _fetch(_json(payload), "Buffalo Bills", "Buffalo Bills", "nfl", "buf")
        self.assertEqual(urls, [f"{ESPN_API}/football/nfl/teams/buf/schedule"])
        self.assertTrue(ok)
        self.assertEqual(start, NOW + timedelta(days=1))
        self.assertEqual(stop, NOW + timedelta(days=1, hours=4))

    def test_finished_games_are_ignored(self):
        payload = _schedule(NOW - timedelta(days=2), NOW - timedelta(hours=5, minutes=1))
        (start, stop, ok), _ = _fetch(_json(payload), "Buffalo Bills", category="nfl", source_id="buf")
        # A successful lookup with no game left is "no event", not a failure.
        self.assertEqual((start, stop, ok), (None, None, True))

    def test_a_game_that_ended_within_the_last_hour_still_counts(self):
        just_over = NOW - timedelta(hours=4, minutes=30)  # a 4 h game that ended 30 min ago
        payload = _schedule(just_over, NOW + timedelta(days=2))
        (start, stop, ok), _ = _fetch(_json(payload), "Buffalo Bills", category="nfl", source_id="buf")
        self.assertEqual((start, stop, ok), (just_over, just_over + timedelta(hours=4), True))

    def test_non_200_is_a_failed_lookup(self):
        for status in (404, 503):
            with self.subTest(status=status):
                result, urls = _fetch(
                    lambda request, status=status: httpx.Response(status, request=request),
                    "Buffalo Bills", category="nfl", source_id="buf",
                )
                self.assertEqual(result, (None, None, False))
                self.assertEqual(len(urls), 1)

    def test_malformed_response_is_a_failed_lookup(self):
        result, _ = _fetch(
            lambda request: httpx.Response(200, text="<html>not json</html>", request=request),
            "Buffalo Bills", category="nfl", source_id="buf",
        )
        self.assertEqual(result, (None, None, False))

    def test_unknown_category_is_a_failed_lookup_without_a_request(self):
        result, urls = _fetch(_json(_schedule(NOW + timedelta(days=1))), "Some Team", category="cricket", source_id="7")
        self.assertEqual(result, (None, None, False))
        self.assertEqual(urls, [])

    def test_unknown_team_without_a_category_is_a_failed_lookup(self):
        result, urls = _fetch(_json(_schedule(NOW + timedelta(days=1))), "Nobody FC")
        self.assertEqual(result, (None, None, False))
        self.assertEqual(urls, [])

    def test_endpoint_and_game_length_per_league(self):
        game = NOW + timedelta(days=1)
        cases = [
            ("nfl", "buf", "football/nfl", timedelta(hours=4)),
            ("ncaaf", "130", "football/college-football", timedelta(hours=4)),
            ("ncaam", "130", "basketball/mens-college-basketball", timedelta(hours=3)),
            ("nba", "bos", "basketball/nba", timedelta(hours=3)),
            ("mlb", "nyy", "baseball/mlb", timedelta(hours=4)),
            ("nhl", "bos", "hockey/nhl", timedelta(hours=3)),
        ]
        for category, source_id, path, length in cases:
            with self.subTest(category=category):
                (start, stop, ok), urls = _fetch(_json(_schedule(game)), "Team", category=category, source_id=source_id)
                self.assertEqual(urls, [f"{ESPN_API}/{path}/teams/{source_id}/schedule"])
                self.assertEqual((start, stop - start, ok), (game, length, True))

    def test_team_name_fallback_including_the_ncaa_and_soccer_keys(self):
        """No category/source id: the team's hand-mapped (sport, slug) is used.
        "ncaa" and "soccer" are not catalog categories; only this lookup knows them."""
        game = NOW + timedelta(days=1)
        cases = [
            ("Miami Dolphins", "football/nfl/teams/mia", timedelta(hours=4)),
            ("Florida Gators", "football/college-football/teams/57", timedelta(hours=4)),
            ("Inter Miami", "soccer/usa.1/teams/10739", timedelta(hours=2.5)),
        ]
        for team_name, path, length in cases:
            with self.subTest(team=team_name):
                (start, stop, ok), urls = _fetch(_json(_schedule(game)), team_name)
                self.assertEqual(urls, [f"{ESPN_API}/{path}/schedule"])
                self.assertEqual((start, stop - start, ok), (game, length, True))

    def test_query_is_used_when_the_name_is_empty(self):
        (_, _, ok), urls = _fetch(_json(_schedule(NOW + timedelta(days=1))), "", "Miami Heat")
        self.assertTrue(ok)
        self.assertEqual(urls, [f"{ESPN_API}/basketball/nba/teams/mia/schedule"])

    def test_old_import_locations_still_work(self):
        self.assertIs(catalog.fetch_espn_team_schedule, espn_schedule.fetch_espn_team_schedule)
        self.assertIs(failover.fetch_espn_team_schedule, espn_schedule.fetch_espn_team_schedule)


if __name__ == "__main__":
    unittest.main()
