"""Jellyball's Jellyfin client against a REAL Jellyfin server (the official Docker image).

The unit tests in test_jellyfin_client.py use a hand-written fake, so they can only
confirm Jellyball agrees with our own assumptions about Jellyfin. These tests run
`jellyfin/jellyfin:12.1` (tools/jellyfin_harness.py) and check those assumptions
against the real thing: which auth header a Jellyfin 12 accepts, whether the Refresh
Guide task is there, what a refresh really does, and how Live TV registration behaves.

Skipped unless JELLYBALL_JELLYFIN_IT=1 is set (and skipped, with the reason, when
Docker is not usable). The first run pulls the image (about 745 MB). Run with:

    JELLYBALL_JELLYFIN_IT=1 python -m unittest test_jellyfin_integration -v

JELLYBALL_JELLYFIN_IMAGE=<tag or image> tests another Jellyfin (e.g. 10.11); the
assertions about Jellyfin 12 behaviour then apply only when the major version is 12.
One Jellyfin container starts per test class (about 8 s) and is always removed.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import sys
import tempfile
import unittest
import urllib.parse
from pathlib import Path
from typing import Optional
from unittest.mock import AsyncMock, patch

HERE = Path(__file__).resolve().parent
if str(HERE / "tools") not in sys.path:
    sys.path.append(str(HERE / "tools"))

import db  # noqa: E402
import jellyfin_client  # noqa: E402
import jellyfin_harness  # noqa: E402

ENABLED = os.environ.get("JELLYBALL_JELLYFIN_IT", "").strip() == "1"
SKIP_REASON = "set JELLYBALL_JELLYFIN_IT=1 to run the real-Jellyfin integration tests (they need Docker)"
WRONG_KEY = "0" * 32


def say(message: str) -> None:
    """Findings go to stderr so they show up in `-v` output and in CI logs."""
    print(message, file=sys.stderr, flush=True)


def expected_major(image: str) -> Optional[int]:
    """12 for `jellyfin/jellyfin:12.1`; None when the tag does not start with a version."""
    found = re.match(r"(\d+)\.", image.rsplit(":", 1)[-1])
    return int(found.group(1)) if found else None


class RealJellyfinCase(unittest.TestCase):
    """One disposable Jellyfin per test class; every test gets a scratch Jellyball database."""

    harness: jellyfin_harness.JellyfinHarness

    @classmethod
    def setUpClass(cls) -> None:
        # A developer's HTTP(S)_PROXY must never be asked to reach 127.0.0.1.
        no_proxy = patch.dict(os.environ, {"NO_PROXY": "127.0.0.1,localhost", "no_proxy": "127.0.0.1,localhost"})
        no_proxy.start()
        cls.addClassCleanup(no_proxy.stop)
        harness = jellyfin_harness.JellyfinHarness()
        cls.addClassCleanup(harness.stop)  # registered before start(): runs even if start() fails
        try:
            harness.start()
        except jellyfin_harness.HarnessUnavailable as exc:
            raise unittest.SkipTest(f"real-Jellyfin harness unavailable: {exc}") from exc
        cls.harness = harness
        say(f"\n[{cls.__name__}] Jellyfin {harness.version} ready at {harness.base_url} in {harness.timings['ready']:.1f}s")

    def setUp(self) -> None:
        tmpdir = tempfile.mkdtemp()
        db_patch = patch.object(db, "DB_FILE", os.path.join(tmpdir, "test.db"))
        db_patch.start()
        db.init_db()
        # The metric writer's queue binds to the first event loop that uses it and
        # every asyncio.run() here makes a new one; the event itself is not under test.
        self.metric_event = AsyncMock()
        metric_patch = patch.object(jellyfin_client, "log_metric_event_async", self.metric_event)
        metric_patch.start()
        jellyfin_client._PREFERRED_AUTH.clear()
        jellyfin_client._LOG_STATE.update(key=None, at=0.0, failing=False)

        def cleanup() -> None:
            metric_patch.stop()
            db_patch.stop()
            db.close_all_db_connections()
            shutil.rmtree(tmpdir, ignore_errors=True)
            jellyfin_client._PREFERRED_AUTH.clear()

        self.addCleanup(cleanup)

    # -- helpers ------------------------------------------------------------------

    @property
    def base(self) -> str:
        return self.harness.base_url

    @property
    def base_key(self) -> str:
        return urllib.parse.urlsplit(self.base).netloc.lower()

    @property
    def major(self) -> int:
        return jellyfin_client.parse_version(self.harness.version)[0]

    def refresh(self, **overrides: str) -> "jellyfin_client.RefreshResult":
        cfg = {"jellyfin_url": self.base, "jellyfin_api_key": self.harness.api_key, "jellyfin_task_id": ""}
        cfg.update(overrides)
        return asyncio.run(jellyfin_client.refresh_guide(cfg))

    def get_status(self, mode: str, key: str, path: str = "/ScheduledTasks") -> int:
        """Status of a GET using Jellyball's own header builder for `mode` and nothing else."""

        async def go() -> int:
            async with jellyfin_client._make_client() as client:
                response = await client.get(f"{self.base}{path}", headers=jellyfin_client.auth_headers(mode, key))
                return response.status_code

        return asyncio.run(go())


@unittest.skipUnless(ENABLED, SKIP_REASON)
class RealJellyfinCompatTests(RealJellyfinCase):
    """What a real Jellyfin 12 answers to the calls Jellyball makes. Nothing here changes
    the server's Live TV setup (it starts and stays without any)."""

    def test_version_is_in_the_tested_range(self) -> None:
        async def fetch() -> dict:
            async with jellyfin_client._make_client() as client:
                return await jellyfin_client.fetch_public_info(client, self.base)

        info = asyncio.run(fetch())
        self.assertTrue(info["ok"], info)
        self.assertEqual(info["version"], self.harness.version)
        self.assertEqual(info["product"], "Jellyfin Server")
        self.assertEqual(info["server_name"], jellyfin_harness.SERVER_NAME)
        wanted = expected_major(self.harness.image)
        if wanted is not None:
            self.assertEqual(jellyfin_client.parse_version(info["version"])[0], wanted, info["version"])
        self.assertIs(jellyfin_client.is_tested_version(info["version"]), True, info["version"])

    def test_header_matrix_for_a_valid_api_key(self) -> None:
        """The reason these tests exist: which header style does a real Jellyfin accept
        for an API key? Observed on 12.1.0: ONLY the modern `Authorization: MediaBrowser`
        header. The legacy `X-Emby-Token` Jellyball sent up to 2.0.0 is not read at all,
        so the request is anonymous and answered 401 (the server log says "challenged",
        not "Invalid token")."""
        key = self.harness.api_key
        modern = self.get_status(jellyfin_client.AUTH_MODERN, key)
        legacy = self.get_status(jellyfin_client.AUTH_LEGACY, key)
        say(f"\nHEADER MATRIX on Jellyfin {self.harness.version} (valid API key, GET /ScheduledTasks): "
            f"modern 'Authorization: MediaBrowser Token' -> {modern}; legacy 'X-Emby-Token' -> {legacy}")
        self.assertEqual(modern, 200, "the modern Authorization header must be accepted")
        if self.major >= 12:
            self.assertEqual(
                legacy, 401,
                "OBSERVED TRUTH: Jellyfin 12 ignores X-Emby-Token, so a client that only sends it is locked out; "
                "if this is now 200 Jellyfin changed and Jellyball's fallback order can be revisited",
            )
        # The same two styles written by hand, so a change in Jellyball's header builder
        # cannot hide a change in Jellyfin.
        self.assertEqual(self.harness.request("GET", "/ScheduledTasks", auth="modern").status_code, 200)
        if self.major >= 12:
            self.assertEqual(self.harness.request("GET", "/ScheduledTasks", auth="legacy").status_code, 401)

    def test_wrong_key_is_rejected_with_401_by_both_styles(self) -> None:
        for mode in (jellyfin_client.AUTH_MODERN, jellyfin_client.AUTH_LEGACY):
            self.assertEqual(self.get_status(mode, WRONG_KEY), 401, mode)
        self.assertEqual(self.harness.request("GET", "/ScheduledTasks", auth="none").status_code, 401)

    def test_authed_request_uses_the_modern_header_first_and_remembers_it(self) -> None:
        async def go() -> tuple:
            async with jellyfin_client._make_client() as client:
                return await jellyfin_client._authed_request(
                    client, "GET", f"{self.base}/ScheduledTasks", self.harness.api_key, self.base_key
                )

        response, mode = asyncio.run(go())
        self.assertEqual((response.status_code, mode), (200, jellyfin_client.AUTH_MODERN))
        self.assertEqual(jellyfin_client._PREFERRED_AUTH[self.base_key], jellyfin_client.AUTH_MODERN)

    def test_authed_request_recovers_when_the_remembered_style_is_legacy(self) -> None:
        """The 2.1.0 fallback against a real 401: legacy is tried first, Jellyfin 12
        refuses it, and the modern header then succeeds and is remembered."""
        jellyfin_client._PREFERRED_AUTH[self.base_key] = jellyfin_client.AUTH_LEGACY

        async def go() -> tuple:
            async with jellyfin_client._make_client() as client:
                return await jellyfin_client._authed_request(
                    client, "GET", f"{self.base}/ScheduledTasks", self.harness.api_key, self.base_key
                )

        response, mode = asyncio.run(go())
        self.assertEqual(response.status_code, 200)
        if self.major >= 12:
            self.assertEqual(mode, jellyfin_client.AUTH_MODERN)
            self.assertEqual(jellyfin_client._PREFERRED_AUTH[self.base_key], jellyfin_client.AUTH_MODERN)

    def test_refresh_guide_task_exists_on_a_server_without_live_tv(self) -> None:
        """Observed on 12.1.0: RefreshGuide is listed (hidden, Idle, daily) before any
        tuner or guide is configured, with an Id that is the same on every install."""
        task = self.harness.task("RefreshGuide")
        self.assertIsNotNone(task, "no RefreshGuide task on a fresh server")
        self.assertEqual(task["Name"], "Refresh Guide")
        self.assertEqual(task["Category"], "Live TV")
        self.assertEqual(task["State"], "Idle")
        self.assertRegex(task["Id"], r"^[0-9a-f]{32}$")

    def test_refresh_guide_succeeds_on_a_server_without_live_tv(self) -> None:
        task = self.harness.task("RefreshGuide")
        previous = self.harness.last_run_end("RefreshGuide")
        result = self.refresh()
        say(f"\nREFRESH on a fresh Jellyfin {self.harness.version}: {result}")
        self.assertTrue(result.ok, result)
        self.assertEqual((result.header, result.status, result.task_id), (jellyfin_client.AUTH_MODERN, 204, task["Id"]))
        self.assertEqual(db.get_setting("jellyfin_task_id"), task["Id"])
        finished = self.harness.wait_task_finished("RefreshGuide", previous)
        self.assertEqual(finished["LastExecutionResult"]["Status"], "Completed")
        saved = json.loads(db.get_setting(jellyfin_client.LAST_REFRESH_KEY))
        self.assertTrue(saved["ok"], saved)
        server = json.loads(db.get_setting(jellyfin_client.SERVER_INFO_KEY))
        self.assertEqual(server["version"], self.harness.version)

    def test_stale_task_id_is_rediscovered_by_a_real_server(self) -> None:
        """A saved task id Jellyfin does not know answers 404 (also for a malformed id)."""
        task = self.harness.task("RefreshGuide")
        result = self.refresh(jellyfin_task_id="stale-id")
        self.assertTrue(result.ok, result)
        self.assertEqual(result.task_id, task["Id"])
        self.assertEqual(db.get_setting("jellyfin_task_id"), task["Id"])

    def test_refresh_guide_with_a_wrong_key_is_an_auth_failure(self) -> None:
        with self.assertLogs(jellyfin_client.LOGGER, level="WARNING") as logs:
            result = self.refresh(jellyfin_api_key=WRONG_KEY)
        self.assertFalse(result.ok)
        self.assertEqual((result.stage, result.status), ("auth", 401))
        stored = db.get_setting(jellyfin_client.LAST_REFRESH_KEY)
        self.assertNotIn(WRONG_KEY, stored)
        self.assertNotIn(WRONG_KEY, "\n".join(logs.output))

    def test_encoding_configuration_names_the_ffmpeg_jellyfin_uses(self) -> None:
        """What ffmpeg discovery can rely on: EncoderAppPathDisplay is the path Jellyfin
        actually runs; EncoderAppPath exists only when a user chose one in the UI."""
        encoding = self.harness.api("GET", "/System/Configuration/encoding")
        display = encoding.get("EncoderAppPathDisplay")
        say(f"\nENCODING on Jellyfin {self.harness.version}: EncoderAppPathDisplay={display!r}, "
            f"EncoderAppPath present={'EncoderAppPath' in encoding}")
        self.assertIsInstance(display, str)
        self.assertTrue(display.endswith("ffmpeg"), display)
        self.assertFalse(encoding.get("EncoderAppPath"), "EncoderAppPath is only set when a user chose a path")


@unittest.skipUnless(ENABLED, SKIP_REASON)
class RealJellyfinLiveTvTests(RealJellyfinCase):
    """Live TV registration and guide refresh with Jellyball-shaped feeds. Each test
    removes what it added; the server's channel list still lags until its next refresh."""

    def setUp(self) -> None:
        super().setUp()
        self.addCleanup(self.harness.clear_livetv)
        self.feeds = self.harness.start_feeds()

    def test_refresh_guide_pulls_the_playlist_and_guide_into_jellyfin(self) -> None:
        channels = jellyfin_harness.FEED_CHANNELS
        self.harness.add_m3u_tuner(self.feeds.m3u_url)
        self.assertTrue(self.feeds.fetches("/playlist.m3u"), "Jellyfin should fetch the M3U as soon as the tuner is added")
        self.harness.add_xmltv_listing(self.feeds.xmltv_url, EnableAllTuners=True)
        self.assertFalse(self.feeds.fetches("/epg.xml"), "the XMLTV is fetched by the guide refresh, not on registration")
        self.assertEqual(self.harness.api("GET", "/LiveTv/Channels")["TotalRecordCount"], 0,
                         "channels appear only after the guide has been refreshed")

        previous = self.harness.last_run_end("RefreshGuide")
        result = self.refresh()
        self.assertTrue(result.ok, result)
        finished = self.harness.wait_task_finished("RefreshGuide", previous)
        self.assertEqual(finished["LastExecutionResult"]["Status"], "Completed")
        self.assertTrue(self.feeds.fetches("/epg.xml"), "the refresh should have downloaded the XMLTV")

        listed = self.harness.api("GET", "/LiveTv/Channels")
        self.assertEqual(sorted(c["Name"] for c in listed["Items"]), sorted(name for _, name, _ in channels))
        for channel in listed["Items"]:
            self.assertIn("CurrentProgram", channel, channel["Name"])
        programmes = self.harness.api("GET", "/LiveTv/Programs", params={"limit": 1})["TotalRecordCount"]
        self.assertGreaterEqual(programmes, len(channels) * 24)
        say(f"\nGUIDE on Jellyfin {self.harness.version}: {listed['TotalRecordCount']} channels, {programmes} programmes")

    def test_registrations_update_in_place_by_id_and_deletes_are_idempotent(self) -> None:
        """What a 'Connect Jellyfin' feature can rely on: POSTing without an Id always adds
        another entry, POSTing with the stored Id updates it, DELETE never errors."""
        harness = self.harness
        tuner = harness.add_m3u_tuner(self.feeds.m3u_url)
        harness.add_m3u_tuner(self.feeds.m3u_url)
        self.assertEqual(len(harness.livetv_config()["TunerHosts"]), 2, "no de-duplication without an Id")
        harness.api("POST", "/LiveTv/TunerHosts", json=dict(tuner, FriendlyName="Renamed"))
        hosts = harness.livetv_config()["TunerHosts"]
        self.assertEqual(len(hosts), 2)
        self.assertEqual([h.get("FriendlyName") for h in hosts if h["Id"] == tuner["Id"]], ["Renamed"])

        listing = harness.add_xmltv_listing(self.feeds.xmltv_url)
        harness.add_xmltv_listing(self.feeds.xmltv_url)
        self.assertEqual(len(harness.livetv_config()["ListingProviders"]), 2)
        harness.api("POST", "/LiveTv/ListingProviders", json=dict(listing, MoviePrefix="x"))
        self.assertEqual(len(harness.livetv_config()["ListingProviders"]), 2)

        for tuner_id in [h["Id"] for h in hosts] + ["no-such-tuner"]:
            self.assertEqual(harness.request("DELETE", "/LiveTv/TunerHosts", params={"id": tuner_id}).status_code, 204)
        for listing_id in [p["Id"] for p in harness.livetv_config()["ListingProviders"]] + ["no-such-listing"]:
            self.assertEqual(harness.request("DELETE", "/LiveTv/ListingProviders", params={"id": listing_id}).status_code, 204)
        config = harness.livetv_config()
        self.assertEqual((config["TunerHosts"], config["ListingProviders"]), ([], []))

    def test_an_unreachable_playlist_is_refused_but_an_unreachable_guide_is_not(self) -> None:
        """The playlist is fetched when the tuner is added, so a URL Jellyfin cannot reach
        is refused there (HTTP 500, a bare text body). The guide URL is not touched until
        a refresh, so the same mistake is accepted silently."""
        nowhere = f"http://{jellyfin_harness.HOST_ALIAS}:1"
        refused = self.harness.request("POST", "/LiveTv/TunerHosts", json={"Type": "m3u", "Url": f"{nowhere}/playlist.m3u"})
        self.assertGreaterEqual(refused.status_code, 400, refused.text)
        self.assertEqual(self.harness.livetv_config()["TunerHosts"], [])
        accepted = self.harness.request("POST", "/LiveTv/ListingProviders", json={"Type": "xmltv", "Path": f"{nowhere}/epg.xml"})
        self.assertEqual(accepted.status_code, 200, accepted.text)
        self.assertEqual(len(self.harness.livetv_config()["ListingProviders"]), 1)


if __name__ == "__main__":
    unittest.main()
