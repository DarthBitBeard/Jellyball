"""Tune-in reliability for Jellyfin (2.1.1): cold-start candidate racing,
immediate No-Signal placeholder, and forced failover on token expiry.

Covers the fixes for "finds a stream but never plays in Jellyfin":
- _race_cold_candidates picks the first healthy candidate in parallel.
- _serve_session_playlist starts the placeholder at t=0 when no healthy
  candidate is found (instead of 503ing after the wait).
- "playlist forbidden" on a cold session fails over immediately.
"""
import asyncio
import unittest
from unittest.mock import patch

import main  # noqa: F401  (the full import graph, as the running server has it)
import sessions
from hls_session import FetchResult, SessionConfig, SessionRegistry, SourceSpec
from state import new_channel_state


def _candidate(url: str, provider: str) -> dict:
    return {"url": url, "provider": provider, "referer": "", "origin": ""}


class _RaceHarness:
    """stream_state with three candidates; only the second is healthy."""

    def __init__(self):
        self.state = {
            "chan1": new_channel_state(
                name="Test",
                candidates=[
                    _candidate("http://cdn-a.example/x.m3u8", "A"),
                    _candidate("http://cdn-b.example/x.m3u8", "B"),
                    _candidate("http://cdn-c.example/x.m3u8", "C"),
                ],
            )
        }
        self.state["chan1"]["active_index"] = 0

    async def fake_fetch(self, url, headers, max_bytes, timeout):
        if "cdn-b.example" in url:
            return FetchResult(200, url, "application/vnd.apple.mpegurl", b"#EXTM3U\n#EXTINF:6,\nseg.ts\n")
        if "cdn-c.example" in url:
            return FetchResult(403, url, "text/plain", b"forbidden")
        return None  # cdn-a hangs / no response


class ColdStartRaceTests(unittest.IsolatedAsyncioTestCase):
    async def test_race_picks_first_healthy_candidate(self):
        harness = _RaceHarness()
        with patch.object(sessions, "stream_state", harness.state), \
                patch.object(sessions, "_session_fetch", harness.fake_fetch):
            picked = await sessions._race_cold_candidates("chan1", timeout=5.0)
        self.assertTrue(picked)
        self.assertEqual(harness.state["chan1"]["active_index"], 1)

    async def test_race_returns_false_when_nothing_healthy(self):
        harness = _RaceHarness()

        async def all_dead(url, headers, max_bytes, timeout):
            return FetchResult(403, url, "text/plain", b"nope")

        with patch.object(sessions, "stream_state", harness.state), \
                patch.object(sessions, "_session_fetch", all_dead):
            picked = await sessions._race_cold_candidates("chan1", timeout=5.0)
        self.assertFalse(picked)
        self.assertEqual(harness.state["chan1"]["active_index"], 0)

    async def test_race_skipped_with_fewer_than_two_candidates(self):
        state = {"chan1": new_channel_state(name="T", candidates=[_candidate("http://x/y.m3u8", "A")])}
        with patch.object(sessions, "stream_state", state):
            self.assertFalse(await sessions._race_cold_candidates("chan1"))

    async def test_race_skipped_for_multiview(self):
        state = {"chan1": new_channel_state(
            name="T", type="multiview",
            candidates=[_candidate("http://a/x.m3u8", "A"), _candidate("http://b/x.m3u8", "B")],
        )}
        with patch.object(sessions, "stream_state", state):
            self.assertFalse(await sessions._race_cold_candidates("chan1"))


class PlaceholderFirstTests(unittest.IsolatedAsyncioTestCase):
    async def test_cold_start_with_no_healthy_candidate_starts_placeholder(self):
        """_serve_session_playlist on a cold channel with no healthy candidate
        starts the No-Signal placeholder immediately (t=0), not after the
        startup wait."""
        harness = _RaceHarness()
        placeholder_started = []

        def fake_start(channel_id):
            placeholder_started.append(channel_id)
            return True

        async def fake_wait_ready(timeout):
            return False

        registry = SessionRegistry.__new__(SessionRegistry)
        # Minimal registry stand-in: get() returns a fresh cold session.
        from hls_session import ChannelSession, SessionHooks
        hooks = SessionHooks(
            fetch=harness.fake_fetch,
            headers_for=lambda source: {},
            resolve_source=lambda cid: None,
            report_failure=lambda cid, key, reason: None,
            report_incompatible=lambda cid, key, reason: False,
        )
        session = ChannelSession("chan1", hooks, SessionConfig())
        session.wait_ready = fake_wait_ready

        async def fake_race(channel_id):
            return False

        with patch.object(sessions, "stream_state", harness.state), \
                patch.object(sessions, "SESSIONS") as mock_sessions, \
                patch.object(sessions, "_race_cold_candidates", fake_race), \
                patch.object(sessions, "_start_on_placeholder", fake_start):
            mock_sessions.get.return_value = session
            mock_sessions.peek.return_value = session
            response = await sessions._serve_session_playlist("chan1", "chan1/seg/")
        self.assertTrue(placeholder_started, "placeholder must start at t=0 on cold start")
        # No window and placeholder "started": without a real ffmpeg the
        # placeholder can't produce segments, so this degrades to 503 (the
        # genuinely-dead case), not a hang.
        self.assertEqual(response.status_code, 503)

    async def test_healthy_race_winner_skips_placeholder(self):
        """When the race finds a healthy candidate, no placeholder is started:
        the session goes straight at the working source."""
        harness = _RaceHarness()
        placeholder_started = []

        def fake_start(channel_id):
            placeholder_started.append(channel_id)
            return True

        async def fake_race(channel_id):
            harness.state["chan1"]["active_index"] = 1
            return True

        from hls_session import ChannelSession, SessionHooks
        hooks = SessionHooks(
            fetch=harness.fake_fetch,
            headers_for=lambda source: {},
            resolve_source=lambda cid: None,
            report_failure=lambda cid, key, reason: None,
            report_incompatible=lambda cid, key, reason: False,
        )
        session = ChannelSession("chan1", hooks, SessionConfig())

        async def fake_wait_ready(timeout):
            return True

        session.wait_ready = fake_wait_ready
        # Give the session a window so it renders instead of 503ing.
        from hls_session import SessionSegment
        session.window.append(SessionSegment(seq=1, duration=6.0, discontinuity=False, data=b""))
        session.target_duration = 6

        with patch.object(sessions, "stream_state", harness.state), \
                patch.object(sessions, "SESSIONS") as mock_sessions, \
                patch.object(sessions, "_race_cold_candidates", fake_race), \
                patch.object(sessions, "_start_on_placeholder", fake_start):
            mock_sessions.get.return_value = session
            response = await sessions._serve_session_playlist("chan1", "chan1/seg/")
        self.assertEqual(placeholder_started, [])
        self.assertEqual(response.status_code, 200)


class ForbiddenFailoverTests(unittest.IsolatedAsyncioTestCase):
    async def _run_on_session_failure(self, session, reason):
        """Run _on_session_failure with the background task executed inline,
        capturing the kwargs passed to failover.request_failover."""
        captured = {}

        async def fake_request_failover(team_id, source_key, reason_, **kwargs):
            captured.update(kwargs)
            captured["reason"] = reason_
            return True

        tasks = []

        def inline_spawn(coro, _name):
            # The real _spawn_background_task is sync and returns a Task;
            # run it inline so the test can await the result.
            task = asyncio.ensure_future(coro)
            tasks.append(task)
            return task

        with patch.object(sessions, "SESSIONS") as mock_sessions, \
                patch("failover.request_failover", fake_request_failover), \
                patch.object(sessions, "_spawn_background_task", inline_spawn), \
                patch("state.stream_state", {"chan1": new_channel_state(name="T")}), \
                patch("multiview._multiview_view_for_session", return_value=None):
            mock_sessions.peek.return_value = session
            sessions._on_session_failure("chan1", ("k",), reason)
            for task in tasks:
                await task
        return captured

    def _cold_session(self):
        from hls_session import ChannelSession, SessionHooks
        hooks = SessionHooks(
            fetch=None, headers_for=None, resolve_source=None,
            report_failure=None, report_incompatible=None,
        )
        return ChannelSession("chan1", hooks, SessionConfig())

    async def test_cold_forbidden_forces_immediate_failover(self):
        """A cold session reporting 'playlist forbidden' passes
        force_failover=True so the next candidate is tried now instead of
        waiting for the background token rescrape."""
        session = self._cold_session()
        self.assertTrue(session.is_cold)
        captured = await self._run_on_session_failure(session, "playlist forbidden")
        self.assertEqual(captured.get("reason"), "playlist forbidden")
        self.assertTrue(captured.get("force_failover"))

    async def test_warm_forbidden_does_not_force_failover(self):
        session = self._cold_session()
        session._ready.set()  # warm: already serving
        self.assertFalse(session.is_cold)
        captured = await self._run_on_session_failure(session, "playlist forbidden")
        self.assertFalse(captured.get("force_failover"))

    async def test_cold_other_reason_does_not_force_failover(self):
        session = self._cold_session()
        captured = await self._run_on_session_failure(session, "playlist stale")
        self.assertFalse(captured.get("force_failover"))


if __name__ == "__main__":
    unittest.main()
