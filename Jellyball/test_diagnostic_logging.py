"""Troubleshooting logs (3.0.1): secrets never reach the log, failures keep a
scrubbed message, rejected candidates say why, and the playback lines appear."""

import logging
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import config
import diaglog
import state


def _capture(fn):
    """Run fn and return what the redacting filter would hand to the log file."""
    with unittest.TestCase().assertLogs("jellyball", level="DEBUG") as logs:
        fn()
    filt = config._RedactingFilter()
    lines = []
    for record in logs.records:
        filt.filter(record)
        lines.append(record.getMessage() + ("\n" + record.exc_text if record.exc_text else ""))
    return "\n".join(lines)


class RedactionTests(unittest.TestCase):
    def test_registered_secrets_are_replaced(self):
        config.register_secret("sk-live-1234567890abcdef")
        out = _capture(lambda: config.LOGGER.warning("key was %s in use", "sk-live-1234567890abcdef"))
        self.assertNotIn("sk-live-1234567890abcdef", out)
        self.assertIn("<redacted>", out)

    def test_secret_shapes_are_replaced(self):
        samples = [
            "GET https://cdn.example.test/a.m3u8?token=abcdef123456&x=1",
            'Authorization: MediaBrowser Token="0123456789abcdef0123"',
            "https://discord.com/api/webhooks/123/very-secret-part",
            "bot 123456789:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA used",
            "api_key=deadbeefcafe",
        ]
        for sample in samples:
            with self.subTest(sample=sample):
                out = config.redact(sample)
                for secret in ("abcdef123456", "0123456789abcdef0123", "very-secret-part", "AAAAAAAAAAAAAAAAAAAA", "deadbeefcafe"):
                    self.assertNotIn(secret, out)

    def test_short_values_are_not_registered(self):
        config.register_secret("abc")
        self.assertEqual(config.redact("abc abc"), "abc abc")

    def test_tracebacks_are_redacted(self):
        config.register_secret("tb-secret-value-42")

        def boom():
            try:
                raise RuntimeError("failed with tb-secret-value-42")
            except RuntimeError:
                config.LOGGER.exception("crash")

        out = _capture(boom)
        self.assertNotIn("tb-secret-value-42", out)
        self.assertIn("RuntimeError", out)


class LogFailureTests(unittest.TestCase):
    def test_failures_keep_a_scrubbed_message(self):
        exc = RuntimeError("Executable doesn't exist at https://cdn.example.test/path/x?token=SECRETTOKEN")
        out = _capture(lambda: config._log_failure("start Playwright", exc))
        self.assertIn("start Playwright failed (RuntimeError: Executable doesn't exist at https://cdn.example.test/...", out)
        self.assertNotIn("SECRETTOKEN", out)

    def test_debug_level_adds_the_traceback(self):
        try:
            raise ValueError("bad value")
        except ValueError as exc:
            err = exc
        with patch.object(config.LOGGER, "isEnabledFor", lambda level: True):
            out = _capture(lambda: config._log_failure("parse", err))
        self.assertIn("Traceback", out)


class PlaybackLinesTests(unittest.TestCase):
    def setUp(self):
        self._saved = dict(state.stream_state)
        state.stream_state.clear()
        state.stream_state["nfl_buf"] = {"name": "Buffalo Bills", "candidates": [], "schedule_status": "scheduled"}
        state.stream_state["special_espn"] = {
            "name": "ESPN", "candidates": [{"provider": "IPTV-Org", "url": "https://x.example.test/a.m3u8"}],
        }
        diaglog._LAST_PLAYLIST_LOG.clear()

    def tearDown(self):
        state.stream_state.clear()
        state.stream_state.update(self._saved)

    def test_tune_in_without_candidates_warns(self):
        out = _capture(lambda: diaglog.log_tune_in("nfl_buf"))
        self.assertIn("Tune-in channel=nfl_buf", out)
        self.assertIn("Tune-in with no stream candidates channel=nfl_buf", out)

    def test_tune_in_lists_candidate_providers(self):
        out = _capture(lambda: diaglog.log_tune_in("special_espn"))
        self.assertIn("candidates=1 providers=IPTV-Org", out)
        self.assertNotIn("no stream candidates", out)

    def test_unplayable_names_state_and_source(self):
        session = SimpleNamespace(state="exhausted", source=None)
        out = _capture(lambda: diaglog.log_unplayable("nfl_buf", session))
        self.assertIn("Channel unplayable, answering 503 channel=nfl_buf state=exhausted source=-", out)

    def test_playlist_requests_are_rate_limited_and_failures_always_logged(self):
        scope = {"path": "/stream/special_espn.m3u8", "method": "GET", "client": ("192.0.2.5", 1), "headers": [(b"user-agent", b"Lavf/61")]}

        def run():
            diaglog.log_request(scope, 200, 12)
            diaglog.log_request(scope, 200, 12)
            diaglog.log_request(scope, 503, 9000)

        out = _capture(run)
        self.assertEqual(out.count("Playback request path=/stream/special_espn.m3u8"), 2)  # 1 INFO + 1 DEBUG
        self.assertIn("Playback request failed path=/stream/special_espn.m3u8 method=GET status=503", out)
        self.assertIn("ua=Lavf/61", out)

    def test_segment_failures_and_guide_requests(self):
        def run():
            diaglog.log_request({"path": "/stream/x/seg/5.ts", "method": "GET", "client": ("192.0.2.5", 1)}, 404, 3)
            diaglog.log_request({"path": "/epg.xml", "method": "GET", "client": ("192.0.2.5", 1), "headers": []}, 200, 40)
            diaglog.log_request({"path": "/api/status", "method": "GET"}, 200, 1)

        out = _capture(run)
        self.assertIn("Segment request failed path=/stream/x/seg/5.ts status=404", out)
        self.assertIn("Guide request path=/epg.xml", out)
        self.assertNotIn("/api/status", out)

    def test_startup_report_lists_the_environment(self):
        out = _capture(lambda: diaglog.startup_report({"jellyfin": "localhost:8096"}))
        for needle in ("Startup report:", "version=", "data_dir=", "ffmpeg=", "chromium=", "channels=2", "jellyfin=localhost:8096"):
            self.assertIn(needle, out)


class RejectionReasonTests(unittest.TestCase):
    def test_a_403_playlist_is_reported(self):
        import asyncio

        import httpx

        import stream_extractor

        def handler(request):
            return httpx.Response(403, text="forbidden")

        async def run():
            reject = {}
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                with patch.object(stream_extractor, "validate_http_url_async", new=_async):
                    ok = await stream_extractor.verify_stream_live(client, "https://cdn.example.test/live.m3u8", reject=reject)
            return ok, reject

        ok, reject = asyncio.run(run())
        self.assertFalse(ok)
        self.assertEqual(reject.get("reason"), "playlist http_403")


async def _async(value):
    return value






class SearchTermTests(unittest.TestCase):
    def test_number_only_terms_are_dropped(self):
        import scrapers

        terms = ["michigan wolverines", "wolverines", "michigan", "mich", "130", " ", "michigan"]
        self.assertEqual(scrapers._usable_search_terms(terms), ["michigan wolverines", "wolverines", "michigan", "mich"])

    def test_the_old_alias_no_longer_matches_a_numbered_listing(self):
        import scrapers

        terms = scrapers._usable_search_terms(["michigan wolverines", "130"])
        self.assertFalse(scrapers._channel_term_matches("Sky sports Premier League ID: 130", terms))


if __name__ == "__main__":
    unittest.main()
