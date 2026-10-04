"""Streaming-engine lane (2.1.0 E1, E2, E4): preferred audio language wiring,
legacy-fallback counters, and the Live Sessions / fallback dashboard cards."""

import asyncio
import re
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

import engine_settings
import engine_stats
import main
import security
from hls_session import SourceSpec
from test_dashboard_cards import _get_dashboard
from test_hls_session import Harness, fast_config, make_ts_segment, playlist_text
from hls_session import ChannelSession


class FakeSettings:
    def __init__(self):
        self.values = {}

    def get(self, key, default=""):
        return self.values.get(key, default)

    def set(self, key, value):
        self.values[key] = value


def _reset_engine_cache():
    """Test-only reset for engine_settings' 30s cache (no production invalidation path)."""
    engine_settings._cache["value"] = None
    engine_settings._cache["at"] = 0.0


class AudioLanguageSettingTests(unittest.TestCase):
    def setUp(self):
        self.fake = FakeSettings()
        self._patches = [
            patch.object(engine_settings, "get_setting", self.fake.get),
            patch.object(engine_settings, "set_setting", self.fake.set),
        ]
        for p in self._patches:
            p.start()
        _reset_engine_cache()

    def tearDown(self):
        for p in self._patches:
            p.stop()
        _reset_engine_cache()

    def test_default_is_no_preference(self):
        self.assertIsNone(engine_settings.preferred_audio_language())

    def test_saved_value_is_used_and_garbage_is_rejected(self):
        self.assertEqual(engine_settings.save_audio_language(" ENG "), "eng")
        self.assertEqual(engine_settings.preferred_audio_language(), "eng")
        self.assertEqual(engine_settings.save_audio_language("klingon; drop table"), "")
        self.assertIsNone(engine_settings.preferred_audio_language())


class SessionWiringTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.harness = Harness()

    def hooks(self, **extra):
        hooks = self.harness.hooks()
        for name, value in extra.items():
            setattr(hooks, name, value)
        return hooks

    def test_normalizer_defaults_to_no_preference_without_the_hook(self):
        session = ChannelSession("c", self.harness.hooks(), fast_config())
        self.assertIsNone(session.normalizer.preferred_audio_language)

    def test_normalizer_gets_the_preferred_language(self):
        session = ChannelSession("c", self.hooks(preferred_audio_language=lambda: "spa"), fast_config())
        self.assertEqual(session.normalizer.preferred_audio_language, "spa")

    def test_a_failing_hook_leaves_the_default(self):
        def boom():
            raise RuntimeError("db gone")

        session = ChannelSession("c", self.hooks(preferred_audio_language=boom), fast_config())
        self.assertIsNone(session.normalizer.preferred_audio_language)

    async def test_a_source_switch_picks_up_a_changed_preference(self):
        base = "http://upstream"
        choice = {"lang": None}
        self.harness.source = SourceSpec(key=("a",), url=f"{base}/a.m3u8", label="a")
        self.harness.set_playlist(f"{base}/a.m3u8", playlist_text(base, [0, 1, 2]))
        for u in range(3):
            self.harness.set_response(f"{base}/seg{u}.ts", make_ts_segment())
        session = ChannelSession("c", self.hooks(preferred_audio_language=lambda: choice["lang"]), fast_config())
        await session._poll_once()
        self.assertIsNone(session.normalizer.preferred_audio_language)
        choice["lang"] = "fra"
        self.harness.source = SourceSpec(key=("b",), url=f"{base}/b.m3u8", label="b")
        self.harness.set_playlist(f"{base}/b.m3u8", playlist_text(base, [0, 1, 2]))
        await session._poll_once()
        self.assertEqual(session.normalizer.preferred_audio_language, "fra")


class LegacyFallbackCountTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.harness = Harness()
        self.recorded = []

    def session(self):
        hooks = self.harness.hooks()
        hooks.on_legacy_fallback = lambda cid, kind, key: self.recorded.append((cid, kind, key))
        return ChannelSession("chan1", hooks, fast_config())

    async def _poll(self, text, **kw):
        base = "http://upstream"
        self.harness.source = SourceSpec(key=("primary",), url=f"{base}/media.m3u8", label="primary")
        self.harness.set_playlist(f"{base}/media.m3u8", text)
        session = self.session()
        await session._poll_once()
        return session

    async def test_fmp4_is_counted(self):
        session = await self._poll(playlist_text("http://upstream", [0, 1, 2], has_map=True))
        self.assertEqual(session.state, "legacy")
        self.assertEqual(self.recorded, [("chan1", "fmp4", ("primary",))])

    async def test_sample_aes_is_counted_separately(self):
        await self._poll(playlist_text("http://upstream", [0, 1, 2], sample_aes=True))
        self.assertEqual([r[1] for r in self.recorded], ["sample_aes"])

    async def test_demuxed_audio_is_counted(self):
        base = "http://upstream"
        self.harness.source = SourceSpec(key=("primary",), url=f"{base}/media.m3u8", label="primary")
        self.harness.set_playlist(
            f"{base}/media.m3u8",
            '#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="a",NAME="en",URI="audio.m3u8"\n'
            '#EXT-X-STREAM-INF:BANDWIDTH=3000000,AUDIO="a"\nvariant.m3u8\n',
        )
        session = self.session()
        await session._poll_once()
        self.assertEqual(session.state, "legacy")
        self.assertEqual([r[1] for r in self.recorded], ["demuxed_audio"])

    async def test_not_counted_when_another_source_takes_over(self):
        self.harness.incompatible_result = True  # a compatible standby exists
        await self._poll(playlist_text("http://upstream", [0, 1, 2], has_map=True))
        self.assertEqual(self.recorded, [])


class CounterTests(unittest.TestCase):
    def setUp(self):
        engine_stats.reset()
        self.addCleanup(engine_stats.reset)

    def test_counts_by_reason_and_provider(self):
        engine_stats.record_legacy_fallback("fmp4", "Alpha")
        engine_stats.record_legacy_fallback("fmp4", "alpha")
        engine_stats.record_legacy_fallback("demuxed_audio", "")
        engine_stats.record_legacy_fallback("something-new", "beta")
        rows = engine_stats.legacy_fallback_counts()
        self.assertEqual(rows[0], {"reason": "fmp4", "provider": "alpha", "count": 2})
        self.assertIn({"reason": "demuxed_audio", "provider": "unknown", "count": 1}, rows)
        self.assertIn({"reason": "other", "provider": "beta", "count": 1}, rows)
        self.assertEqual(engine_stats.legacy_fallback_totals()["fmp4"], 2)


def _request(method, path, **kw):
    async def go():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=main.app), base_url="http://127.0.0.1:8000",
        ) as client:
            return await client.request(method, path, **kw)

    with patch.object(security, "DASHBOARD_PASSWORD", ""):
        return asyncio.run(go())


class HttpSurfaceTests(unittest.TestCase):
    def setUp(self):
        engine_stats.reset()
        self.addCleanup(engine_stats.reset)

    def test_metrics_expose_fallbacks_by_reason_and_provider(self):
        engine_stats.record_legacy_fallback("fmp4", "alpha")
        engine_stats.record_legacy_fallback("fmp4", "alpha")
        body = _request("GET", "/metrics").text
        self.assertIn('jellyball_legacy_fallbacks_total{reason="fmp4",provider="alpha"} 2', body)

    def test_engine_status_endpoint(self):
        engine_stats.record_legacy_fallback("sample_aes", "beta")
        data = _request("GET", "/api/engine/status").json()
        self.assertEqual(data["legacy_fallbacks"], [{"reason": "sample_aes", "provider": "beta", "count": 1}])
        self.assertIn("breakers", data)

    def test_audio_language_post_saves_and_redirects(self):
        fake = FakeSettings()
        with patch.object(engine_settings, "get_setting", fake.get), \
                patch.object(engine_settings, "set_setting", fake.set):
            _reset_engine_cache()
            response = _request(
                "POST", "/settings/audio-language", data={"preferred_audio_language": "deu"},
                headers={"Origin": "http://127.0.0.1:8000"},
            )
            _reset_engine_cache()
        self.assertEqual(response.status_code, 303)
        self.assertEqual(fake.values[engine_settings.AUDIO_LANGUAGE_KEY], "deu")

    def test_cards_render_on_the_dashboard_with_their_script(self):
        engine_stats.record_legacy_fallback("fmp4", "gamma")
        html = _get_dashboard("/").text
        self.assertIn("engine-live-sessions", html)
        self.assertIn("Preferred Audio Language", html)
        self.assertIn("gamma", html)
        self.assertIn("/static/js/engine.js", html)

    def test_engine_script_is_served(self):
        response = _request("GET", "/static/js/engine.js")
        self.assertEqual(response.status_code, 200)
        self.assertIn("/api/sessions", response.text)

    def test_no_secrets_or_hostnames_in_the_script(self):
        text = (Path(__file__).parent / "static" / "js" / "engine.js").read_text(encoding="utf-8")
        self.assertIsNone(re.search(r"https?://", text))


if __name__ == "__main__":
    unittest.main()
