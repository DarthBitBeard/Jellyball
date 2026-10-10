"""Tests for the provider plugin SDK (3.0.0): loader, manifest validation,
failure isolation, permission enforcement, and builtin migration."""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

import plugins
from plugins import sdk


STUB_PROVIDER_CODE = '''
from plugins.sdk import Provider


class TestProvider(Provider):
    name = "Test Plugin"

    def __init__(self):
        super().__init__()
        self.base_url = "https://example.test"

    async def search(self, query_or_terms, *, browser=None, http_client=None):
        return [{
            "url": "https://example.test/stream.m3u8",
            "provider": self.name,
            "match_score": 100,
            "match_title": "Test Stream",
            "discovery_method": "stub",
        }]
'''


def make_plugin_dir(parent, slug, manifest_overrides=None, provider_code=None):
    d = Path(parent) / slug
    d.mkdir(parents=True, exist_ok=True)
    manifest = {
        "name": "Test Plugin",
        "version": "1.0.0",
        "api_version": 1,
        "entry": "provider.py:TestProvider",
        "permissions": ["network"],
    }
    if manifest_overrides:
        manifest.update(manifest_overrides)
    (d / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (d / "provider.py").write_text(
        provider_code if provider_code is not None else STUB_PROVIDER_CODE,
        encoding="utf-8",
    )
    return d


class LoaderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        # Third-party dir patched to the temp dir; builtins still load for real.
        patcher = patch.object(plugins, "THIRD_PARTY_DIR", Path(self.tmp.name))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(plugins.load_plugins)  # restore the real registry

    def test_valid_third_party_plugin_loads(self):
        make_plugin_dir(self.tmp.name, "testplugin")
        records = plugins.load_plugins()
        names = [r.name for r in records]
        self.assertIn("Test Plugin", names)
        record = next(r for r in records if r.name == "Test Plugin")
        self.assertEqual(record.origin, "third_party")
        self.assertFalse(record.builtin)
        self.assertEqual(record.version, "1.0.0")
        self.assertEqual(record.api_version, 1)
        self.assertIsInstance(record.instance, sdk.Provider)

    def test_bad_manifest_is_isolated(self):
        d = Path(self.tmp.name) / "badplugin"
        d.mkdir()
        (d / "manifest.json").write_text("{not valid json", encoding="utf-8")
        records = plugins.load_plugins()
        # The 11 builtins still load; the bad plugin is recorded, not raised.
        self.assertEqual(len(records), 11)
        errors = plugins.get_plugin_errors()
        self.assertTrue(any(e["plugin"] == "badplugin" for e in errors))

    def test_missing_entry_is_isolated(self):
        make_plugin_dir(self.tmp.name, "noentry", {"entry": "provider.py:NoSuchClass"})
        plugins.load_plugins()
        errors = plugins.get_plugin_errors()
        self.assertTrue(any("NoSuchClass" in e["error"] for e in errors))

    def test_newer_api_version_is_rejected(self):
        make_plugin_dir(self.tmp.name, "future", {"api_version": sdk.API_VERSION + 1})
        records = plugins.load_plugins()
        self.assertEqual(len(records), 11)
        errors = plugins.get_plugin_errors()
        self.assertTrue(any("api_version" in e["error"] for e in errors))

    def test_import_failure_is_isolated(self):
        make_plugin_dir(self.tmp.name, "broken", provider_code="raise RuntimeError('boom')\n")
        records = plugins.load_plugins()
        self.assertEqual(len(records), 11)
        errors = plugins.get_plugin_errors()
        self.assertTrue(any(e["plugin"] == "broken" for e in errors))

    def test_name_mismatch_is_rejected(self):
        code = STUB_PROVIDER_CODE.replace('name = "Test Plugin"', 'name = "Other Name"')
        make_plugin_dir(self.tmp.name, "mismatch", provider_code=code)
        records = plugins.load_plugins()
        self.assertEqual(len(records), 11)
        errors = plugins.get_plugin_errors()
        self.assertTrue(any("does not match" in e["error"] for e in errors))

    def test_non_provider_subclass_is_rejected(self):
        code = "class TestProvider:\n    name = 'Test Plugin'\n"
        make_plugin_dir(self.tmp.name, "notprovider", provider_code=code)
        records = plugins.load_plugins()
        self.assertEqual(len(records), 11)
        errors = plugins.get_plugin_errors()
        self.assertTrue(any("not a plugins.sdk.Provider subclass" in e["error"] for e in errors))

    def test_duplicate_name_is_rejected(self):
        code = STUB_PROVIDER_CODE.replace('name = "Test Plugin"', 'name = "Dupe"')
        make_plugin_dir(self.tmp.name, "dup1", {"name": "Dupe"}, provider_code=code)
        make_plugin_dir(self.tmp.name, "dup2", {"name": "Dupe"}, provider_code=code)
        records = plugins.load_plugins()
        dupes = [r for r in records if r.name == "Dupe"]
        self.assertEqual(len(dupes), 1)
        errors = plugins.get_plugin_errors()
        self.assertTrue(any("duplicate provider name" in e["error"] for e in errors))


class ConformanceTests(unittest.TestCase):
    """A stub plugin implementing the SDK must satisfy the stream contract."""

    def test_stub_search_returns_valid_stream_dicts(self):
        import asyncio

        namespace = {}
        exec(compile(STUB_PROVIDER_CODE, "stub_provider", "exec"), namespace)
        provider = namespace["TestProvider"]()
        self.assertIsInstance(provider, sdk.Provider)
        self.assertEqual(provider.base_url, "https://example.test")
        streams = asyncio.run(provider.search(["espn"], browser=None, http_client=None))
        self.assertEqual(len(streams), 1)
        stream = streams[0]
        for key in ("url", "provider", "match_score"):
            self.assertIn(key, stream)
        self.assertEqual(stream["provider"], "Test Plugin")
        self.assertIsInstance(stream["match_score"], int)


class PermissionTests(unittest.TestCase):
    def test_browser_permission_enforced(self):
        sentinel = object()

        class WithBrowser(sdk.Provider):
            name = "WithBrowser"

        class WithoutBrowser(sdk.Provider):
            name = "WithoutBrowser"

        with_browser = WithBrowser()
        with_browser.plugin_permissions = ["network", "browser"]
        without_browser = WithoutBrowser()
        without_browser.plugin_permissions = ["network"]

        self.assertIs(plugins.permitted_browser(with_browser, sentinel), sentinel)
        self.assertIsNone(plugins.permitted_browser(without_browser, sentinel))
        # Providers that predate the permission system are unaffected.
        self.assertIs(plugins.permitted_browser(object(), sentinel), sentinel)
        self.assertIsNone(plugins.permitted_browser(without_browser, None))


class BuiltinMigrationTests(unittest.TestCase):
    EXPECTED_ORDER = [
        "TheTVApp", "DaddyLive", "iSportSurge", "MyBuffStreams", "MethStreams",
        "StreamEast", "Footybite", "1Stream", "Streamed", "TopStreams", "IPTV-Org",
    ]
    EXPECTED_LINEAR = {"TheTVApp", "DaddyLive", "IPTV-Org"}

    def test_all_eleven_builtins_load(self):
        records = plugins.get_plugin_records()
        builtins = [r for r in records if r.builtin]
        self.assertEqual(len(builtins), 11)
        self.assertEqual([r.name for r in builtins], self.EXPECTED_ORDER)

    def test_builtin_load_order_matches_historical(self):
        import scrapers

        self.assertEqual([p.name for p in scrapers.ACTIVE_PROVIDERS], self.EXPECTED_ORDER)

    def test_linear_flags_match_historical(self):
        import scrapers

        self.assertEqual({p.name for p in scrapers.LINEAR_PROVIDERS}, self.EXPECTED_LINEAR)
        records = {r.name: r for r in plugins.get_plugin_records()}
        for name in self.EXPECTED_ORDER:
            self.assertEqual(records[name].linear, name in self.EXPECTED_LINEAR)

    def test_builtin_api_version_pinned(self):
        for record in plugins.get_plugin_records():
            if record.builtin:
                self.assertEqual(record.api_version, 1)
                self.assertLessEqual(record.api_version, sdk.API_VERSION)

    def test_scrapers_aliases(self):
        import scrapers

        self.assertIs(scrapers.BaseProvider, sdk.Provider)
        self.assertIs(scrapers.HtmlAggregatorScraper, sdk.HtmlAggregatorProvider)
        self.assertTrue(issubclass(scrapers.DaddyLiveScraper, sdk.HtmlAggregatorProvider))
        self.assertTrue(issubclass(scrapers.IptvOrgScraper, sdk.Provider))

    def test_reload_rebuilds_registry(self):
        records = plugins.reload_plugins()
        self.assertEqual(len(records), 11)
        self.assertEqual([r.name for r in records], self.EXPECTED_ORDER)


class DashboardTests(unittest.TestCase):
    def test_provider_rows_include_plugin_info(self):
        from routes_providers import provider_rows

        rows = provider_rows({})
        self.assertEqual(len(rows), 11)
        for row in rows:
            plugin = row["plugin"]
            self.assertEqual(plugin["version"], "3.0.0")
            self.assertEqual(plugin["api_version"], 1)
            self.assertTrue(plugin["builtin"])
            self.assertEqual(plugin["origin"], "builtin")
            self.assertIn("network", plugin["permissions"])

    def test_reload_endpoint_rebuilds_provider_lists(self):
        import asyncio

        import scrapers
        from routes_providers import reload_providers

        before = scrapers.ACTIVE_PROVIDERS
        before_names = [p.name for p in before]
        result = asyncio.run(reload_providers(auth=True))
        self.assertEqual(result["reloaded"], 11)
        self.assertEqual(len(result["providers"]), 11)
        self.assertEqual(result["errors"], [])
        # ACTIVE_PROVIDERS is the same list object, repopulated in place.
        self.assertIs(scrapers.ACTIVE_PROVIDERS, before)
        self.assertEqual([p.name for p in scrapers.ACTIVE_PROVIDERS], before_names)

    def test_card_context_includes_plugin_errors(self):
        import asyncio

        from routes_providers import _provider_card_context

        ctx = asyncio.run(_provider_card_context(request=None))
        self.assertIn("providers", ctx)
        self.assertIn("plugin_errors", ctx)
        self.assertEqual(ctx["plugin_errors"], [])


class SdkApiTests(unittest.TestCase):
    def test_api_version(self):
        self.assertEqual(sdk.API_VERSION, 1)

    def test_provider_base_url_override(self):
        import scrapers

        provider = sdk.HtmlAggregatorProvider("Tmp", "https://default.example.test")
        self.assertEqual(provider.base_url, "https://default.example.test")
        scrapers._PROVIDER_BASE_URL_OVERRIDES["Tmp"] = "https://override.example.test"
        try:
            self.assertEqual(provider.base_url, "https://override.example.test")
        finally:
            del scrapers._PROVIDER_BASE_URL_OVERRIDES["Tmp"]
        self.assertEqual(provider.base_url, "https://default.example.test")

    def test_provider_base_url_setter(self):
        provider = sdk.Provider()
        provider.base_url = "https://set.example.test/"
        self.assertEqual(provider.base_url, "https://set.example.test")
        self.assertEqual(provider._default_base_url, "https://set.example.test")

    def test_get_scan_urls(self):
        provider = sdk.HtmlAggregatorProvider("Tmp", "https://example.test", ["/tv/"])
        urls = provider.get_scan_urls()
        self.assertIn("https://example.test", urls)
        self.assertIn("https://example.test/tv/", urls)


if __name__ == "__main__":
    unittest.main()
