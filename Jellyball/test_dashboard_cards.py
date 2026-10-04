"""Dashboard cards and extra tabs registered by feature modules (Phase A
pre-wiring): registration rules, ordering, and rendering through the real page."""

import asyncio
import unittest
from unittest.mock import patch

import httpx
from jinja2 import ChoiceLoader, DictLoader

import dashboard_cards
import epg
import main
import routes_dashboard
import security
from dashboard_cards import Card, Tab, register_card, register_tab


async def _no_tvguide():
    return {}


def _get_dashboard(path: str = "/") -> httpx.Response:
    async def go():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=main.app), base_url="http://127.0.0.1:8000",
        ) as client:
            return await client.get(path)

    with patch.object(security, "DASHBOARD_PASSWORD", ""), \
            patch.object(epg, "_fetch_tvguide_epg", _no_tvguide), \
            patch.object(routes_dashboard, "get_catalog_entries", return_value=[]):
        return asyncio.run(go())


class RegistryTests(unittest.TestCase):
    def setUp(self):
        self._cards = patch.object(dashboard_cards, "_CARDS", [])
        self._tabs = patch.object(dashboard_cards, "_TABS", [])
        self._cards.start()
        self._tabs.start()

    def tearDown(self):
        self._cards.stop()
        self._tabs.stop()

    def test_a_card_must_target_a_known_tab(self):
        with self.assertRaises(ValueError):
            register_card(Card(tab="nowhere", name="a", template="partials/a.html"))
        register_tab(Tab(key="live", label="Live"))
        register_card(Card(tab="live", name="a", template="partials/a.html"))

    def test_card_names_are_unique_identifiers(self):
        register_card(Card(tab="alerts", name="first", template="partials/first.html"))
        with self.assertRaises(ValueError):
            register_card(Card(tab="alerts", name="first", template="partials/other.html"))
        for bad in ("two words", "dash-ed", "", "1leading"):
            with self.subTest(name=bad), self.assertRaises(ValueError):
                register_card(Card(tab="alerts", name=bad, template="partials/x.html"))

    def test_templates_and_assets_are_validated(self):
        with self.assertRaises(ValueError):
            register_card(Card(tab="alerts", name="a", template="partials/a.txt"))
        for bad in ("https://cdn.example.test/x.js", "/other/x.js", "static/js/x.js"):
            with self.subTest(script=bad), self.assertRaises(ValueError):
                register_card(Card(tab="alerts", name="a", template="partials/a.html", scripts=(bad,)))
        with self.assertRaises(ValueError):
            register_card(Card(tab="alerts", name="a", template="partials/a.html", styles=("/other/x.css",)))

    def test_tabs_cannot_shadow_a_built_in_tab_or_repeat(self):
        for bad in ("alerts", "logs", "not valid", ""):
            with self.subTest(key=bad), self.assertRaises(ValueError):
                register_tab(Tab(key=bad, label="x"))
        register_tab(Tab(key="live", label="Live"))
        with self.assertRaises(ValueError):
            register_tab(Tab(key="live", label="Again"))

    def test_cards_are_grouped_by_tab_ordered_then_in_registration_order(self):
        register_card(Card(tab="alerts", name="late", template="partials/late.html", order=200))
        register_card(Card(tab="alerts", name="first_default", template="partials/a.html"))
        register_card(Card(tab="alerts", name="second_default", template="partials/b.html"))
        register_card(Card(tab="alerts", name="early", template="partials/early.html", order=10))
        register_card(Card(tab="logs", name="elsewhere", template="partials/c.html"))
        grouped = dashboard_cards.cards_by_tab()
        self.assertEqual([c.name for c in grouped["alerts"]], ["early", "first_default", "second_default", "late"])
        self.assertEqual([c.name for c in grouped["logs"]], ["elsewhere"])

    def test_assets_are_listed_once_in_card_order(self):
        register_card(Card(tab="alerts", name="a", template="partials/a.html", order=2,
                           scripts=("/static/js/shared.js", "/static/js/a.js")))
        register_card(Card(tab="alerts", name="b", template="partials/b.html", order=1,
                           scripts=("/static/js/shared.js",), styles=("/static/css/b.css",)))
        context = asyncio.run(dashboard_cards.template_context(None))
        self.assertEqual(context["card_scripts"], ["/static/js/shared.js", "/static/js/a.js"])
        self.assertEqual(context["card_styles"], ["/static/css/b.css"])

    def test_a_failing_context_function_is_contained_and_logged_without_its_url_token(self):
        async def broken(request):
            raise RuntimeError("fetch https://jellyfin.example.test/x?api_key=SECRET failed")

        async def fine(request):
            return {"value": 7}

        register_card(Card(tab="alerts", name="broken", template="partials/a.html", context=broken))
        register_card(Card(tab="alerts", name="fine", template="partials/b.html", context=fine))
        register_card(Card(tab="alerts", name="plain", template="partials/c.html"))
        with self.assertLogs("jellyball", level="WARNING") as logs:
            context = asyncio.run(dashboard_cards.template_context(None))
        contexts = context["card_contexts"]
        self.assertEqual(contexts["fine"], {"value": 7})
        self.assertEqual(contexts["plain"], {})
        self.assertIn("RuntimeError", contexts["broken"]["error"])
        self.assertNotIn("SECRET", contexts["broken"]["error"])
        self.assertNotIn("SECRET", "\n".join(logs.output))


class RenderingTests(unittest.TestCase):
    TEMPLATES = {
        "partials/test_card.html": '<section id="card-{{ card.name }}">value={{ ctx.value }} pane={{ pane }}</section>',
        "partials/test_error_card.html": '<section id="card-{{ card.name }}">error={{ ctx.error }}</section>',
    }

    def setUp(self):
        self._patches = [
            patch.object(dashboard_cards, "_CARDS", []),
            patch.object(dashboard_cards, "_TABS", []),
            patch.object(
                routes_dashboard.TEMPLATES.env, "loader",
                ChoiceLoader([DictLoader(self.TEMPLATES), routes_dashboard.TEMPLATES.env.loader]),
            ),
        ]
        for p in self._patches:
            p.start()

    def tearDown(self):
        for p in reversed(self._patches):
            p.stop()

    def test_with_nothing_registered_the_page_has_no_extra_tabs_or_scripts(self):
        response = _get_dashboard()
        self.assertEqual(response.status_code, 200)
        self.assertNotIn('id="card-', response.text)
        self.assertEqual(response.text.count('<script src='), 1)  # dashboard.js only
        self.assertEqual(response.text.count('rel="stylesheet"'), 1)  # dashboard.css only

    def test_a_card_renders_inside_its_tab_after_the_built_in_content(self):
        async def context(request):
            return {"value": 42}

        register_card(Card(tab="alerts", name="demo", template="partials/test_card.html", context=context))
        text = _get_dashboard().text
        pane_start = text.index('id="tab-alerts"')
        card_at = text.index('<section id="card-demo">value=42 pane=alerts</section>')
        # After the pane's own content, and before the end of the pane (alerts is the last pane).
        self.assertGreater(card_at, text.index("Webhook Notification Settings"))
        self.assertGreater(card_at, pane_start)
        self.assertLess(card_at, text.index('id="dashboard-data"'))

    def test_an_extra_tab_gets_a_button_and_a_pane_and_can_be_opened(self):
        register_tab(Tab(key="live", label="Live now"))
        register_card(Card(tab="live", name="demo", template="partials/test_card.html"))
        default = _get_dashboard().text
        self.assertIn('id="btn-live" onclick="switchTab(\'live\')">Live now</button>', default)
        self.assertIn('id="tab-live" class="tab-pane "', default)
        opened = _get_dashboard("/?tab=live").text
        self.assertIn('id="tab-live" class="tab-pane active"', opened)
        self.assertIn('id="btn-live"', opened)
        self.assertIn('class="tab-btn active" id="btn-live"', opened)
        self.assertIn('<section id="card-demo">value= pane=live</section>', opened)

    def test_card_scripts_load_after_the_dashboard_script_and_styles_in_the_head(self):
        register_card(Card(tab="logs", name="demo", template="partials/test_card.html",
                           scripts=("/static/js/demo.js",), styles=("/static/css/demo.css",)))
        text = _get_dashboard().text
        self.assertLess(text.index('<script src="/static/dashboard.js">'), text.index('<script src="/static/js/demo.js">'))
        self.assertLess(text.index('href="/static/css/demo.css"'), text.index("</head>"))

    def test_a_broken_card_does_not_break_the_page(self):
        async def broken(request):
            raise RuntimeError("boom")

        register_card(Card(tab="metrics", name="broken", template="partials/test_error_card.html", context=broken))
        response = _get_dashboard()
        self.assertEqual(response.status_code, 200)
        self.assertIn('<section id="card-broken">error=RuntimeError: boom</section>', response.text)


if __name__ == "__main__":
    unittest.main()
