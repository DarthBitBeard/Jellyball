"""updates.check_for_update against a fake GitHub Releases API (no network)."""

import asyncio
import unittest
from unittest.mock import patch

import httpx

import updates

RELEASE_URL = "https://github.com/DarthBitBeard/Jellyball/releases/tag/v99.0.0"


def _release(tag="v99.0.0", *, prerelease=False, draft=False, html_url=RELEASE_URL):
    return {"tag_name": tag, "prerelease": prerelease, "draft": draft, "html_url": html_url}


class CheckForUpdateTests(unittest.TestCase):
    def setUp(self):
        self._saved = dict(updates._UPDATE_STATE)
        updates._UPDATE_STATE.update(latest="", url="", checked_at=0.0)
        self.requests = []

    def tearDown(self):
        updates._UPDATE_STATE.clear()
        updates._UPDATE_STATE.update(self._saved)

    def _check(self, respond):
        """Run one update check; `respond(request)` returns the fake API's httpx.Response
        (or raises, to simulate a network failure)."""
        def handler(request: httpx.Request) -> httpx.Response:
            self.requests.append(request)
            return respond(request)

        real_client = httpx.AsyncClient

        def client_with_fake_api(*args, **kwargs):
            kwargs["transport"] = httpx.MockTransport(handler)
            return real_client(*args, **kwargs)

        with patch.object(updates.httpx, "AsyncClient", client_with_fake_api):
            asyncio.run(updates.check_for_update())

    def test_a_newer_release_is_recorded_and_reported_as_available(self):
        self._check(lambda request: httpx.Response(200, json=_release()))
        self.assertEqual(updates._UPDATE_STATE["latest"], "99.0.0")
        self.assertEqual(updates._UPDATE_STATE["url"], RELEASE_URL)
        self.assertGreater(updates._UPDATE_STATE["checked_at"], 0)
        self.assertTrue(updates._update_available())

    def test_it_asks_the_configured_url_and_identifies_itself(self):
        self._check(lambda request: httpx.Response(200, json=_release()))
        self.assertEqual(len(self.requests), 1)
        request = self.requests[0]
        self.assertEqual(str(request.url), updates.UPDATE_CHECK_URL)
        self.assertEqual(request.headers["accept"], "application/vnd.github+json")
        self.assertTrue(request.headers["user-agent"].startswith("Jellyball/"))

    def test_the_running_version_is_not_reported_as_an_update(self):
        self._check(lambda request: httpx.Response(200, json=_release(f"v{updates.__version__}")))
        self.assertEqual(updates._UPDATE_STATE["latest"], updates.__version__)
        self.assertFalse(updates._update_available())

    def test_prereleases_and_drafts_are_ignored(self):
        for flags in ({"prerelease": True}, {"draft": True}):
            with self.subTest(flags=flags):
                self._check(lambda request, flags=flags: httpx.Response(200, json=_release(**flags)))
                self.assertEqual(updates._UPDATE_STATE["latest"], "")
                self.assertFalse(updates._update_available())

    def test_a_release_link_outside_github_is_dropped(self):
        self._check(lambda request: httpx.Response(
            200, json=_release(html_url="https://download.example.test/Jellyball.exe")))
        self.assertEqual(updates._UPDATE_STATE["latest"], "99.0.0")
        self.assertEqual(updates._UPDATE_STATE["url"], "")

    def test_failures_leave_the_state_alone_and_never_raise(self):
        def refused(request):
            raise httpx.ConnectError("connection refused", request=request)

        cases = {
            "rate limited": lambda request: httpx.Response(403, json={"message": "rate limit exceeded"}),
            "not found": lambda request: httpx.Response(404),
            "server error": lambda request: httpx.Response(502),
            "not json": lambda request: httpx.Response(200, content=b"<html>maintenance</html>"),
            "connection refused": refused,
        }
        for name, respond in cases.items():
            with self.subTest(name):
                self._check(respond)
                self.assertEqual(updates._UPDATE_STATE["latest"], "")
                self.assertEqual(updates._UPDATE_STATE["checked_at"], 0.0)
                self.assertFalse(updates._update_available())


class VersionComparisonTests(unittest.TestCase):
    def test_versions_compare_numerically_not_as_text(self):
        self.assertGreater(updates._version_tuple("2.10.0"), updates._version_tuple("2.9.9"))
        self.assertGreater(updates._version_tuple("v2.0.1"), updates._version_tuple("2.0.0"))
        self.assertEqual(updates._version_tuple("v2.1.0-beta.1"), (2, 1, 0))
        self.assertEqual(updates._version_tuple(""), (0,))

    def test_availability_needs_a_strictly_newer_release(self):
        with patch.object(updates, "__version__", "2.0.1"):
            for latest, expected in (("2.0.2", True), ("2.1.0", True), ("2.0.1", False), ("2.0.0", False), ("", False)):
                with self.subTest(latest=latest), patch.dict(updates._UPDATE_STATE, {"latest": latest}):
                    self.assertEqual(updates._update_available(), expected)


if __name__ == "__main__":
    unittest.main()
