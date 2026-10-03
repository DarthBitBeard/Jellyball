"""X3: /docs, /redoc and /openapi.json are off by default and, when enabled
with JELLYBALL_API_DOCS=1, sit behind the dashboard password."""

import asyncio
import os
import unittest
from unittest.mock import patch

import httpx

import http_hardening
import main
import security

DOC_PATHS = ("/docs", "/redoc", "/openapi.json")


def _get(path: str, *, enabled: bool, password: str = "secret", auth=None, headers=None) -> httpx.Response:
    async def go():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=main.app), base_url="http://127.0.0.1:8000", follow_redirects=False,
        ) as client:
            return await client.get(path, auth=auth, headers=headers)

    with patch.object(http_hardening, "API_DOCS_ENABLED", enabled), \
            patch.object(security, "DASHBOARD_PASSWORD", password), \
            patch.object(security, "DASHBOARD_USERNAME", "admin"), \
            patch.dict(security._AUTH_FAILURES, clear=True):
        return asyncio.run(go())


class ApiDocsTests(unittest.TestCase):
    def test_off_by_default_even_with_the_password(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("JELLYBALL_API_DOCS", None)
            self.assertFalse(http_hardening.api_docs_enabled())
        for path in DOC_PATHS:
            with self.subTest(path=path):
                self.assertEqual(_get(path, enabled=False, auth=("admin", "secret")).status_code, 404)

    def test_disabled_docs_do_not_answer_anonymous_requests(self):
        for path in DOC_PATHS:
            with self.subTest(path=path):
                self.assertIn(_get(path, enabled=False).status_code, (401, 404))
                self.assertNotIn("openapi", _get(path, enabled=False).text.lower())

    def test_enabled_docs_require_the_dashboard_password(self):
        for path in DOC_PATHS:
            with self.subTest(path=path):
                self.assertEqual(_get(path, enabled=True).status_code, 401)
                self.assertEqual(_get(path, enabled=True, auth=("admin", "wrong")).status_code, 401)
                self.assertEqual(_get(path, enabled=True, auth=("admin", "secret")).status_code, 200)

    def test_enabled_schema_describes_the_api(self):
        schema = _get("/openapi.json", enabled=True, auth=("admin", "secret")).json()
        self.assertEqual(schema["info"]["title"], "Jellyball")
        self.assertIn("/api/status", schema["paths"])

    def test_the_switch_reads_truthy_values_only(self):
        for value, expected in (("1", True), ("true", True), ("on", True), ("0", False), ("", False), ("no", False)):
            with self.subTest(value=value), patch.dict(os.environ, {"JELLYBALL_API_DOCS": value}):
                self.assertEqual(http_hardening.api_docs_enabled(), expected)

    def test_fastapi_builtin_doc_routes_are_not_mounted(self):
        self.assertIsNone(main.app.docs_url)
        self.assertIsNone(main.app.redoc_url)
        self.assertIsNone(main.app.openapi_url)


if __name__ == "__main__":
    unittest.main()
