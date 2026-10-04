"""Every route that is not deliberately public must demand credentials.

A new endpoint added by any feature lane is therefore protected by default. To
make one public, add its path to PUBLIC_BY_DESIGN with the reason: a decision a
reviewer will see in the diff, instead of an `auth` dependency someone forgot.
"""

import asyncio
import os
import re
import shutil
import tempfile
import unittest
import warnings
from unittest.mock import patch

import httpx
from fastapi.openapi.utils import get_openapi

import db
import epg
import main
import security

# Reachable without the dashboard password, on purpose.
PUBLIC_BY_DESIGN = {
    # Monitoring and Jellyfin's tuner: fetched with no credentials (README: Security model).
    "/healthz",
    "/playlist.m3u",
    "/epg.xml",
    # Playback: Jellyfin's ffmpeg and the player fetch these with no credentials.
    "/stream/{team_id}",
    "/stream/{team_id}.m3u8",
    "/stream/{team_id}/seg/{seq}.ts",
    "/multiview/{channel_id}/audio-{audio_index}.m3u8",
    "/multiview/{channel_id}/audio/{audio_index}.m3u8",
    "/multiview/{channel_id}/audio-{audio_index}/seg/{seq}.ts",
    "/multiview/{channel_id}/audio/{audio_index}/seg/{seq}.ts",
    # The loopback relay used by the stream sessions: protected by a signed URL, not a password.
    "/chunk",
    "/chunk.aac",
    "/chunk.mp4",
    "/chunk.ts",
    "/chunk.vtt",
    "/resource",
    "/substream.m3u8",
}


async def _no_tvguide():
    return {}


def _operations():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # duplicate operation ids from the HEAD variants
        schema = get_openapi(title="routes", version="0", routes=main.app.routes)
    return sorted((method.upper(), path) for path, ops in schema["paths"].items() for method in ops)


class AuthGuardrailTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self._db = patch.object(db, "DB_FILE", os.path.join(self.tmp, "guardrail.db"))
        self._db.start()
        db.init_db()

    def tearDown(self):
        self._db.stop()
        db.close_all_db_connections()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _anonymous_statuses(self):
        async def go():
            statuses = {}
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=main.app), base_url="http://127.0.0.1:8000",
            ) as client:
                for method, path in _operations():
                    if path in PUBLIC_BY_DESIGN:
                        continue
                    response = await client.request(method, re.sub(r"\{[^}]+\}", "x", path))
                    statuses[(method, path)] = response.status_code
            return statuses

        with patch.object(security, "DASHBOARD_PASSWORD", "secret"), \
                patch.object(security, "DASHBOARD_USERNAME", "admin"), \
                patch.object(epg, "_fetch_tvguide_epg", _no_tvguide):
            return asyncio.run(go())

    def test_every_route_outside_the_public_list_rejects_an_anonymous_request(self):
        statuses = self._anonymous_statuses()
        self.assertGreater(len(statuses), 30, "the route enumeration looks broken")
        open_routes = sorted(f"{method} {path} -> {status}" for (method, path), status in statuses.items() if status != 401)
        self.assertEqual(
            open_routes, [],
            "these routes answered without credentials; protect them with "
            "Depends(verify_dashboard_auth) or, if public on purpose, add them to PUBLIC_BY_DESIGN",
        )

    def test_the_public_list_names_only_routes_that_exist(self):
        existing = {path for _, path in _operations()}
        self.assertEqual(sorted(PUBLIC_BY_DESIGN - existing), [])


if __name__ == "__main__":
    unittest.main()
