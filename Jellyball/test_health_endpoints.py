"""Tests for /healthz (liveness) and /readyz (readiness) in routes_api."""

import asyncio
import json
import sqlite3
import unittest
from unittest.mock import patch

import routes_api


class FakeConn:
    """Minimal sqlite3 stand-in for the DB writability check."""

    def __init__(self, fail_on=None):
        self.fail_on = fail_on

    def execute(self, sql, *args):
        if self.fail_on and self.fail_on in sql:
            raise sqlite3.OperationalError("database is locked")
        return self

    def fetchone(self):
        return (1,)


class DiskUsage:
    def __init__(self, free):
        self.free = free


class HealthzTests(unittest.TestCase):
    def test_healthz_is_a_static_liveness_answer(self):
        body = asyncio.run(routes_api.healthz())
        self.assertEqual(body["status"], "ok")
        self.assertEqual(body["app"], "jellyball")


class ReadinessCheckTests(unittest.TestCase):
    def test_database_ok_when_readable_and_writable(self):
        with patch.object(routes_api, "_connect_db", return_value=FakeConn()):
            self.assertEqual(routes_api._check_database(), {"status": "ok"})

    def test_database_fails_when_write_lock_unavailable(self):
        with patch.object(routes_api, "_connect_db", return_value=FakeConn(fail_on="BEGIN")):
            check = routes_api._check_database()
            self.assertEqual(check["status"], "fail")

    def test_database_fails_when_connect_raises(self):
        with patch.object(routes_api, "_connect_db", side_effect=sqlite3.OperationalError("nope")):
            self.assertEqual(routes_api._check_database()["status"], "fail")

    def test_disk_ok_warn_fail_thresholds(self):
        ok = routes_api._READYZ_DISK_WARN_BYTES + 1
        warn = routes_api._READYZ_DISK_WARN_BYTES - 1
        fail = routes_api._READYZ_DISK_FAIL_BYTES - 1
        with patch("shutil.disk_usage", return_value=DiskUsage(ok)):
            self.assertEqual(routes_api._check_disk()["status"], "ok")
        with patch("shutil.disk_usage", return_value=DiskUsage(warn)):
            self.assertEqual(routes_api._check_disk()["status"], "warn")
        with patch("shutil.disk_usage", return_value=DiskUsage(fail)):
            check = routes_api._check_disk()
            self.assertEqual(check["status"], "fail")
            self.assertIn("MiB free", check["detail"])

    def test_disk_fail_when_stat_raises(self):
        with patch("shutil.disk_usage", side_effect=OSError("nope")):
            self.assertEqual(routes_api._check_disk()["status"], "fail")

    def test_ffmpeg_ok_and_warn(self):
        with patch.object(routes_api.ffmpeg_proc, "FFMPEG_AVAILABLE", True):
            self.assertEqual(routes_api._check_ffmpeg()["status"], "ok")
        with patch.object(routes_api.ffmpeg_proc, "FFMPEG_AVAILABLE", False), \
                patch("shutil.which", return_value=None):
            check = routes_api._check_ffmpeg()
            # ffmpeg is only needed for Multi-View: warn, never fail.
            self.assertEqual(check["status"], "warn")


class ReadyzEndpointTests(unittest.TestCase):
    def _body(self, response):
        return json.loads(response.body.decode())

    def _run_readyz(self, checks):
        with patch.object(routes_api, "_run_readiness_checks", return_value=checks):
            return asyncio.run(routes_api.readyz())

    def test_all_ok_returns_200(self):
        checks = {
            "database": {"status": "ok"},
            "disk": {"status": "ok", "free_bytes": 10**10},
            "ffmpeg": {"status": "ok", "path": "ffmpeg"},
        }
        response = self._run_readyz(checks)
        self.assertEqual(response.status_code, 200)
        body = self._body(response)
        self.assertEqual(body["status"], "ok")
        self.assertEqual(body["checks"], checks)

    def test_warn_degrades_but_stays_200(self):
        checks = {
            "database": {"status": "ok"},
            "disk": {"status": "warn", "detail": "low"},
            "ffmpeg": {"status": "warn", "detail": "missing"},
        }
        response = self._run_readyz(checks)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self._body(response)["status"], "degraded")

    def test_failed_check_returns_503(self):
        checks = {
            "database": {"status": "fail", "detail": "OperationalError"},
            "disk": {"status": "ok"},
            "ffmpeg": {"status": "ok"},
        }
        response = self._run_readyz(checks)
        self.assertEqual(response.status_code, 503)
        self.assertEqual(self._body(response)["status"], "fail")

    def test_probe_timeout_is_a_503_not_a_hang(self):
        async def slow_checks(coro, timeout=None):
            coro.close()  # wait_for never runs it; close it so it isn't leaked
            raise asyncio.TimeoutError()

        with patch.object(routes_api.asyncio, "wait_for", side_effect=slow_checks):
            response = asyncio.run(routes_api.readyz())
        self.assertEqual(response.status_code, 503)
        self.assertEqual(self._body(response)["checks"]["probe"]["status"], "fail")


if __name__ == "__main__":
    unittest.main()
