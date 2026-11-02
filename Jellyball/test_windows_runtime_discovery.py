"""Windows runs from source: Chromium and ffmpeg are found where the README's
install steps put them, and a missing Chromium fails cheaply and clearly."""

import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import config
import ffmpeg_proc
import scrapers


class PlaywrightBrowserLocationTests(unittest.TestCase):
    def _fake_package(self, root: Path, with_local_browser: bool):
        package = root / "playwright"
        (package / "driver" / "package").mkdir(parents=True)
        if with_local_browser:
            (package / "driver" / "package" / ".local-browsers" / "chromium_headless_shell-1").mkdir(parents=True)
        return SimpleNamespace(origin=str(package / "__init__.py"))

    def test_browsers_inside_the_package_are_detected(self):
        with tempfile.TemporaryDirectory() as tmp:
            spec = self._fake_package(Path(tmp), with_local_browser=True)
            with patch("importlib.util.find_spec", return_value=spec):
                self.assertTrue(config._playwright_package_local_browsers_installed())

    def test_the_default_cache_is_used_when_the_package_has_no_browser(self):
        # `python -m playwright install chromium` (the README step) installs into the
        # per-user cache, not the package; forcing "0" then hid the browser.
        with tempfile.TemporaryDirectory() as tmp:
            spec = self._fake_package(Path(tmp), with_local_browser=False)
            with patch("importlib.util.find_spec", return_value=spec):
                self.assertFalse(config._playwright_package_local_browsers_installed())

    def test_no_playwright_package(self):
        with patch("importlib.util.find_spec", return_value=None):
            self.assertFalse(config._playwright_package_local_browsers_installed())


class FfmpegDiscoveryTests(unittest.TestCase):
    def test_ffmpeg_on_path_wins(self):
        with patch.object(ffmpeg_proc.shutil, "which", return_value=r"C:\tools\ffmpeg.exe"):
            self.assertEqual(ffmpeg_proc._default_ffmpeg_path(), "ffmpeg")

    def test_jellyfins_ffmpeg_is_used_when_none_is_on_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            jellyfin = Path(tmp) / "ffmpeg.exe"
            jellyfin.write_bytes(b"")
            with patch.object(ffmpeg_proc.shutil, "which", return_value=None), \
                    patch.object(ffmpeg_proc, "_jellyfin_ffmpeg_path", return_value=str(jellyfin)):
                self.assertEqual(ffmpeg_proc._default_ffmpeg_path(), str(jellyfin))

    @unittest.skipUnless(os.name == "nt", "Windows install location")
    def test_the_readme_folder_is_used_on_windows(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "ffmpeg" / "bin" / "ffmpeg.exe"
            target.parent.mkdir(parents=True)
            target.write_bytes(b"")
            with patch.object(ffmpeg_proc.shutil, "which", return_value=None), \
                    patch.object(ffmpeg_proc, "_jellyfin_ffmpeg_path", return_value=None), \
                    patch.dict(os.environ, {"LOCALAPPDATA": tmp}):
                self.assertEqual(ffmpeg_proc._default_ffmpeg_path(), str(target))

    def test_falls_back_to_the_plain_name(self):
        with patch.object(ffmpeg_proc.shutil, "which", return_value=None), \
                patch.object(ffmpeg_proc, "_jellyfin_ffmpeg_path", return_value=None), \
                patch.dict(os.environ, {"LOCALAPPDATA": ""}):
            self.assertEqual(ffmpeg_proc._default_ffmpeg_path(), "ffmpeg")


class _FailingPlaywright:
    starts = 0

    async def start(self):
        type(self).starts += 1
        raise RuntimeError("BrowserType.launch: Executable doesn't exist at /opt/x/chrome-headless-shell")


class BrowserRelaunchCooldownTests(unittest.TestCase):
    def setUp(self):
        _FailingPlaywright.starts = 0
        self._patches = [
            patch.object(scrapers, "SHARED_BROWSER", None),
            patch.object(scrapers, "PLAYWRIGHT_CLIENT", None),
            patch.object(scrapers, "_BROWSER_LAUNCH_RETRY_AT", 0.0),
            patch.object(scrapers, "async_playwright", lambda: _FailingPlaywright()),
        ]
        for p in self._patches:
            p.start()

    def tearDown(self):
        for p in reversed(self._patches):
            p.stop()

    def test_a_failed_launch_is_not_retried_by_every_caller(self):
        async def run():
            return [await scrapers.get_healthy_browser() for _ in range(5)]

        with self.assertLogs("jellyball", level="ERROR") as logs:
            results = asyncio.run(run())
        self.assertEqual(results, [None] * 5)
        self.assertEqual(_FailingPlaywright.starts, 1)
        self.assertIn("python -m playwright install chromium", "\n".join(logs.output))

    def test_the_launch_is_retried_after_the_cooldown(self):
        async def run():
            await scrapers.get_healthy_browser()
            scrapers._BROWSER_LAUNCH_RETRY_AT = 0.0  # cooldown elapsed
            await scrapers.get_healthy_browser()

        with self.assertLogs("jellyball", level="ERROR"):
            asyncio.run(run())
        self.assertEqual(_FailingPlaywright.starts, 2)


if __name__ == "__main__":
    unittest.main()
