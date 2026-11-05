"""The packaged app must carry its provider plugins (#48: the 3.0.0 Windows
installer loaded none and every search found nothing), and Jellyfin's ffmpeg
is found where its installer puts it."""

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import ffmpeg_proc
import plugins

HERE = Path(__file__).resolve().parent


class SpecBundlesPluginsTests(unittest.TestCase):
    def setUp(self):
        self.spec = (HERE / "jellyball.spec").read_text(encoding="utf-8")

    def test_plugin_modules_are_hidden_imports(self):
        self.assertIn('hiddenimports += collect_submodules("plugins")', self.spec)

    def test_plugin_manifests_are_bundled_as_data(self):
        self.assertIn('_plugins_dir = ROOT / "plugins"', self.spec)
        # Every built-in plugin needs its manifest.json on disk for discovery.
        manifests = sorted(p.parent.name for p in (HERE / "plugins" / "builtin").glob("*/manifest.json"))
        self.assertGreaterEqual(len(manifests), 5)


class NoBuiltinPluginsWarningTests(unittest.TestCase):
    def test_an_empty_builtin_folder_is_reported_as_a_packaging_problem(self):
        with tempfile.TemporaryDirectory() as empty:
            with patch.object(plugins, "BUILTIN_DIR", Path(empty)), \
                    patch.object(plugins, "THIRD_PARTY_DIR", Path(empty) / "none"), \
                    self.assertLogs("jellyball", level="ERROR") as logs:
                records = plugins.load_plugins()
        try:
            self.assertEqual(records, [])
            self.assertIn("No built-in provider plugins were loaded", "\n".join(logs.output))
        finally:
            plugins.load_plugins()  # restore the real registry for other tests

    def test_normal_load_has_builtins_and_no_error(self):
        records = plugins.load_plugins()
        self.assertTrue(any(r.builtin for r in records))

    def test_frozen_third_party_folder_is_in_the_data_folder(self):
        import config

        with patch("sys.frozen", True, create=True):
            self.assertEqual(plugins._third_party_dir(), Path(config.DATA_DIR) / "plugins" / "third_party")
        self.assertEqual(plugins._third_party_dir(), plugins.PLUGIN_DIR / "third_party")


@unittest.skipUnless(os.name == "nt", "Windows registry and install paths")
class JellyfinFfmpegLocationTests(unittest.TestCase):
    def test_the_service_folder_is_tried_first(self):
        with tempfile.TemporaryDirectory() as tmp:
            ffmpeg = Path(tmp) / "ffmpeg.exe"
            ffmpeg.write_bytes(b"")
            with patch.object(ffmpeg_proc, "_jellyfin_service_dir", return_value=Path(tmp)):
                self.assertEqual(ffmpeg_proc._jellyfin_ffmpeg_path(), str(ffmpeg))

    def test_service_image_path_parsing(self):
        import winreg

        class FakeKey:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        with patch.object(winreg, "OpenKey", return_value=FakeKey()), \
                patch.object(winreg, "QueryValueEx", return_value=('"C:\Jellyfin\Server\jellyfin.exe" --service', 1)):
            self.assertEqual(ffmpeg_proc._jellyfin_service_dir(), Path(r"C:\Jellyfin\Server"))


class EntryModuleLookupTests(unittest.TestCase):
    def test_builtins_are_found_by_module_name_not_by_file(self):
        with tempfile.TemporaryDirectory() as no_files_here:
            # The packaged app has no provider.py beside the manifest.
            self.assertTrue(plugins._entry_module_exists("daddylive", "builtin", Path(no_files_here), "provider.py"))
        self.assertFalse(plugins._entry_module_exists("no_such_plugin", "builtin", HERE, "provider.py"))

    def test_third_party_plugins_need_the_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertFalse(plugins._entry_module_exists("x", "third_party", Path(tmp), "provider.py"))
            (Path(tmp) / "provider.py").write_text("", encoding="utf-8")
            self.assertTrue(plugins._entry_module_exists("x", "third_party", Path(tmp), "provider.py"))


if __name__ == "__main__":
    unittest.main()
