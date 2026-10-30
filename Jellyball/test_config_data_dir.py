"""Tests must not touch the developer's real data directory."""

import os
import tempfile
import unittest
from pathlib import Path

import config


class TestDataDirTests(unittest.TestCase):
    def test_a_unittest_run_gets_a_temporary_data_directory(self):
        self.assertTrue(config._running_under_unittest())
        self.assertTrue(
            Path(config.DATA_DIR).resolve().is_relative_to(Path(tempfile.gettempdir()).resolve()),
            f"{config.DATA_DIR} is outside the temp directory",
        )
        real = os.getenv("LOCALAPPDATA")
        if real:
            self.assertNotEqual(Path(config.DATA_DIR).resolve(), (Path(real) / "Jellyball").resolve())

    def test_an_explicit_override_still_wins(self):
        previous = os.environ.get("JELLYBALL_DATA_DIR")
        with tempfile.TemporaryDirectory() as chosen:
            os.environ["JELLYBALL_DATA_DIR"] = chosen
            try:
                self.assertEqual(config._get_writable_data_dir(), Path(chosen))
            finally:
                if previous is None:
                    os.environ.pop("JELLYBALL_DATA_DIR", None)
                else:
                    os.environ["JELLYBALL_DATA_DIR"] = previous


if __name__ == "__main__":
    unittest.main()
