"""tools/changelog.py: fragments are validated and folded into CHANGELOG.md."""

import importlib.util
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location("changelog_tool", HERE / "tools" / "changelog.py")
tool = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tool)

SAMPLE = """# Changelog

Intro.

## [Unreleased]

### Security

- Existing security note.

## [2.0.1] - 2026-10-02

### Fixed

- Old fix.
"""


def _write(directory, name, text):
    (Path(directory) / name).write_text(text, encoding="utf-8")


class FragmentTests(unittest.TestCase):
    def test_valid_fragments_sort_by_type_then_name(self):
        with tempfile.TemporaryDirectory() as d:
            _write(d, "b-thing.fixed.md", "Fixed b.")
            _write(d, "a-thing.fixed.md", "Fixed a.")
            _write(d, "z.added.md", "Added z.")
            _write(d, "README.md", "ignored")
            names = [f.name for f in tool.load_fragments(Path(d))]
        self.assertEqual(names, ["z.added.md", "a-thing.fixed.md", "b-thing.fixed.md"])

    def test_problems_are_all_reported(self):
        with tempfile.TemporaryDirectory() as d:
            _write(d, "bad name.fixed.md", "x")
            _write(d, "ok.nonsense.md", "x")
            _write(d, "empty.added.md", "  \n")
            _write(d, "bullet.added.md", "- already a bullet")
            with self.assertRaises(ValueError) as ctx:
                tool.load_fragments(Path(d))
        message = str(ctx.exception)
        for name in ("bad name.fixed.md", "ok.nonsense.md", "empty.added.md", "bullet.added.md"):
            self.assertIn(name, message)

    def test_a_missing_directory_means_no_fragments(self):
        self.assertEqual(tool.load_fragments(Path("does-not-exist-here")), [])

    def test_the_real_fragments_are_valid(self):
        tool.load_fragments()


class ReleaseTests(unittest.TestCase):
    def _fragments(self):
        return [
            tool.Fragment("a.added.md", "added", "New thing.\nSecond line."),
            tool.Fragment("s.security.md", "security", "Another security note."),
        ]

    def test_fragments_merge_into_existing_and_new_sections_in_order(self):
        out = tool.release(SAMPLE, self._fragments(), "2.1.0", "2026-11-01")
        self.assertIn("## [Unreleased]\n\n## [2.1.0] - 2026-11-01\n\n### Added\n\n- New thing.\n  Second line.\n\n"
                      "### Security\n\n- Existing security note.\n- Another security note.\n\n## [2.0.1]", out)
        self.assertTrue(out.endswith("### Fixed\n\n- Old fix.\n"))
        self.assertEqual(out.count("## [Unreleased]"), 1)

    def test_older_releases_are_untouched(self):
        out = tool.release(SAMPLE, self._fragments(), "2.1.0", "2026-11-01")
        self.assertEqual(out[out.index("## [2.0.1]"):], SAMPLE[SAMPLE.index("## [2.0.1]"):])

    def test_duplicate_version_and_empty_release_are_refused(self):
        with self.assertRaises(ValueError):
            tool.release(SAMPLE, [], "2.0.1", "2026-11-01")
        with self.assertRaises(ValueError):
            tool.release("## [Unreleased]\n\n## [2.0.1] - x\n", [], "2.1.0", "2026-11-01")

    def test_notes_returns_one_section(self):
        self.assertEqual(tool.notes(SAMPLE, "2.0.1"), "### Fixed\n\n- Old fix.")
        self.assertEqual(tool.notes(SAMPLE, "unreleased"), "### Security\n\n- Existing security note.")
        with self.assertRaises(ValueError):
            tool.notes(SAMPLE, "9.9.9")

    def test_real_changelog_has_notes_for_a_released_version(self):
        text = (tool.CHANGELOG).read_text(encoding="utf-8")
        self.assertTrue(tool.notes(text, "2.0.1"))


if __name__ == "__main__":
    unittest.main()
