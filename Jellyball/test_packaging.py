"""The PyInstaller spec must name every application module.

PyInstaller traces imports from the launcher, which imports `main` lazily, so
the spec lists the modules explicitly as well (see the comment above
`hiddenimports` in jellyball.spec). A module added to the tree but not to that
list would work from source and in Docker, and then be missing from the
installer, which is only built at release time.
"""

import ast
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent

# Tooling and the PyInstaller entry script itself: never imported by the app.
NOT_APPLICATION_MODULES = {"create_brand_assets", "jellyball_launcher"}


def _spec_hidden_imports() -> set:
    spec = (HERE / "jellyball.spec").read_text(encoding="utf-8")
    start = spec.index("hiddenimports = [") + len("hiddenimports = ")
    end = spec.index("\n]", start) + 2
    return set(ast.literal_eval(spec[start:end]))


class SpecListsEveryModuleTests(unittest.TestCase):
    def test_every_application_module_is_a_hidden_import(self):
        modules = {p.stem for p in HERE.glob("*.py") if not p.name.startswith("test_")}
        missing = sorted(modules - NOT_APPLICATION_MODULES - _spec_hidden_imports())
        self.assertEqual(
            missing, [],
            "add these modules to hiddenimports in jellyball.spec: " + ", ".join(missing),
        )

    def test_the_spec_does_not_name_a_module_that_was_deleted(self):
        # Only our own naming families can be checked; third-party names (pystray, PIL, ...) are fine.
        modules = {p.stem for p in HERE.glob("*.py")}
        stale = sorted(
            name for name in _spec_hidden_imports()
            if name.startswith(("routes_", "migrations_")) and name not in modules
        )
        self.assertEqual(stale, [])

    def test_templates_and_static_files_are_bundled_by_walking_their_directories(self):
        # New partials and per-lane scripts are picked up without editing the spec only
        # while it walks these directories recursively.
        spec = (HERE / "jellyball.spec").read_text(encoding="utf-8")
        self.assertIn('for _dashboard_dir_name in ("templates", "static"):', spec)
        self.assertIn("rglob", spec)


if __name__ == "__main__":
    unittest.main()
