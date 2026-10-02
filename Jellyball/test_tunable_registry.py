"""Feature modules add Advanced Settings through tunables.register_tunables
instead of editing the shared TUNABLES list (Phase A pre-wiring)."""

import types
import unittest
from unittest.mock import patch

import tunables
from tunables import Tunable, register_tunables


class RegisterTunablesTests(unittest.TestCase):
    def setUp(self):
        # Work on copies so a registration here cannot leak into other tests.
        self._patches = [
            patch.object(tunables, "TUNABLES", list(tunables.TUNABLES)),
            patch.object(tunables, "_TUNABLES_BY_NAME", dict(tunables._TUNABLES_BY_NAME)),
            patch.object(tunables, "_TUNABLE_DEFAULTS", dict(tunables._TUNABLE_DEFAULTS)),
            patch.object(tunables, "_TUNABLE_TARGET_MODULES", list(tunables._TUNABLE_TARGET_MODULES)),
        ]
        for p in self._patches:
            p.start()
        self.module = types.ModuleType("fake_lane_module")
        self.module.GUIDE_REFRESH_SECONDS = 600.0
        self.tunable = Tunable("GUIDE_REFRESH_SECONDS", "Guide refresh interval (s)", "Guide", float, 60, 86400,
                               "GUIDE_REFRESH_SECONDS", "How often the guide is rebuilt.")

    def tearDown(self):
        for p in reversed(self._patches):
            p.stop()

    def test_a_registered_tunable_is_listed_with_its_current_value_as_the_default(self):
        register_tunables(self.module, self.tunable)
        self.assertIn(self.tunable, tunables.TUNABLES)
        self.assertEqual(tunables._TUNABLE_DEFAULTS["GUIDE_REFRESH_SECONDS"], 600.0)
        groups = {group["name"]: group["fields"] for group in tunables._advanced_settings_html()}
        self.assertEqual([field["name"] for field in groups["Guide"]], ["GUIDE_REFRESH_SECONDS"])
        self.assertFalse(groups["Guide"][0]["overridden"])

    def test_applying_a_value_rebinds_the_global_on_the_owning_module(self):
        register_tunables(self.module, self.tunable)
        tunables._apply_tunable(self.tunable, 120.0)
        self.assertEqual(self.module.GUIDE_REFRESH_SECONDS, 120.0)
        groups = {group["name"]: group["fields"] for group in tunables._advanced_settings_html()}
        self.assertTrue(groups["Guide"][0]["overridden"])
        self.assertEqual(groups["Guide"][0]["value"], 120.0)

    def test_a_saved_override_is_applied_at_startup_and_clamped(self):
        register_tunables(self.module, self.tunable)
        saved = {self.tunable.key: "5"}  # below the minimum of 60
        with patch.object(tunables, "get_setting", lambda key, default="": saved.get(key, "")):
            tunables._load_tunable_overrides()
        self.assertEqual(self.module.GUIDE_REFRESH_SECONDS, 60.0)

    def test_the_default_is_read_at_registration_not_after_an_override(self):
        register_tunables(self.module, self.tunable)
        tunables._apply_tunable(self.tunable, 90.0)
        self.assertEqual(tunables._TUNABLE_DEFAULTS["GUIDE_REFRESH_SECONDS"], 600.0)

    def test_a_duplicate_name_is_rejected_and_registers_nothing(self):
        register_tunables(self.module, self.tunable)
        before = len(tunables.TUNABLES)
        with self.assertRaises(ValueError):
            register_tunables(self.module, self.tunable)
        with self.assertRaises(ValueError):
            register_tunables(self.module, Tunable("IDLE_HEALTH_INTERVAL", "x", "g", float, 1, 2, "GUIDE_REFRESH_SECONDS"))
        self.assertEqual(len(tunables.TUNABLES), before)

    def test_a_target_the_module_does_not_have_is_rejected(self):
        broken = Tunable("NO_SUCH_GLOBAL_SETTING", "x", "Guide", int, 1, 2, "NO_SUCH_GLOBAL")
        with self.assertRaises(ValueError):
            register_tunables(self.module, broken)
        self.assertNotIn(broken, tunables.TUNABLES)
        self.assertNotIn(self.module, tunables._TUNABLE_TARGET_MODULES)

    def test_a_session_config_target_needs_no_module_global(self):
        session_setting = Tunable("SESSION_FAKE_SETTING", "x", "Channel sessions", int, 1, 5, "session.fail_threshold")
        register_tunables(self.module, session_setting)
        self.assertIn(session_setting, tunables.TUNABLES)

    def test_the_built_in_tunables_are_unchanged(self):
        names = [t.name for t in tunables.TUNABLES]
        self.assertEqual(len(names), len(set(names)))
        self.assertIn("IDLE_HEALTH_INTERVAL", names)
        self.assertIn("MULTIVIEW_IDLE_TIMEOUT_SECONDS", names)


if __name__ == "__main__":
    unittest.main()
