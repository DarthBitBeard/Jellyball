"""ffmpeg run-roots registry (ffmpeg_proc.register_run_root / run_roots): the
startup kill of orphaned ffmpeg, the periodic sweep and the lifespan wipe
cover every registered root, the built-in Multi-View and placeholder roots
included. Runs on any OS: the Windows process API is replaced by a fake."""

import asyncio
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import ffmpeg_proc
import multiview
import placeholder
from ffmpeg_proc import RUN_PID_FILE, RunRoot


class _FakeProcessApi:
    def __init__(self):
        self.killed = []

    def terminate_pid_if_created_at(self, pid, created):
        self.killed.append((pid, created))
        return True


def _make_run(path: Path, *, age: float = 3600.0, pid: int = 0) -> Path:
    path.mkdir(parents=True)
    (path / "seg_000001.ts").write_bytes(b"x")
    if pid:
        (path / RUN_PID_FILE).write_text(json.dumps({"pid": pid, "created": pid * 10}), encoding="utf-8")
    old = time.time() - age
    os.utime(path, (old, old))
    return path


class _RegistryTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.mv_root, self.ph_root = self.tmp / "multiview", self.tmp / "placeholder"
        # Work on a copy so registrations made here never leak into other tests.
        self._patches = [
            patch.object(ffmpeg_proc, "_RUN_ROOTS", list(ffmpeg_proc._RUN_ROOTS)),
            patch.object(multiview, "MULTIVIEW_OUTPUT_ROOT", self.mv_root),
            patch.object(placeholder, "PLACEHOLDER_OUTPUT_DIR", self.ph_root),
            patch.object(placeholder, "_PLACEHOLDER_STATE", None),
            patch.object(multiview, "_MULTIVIEW_PROCESSES", {}),
            patch.object(multiview, "_MULTIVIEW_PENDING_RUN_DIRS", set()),
        ]
        for p in self._patches:
            p.start()

    def tearDown(self):
        for p in reversed(self._patches):
            p.stop()
        self._tmp.cleanup()


class RegistrySemanticsTests(_RegistryTestCase):
    def test_builtin_roots_are_registered_and_resolved_at_call_time(self):
        roots = ffmpeg_proc.run_roots()
        self.assertIn(RunRoot(self.mv_root, True), roots)
        self.assertIn(RunRoot(self.ph_root, False), roots)

    def test_register_a_path_or_a_callable(self):
        fixed, moving = self.tmp / "fixed", [self.tmp / "a"]
        ffmpeg_proc.register_run_root(fixed)
        ffmpeg_proc.register_run_root(lambda: moving[0], per_channel=True)
        self.assertEqual(ffmpeg_proc.run_roots()[-2:], [RunRoot(fixed, False), RunRoot(self.tmp / "a", True)])
        moving[0] = self.tmp / "b"
        self.assertEqual(ffmpeg_proc.run_roots()[-1], RunRoot(self.tmp / "b", True))

    def test_live_run_dirs_unions_every_owner(self):
        mine = self.tmp / "extra" / "run1"
        ffmpeg_proc.register_run_root(self.tmp / "extra", live_dirs=lambda: [mine])
        ffmpeg_proc.register_run_root(self.tmp / "no_live")
        multiview._MULTIVIEW_PENDING_RUN_DIRS.add(self.mv_root / "mv" / "run2")
        multiview._MULTIVIEW_PROCESSES["mv"] = {"output_dir": self.mv_root / "mv" / "run3"}
        placeholder._PLACEHOLDER_STATE = {"output_dir": self.ph_root / "run4"}
        self.assertEqual(ffmpeg_proc.live_run_dirs(), {
            mine, self.mv_root / "mv" / "run2", self.mv_root / "mv" / "run3", self.ph_root / "run4",
        })

    def test_pid_files_follow_each_root_layout(self):
        extra = self.tmp / "extra"
        ffmpeg_proc.register_run_root(extra)
        expected = {
            _make_run(self.mv_root / "mv" / "run1", pid=11) / RUN_PID_FILE,
            _make_run(self.ph_root / "run2", pid=12) / RUN_PID_FILE,
            _make_run(extra / "run3", pid=13) / RUN_PID_FILE,
        }
        _make_run(self.ph_root / "nested" / "run9", pid=19)  # wrong depth for a flat root
        self.assertEqual(set(ffmpeg_proc.run_pid_files()), expected)


class KillAndSweepTests(_RegistryTestCase):
    def test_orphans_are_killed_in_every_registered_root(self):
        extra = self.tmp / "recordings"
        ffmpeg_proc.register_run_root(extra, per_channel=True)
        _make_run(self.mv_root / "mv" / "run1", pid=101)
        _make_run(self.ph_root / "run2", pid=102)
        _make_run(extra / "rec" / "run3", pid=103)
        api = _FakeProcessApi()
        with patch.object(multiview, "_win32_process_api", return_value=api), \
                patch.object(multiview.LOGGER, "warning"):
            self.assertEqual(multiview._kill_orphaned_ffmpeg(), 3)
        self.assertEqual(sorted(api.killed), [(101, 1010), (102, 1020), (103, 1030)])

    def test_no_process_api_means_nothing_is_killed(self):
        _make_run(self.ph_root / "run2", pid=102)
        with patch.object(multiview, "_win32_process_api", return_value=None):
            self.assertEqual(multiview._kill_orphaned_ffmpeg(), 0)

    def test_a_newly_registered_root_is_swept_like_the_builtin_ones(self):
        extra = self.tmp / "recordings"
        in_use = extra / "run7"
        ffmpeg_proc.register_run_root(extra, live_dirs=lambda: {in_use})
        stale = [
            _make_run(self.mv_root / "mv" / "run1"),
            _make_run(self.ph_root / "run2"),
            _make_run(extra / "run3"),
        ]
        kept = [
            _make_run(in_use),                       # reported live by its owner
            _make_run(extra / "run8", age=0.0),      # too young: may be a spawn in progress
            _make_run(self.ph_root / "run9", age=0.0),
        ]
        removed = asyncio.run(multiview._sweep_output_dirs(min_age=60.0))
        for path in stale:
            self.assertFalse(path.exists(), path)
        for path in kept:
            self.assertTrue(path.exists(), path)
        # The emptied Multi-View channel dir goes too; flat roots have none.
        self.assertEqual(set(removed), {*stale, self.mv_root / "mv"})


if __name__ == "__main__":
    unittest.main()
