"""Subprocess wrappers around tools/e2e_failover.py and tools/e2e_multiview.py.

Those two scripts are real end-to-end checks: they start fake HLS origins,
run Jellyball in-process, and drive a real ffmpeg against it the way
Jellyfin's live-TV transcode does (see each script's own docstring). Nothing
runs them today, so this file wraps them as ordinary unittest cases that
subprocess out to `sys.executable tools/e2e_*.py` with shortened durations
and assert a clean (PASS) exit code.

They are skipped unless BOTH are true:
  * JELLYBALL_E2E=1 is set, and
  * an ffmpeg binary can actually be found - via FFMPEG_PATH, else PATH,
    else %LOCALAPPDATA%/ffmpeg/bin/ffmpeg.exe (the same order the tools
    scripts themselves fall back through).

so the normal `python -m unittest discover -p "test_*.py"` run (and CI) stays
fast and has no ffmpeg dependency. Run explicitly with, e.g.:

    JELLYBALL_E2E=1 python -m unittest test_e2e_tools -v
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import unittest
from pathlib import Path
from typing import Optional

HERE = Path(__file__).resolve().parent
TOOLS_DIR = HERE / "tools"


def _find_ffmpeg() -> Optional[str]:
    path_env = os.environ.get("FFMPEG_PATH")
    if path_env and Path(path_env).is_file():
        return path_env
    on_path = shutil.which("ffmpeg")
    if on_path:
        return on_path
    default = Path(os.environ.get("LOCALAPPDATA", "")) / "ffmpeg" / "bin" / "ffmpeg.exe"
    if default.is_file():
        return str(default)
    return None


FFMPEG_PATH = _find_ffmpeg()
E2E_REQUESTED = os.environ.get("JELLYBALL_E2E", "").strip() == "1"
E2E_ENABLED = E2E_REQUESTED and FFMPEG_PATH is not None

if E2E_REQUESTED and FFMPEG_PATH is None:
    SKIP_REASON = "JELLYBALL_E2E=1 but no ffmpeg found (FFMPEG_PATH/PATH/%LOCALAPPDATA%/ffmpeg/bin/ffmpeg.exe)"
else:
    SKIP_REASON = "set JELLYBALL_E2E=1 (and have ffmpeg available) to run the e2e tool wrappers"


@unittest.skipUnless(E2E_ENABLED, SKIP_REASON)
class E2EToolWrapperTests(unittest.TestCase):
    """Runs the real tools/e2e_*.py scripts as subprocesses.

    Durations are shortened from the scripts' own defaults just to keep a
    manual `JELLYBALL_E2E=1` run reasonably quick; both scripts print a
    human-readable PASS/FAIL breakdown (kept in the failure message here) of
    exactly what they checked, e.g. failover discontinuity/active-candidate
    for e2e_failover.py, and encoder/audio-channel/set-audio/pid-stability
    for e2e_multiview.py.
    """

    def _run_tool(self, script_name: str, *extra_args: str, timeout: float) -> None:
        script_path = TOOLS_DIR / script_name
        self.assertTrue(script_path.is_file(), f"missing tool script: {script_path}")
        cmd = [sys.executable, str(script_path), "--ffmpeg", FFMPEG_PATH, *extra_args]
        try:
            result = subprocess.run(
                cmd,
                cwd=HERE,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            self.fail(f"{script_name} did not finish within {timeout}s: {exc}")
        if result.returncode != 0:
            self.fail(
                f"{script_name} exited with {result.returncode}\n"
                f"--- stdout ---\n{result.stdout}\n"
                f"--- stderr ---\n{result.stderr}"
            )

    def test_e2e_failover(self) -> None:
        # Shortened from the script's defaults (45s run / kill at 15s); this
        # still comfortably exercises startup, mid-run failover at 8s, and
        # enough post-failover playback to confirm the session stays on B.
        self._run_tool("e2e_failover.py", "--seconds", "20", "--kill-at", "8", timeout=120)

    def test_e2e_multiview(self) -> None:
        # Shortened from the script's default (40s); the audio switch fires
        # at seconds/2 and the script keeps checking for a while afterward,
        # so this still exercises grid encode, per-audio outputs, the
        # /multiview/*/audio-1.m3u8 route, and the set-audio PID-stability
        # check.
        self._run_tool("e2e_multiview.py", "--seconds", "24", timeout=150)


if __name__ == "__main__":
    unittest.main()
