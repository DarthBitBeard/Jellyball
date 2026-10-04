"""The shared "No Signal" placeholder stream (one ffmpeg run, started on demand)."""

import asyncio
import logging
import os
import sys
import time
from collections import deque
from pathlib import Path
from typing import List, Optional, Set, Tuple

from config import _log_failure, _resource_path, DATA_DIR, LOGGER
from state import _spawn_background_task
import ffmpeg_proc
from ffmpeg_proc import (
    _create_run_job,
    _drain_multiview_log,
    _ffmpeg_creationflags,
    _ffmpeg_filter_path,
    _multiview_backoff_seconds,
    _multiview_error_from_log,
    _prepare_run_dir,
    _remove_tree_later,
    _terminate_ffmpeg,
    _wait_for_first_segment,
    _write_run_pid_file,
    FFMPEG_PATH,
)
from network_safety import bounded_float


# --- SHARED "NO SIGNAL" PLACEHOLDER ---
# A single, always-on synthetic HLS stream shown for ANY channel (regular team,
# always-live special channel, or Multi-View member) that currently has no
# live candidates - instead of a bare 404. This turns Jellyfin's ugly "fatal
# player error" into a clean "no signal" screen, and - since Multi-View's
# ffmpeg pulls every member through this exact same /stream/{team_id} route -
# also transparently fixes a Multi-View channel refusing to start entirely
# just because one of its members isn't currently live: that member's input
# simply resolves to this placeholder instead of a 404, so ffmpeg's -i for it
# always succeeds.
PLACEHOLDER_OUTPUT_DIR = DATA_DIR / "placeholder"
PLACEHOLDER_STARTUP_TIMEOUT_SECONDS = 20.0
PLACEHOLDER_IDLE_SECONDS = bounded_float(os.getenv("PLACEHOLDER_IDLE_SECONDS", "900"), 900.0, 60.0, 86400.0)
_PLACEHOLDER_STATE: Optional[dict] = None
_PLACEHOLDER_LOCK = asyncio.Lock()
# Bumped per placeholder ffmpeg start: names its run dir and is part of its
# source key, so channel sessions treat a restarted placeholder (segment
# numbers back at 0) as a new source instead of a lagging edge.
_PLACEHOLDER_RUN_COUNTER = 0
# Last spawn failure(s): {"count", "last_failure", "last_error"}. The next start
# waits _multiview_backoff_seconds(count), so a broken placeholder ffmpeg is
# no longer respawned (and logged as an ERROR) on every session poll.
_PLACEHOLDER_FAILURE: Optional[dict] = None
# Turned off once drawtext failed with this ffmpeg build (no libfreetype, no
# usable font): later starts render the logo without the caption.
_PLACEHOLDER_DRAWTEXT_OK = True
_PLACEHOLDER_PENDING_DIRS: Set[Path] = set()
_DRAWTEXT_FAILURE_MARKERS = (
    "drawtext", "fontconfig", "freetype", "fontfile", "font file", "could not load font", "cannot find a valid font",
)


def _placeholder_live_run_dirs() -> Set[Path]:
    live = set(_PLACEHOLDER_PENDING_DIRS)
    if _PLACEHOLDER_STATE:
        live.add(_PLACEHOLDER_STATE["output_dir"])
    return live


# Read at call time, so a patched PLACEHOLDER_OUTPUT_DIR is what gets swept.
ffmpeg_proc.register_run_root(lambda: PLACEHOLDER_OUTPUT_DIR, live_dirs=_placeholder_live_run_dirs)


def _placeholder_font_option() -> str:
    """drawtext font selection. Bundled Windows ffmpeg builds usually have no
    fontconfig configuration, so font='Sans' fails or falls back unpredictably
    there: point at a real Windows font file instead."""
    if sys.platform == "win32":
        fonts_dir = Path(os.environ.get("WINDIR") or os.environ.get("SystemRoot") or r"C:\Windows") / "Fonts"
        for name in ("segoeui.ttf", "arial.ttf", "tahoma.ttf", "verdana.ttf"):
            candidate = fonts_dir / name
            if candidate.is_file():
                return f"fontfile='{_ffmpeg_filter_path(candidate)}':"
        return ""
    return "font='Sans':"


def _build_placeholder_ffmpeg_args(out_dir: Path, drawtext: bool = True) -> List[str]:
    """Pure command-builder for the shared "No Signal" loop (no I/O besides
    locating a font, unit-testable)."""
    logo_path = _resource_path("assets/jellyball-logo.png")
    video_filter = (
        "scale=1920:1080:force_original_aspect_ratio=decrease,"
        "pad=1920:1080:(ow-iw)/2:(oh-ih)/2"
    )
    if drawtext:
        video_filter += (
            f",drawtext={_placeholder_font_option()}text='No Signal':fontcolor=white:fontsize=64:"
            "box=1:boxcolor=black@0.5:boxborderw=16:x=(w-text_w)/2:y=h-200"
        )
    # -re paces both inputs at real time: without it ffmpeg encoded this loop as
    # fast as the CPU allowed (pinning a core and racing segments far ahead of
    # the wall clock). 1080p30 with 2s segments so a channel that starts on the
    # placeholder and then goes live doesn't make Jellyfin lock in a low
    # resolution/frame rate from its initial probe, and so a cold start has a
    # playable buffer within a few seconds.
    return [
        "-y",
        "-hide_banner", "-nostats", "-loglevel", "warning",
        "-re", "-loop", "1", "-framerate", "30", "-i", str(logo_path),
        "-re", "-f", "lavfi", "-i", "anullsrc=r=48000:cl=stereo",
        "-vf", video_filter,
        "-c:v", "libx264", "-preset", "ultrafast", "-tune", "stillimage",
        "-b:v", "1M", "-maxrate", "1M", "-bufsize", "2M",
        "-g", "60", "-keyint_min", "60", "-sc_threshold", "0", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "64k", "-ac", "2", "-ar", "48000",
        "-f", "hls",
        "-hls_time", "2",
        "-hls_list_size", "8",
        "-hls_flags", "delete_segments+independent_segments",
        "-hls_segment_type", "mpegts",
        "-hls_segment_filename", "seg_%05d.ts",
        "index.m3u8",
    ]


def _looks_like_drawtext_failure(log_lines) -> bool:
    text = "\n".join(str(line) for line in log_lines).lower()
    return any(marker in text for marker in _DRAWTEXT_FAILURE_MARKERS)


def _placeholder_cooldown_remaining() -> float:
    record = _PLACEHOLDER_FAILURE
    if not record:
        return 0.0
    elapsed = time.monotonic() - record["last_failure"]
    return max(0.0, _multiview_backoff_seconds(record["count"]) - elapsed)


def _record_placeholder_failure(error: str) -> float:
    global _PLACEHOLDER_FAILURE
    record = _PLACEHOLDER_FAILURE or {"count": 0, "last_failure": 0.0, "last_error": ""}
    record["count"] += 1
    record["last_failure"] = time.monotonic()
    record["last_error"] = error
    _PLACEHOLDER_FAILURE = record
    return _multiview_backoff_seconds(record["count"])


async def _drain_placeholder_log(process: "asyncio.subprocess.Process", log_lines: "deque[str]") -> None:
    # Same chunked reader as Multi-View (readline() can die on CR-only progress output).
    await _drain_multiview_log("placeholder", process, log_lines)


async def _watch_placeholder_process(process: "asyncio.subprocess.Process") -> None:
    returncode = await process.wait()
    if _PLACEHOLDER_STATE and _PLACEHOLDER_STATE["process"] is process:
        _PLACEHOLDER_STATE["exited"] = True
        if returncode != 0:
            LOGGER.warning("placeholder ffmpeg exited unexpectedly code=%s", returncode)


async def _stop_placeholder_process() -> None:
    global _PLACEHOLDER_STATE
    state, _PLACEHOLDER_STATE = _PLACEHOLDER_STATE, None
    if not state:
        return
    await _terminate_ffmpeg(state, "placeholder", term_timeout=5.0, kill_timeout=3.0)
    tasks = [state.get(key) for key in ("watch_task", "log_task") if state.get(key)]
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    _remove_tree_later(state["output_dir"], "placeholder")


async def _launch_placeholder_run(drawtext: bool) -> Tuple[bool, List[str]]:
    """One placeholder ffmpeg attempt (caller holds _PLACEHOLDER_LOCK):
    (ready, log lines of a failed attempt)."""
    global _PLACEHOLDER_STATE, _PLACEHOLDER_RUN_COUNTER
    _PLACEHOLDER_RUN_COUNTER += 1
    run_id = _PLACEHOLDER_RUN_COUNTER
    out_dir = PLACEHOLDER_OUTPUT_DIR / f"run{run_id}"
    _PLACEHOLDER_PENDING_DIRS.add(out_dir)
    try:
        await asyncio.to_thread(_prepare_run_dir, out_dir, 0)
        args = _build_placeholder_ffmpeg_args(out_dir, drawtext=drawtext)
        try:
            process = await asyncio.create_subprocess_exec(
                FFMPEG_PATH, *args,
                cwd=str(out_dir),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                creationflags=_ffmpeg_creationflags(),
            )
        except (FileNotFoundError, OSError) as exc:
            _log_failure("spawn placeholder ffmpeg", exc, logging.ERROR)
            _remove_tree_later(out_dir, "placeholder")
            return False, ["could not launch ffmpeg"]
        job = _create_run_job(process.pid)
        _write_run_pid_file(out_dir, process.pid)

        log_lines: "deque[str]" = deque(maxlen=200)
        watch_task = _spawn_background_task(_watch_placeholder_process(process), "watch placeholder ffmpeg")
        log_task = _spawn_background_task(_drain_placeholder_log(process, log_lines), "drain placeholder ffmpeg log")
        state = {
            "process": process,
            "job": job,
            "run_id": run_id,
            "output_dir": out_dir,
            "exited": False,
            "watch_task": watch_task,
            "log_task": log_task,
            "log_lines": log_lines,
            "ready": False,
            "last_access": time.monotonic(),
        }
        _PLACEHOLDER_STATE = state
        try:
            ready = await _wait_for_first_segment(out_dir, process, PLACEHOLDER_STARTUP_TIMEOUT_SECONDS)
        except asyncio.CancelledError:
            if _PLACEHOLDER_STATE is state:
                await _stop_placeholder_process()
            raise
        if ready and _PLACEHOLDER_STATE is state:
            state["ready"] = True
            return True, []
        lines = list(log_lines)
        if _PLACEHOLDER_STATE is state:
            await _stop_placeholder_process()
        return False, lines
    finally:
        _PLACEHOLDER_PENDING_DIRS.discard(out_dir)


async def _ensure_placeholder_running() -> bool:
    """Start the shared placeholder stream if it isn't already running. Idempotent;
    after a failed start it declines (returns False) until the backoff expires."""
    global _PLACEHOLDER_FAILURE, _PLACEHOLDER_DRAWTEXT_OK
    async with _PLACEHOLDER_LOCK:
        if _PLACEHOLDER_STATE and _PLACEHOLDER_STATE["process"].returncode is None and not _PLACEHOLDER_STATE.get("exited"):
            return True
        if not ffmpeg_proc.FFMPEG_AVAILABLE or _placeholder_cooldown_remaining() > 0:
            return False
        if _PLACEHOLDER_STATE is not None:  # a dead run: reap it before starting over
            await _stop_placeholder_process()

        lines: List[str] = []
        for _attempt in range(2):
            ready, lines = await _launch_placeholder_run(_PLACEHOLDER_DRAWTEXT_OK)
            if ready:
                _PLACEHOLDER_FAILURE = None
                return True
            if _PLACEHOLDER_DRAWTEXT_OK and _looks_like_drawtext_failure(lines):
                _PLACEHOLDER_DRAWTEXT_OK = False
                LOGGER.warning(
                    "placeholder ffmpeg: drawtext failed (%s); retrying without the 'No Signal' caption",
                    _multiview_error_from_log(lines),
                )
                continue
            break
        error = _multiview_error_from_log(lines)
        retry_in = _record_placeholder_failure(error)
        LOGGER.error("placeholder ffmpeg failed to produce a first segment: %s (retrying in %.0fs)", error, retry_in)
        return False
