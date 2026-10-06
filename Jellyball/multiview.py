"""Multi-View: ffmpeg grid compositing of 2 or 4 channels, its run
lifecycle (start, restart backoff, watchdog, idle stop, output sweep) and its
per-member audio views.
"""

import asyncio
import json
import logging
import os
import re
import sys
import time
from collections import deque
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Set, Tuple

from fastapi import HTTPException, Request
from fastapi.responses import Response

import config
from config import _log_failure, DATA_DIR, LOGGER
from state import _spawn_background_task, PLACEHOLDER_SESSION_ID, stream_state
from alerts import send_alert
from upstream import HLS_MEDIA_TYPE
import ffmpeg_proc
from ffmpeg_proc import (
    _create_run_job,
    _drain_multiview_log,
    _ffmpeg_creationflags,
    _multiview_backoff_seconds,
    _multiview_error_from_log,
    _prepare_run_dir,
    _remove_tree_later,
    _rmtree_with_retries,
    _terminate_ffmpeg,
    _wait_for_first_segment,
    _win32_process_api,
    _write_run_pid_file,
    RUN_PID_FILE,
)
import placeholder
from placeholder import _PLACEHOLDER_PENDING_DIRS, _stop_placeholder_process, PLACEHOLDER_IDLE_SECONDS
from sessions import (
    _is_placeholder_key,
    _maybe_record_playback_event,
    _placeholder_source,
    _serve_session_playlist,
    SESSIONS,
)
from hls_session import SourceSpec
from network_safety import bounded_float, bounded_int


MULTIVIEW_HWACCEL = os.getenv("MULTIVIEW_HWACCEL", "nvenc").strip().lower()
MULTIVIEW_BITRATE = os.getenv("MULTIVIEW_BITRATE", "6M")
MULTIVIEW_SEGMENT_SECONDS = bounded_int(os.getenv("MULTIVIEW_SEGMENT_SECONDS", "4"), 4, 1, 15)
MULTIVIEW_IDLE_TIMEOUT_SECONDS = bounded_float(os.getenv("MULTIVIEW_IDLE_TIMEOUT_SECONDS", "180"), 180.0, 30.0, 3600.0)
MULTIVIEW_IDLE_CHECK_INTERVAL = bounded_float(os.getenv("MULTIVIEW_IDLE_CHECK_INTERVAL", "30"), 30.0, 5.0, 300.0)
MULTIVIEW_STARTUP_TIMEOUT_SECONDS = bounded_float(os.getenv("MULTIVIEW_STARTUP_TIMEOUT_SECONDS", "30"), 30.0, 5.0, 120.0)
# Each running Multi-View channel is its own ffmpeg transcode (GPU encoder session +
# CPU/network for every member stream it composites); an unbounded number of them can
# exhaust NVENC/QSV session limits or the host's CPU. Cap concurrent transcodes.
MAX_CONCURRENT_MULTIVIEW = bounded_int(os.getenv("MAX_CONCURRENT_MULTIVIEW", "3"), 3, 1, 32)
MULTIVIEW_OUTPUT_ROOT = DATA_DIR / "multiview"


def _env_choice(name: str, default: str, allowed) -> str:
    """A string setting limited to known values: an unknown ffmpeg preset or
    tune would make every single Multi-View spawn fail."""
    value = (os.getenv(name) or "").strip().lower()
    if not value:
        return default
    if value not in allowed:
        LOGGER.warning("Ignoring %s=%r (expected one of %s); using %r", name, value, ", ".join(sorted(allowed)), default)
        return default
    return value


# Encoder tuning. The GOP is FPS x segment length and keyframes are forced on
# the segment grid (see _multiview_video_encoder_args), so any combination
# keeps every per-audio output cutting at the same instants.
MULTIVIEW_FPS = bounded_int(os.getenv("MULTIVIEW_FPS", "30"), 30, 10, 60)
MULTIVIEW_HLS_LIST_SIZE = bounded_int(os.getenv("MULTIVIEW_HLS_LIST_SIZE", "8"), 8, 3, 30)
MULTIVIEW_NVENC_PRESET = _env_choice(
    "MULTIVIEW_NVENC_PRESET", "p4",
    {"p1", "p2", "p3", "p4", "p5", "p6", "p7", "default", "slow", "medium", "fast", "hp", "hq", "bd", "ll", "llhq", "llhp"},
)
MULTIVIEW_NVENC_TUNE = _env_choice("MULTIVIEW_NVENC_TUNE", "ll", {"hq", "ll", "ull", "lossless"})
# After a GPU encoder failure new runs are software-encoded for this long,
# then the GPU is tried again (the failure is often transient: every NVENC
# session taken by Jellyfin's own transcodes, a driver update, ...).
NVENC_FALLBACK_SECONDS = bounded_float(os.getenv("NVENC_FALLBACK_SECONDS", "600"), 600.0, 30.0, 86400.0)

# Watchdog/view-failure restarts: the first MULTIVIEW_RESTART_BURST inside a
# rolling window are immediate; each further one waits exponentially longer.
MULTIVIEW_RESTART_WINDOW_SECONDS = bounded_float(os.getenv("MULTIVIEW_RESTART_WINDOW_SECONDS", "600"), 600.0, 60.0, 86400.0)
MULTIVIEW_RESTART_BURST = bounded_int(os.getenv("MULTIVIEW_RESTART_BURST", "2"), 2, 0, 20)
# A refused start (concurrency cap reached / ffmpeg unavailable) is retried
# after a short delay that doubles per consecutive refusal, up to a minute.
MULTIVIEW_REFUSAL_BACKOFF_SECONDS = bounded_float(os.getenv("MULTIVIEW_REFUSAL_BACKOFF_SECONDS", "5"), 5.0, 1.0, 300.0)
MULTIVIEW_REFUSAL_BACKOFF_MAX_SECONDS = max(60.0, MULTIVIEW_REFUSAL_BACKOFF_SECONDS)
# How often leftover run directories (~100MB of segments each) are swept.
MULTIVIEW_SWEEP_INTERVAL = bounded_float(os.getenv("MULTIVIEW_SWEEP_INTERVAL", "600"), 600.0, 60.0, 86400.0)

MULTIVIEW_LAYOUTS = {
    "side_by_side_2": {"count": 2, "pane_w": 960, "pane_h": 1080, "xstack": "0_0|w0_0"},
    "grid_2x2": {"count": 4, "pane_w": 960, "pane_h": 540, "xstack": "0_0|w0_0|0_h0|w0_h0"},
}

_MULTIVIEW_PROCESSES: Dict[str, dict] = {}
# Tracks recent spawn failures per channel so a client that keeps retrying a
# permanently-broken Multi-View (e.g. one member currently has no live
# candidates, so ffmpeg's -i for it 404s and the whole process aborts) can't
# force a new ffmpeg spawn attempt on every single request. Real players
# (Jellyfin's tuner among them) retry a failed live-TV stream on their own
# schedule regardless of what we return, so this backoff is the only thing
# standing between "one dead input" and continuous CPU/GPU churn.
_MULTIVIEW_FAILURES: Dict[str, dict] = {}
# Short "don't start before" holds on top of the failure backoff, for refused
# starts and backed-off restarts: {"until": monotonic, "kind": str, "reason": str}.
_MULTIVIEW_HOLDS: Dict[str, dict] = {}
# Consecutive refused starts per channel (sizes the refusal hold).
_MULTIVIEW_REFUSALS: Dict[str, int] = {}
# Rolling restart history: {"times": deque of monotonic, "last_alert": Optional[float]}.
# Unlike _MULTIVIEW_FAILURES it survives successful spawns, so a run that
# starts fine and then stalls every minute still backs off.
_MULTIVIEW_RESTARTS: Dict[str, dict] = {}
# Multi-Views an admin stopped (monotonic time of the Stop). While set, nothing
# auto-starts the grid and its viewers get the "No Signal" placeholder.
# Cleared by an explicit play: the next NEW viewer session (a playlist or
# segment request for this Multi-View while none of its view sessions is
# running, i.e. a fresh tune) or POST /multiview/{id}/start. A viewer who was
# already watching when Stop was pressed keeps getting No Signal until their
# session idles out (SESSION_IDLE_SECONDS after they stop pulling), so the
# player that happened to be open can't silently undo the Stop.
_MULTIVIEW_MANUAL_STOPS: Dict[str, float] = {}
# Channels holding a concurrency slot while their spawn warms members and waits
# for first segments. Reserved before the (up to 20s) warm-up, so simultaneous
# starts can't all pass MAX_CONCURRENT_MULTIVIEW; released when the spawn ends
# (a successful run keeps the slot through its _MULTIVIEW_PROCESSES entry).
_MULTIVIEW_SLOT_RESERVATIONS: Set[str] = set()
# Run directories created by a spawn but not registered yet (the sweep skips them).
_MULTIVIEW_PENDING_RUN_DIRS: Set[Path] = set()
_MULTIVIEW_RUN_COUNTER = 0
# At most one start task per channel; this (not a lock) serializes spawns.
_MULTIVIEW_START_TASKS: Dict[str, asyncio.Task] = {}
_MULTIVIEW_LAST_VIEWER: Dict[str, float] = {}


def _multiview_live_run_dirs() -> Set[Path]:
    return {entry["output_dir"] for entry in _MULTIVIEW_PROCESSES.values()} | _MULTIVIEW_PENDING_RUN_DIRS


# <root>/<channel>/runN. Read at call time, so a patched MULTIVIEW_OUTPUT_ROOT is what gets swept.
ffmpeg_proc.register_run_root(lambda: MULTIVIEW_OUTPUT_ROOT, per_channel=True, live_dirs=_multiview_live_run_dirs)


def _multiview_hold_remaining(channel_id: str) -> float:
    hold = _MULTIVIEW_HOLDS.get(channel_id)
    if not hold:
        return 0.0
    remaining = hold["until"] - time.monotonic()
    if remaining <= 0:
        _MULTIVIEW_HOLDS.pop(channel_id, None)
        return 0.0
    return remaining


def _multiview_cooldown_remaining(channel_id: str) -> float:
    """Seconds before this Multi-View may be started again (failure backoff,
    refusal hold or restart backoff, whichever ends last)."""
    record = _MULTIVIEW_FAILURES.get(channel_id)
    failure_remaining = 0.0
    if record:
        elapsed = time.monotonic() - record["last_failure"]
        failure_remaining = max(0.0, _multiview_backoff_seconds(record["count"]) - elapsed)
    return max(failure_remaining, _multiview_hold_remaining(channel_id))


def _set_multiview_hold(channel_id: str, seconds: float, kind: str, reason: str) -> None:
    until = time.monotonic() + seconds
    current = _MULTIVIEW_HOLDS.get(channel_id)
    if current is None or current["until"] < until:
        _MULTIVIEW_HOLDS[channel_id] = {"until": until, "kind": kind, "reason": reason}


def _record_multiview_failure(channel_id: str, error: str) -> None:
    record = _MULTIVIEW_FAILURES.setdefault(channel_id, {"count": 0, "last_failure": 0.0, "last_error": ""})
    record["count"] += 1
    record["last_failure"] = time.monotonic()
    record["last_error"] = error


def _clear_multiview_failure(channel_id: str) -> None:
    _MULTIVIEW_FAILURES.pop(channel_id, None)


def _record_multiview_refusal(channel_id: str, reason: str) -> float:
    """A start refused before anything was launched. Viewers get the
    placeholder meanwhile; nothing retries the spawn until the hold expires,
    so this logs once per hold instead of on every 0.5s session poll."""
    count = _MULTIVIEW_REFUSALS.get(channel_id, 0) + 1
    _MULTIVIEW_REFUSALS[channel_id] = count
    delay = min(MULTIVIEW_REFUSAL_BACKOFF_MAX_SECONDS, MULTIVIEW_REFUSAL_BACKOFF_SECONDS * (2 ** (count - 1)))
    _set_multiview_hold(channel_id, delay, "refused", reason)
    LOGGER.warning(
        "Refusing to start multiview channel=%s: %s; next attempt in %.0fs (viewers see No Signal)",
        channel_id, reason, delay,
    )
    return delay


def _multiview_restart_delay(restart_count: int) -> float:
    """Hold before the Nth restart inside the rolling window may start."""
    excess = restart_count - MULTIVIEW_RESTART_BURST
    if excess <= 0:
        return 0.0
    return min(ffmpeg_proc.MULTIVIEW_BACKOFF_MAX_SECONDS, ffmpeg_proc.MULTIVIEW_BACKOFF_BASE_SECONDS * (2 ** (excess - 1)))


def _recent_multiview_restarts(channel_id: str, now: Optional[float] = None) -> int:
    record = _MULTIVIEW_RESTARTS.get(channel_id)
    if not record:
        return 0
    now = time.monotonic() if now is None else now
    times = record["times"]
    while times and now - times[0] > MULTIVIEW_RESTART_WINDOW_SECONDS:
        times.popleft()
    return len(times)


def _note_multiview_restart(channel_id: str, reason: str) -> Tuple[int, float]:
    """Count a restart in the rolling window; returns (restarts in window, hold
    before the next start). Sends one alert when the backoff kicks in, then at
    most one per window while it stays engaged."""
    now = time.monotonic()
    record = _MULTIVIEW_RESTARTS.setdefault(channel_id, {"times": deque(), "last_alert": None})
    _recent_multiview_restarts(channel_id, now)
    record["times"].append(now)
    count = len(record["times"])
    delay = _multiview_restart_delay(count)
    if delay > 0:
        _set_multiview_hold(channel_id, delay, "restart", reason)
        last_alert = record["last_alert"]
        if last_alert is None or now - last_alert >= MULTIVIEW_RESTART_WINDOW_SECONDS:
            record["last_alert"] = now
            name = (stream_state.get(channel_id) or {}).get("name", channel_id)
            _spawn_background_task(
                send_alert(
                    "⚠️ Multi-View Unstable",
                    f"**{name}** restarted {count} times in {MULTIVIEW_RESTART_WINDOW_SECONDS / 60:.0f} min "
                    f"(last: {reason}). Restarts now back off; next start in {delay:.0f}s.",
                    "warning",
                ),
                f"send multiview restart alert channel={channel_id}",
            )
    return count, delay


def _forget_multiview_channel(channel_id: str) -> None:
    """Drop every per-channel Multi-View bookkeeping entry (channel removed)."""
    for mapping in (
        _MULTIVIEW_FAILURES, _MULTIVIEW_HOLDS, _MULTIVIEW_REFUSALS, _MULTIVIEW_RESTARTS,
        _MULTIVIEW_MANUAL_STOPS, _MULTIVIEW_LAST_VIEWER, _MULTIVIEW_START_TASKS,
    ):
        mapping.pop(channel_id, None)
    _MULTIVIEW_SLOT_RESERVATIONS.discard(channel_id)


def _multiview_bufsize(bitrate: str) -> str:
    """Double a ffmpeg bitrate string (e.g. "6M" -> "12M", "6000k" -> "12000k") for -bufsize."""
    match = re.match(r"^(\d+(?:\.\d+)?)([A-Za-z]*)$", bitrate.strip())
    if not match:
        return bitrate
    value, unit = match.groups()
    doubled = float(value) * 2
    doubled_str = str(int(doubled)) if doubled == int(doubled) else str(doubled)
    return f"{doubled_str}{unit}"


def _multiview_video_encoder_args(encoder: Optional[str] = None) -> List[str]:
    """Encoder + rate-control flags. Kept per-backend because several of these
    (e.g. -forced-idr, -sc_threshold) are private AVOptions that only exist on
    some encoders and make ffmpeg fail outright with "Unrecognized option" on
    others (notably h264_qsv), so they must not be shared unconditionally.

    Keyframes are forced on the segment grid (fps is pinned to MULTIVIEW_FPS in
    the filter graph), so every HLS output of the tee cuts at the same instants
    and segments line up across the per-audio outputs."""
    encoder = (encoder or MULTIVIEW_HWACCEL).lower()
    gop = MULTIVIEW_FPS * MULTIVIEW_SEGMENT_SECONDS
    common_rate_args = [
        "-b:v", MULTIVIEW_BITRATE,
        "-maxrate", MULTIVIEW_BITRATE,
        "-bufsize", _multiview_bufsize(MULTIVIEW_BITRATE),
        "-g", str(gop),
        "-keyint_min", str(gop),
        "-bf", "0",
        "-pix_fmt", "yuv420p",
        "-force_key_frames", f"expr:gte(t,n_forced*{MULTIVIEW_SEGMENT_SECONDS})",
    ]
    if encoder == "nvenc":
        return [
            "-c:v", "h264_nvenc", "-preset", MULTIVIEW_NVENC_PRESET, "-tune", MULTIVIEW_NVENC_TUNE, "-rc", "cbr",
            *common_rate_args,
            "-forced-idr", "1",
        ]
    if encoder == "qsv":
        return [
            "-c:v", "h264_qsv", "-preset", "veryfast", "-look_ahead", "0",
            *common_rate_args,
        ]
    return [
        "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
        *common_rate_args,
        "-sc_threshold", "0",
    ]


def _build_xstack_filter(layout: str, member_count: int) -> str:
    """Return the video part of -filter_complex: scale/pad each input and stack
    them into one grid, pinned to MULTIVIEW_FPS (xstack otherwise emits a frame
    whenever *any* input has one, so mixed 30/60fps members made the output rate
    irregular and broke the GOP-to-segment alignment)."""
    spec = MULTIVIEW_LAYOUTS[layout]
    pane_w, pane_h = spec["pane_w"], spec["pane_h"]
    parts = []
    labels = []
    for idx in range(member_count):
        label = f"v{idx}"
        labels.append(label)
        parts.append(
            f"[{idx}:v]scale={pane_w}:{pane_h}:force_original_aspect_ratio=decrease,"
            f"pad={pane_w}:{pane_h}:(ow-iw)/2:(oh-ih)/2,setsar=1[{label}]"
        )
    stack_inputs = "".join(f"[{label}]" for label in labels)
    parts.append(f"{stack_inputs}xstack=inputs={member_count}:layout={spec['xstack']},fps={MULTIVIEW_FPS}[vout]")
    return ";".join(parts)


def _build_multiview_audio_filter(audio_presence: List[bool]) -> str:
    """Silent stand-in tracks [aN] for members without audio (so every
    per-audio output always has a track). Members that do have audio are
    stream-copied, not filtered: decoding + re-encoding the members' live audio
    made ffmpeg's scheduler throttle every input (measured ~0.27x real time
    with two live inputs, vs 1.2x when the audio is copied)."""
    return ";".join(
        f"anullsrc=r=48000:cl=stereo[a{idx}]"
        for idx, has_audio in enumerate(audio_presence)
        if not has_audio
    )


def _multiview_tee_outputs(member_count: int) -> str:
    """One muxed HLS output per member audio, all fed by the same encoded video.

    Escaping is exact on purpose: inside the tee spec, the select value must be
    written as select=\\'v:0,a:N\\' (a literal backslash before each quote in the
    argv element) or ffmpeg rejects it. onfail=ignore keeps one broken output
    from killing the others (the watchdog restarts the run if one stalls). No
    temp_file flag: on Windows its rename-over fails whenever we have the
    playlist open for reading, which would drop that output."""
    slaves = []
    for idx in range(member_count):
        options = ":".join([
            "f=hls",
            f"select=\\'v:0,a:{idx}\\'",
            "onfail=ignore",
            f"hls_time={MULTIVIEW_SEGMENT_SECONDS}",
            f"hls_list_size={MULTIVIEW_HLS_LIST_SIZE}",
            "hls_flags=delete_segments+independent_segments",
            "hls_segment_type=mpegts",
            f"hls_segment_filename=a{idx}/seg_%06d.ts",
        ])
        slaves.append(f"[{options}]a{idx}/index.m3u8")
    return "|".join(slaves)


def _internal_base_url() -> str:
    """Base URL ffmpeg uses to read our own member sessions."""
    host = (os.getenv("JELLYBALL_HOST") or "127.0.0.1").strip()
    if host in ("", "0.0.0.0", "::", "[::]", "localhost"):
        host = "127.0.0.1"
    return f"http://{host}:{config.PORT}"


def _multiview_input_url(team_id: str) -> str:
    if team_id not in stream_state:
        # A member that was deleted shows the "No Signal" pane instead of
        # breaking the whole Multi-View.
        team_id = PLACEHOLDER_SESSION_ID
    return f"{_internal_base_url()}/stream/{team_id}.m3u8"


def _build_multiview_ffmpeg_args(
    channel_id: str,
    data: dict,
    out_dir: Path,
    audio_presence: Optional[List[Optional[bool]]] = None,
    encoder: Optional[str] = None,
    *,
    hw_decode: Optional[bool] = None,
    input_ids: Optional[List[str]] = None,
) -> List[str]:
    """Pure command-builder (no I/O) so it can be unit tested without spawning ffmpeg.

    Inputs are the members' channel sessions, which never 502, follow failover,
    and are normalized (fixed PIDs, continuous timestamps), so one member's
    provider switching no longer freezes its pane or the whole grid. The run
    writes one HLS output per member audio under a{N}/ (relative to cwd=out_dir).

    `audio_presence[i]` must be True for member i's audio to be mapped: None
    (unknown) gets the silent track like False, because mapping a missing
    N:a:0 makes ffmpeg fail the whole run. `input_ids[i]` is the channel
    session that feeds pane i (default: the member itself; the placeholder for
    a member that wasn't ready in time). `hw_decode` (default: encoder is
    nvenc) adds -hwaccel cuda per input."""
    member_team_ids: List[str] = data["member_team_ids"]
    layout = data.get("layout", "grid_2x2")
    encoder = (encoder or MULTIVIEW_HWACCEL).lower()
    if hw_decode is None:
        hw_decode = encoder == "nvenc"
    audio_presence = (
        [has_audio is True for has_audio in audio_presence] if audio_presence else [True] * len(member_team_ids)
    )
    sources = list(input_ids) if input_ids else list(member_team_ids)

    loglevel = os.getenv("MULTIVIEW_FFMPEG_LOGLEVEL", "warning").strip() or "warning"
    args: List[str] = ["-y", "-hide_banner", "-loglevel", loglevel]
    # Progress lines only when debugging with a verbose level.
    args += ["-stats", "-stats_period", "5"] if loglevel in ("info", "verbose", "debug") else ["-nostats"]
    for source_id in sources:
        if hw_decode:
            # Decode on the GPU as well (frames are downloaded for the CPU
            # scale/pad/xstack filters). ffmpeg falls back to software per
            # stream when NVDEC can't handle a codec/profile, but a CUDA
            # *device* failure (no/broken driver) is fatal for the whole run;
            # _start_multiview_run then retries without -hwaccel.
            args += ["-hwaccel", "cuda"]
        args += [
            # Our own normalized sessions (one program, fixed PIDs): a short
            # probe is plenty, and it opens each input much faster than the
            # 5MB/5s default when four members open one after another.
            "-probesize", "1000000",
            "-analyzeduration", "1000000",
            "-reconnect", "1",
            "-reconnect_streamed", "1",
            "-reconnect_on_network_error", "1",
            "-reconnect_delay_max", "5",
            "-rw_timeout", "15000000",
            "-thread_queue_size", "1024",
            "-i", _multiview_input_url(source_id),
        ]

    filter_graph = ";".join(part for part in (
        _build_xstack_filter(layout, len(member_team_ids)),
        _build_multiview_audio_filter(audio_presence),
    ) if part)
    args += ["-filter_complex", filter_graph]
    args += ["-map", "[vout]"]
    for idx, has_audio in enumerate(audio_presence):
        args += ["-map", f"{idx}:a:0" if has_audio else f"[a{idx}]"]
    args += _multiview_video_encoder_args(encoder)
    # Copy member audio as-is; only the generated silent tracks are encoded.
    args += ["-c:a", "copy"]
    for idx, has_audio in enumerate(audio_presence):
        if not has_audio:
            args += [f"-c:a:{idx}", "aac", f"-b:a:{idx}", "96k"]
    # Don't let one briefly starved audio member hold every output for the
    # default 10s interleave window.
    args += ["-max_interleave_delta", "2000000"]
    args += ["-f", "tee", _multiview_tee_outputs(len(member_team_ids))]
    return args


def _multiview_popen_kwargs(out_dir: Path) -> dict:
    kwargs = {
        "cwd": str(out_dir),
        "stdin": asyncio.subprocess.DEVNULL,
        "stdout": asyncio.subprocess.PIPE,
        "stderr": asyncio.subprocess.STDOUT,
    }
    if sys.platform == "win32":
        kwargs["creationflags"] = _ffmpeg_creationflags()
    return kwargs


async def _watch_multiview_process(channel_id: str, process: "asyncio.subprocess.Process") -> None:
    returncode = await process.wait()
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if entry and entry["process"] is process:
        entry["exited"] = True
        entry["exit_code"] = returncode
        if returncode != 0:
            log_tail = "\n".join(list(entry.get("log_lines", []))[-25:])
            LOGGER.warning("multiview ffmpeg exited unexpectedly channel=%s code=%s\n%s", channel_id, returncode, log_tail)


def _kill_orphaned_ffmpeg() -> int:
    """Startup: kill ffmpeg runs a crashed previous instance left behind (pid
    and creation time must both match its pid file). Returns how many were
    killed. No-op off Windows."""
    api = _win32_process_api()
    if api is None:
        return 0
    pid_files = ffmpeg_proc.run_pid_files()
    killed = 0
    for pid_file in pid_files:
        try:
            record = json.loads(pid_file.read_text(encoding="utf-8"))
            pid, created = int(record["pid"]), int(record["created"])
        except (OSError, ValueError, KeyError, TypeError):
            continue
        try:
            if api.terminate_pid_if_created_at(pid, created):
                killed += 1
                LOGGER.warning("Killed orphaned ffmpeg pid=%d left by a previous run (%s)", pid, pid_file.parent)
        except Exception as exc:
            _log_failure(f"kill orphaned ffmpeg pid={pid}", exc)
    return killed


async def _stop_multiview_process(
    channel_id: str,
    *,
    run_id: Optional[int] = None,
    term_timeout: float = 5.0,
    kill_timeout: float = 3.0,
) -> bool:
    """Stop the channel's current run (only if it is `run_id`, when given, so a
    caller holding a stale snapshot can't kill a newer run). True if stopped."""
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if not entry or (run_id is not None and entry.get("run_id") != run_id):
        return False
    _MULTIVIEW_PROCESSES.pop(channel_id, None)
    await _terminate_ffmpeg(entry, f"multiview={channel_id}", term_timeout=term_timeout, kill_timeout=kill_timeout)
    for task_key in ("watch_task", "log_task"):
        task = entry.get(task_key)
        if task and not task.done():
            task.cancel()
    tasks = [entry.get(key) for key in ("watch_task", "log_task") if entry.get(key)]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    _remove_tree_later(entry["output_dir"], f"multiview={channel_id}")
    return True


def _multiview_entry_alive(entry: dict) -> bool:
    return entry["process"].returncode is None and not entry.get("exited")


def _running_multiview_count(exclude: str = "") -> int:
    """Concurrency slots in use: live runs plus spawns that reserved a slot
    and are still warming up or waiting for first segments."""
    in_use = {cid for cid, entry in _MULTIVIEW_PROCESSES.items() if _multiview_entry_alive(entry)}
    in_use |= _MULTIVIEW_SLOT_RESERVATIONS
    in_use.discard(exclude)
    return len(in_use)


MULTIVIEW_MEMBER_WARM_TIMEOUT = bounded_float(os.getenv("MULTIVIEW_MEMBER_WARM_TIMEOUT", "20"), 20.0, 3.0, 120.0)
MULTIVIEW_WATCHDOG_INTERVAL = bounded_float(os.getenv("MULTIVIEW_WATCHDOG_INTERVAL", "3"), 3.0, 1.0, 60.0)
MULTIVIEW_AUDIO_CHANNELS = os.getenv("MULTIVIEW_AUDIO_CHANNELS", "1").strip().lower() not in ("0", "false", "no")
# Extra wait for the placeholder session when a member has to be replaced by it.
MULTIVIEW_STANDIN_WAIT_SECONDS = 10.0

# MPEG-TS stream_type ffmpeg writes for AAC (ADTS): the generated silent track.
_MULTIVIEW_SILENCE_AUDIO_TYPE = 0x0F
# Monotonic time of the last GPU encoder / CUDA device failure (None = none yet).
_HW_ENCODER_FAILED_AT: Optional[float] = None
_CUDA_DECODE_FAILED_AT: Optional[float] = None
# Encoder-specific failure wording, matched against the message part of a
# failed run's log lines. Deliberately no bare "cuda": NVDEC's per-stream
# software fallback ("Failed setup for format cuda") mentions it too, and
# leaves the GPU encoder perfectly usable.
_HW_ENCODER_MARKERS = {
    "nvenc": (
        "h264_nvenc", "hevc_nvenc", "openencodesessionex", "nvenc", "no capable devices found",
        "cannot load nvcuda", "cannot load libcuda", "nvenc api version",
        "driver does not support the required nvenc api version",
    ),
    "qsv": (
        "h264_qsv", "hevc_qsv", "qsv", "mfx session", "libmfx", "libvpl", "device creation failed",
    ),
}
_HW_ENCODER_NAMES = {"nvenc": ("h264_nvenc", "hevc_nvenc"), "qsv": ("h264_qsv", "hevc_qsv")}
# CUDA *device* setup failures (no/broken NVIDIA driver). Unlike the per-stream
# NVDEC fallback these are fatal for every input opened with -hwaccel cuda
# (ffmpeg 9: "Hardware device setup failed for decoder" aborts the run), so a
# retry must drop -hwaccel.
_CUDA_INIT_FAILURE_MARKERS = (
    "cannot load nvcuda", "cannot load libcuda", "could not dynamically load cuda",
    "device creation failed", "no device available for decoder", "hardware device setup failed",
    "cuinit",
)
_FFMPEG_LOG_CONTEXT_RE = re.compile(r"^\s*\[([^\]]*)\]\s*")


def _ffmpeg_log_parts(line: str) -> Tuple[List[str], str]:
    """'[vost#0:0/h264_nvenc @ 0x1] [enc:h264_nvenc @ 0x2] Error ...' ->
    (['vost#0:0/h264_nvenc', 'enc:h264_nvenc'], 'error ...'), lowercased."""
    contexts: List[str] = []
    rest = line
    while True:
        match = _FFMPEG_LOG_CONTEXT_RE.match(rest)
        if not match:
            break
        contexts.append(match.group(1).split(" @ ")[0].strip().lower())
        rest = rest[match.end():]
    return contexts, rest.strip().lower()


def _is_ffmpeg_stream_info(message: str) -> bool:
    # Stream mapping / description / metadata lines name the encoder even when
    # it works ("Stream #0:0 -> #0:0 (h264 (native) -> h264 (h264_nvenc))").
    return message.startswith(("stream #", "stream mapping", "encoder")) or " -> " in message


def _looks_like_hw_encoder_failure(log_lines, encoder: str = "nvenc") -> bool:
    """True when a failed run's log blames the GPU *encoder* (NVENC or QSV)."""
    encoder = (encoder or "").lower()
    markers = _HW_ENCODER_MARKERS.get(encoder)
    if not markers:
        return False
    names = _HW_ENCODER_NAMES[encoder]
    for line in log_lines:
        contexts, message = _ffmpeg_log_parts(str(line))
        if not message or _is_ffmpeg_stream_info(message):
            continue
        if any(marker in message for marker in markers):
            return True
        # Anything the encoder itself logged on a failed run ("[h264_nvenc @ ..] InitializeEncoder failed").
        if any(context in names for context in contexts):
            return True
        # fftools' wrapper around it ("[enc:h264_nvenc @ ..] Error while opening encoder").
        if ("error while opening encoder" in message or "could not open encoder" in message) and any(
            context.endswith(names) for context in contexts
        ):
            return True
    return False


def _looks_like_cuda_init_failure(log_lines) -> bool:
    for line in log_lines:
        _, message = _ffmpeg_log_parts(str(line))
        if any(marker in message for marker in _CUDA_INIT_FAILURE_MARKERS):
            return True
    return False


def _multiview_encoder_plan(now: Optional[float] = None) -> Tuple[str, bool]:
    """(encoder, use -hwaccel cuda) for the next run. Within
    NVENC_FALLBACK_SECONDS of a GPU encoder failure new runs use libx264, then
    the GPU is tried again. CUDA decoding stays on in that fallback unless the
    CUDA device itself failed."""
    now = time.monotonic() if now is None else now
    encoder = MULTIVIEW_HWACCEL
    if (
        encoder in _HW_ENCODER_MARKERS
        and _HW_ENCODER_FAILED_AT is not None
        and now - _HW_ENCODER_FAILED_AT < NVENC_FALLBACK_SECONDS
    ):
        encoder = "none"
    cuda_broken = _CUDA_DECODE_FAILED_AT is not None and now - _CUDA_DECODE_FAILED_AT < NVENC_FALLBACK_SECONDS
    return encoder, MULTIVIEW_HWACCEL == "nvenc" and not cuda_broken


def _multiview_audio_index(data: dict) -> int:
    members = data.get("member_team_ids") or []
    active = data.get("active_audio_team_id") or (members[0] if members else "")
    return members.index(active) if active in members else 0


def _multiview_member_label(team_id: str) -> str:
    member = stream_state.get(team_id)
    return str(member.get("name") or team_id) if member else f"{team_id} (removed)"


class _MultiviewInput(NamedTuple):
    team_id: str                # the configured member
    session_id: str             # channel session ffmpeg reads for this pane
    has_audio: bool             # known to carry audio (else: silent track)
    audio_type: Optional[int]   # MPEG-TS stream_type of that audio, if known
    standin: bool               # an existing member replaced by the placeholder for this run


async def _warm_multiview_members(member_team_ids: List[str]) -> List[_MultiviewInput]:
    """Start every member's channel session in parallel and wait for each to
    have a playable window before ffmpeg opens them one after another (four
    cold members used to blow the startup timeout).

    A member still not ready after MULTIVIEW_MEMBER_WARM_TIMEOUT is fed from
    the "No Signal" placeholder session for this run instead: its own
    /stream/x.m3u8 would answer 503, ffmpeg's -i would fail, and one slow
    member would push the whole grid into failure backoff. The placeholder is
    only warmed once some member is still pending halfway through the wait,
    and the watchdog swaps the real member in once it is live."""
    session_ids = [team_id if team_id in stream_state else PLACEHOLDER_SESSION_ID for team_id in member_team_ids]
    waits: Dict[str, asyncio.Future] = {}
    for session_id in session_ids:
        if session_id not in waits:
            session = SESSIONS.get(session_id)
            session.touch()
            waits[session_id] = asyncio.ensure_future(session.wait_ready(MULTIVIEW_MEMBER_WARM_TIMEOUT))
    try:
        _, pending = await asyncio.wait(list(waits.values()), timeout=MULTIVIEW_MEMBER_WARM_TIMEOUT / 2)
        if pending and PLACEHOLDER_SESSION_ID not in waits:
            SESSIONS.get(PLACEHOLDER_SESSION_ID).touch()
        await asyncio.gather(*waits.values(), return_exceptions=True)
    finally:
        for wait in waits.values():
            if not wait.done():
                wait.cancel()

    def is_ready(session_id: str) -> bool:
        wait = waits.get(session_id)
        return bool(wait and wait.done() and not wait.cancelled() and wait.exception() is None and wait.result())

    standins = {
        index for index, session_id in enumerate(session_ids)
        if session_id != PLACEHOLDER_SESSION_ID and not is_ready(session_id)
    }
    placeholder_ready = is_ready(PLACEHOLDER_SESSION_ID)
    if (standins or PLACEHOLDER_SESSION_ID in waits) and not placeholder_ready:
        placeholder = SESSIONS.get(PLACEHOLDER_SESSION_ID)
        placeholder.touch()
        placeholder_ready = await placeholder.wait_ready(MULTIVIEW_STANDIN_WAIT_SECONDS)

    inputs: List[_MultiviewInput] = []
    for index, (team_id, session_id) in enumerate(zip(member_team_ids, session_ids)):
        standin = index in standins
        if standin:
            LOGGER.warning(
                "Multi-View member %s not ready after %.0fs; its pane shows No Signal for this run",
                team_id, MULTIVIEW_MEMBER_WARM_TIMEOUT,
            )
            session_id = PLACEHOLDER_SESSION_ID
        ready = placeholder_ready if session_id == PLACEHOLDER_SESSION_ID else True
        session = SESSIONS.peek(session_id)
        # Unknown audio (None) counts as none: -map N:a:0 on an input without
        # audio fails the whole run, the silent track never does.
        has_audio = bool(ready and session is not None and session.has_audio is True)
        signature = session.codec_signature if has_audio and session is not None else None
        audio_type = signature[1] if signature and len(signature) > 1 else None
        inputs.append(_MultiviewInput(team_id, session_id, has_audio, audio_type, standin))
    return inputs


def _multiview_spawn_wanted(channel_id: str, data: dict) -> bool:
    """Re-checked after every await of a spawn: the channel may have been
    removed/replaced, or stopped from the dashboard, in the meantime."""
    return stream_state.get(channel_id) is data and channel_id not in _MULTIVIEW_MANUAL_STOPS


async def _spawn_multiview(channel_id: str, data: dict) -> None:
    """Start ffmpeg for a Multi-View channel unless it's already running.

    Only ever runs as the channel's _MULTIVIEW_START_TASKS entry (see
    _request_multiview_start), which serializes starts per channel; Stop and
    Remove cancel that task. Outcomes are recorded, not returned: failures in
    _MULTIVIEW_FAILURES (backoff), refusals as a short hold, success as a
    ready _MULTIVIEW_PROCESSES entry."""
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if entry and _multiview_entry_alive(entry):
        return
    if not _multiview_spawn_wanted(channel_id, data) or _multiview_cooldown_remaining(channel_id) > 0:
        return
    if not ffmpeg_proc.FFMPEG_AVAILABLE:
        _record_multiview_refusal(channel_id, "ffmpeg is unavailable")
        return
    in_use = _running_multiview_count(exclude=channel_id)
    if in_use >= MAX_CONCURRENT_MULTIVIEW:
        _record_multiview_refusal(
            channel_id,
            f"{in_use}/{MAX_CONCURRENT_MULTIVIEW} concurrent Multi-View streams already running "
            "(stop one, or raise MAX_CONCURRENT_MULTIVIEW)",
        )
        return
    _MULTIVIEW_REFUSALS.pop(channel_id, None)
    _MULTIVIEW_SLOT_RESERVATIONS.add(channel_id)
    try:
        await _start_multiview_run(channel_id, data)
    finally:
        _MULTIVIEW_SLOT_RESERVATIONS.discard(channel_id)


async def _start_multiview_run(channel_id: str, data: dict) -> None:
    global _MULTIVIEW_RUN_COUNTER, _HW_ENCODER_FAILED_AT, _CUDA_DECODE_FAILED_AT
    inputs = await _warm_multiview_members(list(data["member_team_ids"]))
    if not _multiview_spawn_wanted(channel_id, data):
        LOGGER.info("Multi-View channel=%s was removed or stopped while warming up; not starting", channel_id)
        return
    stale = _MULTIVIEW_PROCESSES.get(channel_id)
    if stale is not None:  # a dead run the idle monitor hasn't reaped yet
        await _stop_multiview_process(channel_id, run_id=stale.get("run_id"))

    encoder, hw_decode = _multiview_encoder_plan()
    error = "ffmpeg exited unexpectedly"
    for _attempt in range(3):
        _MULTIVIEW_RUN_COUNTER += 1
        outcome, lines = await _launch_multiview_run(channel_id, data, _MULTIVIEW_RUN_COUNTER, inputs, encoder, hw_decode)
        if outcome != "failed":
            return
        error = _multiview_error_from_log(lines) if lines else "ffmpeg exited unexpectedly"
        if encoder in _HW_ENCODER_MARKERS and _looks_like_hw_encoder_failure(lines, encoder):
            # GPU encoder unavailable (driver, session limit shared with
            # Jellyfin's own transcodes, ...): retry this same spawn with
            # libx264 right away, and keep new runs on it for a while.
            cuda_broken = hw_decode and _looks_like_cuda_init_failure(lines)
            _HW_ENCODER_FAILED_AT = time.monotonic()
            if cuda_broken:
                _CUDA_DECODE_FAILED_AT = _HW_ENCODER_FAILED_AT
            hw_decode = hw_decode and not cuda_broken
            LOGGER.warning(
                "%s unavailable for multiview channel=%s (%s); retrying with libx264%s, GPU re-tried in %.0fs",
                encoder.upper(), channel_id, error, " + CUDA decoding" if hw_decode else "", NVENC_FALLBACK_SECONDS,
            )
            encoder = "none"
            continue
        if hw_decode and _looks_like_cuda_init_failure(lines):
            _CUDA_DECODE_FAILED_AT = time.monotonic()
            hw_decode = False
            LOGGER.warning("CUDA decoding unavailable for multiview channel=%s (%s); retrying with software decoding", channel_id, error)
            continue
        break
    _record_multiview_failure(channel_id, error)
    LOGGER.error("multiview channel=%s failed to produce first segments in time: %s", channel_id, error)


async def _launch_multiview_run(
    channel_id: str,
    data: dict,
    run_id: int,
    inputs: List[_MultiviewInput],
    encoder: str,
    hw_decode: bool,
) -> Tuple[str, List[str]]:
    """One ffmpeg attempt: ("ready" | "failed" | "aborted", log lines of a failed run)."""
    cid_path = Path(channel_id)
    safe_channel_id = os.path.basename(channel_id)
    if (
        not channel_id
        or channel_id in {".", ".."}
        or cid_path.is_absolute()
        or len(cid_path.parts) != 1
        or cid_path.name != channel_id
        or safe_channel_id != channel_id
        or Path(safe_channel_id).name != safe_channel_id
    ):
        LOGGER.error("Rejected unsafe multiview channel id for run path: %r", channel_id)
        return "failed", ["unsafe channel id"]
    run_dir = MULTIVIEW_OUTPUT_ROOT / safe_channel_id / f"run{run_id}"
    try:
        run_dir.resolve().relative_to(MULTIVIEW_OUTPUT_ROOT.resolve())
    except Exception:
        LOGGER.error("Rejected multiview run dir outside root for channel id: %r", channel_id)
        return "failed", ["unsafe run directory"]
    _MULTIVIEW_PENDING_RUN_DIRS.add(run_dir)
    try:
        await asyncio.to_thread(_prepare_run_dir, run_dir, len(inputs))
        if not _multiview_spawn_wanted(channel_id, data):
            _remove_tree_later(run_dir, f"multiview={channel_id}")
            return "aborted", []
        args = _build_multiview_ffmpeg_args(
            channel_id, data, run_dir, [item.has_audio for item in inputs], encoder,
            hw_decode=hw_decode, input_ids=[item.session_id for item in inputs],
        )
        try:
            process = await asyncio.create_subprocess_exec(ffmpeg_proc.MULTIVIEW_FFMPEG_PATH, *args, **_multiview_popen_kwargs(run_dir))
        except (FileNotFoundError, OSError) as exc:
            _log_failure(f"spawn ffmpeg multiview={channel_id}", exc, logging.ERROR)
            _remove_tree_later(run_dir, f"multiview={channel_id}")
            return "failed", ["could not launch ffmpeg"]
        job = _create_run_job(process.pid)
        _write_run_pid_file(run_dir, process.pid)

        log_lines: "deque[str]" = deque(maxlen=200)
        watch_task = _spawn_background_task(
            _watch_multiview_process(channel_id, process),
            f"watch multiview ffmpeg={channel_id}",
        )
        log_task = _spawn_background_task(
            _drain_multiview_log(channel_id, process, log_lines),
            f"drain multiview ffmpeg log={channel_id}",
        )
        now = time.monotonic()
        entry = {
            "process": process,
            "job": job,
            "output_dir": run_dir,
            "run_id": run_id,
            "audio_count": len(inputs),
            # Output a{N} carries member N's own (stream-copied) audio codec,
            # or the generated AAC silence; part of the views' source key.
            "audio_types": [item.audio_type if item.has_audio else _MULTIVIEW_SILENCE_AUDIO_TYPE for item in inputs],
            "standins": [item.team_id for item in inputs if item.standin],
            "encoder": encoder,
            "hw_decode": hw_decode,
            "ready": False,
            "started_at": now,
            "last_access": max(now, _MULTIVIEW_LAST_VIEWER.get(channel_id, 0.0)),
            "exited": False,
            "exit_code": None,
            "watch_task": watch_task,
            "log_task": log_task,
            "log_lines": log_lines,
        }
        _MULTIVIEW_PROCESSES[channel_id] = entry
        try:
            results = await asyncio.gather(*(
                _wait_for_first_segment(run_dir / f"a{idx}", process, MULTIVIEW_STARTUP_TIMEOUT_SECONDS)
                for idx in range(len(inputs))
            ))
        except asyncio.CancelledError:
            # Stop/Remove cancelled this spawn: never leave its ffmpeg behind.
            await _stop_multiview_process(channel_id, run_id=run_id)
            raise
        if not _multiview_spawn_wanted(channel_id, data) or _MULTIVIEW_PROCESSES.get(channel_id) is not entry:
            # Stopped/removed meanwhile, or the run was already taken down
            # elsewhere (then this stop is a no-op): not a failure to back off.
            await _stop_multiview_process(channel_id, run_id=run_id)
            return "aborted", []
        if all(results):
            entry["ready"] = True
            _clear_multiview_failure(channel_id)
            LOGGER.info(
                "Multi-View running channel=%s run=%d encoder=%s%s",
                channel_id, run_id, encoder, " (CUDA decoding)" if hw_decode else "",
            )
            for session_id in _multiview_view_session_ids(channel_id):
                SESSIONS.poke(session_id)
            return "ready", []
        lines = list(log_lines)
        await _stop_multiview_process(channel_id, run_id=run_id)
        return "failed", lines
    finally:
        _MULTIVIEW_PENDING_RUN_DIRS.discard(run_dir)


def _request_multiview_start(channel_id: str) -> None:
    data = stream_state.get(channel_id)
    if not data or data.get("type") != "multiview":
        return
    if channel_id in _MULTIVIEW_MANUAL_STOPS:
        return
    task = _MULTIVIEW_START_TASKS.get(channel_id)
    if task is not None and not task.done():
        return
    if _multiview_cooldown_remaining(channel_id) > 0:
        return
    task = _spawn_background_task(_spawn_multiview(channel_id, data), f"start multiview channel={channel_id}")
    _MULTIVIEW_START_TASKS[channel_id] = task

    def _forget(done: asyncio.Task, cid: str = channel_id) -> None:
        if _MULTIVIEW_START_TASKS.get(cid) is done:
            _MULTIVIEW_START_TASKS.pop(cid, None)

    task.add_done_callback(_forget)


def _multiview_start_in_progress(channel_id: str) -> bool:
    task = _MULTIVIEW_START_TASKS.get(channel_id)
    return task is not None and not task.done()


async def _cancel_multiview_start(channel_id: str) -> bool:
    """Cancel and await an in-progress spawn (which stops any ffmpeg it had
    already launched). True if one was running."""
    task = _MULTIVIEW_START_TASKS.pop(channel_id, None)
    if task is None or task.done():
        return False
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    return True


async def _stop_multiview_manually(channel_id: str) -> None:
    """Dashboard Stop: stop the grid and keep it stopped (see _MULTIVIEW_MANUAL_STOPS)."""
    _MULTIVIEW_MANUAL_STOPS[channel_id] = time.monotonic()
    await _cancel_multiview_start(channel_id)
    await _stop_multiview_process(channel_id)
    LOGGER.info("Multi-View channel=%s stopped from the dashboard; it stays stopped until a new viewer tunes in", channel_id)
    for session_id in _multiview_view_session_ids(channel_id):
        SESSIONS.poke(session_id)


def _clear_multiview_manual_stop(channel_id: str, why: str) -> None:
    if _MULTIVIEW_MANUAL_STOPS.pop(channel_id, None) is not None:
        LOGGER.info("Multi-View channel=%s may start again (%s)", channel_id, why)


def _multiview_view_session_ids(channel_id: str) -> List[str]:
    data = stream_state.get(channel_id) or {}
    return [channel_id] + [f"{channel_id}#a{idx}" for idx in range(len(data.get("member_team_ids") or []))]


def _multiview_view_for_session(session_id: str) -> Optional[Tuple[str, Optional[int]]]:
    """Map a session id to (multiview channel, audio index or None for 'active audio')."""
    if "#a" in session_id:
        channel_id, _, index_text = session_id.rpartition("#a")
        data = stream_state.get(channel_id)
        if data and data.get("type") == "multiview" and index_text.isdigit():
            return channel_id, int(index_text)
        return None
    data = stream_state.get(session_id)
    if data and data.get("type") == "multiview":
        return session_id, None
    return None


def _multiview_output_audio_tag(entry: dict, index: int) -> str:
    """Source-key component for Multi-View output a{index}: its audio codec
    (MPEG-TS stream_type) when known, else the output index itself."""
    audio_types = entry.get("audio_types") or []
    audio_type = audio_types[index] if 0 <= index < len(audio_types) else None
    return f"audio:{audio_type:#04x}" if isinstance(audio_type, int) else f"audio:a{index}"


def _resolve_multiview_view_source(channel_id: str, audio_index: Optional[int]) -> Optional[SourceSpec]:
    data = stream_state.get(channel_id)
    if not data or data.get("type") != "multiview":
        return None
    if channel_id in _MULTIVIEW_MANUAL_STOPS:
        return _placeholder_source()
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if entry and entry.get("ready") and _multiview_entry_alive(entry):
        members = data.get("member_team_ids") or []
        index = _multiview_audio_index(data) if audio_index is None else audio_index
        index = max(0, min(index, entry["audio_count"] - 1))
        label = _multiview_member_label(members[index]) if index < len(members) else f"audio {index}"
        # One key per (run, audio codec). All outputs of a run share segment
        # numbers and video timestamps, so switching the main channel's audio
        # to an output whose audio has the same codec is just a URL change
        # and continues seamlessly. Member audio is stream-copied, though, so
        # each output carries its own member's codec: switching to a different
        # one (say AAC -> AC-3) gets a new key, i.e. a new normalizer epoch and
        # a discontinuity, so the player re-probes instead of hitting a PMT
        # change mid-stream. When a codec isn't known the key uses the output
        # index, making every switch to/from it a clean discontinuity. A new
        # run (restart) is always a new key.
        return SourceSpec(
            key=("multiview", channel_id, entry["run_id"], _multiview_output_audio_tag(entry, index)),
            url=str(entry["output_dir"] / f"a{index}" / "index.m3u8"),
            local=True,
            label=f"Multi-View {label}",
        )
    _request_multiview_start(channel_id)
    if _multiview_cooldown_remaining(channel_id) > 0 or not ffmpeg_proc.FFMPEG_AVAILABLE:
        # Backoff, refusal (concurrency cap) or no ffmpeg: No Signal, not a 503.
        return _placeholder_source()
    return None  # starting: the session waits (and keeps its current window)


def _multiview_view_sessions_running(channel_id: str) -> bool:
    for session_id in _multiview_view_session_ids(channel_id):
        session = SESSIONS.peek(session_id)
        if session is not None and session.is_running:
            return True
    return False


def _touch_multiview_viewer(channel_id: str) -> None:
    now = time.monotonic()
    if channel_id in _MULTIVIEW_MANUAL_STOPS and not _multiview_view_sessions_running(channel_id):
        # No view session is running, so this request starts a new one: a
        # fresh tune after the Stop counts as an explicit play.
        _clear_multiview_manual_stop(channel_id, "new viewer tuned in")
    _MULTIVIEW_LAST_VIEWER[channel_id] = now
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if entry:
        entry["last_access"] = now


def _on_multiview_view_failure(session_id: str, source_key: tuple, reason: str) -> None:
    view = _multiview_view_for_session(session_id)
    if view is None or len(source_key) < 3 or source_key[0] != "multiview":
        return
    channel_id = view[0]
    run_id = source_key[2]
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if entry and entry.get("run_id") == run_id:
        _spawn_background_task(
            _restart_multiview(channel_id, f"view reported {reason}", run_id=run_id),
            f"restart multiview channel={channel_id}",
        )


async def _restart_multiview(
    channel_id: str,
    reason: str,
    run_id: Optional[int] = None,
    *,
    count_toward_backoff: bool = True,
) -> bool:
    """Restart a Multi-View. With `run_id` it only acts if that run is still
    the current one: the watchdog and the view sessions work from snapshots,
    and a restart that already replaced run N with N+1 must not be followed
    by a second one killing N+1. Without `run_id` (a config change, e.g. a
    member was removed) it also cancels a spawn in progress so the new run
    picks up the change. Restarts are counted in a rolling window and backed
    off (see _note_multiview_restart). True if a restart happened."""
    entry = _MULTIVIEW_PROCESSES.get(channel_id)
    if run_id is not None:
        if entry is None or entry.get("run_id") != run_id:
            LOGGER.info(
                "Ignoring restart of stale multiview run channel=%s run=%s current=%s reason=%s",
                channel_id, run_id, entry.get("run_id") if entry else None, reason,
            )
            return False
    else:
        cancelled = await _cancel_multiview_start(channel_id)
        entry = _MULTIVIEW_PROCESSES.get(channel_id)
        if entry is None and not cancelled:
            return False
    if entry is not None and entry.get("restarting"):
        return False
    if entry is not None:
        entry["restarting"] = True
    count, delay = _note_multiview_restart(channel_id, reason) if count_toward_backoff else (0, 0.0)
    if delay > 0:
        LOGGER.warning(
            "Restarting multiview channel=%s run=%s reason=%s; %d restarts in %.0f min, next start held %.0fs",
            channel_id, entry.get("run_id") if entry else None, reason, count,
            MULTIVIEW_RESTART_WINDOW_SECONDS / 60, delay,
        )
    else:
        LOGGER.warning(
            "Restarting multiview channel=%s run=%s reason=%s",
            channel_id, entry.get("run_id") if entry else None, reason,
        )
    if entry is not None:
        await _stop_multiview_process(channel_id, run_id=entry["run_id"])
    _request_multiview_start(channel_id)  # no-op while held; the next viewer poll starts it
    return True


def _newest_segment_age(output_dir: Path) -> Optional[float]:
    """Seconds since this output last produced a segment, or None if it has
    no playlist either (broken output).

    ffmpeg's delete_segments removes old files constantly, so a file can
    vanish between the glob and its stat: that one file is skipped rather
    than the whole output being reported stalled (which restarted healthy
    grids). An output with no segment file visible falls back to its
    playlist's age, so it only counts as stalled once the playlist is stale."""
    newest = None
    try:
        paths = list(output_dir.glob("seg_*.ts"))
    except OSError:
        paths = []
    for path in paths:
        try:
            mtime = path.stat().st_mtime
        except OSError:
            continue
        newest = mtime if newest is None or mtime > newest else newest
    if newest is None:
        try:
            newest = (output_dir / "index.m3u8").stat().st_mtime
        except FileNotFoundError:
            return None
        except OSError:
            return 0.0  # can't tell right now (e.g. a sharing violation): not evidence of a stall
    return max(0.0, time.time() - newest)


async def _swap_in_ready_members(channel_id: str, entry: dict) -> None:
    """A pane showing the stand-in placeholder gets its real member back once
    that member is live: keep its session warm meanwhile and restart the run
    when it flows, as long as that restart wouldn't be backed off."""
    live = []
    for team_id in entry.get("standins") or []:
        if team_id not in stream_state:
            continue
        session = SESSIONS.get(team_id)
        session.touch()
        source = session.source
        if session.is_flowing() and source is not None and not _is_placeholder_key(source.key):
            live.append(team_id)
    if live and _multiview_restart_delay(_recent_multiview_restarts(channel_id) + 1) == 0:
        await _restart_multiview(channel_id, f"member(s) {live} live now", run_id=entry["run_id"])


async def _multiview_watchdog_pass(stall_after: float) -> None:
    now = time.monotonic()
    for channel_id, entry in list(_MULTIVIEW_PROCESSES.items()):
        if not entry.get("ready") or entry.get("restarting"):
            continue
        run_id = entry["run_id"]
        watched = now - entry.get("last_access", 0.0) < MULTIVIEW_IDLE_TIMEOUT_SECONDS
        if not _multiview_entry_alive(entry):
            if watched:
                await _restart_multiview(channel_id, f"ffmpeg exited code={entry.get('exit_code')}", run_id=run_id)
            continue
        if now - entry["started_at"] < MULTIVIEW_STARTUP_TIMEOUT_SECONDS:
            continue
        ages = await asyncio.to_thread(
            lambda e=entry: [_newest_segment_age(e["output_dir"] / f"a{i}") for i in range(e["audio_count"])]
        )
        stalled = [i for i, age in enumerate(ages) if age is None or age > stall_after]
        if stalled:
            await _restart_multiview(channel_id, f"output stalled audio={stalled}", run_id=run_id)
        elif watched and entry.get("standins"):
            await _swap_in_ready_members(channel_id, entry)


async def multiview_watchdog() -> None:
    """Restart a Multi-View run whose output stalls (one frozen input can hold
    xstack) or whose ffmpeg died while people are watching. The channel
    sessions hide the restart behind a discontinuity."""
    stall_after = max(3.0 * MULTIVIEW_SEGMENT_SECONDS, 12.0)
    while True:
        await asyncio.sleep(MULTIVIEW_WATCHDOG_INTERVAL)
        try:
            await _multiview_watchdog_pass(stall_after)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_failure("multiview watchdog", exc)


async def multiview_idle_monitor() -> None:
    while True:
        try:
            now = time.monotonic()
            for channel_id, entry in list(_MULTIVIEW_PROCESSES.items()):
                if entry.get("restarting"):
                    continue
                if not _multiview_entry_alive(entry):
                    if now - entry.get("last_access", 0.0) >= MULTIVIEW_IDLE_TIMEOUT_SECONDS:
                        LOGGER.warning("Reaping dead multiview ffmpeg channel=%s code=%s", channel_id, entry.get("exit_code"))
                        await _stop_multiview_process(channel_id, run_id=entry.get("run_id"))
                elif now - entry.get("last_access", now) > MULTIVIEW_IDLE_TIMEOUT_SECONDS:
                    LOGGER.info("Stopping idle multiview ffmpeg channel=%s", channel_id)
                    await _stop_multiview_process(channel_id, run_id=entry.get("run_id"))

            # The placeholder runs on demand: channel sessions (re)start it when a
            # channel has nothing live, and it stops after a stretch of disuse.
            state = placeholder._PLACEHOLDER_STATE
            if state and now - state.get("last_access", now) > PLACEHOLDER_IDLE_SECONDS:
                LOGGER.info("Stopping idle placeholder ffmpeg")
                await _stop_placeholder_process()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_failure("multiview idle monitor", exc)
        await asyncio.sleep(MULTIVIEW_IDLE_CHECK_INTERVAL)


def _sweep_output_dirs_sync(live_dirs: Set[Path], min_age: float) -> List[Path]:
    """Blocking. Delete the run dirs under every registered run root
    (ffmpeg_proc.run_roots(): Multi-View's <root>/<channel>/runN, the
    placeholder's <root>/runN, ...) that no live run owns, plus per-channel
    dirs left empty.
    Dirs younger than `min_age` seconds are kept: a spawn may have just
    created one it hasn't registered yet."""
    removed: List[Path] = []
    now = time.time()
    live_parents = {path.parent for path in live_dirs}

    def stale(path: Path) -> bool:
        if path in live_dirs:
            return False
        try:
            return path.is_dir() and now - path.stat().st_mtime >= min_age
        except OSError:
            return False

    def children(path: Path) -> List[Path]:
        try:
            return list(path.iterdir())
        except OSError:
            return []

    for root in ffmpeg_proc.run_roots():
        if not root.per_channel:
            for run_dir in children(root.path):
                if stale(run_dir) and _rmtree_with_retries(run_dir, attempts=2, delay=0.5):
                    removed.append(run_dir)
            continue
        for channel_dir in children(root.path):
            if not channel_dir.is_dir():
                continue
            for run_dir in children(channel_dir):
                if stale(run_dir) and _rmtree_with_retries(run_dir, attempts=2, delay=0.5):
                    removed.append(run_dir)
            if channel_dir not in live_parents and not children(channel_dir):
                try:
                    channel_dir.rmdir()
                    removed.append(channel_dir)
                except OSError:
                    pass
    return removed


async def _sweep_output_dirs(min_age: float = 60.0) -> List[Path]:
    live: Set[Path] = ffmpeg_proc.live_run_dirs()
    removed = await asyncio.to_thread(_sweep_output_dirs_sync, live, min_age)
    if removed:
        LOGGER.info("Removed %d leftover Multi-View/placeholder output dir(s)", len(removed))
    return removed


async def multiview_output_sweeper() -> None:
    """Periodic safety net for run directories a stop couldn't delete (files
    still locked after all retries, a crash mid-stop, ...)."""
    while True:
        await asyncio.sleep(MULTIVIEW_SWEEP_INTERVAL)
        try:
            await _sweep_output_dirs()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_failure("sweep multiview output dirs", exc)


def _multiview_member_validation(member_team_ids: List[str]) -> Optional[str]:
    """Return an error message if the requested member list is invalid, else None."""
    if len(member_team_ids) not in (2, 4):
        return "Select exactly 2 or 4 channels for a Multi-View."
    seen = set()
    for team_id in member_team_ids:
        if team_id in seen:
            return "Duplicate channel selected."
        seen.add(team_id)
        member_data = stream_state.get(team_id)
        if not member_data:
            return f"Unknown channel: {team_id}"
        if member_data.get("type") == "multiview":
            return "A Multi-View channel cannot include another Multi-View channel."
    return None


def _multiview_audio_view(channel_id: str, audio_index: int) -> Optional[dict]:
    data = stream_state.get(channel_id)
    if not data or data.get("type") != "multiview":
        return None
    if not 0 <= audio_index < len(data.get("member_team_ids") or []):
        return None
    return data


async def _serve_multiview_audio_playlist(channel_id: str, audio_index: int, request: Request, segment_prefix: str):
    """Per-audio Multi-View channel ("🔊 Team · Name"): the same composited video
    with one member's audio. Switching audio inside Jellyfin = changing channel,
    which works on every client."""
    if _multiview_audio_view(channel_id, audio_index) is None:
        raise HTTPException(status_code=404, detail="Multi-View channel not found")
    if request.method == "HEAD":
        return Response(status_code=200, media_type=HLS_MEDIA_TYPE, headers={"Cache-Control": "no-cache"})
    _touch_multiview_viewer(channel_id)
    _maybe_record_playback_event(channel_id)
    return await _serve_session_playlist(f"{channel_id}#a{audio_index}", segment_prefix)
