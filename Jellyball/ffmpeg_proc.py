"""ffmpeg discovery and child-process control shared by Multi-View and the
No Signal placeholder: binary paths and version probes, Windows job objects,
pid files, termination, run-directory cleanup, log draining and spawn backoff.

FFMPEG_AVAILABLE and MULTIVIEW_FFMPEG_PATH are rebound at startup, so other
modules read them as `ffmpeg_proc.FFMPEG_AVAILABLE`.
"""

import asyncio
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from collections import deque
from pathlib import Path
from typing import Callable, Iterable, List, NamedTuple, Optional, Set, Tuple, Union

from config import _log_failure, BUNDLE_DIR, DATA_DIR, LOGGER
from state import _spawn_background_task
from network_safety import bounded_float


def _default_ffmpeg_path() -> str:
    """Prefer a bundled ffmpeg binary in the packaged executable; otherwise
    fall back to a plain "ffmpeg" lookup on PATH (dev runs, Docker, or a
    build that didn't have one available to bundle)."""
    if getattr(sys, "frozen", False):
        bundled_name = "ffmpeg.exe" if sys.platform == "win32" else "ffmpeg"
        bundled_path = BUNDLE_DIR / "ffmpeg_bin" / bundled_name
        if bundled_path.is_file():
            return str(bundled_path)
    return "ffmpeg"


FFMPEG_PATH = os.getenv("FFMPEG_PATH") or _default_ffmpeg_path()
MULTIVIEW_OUTPUT_ROOT = (DATA_DIR / "multiview").resolve()


def _resolve_within(base_dir: Path, candidate: Path) -> Optional[Path]:
    """Resolve candidate and ensure it is contained under base_dir."""
    try:
        resolved_base = base_dir.resolve()
        resolved_candidate = candidate.resolve()
        resolved_candidate.relative_to(resolved_base)
        return resolved_candidate
    except (OSError, ValueError):
        return None


def _jellyfin_ffmpeg_path() -> Optional[str]:
    """Jellyfin's own ffmpeg build, when Jellyball runs on the Jellyfin server."""
    candidates: List[Path] = []
    if sys.platform == "win32":
        for root in (os.getenv("ProgramW6432"), os.getenv("ProgramFiles"), r"C:\Program Files"):
            if root:
                candidates.append(Path(root) / "Jellyfin" / "Server" / "ffmpeg.exe")
    else:
        candidates += [Path("/usr/lib/jellyfin-ffmpeg/ffmpeg"), Path("/usr/share/jellyfin-ffmpeg/ffmpeg")]
    for candidate in candidates:
        if candidate.is_file():
            return str(candidate)
    return None


# Multi-View's encoder must match the GPU driver. The bundled ffmpeg is a very
# recent build (its NVENC needs NVIDIA driver 610+); on a Jellyfin server the
# Jellyfin ffmpeg is built for whatever driver Jellyfin's own hardware
# transcoding already uses, so prefer it unless a path is configured.
MULTIVIEW_FFMPEG_PATH = (
    os.getenv("MULTIVIEW_FFMPEG_PATH")
    or (None if os.getenv("FFMPEG_PATH") else _jellyfin_ffmpeg_path())
    or FFMPEG_PATH
)
MULTIVIEW_FFMPEG_VERSION_INFO = ""

# Spawn-failure backoff (and the step/cap of multiview's restart backoff).
MULTIVIEW_BACKOFF_BASE_SECONDS = bounded_float(os.getenv("MULTIVIEW_BACKOFF_BASE_SECONDS", "15"), 15.0, 1.0, 600.0)
MULTIVIEW_BACKOFF_MAX_SECONDS = max(
    MULTIVIEW_BACKOFF_BASE_SECONDS,
    bounded_float(os.getenv("MULTIVIEW_BACKOFF_MAX_SECONDS", "300"), 300.0, 1.0, 3600.0),
)

FFMPEG_AVAILABLE = False
FFMPEG_VERSION_INFO = ""


def _multiview_backoff_seconds(failure_count: int) -> float:
    return min(MULTIVIEW_BACKOFF_MAX_SECONDS, MULTIVIEW_BACKOFF_BASE_SECONDS * (2 ** max(0, failure_count - 1)))


def _multiview_error_from_log(log_lines) -> str:
    """Pull the most useful line out of ffmpeg's log for a human-readable failure reason."""
    lines = list(log_lines)
    for line in reversed(lines):
        if "Error opening input" in line or "error while opening" in line.lower():
            return line.strip()
    for line in reversed(lines):
        if line.strip():
            return line.strip()
    return "ffmpeg exited unexpectedly"


async def _probe_ffmpeg_version(path: str, timeout: float = 5.0) -> Tuple[Optional[int], str]:
    """Run `<path> -version`; returns (exit code, first output line). A binary
    that hangs (e.g. on an unreachable network path) is killed on timeout
    instead of being left running for the life of the service."""
    process = await asyncio.create_subprocess_exec(
        path, "-version",
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        creationflags=_child_process_creationflags(),
    )
    try:
        stdout, _ = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        if process.returncode is None:
            try:
                process.kill()
            except (ProcessLookupError, OSError):
                pass
            try:
                await asyncio.wait_for(process.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
        raise
    first_line = stdout.decode(errors="replace").splitlines()[0] if stdout else ""
    return process.returncode, first_line


async def _check_ffmpeg_available() -> None:
    """Probe FFMPEG_PATH once at startup so failures surface as a clear log/dashboard warning."""
    global FFMPEG_AVAILABLE, FFMPEG_VERSION_INFO
    try:
        returncode, FFMPEG_VERSION_INFO = await _probe_ffmpeg_version(FFMPEG_PATH)
        FFMPEG_AVAILABLE = returncode == 0
    except (FileNotFoundError, OSError, asyncio.TimeoutError) as exc:
        FFMPEG_AVAILABLE = False
        FFMPEG_VERSION_INFO = ""
        _log_failure("locate ffmpeg for multiview", exc, logging.WARNING)
    if not FFMPEG_AVAILABLE:
        LOGGER.warning("ffmpeg not found at FFMPEG_PATH=%r; Multi-View channels unavailable", FFMPEG_PATH)
    await _check_multiview_ffmpeg()


async def _check_multiview_ffmpeg() -> None:
    global MULTIVIEW_FFMPEG_PATH, MULTIVIEW_FFMPEG_VERSION_INFO
    if MULTIVIEW_FFMPEG_PATH != FFMPEG_PATH:
        try:
            returncode, first_line = await _probe_ffmpeg_version(MULTIVIEW_FFMPEG_PATH)
            if returncode == 0 and first_line:
                MULTIVIEW_FFMPEG_VERSION_INFO = first_line
            else:
                raise OSError(f"exit code {returncode}")
        except (FileNotFoundError, OSError, asyncio.TimeoutError) as exc:
            _log_failure(f"probe Multi-View ffmpeg {MULTIVIEW_FFMPEG_PATH!r}; using {FFMPEG_PATH!r}", exc)
            MULTIVIEW_FFMPEG_PATH = FFMPEG_PATH
    if MULTIVIEW_FFMPEG_PATH == FFMPEG_PATH:
        MULTIVIEW_FFMPEG_VERSION_INFO = FFMPEG_VERSION_INFO
    if MULTIVIEW_FFMPEG_VERSION_INFO:
        LOGGER.info("Multi-View ffmpeg: %s (%s)", MULTIVIEW_FFMPEG_VERSION_INFO, MULTIVIEW_FFMPEG_PATH)


def _child_process_creationflags() -> int:
    """No console window per ffmpeg/taskkill child in the windowed tray exe."""
    if sys.platform == "win32":
        return getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)
    return 0


def _ffmpeg_creationflags() -> int:
    """Multi-View / placeholder ffmpeg: no console window, and below-normal
    priority so a CPU (libx264) fallback encode can't starve Jellyfin's own
    transcodes or this server's request handling."""
    flags = _child_process_creationflags()
    if sys.platform == "win32":
        flags |= getattr(subprocess, "BELOW_NORMAL_PRIORITY_CLASS", 0x00004000)
    return flags


async def _drain_multiview_log(channel_id: str, process: "asyncio.subprocess.Process", log_lines: "deque[str]") -> None:
    """Continuously read ffmpeg's combined stdout/stderr pipe into a rolling
    buffer. This is not optional: an unread PIPE fills its OS buffer (~64KB) and
    blocks ffmpeg forever. Read in chunks rather than readline(): ffmpeg's
    progress output ends in '\\r', and readline() raises once 64KB arrive with no
    '\\n', which used to kill this task and then hang ffmpeg."""
    pending = b""
    try:
        while True:
            chunk = await process.stdout.read(8192)
            if not chunk:
                break
            pending += chunk
            *lines, pending = re.split(rb"[\r\n]+", pending)
            for line in lines:
                if line.strip():
                    log_lines.append(line.decode(errors="replace").rstrip())
            if len(pending) > 16384:
                log_lines.append(pending[-1024:].decode(errors="replace"))
                pending = b""
        if pending.strip():
            log_lines.append(pending.decode(errors="replace").rstrip())
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _log_failure(f"drain ffmpeg log multiview={channel_id}", exc)


class _Win32ProcessApi:
    """The kernel32 calls used for ffmpeg process control, with explicit
    prototypes. Without restype=HANDLE ctypes returns a C int, which truncates
    64-bit handles; a private WinDLL instance keeps these prototypes from
    clashing with any other ctypes user of kernel32."""

    PROCESS_TERMINATE = 0x0001
    PROCESS_SET_QUOTA = 0x0100
    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    SYNCHRONIZE = 0x00100000
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000
    JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS = 9

    def __init__(self) -> None:
        import ctypes
        from ctypes import wintypes

        class _IO_COUNTERS(ctypes.Structure):
            _fields_ = [(name, ctypes.c_ulonglong) for name in (
                "ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
                "ReadTransferCount", "WriteTransferCount", "OtherTransferCount",
            )]

        class _JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_int64),
                ("PerJobUserTimeLimit", ctypes.c_int64),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class _JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", _JOBOBJECT_BASIC_LIMIT_INFORMATION),
                ("IoInfo", _IO_COUNTERS),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        self.ctypes = ctypes
        self.wintypes = wintypes
        self.extended_limit_info = _JOBOBJECT_EXTENDED_LIMIT_INFORMATION
        k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        HANDLE, BOOL, DWORD, UINT = wintypes.HANDLE, wintypes.BOOL, wintypes.DWORD, wintypes.UINT
        PFILETIME = ctypes.POINTER(wintypes.FILETIME)
        prototypes = {
            "CreateJobObjectW": (HANDLE, [wintypes.LPVOID, wintypes.LPCWSTR]),
            "SetInformationJobObject": (BOOL, [HANDLE, ctypes.c_int, wintypes.LPVOID, DWORD]),
            "AssignProcessToJobObject": (BOOL, [HANDLE, HANDLE]),
            "TerminateJobObject": (BOOL, [HANDLE, UINT]),
            "OpenProcess": (HANDLE, [DWORD, BOOL, DWORD]),
            "TerminateProcess": (BOOL, [HANDLE, UINT]),
            "GetProcessTimes": (BOOL, [HANDLE, PFILETIME, PFILETIME, PFILETIME, PFILETIME]),
            "WaitForSingleObject": (DWORD, [HANDLE, DWORD]),
            "CloseHandle": (BOOL, [HANDLE]),
        }
        for name, (restype, argtypes) in prototypes.items():
            function = getattr(k32, name)
            function.restype = restype
            function.argtypes = argtypes
        self.k32 = k32

    def close(self, handle) -> None:
        if handle:
            self.k32.CloseHandle(handle)

    def create_kill_on_close_job(self):
        h_job = self.k32.CreateJobObjectW(None, None)
        if not h_job:
            return None
        info = self.extended_limit_info()
        info.BasicLimitInformation.LimitFlags = self.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if not self.k32.SetInformationJobObject(
            h_job, self.JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS,
            self.ctypes.byref(info), self.ctypes.sizeof(info),
        ):
            self.close(h_job)
            return None
        return h_job

    def assign_pid(self, h_job, pid: int) -> bool:
        h_process = self.k32.OpenProcess(self.PROCESS_TERMINATE | self.PROCESS_SET_QUOTA, False, pid)
        if not h_process:
            return False
        try:
            return bool(self.k32.AssignProcessToJobObject(h_job, h_process))
        finally:
            self.close(h_process)

    def terminate_job(self, h_job) -> bool:
        return bool(self.k32.TerminateJobObject(h_job, 1))

    def _creation_time(self, h_process) -> Optional[int]:
        times = [self.wintypes.FILETIME() for _ in range(4)]
        if not self.k32.GetProcessTimes(h_process, *(self.ctypes.byref(t) for t in times)):
            return None
        return (times[0].dwHighDateTime << 32) | times[0].dwLowDateTime

    def process_creation_time(self, pid: int) -> Optional[int]:
        h_process = self.k32.OpenProcess(self.PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not h_process:
            return None
        try:
            return self._creation_time(h_process)
        finally:
            self.close(h_process)

    def terminate_pid_if_created_at(self, pid: int, created: int, wait_ms: int = 3000) -> bool:
        """Kill `pid` only if it is still the process created at `created`
        (a FILETIME): a bare pid may have been reused by anything since."""
        access = self.PROCESS_TERMINATE | self.PROCESS_QUERY_LIMITED_INFORMATION | self.SYNCHRONIZE
        h_process = self.k32.OpenProcess(access, False, pid)
        if not h_process:
            return False
        try:
            if self._creation_time(h_process) != created:
                return False
            if not self.k32.TerminateProcess(h_process, 1):
                return False
            self.k32.WaitForSingleObject(h_process, wait_ms)
            return True
        finally:
            self.close(h_process)


_WIN32_PROCESS_API = None  # None = not loaded yet, False = unavailable


def _win32_process_api() -> Optional[_Win32ProcessApi]:
    global _WIN32_PROCESS_API
    if sys.platform != "win32":
        return None
    if _WIN32_PROCESS_API is None:
        try:
            _WIN32_PROCESS_API = _Win32ProcessApi()
        except Exception as exc:
            _log_failure("load kernel32 process API", exc)
            _WIN32_PROCESS_API = False
    return _WIN32_PROCESS_API or None


_WINDOWS_CLEANUP_JOB_HANDLE = None


def _get_windows_cleanup_job():
    """Lazily create one Windows Job Object with KILL_ON_JOB_CLOSE for the lifetime
    of this process. Any ffmpeg child assigned to it is terminated by Windows itself
    if this process dies — including an ungraceful crash or Task Manager 'End Task'
    where our own lifespan shutdown / _stop_*_process cleanup never gets to run.
    Only a fallback now: each run normally gets its own job (_create_run_job)."""
    global _WINDOWS_CLEANUP_JOB_HANDLE
    if _WINDOWS_CLEANUP_JOB_HANDLE is not None:
        return _WINDOWS_CLEANUP_JOB_HANDLE
    api = _win32_process_api()
    if api is None:
        return None
    try:
        _WINDOWS_CLEANUP_JOB_HANDLE = api.create_kill_on_close_job()
    except Exception as exc:
        _log_failure("create Windows job object for child-process cleanup", exc)
        return None
    return _WINDOWS_CLEANUP_JOB_HANDLE


def _assign_child_to_cleanup_job(pid: int) -> None:
    """Best-effort; a failure here just means we fall back to explicit stop-on-shutdown
    cleanup (already in place) instead of Windows guaranteeing it on an ungraceful exit."""
    api = _win32_process_api()
    h_job = _get_windows_cleanup_job()
    if api is None or not h_job:
        return
    try:
        api.assign_pid(h_job, pid)
    except Exception as exc:
        _log_failure(f"assign pid={pid} to cleanup job object", exc)


def _create_run_job(pid: int):
    """Put one ffmpeg run in its own Job Object (KILL_ON_JOB_CLOSE). Stopping
    the run is then TerminateJobObject, which acts on the process objects in
    the job and can never hit an unrelated process that reused the pid; and
    Windows still kills the run if Jellyball dies without cleaning up (the job
    handle closes with us). Returns the job handle, or None off Windows or if
    no job could be made (then the shared cleanup job gives crash safety)."""
    api = _win32_process_api()
    if api is None:
        return None
    h_job = None
    try:
        h_job = api.create_kill_on_close_job()
        if h_job and api.assign_pid(h_job, pid):
            return h_job
    except Exception as exc:
        _log_failure(f"create job object for ffmpeg pid={pid}", exc)
    if h_job:
        api.close(h_job)
    _assign_child_to_cleanup_job(pid)
    return None


RUN_PID_FILE = "ffmpeg.pid"


# --- Run roots ----------------------------------------------------------------
# Directories that hold ffmpeg run directories (Multi-View, the placeholder,
# any later feature). Each owner registers its root when imported; the startup
# kill of orphaned ffmpeg, the periodic sweep of leftover run directories and
# the startup/shutdown wipe all iterate this registry instead of naming roots.


class RunRoot(NamedTuple):
    path: Path
    # True: <root>/<channel>/runN (empty channel dirs are swept too);
    # False: <root>/runN.
    per_channel: bool


_RUN_ROOTS: List[Tuple[Callable[[], Path], bool, Optional[Callable[[], Iterable[Path]]]]] = []


def register_run_root(
    path: Union[Path, Callable[[], Path]],
    *,
    per_channel: bool = False,
    live_dirs: Optional[Callable[[], Iterable[Path]]] = None,
) -> None:
    """Register a directory of ffmpeg run directories. `path` may be a
    callable, read on every use (so a module constant patched in a test is
    honoured). `live_dirs` returns the run directories in use right now
    (running or still starting), which the sweep never deletes."""
    if isinstance(path, Path):
        fixed = path

        def getter() -> Path:
            return fixed
    else:
        getter = path
    _RUN_ROOTS.append((getter, per_channel, live_dirs))


def run_roots() -> List[RunRoot]:
    """The registered roots, in registration order, resolved now."""
    return [RunRoot(Path(getter()), per_channel) for getter, per_channel, _ in _RUN_ROOTS]


def live_run_dirs() -> Set[Path]:
    """Every run directory a registered owner reports as in use."""
    live: Set[Path] = set()
    for _, _, live_dirs in _RUN_ROOTS:
        if live_dirs is not None:
            live |= set(live_dirs())
    return live


def run_pid_files() -> List[Path]:
    """The pid file of every run directory under the registered roots."""
    pid_files: List[Path] = []
    for root in run_roots():
        pattern = f"*/run*/{RUN_PID_FILE}" if root.per_channel else f"run*/{RUN_PID_FILE}"
        try:
            pid_files += list(root.path.glob(pattern))
        except OSError:
            continue
    return pid_files


def _write_run_pid_file(run_dir: Path, pid: int) -> None:
    """Record pid + creation time in the run dir so the next startup can kill
    this ffmpeg if Jellyball died without stopping it (and its job didn't take
    it down). Windows only: the creation time is what proves a live pid is
    still our ffmpeg rather than a reused pid."""
    api = _win32_process_api()
    if api is None:
        return
    try:
        created = api.process_creation_time(pid)
        if created is not None:
            (run_dir / RUN_PID_FILE).write_text(json.dumps({"pid": pid, "created": created}), encoding="utf-8")
    except Exception as exc:
        _log_failure(f"write ffmpeg pid file pid={pid}", exc)


async def _taskkill_tree(pid: int, label: str) -> None:
    try:
        kill_proc = await asyncio.create_subprocess_exec(
            "taskkill", "/F", "/T", "/PID", str(pid),
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            creationflags=_child_process_creationflags(),
        )
        await asyncio.wait_for(kill_proc.wait(), timeout=10.0)
    except (OSError, asyncio.TimeoutError) as exc:
        _log_failure(f"taskkill ffmpeg {label}", exc, logging.ERROR)


async def _terminate_ffmpeg(entry: dict, label: str, *, term_timeout: float = 5.0, kill_timeout: float = 3.0) -> None:
    """Stop one Multi-View/placeholder ffmpeg run.

    Windows: TerminateJobObject on the run's own job. It doesn't depend on
    process.returncode (asyncio's Proactor loop has been seen reporting a
    heavily-piped ffmpeg as exited while it was still encoding) and, unlike
    `taskkill /PID`, can't kill an unrelated process that reused the pid.
    Without a job: TerminateProcess through our own process handle (also
    immune to pid reuse), and taskkill only as a last resort while asyncio
    still considers the process alive."""
    process: asyncio.subprocess.Process = entry["process"]
    job = entry.pop("job", None)
    try:
        if sys.platform == "win32":
            api = _win32_process_api()
            terminated = False
            if job and api is not None:
                try:
                    terminated = api.terminate_job(job)
                except Exception as exc:
                    _log_failure(f"terminate job ffmpeg {label}", exc)
            if not terminated and process.returncode is None:
                try:
                    process.kill()
                except (ProcessLookupError, OSError):
                    pass
            try:
                await asyncio.wait_for(process.wait(), timeout=kill_timeout)
            except asyncio.TimeoutError:
                if process.returncode is None:
                    await _taskkill_tree(process.pid, label)
                    try:
                        await asyncio.wait_for(process.wait(), timeout=kill_timeout)
                    except asyncio.TimeoutError:
                        LOGGER.error("ffmpeg %s pid=%s did not exit after kill", label, process.pid)
        elif process.returncode is None:
            try:
                process.terminate()
                await asyncio.wait_for(process.wait(), timeout=term_timeout)
            except (asyncio.TimeoutError, ProcessLookupError):
                try:
                    process.kill()
                    await asyncio.wait_for(process.wait(), timeout=kill_timeout)
                except (asyncio.TimeoutError, ProcessLookupError) as exc:
                    _log_failure(f"kill ffmpeg {label}", exc, logging.ERROR)
    finally:
        if job:
            api = _win32_process_api()
            if api is not None:
                api.close(job)


def _rmtree_with_retries(path: Path, attempts: int = 5, delay: float = 1.0) -> bool:
    """Blocking (run it in a thread). Windows keeps a just-killed process's
    files locked for a moment, so one rmtree right after the kill used to
    leave the whole run directory (~100MB of segments) behind."""
    for attempt in range(attempts):
        try:
            shutil.rmtree(path)
            return True
        except OSError:
            if not path.exists():
                return True
            if attempt + 1 < attempts:
                time.sleep(delay)
    return not path.exists()


def _remove_tree_later(path: Path, label: str) -> None:
    """Delete a run directory off the event loop, retrying while files are locked."""
    _spawn_background_task(asyncio.to_thread(_rmtree_with_retries, path), f"remove {label} output dir")


def _prepare_run_dir(run_dir: Path, audio_outputs: int) -> None:
    """Blocking: a fresh, empty run directory (plus a{N}/ per Multi-View audio output)."""
    if run_dir.exists():
        shutil.rmtree(run_dir, ignore_errors=True)
    run_dir.mkdir(parents=True, exist_ok=True)
    for idx in range(audio_outputs):
        (run_dir / f"a{idx}").mkdir(parents=True, exist_ok=True)


async def _wait_for_first_segment(
    out_dir: Path,
    process: "asyncio.subprocess.Process",
    timeout: float,
    poll_interval: float = 0.25,
) -> bool:
    resolved_out_dir = _resolve_within(MULTIVIEW_OUTPUT_ROOT, out_dir)
    if resolved_out_dir is None:
        return False
    playlist = resolved_out_dir / "index.m3u8"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.returncode is not None:
            return False
        if playlist.exists():
            try:
                text = playlist.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                text = ""
            if "#EXTINF" in text:
                first_segment = next((line for line in text.splitlines() if line.endswith(".ts")), None)
                if first_segment:
                    segment_path = Path(first_segment)
                    if segment_path.is_absolute() or ".." in segment_path.parts:
                        await asyncio.sleep(poll_interval)
                        continue
                    candidate = out_dir / segment_path
                    try:
                        resolved_candidate = candidate.resolve()
                        resolved_candidate.relative_to(resolved_out_dir)
                    except (OSError, ValueError):
                        resolved_candidate = None
                    if resolved_candidate is not None and resolved_candidate.exists():
                        return True
        await asyncio.sleep(poll_interval)
    return False


def _ffmpeg_filter_path(path: Path) -> str:
    """A path for a single-quoted filter option value: forward slashes, and
    the drive colon escaped (ffmpeg splits filter options on ':')."""
    return str(path).replace("\\", "/").replace(":", "\\:").replace("'", "'\\''")
