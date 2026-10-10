"""ffmpeg remux ingest for sources the session engine cannot read directly.

fMP4/CMAF segments and demuxed-audio renditions are remuxed (never
transcoded in the common case) by one ffmpeg per active source into MPEG-TS
HLS on local disk. The channel session then reads that local HLS through the
existing `SourceSpec.local` path, so normalization, discontinuity handling,
and real-playback failover all work exactly like they do for native TS
sources. This replaces the old pre-session passthrough proxy.

SAMPLE-AES sources are NOT handled here (ffmpeg cannot decrypt them); they
use the minimal shim in legacy_proxy.py.
"""

import asyncio
import os
import shutil
import sys
import tempfile
import time
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set

import ffmpeg_proc
from config import LOGGER, _log_failure
from network_safety import bounded_int

# Scratch root for per-source remux output. Under the OS temp dir by default;
# each run directory holds one index.m3u8 plus a rolling set of segments and
# is deleted when the remux stops or the source switches.
REMUX_OUTPUT_ROOT = Path(
    os.getenv("JELLYBALL_REMUX_DIR", "") or os.path.join(tempfile.gettempdir(), "jellyball-remux")
)
# Cap on simultaneous remux ffmpeg processes (each is one lightweight -c copy).
REMUX_CONCURRENCY = bounded_int(os.getenv("REMUX_CONCURRENCY", "4"), 4, 1, 32)
# Seconds of HLS per output segment; matches the session's live-edge math.
_REMUX_HLS_TIME = 6
_REMUX_HLS_LIST_SIZE = 12
# How long to wait for the first segments before the spawn counts as failed.
_STARTUP_TIMEOUT_SECONDS = 30.0
# The local playlist is healthy while its mtime advances within this window.
_HEALTHY_MTIME_WINDOW = 3 * _REMUX_HLS_TIME + 10.0

_REMUX_SEMAPHORE: Optional[asyncio.Semaphore] = None
# Run directories currently owned by a live RemuxSession (never swept).
_REMUX_LIVE_DIRS: Set[Path] = set()


def _remux_live_run_dirs() -> Set[Path]:
    return set(_REMUX_LIVE_DIRS)


# Read at call time, so a patched REMUX_OUTPUT_ROOT is what gets swept.
ffmpeg_proc.register_run_root(lambda: REMUX_OUTPUT_ROOT, live_dirs=_remux_live_run_dirs)


def headers_to_ffmpeg_args(headers: Dict[str, str]) -> List[str]:
    """Render request headers as ffmpeg `-headers` / `-cookies` arguments.

    ffmpeg's HLS demuxer takes extra headers as one CRLF-joined `-headers`
    value; cookies go through the separate `-cookies` option. Values are
    passed as single argv entries (no shell), with CR/LF stripped so a
    hostile header value cannot inject extra ffmpeg options.
    """
    header_lines = []
    cookies = ""
    for key, value in (headers or {}).items():
        clean_key = str(key).replace("\r", "").replace("\n", "").strip()
        clean_value = str(value).replace("\r", "").replace("\n", "").strip()
        if not clean_key or not clean_value:
            continue
        if clean_key.lower() == "cookie":
            cookies = clean_value
        else:
            header_lines.append(f"{clean_key}: {clean_value}")
    args: List[str] = []
    if header_lines:
        args += ["-headers", "\r\n".join(header_lines) + "\r\n"]
    if cookies:
        args += ["-cookies", cookies]
    return args


@dataclass
class RemuxSpec:
    """What to remux: variant URL + headers + which streams to map."""

    url: str  # chosen variant (or media playlist) URL
    audio_url: str = ""  # demuxed audio rendition URL, if any
    referer: str = ""
    origin: str = ""
    cookies: str = ""
    user_agent: str = ""

    def ffmpeg_input_args(self) -> List[str]:
        """Input-side ffmpeg args: headers plus one or two -i inputs."""
        from config import _upstream_media_headers

        headers = _upstream_media_headers(self.referer, self.origin, self.cookies)
        if self.user_agent:
            headers["User-Agent"] = self.user_agent
        args = headers_to_ffmpeg_args(headers)
        args += ["-i", self.url]
        if self.audio_url:
            args += ["-i", self.audio_url]
        return args


def build_remux_args(spec: RemuxSpec, out_dir, transcode_audio: bool = False) -> List[str]:
    """Pure command-builder for the remux ffmpeg (no I/O, unit-testable).

    `-c copy` keeps this a remux (no CPU cost). When the source carries an
    audio codec that does not fit MPEG-TS, the caller retries once with
    `transcode_audio=True` (`-c:a aac`, video still copied).
    """
    ffmpeg = ffmpeg_proc.FFMPEG_PATH
    out_dir = Path(out_dir)
    args = [ffmpeg, "-y", "-hide_banner", "-nostats", "-loglevel", "warning"]
    args += spec.ffmpeg_input_args()
    if spec.audio_url:
        args += ["-map", "0:v:0", "-map", "1:a:0"]
    if transcode_audio:
        args += ["-c:v", "copy", "-c:a", "aac"]
    else:
        args += ["-c", "copy"]
    args += [
        "-f", "hls",
        "-hls_time", str(_REMUX_HLS_TIME),
        "-hls_list_size", str(_REMUX_HLS_LIST_SIZE),
        "-hls_flags", "delete_segments+independent_segments",
        "-hls_segment_filename", str(out_dir / "seg%05d.ts"),
        str(out_dir / "index.m3u8"),
    ]
    return args


class RemuxSession:
    """One ffmpeg remuxing an upstream source to local HLS on disk."""

    def __init__(self) -> None:
        self.spec: Optional[RemuxSpec] = None
        self.out_dir: Optional[Path] = None
        self.process: Optional["asyncio.subprocess.Process"] = None
        self._job = None  # Windows Job Object for crash-safe cleanup
        self._last_playlist_mtime: float = 0.0
        self._last_healthy_at: float = time.monotonic()
        self._stderr_tail: List[str] = []
        self.transcoded_audio = False  # True when the -c:a aac fallback was used

    @property
    def index_path(self) -> Optional[Path]:
        return self.out_dir / "index.m3u8" if self.out_dir else None

    async def start(self, spec: RemuxSpec) -> str:
        """Spawn ffmpeg and wait for the local playlist to start advancing.

        Returns the local index.m3u8 path. Raises RuntimeError on immediate
        failure (ffmpeg missing, spawn error, no segments within the startup
        window). Retries once with `-c:a aac` when the `-c copy` attempt fails
        to mux (exotic audio codecs that do not fit MPEG-TS).
        """
        global _REMUX_SEMAPHORE
        if not ffmpeg_proc.FFMPEG_AVAILABLE:
            raise RuntimeError("ffmpeg is not available")
        if _REMUX_SEMAPHORE is None:
            _REMUX_SEMAPHORE = asyncio.Semaphore(REMUX_CONCURRENCY)
        self.spec = spec
        self.out_dir = REMUX_OUTPUT_ROOT / f"run{int(time.time() * 1000)}_{os.getpid()}"
        self.out_dir.mkdir(parents=True, exist_ok=True)
        _REMUX_LIVE_DIRS.add(self.out_dir)

        async with _REMUX_SEMAPHORE:
            try:
                await self._spawn_with_fallback()
            except Exception:
                await self._cleanup_dir()
                raise
        index = self.index_path
        assert index is not None
        return str(index)

    async def _spawn_with_fallback(self) -> None:
        try:
            await self._spawn(transcode_audio=False)
        except RuntimeError as exc:
            LOGGER.warning("Remux -c copy failed, retrying with -c:a aac url=%s (%s)", self._safe_url(), exc)
            await self._stop_process()
            await self._spawn(transcode_audio=True)
            self.transcoded_audio = True
            self._record_telemetry("transcode")

    def _safe_url(self) -> str:
        url = (self.spec.url if self.spec else "") or ""
        parsed = urllib.parse.urlsplit(url)
        return f"{parsed.scheme}://{parsed.netloc}/..." if parsed.netloc else url[:60]

    async def _spawn(self, transcode_audio: bool) -> None:
        assert self.spec is not None and self.out_dir is not None
        args = build_remux_args(self.spec, self.out_dir, transcode_audio=transcode_audio)
        LOGGER.info(
            "Starting remux ingest url=%s audio=%s transcode_audio=%s",
            self._safe_url(), bool(self.spec.audio_url), transcode_audio,
        )
        try:
            self.process = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
                creationflags=ffmpeg_proc._child_process_creationflags(),
            )
        except OSError as exc:
            raise RuntimeError(f"could not spawn ffmpeg: {exc}") from exc
        self._job = ffmpeg_proc._create_run_job(self.process.pid)
        ffmpeg_proc._write_run_pid_file(self.out_dir, self.process.pid)
        self._record_telemetry("started")
        await self._wait_for_startup()

    async def _wait_for_startup(self) -> None:
        """Wait until the local playlist exists with segments, or fail fast."""
        assert self.index_path is not None
        deadline = time.monotonic() + _STARTUP_TIMEOUT_SECONDS
        drain_task = asyncio.create_task(self._drain_stderr())
        try:
            while time.monotonic() < deadline:
                if self.process is not None and self.process.returncode is not None:
                    raise RuntimeError(f"ffmpeg exited during startup: {self._stderr_summary()}")
                if self._playlist_has_segments():
                    self._last_playlist_mtime = self.index_path.stat().st_mtime
                    return
                await asyncio.sleep(0.5)
            raise RuntimeError(f"ffmpeg produced no segments in {_STARTUP_TIMEOUT_SECONDS:.0f}s: {self._stderr_summary()}")
        finally:
            if not drain_task.done():
                drain_task.cancel()

    def _playlist_has_segments(self) -> bool:
        assert self.index_path is not None
        try:
            text = self.index_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return False
        return "#EXTINF" in text

    async def _drain_stderr(self) -> None:
        """Keep the last stderr lines for failure diagnosis (bounded)."""
        if self.process is None or self.process.stderr is None:
            return
        try:
            async for raw in self.process.stderr:
                line = raw.decode("utf-8", errors="replace").strip()
                if line:
                    self._stderr_tail.append(line)
                    del self._stderr_tail[:-20]
        except (asyncio.CancelledError, Exception):
            pass

    def _stderr_summary(self) -> str:
        return "; ".join(self._stderr_tail[-3:]) or "no ffmpeg output"

    def healthy(self) -> bool:
        """Process alive and the local playlist still advancing."""
        if self.process is None or self.process.returncode is not None:
            return False
        if self.index_path is None:
            return False
        try:
            mtime = self.index_path.stat().st_mtime
        except OSError:
            return False
        # st_mtime is wall-clock while time.monotonic is not; compare mtime
        # samples against each other and track the healthy instant separately.
        if mtime > self._last_playlist_mtime:
            self._last_playlist_mtime = mtime
            self._last_healthy_at = time.monotonic()
            return True
        return (time.monotonic() - self._last_healthy_at) < _HEALTHY_MTIME_WINDOW

    async def stop(self) -> None:
        """Terminate ffmpeg and delete the scratch directory."""
        await self._stop_process()
        await self._cleanup_dir()

    async def _stop_process(self) -> None:
        process, self.process = self.process, None
        job, self._job = self._job, None
        if process is None:
            return
        try:
            if sys.platform == "win32":
                api = ffmpeg_proc._win32_process_api()
                if job is not None and api is not None:
                    api.terminate_job(job)
                else:
                    process.terminate()
            else:
                process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
        except (ProcessLookupError, OSError) as exc:
            _log_failure("stop remux ffmpeg", exc)
        finally:
            if job is not None:
                api = ffmpeg_proc._win32_process_api()
                if api is not None:
                    api.close(job)

    async def _cleanup_dir(self) -> None:
        out_dir, self.out_dir = self.out_dir, None
        if out_dir is not None:
            _REMUX_LIVE_DIRS.discard(out_dir)
            try:
                await asyncio.to_thread(shutil.rmtree, out_dir, ignore_errors=True)
            except Exception as exc:
                _log_failure(f"clean remux dir {out_dir}", exc)

    def _record_telemetry(self, event: str) -> None:
        try:
            from engine_stats import record_remux_event
        except ImportError:
            return
        provider = ""
        if self.spec is not None:
            # The referer host usually identifies the provider; fall back to
            # the stream host when there is none.
            for candidate in (self.spec.referer, self.spec.url):
                parsed = urllib.parse.urlsplit(candidate or "")
                if parsed.netloc:
                    provider = parsed.netloc
                    break
        try:
            record_remux_event(event, provider)
        except Exception:
            pass
