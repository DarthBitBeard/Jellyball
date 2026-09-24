"""Long-running stability soak test for the Jellyball server (no internet needed).

    python tools/soak.py [--minutes 30] [--channels 3] [--flap-every 90]

Reuses tools/e2e_failover.py's helpers (free_port, make_source, serve) to run
Jellyball in-process on a scratch data directory, with N independent channels
each backed by a pair of fake live HLS origins (A/B). A small custom origin
(unlike e2e_failover's, which only supports kill) loops a short pre-generated
segment set forever -- looping in MEDIA-SEQUENCE with a MEDIA-SEQUENCE that
never stops climbing and an #EXT-X-DISCONTINUITY every time the physical
segment set wraps -- so a couple of minutes of encoded video can drive a
multi-hour run. Every --flap-every seconds one channel's currently active
origin is killed and revived a bit later, forcing repeated failover.

One real ffmpeg consumer per channel stream-copies the channel's HLS output
to a null muxer (`-c copy -f null -`) and reports progress via
`-progress pipe:1`, so we can track how far behind wall-clock its decoded
output time falls, and whether it ever dies (a restart counts as a failure).

Every 30s the script samples and logs (CSV + a printed line): process RSS,
Windows handle count, Python thread count, number of live ffmpeg children,
and per-channel session snapshot fields (state, source, window_segments,
memory_mb, source_switches, segment_failures) plus each consumer's lag
(wall time elapsed since it started minus its own decoded output time).

At the end it prints a PASS/FAIL summary. FAIL if: any consumer restarted;
any consumer's final lag exceeds its startup lag by more than 30s; process
RSS grew by more than 30% between the 25% and 100% marks of the run; or any
channel's session memory_mb ever exceeded 300. On PASS (and without --keep)
the scratch work dir is removed; it is kept on FAIL for inspection.
"""
from __future__ import annotations

import argparse
import csv
import ctypes
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))

from e2e_failover import free_port, make_source, serve  # noqa: E402

SEGMENT_SECONDS = 2
WINDOW = 5
SOURCE_SECONDS = 125  # ~2 minutes of pre-generated segments, looped forever
SAMPLE_EVERY = 30.0


# ---------------------------------------------------------------------------
# Custom looping origin: kill + revive, infinite MEDIA-SEQUENCE, discontinuity
# on wrap. Content is generated once per "content key" (a/b) and shared by
# many independently killable/revivable routes (one pair per channel), so an
# hours-long run only needs a couple of ffmpeg encodes up front.
# ---------------------------------------------------------------------------

def build_soak_origin(root: Path, routes: dict):
    """routes: {route_name: content_key}. Each route has its own kill state;
    content_key selects which pre-generated segment set it loops."""
    from fastapi import FastAPI, Response

    origin = FastAPI()
    started = time.monotonic()
    killed = {route: False for route in routes}
    file_cache: dict = {}

    def files_for(content_key: str):
        if content_key not in file_cache:
            file_cache[content_key] = sorted(p.name for p in (root / content_key).glob(f"{content_key}_*.ts"))
        return file_cache[content_key]

    def playlist(route: str) -> str:
        content_key = routes[route]
        names = files_for(content_key)
        n = len(names)
        live_index = int((time.monotonic() - started) / SEGMENT_SECONDS)
        first = max(0, live_index - WINDOW + 1)
        lines = ["#EXTM3U", "#EXT-X-VERSION:3", f"#EXT-X-TARGETDURATION:{SEGMENT_SECONDS}", f"#EXT-X-MEDIA-SEQUENCE:{first}"]
        for seq in range(first, live_index + 1):
            file_index = seq % n
            if file_index == 0 and seq > 0:
                lines.append("#EXT-X-DISCONTINUITY")
            lines += [f"#EXTINF:{SEGMENT_SECONDS}.000,", f"{content_key}_{file_index:04d}.ts"]
        return "\n".join(lines) + "\n"

    @origin.get("/{route}/live.m3u8")
    async def live(route: str):
        if route not in routes or killed.get(route):
            return Response(status_code=404)
        return Response(playlist(route), media_type="application/vnd.apple.mpegurl")

    @origin.get("/{route}/{segment}")
    async def segment(route: str, segment: str):
        if route not in routes or killed.get(route):
            return Response(status_code=404)
        path = root / routes[route] / segment
        if not path.is_file():
            return Response(status_code=404)
        return Response(path.read_bytes(), media_type="video/mp2t")

    @origin.post("/control/kill/{route}")
    async def kill(route: str):
        if route in killed:
            killed[route] = True
        return {"killed": sorted(r for r, v in killed.items() if v)}

    @origin.post("/control/revive/{route}")
    async def revive(route: str):
        if route in killed:
            killed[route] = False
        return {"killed": sorted(r for r, v in killed.items() if v)}

    return origin


# ---------------------------------------------------------------------------
# Process stats (RSS / handles / threads) without requiring psutil
# ---------------------------------------------------------------------------

try:
    import psutil
except ImportError:
    psutil = None


class _PROCESS_MEMORY_COUNTERS(ctypes.Structure):
    _fields_ = [
        ("cb", ctypes.c_ulong),
        ("PageFaultCount", ctypes.c_ulong),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
    ]


def process_stats():
    """Returns (rss_mb, handle_count, thread_count) for this process."""
    threads = threading.active_count()
    if psutil is not None:
        proc = psutil.Process(os.getpid())
        rss_mb = proc.memory_info().rss / (1024 * 1024)
        try:
            handles = proc.num_handles()
        except AttributeError:
            handles = -1
        return rss_mb, handles, threads
    try:
        kernel32 = ctypes.windll.kernel32
        psapi = ctypes.windll.psapi
        handle = kernel32.GetCurrentProcess()
        counters = _PROCESS_MEMORY_COUNTERS()
        counters.cb = ctypes.sizeof(_PROCESS_MEMORY_COUNTERS)
        psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb)
        rss_mb = counters.WorkingSetSize / (1024 * 1024)
        handle_count = ctypes.c_ulong()
        kernel32.GetProcessHandleCount(handle, ctypes.byref(handle_count))
        return rss_mb, handle_count.value, threads
    except Exception:
        return -1.0, -1, threads


# ---------------------------------------------------------------------------
# ffmpeg consumer: stream-copies the channel playlist to null, tracks output
# time via -progress pipe:1, restarts (and counts) on unexpected exit.
# ---------------------------------------------------------------------------

class Consumer:
    def __init__(self, channel_id: str, url: str, ffmpeg: str, log_path: Path):
        self.channel_id = channel_id
        self.url = url
        self.ffmpeg = ffmpeg
        self.log_path = log_path
        self.process: subprocess.Popen | None = None
        self.reader_thread: threading.Thread | None = None
        self.started_at = 0.0
        self.out_time_s = 0.0
        self.restarts = 0
        self.lock = threading.Lock()
        self._attempt = 0

    def start(self) -> None:
        self._attempt += 1
        mode = "a" if self._attempt > 1 else "w"
        self._log_fh = open(self.log_path, mode, encoding="utf-8", errors="replace")
        if self._attempt > 1:
            self._log_fh.write(f"\n--- restart attempt {self._attempt} ---\n")
            self._log_fh.flush()
        cmd = [
            self.ffmpeg, "-hide_banner", "-loglevel", "warning", "-nostats",
            "-progress", "pipe:1", "-i", self.url,
            "-map", "0:v:0", "-map", "0:a:0", "-c", "copy", "-f", "null", "-",
        ]
        self.process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=self._log_fh,
            text=True, bufsize=1, encoding="utf-8", errors="replace",
        )
        with self.lock:
            self.started_at = time.monotonic()
            self.out_time_s = 0.0
        self.reader_thread = threading.Thread(target=self._read_progress, daemon=True)
        self.reader_thread.start()

    def _read_progress(self) -> None:
        proc = self.process
        if proc is None or proc.stdout is None:
            return
        try:
            for line in proc.stdout:
                line = line.strip()
                if line.startswith("out_time_ms="):
                    try:
                        micros = int(line.split("=", 1)[1])
                    except ValueError:
                        continue
                    with self.lock:
                        self.out_time_s = micros / 1_000_000.0
        except (ValueError, OSError):
            pass

    def is_alive(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def lag_seconds(self) -> float:
        with self.lock:
            started_at, out_time_s = self.started_at, self.out_time_s
        wall_elapsed = time.monotonic() - started_at
        return wall_elapsed - out_time_s

    def stop(self) -> None:
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
        try:
            self._log_fh.close()
        except Exception:
            pass


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ffmpeg", default=os.environ.get("FFMPEG_PATH") or shutil.which("ffmpeg")
                        or str(Path(os.environ.get("LOCALAPPDATA", "")) / "ffmpeg" / "bin" / "ffmpeg.exe"))
    parser.add_argument("--minutes", type=float, default=30.0)
    parser.add_argument("--channels", type=int, default=3)
    parser.add_argument("--flap-every", type=float, default=90.0)
    parser.add_argument("--keep", action="store_true", help="keep the scratch directory even on PASS")
    args = parser.parse_args()
    ffmpeg = args.ffmpeg
    total_seconds = args.minutes * 60.0

    work = Path(tempfile.mkdtemp(prefix="jellyball-soak-"))
    print(f"work dir: {work}")
    origin_root = work / "origin"
    make_source(ffmpeg, origin_root / "a", "a", SOURCE_SECONDS, size="640x360", pattern="testsrc2", offset=0, pid=0x100)
    make_source(ffmpeg, origin_root / "b", "b", SOURCE_SECONDS, size="1280x720", pattern="testsrc", offset=5000, pid=0x300)

    channel_ids = [f"soak{i}" for i in range(args.channels)]
    routes = {}          # route_name -> content_key ("a"/"b")
    channel_routes = {}  # channel_id -> (route_a, route_b)
    for i, cid in enumerate(channel_ids):
        route_a, route_b = f"ch{i}a", f"ch{i}b"
        routes[route_a] = "a"
        routes[route_b] = "b"
        channel_routes[cid] = (route_a, route_b)

    origin_port, app_port = free_port(), free_port()
    os.environ.update({
        "JELLYBALL_DATA_DIR": str(work / "data"),
        "ALLOW_PRIVATE_UPSTREAMS": "1",
        "PORT": str(app_port),
        "FFMPEG_PATH": ffmpeg,
        "SESSION_IDLE_SECONDS": "60",
    })
    import httpx
    import main as jellyball

    serve(build_soak_origin(origin_root, routes), origin_port)
    serve(jellyball.app, app_port)
    origin_base = f"http://127.0.0.1:{origin_port}"
    app_base = f"http://127.0.0.1:{app_port}"
    http = httpx.Client(timeout=5.0)

    for cid in channel_ids:
        route_a, route_b = channel_routes[cid]
        jellyball.stream_state[cid] = {
            "name": cid, "query": cid, "active_index": 0, "is_healthy": True,
            "candidates": [
                {"provider": "OriginA", "url": f"{origin_base}/{route_a}/live.m3u8", "referer": "", "origin": ""},
                {"provider": "OriginB", "url": f"{origin_base}/{route_b}/live.m3u8", "referer": "", "origin": ""},
            ],
            "always_live": True, "category": "custom", "content_type": "team", "search_terms": [],
            "start_time": "", "stop_time": "", "logo_url": "", "catalog_key": "", "tvg_id": "", "group_title": "",
            **jellyball._scrape_lifecycle_defaults(),
        }

    consumers = {}
    for cid in channel_ids:
        url = f"{app_base}/stream/{cid}.m3u8"
        consumers[cid] = Consumer(cid, url, ffmpeg, work / f"ffmpeg_{cid}.log")
    for c in consumers.values():
        c.start()

    csv_path = work / "soak_metrics.csv"
    fieldnames = ["elapsed_s", "rss_mb", "handles", "threads", "ffmpeg_children"]
    for cid in channel_ids:
        fieldnames += [f"{cid}_state", f"{cid}_source", f"{cid}_window_segments", f"{cid}_memory_mb",
                       f"{cid}_source_switches", f"{cid}_segment_failures", f"{cid}_out_time_s",
                       f"{cid}_lag_s", f"{cid}_restarts"]
    csv_file = open(csv_path, "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()

    samples: list[dict] = []
    max_session_mem: dict = {cid: 0.0 for cid in channel_ids}
    startup_lag: dict = {}

    def take_sample(elapsed: float) -> dict:
        rss_mb, handles, threads = process_stats()
        ffmpeg_children = sum(1 for c in consumers.values() if c.is_alive())
        row = {"elapsed_s": round(elapsed, 1), "rss_mb": round(rss_mb, 1), "handles": handles,
               "threads": threads, "ffmpeg_children": ffmpeg_children}
        parts = [f"[{elapsed:7.1f}s] rss={rss_mb:.0f}MB handles={handles} threads={threads} ffmpeg={ffmpeg_children}/{len(consumers)}"]
        for cid in channel_ids:
            session = jellyball.SESSIONS.peek(cid)
            snap = session.snapshot() if session else {}
            mem = snap.get("memory_mb", 0.0) or 0.0
            max_session_mem[cid] = max(max_session_mem[cid], mem)
            consumer = consumers[cid]
            lag = consumer.lag_seconds()
            if cid not in startup_lag:
                startup_lag[cid] = lag
            row.update({
                f"{cid}_state": snap.get("state", ""), f"{cid}_source": snap.get("source", ""),
                f"{cid}_window_segments": snap.get("window_segments", 0), f"{cid}_memory_mb": mem,
                f"{cid}_source_switches": snap.get("source_switches", 0),
                f"{cid}_segment_failures": snap.get("segment_failures", 0),
                f"{cid}_out_time_s": round(consumer.out_time_s, 1), f"{cid}_lag_s": round(lag, 1),
                f"{cid}_restarts": consumer.restarts,
            })
            parts.append(f"{cid}[{snap.get('state','?')} src={snap.get('source','')} win={snap.get('window_segments',0)} "
                         f"mem={mem:.1f}MB sw={snap.get('source_switches',0)} segfail={snap.get('segment_failures',0)} "
                         f"lag={lag:.1f}s restarts={consumer.restarts}]")
        print(" ".join(parts), flush=True)
        writer.writerow(row)
        csv_file.flush()
        samples.append(row)
        return row

    def active_route(cid: str) -> str:
        idx = jellyball.stream_state[cid].get("active_index", 0)
        route_a, route_b = channel_routes[cid]
        return route_b if idx == 1 else route_a

    revive_delay = max(10.0, min(args.flap_every - 10.0, args.flap_every / 2.0))
    pending_revives: list = []
    last_flap = 0.0
    flap_idx = 0
    next_sample_mark = SAMPLE_EVERY
    start = time.monotonic()

    try:
        while True:
            elapsed = time.monotonic() - start
            if elapsed >= total_seconds:
                break

            for cid, c in consumers.items():
                if not c.is_alive():
                    c.restarts += 1
                    code = c.process.returncode if c.process else None
                    print(f"[{elapsed:7.1f}s] channel {cid}: ffmpeg exited (code {code}); restarting (restart #{c.restarts})", flush=True)
                    c.start()

            if args.flap_every > 0 and elapsed - last_flap >= args.flap_every:
                cid = channel_ids[flap_idx % len(channel_ids)]
                route = active_route(cid)
                try:
                    http.post(f"{origin_base}/control/kill/{route}")
                except httpx.HTTPError:
                    pass
                pending_revives.append([time.monotonic() + revive_delay, route])
                print(f"[{elapsed:7.1f}s] flap: killed {route} (channel {cid}); reviving in {revive_delay:.0f}s", flush=True)
                flap_idx += 1
                last_flap = elapsed

            still_pending = []
            now_mono = time.monotonic()
            for revive_at, route in pending_revives:
                if now_mono >= revive_at:
                    try:
                        http.post(f"{origin_base}/control/revive/{route}")
                    except httpx.HTTPError:
                        pass
                    print(f"[{elapsed:7.1f}s] flap: revived {route}", flush=True)
                else:
                    still_pending.append([revive_at, route])
            pending_revives = still_pending

            if elapsed >= next_sample_mark:
                take_sample(elapsed)
                next_sample_mark += SAMPLE_EVERY

            time.sleep(1.0)

        final_elapsed = time.monotonic() - start
        final_row = take_sample(final_elapsed)
    finally:
        for c in consumers.values():
            c.stop()
        csv_file.close()

    # -- analysis ------------------------------------------------------------
    print("\n=== SUMMARY ===")
    total_restarts = sum(c.restarts for c in consumers.values())
    for cid in channel_ids:
        print(f"  {cid}: restarts={consumers[cid].restarts} max_session_mem_mb={max_session_mem[cid]:.1f} "
              f"final_lag_s={consumers[cid].lag_seconds():.1f} startup_lag_s={startup_lag.get(cid, 0.0):.1f}")

    fail_reasons = []
    if total_restarts > 0:
        fail_reasons.append(f"consumer(s) restarted: {[c.restarts for c in consumers.values()]}")

    for cid in channel_ids:
        final_lag = consumers[cid].lag_seconds()
        base = startup_lag.get(cid, 0.0)
        if final_lag - base > 30.0:
            fail_reasons.append(f"{cid} media lag grew {final_lag - base:.1f}s beyond startup offset (limit 30s)")

    sample_25 = None
    mark_25 = 0.25 * total_seconds
    for row in samples:
        if row["elapsed_s"] >= mark_25:
            sample_25 = row
            break
    if sample_25 and final_row and sample_25["rss_mb"] > 0:
        growth = (final_row["rss_mb"] - sample_25["rss_mb"]) / sample_25["rss_mb"]
        print(f"  rss @25%={sample_25['rss_mb']:.1f}MB rss @100%={final_row['rss_mb']:.1f}MB growth={growth * 100:.1f}%")
        if growth > 0.30:
            fail_reasons.append(f"RSS grew {growth * 100:.1f}% between 25% and 100% marks (limit 30%)")
    else:
        print("  rss growth check skipped (not enough samples)")

    for cid in channel_ids:
        if max_session_mem[cid] > 300.0:
            fail_reasons.append(f"{cid} session memory_mb reached {max_session_mem[cid]:.1f} (limit 300)")

    ok = not fail_reasons
    print(f"\nCSV: {csv_path}")
    if fail_reasons:
        print("RESULT: FAIL")
        for reason in fail_reasons:
            print(f"  - {reason}")
    else:
        print("RESULT: PASS")

    if not ok or args.keep:
        print(f"kept {work}")
    else:
        shutil.rmtree(work, ignore_errors=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
