"""End-to-end failover check with real ffmpeg (no internet needed).

    python tools/e2e_failover.py [--ffmpeg PATH] [--seconds 45] [--kill-at 15]

Starts two fake live HLS origins (A and B: different PIDs, resolutions and
timestamp bases - like two unrelated providers), runs Jellyball in-process on
a scratch data directory with one channel whose candidates are A then B, and
plays http://127.0.0.1:<port>/stream/e2e.m3u8 with ffmpeg the way Jellyfin's
live-TV transcode does. Partway through, origin A starts returning 404; the
channel session must fail over to B and ffmpeg must keep producing output.

Pass criteria: ffmpeg decodes (nearly) the whole run, never reports a "New
video/audio stream" (the old freeze), and the channel playlist carried an
#EXT-X-DISCONTINUITY.
"""
from __future__ import annotations

import argparse
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))

SEGMENT_SECONDS = 2
WINDOW = 5


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def make_source(ffmpeg: str, out: Path, name: str, total: int, *, size: str, pattern: str, offset: int, pid: int) -> None:
    out.mkdir(parents=True, exist_ok=True)
    if (out / f"{name}.m3u8").exists():
        return
    subprocess.run([
        ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
        "-f", "lavfi", "-i", f"{pattern}=size={size}:rate=30",
        "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000",
        "-t", str(total), "-c:v", "libx264", "-preset", "ultrafast",
        "-g", str(30 * SEGMENT_SECONDS), "-keyint_min", str(30 * SEGMENT_SECONDS), "-sc_threshold", "0",
        "-c:a", "aac", "-ac", "2", "-output_ts_offset", str(offset),
        "-f", "hls", "-hls_time", str(SEGMENT_SECONDS), "-hls_list_size", "0",
        "-hls_segment_options", f"mpegts_start_pid={pid}:mpegts_pmt_start_pid={0x1000 if pid == 0x100 else pid - 0x100}",
        "-hls_segment_filename", str(out / f"{name}_%04d.ts"), str(out / f"{name}.m3u8"),
    ], check=True)


def build_origin(root: Path):
    from fastapi import FastAPI, Response

    origin = FastAPI()
    started = time.monotonic()
    killed = set()

    def playlist(name: str) -> str:
        files = sorted(p.name for p in (root / name).glob(f"{name}_*.ts"))
        live_index = min(len(files) - 1, int((time.monotonic() - started) / SEGMENT_SECONDS) + WINDOW)
        first = max(0, live_index - WINDOW + 1)
        lines = ["#EXTM3U", "#EXT-X-VERSION:3", f"#EXT-X-TARGETDURATION:{SEGMENT_SECONDS}", f"#EXT-X-MEDIA-SEQUENCE:{first}"]
        for index in range(first, live_index + 1):
            lines += [f"#EXTINF:{SEGMENT_SECONDS}.000,", files[index]]
        return "\n".join(lines) + "\n"

    @origin.get("/{name}/live.m3u8")
    async def live(name: str):
        if name in killed:
            return Response(status_code=404)
        return Response(playlist(name), media_type="application/vnd.apple.mpegurl")

    @origin.get("/{name}/{segment}")
    async def segment(name: str, segment: str):
        path = root / name / segment
        if name in killed or not path.is_file():
            return Response(status_code=404)
        return Response(path.read_bytes(), media_type="video/mp2t")

    @origin.post("/control/kill/{name}")
    async def kill(name: str):
        killed.add(name)
        return {"killed": sorted(killed)}

    return origin


def serve(app, port: int):
    import uvicorn

    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_config=None, access_log=False))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 30
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.1)
    if not server.started:
        raise RuntimeError(f"server on {port} did not start")
    return server, thread


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ffmpeg", default=os.environ.get("FFMPEG_PATH") or shutil.which("ffmpeg")
                        or str(Path(os.environ.get("LOCALAPPDATA", "")) / "ffmpeg" / "bin" / "ffmpeg.exe"))
    parser.add_argument("--seconds", type=int, default=45)
    parser.add_argument("--kill-at", type=int, default=15)
    parser.add_argument("--keep", action="store_true", help="keep the scratch directory")
    args = parser.parse_args()
    ffmpeg = args.ffmpeg
    ffprobe = str(Path(ffmpeg).with_name("ffprobe.exe" if ffmpeg.lower().endswith(".exe") else "ffprobe"))

    work = Path(tempfile.mkdtemp(prefix="jellyball-e2e-"))
    total = args.seconds + 30
    print(f"work dir: {work}")
    make_source(ffmpeg, work / "origin" / "a", "a", total, size="640x360", pattern="testsrc2", offset=0, pid=0x100)
    make_source(ffmpeg, work / "origin" / "b", "b", total, size="1280x720", pattern="testsrc", offset=5000, pid=0x300)

    origin_port, app_port = free_port(), free_port()
    os.environ.update({
        "JELLYBALL_DATA_DIR": str(work / "data"),
        "ALLOW_PRIVATE_UPSTREAMS": "1",
        "PORT": str(app_port),
        "FFMPEG_PATH": ffmpeg,
        "SESSION_IDLE_SECONDS": "30",
    })
    import httpx
    import main as jellyball

    serve(build_origin(work / "origin"), origin_port)
    serve(jellyball.app, app_port)

    base = f"http://127.0.0.1:{origin_port}"
    jellyball.stream_state["e2e"] = {
        "name": "E2E Test", "query": "e2e", "active_index": 0, "is_healthy": True,
        "candidates": [
            {"provider": "OriginA", "url": f"{base}/a/live.m3u8", "referer": "", "origin": ""},
            {"provider": "OriginB", "url": f"{base}/b/live.m3u8", "referer": "", "origin": ""},
        ],
        "always_live": True, "category": "custom", "content_type": "team", "search_terms": [],
        "start_time": "", "stop_time": "", "logo_url": "", "catalog_key": "", "tvg_id": "", "group_title": "",
        **jellyball._scrape_lifecycle_defaults(),
    }

    channel_url = f"http://127.0.0.1:{app_port}/stream/e2e.m3u8"
    output = work / "out.ts"
    log_path = work / "ffmpeg.log"
    print(f"playing {channel_url} for {args.seconds}s; killing origin A at {args.kill_at}s")
    with open(log_path, "w", encoding="utf-8") as log:
        player = subprocess.Popen([
            ffmpeg, "-hide_banner", "-loglevel", "info", "-y", "-i", channel_url,
            "-map", "0:v:0", "-map", "0:a:0", "-c:v", "libx264", "-preset", "ultrafast",
            "-c:a", "aac", "-t", str(args.seconds), "-f", "mpegts", str(output),
        ], stdout=log, stderr=subprocess.STDOUT)

        saw_discontinuity = False
        started = time.monotonic()
        killed = False
        while player.poll() is None and time.monotonic() - started < args.seconds + 60:
            if not killed and time.monotonic() - started >= args.kill_at:
                httpx.post(f"{base}/control/kill/a")
                killed = True
                print(f"[{time.monotonic() - started:5.1f}s] origin A killed")
            try:
                text = httpx.get(channel_url, timeout=5).text
                saw_discontinuity = saw_discontinuity or "#EXT-X-DISCONTINUITY\n" in text
            except httpx.HTTPError:
                pass
            time.sleep(1)
        if player.poll() is None:
            player.kill()

    session = jellyball.SESSIONS.peek("e2e")
    snapshot = session.snapshot() if session else {}
    log_text = log_path.read_text(encoding="utf-8", errors="replace")
    probe = subprocess.run(
        [ffprobe, "-v", "error", "-show_entries", "format=duration", "-of", "default=nk=1:nw=1", str(output)],
        capture_output=True, text=True,
    )
    duration = float(probe.stdout.strip() or 0)
    new_stream = "New video stream" in log_text or "New audio stream" in log_text
    active = jellyball.stream_state["e2e"]["active_index"]

    print(f"ffmpeg exit code      : {player.returncode}")
    print(f"output duration       : {duration:.1f}s of {args.seconds}s")
    print(f"active candidate      : {active} ({'B' if active == 1 else 'A'})")
    print(f"playlist discontinuity: {saw_discontinuity}")
    print(f"'New stream' in log   : {new_stream}")
    print(f"session               : {snapshot}")
    ok = duration >= args.seconds - 6 and not new_stream and saw_discontinuity and active == 1
    print("RESULT:", "PASS" if ok else "FAIL")
    if not ok or args.keep:
        print(f"kept {work} (ffmpeg log: {log_path})")
    else:
        shutil.rmtree(work, ignore_errors=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
