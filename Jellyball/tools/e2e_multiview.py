"""End-to-end Multi-View check with real ffmpeg (no internet needed).

    python tools/e2e_multiview.py [--ffmpeg PATH] [--seconds 40]

Two member channels fed by fake live origins (different PIDs/timestamps), one
side-by-side Multi-View over them. Verifies:
  * the main channel (/stream/mv.m3u8) plays through a Jellyfin-style ffmpeg;
  * every per-audio output (a0, a1) lists identical segment names/durations;
  * the per-audio channel (/multiview/mv/audio-1.m3u8) serves a playlist;
  * switching the main channel's audio via the dashboard route does NOT
    restart the Multi-View ffmpeg (same PID) and playback continues;
  * with MULTIVIEW_HWACCEL=nvenc on a machine without NVENC, the run falls
    back to libx264 by itself.
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))

from e2e_failover import build_origin, free_port, make_source, serve  # noqa: E402


def member_state(jellyball, name: str, url: str) -> dict:
    return {
        "name": name, "query": name, "active_index": 0, "is_healthy": True,
        "candidates": [{"provider": f"Origin{name}", "url": url, "referer": "", "origin": ""}],
        "always_live": True, "category": "custom", "content_type": "team", "search_terms": [],
        "start_time": "", "stop_time": "", "logo_url": "", "catalog_key": "", "tvg_id": "", "group_title": "",
        **jellyball._scrape_lifecycle_defaults(),
    }


def segment_listing(playlist: Path) -> list:
    try:
        lines = playlist.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    return [line for line in lines if line.startswith("#EXTINF") or line.endswith(".ts")]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ffmpeg", default=os.environ.get("FFMPEG_PATH") or shutil.which("ffmpeg")
                        or str(Path(os.environ.get("LOCALAPPDATA", "")) / "ffmpeg" / "bin" / "ffmpeg.exe"))
    parser.add_argument("--seconds", type=int, default=40)
    parser.add_argument("--hwaccel", default="nvenc")
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()
    ffmpeg = args.ffmpeg
    ffprobe = str(Path(ffmpeg).with_name("ffprobe.exe" if ffmpeg.lower().endswith(".exe") else "ffprobe"))

    work = Path(tempfile.mkdtemp(prefix="jellyball-mv-"))
    print(f"work dir: {work}")
    total = args.seconds + 300
    make_source(ffmpeg, work / "origin" / "a", "a", total, size="640x360", pattern="testsrc2", offset=0, pid=0x100)
    make_source(ffmpeg, work / "origin" / "b", "b", total, size="1280x720", pattern="testsrc", offset=5000, pid=0x300)

    origin_port, app_port = free_port(), free_port()
    os.environ.update({
        "JELLYBALL_DATA_DIR": str(work / "data"),
        "ALLOW_PRIVATE_UPSTREAMS": "1",
        "PORT": str(app_port),
        "FFMPEG_PATH": ffmpeg,
        "MULTIVIEW_HWACCEL": args.hwaccel,
        "MULTIVIEW_SEGMENT_SECONDS": "2",
        "SESSION_IDLE_SECONDS": "30",
        "MULTIVIEW_FFMPEG_LOGLEVEL": os.environ.get("MULTIVIEW_FFMPEG_LOGLEVEL", "info"),
    })
    import httpx
    import main as jellyball

    serve(build_origin(work / "origin"), origin_port)
    serve(jellyball.app, app_port)
    base = f"http://127.0.0.1:{origin_port}"
    app_base = f"http://127.0.0.1:{app_port}"

    jellyball.stream_state["ta"] = member_state(jellyball, "TeamA", f"{base}/a/live.m3u8")
    jellyball.stream_state["tb"] = member_state(jellyball, "TeamB", f"{base}/b/live.m3u8")
    jellyball.stream_state["mv"] = {
        "name": "E2E Grid", "query": "", "type": "multiview",
        "candidates": [{"synthetic": True}], "active_index": 0, "is_healthy": False,
        "logo_url": "", "start_time": "", "stop_time": "", "category": "multiview", "source_id": "",
        "content_type": "multiview", "search_terms": [], "always_live": True, "catalog_key": "",
        "tvg_id": "", "group_title": "Multi-View", "layout": "side_by_side_2",
        "member_team_ids": ["ta", "tb"], "active_audio_team_id": "ta",
        **jellyball._scrape_lifecycle_defaults(),
    }

    m3u = httpx.get(f"{app_base}/playlist.m3u", timeout=10).text
    audio_entries = [line for line in m3u.splitlines() if "/multiview/mv/audio-" in line]
    print(f"per-audio M3U entries : {audio_entries}")

    channel_url = f"{app_base}/stream/mv.m3u8"
    output = work / "out.ts"
    log_path = work / "ffmpeg.log"
    print(f"playing {channel_url} for {args.seconds}s")
    results = {}
    with open(log_path, "w", encoding="utf-8") as log:
        player = subprocess.Popen([
            ffmpeg, "-hide_banner", "-loglevel", "info", "-y", "-i", channel_url,
            "-map", "0:v:0", "-map", "0:a:0", "-c:v", "libx264", "-preset", "ultrafast",
            "-c:a", "aac", "-t", str(args.seconds), "-f", "mpegts", str(output),
        ], stdout=log, stderr=subprocess.STDOUT)

        started = time.monotonic()
        switched = False
        pid_before = pid_after = None
        while player.poll() is None and time.monotonic() - started < args.seconds + 90:
            entry = jellyball._MULTIVIEW_PROCESSES.get("mv")
            if entry and entry.get("ready") and not switched and time.monotonic() - started > args.seconds / 2:
                pid_before = entry["process"].pid
                results["encoder"] = entry.get("encoder")
                run_dir = entry["output_dir"]
                results["outputs_identical"] = segment_listing(run_dir / "a0" / "index.m3u8") == segment_listing(run_dir / "a1" / "index.m3u8")
                resp = httpx.get(f"{app_base}/multiview/mv/audio-1.m3u8", timeout=30)
                results["audio_channel_status"] = resp.status_code
                results["audio_channel_segments"] = resp.text.count("#EXTINF")
                resp = httpx.post(f"{app_base}/multiview/mv/set-audio", data={"active_audio_team_id": "tb"},
                                  timeout=10, follow_redirects=False)
                results["set_audio_status"] = resp.status_code
                switched = True
                print(f"[{time.monotonic() - started:5.1f}s] switched main audio to TeamB (ffmpeg pid {pid_before})")
            if int(time.monotonic() - started) % 5 == 0:
                parts = []
                for sid in ("ta", "tb", "mv"):
                    sess = jellyball.SESSIONS.peek(sid)
                    if sess and sess.window:
                        parts.append(f"{sid}:seq={sess.window[-1].seq} age={time.monotonic() - sess.last_new_segment_at:.1f}s n={sess.stats['segments']}")
                mv_entry = jellyball._MULTIVIEW_PROCESSES.get("mv")
                if mv_entry and mv_entry.get("ready"):
                    ages = [jellyball._newest_segment_age(mv_entry["output_dir"] / f"a{i}") for i in range(2)]
                    count = len(list((mv_entry["output_dir"] / "a0").glob("seg_*.ts")))
                    parts.append(f"mvdisk newest_age={ages} files={count}")
                print(f"[{time.monotonic() - started:5.1f}s] " + " | ".join(parts), flush=True)
            time.sleep(1)
        if player.poll() is None:
            player.kill()
        entry = jellyball._MULTIVIEW_PROCESSES.get("mv")
        pid_after = entry["process"].pid if entry else None

    mv_log = list(entry.get("log_lines", [])) if entry else []
    print("multiview ffmpeg log tail:")
    for line in mv_log[-25:]:
        print("  " + line)
    probe = subprocess.run(
        [ffprobe, "-v", "error", "-show_entries", "format=duration", "-of", "default=nk=1:nw=1", str(output)],
        capture_output=True, text=True,
    )
    duration = float(probe.stdout.strip() or 0)
    session = jellyball.SESSIONS.peek("mv")
    snapshot = session.snapshot() if session else {}
    failure = jellyball._MULTIVIEW_FAILURES.get("mv")

    print(f"ffmpeg exit code      : {player.returncode}")
    print(f"output duration       : {duration:.1f}s of {args.seconds}s")
    print(f"multiview encoder     : {results.get('encoder')}")
    print(f"a0/a1 identical lists : {results.get('outputs_identical')}")
    print(f"audio channel         : HTTP {results.get('audio_channel_status')} with {results.get('audio_channel_segments')} segments")
    print(f"set-audio             : HTTP {results.get('set_audio_status')}; ffmpeg pid {pid_before} -> {pid_after}")
    print(f"main session          : source={snapshot.get('source')} switches={snapshot.get('source_switches')} state={snapshot.get('state')}")
    print(f"multiview failure     : {failure}")
    ok = (
        duration >= args.seconds - 8
        and results.get("outputs_identical")
        and results.get("audio_channel_status") == 200
        and results.get("set_audio_status") == 303
        and pid_before is not None and pid_before == pid_after
        and "TeamB" in str(snapshot.get("source"))
    )
    print("RESULT:", "PASS" if ok else "FAIL")
    if not ok or args.keep:
        print(f"kept {work} (ffmpeg log: {log_path})")
    else:
        shutil.rmtree(work, ignore_errors=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
