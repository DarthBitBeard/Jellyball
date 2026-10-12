"""Tests for the 3.0 remux ingest (RemuxSpec / RemuxSession)."""

import asyncio
import os
import shutil
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import remux_ingest
from remux_ingest import (
    RemuxSession,
    RemuxSpec,
    build_remux_args,
    headers_to_ffmpeg_args,
)
import ffmpeg_proc


class HeadersToArgsTests(unittest.TestCase):
    def test_referer_and_user_agent(self):
        args = headers_to_ffmpeg_args({"Referer": "https://example.test/watch", "User-Agent": "TestAgent/1.0"})
        self.assertIn("-headers", args)
        idx = args.index("-headers")
        self.assertIn("Referer: https://example.test/watch", args[idx + 1])
        self.assertIn("User-Agent: TestAgent/1.0", args[idx + 1])

    def test_origin_header(self):
        args = headers_to_ffmpeg_args({"Origin": "https://example.test"})
        self.assertIn("-headers", args)
        idx = args.index("-headers")
        self.assertIn("Origin: https://example.test", args[idx + 1])

    def test_cookie_header(self):
        args = headers_to_ffmpeg_args({"Cookie": "session=abc123"})
        self.assertIn("-cookies", args)
        idx = args.index("-cookies")
        self.assertEqual(args[idx + 1], "session=abc123")

    def test_crlf_injection_stripped(self):
        args = headers_to_ffmpeg_args({"X-Evil": "a\r\nInjected: yes"})
        idx = args.index("-headers")
        # The injected CRLF is stripped: the value cannot smuggle an extra
        # header line into the -headers blob.
        self.assertEqual(args[idx + 1].count("\r\n"), 1)

    def test_empty_headers(self):
        self.assertEqual(headers_to_ffmpeg_args({}), [])


class BuildArgsTests(unittest.TestCase):
    def _spec(self, **kw):
        base = dict(
            url="https://example.test/live/index.m3u8",
            audio_url="",
            referer="https://example.test/watch",
            origin="",
            cookies="",
            user_agent="TestAgent/1.0",
        )
        base.update(kw)
        return RemuxSpec(**base)

    def test_copy_by_default(self):
        args = build_remux_args(self._spec(), "/tmp/out", transcode_audio=False)
        self.assertIn("-c", args)
        self.assertEqual(args[args.index("-c") + 1], "copy")
        self.assertIn("-f", args)
        self.assertEqual(args[args.index("-f") + 1], "hls")
        self.assertIn("https://example.test/live/index.m3u8", args)

    def test_transcode_audio_fallback(self):
        args = build_remux_args(self._spec(), "/tmp/out", transcode_audio=True)
        self.assertIn("-c:a", args)
        self.assertEqual(args[args.index("-c:a") + 1], "aac")
        self.assertIn("-c:v", args)
        self.assertEqual(args[args.index("-c:v") + 1], "copy")

    def test_demuxed_audio_maps_both_inputs(self):
        spec = self._spec(audio_url="https://example.test/live/audio.m3u8")
        args = build_remux_args(spec, "/tmp/out", transcode_audio=False)
        self.assertEqual(args.count("-i"), 2)
        self.assertIn("https://example.test/live/audio.m3u8", args)
        self.assertIn("-map", args)
        self.assertIn("0:v:0", args)
        self.assertIn("1:a:0", args)

    def test_scratch_dir_used(self):
        # Path() normalizes separators, so assert against the platform form
        # (Windows turns "/tmp/..." into "\\tmp\\...").
        scratch = Path("/tmp/my-scratch")
        args = build_remux_args(self._spec(), scratch, transcode_audio=False)
        prefix = str(scratch)
        self.assertTrue(any(a.startswith(prefix) for a in args), args)


class HealthyStateMachineTests(unittest.TestCase):
    def test_healthy_requires_process_and_advancing_playlist(self):
        session = RemuxSession()
        # Not started: unhealthy.
        self.assertFalse(session.healthy())

    def test_stop_is_safe_when_never_started(self):
        session = RemuxSession()
        asyncio.run(session.stop())  # should not raise


@unittest.skipUnless(
    ffmpeg_proc.FFMPEG_AVAILABLE or shutil.which("ffmpeg"),
    "ffmpeg not available",
)
class RemuxIntegrationTests(unittest.TestCase):
    """End-to-end: live fMP4 HLS source remuxed to advancing local TS playlist.

    Uses a live (no EXT-X-ENDLIST) source so remux ffmpeg stays up the way it
    does for real provider streams; a short VOD fixture exits immediately and
    cannot satisfy RemuxSession.healthy().
    """

    @classmethod
    def setUpClass(cls):
        # Unit suites do not run the app startup probe; mark ffmpeg usable when
        # the binary is on PATH so this integration test actually runs.
        if not ffmpeg_proc.FFMPEG_AVAILABLE and shutil.which("ffmpeg"):
            ffmpeg_proc.FFMPEG_AVAILABLE = True

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="remux-test-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self._source_proc = None

    def tearDown(self):
        proc = self._source_proc
        self._source_proc = None
        if proc is not None and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

    def _start_live_fmp4_source(self) -> Path:
        """Spawn ffmpeg writing a live fMP4 HLS playlist under self.tmp/src."""
        src_dir = self.tmp / "src"
        src_dir.mkdir()
        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-re",
            "-f", "lavfi", "-i", "testsrc=size=320x240:rate=10",
            "-f", "lavfi", "-i", "sine=frequency=440",
            "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
            "-c:a", "aac",
            "-f", "hls", "-hls_time", "2", "-hls_list_size", "6",
            "-hls_flags", "delete_segments+append_list",
            "-hls_segment_type", "fmp4",
            str(src_dir / "index.m3u8"),
        ]
        self._source_proc = subprocess.Popen(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
        )
        playlist = src_dir / "index.m3u8"
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            if self._source_proc.poll() is not None:
                err = (self._source_proc.stderr.read() or b"").decode("utf-8", "replace")
                self.fail(f"live fMP4 source ffmpeg exited early: {err[-500:]}")
            if playlist.exists() and "#EXTINF" in playlist.read_text(encoding="utf-8", errors="replace"):
                return playlist
            time.sleep(0.25)
        self.fail("live fMP4 source produced no playlist segments")

    def test_remux_produces_advancing_ts_playlist(self):
        async def run():
            src = self._start_live_fmp4_source()
            session = RemuxSession()
            # file:// is not supported by ffmpeg HLS; serve over local HTTP.
            import http.server
            import threading
            import functools

            handler = functools.partial(
                http.server.SimpleHTTPRequestHandler, directory=str(src.parent)
            )
            httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
            port = httpd.server_address[1]
            thread = threading.Thread(target=httpd.serve_forever, daemon=True)
            thread.start()
            # Isolate remux scratch so this test cannot pollute /tmp/jellyball-remux
            # (which is registered as a sweep root and would break SweepTests).
            remux_root = self.tmp / "remux-out"
            remux_root.mkdir()
            try:
                with patch.object(remux_ingest, "REMUX_OUTPUT_ROOT", remux_root):
                    spec = RemuxSpec(
                        url=f"http://127.0.0.1:{port}/index.m3u8",
                        audio_url="",
                        referer="",
                        origin="",
                        cookies="",
                        user_agent="test",
                    )
                    local_path = await session.start(spec)
                    self.assertTrue(Path(local_path).exists())
                    self.assertTrue(str(local_path).startswith(str(remux_root)))
                    for _ in range(30):
                        await asyncio.sleep(1)
                        if session.healthy():
                            break
                    self.assertTrue(session.healthy())
                    from ts_normalize import find_ts_start
                    playlist_dir = Path(local_path).parent
                    # remux writes seg%05d.ts (e.g. seg00000.ts), not seg-*.ts.
                    segments = sorted(playlist_dir.glob("seg*.ts"))
                    self.assertTrue(segments, "no segments produced")
                    data = segments[0].read_bytes()
                    self.assertGreaterEqual(find_ts_start(data), 0)
                    await session.stop()
                    self.assertFalse(session.healthy())
            finally:
                httpd.shutdown()
                thread.join(timeout=5)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
