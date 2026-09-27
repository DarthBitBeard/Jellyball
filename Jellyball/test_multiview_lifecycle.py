"""Multi-View / placeholder process lifecycle: watchdog, restarts and backoff,
concurrency slots, refusals, NVENC fallback, member warm-up, source keys, the
output-dir sweep, manual stop, channel removal and Windows process control."""
import asyncio
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from collections import deque
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

import main
import ffmpeg_proc
import placeholder
import state
import config
import shutil
from hls_session import SourceSpec


PLACEHOLDER_SPEC = SourceSpec(key=("placeholder", 1), url="placeholder/index.m3u8", local=True, label="No Signal")

# What this dev PC's ffmpeg 9.0 prints for NVENC + -hwaccel cuda with no NVIDIA driver.
LOG_NO_NVIDIA_DRIVER = [
    "[CUDA @ 000001a91a767780] Cannot load nvcuda.dll",
    "[CUDA @ 000001a91a767780] Could not dynamically load CUDA",
    "Device creation failed: -1.",
    "[vist#0:0/h264 @ 000001a918220740] [dec:h264 @ 000001a91a796440] No device available for decoder: "
    "device type cuda needed for codec h264.",
    "[vist#0:0/h264 @ 000001a918220740] [dec:h264 @ 000001a91a796440] Hardware device setup failed for decoder: "
    "Operation not permitted",
    "Error opening output file -.",
]
# NVDEC falling back to software for one stream, plus unrelated noise: NVENC is fine.
LOG_NVDEC_FALLBACK_ONLY = [
    "[h264 @ 0000020f1a2b3c40] Failed setup for format cuda: hwaccel initialisation returned error.",
    "Stream #0:0 -> #0:0 (h264 (native) -> h264 (h264_nvenc))",
    "      encoder         : Lavc61.3.100 h264_nvenc",
    "[http @ 0000020f1a2b3c41] HTTP error 404 Not Found",
    "Error opening input file http://127.0.0.1:8000/stream/ta.m3u8.",
]
LOG_NVENC_SESSION_LIMIT = [
    "[h264_nvenc @ 0000020f1a2b3c42] OpenEncodeSessionEx failed: incompatible client key (21): (no details)",
    "[vost#0:0/h264_nvenc @ 0000020f1a2b3c43] [enc:h264_nvenc @ 0000020f1a2b3c44] Error while opening encoder - "
    "maybe incorrect parameters such as bit_rate, rate, width or height.",
]
LOG_NVENC_DRIVER_TOO_OLD = [
    "[h264_nvenc @ 0x55d1] Driver does not support the required nvenc API version. Required: 13.0 Found: 12.1",
]
LOG_QSV_FAILURE = [
    "[h264_qsv @ 0x55d2] Error initializing an internal MFX session: unsupported (-3)",
]


class FakeSession:
    def __init__(self, ready=True, has_audio=True, signature=(0x1B, 0x0F), running=False, flowing=False, source=None):
        self.ready = ready
        self.has_audio = has_audio if ready else None
        self.codec_signature = signature if ready else None
        self.window = [object()] if ready else []
        self.running = running
        self.flowing = flowing
        self.source = source
        self.touched = 0

    def touch(self):
        self.touched += 1

    @property
    def is_running(self):
        return self.running

    def is_flowing(self):
        return self.flowing

    async def wait_ready(self, timeout):
        if self.ready:
            return True
        await asyncio.sleep(timeout)
        return False


class FakeRegistry:
    def __init__(self, sessions=None):
        self.sessions = dict(sessions or {})
        self.poked = []
        self.closed = []

    def get(self, session_id):
        return self.sessions.setdefault(session_id, FakeSession())

    def peek(self, session_id):
        return self.sessions.get(session_id)

    def poke(self, session_id):
        self.poked.append(session_id)

    async def close(self, session_id):
        self.closed.append(session_id)
        self.sessions.pop(session_id, None)


class FakeProcess:
    def __init__(self, returncode=None, pid=424242):
        self.returncode = returncode
        self.pid = pid
        self.kill = MagicMock(side_effect=self._die)
        self.terminate = MagicMock(side_effect=self._die)

    def _die(self):
        self.returncode = 1

    async def wait(self):
        return self.returncode


def multiview_data(members=("ta", "tb"), layout="side_by_side_2"):
    return {
        "name": "MV", "type": "multiview", "layout": layout,
        "member_team_ids": list(members), "active_audio_team_id": members[0],
    }


def live_entry(run_id=1, output_dir=Path("/fake/run1"), audio_count=2, **extra):
    entry = {
        "process": FakeProcess(), "run_id": run_id, "output_dir": output_dir, "audio_count": audio_count,
        "audio_types": [0x0F] * audio_count, "standins": [], "ready": True, "exited": False,
        "started_at": time.monotonic() - 1000, "last_access": time.monotonic(), "log_lines": deque(),
    }
    entry.update(extra)
    return entry


class MultiviewStateTestCase(unittest.TestCase):
    """Snapshot/restore the module-level state these tests touch."""

    # (owning module, global name): each global lives in the module whose code uses it.
    DICTS = tuple((main, name) for name in (
        "_MULTIVIEW_PROCESSES", "_MULTIVIEW_FAILURES", "_MULTIVIEW_HOLDS", "_MULTIVIEW_REFUSALS",
        "_MULTIVIEW_RESTARTS", "_MULTIVIEW_MANUAL_STOPS", "_MULTIVIEW_START_TASKS", "_MULTIVIEW_LAST_VIEWER",
    )) + ((main, "_LAST_PLAYBACK_EVENT"),)
    SETS = ((main, "_MULTIVIEW_SLOT_RESERVATIONS"), (main, "_MULTIVIEW_PENDING_RUN_DIRS"),
            (placeholder, "_PLACEHOLDER_PENDING_DIRS"))
    SCALARS = (
        (main, "_HW_ENCODER_FAILED_AT"), (main, "_CUDA_DECODE_FAILED_AT"), (main, "_MULTIVIEW_RUN_COUNTER"),
        (placeholder, "_PLACEHOLDER_STATE"), (placeholder, "_PLACEHOLDER_FAILURE"),
        (placeholder, "_PLACEHOLDER_DRAWTEXT_OK"), (placeholder, "_PLACEHOLDER_RUN_COUNTER"),
        (ffmpeg_proc, "FFMPEG_AVAILABLE"), (main, "_PLACEHOLDER_START_TASK"),
    )

    def setUp(self):
        self._saved = {}
        for module, name in self.DICTS:
            self._saved[name] = dict(getattr(module, name))
            getattr(module, name).clear()
        for module, name in self.SETS:
            self._saved[name] = set(getattr(module, name))
            getattr(module, name).clear()
        for module, name in self.SCALARS:
            self._saved[name] = getattr(module, name)
        main._HW_ENCODER_FAILED_AT = None
        main._CUDA_DECODE_FAILED_AT = None
        placeholder._PLACEHOLDER_STATE = None
        placeholder._PLACEHOLDER_FAILURE = None
        placeholder._PLACEHOLDER_DRAWTEXT_OK = True
        main._PLACEHOLDER_START_TASK = None
        state_patch = patch.dict(state.stream_state, {}, clear=True)
        state_patch.start()
        self.addCleanup(state_patch.stop)

    def tearDown(self):
        for module, name in self.DICTS:
            getattr(module, name).clear()
            getattr(module, name).update(self._saved[name])
        for module, name in self.SETS:
            getattr(module, name).clear()
            getattr(module, name).update(self._saved[name])
        for module, name in self.SCALARS:
            setattr(module, name, self._saved[name])


class NewestSegmentAgeTests(unittest.TestCase):
    def test_segment_deleted_between_glob_and_stat_is_skipped(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            live = out / "seg_000002.ts"
            live.write_bytes(b"x")
            vanished = out / "seg_000001.ts"  # listed, then removed by delete_segments
            with patch.object(Path, "glob", return_value=[vanished, live]):
                age = main._newest_segment_age(out)
            self.assertIsNotNone(age)
            self.assertLess(age, 5.0)

    def test_no_segments_falls_back_to_playlist_age(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            playlist = out / "index.m3u8"
            playlist.write_text("#EXTM3U\n", encoding="utf-8")
            self.assertLess(main._newest_segment_age(out), 5.0)  # fresh playlist: not stalled
            old = time.time() - 120
            os.utime(playlist, (old, old))
            self.assertGreater(main._newest_segment_age(out), 100.0)  # stale playlist: stalled

    def test_no_segments_and_no_playlist_is_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(main._newest_segment_age(Path(tmp)))


class WatchdogTests(MultiviewStateTestCase):
    def test_rotating_output_is_not_restarted_but_stale_one_is(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            (run_dir / "a0").mkdir()
            playlist = run_dir / "a0" / "index.m3u8"
            playlist.write_text("#EXTM3U\n", encoding="utf-8")
            main._MULTIVIEW_PROCESSES["mv"] = live_entry(run_id=2, output_dir=run_dir, audio_count=1)
            with patch.object(main, "_restart_multiview", AsyncMock(return_value=True)) as restart:
                asyncio.run(main._multiview_watchdog_pass(12.0))
                restart.assert_not_awaited()
                old = time.time() - 60
                os.utime(playlist, (old, old))
                asyncio.run(main._multiview_watchdog_pass(12.0))
            restart.assert_awaited_once_with("mv", "output stalled audio=[0]", run_id=2)

    def test_dead_watched_run_is_restarted_with_its_run_id(self):
        entry = live_entry(run_id=9)
        entry["exited"] = True
        main._MULTIVIEW_PROCESSES["mv"] = entry
        with patch.object(main, "_restart_multiview", AsyncMock(return_value=True)) as restart:
            asyncio.run(main._multiview_watchdog_pass(12.0))
        self.assertEqual(restart.await_args.kwargs, {"run_id": 9})

    def test_standin_member_swapped_in_once_live(self):
        state.stream_state["tb"] = {"name": "TB"}
        entry = live_entry(run_id=4, standins=["tb"])
        registry = FakeRegistry({"tb": FakeSession(flowing=True, source=SourceSpec(key=("prov", "cdn", "/x"), url="u"))})
        with patch.object(main, "SESSIONS", registry), \
                patch.object(main, "_restart_multiview", AsyncMock(return_value=True)) as restart:
            asyncio.run(main._swap_in_ready_members("mv", entry))
            restart.assert_awaited_once()
            self.assertEqual(restart.await_args.kwargs, {"run_id": 4})

            # A member whose own session is only showing the placeholder isn't "live".
            restart.reset_mock()
            registry.sessions["tb"].source = SourceSpec(key=("placeholder", 3), url="u")
            asyncio.run(main._swap_in_ready_members("mv", entry))
            restart.assert_not_awaited()

            # Not when that restart would be backed off.
            registry.sessions["tb"].source = SourceSpec(key=("prov", "cdn", "/x"), url="u")
            now = time.monotonic()
            main._MULTIVIEW_RESTARTS["mv"] = {"times": deque([now] * main.MULTIVIEW_RESTART_BURST), "last_alert": None}
            asyncio.run(main._swap_in_ready_members("mv", entry))
            restart.assert_not_awaited()
        self.assertGreater(registry.sessions["tb"].touched, 0)  # kept warm meanwhile


class RestartTests(MultiviewStateTestCase):
    def test_restart_for_a_replaced_run_is_ignored(self):
        entry = live_entry(run_id=7)
        main._MULTIVIEW_PROCESSES["mv"] = entry
        with patch.object(main, "_stop_multiview_process", AsyncMock()) as stop, \
                patch.object(main, "_request_multiview_start") as request:
            self.assertFalse(asyncio.run(main._restart_multiview("mv", "output stalled", run_id=6)))
        stop.assert_not_awaited()
        request.assert_not_called()
        self.assertFalse(entry.get("restarting"))
        self.assertNotIn("mv", main._MULTIVIEW_RESTARTS)

    def test_restart_of_current_run_stops_exactly_that_run(self):
        main._MULTIVIEW_PROCESSES["mv"] = live_entry(run_id=7)
        with patch.object(main, "_stop_multiview_process", AsyncMock(return_value=True)) as stop, \
                patch.object(main, "_request_multiview_start") as request:
            self.assertTrue(asyncio.run(main._restart_multiview("mv", "output stalled", run_id=7)))
        stop.assert_awaited_once_with("mv", run_id=7)
        request.assert_called_once_with("mv")

    def test_stop_process_refuses_other_run(self):
        entry = live_entry(run_id=3)
        main._MULTIVIEW_PROCESSES["mv"] = entry
        self.assertFalse(asyncio.run(main._stop_multiview_process("mv", run_id=2)))
        self.assertIs(main._MULTIVIEW_PROCESSES["mv"], entry)
        entry["process"].kill.assert_not_called()

    def test_restart_backoff_builds_in_rolling_window_and_alerts_once(self):
        with patch.object(main, "MULTIVIEW_RESTART_BURST", 2), \
                patch.object(ffmpeg_proc, "MULTIVIEW_BACKOFF_BASE_SECONDS", 15.0), \
                patch.object(ffmpeg_proc, "MULTIVIEW_BACKOFF_MAX_SECONDS", 300.0), \
                patch.object(main, "send_alert", MagicMock(return_value=None)) as alert, \
                patch.object(main, "_spawn_background_task") as spawn:
            delays = [main._note_multiview_restart("mv", "output stalled")[1] for _ in range(7)]
            # A successful spawn clears the failure record, not the restart history.
            main._clear_multiview_failure("mv")
            self.assertEqual(main._recent_multiview_restarts("mv"), 7)
        self.assertEqual(delays, [0.0, 0.0, 15.0, 30.0, 60.0, 120.0, 240.0])
        self.assertEqual(alert.call_count, 1)
        self.assertEqual(spawn.call_count, 1)
        self.assertGreater(main._multiview_cooldown_remaining("mv"), 200.0)
        self.assertEqual(main._MULTIVIEW_HOLDS["mv"]["kind"], "restart")

    def test_restarts_age_out_of_the_window(self):
        old = time.monotonic() - main.MULTIVIEW_RESTART_WINDOW_SECONDS - 5
        main._MULTIVIEW_RESTARTS["mv"] = {"times": deque([old] * 5), "last_alert": None}
        with patch.object(main, "_spawn_background_task") as spawn:
            count, delay = main._note_multiview_restart("mv", "output stalled")
        self.assertEqual((count, delay), (1, 0.0))
        spawn.assert_not_called()

    def test_held_restart_does_not_start_until_hold_expires(self):
        state.stream_state["mv"] = multiview_data()
        main._set_multiview_hold("mv", 30.0, "restart", "stalled")
        with patch.object(main, "_spawn_background_task") as spawn:
            main._request_multiview_start("mv")
        spawn.assert_not_called()


class SlotReservationTests(MultiviewStateTestCase):
    def test_simultaneous_starts_cannot_exceed_cap(self):
        ffmpeg_proc.FFMPEG_AVAILABLE = True
        data1, data2 = multiview_data(), multiview_data()
        state.stream_state.update({"mv1": data1, "mv2": data2})
        started = []

        async def slow_run(channel_id, data):
            started.append(channel_id)
            await asyncio.sleep(0.05)  # the member warm-up

        async def scenario():
            await asyncio.gather(main._spawn_multiview("mv1", data1), main._spawn_multiview("mv2", data2))

        with patch.object(main, "MAX_CONCURRENT_MULTIVIEW", 1), \
                patch.object(main, "_start_multiview_run", slow_run), \
                self.assertLogs(config.LOGGER, "WARNING"):
            asyncio.run(scenario())
        self.assertEqual(started, ["mv1"])
        self.assertEqual(main._MULTIVIEW_HOLDS["mv2"]["kind"], "refused")
        self.assertEqual(main._MULTIVIEW_SLOT_RESERVATIONS, set())

    def test_reservation_released_when_spawn_fails(self):
        ffmpeg_proc.FFMPEG_AVAILABLE = True
        data = multiview_data()
        state.stream_state["mv"] = data

        async def failing_run(channel_id, data):
            self.assertEqual(main._running_multiview_count(), 1)  # reserved during the spawn
            raise RuntimeError("boom")

        with patch.object(main, "_start_multiview_run", failing_run):
            with self.assertRaises(RuntimeError):
                asyncio.run(main._spawn_multiview("mv", data))
        self.assertEqual(main._running_multiview_count(), 0)


class RefusalTests(MultiviewStateTestCase):
    def test_refusal_logs_once_per_hold_and_view_gets_placeholder(self):
        ffmpeg_proc.FFMPEG_AVAILABLE = True
        state.stream_state["mv"] = multiview_data()
        main._MULTIVIEW_PROCESSES["other"] = live_entry()

        async def scenario():
            results = []
            for _ in range(6):
                results.append(main._resolve_multiview_view_source("mv", None))
                await asyncio.sleep(0.01)
            return results

        with patch.object(main, "MAX_CONCURRENT_MULTIVIEW", 1), \
                patch.object(main, "_placeholder_source", return_value=PLACEHOLDER_SPEC), \
                self.assertLogs(config.LOGGER, "WARNING") as logs:
            results = asyncio.run(scenario())
        refusals = [line for line in logs.output if "Refusing to start multiview" in line]
        self.assertEqual(len(refusals), 1)
        self.assertIs(results[-1], PLACEHOLDER_SPEC)
        self.assertGreater(main._multiview_cooldown_remaining("mv"), 0.0)

    def test_refusal_hold_grows_and_caps(self):
        with patch.object(config.LOGGER, "warning"):
            delays = [main._record_multiview_refusal("mv", "cap") for _ in range(6)]
        self.assertEqual(delays, [5.0, 10.0, 20.0, 40.0, 60.0, 60.0])

    def test_no_ffmpeg_is_a_refusal_not_a_failure(self):
        ffmpeg_proc.FFMPEG_AVAILABLE = False
        data = multiview_data()
        state.stream_state["mv"] = data
        with patch.object(config.LOGGER, "warning"):
            asyncio.run(main._spawn_multiview("mv", data))
        self.assertNotIn("mv", main._MULTIVIEW_FAILURES)
        self.assertEqual(main._MULTIVIEW_HOLDS["mv"]["kind"], "refused")


class HwEncoderFailureTests(MultiviewStateTestCase):
    def test_nvenc_markers(self):
        self.assertTrue(main._looks_like_hw_encoder_failure(LOG_NO_NVIDIA_DRIVER, "nvenc"))
        self.assertTrue(main._looks_like_hw_encoder_failure(LOG_NVENC_SESSION_LIMIT, "nvenc"))
        self.assertTrue(main._looks_like_hw_encoder_failure(LOG_NVENC_DRIVER_TOO_OLD, "nvenc"))
        # NVDEC's per-stream fallback, stream mapping and input errors are not NVENC failures.
        self.assertFalse(main._looks_like_hw_encoder_failure(LOG_NVDEC_FALLBACK_ONLY, "nvenc"))
        self.assertFalse(main._looks_like_hw_encoder_failure(
            ["[vost#0:0/libx264 @ 0x1] [enc:libx264 @ 0x2] Error while opening encoder"], "nvenc"))
        self.assertFalse(main._looks_like_hw_encoder_failure(LOG_QSV_FAILURE, "nvenc"))

    def test_qsv_markers(self):
        self.assertTrue(main._looks_like_hw_encoder_failure(LOG_QSV_FAILURE, "qsv"))
        self.assertFalse(main._looks_like_hw_encoder_failure(LOG_NVENC_SESSION_LIMIT, "qsv"))
        self.assertFalse(main._looks_like_hw_encoder_failure(LOG_NO_NVIDIA_DRIVER, "none"))

    def test_cuda_init_markers(self):
        self.assertTrue(main._looks_like_cuda_init_failure(LOG_NO_NVIDIA_DRIVER))
        self.assertFalse(main._looks_like_cuda_init_failure(LOG_NVENC_SESSION_LIMIT))
        self.assertFalse(main._looks_like_cuda_init_failure(LOG_NVDEC_FALLBACK_ONLY))

    def _run_spawn(self, first_failure_log):
        data = multiview_data()
        state.stream_state["mv"] = data
        inputs = [main._MultiviewInput(t, t, True, 0x0F, False) for t in data["member_team_ids"]]
        attempts = []

        async def fake_launch(channel_id, data, run_id, inputs, encoder, hw_decode):
            attempts.append((encoder, hw_decode))
            return ("failed", first_failure_log) if encoder == "nvenc" else ("ready", [])

        with patch.object(main, "MULTIVIEW_HWACCEL", "nvenc"), \
                patch.object(main, "_warm_multiview_members", AsyncMock(return_value=inputs)), \
                patch.object(main, "_launch_multiview_run", fake_launch), \
                patch.object(config.LOGGER, "warning"):
            asyncio.run(main._start_multiview_run("mv", data))
            plan = main._multiview_encoder_plan()
            later = main._multiview_encoder_plan(now=time.monotonic() + main.NVENC_FALLBACK_SECONDS + 1)
        return attempts, plan, later

    def test_nvenc_failure_retries_same_spawn_with_libx264_without_broken_cuda(self):
        attempts, plan, later = self._run_spawn(LOG_NO_NVIDIA_DRIVER)
        self.assertEqual(attempts, [("nvenc", True), ("none", False)])
        self.assertEqual(plan, ("none", False))
        self.assertEqual(later, ("nvenc", True))  # GPU re-tested after the fallback window
        self.assertNotIn("mv", main._MULTIVIEW_FAILURES)

    def test_nvenc_only_failure_keeps_cuda_decoding(self):
        attempts, plan, _ = self._run_spawn(LOG_NVENC_SESSION_LIMIT)
        self.assertEqual(attempts, [("nvenc", True), ("none", True)])
        self.assertEqual(plan, ("none", True))

    def test_fallback_window_is_configurable(self):
        main._HW_ENCODER_FAILED_AT = time.monotonic()
        with patch.object(main, "MULTIVIEW_HWACCEL", "nvenc"), patch.object(main, "NVENC_FALLBACK_SECONDS", 30.0):
            self.assertEqual(main._multiview_encoder_plan()[0], "none")
            self.assertEqual(main._multiview_encoder_plan(now=time.monotonic() + 31)[0], "nvenc")


class MemberInputTests(MultiviewStateTestCase):
    def test_unknown_audio_gets_silent_track_and_inputs_probe_briefly(self):
        data = multiview_data()
        state.stream_state.update({"ta": {"name": "TA"}, "tb": {"name": "TB"}})
        with patch.object(config, "PORT", 8000), patch.dict(os.environ, {"JELLYBALL_HOST": ""}):
            args = main._build_multiview_ffmpeg_args(
                "mv", data, Path("/x"), [True, None], "none", input_ids=["ta", state.PLACEHOLDER_SESSION_ID],
            )
        maps = [args[i + 1] for i, a in enumerate(args) if a == "-map"]
        self.assertEqual(maps, ["[vout]", "0:a:0", "[a1]"])
        self.assertIn("anullsrc=r=48000:cl=stereo[a1]", args[args.index("-filter_complex") + 1])
        self.assertEqual(args[args.index("-c:a:1") + 1], "aac")
        self.assertEqual([args[i + 1] for i, a in enumerate(args) if a == "-probesize"], ["1000000"] * 2)
        self.assertEqual([args[i + 1] for i, a in enumerate(args) if a == "-analyzeduration"], ["1000000"] * 2)
        self.assertEqual([args[i + 1] for i, a in enumerate(args) if a == "-i"], [
            "http://127.0.0.1:8000/stream/ta.m3u8",
            "http://127.0.0.1:8000/stream/__placeholder__.m3u8",
        ])
        self.assertNotIn("-hwaccel", args)

    def test_cuda_decoding_independent_of_encoder(self):
        args = main._build_multiview_ffmpeg_args("mv", multiview_data(), Path("/x"), [True, True], "none", hw_decode=True)
        self.assertEqual(args.count("-hwaccel"), 2)
        self.assertEqual(args[args.index("-c:v") + 1], "libx264")
        args = main._build_multiview_ffmpeg_args("mv", multiview_data(), Path("/x"), [True, True], "nvenc", hw_decode=False)
        self.assertNotIn("-hwaccel", args)

    def test_warmup_replaces_unready_member_with_placeholder(self):
        state.stream_state.update({"ta": {"name": "TA"}, "tb": {"name": "TB"}})
        registry = FakeRegistry({
            "ta": FakeSession(ready=True, has_audio=None),              # ready, audio unknown
            "tb": FakeSession(ready=False),                             # never gets ready
            state.PLACEHOLDER_SESSION_ID: FakeSession(ready=True, has_audio=True, signature=(0x1B, 0x0F)),
        })
        with patch.object(main, "SESSIONS", registry), \
                patch.object(main, "MULTIVIEW_MEMBER_WARM_TIMEOUT", 0.2), \
                patch.object(main, "MULTIVIEW_STANDIN_WAIT_SECONDS", 0.1), \
                patch.object(config.LOGGER, "warning"):
            inputs = asyncio.run(main._warm_multiview_members(["ta", "tb", "gone"]))
        self.assertEqual(inputs, [
            main._MultiviewInput("ta", "ta", False, None, False),
            main._MultiviewInput("tb", state.PLACEHOLDER_SESSION_ID, True, 0x0F, True),
            main._MultiviewInput("gone", state.PLACEHOLDER_SESSION_ID, True, 0x0F, False),
        ])
        self.assertGreater(registry.sessions["tb"].touched, 0)


class SourceKeyTests(MultiviewStateTestCase):
    def test_view_key_changes_only_with_output_audio_codec(self):
        state.stream_state.update({
            "mv": multiview_data(("ta", "tb", "tc", "td"), "grid_2x2"),
            "ta": {"name": "TA"}, "tb": {"name": "TB"}, "tc": {"name": "TC"}, "td": {"name": "TD"},
        })
        main._MULTIVIEW_PROCESSES["mv"] = live_entry(run_id=3, audio_count=4, audio_types=[0x0F, 0x0F, 0x81, None])
        keys = [main._resolve_multiview_view_source("mv", index).key for index in range(4)]
        self.assertEqual(keys[0], ("multiview", "mv", 3, "audio:0x0f"))
        self.assertEqual(keys[0], keys[1])        # AAC -> AAC: seamless URL switch
        self.assertNotEqual(keys[0], keys[2])     # AAC -> AC-3: new epoch + discontinuity
        self.assertEqual(keys[3], ("multiview", "mv", 3, "audio:a3"))  # unknown codec: per output
        main._MULTIVIEW_PROCESSES["mv"]["run_id"] = 4
        self.assertNotEqual(main._resolve_multiview_view_source("mv", 0).key, keys[0])

    def test_view_failure_restarts_with_the_reported_run(self):
        state.stream_state["mv"] = multiview_data()
        main._MULTIVIEW_PROCESSES["mv"] = live_entry(run_id=5)
        with patch.object(main, "_restart_multiview", MagicMock(return_value="coro")) as restart, \
                patch.object(main, "_spawn_background_task") as spawn:
            main._on_multiview_view_failure("mv", ("multiview", "mv", 5, "audio:0x0f"), "playlist stale")
            main._on_multiview_view_failure("mv", ("multiview", "mv", 4, "audio:0x0f"), "playlist stale")
            main._on_multiview_view_failure("mv", ("placeholder", 2), "playlist stale")
        restart.assert_called_once_with("mv", "view reported playlist stale", run_id=5)
        spawn.assert_called_once()

    def test_placeholder_key_carries_run_id(self):
        def state(run_id):
            return {
                "process": FakeProcess(), "ready": True, "exited": False, "run_id": run_id,
                "output_dir": Path(f"/p/run{run_id}"), "last_access": 0.0,
            }

        placeholder._PLACEHOLDER_STATE = state(4)
        first = main._placeholder_source()
        placeholder._PLACEHOLDER_STATE = state(5)
        second = main._placeholder_source()
        self.assertEqual(first.key, ("placeholder", 4))
        self.assertNotEqual(first.key, second.key)
        self.assertEqual(first.key[:1], main.PLACEHOLDER_SOURCE_KEY)
        for key in (first.key, second.key, main.PLACEHOLDER_SOURCE_KEY, ["placeholder", 9]):
            self.assertTrue(main._is_placeholder_key(key))
        for key in (("multiview", "mv", 1, "audio:0x0f"), ("prov", "cdn", "/p"), (), None):
            self.assertFalse(main._is_placeholder_key(key))


class SweepTests(MultiviewStateTestCase):
    def test_sweep_keeps_live_and_pending_dirs_and_removes_the_rest(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, placeholder_root = Path(tmp) / "multiview", Path(tmp) / "placeholder"
            live_run, dead_run = root / "mv1" / "run3", root / "mv1" / "run2"
            pending_run, fresh_run = root / "mv2" / "run9", root / "mv3" / "run10"
            orphan_run, empty_channel = root / "mv_old" / "run1", root / "mv_gone"
            placeholder_live, placeholder_dead = placeholder_root / "run5", placeholder_root / "run4"
            for path in (live_run, dead_run, pending_run, fresh_run, orphan_run, empty_channel,
                         placeholder_live, placeholder_dead):
                path.mkdir(parents=True)
            for path in (live_run, dead_run, orphan_run, placeholder_live, placeholder_dead):
                (path / "seg_000001.ts").write_bytes(b"x" * 10)
            old = time.time() - 3600
            for path in (live_run, dead_run, pending_run, orphan_run, placeholder_live, placeholder_dead):
                os.utime(path, (old, old))
            main._MULTIVIEW_PROCESSES["mv1"] = live_entry(output_dir=live_run)
            main._MULTIVIEW_PENDING_RUN_DIRS.add(pending_run)
            placeholder._PLACEHOLDER_STATE = {"process": FakeProcess(), "output_dir": placeholder_live}
            with patch.object(main, "MULTIVIEW_OUTPUT_ROOT", root), \
                    patch.object(placeholder, "PLACEHOLDER_OUTPUT_DIR", placeholder_root):
                removed = asyncio.run(main._sweep_output_dirs(min_age=60.0))
            for path in (live_run, pending_run, fresh_run, placeholder_live):
                self.assertTrue(path.exists(), path)
            for path in (dead_run, orphan_run, orphan_run.parent, empty_channel, placeholder_dead):
                self.assertFalse(path.exists(), path)
            self.assertEqual(set(removed), {dead_run, orphan_run, orphan_run.parent, empty_channel, placeholder_dead})

    def test_rmtree_retries_while_files_are_locked(self):
        calls = []

        def flaky_rmtree(path):
            calls.append(path)
            if len(calls) < 3:
                raise PermissionError("file in use")

        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "run1"
            target.mkdir()
            with patch.object(shutil, "rmtree", flaky_rmtree):
                self.assertTrue(ffmpeg_proc._rmtree_with_retries(target, attempts=5, delay=0.01))
        self.assertEqual(len(calls), 3)


class ManualStopTests(MultiviewStateTestCase):
    def test_stop_cancels_spawn_and_suppresses_auto_start_until_new_viewer(self):
        ffmpeg_proc.FFMPEG_AVAILABLE = True
        state.stream_state["mv"] = multiview_data()
        registry = FakeRegistry()

        async def slow_warm(members):
            await asyncio.sleep(10)
            return []

        async def scenario():
            main._request_multiview_start("mv")
            task = main._MULTIVIEW_START_TASKS["mv"]
            await asyncio.sleep(0.01)  # the spawn is warming members up
            self.assertIn("mv", main._MULTIVIEW_SLOT_RESERVATIONS)
            await main._stop_multiview_manually("mv")
            self.assertTrue(task.cancelled())
            self.assertNotIn("mv", main._MULTIVIEW_SLOT_RESERVATIONS)
            main._request_multiview_start("mv")
            self.assertNotIn("mv", main._MULTIVIEW_START_TASKS)  # no auto-start while stopped
            self.assertIs(main._resolve_multiview_view_source("mv", None), PLACEHOLDER_SPEC)
            # Someone was already watching: their polls don't undo the Stop.
            registry.sessions["mv"] = FakeSession(running=True)
            main._touch_multiview_viewer("mv")
            self.assertIn("mv", main._MULTIVIEW_MANUAL_STOPS)
            # A fresh tune (no view session running) is an explicit play.
            registry.sessions.clear()
            main._touch_multiview_viewer("mv")
            self.assertNotIn("mv", main._MULTIVIEW_MANUAL_STOPS)

        with patch.object(main, "SESSIONS", registry), \
                patch.object(main, "_warm_multiview_members", slow_warm), \
                patch.object(main, "_placeholder_source", return_value=PLACEHOLDER_SPEC):
            asyncio.run(scenario())
        self.assertIn("mv", registry.poked)

    def test_spawn_gives_up_when_channel_removed_during_warmup(self):
        ffmpeg_proc.FFMPEG_AVAILABLE = True
        data = multiview_data()
        state.stream_state["mv"] = data

        async def warm_then_remove(members):
            state.stream_state.pop("mv", None)
            return [main._MultiviewInput(t, t, True, 0x0F, False) for t in members]

        launch = AsyncMock(return_value=("ready", []))
        with patch.object(main, "_warm_multiview_members", warm_then_remove), \
                patch.object(main, "_launch_multiview_run", launch):
            asyncio.run(main._spawn_multiview("mv", data))
        launch.assert_not_awaited()

    def test_cancelled_spawn_stops_the_ffmpeg_it_launched(self):
        data = multiview_data()
        state.stream_state["mv"] = data
        inputs = [main._MultiviewInput(t, t, True, 0x0F, False) for t in data["member_team_ids"]]
        process = FakeProcess()

        async def fake_exec(*args, **kwargs):
            return process

        async def never_ready(*args, **kwargs):
            await asyncio.sleep(10)

        async def scenario():
            task = asyncio.create_task(main._launch_multiview_run("mv", data, 5, inputs, "none", False))
            await asyncio.sleep(0.2)
            self.assertEqual(main._MULTIVIEW_PROCESSES["mv"]["run_id"], 5)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(main, "MULTIVIEW_OUTPUT_ROOT", Path(tmp)), \
                patch.object(asyncio, "create_subprocess_exec", fake_exec), \
                patch.object(main, "_wait_for_first_segment", never_ready), \
                patch.object(main, "_watch_multiview_process", AsyncMock()), \
                patch.object(main, "_drain_multiview_log", AsyncMock()), \
                patch.object(main, "_create_run_job", MagicMock(return_value=None)), \
                patch.object(main, "_write_run_pid_file", MagicMock()), \
                patch.object(main, "_stop_multiview_process", AsyncMock(return_value=True)) as stop:
            asyncio.run(scenario())
        stop.assert_awaited_once_with("mv", run_id=5)
        self.assertEqual(main._MULTIVIEW_PENDING_RUN_DIRS, set())


class RemoveChannelTests(MultiviewStateTestCase):
    def test_remove_multiview_cancels_spawn_and_prunes_per_channel_state(self):
        state.stream_state["mv"] = multiview_data()
        for mapping in (main._MULTIVIEW_FAILURES, main._MULTIVIEW_HOLDS, main._MULTIVIEW_RESTARTS):
            mapping["mv"] = {"count": 1, "last_failure": 0.0, "last_error": "", "until": 0.0, "times": deque()}
            mapping["other"] = dict(mapping["mv"])
        main._MULTIVIEW_REFUSALS["mv"] = 2
        main._MULTIVIEW_MANUAL_STOPS["mv"] = 1.0
        main._MULTIVIEW_LAST_VIEWER["mv"] = 1.0
        main._LAST_PLAYBACK_EVENT["mv"] = 1.0
        registry = FakeRegistry()

        async def scenario():
            task = asyncio.create_task(asyncio.sleep(10))
            main._MULTIVIEW_START_TASKS["mv"] = task
            await main._remove_channel("mv")
            return task

        with patch.object(main, "SESSIONS", registry), \
                patch.object(main, "delete_multiview_channel_async", AsyncMock()), \
                patch.object(main, "_remove_tree_later", MagicMock()):
            task = asyncio.run(scenario())
        self.assertTrue(task.cancelled())
        for mapping in (
            main._MULTIVIEW_FAILURES, main._MULTIVIEW_HOLDS, main._MULTIVIEW_RESTARTS, main._MULTIVIEW_REFUSALS,
            main._MULTIVIEW_MANUAL_STOPS, main._MULTIVIEW_LAST_VIEWER, main._MULTIVIEW_START_TASKS,
            main._LAST_PLAYBACK_EVENT,
        ):
            self.assertNotIn("mv", mapping)
        self.assertIn("other", main._MULTIVIEW_FAILURES)
        self.assertNotIn("mv", state.stream_state)
        self.assertEqual(registry.closed, ["mv", "mv#a0", "mv#a1"])


class PlaceholderTests(MultiviewStateTestCase):
    def test_spawn_failure_backs_off_instead_of_respawning_every_poll(self):
        ffmpeg_proc.FFMPEG_AVAILABLE = True
        launches = []

        async def failing_exec(*args, **kwargs):
            launches.append(args)
            raise OSError("cannot run ffmpeg")

        async def scenario():
            first = await placeholder._ensure_placeholder_running()
            second = await placeholder._ensure_placeholder_running()
            return first, second

        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(placeholder, "PLACEHOLDER_OUTPUT_DIR", Path(tmp)), \
                patch.object(asyncio, "create_subprocess_exec", failing_exec), \
                patch.object(config.LOGGER, "error") as error_log:
            self.assertEqual(asyncio.run(scenario()), (False, False))
            with patch.object(main, "_spawn_background_task") as spawn:
                main._request_placeholder_start()
            spawn.assert_not_called()
        self.assertEqual(len(launches), 1)
        self.assertEqual(error_log.call_count, 1)
        self.assertGreater(placeholder._placeholder_cooldown_remaining(), 0.0)

    def test_drawtext_failure_retries_without_caption(self):
        ffmpeg_proc.FFMPEG_AVAILABLE = True
        attempts = []

        async def fake_launch(drawtext):
            attempts.append(drawtext)
            if drawtext:
                return False, ["[Parsed_drawtext_2 @ 0x1] Cannot find a valid font for the family Sans"]
            return True, []

        with patch.object(placeholder, "_launch_placeholder_run", fake_launch), patch.object(config.LOGGER, "warning"):
            self.assertTrue(asyncio.run(placeholder._ensure_placeholder_running()))
        self.assertEqual(attempts, [True, False])
        self.assertFalse(placeholder._PLACEHOLDER_DRAWTEXT_OK)
        self.assertIsNone(placeholder._PLACEHOLDER_FAILURE)

    def test_placeholder_args_font_and_caption_free_variant(self):
        args = placeholder._build_placeholder_ffmpeg_args(Path("/x"))
        video_filter = args[args.index("-vf") + 1]
        self.assertIn("No Signal", video_filter)
        if sys.platform == "win32":
            self.assertIn("fontfile='C\\:/", video_filter)
            self.assertNotIn("font='Sans'", video_filter)
        plain = placeholder._build_placeholder_ffmpeg_args(Path("/x"), drawtext=False)
        self.assertNotIn("drawtext", plain[plain.index("-vf") + 1])
        self.assertEqual(ffmpeg_proc._ffmpeg_filter_path(Path(r"C:\Windows\Fonts\arial.ttf")).replace("\\\\", "\\"),
                         "C\\:/Windows/Fonts/arial.ttf")


class EncoderSettingsTests(unittest.TestCase):
    def test_env_choice_rejects_unknown_values(self):
        with patch.dict(os.environ, {"X_PRESET": "P7"}):
            self.assertEqual(main._env_choice("X_PRESET", "p4", {"p4", "p7"}), "p7")
        with patch.dict(os.environ, {"X_PRESET": "turbo"}), patch.object(config.LOGGER, "warning"):
            self.assertEqual(main._env_choice("X_PRESET", "p4", {"p4", "p7"}), "p4")

    def test_gop_and_keyframes_follow_fps_and_segment_length(self):
        with patch.object(main, "MULTIVIEW_FPS", 60), patch.object(main, "MULTIVIEW_SEGMENT_SECONDS", 4), \
                patch.object(main, "MULTIVIEW_NVENC_PRESET", "p2"), patch.object(main, "MULTIVIEW_NVENC_TUNE", "ull"):
            args = main._multiview_video_encoder_args("nvenc")
        self.assertEqual(args[args.index("-g") + 1], "240")
        self.assertEqual(args[args.index("-force_key_frames") + 1], "expr:gte(t,n_forced*4)")
        self.assertEqual(args[args.index("-preset") + 1], "p2")
        self.assertEqual(args[args.index("-tune") + 1], "ull")
        with patch.object(main, "MULTIVIEW_HLS_LIST_SIZE", 12):
            self.assertIn("hls_list_size=12", main._multiview_tee_outputs(2))


class ProcessControlTests(unittest.TestCase):
    def test_version_probe_kills_a_hung_ffmpeg(self):
        class HangingProcess:
            returncode = None
            killed = False

            async def communicate(self):
                await asyncio.sleep(10)

            def kill(self):
                self.killed = True
                self.returncode = 1

            async def wait(self):
                return self.returncode

        process = HangingProcess()

        async def fake_exec(*args, **kwargs):
            return process

        with patch.object(asyncio, "create_subprocess_exec", fake_exec):
            with self.assertRaises(asyncio.TimeoutError):
                asyncio.run(ffmpeg_proc._probe_ffmpeg_version("ffmpeg", timeout=0.05))
        self.assertTrue(process.killed)

    @unittest.skipUnless(sys.platform == "win32", "Windows process control")
    def test_ffmpeg_runs_below_normal_priority_without_a_window(self):
        flags = ffmpeg_proc._ffmpeg_creationflags()
        self.assertTrue(flags & subprocess.BELOW_NORMAL_PRIORITY_CLASS)
        self.assertTrue(flags & subprocess.CREATE_NO_WINDOW)

    @unittest.skipUnless(sys.platform == "win32", "Windows process control")
    def test_kernel32_prototypes_return_full_handles(self):
        from ctypes import wintypes

        api = ffmpeg_proc._win32_process_api()
        for name in ("CreateJobObjectW", "OpenProcess"):
            self.assertIs(getattr(api.k32, name).restype, wintypes.HANDLE)
        self.assertIsNotNone(api.k32.TerminateJobObject.argtypes)

    @unittest.skipUnless(sys.platform == "win32", "Windows process control")
    def test_run_job_terminates_the_process(self):
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        try:
            job = ffmpeg_proc._create_run_job(child.pid)
            self.assertTrue(job)
            entry = {"process": FakeProcess(returncode=None, pid=child.pid), "job": job}
            with patch.object(ffmpeg_proc, "_taskkill_tree", AsyncMock()) as taskkill:
                asyncio.run(ffmpeg_proc._terminate_ffmpeg(entry, "test", kill_timeout=0.1))
            self.assertIsNotNone(child.wait(timeout=10))
            entry["process"].kill.assert_not_called()  # the job did it
            taskkill.assert_not_awaited()
            self.assertNotIn("job", entry)
        finally:
            if child.poll() is None:
                child.kill()

    @unittest.skipUnless(sys.platform == "win32", "Windows process control")
    def test_no_taskkill_for_a_process_asyncio_saw_exit(self):
        entry = {"process": FakeProcess(returncode=0)}
        with patch.object(ffmpeg_proc, "_taskkill_tree", AsyncMock()) as taskkill:
            asyncio.run(ffmpeg_proc._terminate_ffmpeg(entry, "test", kill_timeout=0.1))
        entry["process"].kill.assert_not_called()
        taskkill.assert_not_awaited()

    @unittest.skipUnless(sys.platform == "win32", "Windows process control")
    def test_startup_kills_orphans_only_when_creation_time_matches(self):
        api = ffmpeg_proc._win32_process_api()
        orphan = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        bystander = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        try:
            with tempfile.TemporaryDirectory() as tmp:
                root, placeholder_root = Path(tmp) / "multiview", Path(tmp) / "placeholder"
                orphan_dir, bystander_dir = root / "mv" / "run1", placeholder_root / "run2"
                orphan_dir.mkdir(parents=True)
                bystander_dir.mkdir(parents=True)
                (orphan_dir / ffmpeg_proc.RUN_PID_FILE).write_text(json.dumps(
                    {"pid": orphan.pid, "created": api.process_creation_time(orphan.pid)}), encoding="utf-8")
                # Same pid, different creation time: a reused pid, must survive.
                (bystander_dir / ffmpeg_proc.RUN_PID_FILE).write_text(json.dumps(
                    {"pid": bystander.pid, "created": api.process_creation_time(bystander.pid) + 1}), encoding="utf-8")
                with patch.object(main, "MULTIVIEW_OUTPUT_ROOT", root), \
                        patch.object(placeholder, "PLACEHOLDER_OUTPUT_DIR", placeholder_root), \
                        patch.object(config.LOGGER, "warning"):
                    self.assertEqual(main._kill_orphaned_ffmpeg(), 1)
            self.assertIsNotNone(orphan.wait(timeout=10))
            self.assertIsNone(bystander.poll())
        finally:
            for child in (orphan, bystander):
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=10)

    @unittest.skipUnless(sys.platform == "win32", "Windows process control")
    def test_pid_file_records_creation_time(self):
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        try:
            with tempfile.TemporaryDirectory() as tmp:
                ffmpeg_proc._write_run_pid_file(Path(tmp), child.pid)
                record = json.loads((Path(tmp) / ffmpeg_proc.RUN_PID_FILE).read_text(encoding="utf-8"))
            self.assertEqual(record["pid"], child.pid)
            self.assertEqual(record["created"], ffmpeg_proc._win32_process_api().process_creation_time(child.pid))
        finally:
            child.kill()
            child.wait(timeout=10)


if __name__ == "__main__":
    unittest.main()
