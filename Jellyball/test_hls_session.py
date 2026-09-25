"""Unit tests for hls_session: playlist parsing, AES-128 decryption, and
ChannelSession's polling/publishing/failover state machine.

No network and no ffmpeg: segment "downloads" are served from an in-memory
dict via a fake SessionHooks.fetch, and segment bodies are small synthetic
MPEG-TS blobs built by hand (just enough structure for TsNormalizer to parse
them: PAT + PMT + one video PES with PTS/DTS, optionally one audio PES).
"""
import asyncio
import os
import sys
import time
import unittest
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from ts_normalize import crc32_mpeg2
from hls_session import (
    ChannelSession,
    FetchResult,
    SessionConfig,
    SessionHooks,
    SourceSpec,
    aes128_decrypt,
    choose_variant,
    parse_master_playlist,
    parse_media_playlist,
)

SYNC_BYTE = 0x47
TS_PACKET_SIZE = 188


# ---------------------------------------------------------------------------
# Tiny synthetic MPEG-TS segment builder (just enough for TsNormalizer).
# ---------------------------------------------------------------------------

def _ts_packet(pid: int, cc: int, payload: bytes, unit_start: bool) -> bytes:
    stuffing = 184 - len(payload)
    assert stuffing >= 0
    if stuffing == 0:
        afc = 0x1
        af = b""
    else:
        afc = 0x3
        af = bytes([stuffing - 1]) + (b"\x00" + b"\xFF" * (stuffing - 2) if stuffing >= 2 else b"")
    byte1 = (0x40 if unit_start else 0x00) | ((pid >> 8) & 0x1F)
    byte2 = pid & 0xFF
    byte3 = (afc << 4) | (cc & 0x0F)
    packet = bytes([SYNC_BYTE, byte1, byte2, byte3]) + af + payload
    assert len(packet) == TS_PACKET_SIZE
    return packet


def _pts_bytes(prefix4: int, value: int) -> bytes:
    value &= (1 << 33) - 1
    b0 = (prefix4 << 4) | (((value >> 30) & 0x07) << 1) | 0x01
    b1 = (value >> 22) & 0xFF
    b2 = (((value >> 15) & 0x7F) << 1) | 0x01
    b3 = (value >> 7) & 0xFF
    b4 = ((value & 0x7F) << 1) | 0x01
    return bytes([b0, b1, b2, b3, b4])


def _build_pes(stream_id: int, pts: int, dts: int = None) -> bytes:
    if dts is None:
        header2 = 0x80
        header_data = _pts_bytes(0b0010, pts)
    else:
        header2 = 0xC0
        header_data = _pts_bytes(0b0011, pts) + _pts_bytes(0b0001, dts)
    optional_header = bytes([0x80, header2, len(header_data)]) + header_data
    payload = b"\x00" * 16
    body = bytes([0x00, 0x00, 0x01, stream_id])
    pes_payload = optional_header + payload
    return body + len(pes_payload).to_bytes(2, "big") + pes_payload


def _build_pat_section(pmt_pid: int) -> bytes:
    body = bytes([
        0x00, 0xB0, 0x0D, 0x00, 0x01, 0xC1, 0x00, 0x00,
        0x00, 0x01, 0xE0 | ((pmt_pid >> 8) & 0x1F), pmt_pid & 0xFF,
    ])
    return body + crc32_mpeg2(body).to_bytes(4, "big")


def _build_pmt_section(pcr_pid: int, streams: List[Tuple[int, int]]) -> bytes:
    streams_bytes = bytearray()
    for stream_type, pid in streams:
        streams_bytes.extend([stream_type, 0xE0 | ((pid >> 8) & 0x1F), pid & 0xFF, 0xF0, 0x00])
    section_length = 9 + len(streams_bytes) + 4
    body = bytes([
        0x02, 0xB0 | ((section_length >> 8) & 0x0F), section_length & 0xFF,
        0x00, 0x01, 0xC1, 0x00, 0x00,
        0xE0 | ((pcr_pid >> 8) & 0x1F), pcr_pid & 0xFF, 0xF0, 0x00,
    ]) + bytes(streams_bytes)
    return body + crc32_mpeg2(body).to_bytes(4, "big")


def make_ts_segment(pts: int = 90000, video_pid: int = 0x101, audio_pid: int = 0x102,
                     include_audio: bool = True) -> bytes:
    packets = []
    pmt_pid = 0x200
    streams = [(0x1B, video_pid)]
    if include_audio:
        streams.append((0x0F, audio_pid))
    packets.append(_ts_packet(0x0000, 0, b"\x00" + _build_pat_section(pmt_pid), True))
    packets.append(_ts_packet(pmt_pid, 0, b"\x00" + _build_pmt_section(video_pid, streams), True))
    packets.append(_ts_packet(video_pid, 0, _build_pes(0xE0, pts, pts), True))
    if include_audio:
        packets.append(_ts_packet(audio_pid, 0, _build_pes(0xC0, pts), True))
    return b"".join(packets)


NOT_TS_DATA = b"this is not mpeg-ts, just filler bytes that are not a segment"


# ---------------------------------------------------------------------------
# Playlist parsing tests
# ---------------------------------------------------------------------------

class MasterPlaylistTests(unittest.TestCase):
    def test_picks_highest_bandwidth_variant(self):
        text = (
            "#EXTM3U\n"
            "#EXT-X-STREAM-INF:BANDWIDTH=1000000,RESOLUTION=640x360\n"
            "low.m3u8\n"
            "#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080\n"
            "high.m3u8\n"
        )
        master = parse_master_playlist(text, "http://host/master.m3u8")
        variant, demuxed = choose_variant(master)
        self.assertTrue(variant.uri.endswith("high.m3u8"))
        self.assertFalse(demuxed)

    def test_bandwidth_cap_picks_highest_under_cap(self):
        text = (
            "#EXTM3U\n"
            "#EXT-X-STREAM-INF:BANDWIDTH=1000000,RESOLUTION=640x360\n"
            "low.m3u8\n"
            "#EXT-X-STREAM-INF:BANDWIDTH=1800000,RESOLUTION=1280x720\n"
            "mid.m3u8\n"
            "#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080\n"
            "high.m3u8\n"
        )
        master = parse_master_playlist(text, "http://host/master.m3u8")
        variant, demuxed = choose_variant(master, bandwidth_cap=2000000)
        self.assertTrue(variant.uri.endswith("mid.m3u8"))

    def test_bandwidth_cap_below_everything_picks_cheapest(self):
        text = (
            "#EXTM3U\n"
            "#EXT-X-STREAM-INF:BANDWIDTH=1000000,RESOLUTION=640x360\n"
            "low.m3u8\n"
            "#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080\n"
            "high.m3u8\n"
        )
        master = parse_master_playlist(text, "http://host/master.m3u8")
        variant, _ = choose_variant(master, bandwidth_cap=1)
        self.assertTrue(variant.uri.endswith("low.m3u8"))

    def test_skips_audio_only_variant_even_if_higher_bandwidth(self):
        text = (
            "#EXTM3U\n"
            "#EXT-X-STREAM-INF:BANDWIDTH=9999999,CODECS=\"mp4a.40.2\"\n"
            "audio-only.m3u8\n"
            "#EXT-X-STREAM-INF:BANDWIDTH=2000000,RESOLUTION=1280x720,CODECS=\"avc1.4d401f,mp4a.40.2\"\n"
            "video.m3u8\n"
        )
        master = parse_master_playlist(text, "http://host/master.m3u8")
        variant, demuxed = choose_variant(master)
        self.assertTrue(variant.uri.endswith("video.m3u8"))
        self.assertFalse(demuxed)

    def test_detects_demuxed_audio_group_with_uri(self):
        text = (
            "#EXTM3U\n"
            "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aud1\",NAME=\"English\",URI=\"audio.m3u8\"\n"
            "#EXT-X-STREAM-INF:BANDWIDTH=4000000,RESOLUTION=1920x1080,AUDIO=\"aud1\"\n"
            "video.m3u8\n"
        )
        master = parse_master_playlist(text, "http://host/master.m3u8")
        self.assertIn("aud1", master.audio_groups_with_uri)
        variant, demuxed = choose_variant(master)
        self.assertTrue(variant.uri.endswith("video.m3u8"))
        self.assertTrue(demuxed)


class MediaPlaylistTests(unittest.TestCase):
    def test_media_sequence_numbering(self):
        text = (
            "#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:100\n"
            "#EXTINF:6.0,\nseg0.ts\n#EXTINF:6.0,\nseg1.ts\n#EXTINF:6.0,\nseg2.ts\n"
        )
        playlist = parse_media_playlist(text, "http://host/media.m3u8")
        self.assertEqual([s.useq for s in playlist.segments], [100, 101, 102])
        self.assertEqual(playlist.media_sequence, 100)
        self.assertEqual(playlist.target_duration, 6.0)

    def test_discontinuity_flag_on_following_segment(self):
        text = (
            "#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:0\n"
            "#EXTINF:6.0,\nseg0.ts\n"
            "#EXT-X-DISCONTINUITY\n#EXTINF:6.0,\nseg1.ts\n"
            "#EXTINF:6.0,\nseg2.ts\n"
        )
        playlist = parse_media_playlist(text, "http://host/media.m3u8")
        flags = [s.discontinuity for s in playlist.segments]
        self.assertEqual(flags, [False, True, False])

    def test_key_with_iv_and_without(self):
        iv_hex = "0" * 31 + "1"
        text = (
            "#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:0\n"
            f"#EXT-X-KEY:METHOD=AES-128,URI=\"key1\",IV=0x{iv_hex}\n"
            "#EXTINF:6.0,\nseg0.ts\n"
            "#EXT-X-KEY:METHOD=AES-128,URI=\"key2\"\n"
            "#EXTINF:6.0,\nseg1.ts\n"
            "#EXT-X-KEY:METHOD=NONE\n"
            "#EXTINF:6.0,\nseg2.ts\n"
        )
        playlist = parse_media_playlist(text, "http://host/media.m3u8")
        seg0, seg1, seg2 = playlist.segments
        self.assertEqual(seg0.key.method, "AES-128")
        self.assertEqual(seg0.key.iv, bytes(15) + b"\x01")
        self.assertTrue(seg0.key.uri.endswith("key1"))
        self.assertEqual(seg1.key.method, "AES-128")
        self.assertIsNone(seg1.key.iv)
        self.assertIsNone(seg2.key)

    def test_byterange_with_implicit_offset(self):
        text = (
            "#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:0\n"
            "#EXT-X-BYTERANGE:1000@500\n#EXTINF:6.0,\nvideo.ts\n"
            "#EXT-X-BYTERANGE:500\n#EXTINF:6.0,\nvideo.ts\n"
        )
        playlist = parse_media_playlist(text, "http://host/media.m3u8")
        first, second = playlist.segments
        self.assertEqual(first.byterange, (1000, 500))
        self.assertEqual(second.byterange, (500, 1500))

    def test_map_sets_has_map(self):
        text = (
            "#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:0\n"
            "#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:6.0,\nseg0.m4s\n"
        )
        playlist = parse_media_playlist(text, "http://host/media.m3u8")
        self.assertTrue(playlist.has_map)

    def test_endlist_sets_endlist_true(self):
        text = (
            "#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:0\n"
            "#EXTINF:6.0,\nseg0.ts\n#EXT-X-ENDLIST\n"
        )
        playlist = parse_media_playlist(text, "http://host/media.m3u8")
        self.assertTrue(playlist.endlist)

    def test_local_torn_read_drops_last_line(self):
        # No trailing newline: the last line ("seg1.t") is an in-progress
        # write and must be dropped rather than treated as a real entry.
        text = "#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:6.0,\nseg0.ts\n#EXTINF:6.0,\nseg1.t"
        playlist = parse_media_playlist(text, "/var/media/out.m3u8", local=True)
        self.assertEqual(len(playlist.segments), 1)
        self.assertTrue(playlist.segments[0].uri.endswith("seg0.ts"))

    def test_local_path_traversal_rejected(self):
        text = (
            "#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:0\n"
            "#EXTINF:6.0,\n../evil.ts\n#EXTINF:6.0,\nseg0.ts\n"
        )
        playlist = parse_media_playlist(text, "/var/media/out.m3u8", local=True)
        self.assertEqual(len(playlist.segments), 1)
        self.assertTrue(playlist.segments[0].uri.endswith("seg0.ts"))


class Aes128DecryptTests(unittest.TestCase):
    def test_round_trip_with_pkcs7_padding(self):
        key = os.urandom(16)
        iv = os.urandom(16)
        plaintext = b"hello jellyball, this is a test HLS segment payload!"
        pad_len = 16 - (len(plaintext) % 16)
        padded = plaintext + bytes([pad_len]) * pad_len
        encryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).encryptor()
        ciphertext = encryptor.update(padded) + encryptor.finalize()

        result = aes128_decrypt(ciphertext, key, iv)
        self.assertEqual(result, plaintext)

    def test_round_trip_exact_block_multiple(self):
        key = os.urandom(16)
        iv = os.urandom(16)
        plaintext = os.urandom(32)  # exactly two blocks, still gets a full pad block
        pad_len = 16
        padded = plaintext + bytes([pad_len]) * pad_len
        encryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).encryptor()
        ciphertext = encryptor.update(padded) + encryptor.finalize()

        result = aes128_decrypt(ciphertext, key, iv)
        self.assertEqual(result, plaintext)


# ---------------------------------------------------------------------------
# ChannelSession tests
# ---------------------------------------------------------------------------

class Harness:
    """Fake SessionHooks backed by an in-memory URL -> FetchResult map."""

    def __init__(self):
        self.responses: Dict[str, FetchResult] = {}
        self.source: Optional[SourceSpec] = None
        self.failures: List[Tuple[str, Tuple, str]] = []
        self.incompatibles: List[Tuple[str, Tuple, str]] = []
        self.media_info: List[Tuple[str, Tuple, bool, Tuple]] = []
        self.fetch_calls: List[str] = []
        self.incompatible_result: Optional[bool] = None

    def set_response(self, url: str, body: bytes, status: int = 200, content_type: str = "application/octet-stream"):
        self.responses[url] = FetchResult(status=status, url=url, content_type=content_type, body=body)

    def set_playlist(self, url: str, text: str):
        self.set_response(url, text.encode("utf-8"), content_type="application/vnd.apple.mpegurl")

    async def fetch(self, url, headers, max_bytes, timeout):
        self.fetch_calls.append(url)
        if url in self.responses:
            return self.responses[url]
        base = url.split("?", 1)[0]
        return self.responses.get(base)

    def headers_for(self, referer, origin):
        return {}

    def resolve_source(self, channel_id):
        return self.source

    def report_failure(self, channel_id, key, reason):
        self.failures.append((channel_id, key, reason))

    def report_incompatible(self, channel_id, key, reason):
        self.incompatibles.append((channel_id, key, reason))
        return self.incompatible_result

    def on_media_info(self, channel_id, key, has_audio, signature):
        self.media_info.append((channel_id, key, has_audio, signature))

    def hooks(self) -> SessionHooks:
        return SessionHooks(
            fetch=self.fetch,
            headers_for=self.headers_for,
            resolve_source=self.resolve_source,
            report_failure=self.report_failure,
            report_incompatible=self.report_incompatible,
            on_media_info=self.on_media_info,
        )


def playlist_text(base: str, useqs: List[int], durations: Optional[List[float]] = None,
                   media_sequence: Optional[int] = None, target_duration: int = 6,
                   endlist: bool = False, discontinuity_useqs: frozenset = frozenset(),
                   has_map: bool = False, sample_aes: bool = False) -> str:
    if durations is None:
        durations = [6.0] * len(useqs)
    if media_sequence is None:
        media_sequence = useqs[0] if useqs else 0
    lines = ["#EXTM3U", "#EXT-X-VERSION:3", f"#EXT-X-TARGETDURATION:{target_duration}",
              f"#EXT-X-MEDIA-SEQUENCE:{media_sequence}"]
    if has_map:
        lines.append("#EXT-X-MAP:URI=\"init.mp4\"")
    if sample_aes:
        lines.append("#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"key\"")
    for useq, duration in zip(useqs, durations):
        if useq in discontinuity_useqs:
            lines.append("#EXT-X-DISCONTINUITY")
        lines.append(f"#EXTINF:{duration:.3f},")
        lines.append(f"{base}/seg{useq}.ts")
    if endlist:
        lines.append("#EXT-X-ENDLIST")
    return "\n".join(lines) + "\n"


def fast_config(**overrides) -> SessionConfig:
    base = dict(
        idle_timeout=5.0,
        min_start_segments=3,
        live_edge_segments=3,
        window_min_segments=3,
        window_min_seconds=1000.0,  # keep window-seconds trimming out of the way by default
        window_max_segments=3,
        grace_segments=5,
        stale_min_seconds=0.05,
        fail_threshold=3,
        failure_report_cooldown=1.0,
        max_catchup_segments=8,
    )
    base.update(overrides)
    return SessionConfig(**base)


class ChannelSessionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.harness = Harness()

    def make_session(self, cfg: Optional[SessionConfig] = None) -> ChannelSession:
        return ChannelSession("chan1", self.harness.hooks(), cfg or fast_config())

    async def test_starts_at_live_edge(self):
        base = "http://upstream"
        self.harness.source = SourceSpec(key=("primary",), url=f"{base}/media.m3u8", label="primary")
        useqs = list(range(6))
        self.harness.set_playlist(f"{base}/media.m3u8", playlist_text(base, useqs))
        for u in useqs:
            self.harness.set_response(f"{base}/seg{u}.ts", make_ts_segment(pts=90000 * (u + 1)))

        session = self.make_session()
        await session._poll_once()

        self.assertEqual(len(session.window), 3)
        self.assertEqual(session.last_useq, 5)

    async def test_render_playlist_monotonic_sequence_and_prefix(self):
        base = "http://upstream"
        self.harness.source = SourceSpec(key=("primary",), url=f"{base}/media.m3u8", label="primary")
        useqs = list(range(4))
        self.harness.set_playlist(f"{base}/media.m3u8", playlist_text(base, useqs, target_duration=6))
        for u in useqs:
            self.harness.set_response(f"{base}/seg{u}.ts", make_ts_segment())

        session = self.make_session()
        await session._poll_once()
        rendered = session.render_playlist("chan1/")

        lines = rendered.splitlines()
        self.assertEqual(lines[0], "#EXTM3U")
        first_seq = session.window[0].seq
        self.assertIn(f"#EXT-X-MEDIA-SEQUENCE:{first_seq}", lines)
        self.assertIn("#EXT-X-TARGETDURATION:6", lines)
        uri_lines = [l for l in lines if l.startswith("chan1/")]
        self.assertEqual(len(uri_lines), 3)
        seqs = [int(l[len("chan1/"):-len(".ts")]) for l in uri_lines]
        self.assertEqual(seqs, sorted(seqs))
        self.assertEqual(seqs, list(range(first_seq, first_seq + 3)))

    async def test_source_key_change_marks_discontinuity_and_continues_sequence(self):
        base = "http://upstream"
        self.harness.source = SourceSpec(key=("primary",), url=f"{base}/media.m3u8", label="primary")
        self.harness.set_playlist(f"{base}/media.m3u8", playlist_text(base, list(range(3))))
        for u in range(3):
            self.harness.set_response(f"{base}/seg{u}.ts", make_ts_segment())

        session = self.make_session()
        await session._poll_once()
        seq_before_switch = session.next_seq
        self.assertEqual(session.stats["source_switches"], 0)

        # Failover to a brand-new source: different key, own sequence space.
        # Its segments are served from a distinct base path so they can't
        # collide with the primary source's URLs in the harness.
        self.harness.source = SourceSpec(key=("backup",), url=f"{base}/media2.m3u8", label="backup")
        self.harness.set_playlist(f"{base}/media2.m3u8", playlist_text("http://backup", list(range(3))))
        for u in range(3):
            self.harness.set_response(f"http://backup/seg{u}.ts", make_ts_segment())

        await session._poll_once()

        self.assertEqual(session.stats["source_switches"], 1)
        published = list(session.window)
        # The first segment ingested after the switch must carry the marker.
        switched_segment = next(s for s in published if s.seq == seq_before_switch)
        self.assertTrue(switched_segment.discontinuity)
        # Sequence numbering is proxy-owned and never resets on a source switch.
        # The switch happened right after the last publish, so only one
        # segment of the new source is appended (no replayed overlap).
        self.assertEqual(session.next_seq, seq_before_switch + 1)

    async def test_switch_appends_segments_covering_the_outage_gap(self):
        base = "http://upstream"
        self.harness.source = SourceSpec(key=("primary",), url=f"{base}/media.m3u8", label="primary")
        self.harness.set_playlist(f"{base}/media.m3u8", playlist_text(base, list(range(3))))
        for u in range(3):
            self.harness.set_response(f"{base}/seg{u}.ts", make_ts_segment())
        session = self.make_session()
        await session._poll_once()
        seq_before_switch = session.next_seq
        # The old source went quiet ~7s before failover: with ~6s segments,
        # two new-source segments are needed to cover that gap.
        session.last_new_segment_at -= 7.0
        self.harness.source = SourceSpec(key=("backup",), url=f"{base}/media2.m3u8", label="backup")
        self.harness.set_playlist(f"{base}/media2.m3u8", playlist_text("http://backup", list(range(3))))
        for u in range(3):
            self.harness.set_response(f"http://backup/seg{u}.ts", make_ts_segment())
        await session._poll_once()
        durations = [s.duration for s in list(session.window)[-3:]]
        expected = 1 if durations[-1] >= 7.0 else (2 if sum(durations[-2:]) >= 7.0 else 3)
        self.assertEqual(session.next_seq - seq_before_switch, expected)

    async def test_same_key_url_only_change_no_discontinuity_continues_by_upstream_seq(self):
        base = "http://upstream"
        url1 = f"{base}/media.m3u8"
        self.harness.source = SourceSpec(key=("primary",), url=url1, label="primary")
        self.harness.set_playlist(url1, playlist_text(base, list(range(6)), media_sequence=0))
        for u in range(8):
            self.harness.set_response(f"{base}/seg{u}.ts", make_ts_segment())

        session = self.make_session()
        await session._poll_once()
        self.assertEqual(session.last_useq, 5)
        seq_before = session.next_seq

        # Token refresh: same key, new URL, server keeps serving continuing
        # upstream sequence numbers from the same segment namespace.
        url2 = f"{base}/media.m3u8?token=abc"
        self.harness.set_response(url2, playlist_text(base, list(range(4, 8)), media_sequence=4).encode())
        self.harness.source = SourceSpec(key=("primary",), url=url2, label="primary")

        await session._poll_once()

        self.assertEqual(session.stats["source_switches"], 0)
        self.assertEqual(session.last_useq, 7)
        new_segments = list(session.window)[-2:]
        self.assertTrue(all(not s.discontinuity for s in new_segments))
        self.assertEqual(session.next_seq, seq_before + 2)

    async def test_window_trim_increments_discontinuity_sequence_on_rolloff(self):
        from hls_session import SessionSegment

        # Deterministic, direct exercise of the trim/rollover accounting:
        # a discontinuity segment sits at the head of the window and must
        # bump discontinuity_seq exactly when it rolls off into grace.
        cfg = fast_config(window_min_segments=1, window_min_seconds=0.0, window_max_segments=1)
        session = self.make_session(cfg)
        session.window.append(SessionSegment(seq=1, duration=6.0, discontinuity=True, data=b"a"))
        session.window.append(SessionSegment(seq=2, duration=6.0, discontinuity=False, data=b"b"))
        before = session.discontinuity_seq

        session._trim_window()

        self.assertEqual(session.discontinuity_seq, before + 1)
        self.assertEqual(len(session.window), 1)
        self.assertEqual(session.window[0].seq, 2)
        self.assertIn(1, session.grace)

    async def test_get_segment_from_window_and_grace(self):
        base = "http://upstream"
        self.harness.source = SourceSpec(key=("primary",), url=f"{base}/media.m3u8", label="primary")
        self.harness.set_playlist(f"{base}/media.m3u8", playlist_text(base, list(range(3))))
        for u in range(3):
            self.harness.set_response(f"{base}/seg{u}.ts", make_ts_segment())

        cfg = fast_config(window_min_segments=1, window_min_seconds=0.0, window_max_segments=1, grace_segments=5)
        session = self.make_session(cfg)
        await session._poll_once()

        in_window_seq = session.window[-1].seq
        self.assertIsNotNone(session.get_segment(in_window_seq))
        grace_seq = next(iter(session.grace))
        self.assertIsNotNone(session.get_segment(grace_seq))
        self.assertIsNone(session.get_segment(999999))

    async def test_failed_segment_downloads_are_skipped_and_report_failure(self):
        base = "http://upstream"
        self.harness.source = SourceSpec(key=("primary",), url=f"{base}/media.m3u8", label="primary")
        self.harness.set_playlist(f"{base}/media.m3u8", playlist_text(base, list(range(3))))
        # No segment responses registered at all: every download fails.

        cfg = fast_config(fail_threshold=3)
        session = self.make_session(cfg)
        await session._poll_once()

        self.assertEqual(len(session.window), 0)
        self.assertEqual(session.stats["segment_failures"], 3)
        self.assertEqual(len(self.harness.failures), 1)
        self.assertEqual(self.harness.failures[0][2], "segments failing")

    async def test_stale_playlist_triggers_report_failure_rate_limited(self):
        base = "http://upstream"
        self.harness.source = SourceSpec(key=("primary",), url=f"{base}/media.m3u8", label="primary")
        self.harness.set_playlist(f"{base}/media.m3u8", playlist_text(base, list(range(3))))
        for u in range(3):
            self.harness.set_response(f"{base}/seg{u}.ts", make_ts_segment())

        cfg = fast_config(stale_min_seconds=0.01, failure_report_cooldown=100.0)
        session = self.make_session(cfg)
        await session._poll_once()
        self.assertEqual(len(self.harness.failures), 0)

        # No new segments upstream; make the "last new segment" look old
        # enough to exceed the stale threshold.
        session.last_new_segment_at = session._stale_timer_start = time.monotonic() - 100.0
        await session._poll_once()
        self.assertEqual(len(self.harness.failures), 1)
        self.assertEqual(self.harness.failures[0][2], "playlist stale")
        # Reporting restarts the stale timer but must not make the channel
        # look flowing again (that hid stalls from health and window-close).
        self.assertFalse(session.is_flowing())

        # A second poll right away must be rate limited by the cooldown.
        session.last_new_segment_at = session._stale_timer_start = time.monotonic() - 100.0
        await session._poll_once()
        self.assertEqual(len(self.harness.failures), 1)

    async def test_master_playlist_resolved_to_variant(self):
        base = "http://upstream"
        master_url = f"{base}/master.m3u8"
        variant_url = f"{base}/variant.m3u8"
        self.harness.source = SourceSpec(key=("primary",), url=master_url, label="primary")
        self.harness.set_playlist(master_url, (
            "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=3000000,RESOLUTION=1280x720\nvariant.m3u8\n"
        ))
        self.harness.set_playlist(variant_url, playlist_text(base, list(range(3))))
        for u in range(3):
            self.harness.set_response(f"{base}/seg{u}.ts", make_ts_segment())

        session = self.make_session()
        await session._poll_once()

        self.assertEqual(session.media_url, variant_url)
        self.assertEqual(len(session.window), 3)

    async def test_fmp4_source_before_any_segment_goes_legacy(self):
        base = "http://upstream"
        self.harness.source = SourceSpec(key=("primary",), url=f"{base}/media.m3u8", label="primary")
        self.harness.set_playlist(f"{base}/media.m3u8", playlist_text(base, list(range(3)), has_map=True))

        session = self.make_session()
        await session._poll_once()

        # No other compatible source (hook returned None): legacy passthrough.
        self.assertEqual(session.state, "legacy")
        self.assertEqual(len(self.harness.incompatibles), 1)

    async def test_fmp4_cold_start_tries_another_source_before_legacy(self):
        base = "http://upstream"
        self.harness.source = SourceSpec(key=("primary",), url=f"{base}/media.m3u8", label="primary")
        self.harness.set_playlist(f"{base}/media.m3u8", playlist_text(base, list(range(3)), has_map=True))
        self.harness.incompatible_result = True  # main found a compatible standby

        session = self.make_session()
        await session._poll_once()
        self.assertNotEqual(session.state, "legacy")
        self.assertIsNone(session.source)

        self.harness.source = SourceSpec(key=("backup",), url="http://backup/media.m3u8", label="backup")
        self.harness.set_playlist("http://backup/media.m3u8", playlist_text("http://backup", list(range(3))))
        for u in range(3):
            self.harness.set_response(f"http://backup/seg{u}.ts", make_ts_segment())
        await session._poll_once()
        self.assertEqual(session.state, "live")
        self.assertEqual(len(session.window), 3)

    async def test_single_non_ts_segment_is_skipped_not_incompatible(self):
        base = "http://upstream"
        self.harness.source = SourceSpec(key=("primary",), url=f"{base}/media.m3u8", label="primary")
        self.harness.set_playlist(f"{base}/media.m3u8", playlist_text(base, list(range(4))))
        self.harness.set_response(f"{base}/seg0.ts", b"<html>error</html>" * 20)
        for u in range(1, 4):
            self.harness.set_response(f"{base}/seg{u}.ts", make_ts_segment())
        cfg = fast_config(live_edge_segments=4, min_start_segments=3, window_max_segments=4)
        session = self.make_session(cfg)
        await session._poll_once()
        self.assertEqual(len(self.harness.incompatibles), 0)
        self.assertEqual(len(session.window), 3)
        self.assertEqual(session.stats["segment_failures"], 1)

    async def test_slow_segment_is_skipped_after_its_deadline(self):
        base = "http://upstream"
        self.harness.source = SourceSpec(key=("primary",), url=f"{base}/media.m3u8", label="primary")
        self.harness.set_playlist(f"{base}/media.m3u8", playlist_text(base, list(range(3)), durations=[0.1] * 3))
        for u in range(3):
            self.harness.set_response(f"{base}/seg{u}.ts", make_ts_segment())
        original_fetch = self.harness.fetch

        async def slow_fetch(url, headers, max_bytes, timeout):
            if url.endswith("seg0.ts"):
                await asyncio.sleep(5.0)
            return await original_fetch(url, headers, max_bytes, timeout)

        self.harness.fetch = slow_fetch
        cfg = fast_config(segment_deadline_min=0.2, segment_deadline_factor=1.0, min_start_segments=2)
        session = ChannelSession("chan1", self.harness.hooks(), cfg)
        started = time.monotonic()
        await session._poll_once()
        self.assertLess(time.monotonic() - started, 2.0)
        self.assertEqual(len(session.window), 2)

    async def test_target_duration_is_clamped_and_resets_with_the_run(self):
        base = "http://upstream"
        self.harness.source = SourceSpec(key=("primary",), url=f"{base}/media.m3u8", label="primary")
        self.harness.set_playlist(f"{base}/media.m3u8", playlist_text(base, list(range(3)), target_duration=600))
        for u in range(3):
            self.harness.set_response(f"{base}/seg{u}.ts", make_ts_segment())
        session = self.make_session(fast_config(max_target_duration=15))
        await session._poll_once()
        self.assertEqual(session.target_duration, 15)
        session._release_memory()
        self.assertEqual(session.target_duration, 0)

    async def test_failure_report_cooldown_map_is_bounded(self):
        session = self.make_session()
        for i in range(100):
            session._note_report(("run", i), float(i) * 1000)
        self.assertLessEqual(len(session._last_report), 32)

    async def test_forbidden_playlist_is_reported_as_token_expiry(self):
        base = "http://upstream"
        self.harness.source = SourceSpec(key=("primary",), url=f"{base}/media.m3u8", label="primary")
        self.harness.set_response(f"{base}/media.m3u8", b"denied", status=403)
        session = self.make_session(fast_config(fail_threshold=2))
        await session._poll_once()
        await session._poll_once()
        self.assertEqual(self.harness.failures[-1][2], "playlist forbidden")

    async def test_fmp4_source_after_segments_reports_incompatible(self):
        base = "http://upstream"
        self.harness.source = SourceSpec(key=("primary",), url=f"{base}/media.m3u8", label="primary")
        self.harness.set_playlist(f"{base}/media.m3u8", playlist_text(base, list(range(3))))
        for u in range(3):
            self.harness.set_response(f"{base}/seg{u}.ts", make_ts_segment())

        cfg = fast_config(failure_report_cooldown=100.0)
        session = self.make_session(cfg)
        await session._poll_once()
        self.assertEqual(len(session.window), 3)

        self.harness.set_playlist(f"{base}/media.m3u8", playlist_text(base, list(range(3, 6)), has_map=True))
        await session._poll_once()

        self.assertEqual(len(self.harness.incompatibles), 1)
        self.assertNotEqual(session.state, "legacy")

    async def test_wait_ready_returns_once_three_segments_published(self):
        base = "http://upstream"
        self.harness.source = SourceSpec(key=("primary",), url=f"{base}/media.m3u8", label="primary")
        self.harness.set_playlist(f"{base}/media.m3u8", playlist_text(base, list(range(3))))
        for u in range(3):
            self.harness.set_response(f"{base}/seg{u}.ts", make_ts_segment())

        session = self.make_session()
        import asyncio
        waiter = asyncio.ensure_future(session.wait_ready(2.0))
        await session._poll_once()
        result = await waiter
        self.assertTrue(result)
        self.assertEqual(session.state, "live")

    async def test_lagging_cdn_older_playlist_is_ignored(self):
        base = "http://upstream"
        self.harness.source = SourceSpec(key=("primary",), url=f"{base}/media.m3u8", label="primary")
        self.harness.set_playlist(f"{base}/media.m3u8", playlist_text(base, list(range(6))))
        for u in range(6):
            self.harness.set_response(f"{base}/seg{u}.ts", make_ts_segment())

        session = self.make_session()
        await session._poll_once()
        self.assertEqual(session.last_useq, 5)
        seq_after_first_poll = session.next_seq

        # CDN edge serves a slightly-stale copy of the playlist (older than
        # what we've already consumed), within the "just wait" tolerance.
        self.harness.set_playlist(f"{base}/media.m3u8", playlist_text(base, list(range(3))))
        await session._poll_once()

        self.assertEqual(session.last_useq, 5)
        self.assertEqual(session.next_seq, seq_after_first_poll)
        self.assertEqual(session.stats["source_switches"], 0)

    async def test_big_sequence_reset_triggers_new_epoch_and_discontinuity(self):
        base = "http://upstream"
        self.harness.source = SourceSpec(key=("primary",), url=f"{base}/media.m3u8", label="primary")
        self.harness.set_playlist(f"{base}/media.m3u8", playlist_text(base, list(range(20, 26))))
        for u in range(20, 26):
            self.harness.set_response(f"{base}/seg{u}.ts", make_ts_segment())

        session = self.make_session()
        await session._poll_once()
        self.assertEqual(session.last_useq, 25)
        seq_before = session.next_seq

        # Upstream restarted its own numbering from 0: this is far enough
        # back that it can't be a lagging-CDN copy, so it's treated as a hard
        # sequence reset -> new epoch + discontinuity, not silently ignored.
        self.harness.set_playlist(f"{base}/media.m3u8", playlist_text(base, list(range(3))))
        for u in range(3):
            self.harness.set_response(f"{base}/seg{u}.ts", make_ts_segment())

        with patch.object(session.normalizer, "start_new_epoch", wraps=session.normalizer.start_new_epoch) as spy:
            # A single far-behind response could be a stale CDN edge: ignored.
            await session._poll_once()
            spy.assert_not_called()
            self.assertEqual(session.next_seq, seq_before)
            # Confirmed by the next poll: a real reset.
            await session._poll_once()
            spy.assert_called_once()

        self.assertEqual(session.next_seq, seq_before + 3)
        switched_segment = next(s for s in session.window if s.seq == seq_before)
        self.assertTrue(switched_segment.discontinuity)


if __name__ == "__main__":
    unittest.main()
