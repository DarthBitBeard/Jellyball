"""Unit tests for ts_normalize.TsNormalizer.

Builds small synthetic MPEG-TS segments (PAT/PMT + PES packets with PTS/DTS,
optionally PCR) by hand so the normalizer can be exercised without ffmpeg or
any real media file. The builder here is independent of TsNormalizer's own
private section/PES builders (it only reuses the public crc32_mpeg2 helper),
so the tests check real wire-format behavior rather than testing the module
against itself.
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ts_normalize import (
    TsNormalizer,
    crc32_mpeg2,
    find_ts_start,
    OUT_VIDEO_PID,
    OUT_AUDIO_PID,
    OUT_PMT_PID,
    PAT_PID,
    TIMESTAMP_MODULO,
    CLOCK_HZ,
)

SYNC_BYTE = 0x47
TS_PACKET_SIZE = 188


# ---------------------------------------------------------------------------
# Synthetic MPEG-TS builder
# ---------------------------------------------------------------------------

def _adaptation_field(total_len: int, pcr: int = None) -> bytes:
    """Build an adaptation field of exactly `total_len` bytes (length byte
    included). `pcr` (if given) is written as PCR_flag + a 6-byte PCR field
    (33-bit base, 6 reserved bits, 9-bit extension)."""
    if total_len <= 0:
        return b""
    if total_len == 1:
        return bytes([0])
    length_value = total_len - 1
    flags = 0x10 if pcr is not None else 0x00
    body = bytes([flags])
    if pcr is not None:
        base = pcr % TIMESTAMP_MODULO
        field = (base << 15) | (0x3F << 9) | 0  # reserved bits all 1, ext=0
        body += field.to_bytes(6, "big")
    stuffing = length_value - len(body)
    assert stuffing >= 0, "adaptation field too small for requested fields"
    body += b"\xFF" * stuffing
    return bytes([length_value]) + body


def _ts_packet(pid: int, cc: int, payload: bytes, unit_start: bool, pcr: int = None) -> bytes:
    stuffing_needed = 184 - len(payload)
    assert stuffing_needed >= 0, "payload too large for a single TS packet"
    if stuffing_needed == 0 and pcr is None:
        afc = 0x1
        af = b""
    else:
        afc = 0x3
        af = _adaptation_field(stuffing_needed, pcr)
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


def read_pcr_base(buf: bytes, i: int) -> int:
    """Independent reference reader for the 33-bit PCR base (bytes i..i+5)."""
    value = int.from_bytes(buf[i:i + 6], "big")
    return (value >> 15) & ((1 << 33) - 1)


def read_pts_dts(buf: bytes, i: int) -> int:
    return (
        ((buf[i] >> 1) & 0x07) << 30
        | buf[i + 1] << 22
        | (buf[i + 2] >> 1) << 15
        | buf[i + 3] << 7
        | (buf[i + 4] >> 1)
    )


def build_pes(stream_id: int, payload: bytes = b"\x00" * 20, pts: int = None, dts: int = None) -> bytes:
    if pts is None:
        header2 = 0x00
        header_data = b""
    elif dts is None:
        header2 = 0x80  # PTS only (flags = 0b10 in top 2 bits)
        header_data = _pts_bytes(0b0010, pts)
    else:
        header2 = 0xC0  # PTS + DTS (flags = 0b11)
        header_data = _pts_bytes(0b0011, pts) + _pts_bytes(0b0001, dts)
    optional_header = bytes([0x80, header2, len(header_data)]) + header_data
    body = bytes([0x00, 0x00, 0x01, stream_id])
    pes_payload = optional_header + payload
    pkt_len = len(pes_payload) if len(pes_payload) < 0xFFFF else 0
    return body + pkt_len.to_bytes(2, "big") + pes_payload


def build_pat_section(pmt_pid: int, program_number: int = 1, tsid: int = 1) -> bytes:
    body = bytes([
        0x00,
        0xB0, 0x0D,
        (tsid >> 8) & 0xFF, tsid & 0xFF,
        0xC1,
        0x00, 0x00,
        (program_number >> 8) & 0xFF, program_number & 0xFF,
        0xE0 | ((pmt_pid >> 8) & 0x1F), pmt_pid & 0xFF,
    ])
    return body + crc32_mpeg2(body).to_bytes(4, "big")


def iso639_descriptor(*entries) -> bytes:
    """ISO_639_language_descriptor; each entry is "eng" or ("eng", audio_type)."""
    body = b""
    for entry in entries:
        code, audio_type = (entry, 0) if isinstance(entry, str) else entry
        body += code.encode("ascii") + bytes([audio_type])
    return bytes([0x0A, len(body)]) + body


def build_pmt_section(pcr_pid: int, streams, program_number: int = 1, version: int = 0) -> bytes:
    """`streams` is a list of (stream_type, pid) or (stream_type, pid, descriptors) tuples."""
    streams_bytes = bytearray()
    for entry in streams:
        stream_type, pid = entry[0], entry[1]
        descriptors = entry[2] if len(entry) > 2 else b""
        streams_bytes.extend([
            stream_type,
            0xE0 | ((pid >> 8) & 0x1F), pid & 0xFF,
            0xF0 | ((len(descriptors) >> 8) & 0x0F), len(descriptors) & 0xFF,
        ])
        streams_bytes.extend(descriptors)
    program_info = b""
    section_length = 9 + len(program_info) + len(streams_bytes) + 4
    body = bytes([
        0x02,
        0xB0 | ((section_length >> 8) & 0x0F), section_length & 0xFF,
        (program_number >> 8) & 0xFF, program_number & 0xFF,
        0xC1 | ((version & 0x1F) << 1),
        0x00, 0x00,
        0xE0 | ((pcr_pid >> 8) & 0x1F), pcr_pid & 0xFF,
        0xF0 | ((len(program_info) >> 8) & 0x0F), len(program_info) & 0xFF,
    ]) + program_info + bytes(streams_bytes)
    return body + crc32_mpeg2(body).to_bytes(4, "big")


class TsBuilder:
    """Accumulates TS packets for one synthetic segment, tracking per-PID
    continuity counters like a real encoder would."""

    def __init__(self):
        self.packets = []
        self._cc = {}

    def _next_cc(self, pid: int) -> int:
        value = (self._cc.get(pid, -1) + 1) & 0x0F
        self._cc[pid] = value
        return value

    def add_pat(self, pmt_pid: int = 0x1000, pid: int = PAT_PID) -> "TsBuilder":
        section = build_pat_section(pmt_pid)
        self.packets.append(_ts_packet(pid, self._next_cc(pid), b"\x00" + section, True))
        return self

    def add_pmt(self, pmt_pid: int, pcr_pid: int, streams) -> "TsBuilder":
        section = build_pmt_section(pcr_pid, streams)
        self.packets.append(_ts_packet(pmt_pid, self._next_cc(pmt_pid), b"\x00" + section, True))
        return self

    def add_pes(self, pid: int, stream_id: int, pts=None, dts=None, payload=b"\x00" * 20, pcr=None) -> "TsBuilder":
        pes = build_pes(stream_id, payload, pts, dts)
        self.packets.append(_ts_packet(pid, self._next_cc(pid), pes, True, pcr=pcr))
        return self

    def add_null(self, count: int = 1) -> "TsBuilder":
        for _ in range(count):
            self.packets.append(_ts_packet(0x1FFF, 0, b"\xFF" * 184, False))
        return self

    def build(self) -> bytes:
        return b"".join(self.packets)


VIDEO_STREAM_ID = 0xE0
AUDIO_STREAM_ID = 0xC0
VIDEO_STREAM_TYPE = 0x1B  # H.264
AUDIO_STREAM_TYPE = 0x0F  # AAC


def make_segment(
    video_pid=0x0101, audio_pid=0x0102, pmt_pid=0x0200,
    video_pts=None, video_dts=None, audio_pts=None,
    pcr=None, pcr_pid=None, include_audio=True, include_pat_pmt=True,
) -> bytes:
    builder = TsBuilder()
    streams = [(VIDEO_STREAM_TYPE, video_pid)]
    if include_audio:
        streams.append((AUDIO_STREAM_TYPE, audio_pid))
    effective_pcr_pid = pcr_pid if pcr_pid is not None else video_pid
    if include_pat_pmt:
        builder.add_pat(pmt_pid=pmt_pid)
        builder.add_pmt(pmt_pid=pmt_pid, pcr_pid=effective_pcr_pid, streams=streams)
    if video_pts is not None:
        builder.add_pes(
            video_pid, VIDEO_STREAM_ID, pts=video_pts,
            dts=video_dts if video_dts is not None else video_pts,
            pcr=pcr,
        )
    if include_audio and audio_pts is not None:
        builder.add_pes(audio_pid, AUDIO_STREAM_ID, pts=audio_pts)
    return builder.build()


def packets_of(data: bytes):
    for pos in range(0, len(data) - TS_PACKET_SIZE + 1, TS_PACKET_SIZE):
        yield data[pos:pos + TS_PACKET_SIZE]


def pid_of(packet: bytes) -> int:
    return ((packet[1] & 0x1F) << 8) | packet[2]


def cc_of(packet: bytes) -> int:
    return packet[3] & 0x0F


def has_payload(packet: bytes) -> bool:
    return bool((packet[3] >> 4) & 0x01)


def payload_offset(packet: bytes) -> int:
    afc = (packet[3] >> 4) & 0x03
    offset = 4
    if afc & 0x02:
        offset += 1 + packet[4]
    return offset


def pes_starts(data: bytes):
    """(pid, pts, dts) of every PES header that fits in its first packet."""
    found = []
    for packet in packets_of(data):
        if not packet[1] & 0x40 or not has_payload(packet):
            continue
        off = payload_offset(packet)
        if off + 14 > TS_PACKET_SIZE or packet[off:off + 3] != b"\x00\x00\x01":
            continue
        flags = packet[off + 7] >> 6
        if not flags & 0x02:
            continue
        pts = read_pts_dts(packet, off + 9)
        dts = read_pts_dts(packet, off + 14) if flags == 0x03 else pts
        found.append((pid_of(packet), pts, dts))
    return found


def dts_list(data: bytes, pid: int):
    return [dts for p, _, dts in pes_starts(data) if p == pid]


def pcr_list(data: bytes):
    found = []
    for packet in packets_of(data):
        afc = (packet[3] >> 4) & 0x03
        if afc & 0x02 and packet[4] >= 7 and packet[5] & 0x10:
            found.append((pid_of(packet), read_pcr_base(packet, 6)))
    return found


def ts_after(a: int, b: int) -> bool:
    """a is strictly later than b on the 33-bit wrapping clock."""
    diff = (a - b) % TIMESTAMP_MODULO
    return 0 < diff < TIMESTAMP_MODULO // 2


def add_split_pes(builder: "TsBuilder", pid: int, stream_id: int, pts: int, dts: int = None, head: int = 10) -> None:
    """A PES whose header straddles two TS packets (only `head` bytes in the first)."""
    pes = build_pes(stream_id, b"\x00" * 20, pts, dts)
    builder.packets.append(_ts_packet(pid, builder._next_cc(pid), pes[:head], True))
    builder.packets.append(_ts_packet(pid, builder._next_cc(pid), pes[head:], False))


def make_av_segment(
    video_pid=0x0111, audio_pid=0x0112, pmt_pid=0x0150,
    video_start=900_000, audio_start=None, video_frames=60, audio_frames=94,
    video_step=3000, audio_step=1920, include_pat_pmt=True, streams=None,
) -> bytes:
    """A segment with many video and audio PES, interleaved by DTS like a real
    mux (30 fps video, 48 kHz AAC). A PCR rides on every 10th video frame."""
    if audio_start is None:
        audio_start = video_start
    builder = TsBuilder()
    if include_pat_pmt:
        builder.add_pat(pmt_pid=pmt_pid)
        builder.add_pmt(pmt_pid=pmt_pid, pcr_pid=video_pid, streams=streams or [
            (VIDEO_STREAM_TYPE, video_pid), (AUDIO_STREAM_TYPE, audio_pid),
        ])
    events = [(video_start + i * video_step, 0, i) for i in range(video_frames)]
    events += [(audio_start + j * audio_step, 1, j) for j in range(audio_frames)]
    for ts, kind, i in sorted(events):
        ts %= TIMESTAMP_MODULO
        if kind == 0:
            builder.add_pes(video_pid, VIDEO_STREAM_ID, pts=ts, dts=ts, payload=b"\x11" * 60,
                            pcr=(ts - 9000) if i % 10 == 0 else None)
            builder.packets.append(_ts_packet(video_pid, builder._next_cc(video_pid), b"\x11" * 184, False))
        else:
            builder.add_pes(audio_pid, AUDIO_STREAM_ID, pts=ts, payload=b"\x22" * 40)
    return builder.build()


ALLOWED_OUT_PIDS = {PAT_PID, OUT_PMT_PID, OUT_VIDEO_PID, OUT_AUDIO_PID}


class TsNormalizeBasicsTests(unittest.TestCase):
    def test_output_pids_are_only_expected_values(self):
        data = make_segment(video_pid=0x0245, audio_pid=0x0246, pmt_pid=0x0300,
                             video_pts=90000, audio_pts=90000)
        result = TsNormalizer().normalize(data, 6.0)
        self.assertTrue(result.normalized)
        allowed = {PAT_PID, OUT_PMT_PID, OUT_VIDEO_PID, OUT_AUDIO_PID}
        seen = {pid_of(p) for p in packets_of(result.data)}
        self.assertTrue(seen.issubset(allowed), seen)

    def test_first_two_packets_are_pat_then_pmt_with_valid_crc(self):
        data = make_segment(video_pts=90000, audio_pts=90000)
        result = TsNormalizer().normalize(data, 6.0)
        packets = list(packets_of(result.data))
        self.assertEqual(pid_of(packets[0]), PAT_PID)
        self.assertEqual(pid_of(packets[1]), OUT_PMT_PID)

        for packet in (packets[0], packets[1]):
            off = payload_offset(packet)
            pointer = packet[off]
            section = packet[off + 1 + pointer:]
            section_length = ((section[1] & 0x0F) << 8) | section[2]
            section = section[:3 + section_length]
            body, crc = section[:-4], int.from_bytes(section[-4:], "big")
            self.assertEqual(crc32_mpeg2(body), crc)

    def test_continuity_continuous_across_two_segments(self):
        normalizer = TsNormalizer()
        seg1 = make_segment(video_pts=90000, audio_pts=90000)
        seg2 = make_segment(video_pts=180000, audio_pts=180000)
        out1 = normalizer.normalize(seg1, 6.0).data
        out2 = normalizer.normalize(seg2, 6.0).data

        for pid in (OUT_VIDEO_PID, OUT_AUDIO_PID):
            last_cc = None
            for packet in packets_of(out1):
                if pid_of(packet) == pid:
                    last_cc = cc_of(packet)
            first_cc = None
            for packet in packets_of(out2):
                if pid_of(packet) == pid:
                    first_cc = cc_of(packet)
                    break
            self.assertIsNotNone(last_cc)
            self.assertIsNotNone(first_cc)
            self.assertEqual(first_cc, (last_cc + 1) & 0x0F)

    def test_continuity_continuous_across_source_switch_with_different_pids(self):
        normalizer = TsNormalizer()
        seg1 = make_segment(video_pid=0x0111, audio_pid=0x0112, pmt_pid=0x0150,
                             video_pts=90000, audio_pts=90000)
        out1 = normalizer.normalize(seg1, 6.0).data
        normalizer.start_new_epoch()
        seg2 = make_segment(video_pid=0x0221, audio_pid=0x0222, pmt_pid=0x0250,
                             video_pts=12345, audio_pts=12345)
        out2 = normalizer.normalize(seg2, 6.0).data

        for pid in (PAT_PID, OUT_PMT_PID, OUT_VIDEO_PID, OUT_AUDIO_PID):
            last_cc = None
            for packet in packets_of(out1):
                if pid_of(packet) == pid and has_payload(packet):
                    last_cc = cc_of(packet)
            first_cc = None
            for packet in packets_of(out2):
                if pid_of(packet) == pid and has_payload(packet):
                    first_cc = cc_of(packet)
                    break
            self.assertIsNotNone(last_cc, f"pid {pid:#x} missing from segment 1")
            self.assertIsNotNone(first_cc, f"pid {pid:#x} missing from segment 2")
            self.assertEqual(first_cc, (last_cc + 1) & 0x0F, f"pid {pid:#x} continuity broken")

    def test_timestamps_continue_across_new_epoch(self):
        normalizer = TsNormalizer()
        duration = 6.0
        seg1 = make_segment(video_pid=0x0111, audio_pid=0x0112, pmt_pid=0x0150,
                             video_pts=500_000, audio_pts=500_000)
        result1 = normalizer.normalize(seg1, duration)
        self.assertEqual(result1.first_dts, 500_000)

        normalizer.start_new_epoch()
        # New source with an unrelated timestamp base and different PIDs.
        seg2 = make_segment(video_pid=0x0221, audio_pid=0x0222, pmt_pid=0x0250,
                             video_pts=999_999, audio_pts=999_999)
        result2 = normalizer.normalize(seg2, duration)

        expected = (result1.first_dts + int(round(duration * CLOCK_HZ))) % TIMESTAMP_MODULO
        self.assertLessEqual(abs(result2.first_dts - expected), 1)

    def test_pcr_shifted_by_same_offset_as_timestamps(self):
        normalizer = TsNormalizer()
        duration = 2.0
        seg1 = make_segment(video_pid=0x0111, audio_pid=0x0112, pmt_pid=0x0150,
                             video_pts=90_000, audio_pts=90_000)
        result1 = normalizer.normalize(seg1, duration)

        normalizer.start_new_epoch()
        source_pcr = 300_000
        seg2 = make_segment(video_pid=0x0221, audio_pid=0x0222, pmt_pid=0x0250,
                             video_pts=300_000, audio_pts=300_000, pcr=source_pcr)
        result2 = normalizer.normalize(seg2, duration)

        offset = result2.first_dts - 300_000
        # find the PCR-bearing packet in the output and check its shifted base
        pcr_packet = None
        for packet in packets_of(result2.data):
            if pid_of(packet) != OUT_VIDEO_PID:
                continue
            afc = (packet[3] >> 4) & 0x03
            if afc & 0x02 and packet[4] >= 7 and packet[5] & 0x10:
                pcr_packet = packet
                break
        self.assertIsNotNone(pcr_packet, "no PCR-bearing packet found in output")
        actual_base = read_pcr_base(pcr_packet, 6)
        expected_base = (source_pcr + offset) % TIMESTAMP_MODULO
        self.assertEqual(actual_base, expected_base)

    def test_junk_prefix_is_stripped(self):
        junk = b"\x89PNG\r\n\x1a\n" + b"\x00" * 13  # fake PNG signature + padding
        real = make_segment(video_pts=90000, audio_pts=90000)
        data = junk + real
        self.assertEqual(find_ts_start(data), len(junk))

        plain_result = TsNormalizer().normalize(real, 6.0)
        prefixed_result = TsNormalizer().normalize(data, 6.0)
        self.assertTrue(prefixed_result.normalized)
        self.assertEqual(prefixed_result.data, plain_result.data)
        self.assertEqual(prefixed_result.first_dts, plain_result.first_dts)

    def test_segment_without_pat_pmt_reuses_known_mapping(self):
        normalizer = TsNormalizer()
        seg1 = make_segment(video_pid=0x0111, audio_pid=0x0112, pmt_pid=0x0150,
                             video_pts=90000, audio_pts=90000)
        first = normalizer.normalize(seg1, 6.0)
        self.assertTrue(first.normalized)

        # Second segment carries only PES data for the already-known PIDs, no
        # PAT/PMT at all (common for TS segments after the first in a run).
        builder = TsBuilder()
        builder.add_pes(0x0111, VIDEO_STREAM_ID, pts=96000, dts=96000)
        builder.add_pes(0x0112, AUDIO_STREAM_ID, pts=96000)
        seg2 = builder.build()

        second = normalizer.normalize(seg2, 6.0)
        self.assertTrue(second.normalized)
        seen = {pid_of(p) for p in packets_of(second.data)}
        self.assertIn(OUT_VIDEO_PID, seen)
        self.assertIn(OUT_AUDIO_PID, seen)
        # A fresh PAT/PMT is still emitted at the start of every segment.
        packets = list(packets_of(second.data))
        self.assertEqual(pid_of(packets[0]), PAT_PID)
        self.assertEqual(pid_of(packets[1]), OUT_PMT_PID)

    def test_non_ts_data_passes_through_unnormalized(self):
        data = b"this is not an mpeg-ts segment at all, just some bytes"
        result = TsNormalizer().normalize(data, 6.0)
        self.assertFalse(result.normalized)
        self.assertEqual(result.data, data)
        self.assertIsNone(result.first_dts)
        self.assertFalse(result.has_video)
        self.assertFalse(result.has_audio)

    def test_audio_less_source_reports_has_audio_false(self):
        data = make_segment(video_pts=90000, include_audio=False)
        result = TsNormalizer().normalize(data, 6.0)
        self.assertTrue(result.normalized)
        self.assertTrue(result.has_video)
        self.assertFalse(result.has_audio)
        seen = {pid_of(p) for p in packets_of(result.data)}
        self.assertNotIn(OUT_AUDIO_PID, seen)

    def test_33bit_timestamp_wraparound_handling(self):
        normalizer = TsNormalizer()
        duration = 2.0
        near_wrap = TIMESTAMP_MODULO - int(round(duration * CLOCK_HZ))
        seg1 = make_segment(video_pid=0x0111, audio_pid=0x0112, pmt_pid=0x0150,
                             video_pts=near_wrap, audio_pts=near_wrap)
        result1 = normalizer.normalize(seg1, duration)
        self.assertEqual(result1.first_dts, near_wrap)

        normalizer.start_new_epoch()
        # New source starts its own clock near zero, unrelated to the old one.
        seg2 = make_segment(video_pid=0x0221, audio_pid=0x0222, pmt_pid=0x0250,
                             video_pts=12345, audio_pts=12345)
        result2 = normalizer.normalize(seg2, duration)

        # Expected target wraps exactly to 0 (near_wrap + duration*90000 == modulo).
        self.assertLessEqual(min(result2.first_dts, TIMESTAMP_MODULO - result2.first_dts), 1)

    def test_pes_timestamp_write_wraps_modulo(self):
        # Direct sanity check that a PTS which would overflow 33 bits is
        # written back correctly modulo 2**33 (offset pushes it past the top).
        normalizer = TsNormalizer()
        seg1 = make_segment(video_pid=0x0111, audio_pid=0x0112, pmt_pid=0x0150,
                             video_pts=0, audio_pts=0)
        normalizer.normalize(seg1, 1.0)
        normalizer.start_new_epoch()
        # Force a huge positive offset by making the "previous" segment appear
        # to end far in the future relative to the new source's own clock.
        normalizer.last_segment_first_dts = TIMESTAMP_MODULO - 10
        normalizer.last_segment_duration = 1.0  # target ~= 79999 ticks past 0 => wraps
        seg2 = make_segment(video_pid=0x0221, audio_pid=0x0222, pmt_pid=0x0250,
                             video_pts=0, audio_pts=0)
        result2 = normalizer.normalize(seg2, 1.0)
        self.assertIsNotNone(result2.first_dts)
        self.assertGreaterEqual(result2.first_dts, 0)
        self.assertLess(result2.first_dts, TIMESTAMP_MODULO)

    def test_result_data_is_bytes(self):
        normalized = TsNormalizer().normalize(make_segment(video_pts=90000, audio_pts=90000), 6.0)
        self.assertIs(type(normalized.data), bytes)
        passthrough = TsNormalizer().normalize(make_segment(include_pat_pmt=False, video_pts=90000), 6.0)
        self.assertFalse(passthrough.normalized)
        self.assertIs(type(passthrough.data), bytes)

    def test_bytearray_and_memoryview_input_match_bytes_input(self):
        data = make_av_segment()
        expected = TsNormalizer().normalize(data, 2.0).data
        self.assertEqual(TsNormalizer().normalize(bytearray(data), 2.0).data, expected)
        self.assertEqual(TsNormalizer().normalize(memoryview(data), 2.0).data, expected)


class TsEpochPendingTests(unittest.TestCase):
    """P17: a new epoch is only anchored once a source timestamp is found."""

    def _two_epochs(self, normalizer):
        """Leaves the normalizer with a non-zero (soon to be stale) offset."""
        first = normalizer.normalize(make_segment(video_pid=0x0111, audio_pid=0x0112, pmt_pid=0x0150,
                                                  video_pts=500_000, audio_pts=500_000), 2.0)
        normalizer.start_new_epoch()
        second = normalizer.normalize(make_segment(video_pid=0x0221, audio_pid=0x0222, pmt_pid=0x0250,
                                                   video_pts=9_000_000, audio_pts=9_000_000), 2.0)
        self.assertEqual(second.first_dts, first.first_dts + 2 * CLOCK_HZ)
        self.assertNotEqual(normalizer.offset, 0)
        return second

    def test_segment_without_pts_keeps_epoch_pending_and_is_not_rebased(self):
        normalizer = TsNormalizer()
        previous = self._two_epochs(normalizer)
        normalizer.start_new_epoch()

        # New source whose first segment has PES packets but no PTS at all.
        builder = TsBuilder()
        builder.add_pat(pmt_pid=0x0350)
        builder.add_pmt(pmt_pid=0x0350, pcr_pid=0x0331,
                        streams=[(VIDEO_STREAM_TYPE, 0x0331), (AUDIO_STREAM_TYPE, 0x0332)])
        builder.add_pes(0x0331, VIDEO_STREAM_ID, pts=None, pcr=7_000_000)
        builder.add_pes(0x0332, AUDIO_STREAM_ID, pts=None)
        pending = normalizer.normalize(builder.build(), 2.0)

        self.assertTrue(pending.normalized)
        self.assertTrue(pending.epoch_pending)
        self.assertFalse(pending.discontinuity)
        self.assertIsNone(pending.first_dts)
        self.assertTrue(normalizer.epoch_pending, "epoch must stay pending without a timestamp")
        self.assertTrue(pending.has_video)
        self.assertTrue(pending.has_audio)
        # PIDs are remapped and PAT/PMT emitted as usual...
        packets = list(packets_of(pending.data))
        self.assertEqual(pid_of(packets[0]), PAT_PID)
        self.assertEqual(pid_of(packets[1]), OUT_PMT_PID)
        seen = {pid_of(p) for p in packets}
        self.assertTrue(seen.issubset(ALLOWED_OUT_PIDS), seen)
        self.assertIn(OUT_VIDEO_PID, seen)
        self.assertIn(OUT_AUDIO_PID, seen)
        # ...but the previous source's offset is NOT applied to the PCR.
        self.assertEqual(pcr_list(pending.data), [(OUT_VIDEO_PID, 7_000_000)])

        # The first segment with a timestamp anchors the epoch, and the clock
        # continues as if the pending segment lasted its declared duration.
        anchored = normalizer.normalize(make_segment(video_pid=0x0331, audio_pid=0x0332, pmt_pid=0x0350,
                                                     video_pts=7_200_000, audio_pts=7_200_000), 2.0)
        self.assertFalse(anchored.epoch_pending)
        self.assertFalse(normalizer.epoch_pending)
        self.assertEqual(anchored.first_dts, previous.first_dts + 2 * 2 * CLOCK_HZ)

    def test_split_pes_headers_keep_epoch_pending(self):
        normalizer = TsNormalizer()
        first = normalizer.normalize(make_segment(video_pts=500_000, audio_pts=500_000), 2.0)
        normalizer.start_new_epoch()

        builder = TsBuilder()
        builder.add_pat(pmt_pid=0x0250)
        builder.add_pmt(pmt_pid=0x0250, pcr_pid=0x0221,
                        streams=[(VIDEO_STREAM_TYPE, 0x0221), (AUDIO_STREAM_TYPE, 0x0222)])
        add_split_pes(builder, 0x0221, VIDEO_STREAM_ID, pts=3_000_000, dts=3_000_000)
        add_split_pes(builder, 0x0222, AUDIO_STREAM_ID, pts=3_000_000)
        pending = normalizer.normalize(builder.build(), 2.0)
        self.assertTrue(pending.normalized)
        self.assertTrue(pending.epoch_pending)
        self.assertTrue(normalizer.epoch_pending)

        anchored = normalizer.normalize(make_segment(video_pid=0x0221, audio_pid=0x0222, pmt_pid=0x0250,
                                                     video_pts=3_180_000, audio_pts=3_180_000), 2.0)
        self.assertFalse(normalizer.epoch_pending)
        self.assertEqual(anchored.first_dts, first.first_dts + 2 * 2 * CLOCK_HZ)

    def test_first_ever_segment_without_pts_stays_pending(self):
        normalizer = TsNormalizer()
        builder = TsBuilder()
        builder.add_pat(pmt_pid=0x0150)
        builder.add_pmt(pmt_pid=0x0150, pcr_pid=0x0111, streams=[(VIDEO_STREAM_TYPE, 0x0111)])
        builder.add_pes(0x0111, VIDEO_STREAM_ID, pts=None)
        result = normalizer.normalize(builder.build(), 2.0)
        self.assertTrue(result.normalized)
        self.assertTrue(result.epoch_pending)
        self.assertTrue(normalizer.epoch_pending)
        # The first timestamped segment keeps its own clock, as before.
        second = normalizer.normalize(make_segment(video_pts=123_456, include_audio=False), 2.0)
        self.assertEqual(second.first_dts, 123_456)
        self.assertFalse(normalizer.epoch_pending)


class TsJumpDetectionTests(unittest.TestCase):
    """P18: timestamp jumps inside an epoch start a new epoch automatically."""

    def test_forward_jump_starts_new_epoch_with_continuous_clock(self):
        normalizer = TsNormalizer()
        results = [
            normalizer.normalize(make_segment(video_pts=900_000 + k * 180_000, audio_pts=900_000 + k * 180_000), 2.0)
            for k in range(2)
        ]
        self.assertFalse(any(r.discontinuity for r in results))

        # Encoder restart on a far-away timeline, no #EXT-X-DISCONTINUITY.
        jumped = normalizer.normalize(make_segment(video_pts=50_000_000, audio_pts=50_000_000,
                                                   pcr=50_000_000 - 9000), 2.0)
        self.assertTrue(jumped.discontinuity)
        self.assertTrue(jumped.normalized)
        self.assertFalse(jumped.epoch_pending)
        self.assertEqual(jumped.first_dts, results[-1].first_dts + 180_000)
        # PCR moves by the same new offset as PTS/DTS.
        self.assertEqual(pcr_list(jumped.data), [(OUT_VIDEO_PID, jumped.first_dts - 9000)])

        after = normalizer.normalize(make_segment(video_pts=50_180_000, audio_pts=50_180_000), 2.0)
        self.assertFalse(after.discontinuity)
        self.assertEqual(after.first_dts, jumped.first_dts + 180_000)

    def test_backward_jump_is_detected(self):
        normalizer = TsNormalizer()
        first = normalizer.normalize(make_segment(video_pts=40_000_000, audio_pts=40_000_000), 2.0)
        restarted = normalizer.normalize(make_segment(video_pts=126_000, audio_pts=126_000), 2.0)
        self.assertTrue(restarted.discontinuity)
        self.assertEqual(restarted.first_dts, first.first_dts + 180_000)

    def test_jump_with_real_mux_keeps_every_stream_monotonic(self):
        normalizer = TsNormalizer()
        outputs = [normalizer.normalize(make_av_segment(video_start=900_000), 2.0).data]
        jumped = normalizer.normalize(make_av_segment(video_start=80_000_000, audio_start=80_000_000 - 20_000), 2.0)
        self.assertTrue(jumped.discontinuity)
        outputs.append(jumped.data)
        outputs.append(normalizer.normalize(
            make_av_segment(video_start=80_180_000, audio_start=80_180_000 - 20_000), 2.0).data)
        for pid in (OUT_VIDEO_PID, OUT_AUDIO_PID):
            values = [dts for out in outputs for dts in dts_list(out, pid)]
            for earlier, later in zip(values, values[1:]):
                self.assertTrue(ts_after(later, earlier), f"pid {pid:#x}: {later} not after {earlier}")
        # No gap bigger than a couple of frames at the splice either.
        audio = [dts for out in outputs[:2] for dts in dts_list(out, OUT_AUDIO_PID)]
        self.assertLess(max(b - a for a, b in zip(audio, audio[1:])), 3 * 1920)

    def test_drift_below_threshold_is_not_a_jump(self):
        normalizer = TsNormalizer()
        normalizer.normalize(make_segment(video_pts=900_000, audio_pts=900_000), 2.0)
        offset = normalizer.offset
        # 5 s later than expected: below the 10 s floor.
        drifted = normalizer.normalize(make_segment(video_pts=900_000 + 7 * CLOCK_HZ, audio_pts=900_000 + 7 * CLOCK_HZ), 2.0)
        self.assertFalse(drifted.discontinuity)
        self.assertEqual(normalizer.offset, offset)
        self.assertEqual(drifted.first_dts, 900_000 + 7 * CLOCK_HZ)

    def test_threshold_scales_with_segment_duration(self):
        normalizer = TsNormalizer()
        normalizer.normalize(make_segment(video_pts=900_000, audio_pts=900_000), 10.0)
        # 20 s off with 10 s segments: below 3 x 10 s.
        result = normalizer.normalize(make_segment(video_pts=900_000 + 30 * CLOCK_HZ, audio_pts=900_000 + 30 * CLOCK_HZ), 10.0)
        self.assertFalse(result.discontinuity)
        # 40 s off: above it.
        result = normalizer.normalize(make_segment(video_pts=900_000 + 80 * CLOCK_HZ, audio_pts=900_000 + 80 * CLOCK_HZ), 10.0)
        self.assertTrue(result.discontinuity)

    def test_threshold_is_configurable_and_can_be_disabled(self):
        sensitive = TsNormalizer(jump_threshold=2 * CLOCK_HZ, jump_threshold_segments=0.0)
        sensitive.normalize(make_segment(video_pts=900_000, audio_pts=900_000), 2.0)
        result = sensitive.normalize(make_segment(video_pts=900_000 + 7 * CLOCK_HZ, audio_pts=900_000 + 7 * CLOCK_HZ), 2.0)
        self.assertTrue(result.discontinuity)
        self.assertEqual(result.first_dts, 900_000 + 2 * CLOCK_HZ)

        disabled = TsNormalizer(jump_threshold=None)
        disabled.normalize(make_segment(video_pts=900_000, audio_pts=900_000), 2.0)
        result = disabled.normalize(make_segment(video_pts=50_000_000, audio_pts=50_000_000), 2.0)
        self.assertFalse(result.discontinuity)
        self.assertEqual(result.first_dts, 50_000_000)

    def test_33bit_wrap_inside_epoch_is_not_a_jump(self):
        normalizer = TsNormalizer()
        near_wrap = TIMESTAMP_MODULO - 90_000
        first = normalizer.normalize(make_segment(video_pts=near_wrap, audio_pts=near_wrap), 2.0)
        wrapped = normalizer.normalize(make_segment(video_pts=90_000, audio_pts=90_000), 2.0)
        self.assertFalse(wrapped.discontinuity)
        self.assertEqual(normalizer.offset, 0)
        self.assertEqual(wrapped.first_dts, (first.first_dts + 180_000) % TIMESTAMP_MODULO)

    def test_33bit_wrap_of_rebased_source_is_not_a_jump(self):
        normalizer = TsNormalizer()
        normalizer.normalize(make_segment(video_pid=0x0111, audio_pid=0x0112, pmt_pid=0x0150,
                                          video_pts=500_000, audio_pts=500_000), 2.0)
        normalizer.start_new_epoch()
        near_wrap = TIMESTAMP_MODULO - 90_000
        rebased = normalizer.normalize(make_segment(video_pid=0x0221, audio_pid=0x0222, pmt_pid=0x0250,
                                                    video_pts=near_wrap, audio_pts=near_wrap), 2.0)
        self.assertEqual(rebased.first_dts, 680_000)
        offset = normalizer.offset
        # The new source's own clock wraps past 2**33 between segments.
        wrapped = normalizer.normalize(make_segment(video_pid=0x0221, audio_pid=0x0222, pmt_pid=0x0250,
                                                    video_pts=90_000, audio_pts=90_000), 2.0)
        self.assertFalse(wrapped.discontinuity)
        self.assertEqual(normalizer.offset, offset)
        self.assertEqual(wrapped.first_dts, 860_000)

    def test_caller_started_epoch_is_not_flagged(self):
        normalizer = TsNormalizer()
        normalizer.normalize(make_segment(video_pts=500_000, audio_pts=500_000), 2.0)
        normalizer.start_new_epoch()
        result = normalizer.normalize(make_segment(video_pts=70_000_000, audio_pts=70_000_000), 2.0)
        self.assertFalse(result.discontinuity)
        self.assertEqual(result.first_dts, 680_000)


class TsProgramReuseTests(unittest.TestCase):
    """P19: PAT/PMT-less segments after a new epoch, and the pass-through path."""

    def _first_epoch(self, normalizer):
        return normalizer.normalize(make_segment(video_pid=0x0111, audio_pid=0x0112, pmt_pid=0x0150,
                                                 video_pts=90_000, audio_pts=90_000), 2.0)

    def test_psi_less_segment_after_new_epoch_reuses_matching_map(self):
        normalizer = TsNormalizer()
        first = self._first_epoch(normalizer)
        normalizer.start_new_epoch()  # e.g. an upstream #EXT-X-DISCONTINUITY

        seg = make_segment(video_pid=0x0111, audio_pid=0x0112, pmt_pid=0x0150,
                           video_pts=5_000_000, audio_pts=5_000_000, include_pat_pmt=False)
        result = normalizer.normalize(seg, 2.0)
        self.assertTrue(result.normalized)
        self.assertTrue(result.has_video)
        self.assertTrue(result.has_audio)
        packets = list(packets_of(result.data))
        self.assertEqual(pid_of(packets[0]), PAT_PID)
        self.assertEqual(pid_of(packets[1]), OUT_PMT_PID)
        self.assertTrue({pid_of(p) for p in packets}.issubset(ALLOWED_OUT_PIDS))
        # Re-based onto the output clock, not published on the source clock.
        self.assertEqual(result.first_dts, first.first_dts + 2 * CLOCK_HZ)
        self.assertEqual(dts_list(result.data, OUT_AUDIO_PID), [first.first_dts + 2 * CLOCK_HZ])
        # Continuity carries on from the previous segment.
        for pid in (OUT_VIDEO_PID, OUT_AUDIO_PID):
            last = [cc_of(p) for p in packets_of(first.data) if pid_of(p) == pid][-1]
            nxt = [cc_of(p) for p in packets if pid_of(p) == pid][0]
            self.assertEqual(nxt, (last + 1) & 0x0F)

    def test_psi_less_segment_with_foreign_pids_passes_through_unmodified(self):
        normalizer = TsNormalizer()
        first = self._first_epoch(normalizer)
        normalizer.start_new_epoch()
        continuity_before = dict(normalizer.continuity)

        seg = make_segment(video_pid=0x0221, audio_pid=0x0222, video_pts=5_000_000, audio_pts=5_000_000,
                           include_pat_pmt=False)
        result = normalizer.normalize(seg, 2.0)
        self.assertFalse(result.normalized)
        self.assertEqual(result.data, seg)
        self.assertIsNone(result.first_dts)
        self.assertEqual(result.codec_signature, (None, None))
        # Output state is untouched and the epoch is still waiting.
        self.assertEqual(normalizer.continuity, continuity_before)
        self.assertTrue(normalizer.epoch_pending)

        # Once the new source's PMT shows up everything continues cleanly.
        seg2 = make_segment(video_pid=0x0221, audio_pid=0x0222, pmt_pid=0x0250,
                            video_pts=5_180_000, audio_pts=5_180_000)
        second = normalizer.normalize(seg2, 2.0)
        self.assertTrue(second.normalized)
        self.assertEqual(second.first_dts, first.first_dts + 2 * 2 * CLOCK_HZ)
        for pid in (OUT_VIDEO_PID, OUT_AUDIO_PID):
            last = [cc_of(p) for p in packets_of(first.data) if pid_of(p) == pid][-1]
            nxt = [cc_of(p) for p in packets_of(second.data) if pid_of(p) == pid][0]
            self.assertEqual(nxt, (last + 1) & 0x0F)

    def test_psi_less_segment_with_extra_unknown_pes_pid_is_not_reused(self):
        normalizer = TsNormalizer()
        self._first_epoch(normalizer)
        normalizer.start_new_epoch()
        builder = TsBuilder()
        builder.add_pes(0x0111, VIDEO_STREAM_ID, pts=5_000_000, dts=5_000_000)
        builder.add_pes(0x0333, AUDIO_STREAM_ID, pts=5_000_000)  # not in the old PMT
        result = normalizer.normalize(builder.build(), 2.0)
        self.assertFalse(result.normalized)

    def test_psi_less_segment_before_any_program_passes_through(self):
        seg = make_segment(video_pts=90_000, audio_pts=90_000, include_pat_pmt=False)
        result = TsNormalizer().normalize(seg, 2.0)
        self.assertFalse(result.normalized)
        self.assertEqual(result.data, seg)
        self.assertEqual(result.codec_signature, (None, None))


class TsResyncTests(unittest.TestCase):
    """P20(a): sync loss mid-segment recovers instead of dropping the rest."""

    def _packets(self):
        builder = TsBuilder()
        builder.add_pat(pmt_pid=0x0150)
        builder.add_pmt(pmt_pid=0x0150, pcr_pid=0x0111,
                        streams=[(VIDEO_STREAM_TYPE, 0x0111), (AUDIO_STREAM_TYPE, 0x0112)])
        for i in range(6):
            ts = 900_000 + i * 3000
            builder.add_pes(0x0111, VIDEO_STREAM_ID, pts=ts, dts=ts, payload=b"\x11" * 100)
            for _ in range(3):
                builder.packets.append(_ts_packet(0x0111, builder._next_cc(0x0111), b"\x11" * 184, False))
            builder.add_pes(0x0112, AUDIO_STREAM_ID, pts=ts, payload=b"\x22" * 50)
        return builder.packets

    def test_garbage_between_packets_is_skipped(self):
        packets = self._packets()
        clean = b"".join(packets)
        garbage = b"\x00\x47\x13\x10" + b"\xAA" * 40 + b"\x47" + b"\x55" * 12  # stray, unaligned 0x47s
        dirty = b"".join(packets[:4]) + garbage + b"".join(packets[4:])
        expected = TsNormalizer().normalize(clean, 2.0)
        result = TsNormalizer().normalize(dirty, 2.0)
        self.assertTrue(result.normalized)
        self.assertEqual(result.data, expected.data)
        self.assertEqual(len(dts_list(result.data, OUT_VIDEO_PID)), 6)
        self.assertEqual(len(dts_list(result.data, OUT_AUDIO_PID)), 6)

    def test_truncated_packet_is_dropped_and_stream_continues(self):
        packets = self._packets()
        truncated_index = 7  # a video continuation packet
        dirty = b"".join(packets[:truncated_index]) + packets[truncated_index][:100] + b"".join(packets[truncated_index + 1:])
        without = b"".join(packets[:truncated_index] + packets[truncated_index + 1:])
        expected = TsNormalizer().normalize(without, 2.0)
        result = TsNormalizer().normalize(dirty, 2.0)
        self.assertTrue(result.normalized)
        self.assertEqual(result.data, expected.data)
        for packet in packets_of(result.data):
            self.assertEqual(packet[0], SYNC_BYTE)

    def test_garbage_at_the_end_is_dropped(self):
        packets = self._packets()
        clean = b"".join(packets)
        expected = TsNormalizer().normalize(clean, 2.0)
        result = TsNormalizer().normalize(clean + b"\x47" + b"\x99" * 400, 2.0)
        self.assertEqual(result.data, expected.data)

    def test_resync_needs_three_aligned_sync_bytes(self):
        packets = self._packets()
        # A fake sync byte with a second one 188 bytes later but not a third.
        fake = bytearray(b"\x33" * 400)
        fake[10] = SYNC_BYTE
        fake[10 + TS_PACKET_SIZE] = SYNC_BYTE
        dirty = b"".join(packets[:4]) + bytes(fake) + b"".join(packets[4:])
        expected = TsNormalizer().normalize(b"".join(packets), 2.0)
        self.assertEqual(TsNormalizer().normalize(dirty, 2.0).data, expected.data)


class TsEpochAnchorTests(unittest.TestCase):
    """P20(b): new epochs anchor on the earlier of first video/audio DTS."""

    def _switch(self, audio_lead):
        normalizer = TsNormalizer()
        first = normalizer.normalize(make_av_segment(video_pid=0x0111, audio_pid=0x0112, pmt_pid=0x0150,
                                                     video_start=900_000), 2.0)
        normalizer.start_new_epoch()
        second = normalizer.normalize(make_av_segment(video_pid=0x0221, audio_pid=0x0222, pmt_pid=0x0250,
                                                      video_start=5_000_000, audio_start=5_000_000 - audio_lead), 2.0)
        third = normalizer.normalize(make_av_segment(video_pid=0x0221, audio_pid=0x0222, pmt_pid=0x0250,
                                                     video_start=5_180_000, audio_start=5_180_000 - audio_lead), 2.0)
        return normalizer, [first, second, third]

    def test_audio_leading_video_does_not_overlap_previous_source(self):
        lead = 45_000  # audio starts 0.5 s before video in the new source
        normalizer, results = self._switch(lead)
        first, second, third = results
        self.assertFalse(second.discontinuity)
        self.assertFalse(third.discontinuity)
        prev_audio_end = dts_list(first.data, OUT_AUDIO_PID)[-1] + 1920
        prev_video_end = dts_list(first.data, OUT_VIDEO_PID)[-1] + 3000
        new_audio = dts_list(second.data, OUT_AUDIO_PID)
        new_video = dts_list(second.data, OUT_VIDEO_PID)
        # The earliest new timestamp (audio) lands right where the old content
        # ended, and video keeps its offset to audio.
        self.assertEqual(new_audio[0], max(prev_audio_end, prev_video_end))
        self.assertEqual(new_video[0], new_audio[0] + lead)
        self.assertEqual(second.first_dts, new_video[0])
        # Every stream stays strictly monotonic across the splice and after it.
        for pid in (OUT_VIDEO_PID, OUT_AUDIO_PID):
            values = [dts for r in results for dts in dts_list(r.data, pid)]
            for earlier, later in zip(values, values[1:]):
                self.assertTrue(ts_after(later, earlier), f"pid {pid:#x}: {later} not after {earlier}")
        # ...and continuous: the third segment follows the second exactly.
        self.assertEqual(third.first_dts, second.first_dts + 180_000)

    def test_video_anchored_when_audio_lags(self):
        normalizer, (first, second, _) = self._switch(-30_000)  # audio 1/3 s after video
        new_video = dts_list(second.data, OUT_VIDEO_PID)
        prev_end = max(dts_list(first.data, OUT_VIDEO_PID)[-1] + 3000, dts_list(first.data, OUT_AUDIO_PID)[-1] + 1920)
        self.assertEqual(new_video[0], prev_end)
        self.assertEqual(dts_list(second.data, OUT_AUDIO_PID)[0], new_video[0] + 30_000)

    def test_audio_far_from_video_is_not_used_as_anchor(self):
        normalizer, (first, second, _) = self._switch(30 * CLOCK_HZ)
        new_video = dts_list(second.data, OUT_VIDEO_PID)
        prev_end = max(dts_list(first.data, OUT_VIDEO_PID)[-1] + 3000, dts_list(first.data, OUT_AUDIO_PID)[-1] + 1920)
        self.assertEqual(new_video[0], prev_end)

    def test_new_epoch_starts_after_content_that_overran_extinf(self):
        normalizer = TsNormalizer()
        # EXTINF claims 1 s but the segment really carries 2 s of media.
        first = normalizer.normalize(make_av_segment(video_start=900_000), 1.0)
        normalizer.start_new_epoch()
        second = normalizer.normalize(make_av_segment(video_pid=0x0221, audio_pid=0x0222, pmt_pid=0x0250,
                                                      video_start=3_000_000), 2.0)
        for pid in (OUT_VIDEO_PID, OUT_AUDIO_PID):
            self.assertTrue(ts_after(dts_list(second.data, pid)[0], dts_list(first.data, pid)[-1]))


class TsAudioLanguageTests(unittest.TestCase):
    """P20(c): preferred_audio_language picks the audio stream from the PMT."""

    def _segment(self, audio_streams):
        """audio_streams: list of (pid, descriptors); each gets a distinct PTS."""
        builder = TsBuilder()
        streams = [(VIDEO_STREAM_TYPE, 0x0111)] + [(AUDIO_STREAM_TYPE, pid, desc) for pid, desc in audio_streams]
        builder.add_pat(pmt_pid=0x0150)
        builder.add_pmt(pmt_pid=0x0150, pcr_pid=0x0111, streams=streams)
        builder.add_pes(0x0111, VIDEO_STREAM_ID, pts=900_000, dts=900_000)
        for pid, _ in audio_streams:
            builder.add_pes(pid, AUDIO_STREAM_ID, pts=900_000 + pid)  # PTS identifies the source stream
        return builder.build()

    def _picked(self, result):
        audio = dts_list(result.data, OUT_AUDIO_PID)
        self.assertEqual(len(audio), 1, "exactly one audio stream must be mapped")
        return audio[0] - 900_000

    def _output_audio_descriptors(self, result):
        packet = list(packets_of(result.data))[1]
        off = payload_offset(packet)
        section = packet[off + 1 + packet[off]:]
        program_info_length = ((section[10] & 0x0F) << 8) | section[11]
        pos = 12 + program_info_length
        while True:
            pid = ((section[pos + 1] & 0x1F) << 8) | section[pos + 2]
            length = ((section[pos + 3] & 0x0F) << 8) | section[pos + 4]
            if pid == OUT_AUDIO_PID:
                return section[pos + 5:pos + 5 + length]
            pos += 5 + length

    def test_default_is_first_audio_stream(self):
        seg = self._segment([(0x0112, iso639_descriptor("spa")), (0x0113, iso639_descriptor("eng"))])
        self.assertEqual(self._picked(TsNormalizer().normalize(seg, 2.0)), 0x0112)

    def test_preferred_language_is_selected(self):
        seg = self._segment([(0x0112, iso639_descriptor("spa")), (0x0113, iso639_descriptor("eng"))])
        result = TsNormalizer(preferred_audio_language="eng").normalize(seg, 2.0)
        self.assertEqual(self._picked(result), 0x0113)
        self.assertTrue(result.has_audio)
        # The output PMT describes the chosen stream's language.
        self.assertEqual(self._output_audio_descriptors(result), iso639_descriptor("eng"))

    def test_preferred_language_is_case_insensitive_and_reads_multi_entry_descriptors(self):
        seg = self._segment([(0x0112, iso639_descriptor("spa")), (0x0113, iso639_descriptor("fra", "ENG"))])
        self.assertEqual(self._picked(TsNormalizer(preferred_audio_language="Eng").normalize(seg, 2.0)), 0x0113)

    def test_missing_language_falls_back_to_first_audio(self):
        seg = self._segment([(0x0112, iso639_descriptor("spa")), (0x0113, iso639_descriptor("eng"))])
        self.assertEqual(self._picked(TsNormalizer(preferred_audio_language="deu").normalize(seg, 2.0)), 0x0112)
        seg = self._segment([(0x0112, b""), (0x0113, b"")])
        self.assertEqual(self._picked(TsNormalizer(preferred_audio_language="eng").normalize(seg, 2.0)), 0x0112)

    def test_main_track_preferred_over_audio_description(self):
        seg = self._segment([
            (0x0112, iso639_descriptor(("eng", 0x03))),  # visual impaired commentary
            (0x0113, iso639_descriptor(("eng", 0x00))),
        ])
        self.assertEqual(self._picked(TsNormalizer(preferred_audio_language="eng").normalize(seg, 2.0)), 0x0113)

    def test_bibliographic_and_terminology_codes_match(self):
        seg = self._segment([(0x0112, iso639_descriptor("eng")), (0x0113, iso639_descriptor("deu"))])
        self.assertEqual(self._picked(TsNormalizer(preferred_audio_language="ger").normalize(seg, 2.0)), 0x0113)

    def test_language_choice_survives_psi_less_segments(self):
        normalizer = TsNormalizer(preferred_audio_language="eng")
        seg = self._segment([(0x0112, iso639_descriptor("spa")), (0x0113, iso639_descriptor("eng"))])
        normalizer.normalize(seg, 2.0)
        builder = TsBuilder()
        builder.add_pes(0x0111, VIDEO_STREAM_ID, pts=1_080_000, dts=1_080_000)
        builder.add_pes(0x0112, AUDIO_STREAM_ID, pts=1_080_000 + 0x0112)
        builder.add_pes(0x0113, AUDIO_STREAM_ID, pts=1_080_000 + 0x0113)
        result = normalizer.normalize(builder.build(), 2.0)
        self.assertEqual(dts_list(result.data, OUT_AUDIO_PID), [1_080_000 + 0x0113])


if __name__ == "__main__":
    unittest.main()
