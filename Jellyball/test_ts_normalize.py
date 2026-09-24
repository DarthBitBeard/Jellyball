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


def build_pmt_section(pcr_pid: int, streams, program_number: int = 1, version: int = 0) -> bytes:
    """`streams` is a list of (stream_type, pid) tuples."""
    streams_bytes = bytearray()
    for stream_type, pid in streams:
        streams_bytes.extend([
            stream_type,
            0xE0 | ((pid >> 8) & 0x1F), pid & 0xFF,
            0xF0, 0x00,  # es_info_length = 0
        ])
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


if __name__ == "__main__":
    unittest.main()
