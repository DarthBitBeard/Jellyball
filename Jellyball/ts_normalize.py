"""MPEG-TS normalizer that makes segments from different sources splice cleanly.

Why this exists: when a channel fails over to another provider, the proxy keeps
serving one continuous HLS playlist to Jellyfin. Jellyfin's ffmpeg reads that
playlist with its hls demuxer, which feeds every segment into a *single* inner
MPEG-TS demuxer and ignores #EXT-X-DISCONTINUITY. So a new source that:

  * uses different PIDs -> shows up as brand-new streams mid-file, which the
    ffmpeg command line drops, while the streams it was decoding go silent
    (playback freezes);
  * restarts continuity counters -> "continuity check failed" / corrupt packets;
  * has an unrelated timestamp base -> timestamps jump by hours.

`TsNormalizer` rewrites each segment so every source looks like one stream:
the first video and first audio elementary streams are remapped to fixed PIDs,
a fresh PAT/PMT is written at the start of every segment, continuity counters
continue across segments, and PTS/DTS/PCR are shifted by a per-epoch offset so
time carries on smoothly across a source switch (or an upstream discontinuity).

Segments must be fed in playback order; the channel session guarantees that.
Everything here is pure computation on bytes (no I/O), so it is unit testable
and cheap to run in a worker thread.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

TS_PACKET_SIZE = 188
SYNC_BYTE = 0x47
PAT_PID = 0x0000
NULL_PID = 0x1FFF

OUT_PMT_PID = 0x1000
OUT_VIDEO_PID = 0x0100
OUT_AUDIO_PID = 0x0101
OUT_PCR_PID = 0x0102  # only used when a source carries PCR on a dedicated PID
OUT_PROGRAM_NUMBER = 1
OUT_TRANSPORT_STREAM_ID = 1

TIMESTAMP_MODULO = 1 << 33
CLOCK_HZ = 90_000

VIDEO_STREAM_TYPES = {0x01, 0x02, 0x10, 0x1B, 0x20, 0x24, 0x42, 0xD1, 0xEA}
AUDIO_STREAM_TYPES = {0x03, 0x04, 0x0F, 0x11, 0x1C, 0x81, 0x82, 0x83, 0x84, 0x87}
# stream_type 0x06 is "private PES"; DVB carries AC-3/E-AC-3/AAC in it, flagged
# by a descriptor.
PRIVATE_PES_AUDIO_DESCRIPTORS = {0x6A, 0x7A, 0x7C, 0x81}
# PES stream_ids that have no optional header (and therefore no PTS/DTS).
_PES_NO_HEADER_STREAM_IDS = {0xBC, 0xBE, 0xBF, 0xF0, 0xF1, 0xF2, 0xF8, 0xFF}


def _build_crc32_table() -> List[int]:
    table = []
    for byte in range(256):
        crc = byte << 24
        for _ in range(8):
            crc = ((crc << 1) ^ 0x04C11DB7) if crc & 0x80000000 else (crc << 1)
            crc &= 0xFFFFFFFF
        table.append(crc)
    return table


_CRC32_TABLE = _build_crc32_table()


def crc32_mpeg2(data: bytes) -> int:
    """CRC-32/MPEG-2 (not zlib's reflected CRC-32) as used by PSI sections."""
    crc = 0xFFFFFFFF
    for byte in data:
        crc = ((crc << 8) & 0xFFFFFFFF) ^ _CRC32_TABLE[((crc >> 24) ^ byte) & 0xFF]
    return crc


def find_ts_start(data: bytes, probe_packets: int = 3) -> int:
    """Offset of the first TS packet, or -1.

    Some providers disguise segments as images by prepending a PNG/JPEG header
    to real MPEG-TS; skipping to the first run of aligned sync bytes recovers
    the stream instead of rejecting it."""
    limit = min(len(data), 64 * 1024)
    for offset in range(limit):
        if data[offset] != SYNC_BYTE:
            continue
        needed = min(probe_packets, (len(data) - offset) // TS_PACKET_SIZE)
        if needed < 1:
            return -1
        if all(data[offset + k * TS_PACKET_SIZE] == SYNC_BYTE for k in range(needed)):
            return offset
    return -1


def looks_like_ts(data: bytes) -> bool:
    return find_ts_start(data) >= 0


def _read_timestamp(buf, i: int) -> int:
    return (
        ((buf[i] >> 1) & 0x07) << 30
        | buf[i + 1] << 22
        | (buf[i + 2] >> 1) << 15
        | buf[i + 3] << 7
        | (buf[i + 4] >> 1)
    )


def _write_timestamp(buf: bytearray, i: int, value: int) -> None:
    prefix = buf[i] & 0xF0
    value %= TIMESTAMP_MODULO
    buf[i] = prefix | (((value >> 30) & 0x07) << 1) | 0x01
    buf[i + 1] = (value >> 22) & 0xFF
    buf[i + 2] = (((value >> 15) & 0x7F) << 1) | 0x01
    buf[i + 3] = (value >> 7) & 0xFF
    buf[i + 4] = ((value & 0x7F) << 1) | 0x01


def _read_pcr_base(buf, i: int) -> int:
    return buf[i] << 25 | buf[i + 1] << 17 | buf[i + 2] << 9 | buf[i + 3] << 1 | buf[i + 4] >> 7


def _write_pcr_base(buf: bytearray, i: int, value: int) -> None:
    value %= TIMESTAMP_MODULO
    buf[i] = (value >> 25) & 0xFF
    buf[i + 1] = (value >> 17) & 0xFF
    buf[i + 2] = (value >> 9) & 0xFF
    buf[i + 3] = (value >> 1) & 0xFF
    buf[i + 4] = ((value & 0x01) << 7) | 0x7E | (buf[i + 4] & 0x01)


def _ts_diff(a: int, b: int) -> int:
    """Signed a-b on the 33-bit wrapping clock."""
    diff = (a - b) % TIMESTAMP_MODULO
    if diff >= TIMESTAMP_MODULO // 2:
        diff -= TIMESTAMP_MODULO
    return diff


@dataclass
class ElementaryStream:
    pid: int
    stream_type: int
    descriptors: bytes = b""


@dataclass
class ProgramMap:
    pmt_pid: int
    pcr_pid: int
    program_info: bytes = b""
    video: Optional[ElementaryStream] = None
    audio: Optional[ElementaryStream] = None

    def codec_signature(self) -> Tuple[Optional[int], Optional[int]]:
        return (
            self.video.stream_type if self.video else None,
            self.audio.stream_type if self.audio else None,
        )


class _SectionAssembler:
    """Collects a PSI section that may span several TS packets."""

    def __init__(self) -> None:
        self.buffer = bytearray()
        self.active = False

    def feed(self, payload: bytes, unit_start: bool) -> Optional[bytes]:
        if unit_start:
            if not payload:
                return None
            pointer = payload[0]
            self.buffer = bytearray(payload[1 + pointer:])
            self.active = True
        elif self.active:
            self.buffer.extend(payload)
        else:
            return None
        if len(self.buffer) < 3:
            return None
        section_length = ((self.buffer[1] & 0x0F) << 8) | self.buffer[2]
        total = 3 + section_length
        if len(self.buffer) < total:
            return None
        section = bytes(self.buffer[:total])
        self.active = False
        self.buffer = bytearray()
        return section


def _parse_pat(section: bytes) -> Optional[int]:
    """Return the PMT PID of the first non-NIT program."""
    if len(section) < 12 or section[0] != 0x00:
        return None
    section_length = ((section[1] & 0x0F) << 8) | section[2]
    end = min(len(section), 3 + section_length) - 4  # drop CRC
    pos = 8
    while pos + 4 <= end:
        program_number = (section[pos] << 8) | section[pos + 1]
        pid = ((section[pos + 2] & 0x1F) << 8) | section[pos + 3]
        if program_number != 0:
            return pid
        pos += 4
    return None


def _is_audio_stream(stream_type: int, descriptors: bytes) -> bool:
    if stream_type in AUDIO_STREAM_TYPES:
        return True
    if stream_type == 0x06:
        pos = 0
        while pos + 2 <= len(descriptors):
            tag, length = descriptors[pos], descriptors[pos + 1]
            if tag in PRIVATE_PES_AUDIO_DESCRIPTORS:
                return True
            pos += 2 + length
    return False


def _parse_pmt(section: bytes, pmt_pid: int) -> Optional[ProgramMap]:
    if len(section) < 16 or section[0] != 0x02:
        return None
    section_length = ((section[1] & 0x0F) << 8) | section[2]
    end = min(len(section), 3 + section_length) - 4
    pcr_pid = ((section[8] & 0x1F) << 8) | section[9]
    program_info_length = ((section[10] & 0x0F) << 8) | section[11]
    program_info = bytes(section[12:12 + program_info_length])
    pos = 12 + program_info_length
    program = ProgramMap(pmt_pid=pmt_pid, pcr_pid=pcr_pid, program_info=program_info)
    while pos + 5 <= end:
        stream_type = section[pos]
        pid = ((section[pos + 1] & 0x1F) << 8) | section[pos + 2]
        es_info_length = ((section[pos + 3] & 0x0F) << 8) | section[pos + 4]
        descriptors = bytes(section[pos + 5:pos + 5 + es_info_length])
        if program.video is None and stream_type in VIDEO_STREAM_TYPES:
            program.video = ElementaryStream(pid, stream_type, descriptors)
        elif program.audio is None and _is_audio_stream(stream_type, descriptors):
            program.audio = ElementaryStream(pid, stream_type, descriptors)
        pos += 5 + es_info_length
    return program


def _psi_packets(pid: int, section: bytes, cc: int) -> Tuple[List[bytes], int]:
    """Packetize a PSI section; returns (packets, next continuity counter)."""
    payload = b"\x00" + section  # pointer_field = 0
    packets = []
    first = True
    while payload:
        chunk, payload = payload[:184], payload[184:]
        header = bytes([
            SYNC_BYTE,
            (0x40 if first else 0x00) | ((pid >> 8) & 0x1F),
            pid & 0xFF,
            0x10 | (cc & 0x0F),
        ])
        packets.append(header + chunk + b"\xff" * (184 - len(chunk)))
        cc = (cc + 1) & 0x0F
        first = False
    return packets, cc


def _build_pat_section() -> bytes:
    body = bytes([
        0x00,  # table_id
        0xB0, 0x0D,  # section_syntax_indicator=1, length=13
        (OUT_TRANSPORT_STREAM_ID >> 8) & 0xFF, OUT_TRANSPORT_STREAM_ID & 0xFF,
        0xC1,  # version 0, current_next=1
        0x00, 0x00,  # section / last section number
        (OUT_PROGRAM_NUMBER >> 8) & 0xFF, OUT_PROGRAM_NUMBER & 0xFF,
        0xE0 | ((OUT_PMT_PID >> 8) & 0x1F), OUT_PMT_PID & 0xFF,
    ])
    return body + crc32_mpeg2(body).to_bytes(4, "big")


def _build_pmt_section(program: ProgramMap, pcr_pid: int, version: int) -> bytes:
    streams = bytearray()
    for stream, out_pid in ((program.video, OUT_VIDEO_PID), (program.audio, OUT_AUDIO_PID)):
        if stream is None:
            continue
        streams.extend([
            stream.stream_type,
            0xE0 | ((out_pid >> 8) & 0x1F), out_pid & 0xFF,
            0xF0 | ((len(stream.descriptors) >> 8) & 0x0F), len(stream.descriptors) & 0xFF,
        ])
        streams.extend(stream.descriptors)
    program_info = program.program_info
    section_length = 9 + len(program_info) + len(streams) + 4
    body = bytes([
        0x02,
        0xB0 | ((section_length >> 8) & 0x0F), section_length & 0xFF,
        (OUT_PROGRAM_NUMBER >> 8) & 0xFF, OUT_PROGRAM_NUMBER & 0xFF,
        0xC1 | ((version & 0x1F) << 1),
        0x00, 0x00,
        0xE0 | ((pcr_pid >> 8) & 0x1F), pcr_pid & 0xFF,
        0xF0 | ((len(program_info) >> 8) & 0x0F), len(program_info) & 0xFF,
    ]) + program_info + bytes(streams)
    return body + crc32_mpeg2(body).to_bytes(4, "big")


@dataclass
class NormalizeResult:
    data: bytes
    first_dts: Optional[int]  # output clock, first video (else audio) PES in segment
    has_video: bool
    has_audio: bool
    codec_signature: Tuple[Optional[int], Optional[int]]
    normalized: bool  # False when the segment could not be parsed and was passed through


@dataclass
class TsNormalizer:
    """Stateful per-channel normalizer. Feed segments strictly in playback order."""

    program: Optional[ProgramMap] = None
    offset: int = 0
    epoch_pending: bool = True
    pmt_version: int = 0
    last_signature: Optional[Tuple[Optional[int], Optional[int]]] = None
    # Output timeline bookkeeping used to place the next epoch.
    last_segment_first_dts: Optional[int] = None
    last_segment_duration: float = 0.0
    continuity: Dict[int, int] = field(default_factory=dict)

    def start_new_epoch(self) -> None:
        """Call before the first segment of a new source (or after an upstream
        discontinuity): the next segment's timestamps get re-based so they
        continue from where the previous segment ended. The PID map is reset
        because the new source may use different PIDs."""
        self.epoch_pending = True
        self.program = None

    def _next_cc(self, pid: int, has_payload: bool) -> int:
        last = self.continuity.get(pid, 15)
        if not has_payload:
            return last
        value = (last + 1) & 0x0F
        self.continuity[pid] = value
        return value

    def _psi(self, pid: int, section: bytes) -> List[bytes]:
        cc = (self.continuity.get(pid, 15) + 1) & 0x0F
        packets, next_cc = _psi_packets(pid, section, cc)
        self.continuity[pid] = (next_cc - 1) & 0x0F
        return packets

    def _scan_program(self, data: bytes, start: int) -> Optional[ProgramMap]:
        pat = _SectionAssembler()
        pmt = _SectionAssembler()
        pmt_pid: Optional[int] = None
        for pos in range(start, len(data) - TS_PACKET_SIZE + 1, TS_PACKET_SIZE):
            if data[pos] != SYNC_BYTE:
                continue
            pid = ((data[pos + 1] & 0x1F) << 8) | data[pos + 2]
            unit_start = bool(data[pos + 1] & 0x40)
            afc = (data[pos + 3] >> 4) & 0x03
            if not afc & 0x01:
                continue
            payload_start = pos + 4
            if afc & 0x02:
                payload_start += 1 + data[pos + 4]
            payload = data[payload_start:pos + TS_PACKET_SIZE]
            if pid == PAT_PID:
                section = pat.feed(payload, unit_start)
                if section:
                    pmt_pid = _parse_pat(section) or pmt_pid
            elif pmt_pid is not None and pid == pmt_pid:
                section = pmt.feed(payload, unit_start)
                if section:
                    program = _parse_pmt(section, pmt_pid)
                    if program and (program.video or program.audio):
                        return program
        return None

    def normalize(self, data: bytes, duration: float) -> NormalizeResult:
        start = find_ts_start(data)
        if start < 0:
            return NormalizeResult(data, None, False, False, (None, None), False)

        program = self._scan_program(data, start) or self.program
        if program is None:
            # Cannot map PIDs (no PAT/PMT seen yet): pass through untouched.
            return NormalizeResult(data[start:], None, False, False, (None, None), False)
        self.program = program

        signature = program.codec_signature()
        if signature != self.last_signature:
            if self.last_signature is not None:
                self.pmt_version = (self.pmt_version + 1) & 0x1F
            self.last_signature = signature

        pid_map: Dict[int, int] = {}
        if program.video:
            pid_map[program.video.pid] = OUT_VIDEO_PID
        if program.audio:
            pid_map[program.audio.pid] = OUT_AUDIO_PID
        if program.pcr_pid in pid_map:
            out_pcr_pid = pid_map[program.pcr_pid]
        elif program.pcr_pid != NULL_PID:
            pid_map[program.pcr_pid] = OUT_PCR_PID
            out_pcr_pid = OUT_PCR_PID
        else:
            out_pcr_pid = OUT_VIDEO_PID if program.video else OUT_AUDIO_PID

        # Timestamp re-basing for a new epoch: continue from the end of the
        # previous segment on the output clock.
        if self.epoch_pending:
            source_first = self._first_dts(data, start, program)
            if source_first is not None and self.last_segment_first_dts is not None:
                target = (self.last_segment_first_dts + int(round(self.last_segment_duration * CLOCK_HZ))) % TIMESTAMP_MODULO
                self.offset = _ts_diff(target, source_first)
            elif source_first is not None:
                self.offset = 0 if self.last_segment_first_dts is None else self.offset
            self.epoch_pending = False

        out = bytearray()
        for packet in self._psi(PAT_PID, _build_pat_section()):
            out.extend(packet)
        for packet in self._psi(OUT_PMT_PID, _build_pmt_section(program, out_pcr_pid, self.pmt_version)):
            out.extend(packet)

        video_pid = program.video.pid if program.video else None
        audio_pid = program.audio.pid if program.audio else None
        first_dts: Optional[int] = None
        first_audio_dts: Optional[int] = None
        offset = self.offset

        # Rewrite in place on one working copy (per-packet allocations made this
        # several times slower for multi-megabyte 1080p segments), then copy out
        # runs of kept packets.
        buf = bytearray(data[start:start + ((len(data) - start) // TS_PACKET_SIZE) * TS_PACKET_SIZE])
        run_start: Optional[int] = None
        for pos in range(0, len(buf), TS_PACKET_SIZE):
            out_pid = None
            if buf[pos] == SYNC_BYTE:
                pid = ((buf[pos + 1] & 0x1F) << 8) | buf[pos + 2]
                out_pid = pid_map.get(pid)
            if out_pid is None:
                # Drop PAT/PMT (re-emitted above), SI tables, extra streams, null
                # packets and anything that lost sync.
                if run_start is not None:
                    out.extend(buf[run_start:pos])
                    run_start = None
                continue
            if run_start is None:
                run_start = pos
            afc = (buf[pos + 3] >> 4) & 0x03
            has_payload = bool(afc & 0x01)
            buf[pos + 1] = (buf[pos + 1] & 0xE0) | ((out_pid >> 8) & 0x1F)
            buf[pos + 2] = out_pid & 0xFF
            buf[pos + 3] = (buf[pos + 3] & 0xF0) | self._next_cc(out_pid, has_payload)

            payload_start = pos + 4
            if afc & 0x02:
                af_length = buf[pos + 4]
                payload_start = pos + 5 + af_length
                if offset and af_length >= 7 and buf[pos + 5] & 0x10:
                    _write_pcr_base(buf, pos + 6, _read_pcr_base(buf, pos + 6) + offset)

            if has_payload and buf[pos + 1] & 0x40 and (pid == video_pid or pid == audio_pid):
                p = payload_start
                end = pos + TS_PACKET_SIZE
                if (
                    p + 9 <= end
                    and buf[p] == 0x00 and buf[p + 1] == 0x00 and buf[p + 2] == 0x01
                    and buf[p + 3] not in _PES_NO_HEADER_STREAM_IDS
                ):
                    flags = buf[p + 7] >> 6
                    pts_pos = p + 9
                    if flags & 0x02 and pts_pos + 5 <= end:
                        pts = (_read_timestamp(buf, pts_pos) + offset) % TIMESTAMP_MODULO
                        if offset:
                            _write_timestamp(buf, pts_pos, pts)
                        dts = pts
                        if flags == 0x03 and pts_pos + 10 <= end:
                            dts = (_read_timestamp(buf, pts_pos + 5) + offset) % TIMESTAMP_MODULO
                            if offset:
                                _write_timestamp(buf, pts_pos + 5, dts)
                        if pid == video_pid and first_dts is None:
                            first_dts = dts
                        elif pid == audio_pid and first_audio_dts is None:
                            first_audio_dts = dts
        if run_start is not None:
            out.extend(buf[run_start:])

        segment_first = first_dts if first_dts is not None else first_audio_dts
        if segment_first is not None:
            self.last_segment_first_dts = segment_first
            self.last_segment_duration = max(0.0, float(duration))
        elif self.last_segment_first_dts is not None:
            # Keep the running clock estimate moving even for odd segments.
            self.last_segment_first_dts = (
                self.last_segment_first_dts + int(round(self.last_segment_duration * CLOCK_HZ))
            ) % TIMESTAMP_MODULO
            self.last_segment_duration = max(0.0, float(duration))

        return NormalizeResult(
            bytes(out),
            segment_first,
            program.video is not None,
            program.audio is not None,
            signature,
            True,
        )

    @staticmethod
    def _first_dts(data: bytes, start: int, program: ProgramMap) -> Optional[int]:
        """First DTS (source clock) of the preferred stream, without modifying data."""
        wanted = [s.pid for s in (program.video, program.audio) if s is not None]
        found: Dict[int, int] = {}
        for pos in range(start, len(data) - TS_PACKET_SIZE + 1, TS_PACKET_SIZE):
            if data[pos] != SYNC_BYTE or not data[pos + 1] & 0x40:
                continue
            pid = ((data[pos + 1] & 0x1F) << 8) | data[pos + 2]
            if pid not in wanted or pid in found:
                continue
            afc = (data[pos + 3] >> 4) & 0x03
            if not afc & 0x01:
                continue
            p = pos + 4 + ((1 + data[pos + 4]) if afc & 0x02 else 0)
            end = pos + TS_PACKET_SIZE
            if p + 14 > end or data[p:p + 3] != b"\x00\x00\x01" or data[p + 3] in _PES_NO_HEADER_STREAM_IDS:
                continue
            flags = data[p + 7] >> 6
            if not flags & 0x02:
                continue
            value = _read_timestamp(data, p + 9)
            if flags == 0x03 and p + 19 <= end:
                value = _read_timestamp(data, p + 14)
            found[pid] = value
            if wanted and wanted[0] in found:
                break
        for pid in wanted:
            if pid in found:
                return found[pid]
        return None
