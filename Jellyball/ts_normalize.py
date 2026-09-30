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
the first video and the first (or preferred-language) audio elementary stream
are remapped to fixed PIDs, a fresh PAT/PMT is written at the start of every
segment, continuity counters continue across segments, and PTS/DTS/PCR are
shifted by a per-epoch offset so time carries on smoothly across a source
switch (or an upstream discontinuity).

An epoch starts when the caller says so (`start_new_epoch()`, on a source
switch or #EXT-X-DISCONTINUITY) and also automatically when a segment's
timestamps jump away from where the previous segment said the clock would be
(an encoder restart or a CDN edge on a different timeline that the playlist
did not flag). Either way the new epoch is anchored on the earliest of the
segment's first video and first audio DTS, placed just after the end of the
previous segment's content, so the output clock stays monotonic and
continuous.

Segments must be fed in playback order; the channel session guarantees that.
Everything here is pure computation on bytes (no I/O), so it is unit testable
and cheap to run in a worker thread.
"""
from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional, Set, Tuple

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
ISO_639_LANGUAGE_DESCRIPTOR = 0x0A
# PES stream_ids that have no optional header (and therefore no PTS/DTS).
_PES_NO_HEADER_STREAM_IDS = {0xBC, 0xBE, 0xBF, 0xF0, 0xF1, 0xF2, 0xF8, 0xFF}

# An epoch is anchored on the first audio DTS instead of the first video DTS
# only when audio leads by at most this much; a bigger gap means the streams
# are not really aligned near the segment start and anchoring on audio would
# open a long hole in the video.
_MAX_ANCHOR_LEAD = 5 * CLOCK_HZ
# The previous segment's content may run a little past its nominal end
# (first DTS + EXTINF); beyond this it is treated as a bogus timestamp.
_MAX_TAIL_OVERRUN = 10 * CLOCK_HZ
# Largest believable gap between consecutive PES of one stream.
_MAX_FRAME_STEP = CLOCK_HZ

# ISO 639-2 bibliographic -> terminology codes, so "ger" matches "deu" etc.
_ISO639_B_TO_T = {
    "alb": "sqi", "arm": "hye", "baq": "eus", "bur": "mya", "chi": "zho",
    "cze": "ces", "dut": "nld", "fre": "fra", "geo": "kat", "ger": "deu",
    "gre": "ell", "ice": "isl", "mac": "mkd", "mao": "mri", "may": "msa",
    "per": "fas", "rum": "ron", "slo": "slk", "tib": "bod", "wel": "cym",
}
# ISO_639_language_descriptor audio_type values for accessibility tracks.
_ACCESSIBILITY_AUDIO_TYPES = {0x02, 0x03}  # hearing impaired, visual impaired commentary


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


def _resync(data, pos: int, probe_packets: int = 3) -> int:
    """First offset >= pos where sync bytes repeat every 188 bytes for
    `probe_packets` packets (fewer only when the data ends first), or -1."""
    n = len(data)
    while True:
        pos = data.find(b"\x47", pos)
        if pos < 0 or n - pos < TS_PACKET_SIZE:
            return -1
        needed = min(probe_packets, (n - pos) // TS_PACKET_SIZE)
        if all(data[pos + k * TS_PACKET_SIZE] == SYNC_BYTE for k in range(1, needed)):
            return pos
        pos += 1


def _packet_runs(data, start: int) -> List[Tuple[int, int]]:
    """Byte ranges of whole, sync-aligned TS packets from `start` on.

    Checking the sync byte of every packet is done at C speed on a strided
    slice. When sync is lost (a corrupt or truncated packet, junk spliced in),
    the stream is re-found at the next place where 0x47 repeats at a 188-byte
    period instead of the rest of the segment being dropped."""
    runs: List[Tuple[int, int]] = []
    n = len(data)
    pos = start
    while n - pos >= TS_PACKET_SIZE:
        count = (n - pos) // TS_PACKET_SIZE
        end = pos + count * TS_PACKET_SIZE
        good = count - len(data[pos:end:TS_PACKET_SIZE].lstrip(b"\x47"))
        if good == count:
            runs.append((pos, end))
            break
        bad = pos + good * TS_PACKET_SIZE
        # The last packet before the break may itself have been cut short, so
        # look for the next packet starting from inside it.
        resume = _resync(data, bad - TS_PACKET_SIZE + 1 if good else pos + 1)
        if resume < 0:
            if good:
                runs.append((pos, bad))
            break
        run_end = bad if resume >= bad else bad - TS_PACKET_SIZE
        if run_end > pos:
            runs.append((pos, run_end))
        pos = resume
    return runs


def _aligned_packets(data, start: int) -> bytearray:
    """One mutable copy of the segment's packets, re-aligned past any lost sync."""
    runs = _packet_runs(data, start)
    view = memoryview(data)
    if len(runs) == 1:
        begin, end = runs[0]
        return bytearray(view[begin:end])
    buf = bytearray()
    for begin, end in runs:
        buf += view[begin:end]
    return buf


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


def _later(a: Optional[int], b: Optional[int]) -> Optional[int]:
    """The later of two wrapping-clock timestamps (either may be None)."""
    if a is None:
        return b
    if b is None:
        return a
    return a if _ts_diff(a, b) >= 0 else b


def _stream_end(last: Optional[int], previous: Optional[int]) -> Optional[int]:
    """Where a stream's content ends: its last DTS plus one PES step.

    Deliberately errs late: muxers pack several audio frames per PES and flush
    a shorter one at the segment cut, so this can overshoot the true end by a
    frame or two (tens of ms). A new epoch placed there leaves a tiny gap,
    never an overlap with the previous source's tail."""
    if last is None:
        return None
    step = _ts_diff(last, previous) if previous is not None else 0
    if not 0 < step <= _MAX_FRAME_STEP:
        step = 1  # unknown frame length: at least strictly after the last DTS
    return (last + step) % TIMESTAMP_MODULO


def _epoch_anchor(video: Optional[int], audio: Optional[int]) -> Optional[int]:
    """Source-clock DTS a new epoch is anchored on: the earlier of the first
    video and first audio DTS, so audio that leads video does not land on top
    of the previous source's tail."""
    if video is None:
        return audio
    if audio is None:
        return video
    lead = _ts_diff(video, audio)
    if 0 < lead <= _MAX_ANCHOR_LEAD:
        return audio
    return video


@dataclass
class ElementaryStream:
    pid: int
    stream_type: int
    descriptors: bytes = b""

    def languages(self) -> List[Tuple[str, int]]:
        """(ISO 639-2 code, audio_type) pairs from ISO_639_language_descriptors."""
        return _iso639_languages(self.descriptors)


@dataclass
class ProgramMap:
    pmt_pid: int
    pcr_pid: int
    program_info: bytes = b""
    video: Optional[ElementaryStream] = None
    audio: Optional[ElementaryStream] = None
    es_pids: Tuple[int, ...] = ()  # every elementary stream PID the PMT listed

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


# Index key per packet: payload_unit_start_indicator + the PID's high bits.
_KEY_HIGH_TABLE = bytes(b & 0x5F for b in range(256))
_UNIT_START_TABLE = bytes(1 if b & 0x40 else 0 for b in range(256))


class _PacketIndex:
    """(payload_unit_start, PID) of every packet in an aligned buffer, searchable
    with bytes.find so locating PAT/PMT or a stream's first PES never needs a
    Python loop over every packet of a multi-megabyte segment."""

    __slots__ = ("keys",)

    def __init__(self, buf: bytearray) -> None:
        count = len(buf) // TS_PACKET_SIZE
        keys = bytearray(2 * count)
        if count:
            keys[0::2] = buf[1::TS_PACKET_SIZE].translate(_KEY_HIGH_TABLE)
            keys[1::2] = buf[2::TS_PACKET_SIZE]
        self.keys = keys

    def _find_all(self, pid: int, unit_start: bool) -> Iterator[int]:
        needle = bytes((((pid >> 8) & 0x1F) | (0x40 if unit_start else 0), pid & 0xFF))
        keys = self.keys
        i = keys.find(needle)
        while i >= 0:
            if i & 1:  # straddles two packets' keys
                i = keys.find(needle, i + 1)
                continue
            yield (i >> 1) * TS_PACKET_SIZE
            i = keys.find(needle, i + 2)

    def unit_starts(self, pid: int) -> Iterator[int]:
        """Byte offsets of packets on `pid` that start a PES packet / section."""
        return self._find_all(pid, True)

    def positions(self, pid: int) -> Iterator[int]:
        """Byte offsets of every packet on `pid`, in stream order."""
        return heapq.merge(self._find_all(pid, True), self._find_all(pid, False))

    def unit_start_packets(self) -> Iterator[Tuple[int, int]]:
        """(byte offset, PID) of every packet with payload_unit_start set."""
        keys = self.keys
        flags = keys[0::2].translate(_UNIT_START_TABLE)
        i = flags.find(1)
        while i >= 0:
            yield i * TS_PACKET_SIZE, ((keys[2 * i] & 0x1F) << 8) | keys[2 * i + 1]
            i = flags.find(1, i + 1)


def _packet_payload(buf, pos: int) -> Optional[bytes]:
    afc = (buf[pos + 3] >> 4) & 0x03
    if not afc & 0x01:
        return None
    start = pos + 4 + ((1 + buf[pos + 4]) if afc & 0x02 else 0)
    return bytes(buf[start:pos + TS_PACKET_SIZE])


def _pes_payload_start(buf, pos: int) -> Optional[int]:
    """Offset of a PES header in a unit-start packet, or None."""
    afc = (buf[pos + 3] >> 4) & 0x03
    if not afc & 0x01:
        return None
    p = pos + 4 + ((1 + buf[pos + 4]) if afc & 0x02 else 0)
    if p + 4 > pos + TS_PACKET_SIZE or buf[p] or buf[p + 1] or buf[p + 2] != 0x01:
        return None
    return p


def _pes_dts(buf, pos: int) -> Optional[int]:
    """DTS (else PTS) of the PES starting in the packet at `pos`, source clock.

    Only headers that fit in the packet are read; the rewrite loop in
    TsNormalizer.normalize applies the same rule."""
    p = _pes_payload_start(buf, pos)
    end = pos + TS_PACKET_SIZE
    if p is None or p + 14 > end or buf[p + 3] in _PES_NO_HEADER_STREAM_IDS:
        return None
    flags = buf[p + 7] >> 6
    if not flags & 0x02:
        return None
    if flags == 0x03 and p + 19 <= end:
        return _read_timestamp(buf, p + 14)
    return _read_timestamp(buf, p + 9)


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


def _canonical_language(code: Optional[str]) -> str:
    code = (code or "").strip().strip("\x00").lower()
    return _ISO639_B_TO_T.get(code, code)


def _iso639_languages(descriptors: bytes) -> List[Tuple[str, int]]:
    """(language code, audio_type) entries of every ISO_639_language_descriptor."""
    found: List[Tuple[str, int]] = []
    pos = 0
    while pos + 2 <= len(descriptors):
        tag, length = descriptors[pos], descriptors[pos + 1]
        body = descriptors[pos + 2:pos + 2 + length]
        if tag == ISO_639_LANGUAGE_DESCRIPTOR:
            for i in range(0, len(body) - 3, 4):
                code = bytes(body[i:i + 3]).decode("latin-1")
                found.append((_canonical_language(code), body[i + 3]))
        pos += 2 + length
    return found


def _choose_audio(candidates: List[ElementaryStream], preferred_language: Optional[str]) -> Optional[ElementaryStream]:
    """The first audio stream, or the first one in `preferred_language` (a main
    track over hearing/visually impaired ones) when there is such a stream."""
    if not candidates:
        return None
    wanted = _canonical_language(preferred_language)
    if wanted:
        matches = []
        for stream in candidates:
            types = [audio_type for code, audio_type in stream.languages() if code == wanted]
            if types:
                matches.append((stream, types))
        for stream, types in matches:
            if any(t not in _ACCESSIBILITY_AUDIO_TYPES for t in types):
                return stream
        if matches:
            return matches[0][0]
    return candidates[0]


def _parse_pmt(section: bytes, pmt_pid: int, preferred_language: Optional[str] = None) -> Optional[ProgramMap]:
    if len(section) < 16 or section[0] != 0x02:
        return None
    section_length = ((section[1] & 0x0F) << 8) | section[2]
    end = min(len(section), 3 + section_length) - 4
    pcr_pid = ((section[8] & 0x1F) << 8) | section[9]
    program_info_length = ((section[10] & 0x0F) << 8) | section[11]
    program_info = bytes(section[12:12 + program_info_length])
    pos = 12 + program_info_length
    program = ProgramMap(pmt_pid=pmt_pid, pcr_pid=pcr_pid, program_info=program_info)
    audio_candidates: List[ElementaryStream] = []
    es_pids: List[int] = []
    while pos + 5 <= end:
        stream_type = section[pos]
        pid = ((section[pos + 1] & 0x1F) << 8) | section[pos + 2]
        es_info_length = ((section[pos + 3] & 0x0F) << 8) | section[pos + 4]
        descriptors = bytes(section[pos + 5:pos + 5 + es_info_length])
        es_pids.append(pid)
        if program.video is None and stream_type in VIDEO_STREAM_TYPES:
            program.video = ElementaryStream(pid, stream_type, descriptors)
        elif _is_audio_stream(stream_type, descriptors):
            audio_candidates.append(ElementaryStream(pid, stream_type, descriptors))
        pos += 5 + es_info_length
    program.audio = _choose_audio(audio_candidates, preferred_language)
    program.es_pids = tuple(es_pids)
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


_PAT_SECTION = _build_pat_section()


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
    """Outcome of `TsNormalizer.normalize` for one segment.

    `data` is always `bytes`, safe to keep and serve as is.

    When `normalized` is False the segment could not be mapped (not MPEG-TS,
    or no PAT/PMT and nothing known that matches its PIDs). `data` is then
    the input unmodified apart from a stripped junk prefix, still carrying the
    source's own PIDs, continuity counters and timestamps. `has_video`,
    `has_audio` and `codec_signature` are placeholders (False, False,
    (None, None)), NOT a finding that the source lacks audio or video.
    Callers must ignore them and must not report media info for such a
    segment.

    `discontinuity` is True when the normalizer found a timestamp jump
    inside an epoch on its own and started a new epoch at this segment. The
    typical cause is an encoder restart or a CDN edge on another timeline that
    the playlist did not flag with #EXT-X-DISCONTINUITY. The output clock
    still continues seamlessly, but the session should put
    #EXT-X-DISCONTINUITY before this segment, just as for an upstream
    discontinuity. It is never set for epochs the caller started with
    `start_new_epoch()`; the caller already knows about those.

    `epoch_pending` is True when a new epoch was due but this segment had no
    readable PTS/DTS to anchor it on. PIDs, continuity counters and PAT/PMT are
    normalized, but no timestamp (not even PCR) is shifted, because the only
    offset available belongs to the previous source. The epoch anchors on the
    next segment that has a timestamp, and that segment continues the clock
    as if this one had lasted its declared duration.
    """

    data: bytes
    first_dts: Optional[int]  # output clock, first video (else audio) PES in segment
    has_video: bool  # meaningful only when normalized is True
    has_audio: bool  # meaningful only when normalized is True
    codec_signature: Tuple[Optional[int], Optional[int]]  # (None, None) when not normalized
    normalized: bool  # False when the segment could not be parsed and was passed through
    discontinuity: bool = False  # normalizer started a new epoch here on a timestamp jump
    epoch_pending: bool = False  # new epoch still waiting for a segment with a timestamp


@dataclass
class TsNormalizer:
    """Stateful per-channel normalizer. Feed segments strictly in playback order.

    Options (all keyword, all optional):

    * `jump_threshold` - minimum in-epoch timestamp jump, in 90 kHz ticks, that
      starts a new epoch automatically (default 10 s). The effective threshold is
      max(jump_threshold, jump_threshold_segments x segment duration). None
      disables jump detection.
    * `jump_threshold_segments` - see above (default 3.0).
    * `preferred_audio_language` - ISO 639-2 code (e.g. "eng"; bibliographic and
      terminology forms both match) of the audio stream to prefer, from the
      PMT's ISO_639_language_descriptor. Default None: first audio stream.
    """

    program: Optional[ProgramMap] = None
    offset: int = 0
    epoch_pending: bool = True
    pmt_version: int = 0
    last_signature: Optional[Tuple[Optional[int], Optional[int]]] = None
    # Output timeline bookkeeping used to place the next epoch.
    last_segment_first_dts: Optional[int] = None
    last_segment_duration: float = 0.0
    continuity: Dict[int, int] = field(default_factory=dict)
    # Output clock just past the last PES of the previous segment (latest over
    # its streams), or None when unknown.
    last_segment_end_dts: Optional[int] = None
    jump_threshold: Optional[int] = 10 * CLOCK_HZ
    jump_threshold_segments: float = 3.0
    preferred_audio_language: Optional[str] = None
    # The most recent program map; unlike `program` it survives
    # start_new_epoch() so a PAT/PMT-less first segment of the new epoch can
    # reuse it when its PES PIDs match.
    last_program: Optional[ProgramMap] = None

    def start_new_epoch(self) -> None:
        """Call before the first segment of a new source (or after an upstream
        discontinuity): the next segment's timestamps get re-based so they
        continue from where the previous segment ended. The PID map is reset
        because the new source may use different PIDs (the old one is kept
        in `last_program` and reused only if the new segments match it)."""
        self.epoch_pending = True
        if self.program is not None:
            self.last_program = self.program
        self.program = None

    def _psi(self, pid: int, section: bytes) -> List[bytes]:
        cc = (self.continuity.get(pid, 15) + 1) & 0x0F
        packets, next_cc = _psi_packets(pid, section, cc)
        self.continuity[pid] = (next_cc - 1) & 0x0F
        return packets

    def _expected_next_dts(self) -> Optional[int]:
        """Output-clock DTS the next segment should start at: the previous
        segment's first DTS plus its duration, or later if its content (last
        DTS plus one frame, over all streams) ran past that."""
        if self.last_segment_first_dts is None:
            return None
        nominal = (
            self.last_segment_first_dts + int(round(self.last_segment_duration * CLOCK_HZ))
        ) % TIMESTAMP_MODULO
        end = self.last_segment_end_dts
        if end is not None and 0 < _ts_diff(end, nominal) <= _MAX_TAIL_OVERRUN:
            return end
        return nominal

    def _advance_clock(self, duration: float) -> None:
        """Account for a published segment whose timestamps we could not use,
        so the next epoch still lands where the playlist timeline is."""
        if self.last_segment_first_dts is not None:
            self.last_segment_first_dts = self._expected_next_dts()
            self.last_segment_duration = max(0.0, float(duration))
            self.last_segment_end_dts = None

    def _jump_threshold_ticks(self, duration: float) -> Optional[int]:
        if self.jump_threshold is None:
            return None
        segment = max(self.last_segment_duration, duration, 0.0)
        return max(int(self.jump_threshold), int(self.jump_threshold_segments * segment * CLOCK_HZ))

    def _scan_program(self, buf: bytearray, index: _PacketIndex) -> Optional[ProgramMap]:
        pat = _SectionAssembler()
        pmt_pid: Optional[int] = None
        for pos in index.positions(PAT_PID):
            payload = _packet_payload(buf, pos)
            if payload is None:
                continue
            section = pat.feed(payload, bool(buf[pos + 1] & 0x40))
            if section:
                pmt_pid = _parse_pat(section)
                if pmt_pid is not None:
                    break
        if pmt_pid is None:
            return None
        pmt = _SectionAssembler()
        for pos in index.positions(pmt_pid):
            payload = _packet_payload(buf, pos)
            if payload is None:
                continue
            section = pmt.feed(payload, bool(buf[pos + 1] & 0x40))
            if section:
                program = _parse_pmt(section, pmt_pid, self.preferred_audio_language)
                if program and (program.video or program.audio):
                    return program
        return None

    @staticmethod
    def _map_matches(buf: bytearray, index: _PacketIndex, program: ProgramMap) -> bool:
        """Whether a PAT/PMT-less segment carries the PES streams of `program`:
        at least one of its mapped PIDs starts a PES, and no PID outside the
        old PMT does."""
        mapped = {s.pid for s in (program.video, program.audio) if s is not None}
        pes_pids: Set[int] = set()
        for pos, pid in index.unit_start_packets():
            if pid not in pes_pids and _pes_payload_start(buf, pos) is not None:
                pes_pids.add(pid)
        if not pes_pids & mapped:
            return False
        return pes_pids <= (set(program.es_pids) | mapped)

    @staticmethod
    def _first_timestamps(
        buf: bytearray, index: _PacketIndex, program: ProgramMap,
    ) -> Tuple[Optional[int], Optional[int]]:
        """First (video DTS, audio DTS) in the segment on the source clock."""
        found: List[Optional[int]] = []
        for stream in (program.video, program.audio):
            value = None
            if stream is not None:
                for pos in index.unit_starts(stream.pid):
                    value = _pes_dts(buf, pos)
                    if value is not None:
                        break
            found.append(value)
        return found[0], found[1]

    def normalize(self, data: bytes, duration: float) -> NormalizeResult:
        if not isinstance(data, bytes):
            data = bytes(data)  # any bytes-like input; results always carry bytes
        start = find_ts_start(data)
        if start < 0:
            return NormalizeResult(data, None, False, False, (None, None), False)

        buf = _aligned_packets(data, start)
        index = _PacketIndex(buf)
        program = self._scan_program(buf, index)
        if program is None:
            # No PAT/PMT in this segment. Within an epoch the PIDs cannot have
            # changed; right after start_new_epoch() reuse the previous map
            # only if this segment's PES PIDs prove it still applies.
            program = self.program
            if program is None and self.last_program is not None and self._map_matches(buf, index, self.last_program):
                program = self.last_program
        if program is None or not buf:
            # Cannot map PIDs: pass through untouched (see NormalizeResult).
            # The segment is still published, so the clock estimate moves on.
            self._advance_clock(duration)
            return NormalizeResult(data[start:] if start else data, None, False, False, (None, None), False)
        self.program = program
        self.last_program = program

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

        discontinuity = False
        pending = False
        if self.epoch_pending:
            # New epoch: continue from the end of the previous segment on the
            # output clock. Only clear the flag once there is a source
            # timestamp to anchor on; a stale offset would jump by hours.
            anchor = _epoch_anchor(*self._first_timestamps(buf, index, program))
            if anchor is None:
                pending = True
                offset = 0
            else:
                expected = self._expected_next_dts()
                self.offset = 0 if expected is None else _ts_diff(expected, anchor)
                self.epoch_pending = False
                offset = self.offset
        else:
            offset = self.offset
            threshold = self._jump_threshold_ticks(duration)
            expected = self._expected_next_dts() if threshold is not None else None
            if expected is not None:
                video_first, audio_first = self._first_timestamps(buf, index, program)
                first = video_first if video_first is not None else audio_first
                # _ts_diff works on the wrapping clock, so a 33-bit PTS wrap
                # is a small step here, not a jump.
                if first is not None and abs(_ts_diff((first + offset) % TIMESTAMP_MODULO, expected)) > threshold:
                    self.offset = offset = _ts_diff(expected, _epoch_anchor(video_first, audio_first))
                    discontinuity = True

        psi = self._psi(PAT_PID, _PAT_SECTION)
        psi += self._psi(OUT_PMT_PID, _build_pmt_section(program, out_pcr_pid, self.pmt_version))

        video_pid = program.video.pid if program.video else None
        audio_pid = program.audio.pid if program.audio else None
        v_first = v_last = v_prev = None
        a_first = a_last = a_prev = None
        continuity = self.continuity
        out_pid_of = pid_map.get
        no_header_ids = _PES_NO_HEADER_STREAM_IDS

        # Rewrite in place on the one working copy, then join the runs of kept
        # packets straight into the output bytes (memoryview slices, so the
        # join is the only other copy).
        view = memoryview(buf)
        chunks: List[object] = psi
        run_start = -1
        for pos in range(0, len(buf), TS_PACKET_SIZE):
            b1 = buf[pos + 1]
            pid = ((b1 & 0x1F) << 8) | buf[pos + 2]
            out_pid = out_pid_of(pid)
            if out_pid is None:
                # Drop PAT/PMT (re-emitted above), SI tables, extra streams and
                # null packets.
                if run_start >= 0:
                    chunks.append(view[run_start:pos])
                    run_start = -1
                continue
            if run_start < 0:
                run_start = pos
            b3 = buf[pos + 3]
            buf[pos + 1] = (b1 & 0xE0) | (out_pid >> 8)
            buf[pos + 2] = out_pid & 0xFF
            if b3 & 0x10:
                cc = (continuity.get(out_pid, 15) + 1) & 0x0F
                continuity[out_pid] = cc
            else:
                cc = continuity.get(out_pid, 15)
            buf[pos + 3] = (b3 & 0xF0) | cc

            if b3 & 0x20:
                af_length = buf[pos + 4]
                p = pos + 5 + af_length
                if offset and af_length >= 7 and buf[pos + 5] & 0x10:
                    _write_pcr_base(buf, pos + 6, _read_pcr_base(buf, pos + 6) + offset)
            else:
                p = pos + 4

            if b1 & 0x40 and b3 & 0x10 and (pid == video_pid or pid == audio_pid):
                end = pos + TS_PACKET_SIZE
                if (
                    p + 14 <= end
                    and buf[p] == 0x00 and buf[p + 1] == 0x00 and buf[p + 2] == 0x01
                    and buf[p + 3] not in no_header_ids
                ):
                    flags = buf[p + 7] >> 6
                    if flags & 0x02:
                        dts = pts = (_read_timestamp(buf, p + 9) + offset) % TIMESTAMP_MODULO
                        if offset:
                            _write_timestamp(buf, p + 9, pts)
                        if flags == 0x03 and p + 19 <= end:
                            dts = (_read_timestamp(buf, p + 14) + offset) % TIMESTAMP_MODULO
                            if offset:
                                _write_timestamp(buf, p + 14, dts)
                        if pid == video_pid:
                            if v_first is None:
                                v_first = dts
                            v_prev, v_last = v_last, dts
                        else:
                            if a_first is None:
                                a_first = dts
                            a_prev, a_last = a_last, dts
        if run_start >= 0:
            chunks.append(view[run_start:])
        out = b"".join(chunks)

        segment_first = None if pending else (v_first if v_first is not None else a_first)
        if segment_first is not None:
            self.last_segment_first_dts = segment_first
            self.last_segment_duration = max(0.0, float(duration))
            self.last_segment_end_dts = _later(_stream_end(v_last, v_prev), _stream_end(a_last, a_prev))
        else:
            # Keep the running clock estimate moving even for odd segments.
            self._advance_clock(duration)

        return NormalizeResult(
            out,
            segment_first,
            program.video is not None,
            program.audio is not None,
            signature,
            True,
            discontinuity=discontinuity,
            epoch_pending=pending,
        )
