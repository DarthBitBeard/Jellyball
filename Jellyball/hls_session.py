"""Per-channel HLS sessions: one proxy-built, continuous playlist per channel.

A `ChannelSession` sits between Jellyfin and whatever upstream currently feeds a
channel. While anyone is watching, it polls the active source's media playlist,
downloads each new segment exactly once, runs it through `TsNormalizer`, and
publishes it into its own sliding-window playlist with proxy-owned, monotonic
media-sequence numbers. When the source changes (failover, provider change,
placeholder -> live, Multi-View restart) the next segment is marked
#EXT-X-DISCONTINUITY and the normalizer re-bases PIDs, continuity counters and
timestamps, so Jellyfin's ffmpeg keeps playing instead of stalling.

Design rules:
  * Only segments that downloaded successfully are published, so the player
    never sees a failing segment URL; an upstream hiccup just means the
    playlist doesn't grow for a moment (and the last good window keeps being
    served instead of a 502).
  * The session reads the active source on every poll through
    `SessionHooks.resolve_source` (pull model): failover code only has to move
    `active_index`; nothing needs to notify sessions (though `poke()` makes the
    switch immediate).
  * Stale playlists and repeated fetch failures are reported through
    `SessionHooks.report_failure`, which drives failover from real playback
    instead of synthetic probes.
  * This module never imports `main`; everything app-specific is injected.
"""
from __future__ import annotations

import asyncio
import logging
import math
import re
import time
import urllib.parse
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Awaitable, Callable, Deque, Dict, List, Optional, Tuple

from ts_normalize import TsNormalizer, find_ts_start

LOGGER = logging.getLogger("jellyball.session")

try:  # AES-128 HLS decryption
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
except ImportError:  # pragma: no cover - dependency is pinned in requirements
    Cipher = None


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SourceSpec:
    """What a session should play. A change of `key` is a source switch (new
    epoch, discontinuity, live-edge restart); a change of only `url`/headers
    with the same key is a token refresh or Multi-View audio switch and
    continues seamlessly."""

    key: Tuple
    url: str
    referer: str = ""
    origin: str = ""
    local: bool = False  # read playlist + segments from disk (placeholder, Multi-View)
    label: str = ""


@dataclass
class FetchResult:
    status: int
    url: str
    content_type: str
    body: bytes


@dataclass
class KeyInfo:
    method: str
    uri: str = ""
    iv: Optional[bytes] = None


@dataclass
class UpstreamSegment:
    useq: int
    uri: str
    duration: float
    discontinuity: bool = False
    key: Optional[KeyInfo] = None
    byterange: Optional[Tuple[int, int]] = None  # (length, offset)


@dataclass
class MediaPlaylist:
    target_duration: float
    media_sequence: int
    segments: List[UpstreamSegment]
    endlist: bool = False
    has_map: bool = False
    sample_aes: bool = False


@dataclass
class Variant:
    uri: str
    bandwidth: int
    codecs: str = ""
    resolution: str = ""
    audio_group: str = ""


@dataclass
class MasterPlaylist:
    variants: List[Variant]
    audio_groups_with_uri: set


@dataclass
class SessionSegment:
    seq: int
    duration: float
    discontinuity: bool
    data: bytes


@dataclass
class SessionConfig:
    idle_timeout: float = 60.0
    min_start_segments: int = 3
    live_edge_segments: int = 3
    window_min_segments: int = 6
    window_min_seconds: float = 30.0
    window_max_segments: int = 30
    grace_segments: int = 12
    max_session_bytes: int = 256 * 1024 * 1024
    stale_min_seconds: float = 15.0
    fail_threshold: int = 3
    failure_report_cooldown: float = 10.0
    playlist_timeout: float = 8.0
    segment_timeout: float = 15.0
    max_playlist_bytes: int = 2 * 1024 * 1024
    max_segment_bytes: int = 48 * 1024 * 1024
    bandwidth_cap: int = 0  # 0 = pick the highest-bandwidth variant
    max_catchup_segments: int = 8
    download_concurrency: int = 3
    legacy_retry_seconds: float = 300.0


@dataclass
class SessionHooks:
    fetch: Callable[[str, Dict[str, str], int, float], Awaitable[Optional[FetchResult]]]
    headers_for: Callable[[str, str], Dict[str, str]]
    resolve_source: Callable[[str], Optional[SourceSpec]]
    report_failure: Callable[[str, Tuple, str], None]
    report_incompatible: Callable[[str, Tuple, str], None]
    on_media_info: Optional[Callable[[str, Tuple, bool, Tuple], None]] = None


# ---------------------------------------------------------------------------
# Playlist parsing
# ---------------------------------------------------------------------------

_ATTR_RE = re.compile(r'([A-Z0-9-]+)=("[^"]*"|[^,]*)')


def _attributes(line: str) -> Dict[str, str]:
    _, _, rest = line.partition(":")
    return {key: value.strip('"') for key, value in _ATTR_RE.findall(rest)}


def _resolve_uri(base: str, uri: str, local: bool) -> Optional[str]:
    if local:
        name = uri.strip()
        # Local playlists are written by our own ffmpeg with plain relative names;
        # refuse anything that could escape the output directory.
        if not name or "/" in name or "\\" in name or ".." in name or ":" in name:
            return None
        return str(Path(base).parent / name)
    return urllib.parse.urljoin(base, uri.strip())


def is_master_playlist(text: str) -> bool:
    return "#EXT-X-STREAM-INF" in text


def parse_master_playlist(text: str, base_url: str) -> MasterPlaylist:
    variants: List[Variant] = []
    audio_groups_with_uri = set()
    pending: Optional[Dict[str, str]] = None
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#EXT-X-STREAM-INF:"):
            pending = _attributes(line)
        elif line.startswith("#EXT-X-MEDIA:"):
            attrs = _attributes(line)
            if attrs.get("TYPE", "").upper() == "AUDIO" and attrs.get("URI"):
                audio_groups_with_uri.add(attrs.get("GROUP-ID", ""))
        elif line.startswith("#"):
            continue
        elif pending is not None:
            try:
                bandwidth = int(pending.get("BANDWIDTH") or pending.get("AVERAGE-BANDWIDTH") or 0)
            except ValueError:
                bandwidth = 0
            variants.append(Variant(
                uri=urllib.parse.urljoin(base_url, line),
                bandwidth=bandwidth,
                codecs=pending.get("CODECS", ""),
                resolution=pending.get("RESOLUTION", ""),
                audio_group=pending.get("AUDIO", ""),
            ))
            pending = None
    return MasterPlaylist(variants, audio_groups_with_uri)


_VIDEO_CODEC_PREFIXES = ("avc", "hvc", "hev", "vp0", "vp9", "av01", "mp4v")


def choose_variant(master: MasterPlaylist, bandwidth_cap: int = 0) -> Tuple[Optional[Variant], bool]:
    """Pick one variant. Returns (variant, demuxed_audio)."""
    variants = list(master.variants)
    if not variants:
        return None, False

    def has_video(v: Variant) -> bool:
        codecs = v.codecs.lower()
        return bool(v.resolution) or any(p in codecs for p in _VIDEO_CODEC_PREFIXES) or not codecs

    video_variants = [v for v in variants if has_video(v)] or variants
    muxed = [v for v in video_variants if v.audio_group not in master.audio_groups_with_uri or not v.audio_group]
    pool = muxed or video_variants
    demuxed = not muxed
    if bandwidth_cap > 0:
        under = [v for v in pool if v.bandwidth <= bandwidth_cap]
        if under:
            return max(under, key=lambda v: v.bandwidth), demuxed
        return min(pool, key=lambda v: v.bandwidth), demuxed
    return max(pool, key=lambda v: v.bandwidth), demuxed


def parse_media_playlist(text: str, base: str, local: bool = False) -> MediaPlaylist:
    target = 0.0
    media_sequence = 0
    segments: List[UpstreamSegment] = []
    endlist = has_map = sample_aes = False
    duration: Optional[float] = None
    discontinuity = False
    key: Optional[KeyInfo] = None
    byterange: Optional[Tuple[int, int]] = None
    next_offset: Dict[str, int] = {}

    lines = text.splitlines()
    if local and text and not text.endswith("\n") and lines:
        # ffmpeg rewrites the playlist in place; a torn read can end mid-line.
        lines = lines[:-1]

    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#EXT-X-TARGETDURATION:"):
            try:
                target = float(line.split(":", 1)[1])
            except ValueError:
                pass
        elif line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            try:
                media_sequence = int(line.split(":", 1)[1])
            except ValueError:
                pass
        elif line.startswith("#EXTINF:"):
            try:
                duration = float(line.split(":", 1)[1].split(",", 1)[0])
            except ValueError:
                duration = None
        elif line.startswith("#EXT-X-DISCONTINUITY") and not line.startswith("#EXT-X-DISCONTINUITY-SEQUENCE"):
            discontinuity = True
        elif line.startswith("#EXT-X-KEY:"):
            attrs = _attributes(line)
            method = attrs.get("METHOD", "NONE").upper()
            if method == "NONE":
                key = None
            else:
                if method != "AES-128":
                    sample_aes = True
                iv_text = attrs.get("IV", "")
                iv = None
                if iv_text.lower().startswith("0x"):
                    try:
                        iv = bytes.fromhex(iv_text[2:].rjust(32, "0"))[-16:]
                    except ValueError:
                        iv = None
                uri = attrs.get("URI", "")
                key = KeyInfo(method, urllib.parse.urljoin(base, uri) if uri and not local else uri, iv)
        elif line.startswith("#EXT-X-BYTERANGE:"):
            value = line.split(":", 1)[1]
            length_text, _, offset_text = value.partition("@")
            try:
                length = int(length_text)
                offset = int(offset_text) if offset_text else -1
                byterange = (length, offset)
            except ValueError:
                byterange = None
        elif line.startswith("#EXT-X-MAP"):
            has_map = True
        elif line.startswith("#EXT-X-ENDLIST"):
            endlist = True
        elif line.startswith("#"):
            continue
        else:
            uri = _resolve_uri(base, line, local)
            if uri is not None and duration is not None:
                resolved_range = None
                if byterange is not None:
                    length, offset = byterange
                    if offset < 0:
                        offset = next_offset.get(uri, 0)
                    resolved_range = (length, offset)
                    next_offset[uri] = offset + length
                segments.append(UpstreamSegment(
                    useq=media_sequence + len(segments),
                    uri=uri,
                    duration=duration,
                    discontinuity=discontinuity,
                    key=key,
                    byterange=resolved_range,
                ))
            duration = None
            discontinuity = False
            byterange = None

    if not target and segments:
        target = max(s.duration for s in segments)
    return MediaPlaylist(target, media_sequence, segments, endlist, has_map, sample_aes)


def aes128_decrypt(data: bytes, key: bytes, iv: bytes) -> bytes:
    if Cipher is None:
        raise RuntimeError("cryptography is not installed")
    usable = len(data) - (len(data) % 16)
    decryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).decryptor()
    plain = decryptor.update(data[:usable]) + decryptor.finalize()
    pad = plain[-1] if plain else 0
    if 1 <= pad <= 16 and plain.endswith(bytes([pad]) * pad):
        plain = plain[:-pad]
    return plain


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------

class ChannelSession:
    def __init__(self, channel_id: str, hooks: SessionHooks, config: SessionConfig) -> None:
        self.channel_id = channel_id
        self.hooks = hooks
        self.cfg = config
        self.window: Deque[SessionSegment] = deque()
        self.grace: "OrderedDict[int, SessionSegment]" = OrderedDict()
        # Seeded from the wall clock so numbering never goes backwards, even
        # across an idle stop/restart or an application restart.
        self.next_seq = int(time.time())
        self.discontinuity_seq = 0
        self.target_duration = 0
        self.source: Optional[SourceSpec] = None
        self.media_url: Optional[str] = None
        self.last_useq: Optional[int] = None
        self.pending_discontinuity = False
        self.normalizer = TsNormalizer()
        self.state = "idle"  # idle | starting | live | legacy
        self.legacy_reason = ""
        self.legacy_since = 0.0
        self.last_access = time.monotonic()
        self.last_new_segment_at = time.monotonic()
        self.consecutive_failures = 0
        self.consecutive_segment_failures = 0
        self.has_audio: Optional[bool] = None
        self.codec_signature: Optional[Tuple] = None
        self.stats: Dict[str, int] = {
            "segments": 0, "segment_failures": 0, "playlist_failures": 0,
            "source_switches": 0, "failover_requests": 0, "bytes": 0,
        }
        self._window_bytes = 0
        self._grace_bytes = 0
        self._task: Optional[asyncio.Task] = None
        self._ready = asyncio.Event()
        self._wake = asyncio.Event()
        self._stopped = False
        self._last_report: Dict[Tuple, float] = {}
        self._keys: "OrderedDict[str, bytes]" = OrderedDict()

    # -- public API ---------------------------------------------------------

    def touch(self) -> None:
        self.last_access = time.monotonic()
        self.ensure_running()

    def ensure_running(self) -> None:
        if self.state == "legacy":
            if time.monotonic() - self.legacy_since < self.cfg.legacy_retry_seconds:
                return
            self.state = "idle"
            self.legacy_reason = ""
            self._ready.clear()
        if self._task is None or self._task.done():
            self._stopped = False
            if not self.window:
                self.state = "starting"
            self._task = asyncio.create_task(self._run(), name=f"hls session {self.channel_id}")

    def poke(self) -> None:
        """Poll now (e.g. right after failover moved the active candidate)."""
        self._wake.set()

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def is_watched(self, within: Optional[float] = None) -> bool:
        within = self.cfg.idle_timeout if within is None else within
        return self.is_running and time.monotonic() - self.last_access < within

    def is_flowing(self) -> bool:
        """True while new segments keep arriving from the current source."""
        stale_after = max(3 * max(self.target_duration, 1), self.cfg.stale_min_seconds)
        return self.state == "live" and time.monotonic() - self.last_new_segment_at < stale_after

    async def wait_ready(self, timeout: float) -> bool:
        if self._ready.is_set():
            return True
        try:
            await asyncio.wait_for(self._ready.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return bool(self.window)

    def render_playlist(self, segment_prefix: str) -> str:
        target = max(1, self.target_duration)
        first_seq = self.window[0].seq if self.window else self.next_seq
        lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:3",
            f"#EXT-X-TARGETDURATION:{target}",
            f"#EXT-X-MEDIA-SEQUENCE:{first_seq}",
            f"#EXT-X-DISCONTINUITY-SEQUENCE:{self.discontinuity_seq}",
        ]
        for segment in self.window:
            if segment.discontinuity:
                lines.append("#EXT-X-DISCONTINUITY")
            lines.append(f"#EXTINF:{segment.duration:.3f},")
            lines.append(f"{segment_prefix}{segment.seq}.ts")
        return "\n".join(lines) + "\n"

    def get_segment(self, seq: int) -> Optional[SessionSegment]:
        if self.window and self.window[0].seq <= seq <= self.window[-1].seq:
            index = seq - self.window[0].seq
            if 0 <= index < len(self.window) and self.window[index].seq == seq:
                return self.window[index]
            for segment in self.window:
                if segment.seq == seq:
                    return segment
        return self.grace.get(seq)

    def snapshot(self) -> dict:
        return {
            "state": self.state,
            "watched": self.is_watched(),
            "flowing": self.is_flowing(),
            "source": self.source.label if self.source else "",
            "source_key": list(self.source.key) if self.source else None,
            "window_segments": len(self.window),
            "target_duration": self.target_duration,
            "media_sequence": self.window[0].seq if self.window else None,
            "discontinuity_sequence": self.discontinuity_seq,
            "has_audio": self.has_audio,
            "legacy_reason": self.legacy_reason,
            "memory_mb": round((self._window_bytes + self._grace_bytes) / (1024 * 1024), 1),
            **self.stats,
        }

    async def close(self) -> None:
        self._stopped = True
        task = self._task
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._task = None
        self._release_memory()

    # -- loop ---------------------------------------------------------------

    async def _run(self) -> None:
        try:
            while not self._stopped:
                if time.monotonic() - self.last_access > self.cfg.idle_timeout:
                    LOGGER.info("Channel session idle; stopping channel=%s", self.channel_id)
                    break
                started = time.monotonic()
                try:
                    await self._poll_once()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # never let one bad poll kill the session
                    LOGGER.warning("Channel session poll failed channel=%s error=%s", self.channel_id, type(exc).__name__)
                    self._note_failure(f"poll error {type(exc).__name__}")
                if self._stopped:
                    break
                delay = max(0.2, self._poll_interval() - (time.monotonic() - started))
                try:
                    await asyncio.wait_for(self._wake.wait(), delay)
                except asyncio.TimeoutError:
                    pass
                self._wake.clear()
        finally:
            if self.state != "legacy":
                self._release_memory()
                self.state = "idle"

    def _poll_interval(self) -> float:
        if not self._ready.is_set():
            return 0.5
        if not self.target_duration:
            return 1.0
        return max(1.0, min(self.target_duration / 2.0, 4.0))

    def _release_memory(self) -> None:
        # Segments that carried a discontinuity tag leave the playlist here.
        self.discontinuity_seq += sum(1 for s in self.window if s.discontinuity)
        self.window.clear()
        self.grace.clear()
        self._window_bytes = self._grace_bytes = 0
        self._ready.clear()
        # Force a fresh live-edge start (and a new timestamp epoch) next time.
        self.source = None
        self.media_url = None
        self.last_useq = None

    # -- polling ------------------------------------------------------------

    async def _poll_once(self) -> None:
        spec = self.hooks.resolve_source(self.channel_id)
        if spec is None:
            return
        if self.source is None or spec.key != self.source.key:
            self._switch_source(spec)
        elif spec != self.source:
            # Same stream, new token/headers (or another Multi-View audio output
            # of the same run): keep the sequence position, re-resolve variants.
            self.source = spec
            self.media_url = None

        playlist = await self._load_media_playlist()
        if playlist is None:
            self.stats["playlist_failures"] += 1
            self._note_failure("playlist unavailable")
            self._check_stale()
            return
        if playlist.has_map or playlist.sample_aes:
            self._incompatible("fMP4 or SAMPLE-AES source")
            return
        self.consecutive_failures = 0

        segments = playlist.segments
        if not segments:
            self._check_stale()
            return
        declared = playlist.target_duration or max(s.duration for s in segments)
        self.target_duration = max(self.target_duration, int(math.ceil(declared - 1e-6)))

        if self.last_useq is None:
            new = segments[-self.cfg.live_edge_segments:]
        else:
            newest = segments[-1].useq
            if newest < self.last_useq:
                if self.last_useq - newest <= len(segments) + 3:
                    # A lagging CDN edge served an older copy; just wait.
                    self._check_stale()
                    return
                LOGGER.info("Upstream media sequence reset channel=%s", self.channel_id)
                self.normalizer.start_new_epoch()
                self.pending_discontinuity = True
                new = segments[-self.cfg.live_edge_segments:]
            else:
                new = [s for s in segments if s.useq > self.last_useq]
                if len(new) > self.cfg.max_catchup_segments:
                    # Far behind (e.g. after a stall): jump to the live edge.
                    new = new[-self.cfg.live_edge_segments:]
                    self.normalizer.start_new_epoch()
                    self.pending_discontinuity = True

        if not new:
            if playlist.endlist:
                self._report_failure("upstream playlist ended")
            else:
                self._check_stale()
            return
        await self._ingest(new)

    def _switch_source(self, spec: SourceSpec) -> None:
        if self.source is not None:
            self.stats["source_switches"] += 1
            LOGGER.info(
                "Channel session switching source channel=%s from=%s to=%s",
                self.channel_id, self.source.label or self.source.key[0], spec.label or spec.key[0],
            )
        self.source = spec
        self.media_url = None
        self.last_useq = None
        self.normalizer.start_new_epoch()
        self.pending_discontinuity = bool(self.window)
        self.consecutive_failures = 0
        self.consecutive_segment_failures = 0
        self.last_new_segment_at = time.monotonic()
        self._keys.clear()

    async def _load_media_playlist(self) -> Optional[MediaPlaylist]:
        source = self.source
        if source is None:
            return None
        if source.local:
            text = await asyncio.to_thread(_read_text_file, source.url, self.cfg.max_playlist_bytes)
            if text is None:
                return None
            return parse_media_playlist(text, source.url, local=True)

        headers = self.hooks.headers_for(source.referer, source.origin)
        url = self.media_url or source.url
        result = await self.hooks.fetch(url, headers, self.cfg.max_playlist_bytes, self.cfg.playlist_timeout)
        if result is None or result.status != 200:
            if self.media_url and result is not None and result.status in (401, 403, 404, 410):
                self.media_url = None  # variant token expired: re-resolve the master next poll
            return None
        text = result.body.decode("utf-8", errors="replace")
        if is_master_playlist(text):
            variant, demuxed = choose_variant(parse_master_playlist(text, result.url), self.cfg.bandwidth_cap)
            if variant is None:
                return None
            if demuxed:
                self._incompatible("separate audio rendition")
                return None
            self.media_url = variant.uri
            result = await self.hooks.fetch(variant.uri, headers, self.cfg.max_playlist_bytes, self.cfg.playlist_timeout)
            if result is None or result.status != 200:
                self.media_url = None
                return None
            text = result.body.decode("utf-8", errors="replace")
            if is_master_playlist(text):
                return None
        elif self.media_url is None:
            self.media_url = result.url
        return parse_media_playlist(text, result.url)

    async def _ingest(self, new: List[UpstreamSegment]) -> None:
        semaphore = asyncio.Semaphore(self.cfg.download_concurrency)

        async def bounded(segment: UpstreamSegment) -> Optional[bytes]:
            async with semaphore:
                return await self._download(segment)

        downloads = [asyncio.create_task(bounded(segment)) for segment in new]
        try:
            for segment, task in zip(new, downloads):
                data = await task
                if self.source is None:
                    return
                self.last_useq = segment.useq
                if segment.discontinuity:
                    self.normalizer.start_new_epoch()
                    self.pending_discontinuity = bool(self.window)
                if data is None:
                    self.stats["segment_failures"] += 1
                    self.consecutive_segment_failures += 1
                    self.pending_discontinuity = bool(self.window)
                    if self.consecutive_segment_failures >= self.cfg.fail_threshold:
                        self._report_failure("segments failing")
                    continue
                if find_ts_start(data) < 0:
                    self._incompatible("segments are not MPEG-TS")
                    return
                result = await asyncio.to_thread(self.normalizer.normalize, data, segment.duration)
                self._on_media_info(result.has_audio, result.codec_signature)
                self._publish(SessionSegment(
                    seq=self.next_seq,
                    duration=segment.duration,
                    discontinuity=self.pending_discontinuity and bool(self.window),
                    data=result.data,
                ))
        finally:
            for task in downloads:
                if not task.done():
                    task.cancel()

    def _on_media_info(self, has_audio: bool, signature: Tuple) -> None:
        changed = self.has_audio != has_audio or self.codec_signature != signature
        self.has_audio = has_audio
        self.codec_signature = signature
        if changed and self.hooks.on_media_info and self.source is not None:
            try:
                self.hooks.on_media_info(self.channel_id, self.source.key, has_audio, signature)
            except Exception:  # hooks must not break ingestion
                LOGGER.debug("on_media_info hook failed channel=%s", self.channel_id)

    def _publish(self, segment: SessionSegment) -> None:
        self.next_seq += 1
        self.pending_discontinuity = False
        self.consecutive_segment_failures = 0
        self.last_new_segment_at = time.monotonic()
        self.stats["segments"] += 1
        self.stats["bytes"] += len(segment.data)
        self.target_duration = max(self.target_duration, int(math.ceil(segment.duration - 1e-6)))
        self.window.append(segment)
        self._window_bytes += len(segment.data)
        self._trim_window()
        if len(self.window) >= self.cfg.min_start_segments or (
            self.state == "live" and self.window
        ):
            self.state = "live"
            self._ready.set()

    def _trim_window(self) -> None:
        def window_seconds() -> float:
            return sum(s.duration for s in self.window)

        while len(self.window) > self.cfg.window_max_segments or (
            len(self.window) > self.cfg.window_min_segments
            and window_seconds() - self.window[0].duration >= self.cfg.window_min_seconds
        ):
            removed = self.window.popleft()
            self._window_bytes -= len(removed.data)
            if removed.discontinuity:
                self.discontinuity_seq += 1
            self.grace[removed.seq] = removed
            self._grace_bytes += len(removed.data)
        while self.grace and (
            len(self.grace) > self.cfg.grace_segments
            or self._window_bytes + self._grace_bytes > self.cfg.max_session_bytes
        ):
            _, dropped = self.grace.popitem(last=False)
            self._grace_bytes -= len(dropped.data)

    async def _download(self, segment: UpstreamSegment) -> Optional[bytes]:
        source = self.source
        if source is None:
            return None
        if source.local:
            data = await asyncio.to_thread(_read_bytes_file, segment.uri, self.cfg.max_segment_bytes)
            return data or None

        headers = self.hooks.headers_for(source.referer, source.origin)
        if segment.byterange:
            length, offset = segment.byterange
            headers["Range"] = f"bytes={offset}-{offset + length - 1}"
        data: Optional[bytes] = None
        for attempt in range(2):
            result = await self.hooks.fetch(segment.uri, headers, self.cfg.max_segment_bytes, self.cfg.segment_timeout)
            if result is not None and result.status in (200, 206) and result.body:
                data = result.body
                if segment.byterange and result.status == 200:
                    length, offset = segment.byterange
                    data = data[offset:offset + length]
                break
            if result is not None and result.status in (401, 403, 404, 410):
                break  # gone; retrying won't help
            if attempt == 0:
                await asyncio.sleep(0.3)
        if data is None:
            return None

        if segment.key is not None and segment.key.method == "AES-128":
            key = await self._get_key(segment.key.uri, headers)
            if key is None:
                return None
            iv = segment.key.iv or segment.useq.to_bytes(16, "big")
            try:
                data = await asyncio.to_thread(aes128_decrypt, data, key, iv)
            except Exception:
                LOGGER.warning("AES-128 segment decryption failed channel=%s", self.channel_id)
                return None
        return data

    async def _get_key(self, uri: str, headers: Dict[str, str]) -> Optional[bytes]:
        if uri in self._keys:
            return self._keys[uri]
        key_headers = {k: v for k, v in headers.items() if k.lower() != "range"}
        result = await self.hooks.fetch(uri, key_headers, 1024, self.cfg.playlist_timeout)
        if result is None or result.status != 200 or len(result.body) != 16:
            return None
        self._keys[uri] = result.body
        while len(self._keys) > 16:
            self._keys.popitem(last=False)
        return result.body

    # -- health -------------------------------------------------------------

    def _note_failure(self, reason: str) -> None:
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.cfg.fail_threshold:
            self._report_failure(reason)

    def _check_stale(self) -> None:
        stale_after = max(3 * max(self.target_duration, 1), self.cfg.stale_min_seconds)
        if time.monotonic() - self.last_new_segment_at > stale_after:
            self._report_failure("playlist stale")

    def _report_failure(self, reason: str) -> None:
        if self.source is None:
            return
        now = time.monotonic()
        key = self.source.key
        if now - self._last_report.get(key, 0.0) < self.cfg.failure_report_cooldown:
            return
        self._last_report[key] = now
        self.last_new_segment_at = now  # don't re-fire every poll while failover happens
        self.consecutive_failures = 0
        self.stats["failover_requests"] += 1
        LOGGER.warning("Channel session requesting failover channel=%s reason=%s", self.channel_id, reason)
        try:
            self.hooks.report_failure(self.channel_id, key, reason)
        except Exception:
            LOGGER.exception("report_failure hook failed channel=%s", self.channel_id)

    def _incompatible(self, reason: str) -> None:
        if self.source is None:
            return
        if not self.window:
            # Nothing served yet: let this viewer use the legacy passthrough proxy.
            LOGGER.info("Channel session using legacy proxy channel=%s reason=%s", self.channel_id, reason)
            self.state = "legacy"
            self.legacy_reason = reason
            self.legacy_since = time.monotonic()
            self._stopped = True
            self._ready.set()
            return
        key = self.source.key
        now = time.monotonic()
        if now - self._last_report.get(key, 0.0) < self.cfg.failure_report_cooldown:
            return
        self._last_report[key] = now
        LOGGER.warning("Channel session source incompatible channel=%s reason=%s", self.channel_id, reason)
        try:
            self.hooks.report_incompatible(self.channel_id, key, reason)
        except Exception:
            LOGGER.exception("report_incompatible hook failed channel=%s", self.channel_id)


def _read_text_file(path: str, max_bytes: int) -> Optional[str]:
    try:
        with open(path, "rb") as handle:
            data = handle.read(max_bytes + 1)
    except OSError:
        return None
    if len(data) > max_bytes:
        return None
    return data.decode("utf-8", errors="replace")


def _read_bytes_file(path: str, max_bytes: int) -> Optional[bytes]:
    try:
        with open(path, "rb") as handle:
            data = handle.read(max_bytes + 1)
    except OSError:
        return None
    return data if len(data) <= max_bytes else None


class SessionRegistry:
    """Owns every channel session. Sessions (and their sequence counters) live
    for the life of the process; only their poll loops and segment memory come
    and go with viewers."""

    def __init__(self, hooks: SessionHooks, config: Optional[SessionConfig] = None) -> None:
        self.hooks = hooks
        self.config = config or SessionConfig()
        self.sessions: Dict[str, ChannelSession] = {}

    def get(self, channel_id: str) -> ChannelSession:
        session = self.sessions.get(channel_id)
        if session is None:
            session = ChannelSession(channel_id, self.hooks, self.config)
            self.sessions[channel_id] = session
        return session

    def peek(self, channel_id: str) -> Optional[ChannelSession]:
        return self.sessions.get(channel_id)

    def poke(self, channel_id: str) -> None:
        session = self.sessions.get(channel_id)
        if session is not None:
            session.poke()

    def is_watched(self, channel_id: str) -> bool:
        session = self.sessions.get(channel_id)
        return session is not None and session.is_watched()

    async def close(self, channel_id: str) -> None:
        session = self.sessions.pop(channel_id, None)
        if session is not None:
            await session.close()

    async def close_all(self) -> None:
        sessions = list(self.sessions.values())
        self.sessions.clear()
        await asyncio.gather(*(s.close() for s in sessions), return_exceptions=True)

    def snapshot(self) -> Dict[str, dict]:
        return {cid: s.snapshot() for cid, s in self.sessions.items() if s.is_running or s.state == "legacy"}
