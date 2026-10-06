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
import engine_stats

LOGGER = logging.getLogger("jellyball.session")

try:  # AES-128 HLS decryption
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
except ImportError:  # pragma: no cover - dependency is pinned in requirements
    Cipher = None


# ---------------------------------------------------------------------------
# Persisted sequence high-water mark
# ---------------------------------------------------------------------------

# app_settings key holding the highest media sequence number any session has
# published. A restart seeds next_seq from max(persisted, wall clock) so an
# NTP step backwards (or a clock change) can't regress sequence numbers and
# confuse players mid-guide-refresh.
_NEXT_SEQ_SETTING_KEY = "engine.next_seq_max"
_NEXT_SEQ_PERSIST_SECONDS = 60.0
# In-process high-water mark: sessions only write when they exceed it, so one
# DB write per minute per active channel is the worst case.
_NEXT_SEQ_HIGH_WATER = 0


def _load_persisted_next_seq() -> int:
    """Seed for a new ChannelSession's next_seq."""
    global _NEXT_SEQ_HIGH_WATER
    persisted = 0
    try:
        # Late import: db is app wiring; this module stays importable without it.
        from db import get_setting
        persisted = int(get_setting(_NEXT_SEQ_SETTING_KEY, "0") or 0)
    except Exception:
        persisted = 0
    _NEXT_SEQ_HIGH_WATER = max(_NEXT_SEQ_HIGH_WATER, persisted)
    return max(_NEXT_SEQ_HIGH_WATER, int(time.time()))


def _persist_next_seq(value: int) -> None:
    """Durably record a new high-water mark (best effort)."""
    global _NEXT_SEQ_HIGH_WATER
    if value <= _NEXT_SEQ_HIGH_WATER:
        return
    _NEXT_SEQ_HIGH_WATER = value
    try:
        from db import set_setting
        set_setting(_NEXT_SEQ_SETTING_KEY, str(value))
    except Exception:
        LOGGER.debug("persist next_seq failed value=%d", value)


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
    # Cookie header captured from the provider's browser session at scrape
    # time; lets upstream media fetches pass the CDN checks the browser did.
    cookies: str = ""


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
    removed_at: float = 0.0  # when it left the window (grace buffer aging)


@dataclass
class SessionConfig:
    idle_timeout: float = 60.0
    min_start_segments: int = 3
    live_edge_segments: int = 3
    window_min_segments: int = 6
    window_min_seconds: float = 30.0
    window_max_segments: int = 30
    grace_segments: int = 12
    grace_seconds: float = 45.0  # rolled-off segments stay fetchable this long
    max_session_bytes: int = 256 * 1024 * 1024
    stale_min_seconds: float = 15.0
    fail_threshold: int = 3
    failure_report_cooldown: float = 10.0
    playlist_timeout: float = 8.0
    segment_timeout: float = 15.0
    # A segment that isn't downloaded within max(min, factor x its duration) is
    # skipped (with a discontinuity) instead of holding back the ones after it.
    segment_deadline_min: float = 8.0
    segment_deadline_factor: float = 2.0
    max_target_duration: int = 15  # clamp: one bogus EXTINF can't disable stale detection
    max_playlist_bytes: int = 2 * 1024 * 1024
    max_segment_bytes: int = 48 * 1024 * 1024
    bandwidth_cap: int = 0  # 0 = pick the highest-bandwidth variant
    max_catchup_segments: int = 8
    download_concurrency: int = 3
    legacy_retry_seconds: float = 300.0
    non_ts_threshold: int = 3  # consecutive non-TS segments before a source is incompatible


@dataclass
class SessionHooks:
    fetch: Callable[[str, Dict[str, str], int, float], Awaitable[Optional[FetchResult]]]
    headers_for: Callable[[SourceSpec], Dict[str, str]]
    resolve_source: Callable[[str], Optional[SourceSpec]]
    report_failure: Callable[[str, Tuple, str], None]
    # Returns True when another (compatible) source will be tried, so a cold
    # start doesn't fall back to the legacy proxy while better sources exist.
    report_incompatible: Callable[[str, Tuple, str], Optional[bool]]
    on_media_info: Optional[Callable[[str, Tuple, bool, Tuple], None]] = None
    # The audio language to prefer (ISO 639-2) or None for the first audio
    # stream. Read at every source switch, so a change applies to the next one.
    preferred_audio_language: Optional[Callable[[], Optional[str]]] = None
    # (channel_id, kind, source_key) when a cold-starting session gives up and
    # hands the channel to the legacy proxy. kind: fmp4 | sample_aes |
    # demuxed_audio | not_ts. Counted by engine_stats.
    on_legacy_fallback: Optional[Callable[[str, str, Tuple], None]] = None


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
        # Seeded from max(persisted high-water mark, wall clock) so numbering
        # never goes backwards, even across an application restart combined
        # with an NTP step backwards.
        self.next_seq = _load_persisted_next_seq()
        self._next_seq_persisted_at = 0.0
        # Time to first published segment for the current run (ms); reset on
        # every new run. None until the first segment is published.
        self.tune_in_latency_ms: Optional[float] = None
        self.discontinuity_seq = 0
        self.target_duration = 0
        self.source: Optional[SourceSpec] = None
        self.media_url: Optional[str] = None
        self.last_useq: Optional[int] = None
        self.pending_discontinuity = False
        self.normalizer = TsNormalizer()
        self._sync_audio_preference()
        self.state = "idle"  # idle | starting | live | legacy
        self.legacy_reason = ""
        self.legacy_since = 0.0
        self.last_access = time.monotonic()
        # When the last segment was published (drives is_flowing and the
        # failover catch-up gap). The stale timer has its own start so reporting
        # a failure doesn't make the channel look like it is flowing again.
        self.last_new_segment_at = time.monotonic()
        self._stale_timer_start = time.monotonic()
        self.consecutive_failures = 0
        self.consecutive_segment_failures = 0
        self.consecutive_non_ts = 0
        self.last_playlist_status: Optional[int] = None
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
        self._last_report: "OrderedDict[Tuple, float]" = OrderedDict()
        self._keys: "OrderedDict[str, bytes]" = OrderedDict()
        self._switch_gap: Optional[float] = None
        self._reset_seen_at: Optional[int] = None  # upstream sequence reset awaiting confirmation
        # Observability: recent segment download times and sizes.
        self._download_seconds: Deque[float] = deque(maxlen=60)
        self._recent_segments: Deque[Tuple[int, float]] = deque(maxlen=30)  # (bytes, duration)
        self.started_at: Optional[float] = None
        self._resolved_once = False
        self._last_poll_error_log: Dict[str, float] = {}
        self._last_playlist_status_log: Tuple[Optional[int], float] = (None, -1e9)

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
            self.started_at = time.monotonic()
            # A new run measures its own time-to-first-segment.
            self.tune_in_latency_ms = None
            self._task = asyncio.create_task(self._run(), name=f"hls session {self.channel_id}")

    def reset_legacy(self) -> None:
        """Leave the legacy passthrough so the next poll uses the session engine.

        Called when failover moved the active candidate while this session was
        parked in legacy state: the new candidate is session-compatible, so
        serving it through the legacy passthrough for the rest of
        legacy_retry_seconds would waste the normalizing engine (and its real
        playback failover). Safe to call when not in legacy state.
        """
        if self.state != "legacy":
            return
        LOGGER.info("Channel session leaving legacy proxy after failover channel=%s", self.channel_id)
        self.state = "idle"
        self.legacy_reason = ""
        self.legacy_since = 0.0
        self._ready.clear()
        self.ensure_running()

    def poke(self) -> None:
        """Poll now (e.g. right after failover moved the active candidate)."""
        self._wake.set()

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def is_watched(self, within: Optional[float] = None) -> bool:
        within = self.cfg.idle_timeout if within is None else within
        return self.is_running and time.monotonic() - self.last_access < within

    def _stale_after(self) -> float:
        return max(3 * max(self.target_duration, 1), self.cfg.stale_min_seconds)

    def is_flowing(self) -> bool:
        """True while new segments keep arriving from the current source."""
        return self.state == "live" and time.monotonic() - self.last_new_segment_at < self._stale_after()

    @property
    def is_cold(self) -> bool:
        """True before the session has ever served segments. A cold session has
        nothing to lose by abandoning a dead candidate fast."""
        return not self._ready.is_set()

    def _fail_threshold(self) -> int:
        """Consecutive playlist failures before reporting. Cold sessions fail
        over on the first failure (a dead candidate must not eat the startup
        budget); live sessions keep the configured tolerance so a transient
        hiccup doesn't interrupt playback. Segment downloads keep the
        configured threshold in both cases: one bad segment doesn't prove the
        source is dead."""
        if self.is_cold:
            return 1
        return self.cfg.fail_threshold

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

    def _sync_audio_preference(self) -> None:
        getter = self.hooks.preferred_audio_language
        if getter is None:
            return
        try:
            self.normalizer.preferred_audio_language = getter() or None
        except Exception:
            LOGGER.exception("preferred_audio_language hook failed channel=%s", self.channel_id)

    def metrics(self) -> dict:
        """Recent throughput/latency numbers for the dashboard and /api/sessions."""
        latencies = sorted(self._download_seconds)
        total_bytes = sum(b for b, _ in self._recent_segments)
        total_seconds = sum(d for _, d in self._recent_segments)
        p95 = latencies[min(len(latencies) - 1, int(math.ceil(0.95 * len(latencies))) - 1)] if latencies else None
        return {
            "bitrate_kbps": round(total_bytes * 8 / total_seconds / 1000) if total_seconds > 0 else None,
            "segment_ms_avg": round(1000 * sum(latencies) / len(latencies)) if latencies else None,
            "segment_ms_p95": round(1000 * p95) if p95 is not None else None,
            "seconds_since_segment": round(time.monotonic() - self.last_new_segment_at, 1) if self.window else None,
            "uptime_seconds": round(time.monotonic() - self.started_at) if self.started_at and self.is_running else 0,
            "tune_in_latency_ms": self.tune_in_latency_ms,
        }

    def snapshot(self) -> dict:
        return {
            **self.metrics(),
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
        # Durable high-water mark so a restart never regresses numbering.
        _persist_next_seq(self.next_seq)

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
                    self._log_poll_error(exc)
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

    def _log_poll_error(self, exc: BaseException) -> None:
        """One line per error type per minute (a stuck bug used to log twice a second)."""
        name = type(exc).__name__
        now = time.monotonic()
        if now - self._last_poll_error_log.get(name, -1e9) < 60.0:
            return
        self._last_poll_error_log[name] = now
        LOGGER.warning(
            "Channel session poll failed channel=%s error=%s detail=%s",
            self.channel_id, name, str(exc)[:160], exc_info=LOGGER.isEnabledFor(logging.DEBUG),
        )

    def _log_playlist_failure(self) -> None:
        """Throttled: the upstream HTTP status on playlist fetch failure, so an
        all-403 (WAF/token) situation is diagnosable from the log instead of
        just counted."""
        status = self.last_playlist_status
        now = time.monotonic()
        last = self._last_playlist_status_log
        if last[0] == status and now - last[1] < 60.0:
            return
        self._last_playlist_status_log = (status, now)
        LOGGER.warning(
            "Upstream playlist fetch failed channel=%s source=%s status=%s",
            self.channel_id,
            self.source.label if self.source else "?",
            status if status is not None else "no-response",
        )

    def _poll_interval(self) -> float:
        if not self._ready.is_set():
            if not self._resolved_once:
                return 1.0  # nothing to play yet (placeholder/Multi-View spinning up)
            if not self.target_duration:
                return 0.5  # first playlist not parsed yet
            return max(0.5, min(self.target_duration / 2.0, 2.0))
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
        # A new run means new clients: the sticky target duration can reset.
        self.target_duration = 0
        self._reset_seen_at = None

    # -- polling ------------------------------------------------------------

    async def _poll_once(self) -> None:
        spec = self.hooks.resolve_source(self.channel_id)
        if spec is None:
            self._resolved_once = False
            return
        self._resolved_once = True
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
            self._log_playlist_failure()
            self._note_failure("playlist unavailable")
            self._check_stale()
            return
        if playlist.has_map or playlist.sample_aes:
            self._incompatible("fMP4 or SAMPLE-AES source", "sample_aes" if playlist.sample_aes else "fmp4")
            return
        self.consecutive_failures = 0

        segments = playlist.segments
        if not segments:
            self._check_stale()
            return
        declared = playlist.target_duration or max(s.duration for s in segments)
        self._raise_target_duration(declared)

        if self.last_useq is None:
            new = segments[-self._live_edge_count(segments):]
        else:
            newest = segments[-1].useq
            if newest < self.last_useq:
                if self.last_useq - newest <= len(segments) + 3:
                    # A lagging CDN edge served an older copy; just wait.
                    self._check_stale()
                    return
                if self._reset_seen_at is None:
                    # One far-behind response is more often a stale edge than
                    # a real encoder restart: wait for a second one before
                    # jumping (which would replay old content).
                    self._reset_seen_at = newest
                    self._check_stale()
                    return
                self._reset_seen_at = None
                LOGGER.info("Upstream media sequence reset channel=%s", self.channel_id)
                self.normalizer.start_new_epoch()
                self.pending_discontinuity = True
                new = segments[-self.cfg.live_edge_segments:]
            else:
                self._reset_seen_at = None
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
        # Durable sequence high-water mark, throttled (see _persist_next_seq).
        now = time.monotonic()
        if now - self._next_seq_persisted_at >= _NEXT_SEQ_PERSIST_SECONDS:
            self._next_seq_persisted_at = now
            _persist_next_seq(self.next_seq)

    def _live_edge_count(self, segments: List[UpstreamSegment]) -> int:
        gap = self._switch_gap
        self._switch_gap = None
        limit = max(1, min(self.cfg.live_edge_segments, len(segments)))
        if gap is None:
            return limit  # cold start: a few segments of buffer for the player
        # Cover the outage as closely as possible: always rounding up added up
        # to a segment of replayed content per failover (the soak test saw the
        # viewer drift ~2 s behind live per failover).
        covered = 0.0
        count = 0
        for segment in reversed(segments[-limit:]):
            if count and covered + segment.duration / 2.0 > gap:
                break
            count += 1
            covered += segment.duration
        return count

    def _switch_source(self, spec: SourceSpec) -> None:
        if self.source is not None:
            self.stats["source_switches"] += 1
            LOGGER.info(
                "Channel session switching source channel=%s from=%s to=%s",
                self.channel_id, self.source.label or self.source.key[0], spec.label or spec.key[0],
            )
        # How much real time passed since the last segment we published: on
        # a mid-playback switch only that much of the new source is appended
        # (see _live_edge_count). Appending a fixed 3 live-edge segments made
        # every failover replay a few seconds, so viewers drifted ~3s further
        # behind live per failover (seen in the soak test).
        self._switch_gap = (time.monotonic() - self.last_new_segment_at) if self.window else None
        self.source = spec
        self.media_url = None
        self.last_useq = None
        self._sync_audio_preference()
        self.normalizer.start_new_epoch()
        self.pending_discontinuity = bool(self.window)
        self.consecutive_failures = 0
        self.consecutive_segment_failures = 0
        self.consecutive_non_ts = 0
        self._reset_seen_at = None
        # The new source gets a full stale window; last_new_segment_at keeps
        # the real publish time (is_flowing stays False until it delivers).
        self._stale_timer_start = time.monotonic()
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

        headers = self.hooks.headers_for(source)
        url = self.media_url or source.url
        result = await self.hooks.fetch(url, headers, self.cfg.max_playlist_bytes, self.cfg.playlist_timeout)
        self.last_playlist_status = result.status if result is not None else None
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
                self._incompatible("separate audio rendition", "demuxed_audio")
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

    def _segment_deadline(self, segment: UpstreamSegment) -> float:
        return max(self.cfg.segment_deadline_min, self.cfg.segment_deadline_factor * max(segment.duration, 0.0))

    async def _ingest(self, new: List[UpstreamSegment]) -> None:
        semaphore = asyncio.Semaphore(self.cfg.download_concurrency)

        async def bounded(segment: UpstreamSegment) -> Optional[bytes]:
            async with semaphore:
                # The deadline covers both download attempts, and starts once a
                # download slot is free (queueing behind others isn't its fault).
                started = time.monotonic()
                try:
                    data = await asyncio.wait_for(self._download(segment), self._segment_deadline(segment))
                except asyncio.TimeoutError:
                    return None
                if data is not None and not (self.source and self.source.local):
                    self._download_seconds.append(time.monotonic() - started)
                return data

        downloads = [asyncio.create_task(bounded(segment)) for segment in new]
        try:
            for segment, task in zip(new, downloads):
                data = await task
                if self.source is None:
                    return
                if data is not None and find_ts_start(data) < 0:
                    # One odd segment (an HTML error page served as 200, an
                    # ID3-only ad slate) is skipped like a failed download; only
                    # a run of them makes the source incompatible.
                    self.consecutive_non_ts += 1
                    if self.consecutive_non_ts >= self.cfg.non_ts_threshold:
                        self._incompatible("segments are not MPEG-TS", "not_ts")
                        return
                    data = None
                elif data is not None:
                    self.consecutive_non_ts = 0
                if data is None:
                    # Do not advance last_useq: a transient 404/timeout must be
                    # retried on the next poll instead of permanently skipped.
                    self.stats["segment_failures"] += 1
                    self.consecutive_segment_failures += 1
                    self.pending_discontinuity = bool(self.window)
                    if self.consecutive_segment_failures >= self.cfg.fail_threshold:
                        self._report_failure("segments failing")
                        return
                    if self._is_stale():
                        # Don't sit through the rest of a failing batch while
                        # the player drains its buffer: fail over now.
                        self._report_failure("playlist stale")
                        return
                    continue
                if segment.discontinuity:
                    self.normalizer.start_new_epoch()
                    self.pending_discontinuity = bool(self.window)
                result = await asyncio.to_thread(self.normalizer.normalize, data, segment.duration)
                if getattr(result, "discontinuity", False):
                    # The normalizer saw a timestamp jump inside the source
                    # (encoder restart without #EXT-X-DISCONTINUITY) and re-based.
                    self.pending_discontinuity = bool(self.window)
                if result.normalized:
                    # Pass-through (unparseable) segments say nothing reliable
                    # about the source's audio/codecs.
                    self._on_media_info(result.has_audio, result.codec_signature)
                self._publish(SessionSegment(
                    seq=self.next_seq,
                    duration=segment.duration,
                    discontinuity=self.pending_discontinuity and bool(self.window),
                    data=result.data,
                ))
                self.last_useq = segment.useq
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

    def _raise_target_duration(self, seconds: float) -> None:
        """Sticky (a playlist's target must not shrink) but clamped."""
        wanted = min(self.cfg.max_target_duration, int(math.ceil(max(seconds, 0.0) - 1e-6)))
        self.target_duration = max(self.target_duration, wanted)

    def _publish(self, segment: SessionSegment) -> None:
        self.next_seq += 1
        if self.tune_in_latency_ms is None and self.started_at is not None:
            # First published segment of this run: time-to-first-segment, the
            # headline number for whether cold-start work is paying off.
            self.tune_in_latency_ms = round((time.monotonic() - self.started_at) * 1000, 1)
            engine_stats.record_tune_in_latency(self.tune_in_latency_ms)
        self.pending_discontinuity = False
        self.consecutive_segment_failures = 0
        self.last_new_segment_at = self._stale_timer_start = time.monotonic()
        self.stats["segments"] += 1
        self.stats["bytes"] += len(segment.data)
        self._recent_segments.append((len(segment.data), segment.duration))
        self._raise_target_duration(segment.duration)
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
            removed.removed_at = time.monotonic()
            self.grace[removed.seq] = removed
            self._grace_bytes += len(removed.data)
        now = time.monotonic()
        while self.grace and (
            len(self.grace) > self.cfg.grace_segments
            or self._window_bytes + self._grace_bytes > self.cfg.max_session_bytes
            or now - next(iter(self.grace.values())).removed_at > self.cfg.grace_seconds
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

        headers = self.hooks.headers_for(source)
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
        if self.consecutive_failures >= self._fail_threshold():
            self._report_failure(reason)

    def _is_stale(self) -> bool:
        return time.monotonic() - max(self.last_new_segment_at, self._stale_timer_start) > self._stale_after()

    def _check_stale(self) -> None:
        if self._is_stale():
            self._report_failure("playlist stale")

    def _note_report(self, key: Tuple, now: float) -> bool:
        """Per-source cooldown; bounded (Multi-View runs add a key per run)."""
        if now - self._last_report.get(key, -1e9) < self.cfg.failure_report_cooldown:
            return False
        self._last_report[key] = now
        self._last_report.move_to_end(key)
        while len(self._last_report) > 32:
            self._last_report.popitem(last=False)
        return True

    def _report_failure(self, reason: str) -> None:
        if self.source is None:
            return
        now = time.monotonic()
        key = self.source.key
        if not self._note_report(key, now):
            return
        if reason == "playlist unavailable" and self.last_playlist_status in (401, 403):
            reason = "playlist forbidden"  # expired token: main refreshes it
        self._stale_timer_start = now  # don't re-fire every poll while failover happens
        self.consecutive_failures = 0
        self.stats["failover_requests"] += 1
        LOGGER.warning("Channel session requesting failover channel=%s reason=%s", self.channel_id, reason)
        try:
            self.hooks.report_failure(self.channel_id, key, reason)
        except Exception:
            LOGGER.exception("report_failure hook failed channel=%s", self.channel_id)

    def _incompatible(self, reason: str, kind: str = "") -> None:
        if self.source is None:
            return
        key = self.source.key
        if not self.window:
            # Nothing served yet. Prefer another, compatible source; only use
            # the legacy passthrough proxy when there is none.
            if self._report_incompatible(key, reason):
                LOGGER.info("Channel session skipping incompatible source channel=%s reason=%s", self.channel_id, reason)
                self.source = None  # re-resolve the (new) active source next poll
                return
            LOGGER.info("Channel session using legacy proxy channel=%s reason=%s", self.channel_id, reason)
            self.state = "legacy"
            self.legacy_reason = reason
            self.legacy_since = time.monotonic()
            self._record_legacy_fallback(kind or "unknown", key)
            self._stopped = True
            self._ready.set()
            return
        if not self._note_report(key, time.monotonic()):
            return
        LOGGER.warning("Channel session source incompatible channel=%s reason=%s", self.channel_id, reason)
        self._report_incompatible(key, reason)

    def _record_legacy_fallback(self, kind: str, key: Tuple) -> None:
        if self.hooks.on_legacy_fallback is None:
            return
        try:
            self.hooks.on_legacy_fallback(self.channel_id, kind, key)
        except Exception:
            LOGGER.exception("on_legacy_fallback hook failed channel=%s", self.channel_id)

    def _report_incompatible(self, key: Tuple, reason: str) -> bool:
        try:
            return bool(self.hooks.report_incompatible(self.channel_id, key, reason))
        except Exception:
            LOGGER.exception("report_incompatible hook failed channel=%s", self.channel_id)
            return False


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
