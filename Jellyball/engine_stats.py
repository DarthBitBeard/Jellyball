"""Counters for streaming-engine decisions: why and for which provider a channel
session handed a source to the legacy passthrough proxy.

These numbers are what decides whether an ffmpeg remux path for fMP4 /
demuxed-audio sources (E4b) is worth enabling by default. In-memory, reset on
restart, like the failover counters; exposed on /metrics and the dashboard.
"""

import threading
from typing import Any, Dict, List, Tuple

# kind -> label shown in the dashboard
LEGACY_REASONS: Dict[str, str] = {
    "fmp4": "fMP4 / CMAF segments",
    "sample_aes": "SAMPLE-AES encryption",
    "demuxed_audio": "Separate audio rendition",
    "not_ts": "Segments are not MPEG-TS",
}
UNKNOWN_PROVIDER = "unknown"

_LOCK = threading.Lock()
_LEGACY_FALLBACKS: Dict[Tuple[str, str], int] = {}
# Cold-start candidate races: total runs vs runs that picked a healthy source.
_COLD_RACES: Dict[str, int] = {"total": 0, "won": 0}
# No-Signal placeholder starts (cold starts with no healthy candidate, and
# slow starters): how often tune-ins degrade to the placeholder.
_PLACEHOLDER_FALLBACKS = 0
# Time-to-first-segment per session run, in ms.
_TUNE_IN_MS: Dict[str, float] = {"count": 0, "total": 0.0, "max": 0.0}


def record_legacy_fallback(kind: str, provider: str) -> None:
    key = (kind if kind in LEGACY_REASONS else "other", (provider or UNKNOWN_PROVIDER).strip().lower() or UNKNOWN_PROVIDER)
    with _LOCK:
        _LEGACY_FALLBACKS[key] = _LEGACY_FALLBACKS.get(key, 0) + 1


def legacy_fallback_counts() -> List[Dict[str, Any]]:
    """[{reason, provider, count}] sorted by count (desc), then reason/provider."""
    with _LOCK:
        items = list(_LEGACY_FALLBACKS.items())
    rows: List[Dict[str, Any]] = [{"reason": reason, "provider": provider, "count": count} for (reason, provider), count in items]
    rows.sort(key=lambda row: (-row["count"], row["reason"], row["provider"]))
    return rows


def legacy_fallback_totals() -> Dict[str, int]:
    totals: Dict[str, int] = {}
    for row in legacy_fallback_counts():
        totals[row["reason"]] = totals.get(row["reason"], 0) + row["count"]
    return totals


def reset() -> None:
    with _LOCK:
        _LEGACY_FALLBACKS.clear()
        _COLD_RACES["total"] = 0
        _COLD_RACES["won"] = 0
        global _PLACEHOLDER_FALLBACKS
        _PLACEHOLDER_FALLBACKS = 0
        _TUNE_IN_MS["count"] = 0
        _TUNE_IN_MS["total"] = 0.0
        _TUNE_IN_MS["max"] = 0.0


def record_cold_race(won: bool) -> None:
    """One cold-start candidate race finished; won=True when it picked a
    healthy candidate (otherwise the tune-in started on the placeholder)."""
    with _LOCK:
        _COLD_RACES["total"] += 1
        if won:
            _COLD_RACES["won"] += 1


def cold_race_stats() -> Dict[str, int]:
    with _LOCK:
        return dict(_COLD_RACES)


def record_placeholder_fallback() -> None:
    with _LOCK:
        global _PLACEHOLDER_FALLBACKS
        _PLACEHOLDER_FALLBACKS += 1


def placeholder_fallback_total() -> int:
    with _LOCK:
        return _PLACEHOLDER_FALLBACKS


def record_tune_in_latency(ms: float) -> None:
    """Time-to-first-segment for one session run, in milliseconds."""
    with _LOCK:
        _TUNE_IN_MS["count"] += 1
        _TUNE_IN_MS["total"] += ms
        _TUNE_IN_MS["max"] = max(_TUNE_IN_MS["max"], ms)


def tune_in_latency_stats() -> Dict[str, float]:
    """{"runs": n, "avg_ms": ..., "max_ms": ...} across session runs."""
    with _LOCK:
        count = _TUNE_IN_MS["count"]
        return {
            "runs": count,
            "avg_ms": round(_TUNE_IN_MS["total"] / count, 1) if count else 0.0,
            "max_ms": round(_TUNE_IN_MS["max"], 1),
        }
