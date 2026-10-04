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
