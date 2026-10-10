"""Counters for streaming-engine decisions: remux ingest events (3.0 replaced the
legacy passthrough proxy with per-source ffmpeg remuxing), cold-start races,
placeholder fallbacks, and tune-in latency.

In-memory, reset on restart, like the failover counters; exposed on /metrics
and the dashboard.
"""

import threading
from typing import Any, Dict, List, Tuple

# event -> label shown in the dashboard
REMUX_EVENTS: Dict[str, str] = {
    "started": "Remux sessions started",
    "transcode": "Fell back to -c:a aac transcode",
    "failed": "Remux failed",
}
UNKNOWN_PROVIDER = "unknown"

_LOCK = threading.Lock()
_REMUX_EVENTS: Dict[Tuple[str, str], int] = {}
# Cold-start candidate races: total runs vs runs that picked a healthy source.
_COLD_RACES: Dict[str, int] = {"total": 0, "won": 0}
# No-Signal placeholder starts (cold starts with no healthy candidate, and
# slow starters): how often tune-ins degrade to the placeholder.
_PLACEHOLDER_FALLBACKS = 0
# Time-to-first-segment per session run, in ms.
_TUNE_IN_MS: Dict[str, float] = {"count": 0, "total": 0.0, "max": 0.0}


def record_remux_event(event: str, provider: str) -> None:
    key = (event if event in REMUX_EVENTS else "other", (provider or UNKNOWN_PROVIDER).strip().lower() or UNKNOWN_PROVIDER)
    with _LOCK:
        _REMUX_EVENTS[key] = _REMUX_EVENTS.get(key, 0) + 1


def remux_counts() -> List[Dict[str, Any]]:
    """[{event, provider, count}] sorted by count (desc), then event/provider."""
    with _LOCK:
        items = list(_REMUX_EVENTS.items())
    rows: List[Dict[str, Any]] = [{"event": event, "provider": provider, "count": count} for (event, provider), count in items]
    rows.sort(key=lambda row: (-row["count"], row["event"], row["provider"]))
    return rows


def remux_totals() -> Dict[str, int]:
    totals: Dict[str, int] = {}
    for row in remux_counts():
        totals[row["event"]] = totals.get(row["event"], 0) + row["count"]
    return totals


def reset() -> None:
    with _LOCK:
        _REMUX_EVENTS.clear()
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
