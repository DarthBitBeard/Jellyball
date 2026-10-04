"""Provider run telemetry (R1): what each provider search actually did.

Recording "success" the moment search() returns hides a dead site: a provider
whose index page vanished just returns [] forever. This module gives every
search a small RunStats (events listed on the index page, matches, page-level
errors) that the scrapers fill in through report_* hooks, and turns the result
into an outcome: ok / empty / timeout / error, plus a coarse error class.

The stats travel in a ContextVar so scrapers need only a one-line hook and
worker threads (asyncio.to_thread copies the context) report into the same
object. Nothing here stores URLs, hostnames or exception text: only a class.
"""

import asyncio
import contextvars
import ssl
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import httpx

from config import _log_failure
from db import _db_session

OUTCOMES = ("ok", "empty", "timeout", "error")


@dataclass
class RunStats:
    provider: str = ""
    index_events: int = 0
    index_pages: int = 0
    matches: int = 0
    page_errors: List[str] = field(default_factory=list)

    @property
    def index_known(self) -> bool:
        """True once the scraper reported any index-page information."""
        return self.index_pages > 0 or self.index_events > 0 or bool(self.page_errors)


_RUN: "contextvars.ContextVar[Optional[RunStats]]" = contextvars.ContextVar("provider_run_stats", default=None)
# Last finished run per provider, for the dashboard cards (in-memory, resets on restart).
_LAST_RUN: Dict[str, dict] = {}


def begin_run(provider: str) -> RunStats:
    stats = RunStats(provider=provider)
    _RUN.set(stats)
    return stats


def current_run() -> Optional[RunStats]:
    return _RUN.get()


def report_index_events(count: int) -> None:
    """A scraper parsed one index page and saw `count` events listed on it."""
    stats = _RUN.get()
    if stats is not None:
        stats.index_pages += 1
        stats.index_events += max(0, int(count))


def report_matches(count: int) -> None:
    stats = _RUN.get()
    if stats is not None:
        stats.matches += max(0, int(count))


def report_page_error(error: "BaseException | str") -> None:
    """A page-level failure that used to be swallowed (fetch error, empty page,
    bot-check). Logged by the caller; this records only its class."""
    stats = _RUN.get()
    if stats is not None and len(stats.page_errors) < 20:
        stats.page_errors.append(error if isinstance(error, str) else classify_error(error))


def classify_error(exc: BaseException) -> str:
    """A short, stable, secret-free label for an exception."""
    if isinstance(exc, (asyncio.TimeoutError, httpx.TimeoutException, TimeoutError)):
        return "timeout"
    if isinstance(exc, httpx.HTTPStatusError):
        return f"http_{exc.response.status_code}"
    if isinstance(exc, ssl.SSLError):
        return "tls"
    if isinstance(exc, httpx.ConnectError):
        return "connect"
    if isinstance(exc, httpx.TransportError):
        return "network"
    if type(exc).__module__.startswith("playwright"):
        return "browser"
    return "other"


def derive_outcome(
    streams: Optional[list],
    stats: RunStats,
    *,
    exc: Optional[BaseException] = None,
    timed_out: bool = False,
) -> Tuple[str, Optional[str]]:
    """(outcome, error_class) for one finished provider search."""
    if timed_out:
        return "timeout", "timeout"
    if exc is not None:
        return "error", classify_error(exc)
    if streams:
        return "ok", None
    if stats.page_errors and stats.index_events == 0:
        return "error", stats.page_errors[0]
    return "empty", None


def finish_run(provider: str, outcome: str, error_class: Optional[str], stats: RunStats, response_time_ms: int, streams: int) -> None:
    previous = _LAST_RUN.get(provider) or {}
    _LAST_RUN[provider] = {
        "at": time.time(),
        "outcome": outcome,
        "error_class": error_class,
        "index_events": stats.index_events if stats.index_known else None,
        "matches": stats.matches,
        "streams": streams,
        "response_time_ms": response_time_ms,
        "last_success_at": time.time() if outcome == "ok" else previous.get("last_success_at"),
    }


def last_run(provider: str) -> Optional[dict]:
    return _LAST_RUN.get(provider)


# --- persistence -------------------------------------------------------------

def record_outcome_sync(
    provider: str,
    response_time_ms: int,
    outcome: str,
    error_class: Optional[str] = None,
    index_events: Optional[int] = None,
    matches: Optional[int] = None,
) -> None:
    success = 0 if outcome in ("timeout", "error") else 1
    with _db_session() as conn:
        conn.execute(
            "INSERT INTO provider_performance "
            "(provider, response_time_ms, success, outcome, error_class, index_events, matches) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (provider, response_time_ms, success, outcome, error_class, index_events, matches),
        )
        conn.commit()


def provider_stats_sync() -> Dict[str, dict]:
    """Per-provider last success, last-24h outcome counts and latest index_events."""
    stats: Dict[str, dict] = {}
    try:
        with _db_session() as conn:
            for provider, ts in conn.execute(
                "SELECT provider, MAX(timestamp) FROM provider_performance WHERE outcome='ok' GROUP BY provider"
            ).fetchall():
                stats.setdefault(provider, {})["last_success"] = ts
            for provider, outcome, count in conn.execute(
                "SELECT provider, COALESCE(outcome, CASE success WHEN 1 THEN 'ok' ELSE 'error' END), COUNT(*) "
                "FROM provider_performance WHERE timestamp > datetime('now', '-1 day') GROUP BY 1, 2"
            ).fetchall():
                stats.setdefault(provider, {}).setdefault("outcomes_24h", {})[outcome] = count
            for provider, events, ts in conn.execute(
                "SELECT p.provider, p.index_events, p.timestamp FROM provider_performance p "
                "WHERE p.index_events IS NOT NULL AND p.id = ("
                "  SELECT MAX(id) FROM provider_performance q "
                "  WHERE q.provider = p.provider AND q.index_events IS NOT NULL)"
            ).fetchall():
                entry = stats.setdefault(provider, {})
                entry["index_events"] = events
                entry["index_events_at"] = ts
    except Exception as exc:  # noqa: BLE001 - a dashboard read must not raise
        _log_failure("read provider stats", exc)
    return stats


def silent_providers_sync(window_hours: int = 6, min_samples: int = 5) -> List[dict]:
    """Providers whose index page has listed zero events in every recent run
    that reported one, with no successful search: the dead-site signal."""
    silent: List[dict] = []
    try:
        with _db_session() as conn:
            rows = conn.execute(
                "SELECT provider, COUNT(*), COALESCE(SUM(index_events), 0), "
                "SUM(CASE WHEN outcome='ok' THEN 1 ELSE 0 END) "
                "FROM provider_performance "
                "WHERE index_events IS NOT NULL AND timestamp > datetime('now', ?) GROUP BY provider",
                (f"-{int(window_hours)} hours",),
            ).fetchall()
        for provider, samples, total_events, ok in rows:
            if samples >= min_samples and not total_events and not ok:
                silent.append({"provider": provider, "samples": samples, "reason": "index page lists no events"})
    except Exception as exc:  # noqa: BLE001
        _log_failure("read silent providers", exc)
    return silent



# --- leaderboard (R2) --------------------------------------------------------

def _rate_color(rate: float) -> str:
    return "var(--success)" if rate > 90 else ("var(--warning)" if rate > 70 else "var(--danger)")


def build_leaderboard(attempts: Dict[str, Dict[str, int]], failovers: Dict[str, int]) -> List[dict]:
    """Rank providers by search success over the window.

    `attempts` is {provider: {outcome: count}}, `failovers` is {provider: n}
    charged to the provider that was serving when the failover happened. A
    provider with no searches in the window has no rate (it is listed last).
    """
    rows: List[dict] = []
    for provider in set(attempts) | set(failovers):
        outcomes = attempts.get(provider, {})
        total = sum(outcomes.values())
        bad = outcomes.get("timeout", 0) + outcomes.get("error", 0)
        rate = round((total - bad) * 100 / total, 1) if total else None
        rows.append({
            "provider": provider,
            "rate": rate,
            "rate_color": _rate_color(rate) if rate is not None else "var(--text-muted)",
            "total": total,
            "failovers": failovers.get(provider, 0),
        })
    def sort_key(row: dict) -> tuple:
        rate = row["rate"]
        return (rate is None, -float(rate or 0), -int(row["failovers"]), str(row["provider"]))

    rows.sort(key=sort_key)
    return rows


def leaderboard_sync() -> List[dict]:
    attempts: Dict[str, Dict[str, int]] = {}
    failovers: Dict[str, int] = {}
    try:
        with _db_session() as conn:
            for provider, outcome, count in conn.execute(
                "SELECT provider, COALESCE(outcome, CASE success WHEN 1 THEN 'ok' ELSE 'error' END), COUNT(*) "
                "FROM provider_performance WHERE timestamp > datetime('now', '-1 day') GROUP BY 1, 2"
            ).fetchall():
                attempts.setdefault(provider, {})[outcome] = count
            failovers = dict(conn.execute(
                "SELECT provider, COUNT(*) FROM stream_events "
                "WHERE event_type='failover' AND timestamp > datetime('now', '-1 day') GROUP BY provider"
            ).fetchall())
    except Exception as exc:  # noqa: BLE001
        _log_failure("read provider leaderboard", exc)
    return build_leaderboard(attempts, failovers)
