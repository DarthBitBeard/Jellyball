"""How the dashboard organises enabled channels: which collapsible section a
channel belongs to, and which status-filter state it is in.

Shared by the server-rendered cards and /api/status (which the dashboard
polls), so the filter chips and the cards never disagree.
"""

from datetime import datetime
from typing import Dict, List, Optional, Tuple

from catalog import is_stream_window_active
from leagues import LEAGUES, LEAGUES_BY_KEY

SPECIAL_GROUP = ("special", "Always-Live Sports Channels")
CUSTOM_GROUP = ("custom", "Custom Channels")

# Status-filter states, in chip order.
STATUS_LIVE = "live"
STATUS_NO_STREAM = "no_stream"
STATUS_UPCOMING = "upcoming"
STATUS_OFF_SEASON = "off_season"
STATUSES = (STATUS_LIVE, STATUS_NO_STREAM, STATUS_UPCOMING, STATUS_OFF_SEASON)


def group_defs() -> List[Tuple[str, str]]:
    """(key, label) of every section, in display order (the catalog's)."""
    return [*((league.key, league.group_title) for league in LEAGUES), SPECIAL_GROUP, CUSTOM_GROUP]


def channel_group(data: dict) -> Tuple[str, str]:
    """The section a channel is listed under."""
    if data.get("always_live") or data.get("content_type") == "channel":
        return SPECIAL_GROUP
    league = LEAGUES_BY_KEY.get(str(data.get("category") or "").lower())
    if league is not None:
        return league.key, league.group_title
    return CUSTOM_GROUP


def channel_status(data: dict, now: Optional[datetime] = None) -> str:
    """live: has a working stream. no_stream: should have one now (always
    live, or inside its event window) but doesn't. off_season: league is out
    of season. upcoming: nothing to show yet (no event, or it hasn't started)."""
    if data.get("is_healthy"):
        return STATUS_LIVE
    if data.get("schedule_status") == "off_season":
        return STATUS_OFF_SEASON
    if is_stream_window_active(data, now):
        return STATUS_NO_STREAM
    return STATUS_UPCOMING


def group_channels(channels: List[dict]) -> List[dict]:
    """Cards (each with "group_key") bucketed into display-ordered sections;
    empty sections are left out."""
    by_key: Dict[str, List[dict]] = {}
    for channel in channels:
        by_key.setdefault(channel["group_key"], []).append(channel)
    return [
        {"key": key, "label": label, "channels": by_key[key]}
        for key, label in group_defs()
        if by_key.get(key)
    ]
