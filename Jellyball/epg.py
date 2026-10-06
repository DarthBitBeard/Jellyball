"""M3U playlist and XMLTV guide generation (/playlist.m3u, /epg.xml),
including the tvguide.com EPG fetch and Multi-View programme composition.
"""

import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse

from network_safety import safe_get

from config import _m3u_attribute, _m3u_title, _public_base_url, _base_url_is_loopback, _xml_attr, _xml_text, LOGGER
import state
from state import stream_state
from catalog import (
    _channel_group_title,
    _channel_is_always_live,
    _channel_is_off_season,
    _channel_listed,
    _channel_logo_url,
    _channel_tvg_id,
    _season_resume_label,
    parse_team_schedule,
    xmltv_ts,
)
from multiview import _multiview_member_label, MULTIVIEW_AUDIO_CHANNELS
from stream_extractor import DEFAULT_USER_AGENT

router = APIRouter()

# When /playlist.m3u was last served with a loopback-only base URL (Jellyfin's
# tuner then points at an address it can never reach). The dashboard surfaces
# this as a warning with the PUBLIC_BASE_URL fix.
_M3U_LOOPBACK_SERVED_AT: float = 0.0


def _m3u_loopback_warning_active() -> bool:
    """True when the M3U was served from a loopback address within the last hour."""
    return time.monotonic() - _M3U_LOOPBACK_SERVED_AT < 3600.0


@router.api_route("/playlist.m3u", methods=["GET", "HEAD"], response_class=PlainTextResponse)
async def generate_m3u(request: Request):
    global _M3U_LOOPBACK_SERVED_AT
    base_url = _public_base_url(request)
    if _base_url_is_loopback(base_url):
        _M3U_LOOPBACK_SERVED_AT = time.monotonic()
        LOGGER.warning(
            "Served /playlist.m3u with a loopback base URL (%s); "
            "Jellyfin on another host cannot tune these URLs. "
            "Set PUBLIC_BASE_URL or fetch the M3U via the LAN address.",
            base_url,
        )
    lines = ["#EXTM3U"]
    for team_id, data in stream_state.items():
        if not _channel_listed(data):
            continue
        # The .m3u8 extension makes Jellyfin treat the URL as an HLS manifest
        # (always through its own ffmpeg, never "direct play" of our URL or
        # raw-TS sharing). Channel names stay stable: health is shown on the
        # dashboard, not appended to the name (which renamed channels in
        # Jellyfin on every guide refresh).
        name = str(data.get("name") or team_id)
        tvg_id = _channel_tvg_id(team_id, data)
        logo = _channel_logo_url(data)
        logo_attr = f' tvg-logo="{_m3u_attribute(logo)}"' if logo else ""
        group = _m3u_attribute(_channel_group_title(data))
        lines.append(
            f'#EXTINF:-1 tvg-id="{_m3u_attribute(tvg_id)}" '
            f'tvg-name="{_m3u_attribute(name)}"{logo_attr} '
            f'group-title="{group}",{_m3u_title(name)}'
        )
        lines.append(f"{base_url}/stream/{urllib.parse.quote(team_id, safe='')}.m3u8")
        for audio_id, audio_name, path in _multiview_audio_channels(team_id, data):
            lines.append(
                f'#EXTINF:-1 tvg-id="{_m3u_attribute(audio_id)}" '
                f'tvg-name="{_m3u_attribute(audio_name)}"{logo_attr} '
                f'group-title="{group}",{_m3u_title(audio_name)}'
            )
            lines.append(f"{base_url}{path}")
    return "\n".join(lines)


def _multiview_audio_tvg_id(base_tvg_id: str, member_team_id: str) -> str:
    """tvg-id for one Multi-View per-audio channel.

    Keyed by the member's own channel id (stable even if the member list is
    reordered) rather than its positional index, and always ending in the
    fixed, non-numeric literal ".audio" - regardless of what the member id
    itself looks like. Jellyfin's M3U tuner derives a channel number from
    tvg-id (or channel-id) when tvg-chno is absent - see
    M3uParser.GetChannelNumber upstream - so ending in a constant word
    instead of a bare digit means it can never be mistaken for one.
    """
    return f"{base_tvg_id}.{member_team_id}.audio"


def _multiview_audio_channels(channel_id: str, data: dict) -> List[Tuple[str, str, str]]:
    """(tvg-id, display name, URL path) for each per-audio Multi-View channel."""
    if data.get("type") != "multiview" or not MULTIVIEW_AUDIO_CHANNELS:
        return []
    base_tvg_id = _channel_tvg_id(channel_id, data)
    mv_name = str(data.get("name") or channel_id)
    return [
        (
            _multiview_audio_tvg_id(base_tvg_id, member),
            f"🔊 {_multiview_member_label(member)} · {mv_name}",
            f"/multiview/{channel_id}/audio-{index}.m3u8",
        )
        for index, member in enumerate(data.get("member_team_ids") or [])
    ]


TVGUIDE_SPECIAL_CHANNEL_IDS = {
    "9200004533": "BigTenNetwork.us",
    "9200006937": "ESPN.us",
    "9200012351": "ESPN2.us",
    "9233011350": "ESPNU.us",
    "9233008440": "FoxSports1.us",
    "9200009884": "FoxSports2.us",
    "9233013235": "CBSSportsNetwork.us",
    "9233011830": "TNT.us",
    "9200017734": "ACCNetwork.us",
    "9233008517": "SECNetwork.us",
    "9200009223": "MLBNetwork.us",
    "9200000070": "NBATV.us",
    "9200004330": "NFLNetwork.us",
    "9233009455": "NHLNetwork.us",
    "9233011874": "ABC.us",
    "9233002271": "FOX.us",
    "9200018514": "CBS.us",
    "9233009876": "NBC.us",
    "9233011398": "CW.us",
    "9233000403": "TBS.us",
    "9233004106": "USANetwork.us",
    "9200009547": "TruTV.us",
}
GUIDE_HORIZON_DAYS = 10
_TVGUIDE_EPG_CACHE: Dict[str, List[dict]] = {}
_TVGUIDE_EPG_CACHED_AT: float = 0.0


async def _fetch_tvguide_epg() -> Dict[str, List[dict]]:
    """Fetch linear schedule blocks for always-live sports channels from TVGuide."""
    global _TVGUIDE_EPG_CACHE, _TVGUIDE_EPG_CACHED_AT
    now = time.monotonic()
    if _TVGUIDE_EPG_CACHE and (now - _TVGUIDE_EPG_CACHED_AT < 1800.0):
        return _TVGUIDE_EPG_CACHE

    url = f"https://backend.tvguide.com/tvschedules/tvguide/9100001138/web?start={int(time.time())}&duration={GUIDE_HORIZON_DAYS * 1440}"
    headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Referer": "https://www.tvguide.com/",
    }
    client = state.SHARED_HTTP_CLIENT or httpx.AsyncClient(timeout=10.0, follow_redirects=False)
    owns_client = state.SHARED_HTTP_CLIENT is None
    try:
        resp = await safe_get(client, url, headers=headers)
        if resp is not None and resp.status_code == 200:
            items = resp.json().get("data", {}).get("items", [])
            schedules: Dict[str, List[dict]] = {}
            for item in items:
                ch_id = str(item.get("channel", {}).get("sourceId", ""))
                tvg_id = TVGUIDE_SPECIAL_CHANNEL_IDS.get(ch_id)
                if tvg_id:
                    schedules[tvg_id] = item.get("programSchedules", [])
            if schedules:
                _TVGUIDE_EPG_CACHE = schedules
                _TVGUIDE_EPG_CACHED_AT = now
    except Exception as exc:
        LOGGER.debug("TVGuide EPG fetch failed: %s", type(exc).__name__)
    finally:
        if owns_client:
            await client.aclose()
    return _TVGUIDE_EPG_CACHE


def _channel_programmes(
    team_id: str,
    data: dict,
    guide_start: datetime,
    guide_end: datetime,
    tvguide_schedules: Dict[str, List[dict]],
) -> List[dict]:
    """Raw (unescaped) programme blocks for one ordinary (non-Multi-View) channel.

    Each entry is {start, stop, title, desc, category, icon}; `category` and
    `icon` may be "" meaning the XML renderer omits that tag. This mirrors the
    guide logic that used to live inline in generate_xmltv() - kept as its own
    helper so a Multi-View channel can pull a member's own guide data (see
    _multiview_member_programmes) to build its composite guide.
    """
    name = str(data.get("name") or team_id)
    logo = _channel_logo_url(data)
    channel_id = _channel_tvg_id(team_id, data)
    programmes: List[dict] = []

    def block(start: datetime, stop: datetime, title: str, desc: str, category: str = "") -> None:
        programmes.append({"start": start, "stop": stop, "title": title, "desc": desc,
                           "category": category, "icon": logo})

    if _channel_is_off_season(data):
        resumes = _season_resume_label(data.get("category", ""))
        block(guide_start, guide_end,
              f"{name}: Off-season" + (f" (resumes {resumes})" if resumes else ""),
              f"{name} is between seasons. The channel returns to the guide automatically"
              + (f" when the season resumes around {resumes}." if resumes else " when the season resumes."))
        return programmes

    dt_start, dt_stop = parse_team_schedule(data)
    has_specific_game = bool(
        dt_start and dt_stop and dt_stop >= guide_start and dt_start <= guide_end
    )

    if has_specific_game:
        if dt_start > guide_start:
            block(guide_start, min(dt_start, guide_end), f"{name} Standby",
                  f"Waiting for the scheduled {name} broadcast.")
        block(max(dt_start, guide_start), min(dt_stop, guide_end), f"{name} Scheduled Event",
              f"Scheduled live stream window for {name}. Searching starts one hour before the event "
              "and continues one hour after the scheduled end.")
        if dt_stop < guide_end:
            block(max(dt_stop, guide_start), guide_end, f"{name} Standby",
                  f"Post-event standby for {name}; the next scheduled event will refresh this guide.")
    else:
        programs = tvguide_schedules.get(channel_id, [])
        if _channel_is_always_live(data) and programs:
            for prog in programs:
                p_start = datetime.fromtimestamp(prog.get("startTime", 0), tz=timezone.utc)
                p_stop = datetime.fromtimestamp(prog.get("endTime", 0), tz=timezone.utc)
                if p_stop <= guide_start or p_start >= guide_end:
                    continue
                block(max(p_start, guide_start), min(p_stop, guide_end),
                      prog.get("title") or f"{name} Live",
                      prog.get("description") or f"Live broadcast on {name}.", "Sports")
        elif _channel_is_always_live(data):
            block(guide_start, guide_end, f"{name} Live",
                  f"Always-live sports channel for {name}; the linear stream is monitored continuously.", "Sports")
        else:
            block(guide_start, guide_end, f"{name} Standby",
                  f"No verified scheduled event is available for {name} in the next {GUIDE_HORIZON_DAYS} days.")
    return programmes


def now_next_for_channel(team_id: str, data: dict) -> Dict[str, str]:
    """Current/next programme titles for a dashboard channel card.

    Mirrors the title logic of _channel_programmes() but reads only the
    already-cached guide data: it never fetches on this path because the
    dashboard polls /api/status every 5 seconds. Returns
    {"now": <title>, "next": <title>}; "next" is "" when nothing follows.
    """
    name = str(data.get("name") or team_id)
    now = datetime.now(timezone.utc)

    if _channel_is_off_season(data):
        resumes = _season_resume_label(data.get("category", ""))
        return {
            "now": f"Off-season{f' (resumes {resumes})' if resumes else ''}",
            "next": "",
        }

    dt_start, dt_stop = parse_team_schedule(data)
    if dt_start and dt_stop:
        if dt_start <= now < dt_stop:
            return {"now": f"{name} Scheduled Event", "next": f"{name} Standby"}
        if now < dt_start:
            return {"now": f"{name} Standby", "next": f"{name} Scheduled Event"}
        return {"now": f"{name} Standby", "next": ""}

    if _channel_is_always_live(data):
        current: Optional[str] = None
        upcoming: Optional[str] = None
        for prog in _TVGUIDE_EPG_CACHE.get(_channel_tvg_id(team_id, data), []):
            p_start = datetime.fromtimestamp(prog.get("startTime", 0), tz=timezone.utc)
            p_stop = datetime.fromtimestamp(prog.get("endTime", 0), tz=timezone.utc)
            if p_start <= now < p_stop:
                current = prog.get("title") or f"{name} Live"
            elif p_start >= now and upcoming is None:
                upcoming = prog.get("title") or f"{name} Live"
            if current is not None and upcoming is not None:
                break
        return {"now": current or f"{name} Live", "next": upcoming or ""}

    return {"now": f"{name} Standby", "next": ""}


def _programme_xml_lines(channel_id: str, programmes: List[dict]) -> List[str]:
    """Render _channel_programmes()-shaped dicts as <programme> XML lines."""
    lines: List[str] = []
    channel_id_esc = _xml_attr(channel_id)
    for p in programmes:
        lines.append(
            f'  <programme channel="{channel_id_esc}" start="{xmltv_ts(p["start"])}" stop="{xmltv_ts(p["stop"])}">'
        )
        lines.append(f'    <title>{_xml_text(p["title"])}</title>')
        if p.get("category"):
            lines.append(f'    <category>{_xml_text(p["category"])}</category>')
        lines.append(f'    <desc>{_xml_text(p["desc"])}</desc>')
        if p.get("icon"):
            lines.append(f'    <icon src="{_xml_attr(p["icon"])}" />')
        lines.append('  </programme>')
    return lines


MULTIVIEW_PANE_LABELS = {
    "side_by_side_2": ["Left", "Right"],
    "grid_2x2": ["Top-left", "Top-right", "Bottom-left", "Bottom-right"],
}
MULTIVIEW_MAX_PROGRAMMES = 500
MULTIVIEW_MIN_INTERVAL_SECONDS = 300.0
_MULTIVIEW_FILLER_TITLE_MARKERS = ("Standby", "No verified scheduled event")


def _multiview_pane_labels(layout: str, count: int) -> List[str]:
    labels = MULTIVIEW_PANE_LABELS.get(layout)
    if labels and len(labels) == count:
        return labels
    return [f"Pane {i + 1}" for i in range(count)]


def _is_filler_programme_title(title: str) -> bool:
    return any(marker in title for marker in _MULTIVIEW_FILLER_TITLE_MARKERS)


def _multiview_member_programmes(
    member_team_ids: List[str],
    guide_start: datetime,
    guide_end: datetime,
    tvguide_schedules: Dict[str, List[dict]],
) -> Dict[str, List[dict]]:
    """Each member's own programme list, or a single "No Signal" block for a
    member that has since been removed (member_team_ids may reference a
    channel id no longer in stream_state)."""
    result: Dict[str, List[dict]] = {}
    for member_id in member_team_ids:
        member_data = stream_state.get(member_id)
        if member_data is None:
            result[member_id] = [{
                "start": guide_start, "stop": guide_end,
                "title": "No Signal", "desc": "", "category": "", "icon": "",
            }]
        else:
            result[member_id] = _channel_programmes(
                member_id, member_data, guide_start, guide_end, tvguide_schedules
            )
    return result


def _programme_at(programmes: List[dict], instant: datetime) -> Optional[dict]:
    for p in programmes:
        if p["start"] <= instant < p["stop"]:
            return p
    return programmes[-1] if programmes else None


def _multiview_boundaries(
    member_programmes: Dict[str, List[dict]], guide_start: datetime, guide_end: datetime
) -> List[datetime]:
    """Every member programme start/stop within the guide window, plus the
    window's own edges - the points at which the composite guide can change."""
    points = {guide_start, guide_end}
    for programmes in member_programmes.values():
        for p in programmes:
            if guide_start <= p["start"] <= guide_end:
                points.add(p["start"])
            if guide_start <= p["stop"] <= guide_end:
                points.add(p["stop"])
    return sorted(points)


def _merge_short_intervals(
    boundaries: List[datetime], min_seconds: float = MULTIVIEW_MIN_INTERVAL_SECONDS
) -> List[Tuple[datetime, datetime]]:
    """Turn sorted boundary points into (start, stop) intervals, folding any
    interval shorter than `min_seconds` into its neighbor so the guide doesn't
    fill up with sliver-length programmes."""
    if len(boundaries) < 2:
        return []
    intervals: List[Tuple[datetime, datetime]] = []
    start = boundaries[0]
    for i in range(1, len(boundaries)):
        stop = boundaries[i]
        is_last = i == len(boundaries) - 1
        if (stop - start).total_seconds() < min_seconds and not is_last:
            continue  # keep accumulating rather than committing a short interval
        intervals.append((start, stop))
        start = stop
    if len(intervals) >= 2 and (intervals[-1][1] - intervals[-1][0]).total_seconds() < min_seconds:
        prev_start, _ = intervals[-2]
        _, last_stop = intervals[-1]
        intervals[-2] = (prev_start, last_stop)
        intervals.pop()
    return intervals


def _cap_intervals(
    intervals: List[Tuple[datetime, datetime]], cap: int = MULTIVIEW_MAX_PROGRAMMES
) -> List[Tuple[datetime, datetime]]:
    """Repeatedly merge the shortest interval into its shorter neighbor until
    at most `cap` remain, so a channel can never emit an unbounded number of
    programmes."""
    intervals = list(intervals)
    while len(intervals) > cap:
        durations = [(b - a).total_seconds() for a, b in intervals]
        idx = min(range(len(durations)), key=lambda i: durations[i])
        if idx == 0:
            merge_idx = 0
        elif idx == len(intervals) - 1:
            merge_idx = idx - 1
        else:
            merge_idx = idx - 1 if durations[idx - 1] <= durations[idx + 1] else idx
        a_start, _ = intervals[merge_idx]
        _, b_stop = intervals[merge_idx + 1]
        intervals[merge_idx] = (a_start, b_stop)
        intervals.pop(merge_idx + 1)
    return intervals


def _cap_programme_dicts(entries: List[dict], cap: int = MULTIVIEW_MAX_PROGRAMMES) -> List[dict]:
    if len(entries) <= cap:
        return entries
    capped = list(entries[:cap])
    capped[-1] = dict(capped[-1])
    capped[-1]["stop"] = entries[-1]["stop"]
    return capped


def _multiview_pane_title(member_id: str, programme: Optional[dict], present: bool) -> str:
    """The text to show for one pane: the real programme title, or the
    member's own display name when there's nothing but filler ("... Standby"
    / "No verified scheduled event"), or "No Signal" for a removed member."""
    if not present:
        return "No Signal"
    title = str((programme or {}).get("title") or "")
    if not title or _is_filler_programme_title(title):
        return _multiview_member_label(member_id)
    return title


def _multiview_pane_info(
    member_team_ids: List[str],
    layout: str,
    member_programmes: Dict[str, List[dict]],
    instant: datetime,
) -> List[Tuple[str, str, str]]:
    """(position label, current pane title, member display name) for every
    pane, sampled at `instant`. `member display name` is "" for a removed
    member (nothing to show in parentheses)."""
    labels = _multiview_pane_labels(layout, len(member_team_ids))
    info: List[Tuple[str, str, str]] = []
    for label, member_id in zip(labels, member_team_ids):
        present = member_id in stream_state
        programme = _programme_at(member_programmes.get(member_id) or [], instant)
        pane_title = _multiview_pane_title(member_id, programme, present)
        member_name = _multiview_member_label(member_id) if present else ""
        info.append((label, pane_title, member_name))
    return info


def _multiview_pane_desc_lines(pane_info: List[Tuple[str, str, str]]) -> List[str]:
    lines = []
    for label, pane_title, member_name in pane_info:
        if member_name and pane_title != member_name:
            lines.append(f"{label}: {pane_title} ({member_name})")
        else:
            lines.append(f"{label}: {pane_title}")
    return lines


def _multiview_programmes(
    channel_id: str,
    data: dict,
    guide_start: datetime,
    guide_end: datetime,
    tvguide_schedules: Dict[str, List[dict]],
) -> Tuple[List[dict], Dict[str, List[dict]]]:
    """Programme dicts for a Multi-View main channel, plus one programme list
    per per-audio channel (keyed by the audio channel's own tvg-id, see
    _multiview_audio_tvg_id / _multiview_audio_channels).

    The main channel's guide window is split at every member's own programme
    boundary (so the composite title/desc only changes when a pane's content
    actually changes), short slivers are merged into a neighbor, and the
    result is capped so a Multi-View channel can never flood the guide.
    """
    member_team_ids = list(data.get("member_team_ids") or [])
    layout = data.get("layout") or ""
    logo = _channel_logo_url(data)
    mv_name = str(data.get("name") or channel_id)
    base_tvg_id = _channel_tvg_id(channel_id, data)

    if not member_team_ids:
        # Malformed/legacy entry with no members recorded - fall back to a
        # single placeholder block rather than emitting an empty title.
        return (
            [{
                "start": guide_start, "stop": guide_end,
                "title": f"{mv_name} Live", "desc": mv_name,
                "category": "Sports", "icon": logo,
            }],
            {},
        )

    member_programmes = _multiview_member_programmes(
        member_team_ids, guide_start, guide_end, tvguide_schedules
    )
    boundaries = _multiview_boundaries(member_programmes, guide_start, guide_end)
    intervals = _cap_intervals(_merge_short_intervals(boundaries))

    active_audio_id = data.get("active_audio_team_id") or member_team_ids[0]
    active_audio_name = _multiview_member_label(active_audio_id)

    main_programmes: List[dict] = []
    for start, stop in intervals:
        pane_info = _multiview_pane_info(member_team_ids, layout, member_programmes, start)
        title = " | ".join(pane_title for _, pane_title, _ in pane_info)
        desc_lines = _multiview_pane_desc_lines(pane_info)
        desc_lines.append(f"Audio: {active_audio_name} — change channel to the 🔊 channels to switch audio")
        main_programmes.append({
            "start": start, "stop": stop,
            "title": title, "desc": "\n".join(desc_lines),
            "category": "Sports", "icon": logo,
        })

    audio_programmes: Dict[str, List[dict]] = {}
    for member_id in member_team_ids:
        audio_id = _multiview_audio_tvg_id(base_tvg_id, member_id)
        present = member_id in stream_state
        member_name = _multiview_member_label(member_id)
        entries: List[dict] = []
        for p in member_programmes.get(member_id) or []:
            pane_info = _multiview_pane_info(member_team_ids, layout, member_programmes, p["start"])
            pane_title = _multiview_pane_title(member_id, p, present)
            desc_lines = [f"Audio from {member_name} in {mv_name}."]
            desc_lines.extend(_multiview_pane_desc_lines(pane_info))
            entries.append({
                "start": p["start"], "stop": p["stop"],
                "title": f"🔊 {pane_title}", "desc": "\n".join(desc_lines),
                "category": "Sports", "icon": logo,
            })
        audio_programmes[audio_id] = _cap_programme_dicts(entries)

    return main_programmes, audio_programmes


@router.api_route("/epg.xml", methods=["GET", "HEAD"], response_class=PlainTextResponse)
async def generate_xmltv(request: Request = None):
    now_utc = datetime.now(timezone.utc)
    xml = ['<?xml version="1.0" encoding="UTF-8"?>', '<tv>']

    for team_id, data in stream_state.items():
        if not _channel_listed(data):
            continue
        channel_id = _channel_tvg_id(team_id, data)
        name_esc = _xml_text(data["name"])
        logo = _channel_logo_url(data)
        icon_tag = f'\n    <icon src="{_xml_attr(logo)}" />' if logo else ""
        xml.append(f'  <channel id="{_xml_attr(channel_id)}">')
        xml.append(f'    <display-name>{name_esc}</display-name>{icon_tag}')
        xml.append('  </channel>')
        for audio_id, audio_name, _ in _multiview_audio_channels(team_id, data):
            xml.append(f'  <channel id="{_xml_attr(audio_id)}">')
            xml.append(f'    <display-name>{_xml_text(audio_name)}</display-name>{icon_tag}')
            xml.append('  </channel>')

    guide_start = now_utc - timedelta(hours=1)
    guide_end = now_utc + timedelta(days=GUIDE_HORIZON_DAYS)
    tvguide_schedules = await _fetch_tvguide_epg()

    for team_id, data in stream_state.items():
        if not _channel_listed(data):
            continue
        channel_id = _channel_tvg_id(team_id, data)

        if data.get("type") == "multiview":
            main_programmes, audio_programmes = _multiview_programmes(
                team_id, data, guide_start, guide_end, tvguide_schedules
            )
            xml.extend(_programme_xml_lines(channel_id, main_programmes))
            for audio_id, programmes in audio_programmes.items():
                xml.extend(_programme_xml_lines(audio_id, programmes))
        else:
            programmes = _channel_programmes(team_id, data, guide_start, guide_end, tvguide_schedules)
            xml.extend(_programme_xml_lines(channel_id, programmes))

    xml.append('</tv>')
    return "\n".join(xml)
