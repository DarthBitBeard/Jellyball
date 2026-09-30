"""Playback routes: /stream/{team_id} channel playlists and segments, and
Multi-View per-member audio playlists.
"""

from fastapi import APIRouter, HTTPException, Request

from state import stream_state
from legacy_proxy import _legacy_proxy_stream
from sessions import _serve_channel_playlist, _serve_session_segment
from multiview import _multiview_audio_view, _serve_multiview_audio_playlist, _touch_multiview_viewer

router = APIRouter()


# The playlist filename is "audio-N", not "N": when an M3U entry has no
# tvg-chno, Jellyfin falls back to a purely numeric URL filename as the channel
# number, which turned the audio channels into stray channels 1, 2, 3.
@router.api_route("/multiview/{channel_id}/audio-{audio_index}.m3u8", methods=["GET", "HEAD"])
async def serve_multiview_audio_playlist(channel_id: str, audio_index: int, request: Request):
    return await _serve_multiview_audio_playlist(channel_id, audio_index, request, f"audio-{audio_index}/seg/")


@router.api_route("/multiview/{channel_id}/audio/{audio_index}.m3u8", methods=["GET", "HEAD"])
async def serve_multiview_audio_playlist_legacy(channel_id: str, audio_index: int, request: Request):
    # Old URL form, kept until Jellyfin's next guide refresh picks up the new one.
    return await _serve_multiview_audio_playlist(channel_id, audio_index, request, f"{audio_index}/seg/")


@router.get("/multiview/{channel_id}/audio-{audio_index}/seg/{seq}.ts")
@router.get("/multiview/{channel_id}/audio/{audio_index}/seg/{seq}.ts")
async def serve_multiview_audio_segment(channel_id: str, audio_index: int, seq: int):
    if _multiview_audio_view(channel_id, audio_index) is None:
        raise HTTPException(status_code=404, detail="Multi-View channel not found")
    _touch_multiview_viewer(channel_id)
    return _serve_session_segment(f"{channel_id}#a{audio_index}", seq)


@router.api_route("/stream/{team_id}.m3u8", methods=["GET", "HEAD"])
async def stream_playlist(team_id: str, request: Request):
    return await _serve_channel_playlist(team_id, request)


@router.api_route("/stream/{team_id}/seg/{seq}.ts", methods=["GET", "HEAD"])
async def stream_segment(team_id: str, seq: int):
    if team_id in stream_state and stream_state[team_id].get("type") == "multiview":
        _touch_multiview_viewer(team_id)
    return _serve_session_segment(team_id, seq)


@router.api_route("/stream/{team_id}", methods=["GET", "HEAD"])
async def proxy_stream(team_id: str, request: Request, provider: str = ""):
    """Extensionless alias kept for existing Jellyfin tuner configs. `?provider=`
    pins one provider for debugging via the legacy passthrough proxy."""
    if provider and request.method == "GET" and team_id in stream_state:
        return await _legacy_proxy_stream(team_id, request, provider)
    return await _serve_channel_playlist(team_id, request)
