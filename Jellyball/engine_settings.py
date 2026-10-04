"""Streaming-engine settings stored in app_settings.

preferred_audio_language: ISO 639-2 code of the audio stream to prefer when a
source carries several (the PMT's ISO_639_language_descriptor). Empty (the
default) keeps the first audio stream, exactly as before. Read at every source
switch of a channel session through a short cache, so a change applies without a
restart (and without touching the database on every segment).
"""

import time
from typing import Any, Dict, Optional

from db import get_setting, set_setting

AUDIO_LANGUAGE_KEY = "preferred_audio_language"
# Offered in the dashboard. ts_normalize matches bibliographic and terminology
# forms alike (fra/fre, deu/ger, ...).
AUDIO_LANGUAGES = {
    "eng": "English", "spa": "Spanish", "fra": "French", "deu": "German", "ita": "Italian",
    "por": "Portuguese", "nld": "Dutch", "rus": "Russian", "ara": "Arabic",
    "zho": "Chinese", "jpn": "Japanese", "kor": "Korean",
}
_CACHE_SECONDS = 30.0
_cache: Dict[str, Any] = {"value": None, "at": 0.0}


def normalize_audio_language(raw: object) -> str:
    """The code if it is a supported language, else '' (no preference)."""
    code = str(raw or "").strip().lower()
    return code if code in AUDIO_LANGUAGES else ""


def preferred_audio_language() -> Optional[str]:
    now = time.monotonic()
    if _cache["value"] is None or now - float(_cache["at"]) > _CACHE_SECONDS:
        _cache["value"] = normalize_audio_language(get_setting(AUDIO_LANGUAGE_KEY, ""))
        _cache["at"] = now
    return str(_cache["value"]) or None


def save_audio_language(raw: object) -> str:
    code = normalize_audio_language(raw)
    set_setting(AUDIO_LANGUAGE_KEY, code)
    _cache["value"] = code
    _cache["at"] = time.monotonic()
    return code
