"""Stream language: which language a candidate stream is in, and which
languages the viewer wants.

A channel such as ESPN exists in many language editions (ESPN Latino, ESPN
Brasil, ESPN Deportes, ...). The viewer picks the languages they understand
(in order) and a mode:

- "prefer" (default): streams in those languages rank first; other languages
  are kept as a last resort so the channel still plays when nothing else works.
- "only": streams known to be in another language are never used.

A stream's language comes from the source when it says so (iptv-org's
language-grouped playlist), otherwise from markers in its title ("Latino",
"Deportes", "Brasil", "(Netherlands)" ...). A stream whose language cannot be
told is treated as unknown: it ranks after the preferred languages but is
never excluded, because most event streams carry no language marker at all.

Settings live in app_settings (dashboard Settings tab); STREAM_LANGUAGES and
STREAM_LANGUAGE_MODE in .env set the defaults.
"""

import os
import re
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence

LANGUAGES: Dict[str, str] = {
    "eng": "English", "spa": "Spanish", "por": "Portuguese", "fra": "French",
    "deu": "German", "ita": "Italian", "nld": "Dutch", "ara": "Arabic",
    "rus": "Russian", "pol": "Polish", "tur": "Turkish", "ell": "Greek",
    "ron": "Romanian", "hun": "Hungarian", "ces": "Czech", "hin": "Hindi",
    "zho": "Chinese", "jpn": "Japanese", "kor": "Korean", "swe": "Swedish",
}
_NAME_TO_CODE = {name.casefold(): code for code, name in LANGUAGES.items()}
_NAME_TO_CODE.update({"castilian": "spa", "flemish": "nld", "mandarin": "zho", "cantonese": "zho"})

LANGUAGES_KEY = "stream_languages"
MODE_KEY = "stream_language_mode"
MODES = ("prefer", "only")

# Title markers, checked as whole words (casefolded). Countries map to the
# language their sports channels broadcast in; "usa"/"us"/"uk" mark the
# English editions that aggregators label by region.
_MARKERS: Dict[str, str] = {
    "latino": "spa", "latina": "spa", "latin": "spa", "deportes": "spa", "espanol": "spa", "español": "spa",
    "spanish": "spa", "mexico": "spa", "méxico": "spa", "argentina": "spa", "colombia": "spa", "chile": "spa",
    "peru": "spa", "perú": "spa", "ecuador": "spa", "uruguay": "spa", "paraguay": "spa", "bolivia": "spa",
    "venezuela": "spa", "spain": "spa", "españa": "spa", "espana": "spa", "caribe": "spa",
    "brasil": "por", "brazil": "por", "portugal": "por", "portuguese": "por",
    "france": "fra", "french": "fra", "francais": "fra", "français": "fra",
    "germany": "deu", "deutschland": "deu", "german": "deu", "deutsch": "deu",
    "italia": "ita", "italy": "ita", "italian": "ita",
    "netherlands": "nld", "nederland": "nld", "dutch": "nld",
    "arabic": "ara", "arab": "ara", "arabia": "ara",
    "russia": "rus", "russian": "rus", "poland": "pol", "polska": "pol",
    "turkiye": "tur", "türkiye": "tur", "turkey": "tur", "turkish": "tur",
    "greece": "ell", "greek": "ell", "romania": "ron", "hungary": "hun", "czech": "ces",
    "hindi": "hin", "japan": "jpn", "korea": "kor", "china": "zho", "sweden": "swe",
    "usa": "eng", "us": "eng", "uk": "eng", "english": "eng", "united states": "eng",
}
_MULTIWORD = {marker: code for marker, code in _MARKERS.items() if " " in marker}
_WORD_RE = re.compile(r"[^\W\d_]+", re.UNICODE)


def language_name(code: str) -> str:
    return LANGUAGES.get(code, code)


def code_for(value: object) -> Optional[str]:
    """A language code from a code ("eng"), a name ("English") or None."""
    text = str(value or "").strip().casefold()
    if not text:
        return None
    if text in LANGUAGES:
        return text
    return _NAME_TO_CODE.get(text)


def detect_language(title: str) -> Optional[str]:
    """The language a stream title marks, or None when it does not say."""
    value = str(title or "").casefold()
    for marker, code in _MULTIWORD.items():
        if marker in value:
            return code
    words = _WORD_RE.findall(value)
    for word in words:
        code = _MARKERS.get(word)
        if code is not None:
            return code
    return None


def stream_language(stream: Dict[str, Any]) -> Optional[str]:
    """A candidate's language: the source's own tag first, then its title."""
    tagged = stream.get("language")
    if isinstance(tagged, (list, tuple, set)):
        languages = [code for code in (code_for(item) for item in tagged) if code]
        prefs = preferred_languages()
        for code in prefs:
            if code in languages:
                return code
        return languages[0] if languages else None
    code = code_for(tagged)
    if code:
        return code
    return detect_language(str(stream.get("match_title") or ""))


# -- preference ---------------------------------------------------------------

def parse_languages(raw: object) -> List[str]:
    """'eng, Spanish' -> ['eng', 'spa'] (unknown entries dropped, order kept)."""
    codes: List[str] = []
    for item in re.split(r"[,;\s]+", str(raw or "")):
        code = code_for(item)
        if code and code not in codes:
            codes.append(code)
    return codes


_DEFAULT_LANGUAGES = parse_languages(os.getenv("STREAM_LANGUAGES", "eng")) or ["eng"]
_DEFAULT_MODE = (os.getenv("STREAM_LANGUAGE_MODE", "prefer").strip().lower() or "prefer")
if _DEFAULT_MODE not in MODES:
    _DEFAULT_MODE = "prefer"

_CACHE_SECONDS = 30.0
_cache: Dict[str, Any] = {"languages": None, "mode": None, "at": 0.0}


def _load() -> None:
    now = time.monotonic()
    if _cache["languages"] is not None and now - float(_cache["at"]) < _CACHE_SECONDS:
        return
    try:
        from db import get_setting  # late: keeps this module importable without a database

        languages = parse_languages(get_setting(LANGUAGES_KEY, "")) or list(_DEFAULT_LANGUAGES)
        mode = get_setting(MODE_KEY, "") or _DEFAULT_MODE
    except Exception:
        languages, mode = list(_DEFAULT_LANGUAGES), _DEFAULT_MODE
    _cache.update(languages=languages, mode=mode if mode in MODES else _DEFAULT_MODE, at=now)


def preferred_languages() -> List[str]:
    _load()
    return list(_cache["languages"])


def language_mode() -> str:
    _load()
    return str(_cache["mode"])


def save_preference(languages: Iterable[object], mode: object) -> Dict[str, Any]:
    from db import set_setting

    codes = parse_languages(",".join(str(item) for item in languages)) or list(_DEFAULT_LANGUAGES)
    chosen = str(mode or "").strip().lower()
    chosen = chosen if chosen in MODES else _DEFAULT_MODE
    set_setting(LANGUAGES_KEY, ",".join(codes))
    set_setting(MODE_KEY, chosen)
    _cache.update(languages=codes, mode=chosen, at=time.monotonic())
    return {"languages": codes, "mode": chosen}


def invalidate_cache() -> None:
    _cache.update(languages=None, mode=None, at=0.0)


# -- ranking and filtering -------------------------------------------------------

def language_rank(stream: Dict[str, Any], prefs: Optional[Sequence[str]] = None) -> int:
    """0.. for the preferred languages in order, then unknown, then any other language."""
    prefs = list(prefs) if prefs is not None else preferred_languages()
    code = stream_language(stream)
    if code is None:
        return len(prefs)
    if code in prefs:
        return prefs.index(code)
    return len(prefs) + 1


def is_excluded(stream: Dict[str, Any], prefs: Optional[Sequence[str]] = None, mode: Optional[str] = None) -> bool:
    """True when "only" mode is on and the stream is known to be in another language."""
    if (mode or language_mode()) != "only":
        return False
    prefs = list(prefs) if prefs is not None else preferred_languages()
    code = stream_language(stream)
    return code is not None and code not in prefs


def apply_to_channel(data: Any) -> int:
    """Bring a channel's existing candidate list in line with the current
    preference (right after it changes): drop streams that "only" mode now
    excludes and move the preferred languages to the front. Returns how many
    were dropped. The candidate that is playing is kept if it survives, so a
    healthy stream is not interrupted just for being re-sorted."""
    candidates = list(data.get("candidates") or [])
    if not candidates:
        return 0
    prefs = preferred_languages()
    mode = language_mode()
    active_index = int(data.get("active_index", 0) or 0)
    active = candidates[active_index] if 0 <= active_index < len(candidates) else None
    kept = [c for c in candidates if not is_excluded(c, prefs, mode)]
    kept.sort(key=lambda c: language_rank(c, prefs))  # stable: keeps the existing order within a language
    data["candidates"] = kept
    data["active_index"] = kept.index(active) if active is not None and active in kept else 0
    return len(candidates) - len(kept)


def listing_allowed(text: str, non_english_fallback=None) -> bool:
    """Whether a provider's channel listing (e.g. "ESPN Deportes", "beIN Sports
    Arabic") is worth opening. A listing in a language the viewer did not pick
    is skipped; with English first, `non_english_fallback` (the providers' old
    region-tag check) keeps excluding regional feeds that carry no language."""
    prefs = preferred_languages()
    code = detect_language(text)
    if code is not None:
        return code in prefs
    if prefs and prefs[0] == "eng" and non_english_fallback is not None:
        return not non_english_fallback(text)
    return True
