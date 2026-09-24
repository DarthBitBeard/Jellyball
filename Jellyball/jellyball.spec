# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller ONEDIR spec for Jellyball.

Produces dist/Jellyball/ containing two executables that share one
_internal payload:
  - Jellyball.exe         windowed (system tray), console=False
  - JellyballConsole.exe  headless/console, console=True (troubleshooting,
                           and used with --console / --service by the
                           installer's Windows service registration)

Both executables run jellyball_launcher.py, which dispatches on argv
(no args = tray, --console = foreground headless, --service = Windows
service via pywin32's servicemanager).
"""
import json
import os
import re
from pathlib import Path

from PyInstaller.utils.hooks import collect_all
from PyInstaller.utils.win32.versioninfo import (
    FixedFileInfo,
    StringFileInfo,
    StringStruct,
    StringTable,
    VarFileInfo,
    VarStruct,
    VSVersionInfo,
)

ROOT = Path(SPECPATH)
ASSETS_DIR = ROOT / "assets"
BUILD_DIR = ROOT / "build"
icon_file = str(ASSETS_DIR / "jellyball.ico")

# ---------------------------------------------------------------------------
# Version (single source: version.py). Parsed with a regex rather than
# imported so this spec doesn't depend on Jellyball's package/module layout
# or leave a stale "version" entry in sys.modules across repeated builds.
# ---------------------------------------------------------------------------
_version_text = (ROOT / "version.py").read_text(encoding="utf-8")
_version_match = re.search(r'__version__\s*=\s*[\'"]([^\'"]+)[\'"]', _version_text)
if not _version_match:
    raise SystemExit("jellyball.spec: could not parse __version__ from version.py")
APP_VERSION = _version_match.group(1)

_version_parts = [int(p) for p in re.findall(r"\d+", APP_VERSION)][:4]
while len(_version_parts) < 4:
    _version_parts.append(0)
VERSION_TUPLE = tuple(_version_parts)

print(f"Jellyball build: version {APP_VERSION} {VERSION_TUPLE}")


def _make_version_info(file_description: str, original_filename: str) -> str:
    """Build a PyInstaller VSVersionInfo, write its text serialization to
    build/, and return the path (EXE(version=...) takes a file path, not an
    object)."""
    info = VSVersionInfo(
        ffi=FixedFileInfo(
            filevers=VERSION_TUPLE,
            prodvers=VERSION_TUPLE,
            mask=0x3F,
            flags=0x0,
            OS=0x40004,  # VOS_NT_WINDOWS32
            fileType=0x1,  # VFT_APP
            subtype=0x0,
            date=(0, 0),
        ),
        kids=[
            StringFileInfo(
                [
                    StringTable(
                        "040904B0",
                        [
                            StringStruct("CompanyName", "Jellyball"),
                            StringStruct("FileDescription", file_description),
                            StringStruct("FileVersion", APP_VERSION),
                            StringStruct("InternalName", original_filename),
                            StringStruct(
                                "LegalCopyright",
                                "Jellyball",
                            ),
                            StringStruct("OriginalFilename", original_filename),
                            StringStruct("ProductName", "Jellyball"),
                            StringStruct("ProductVersion", APP_VERSION),
                        ],
                    )
                ]
            ),
            VarFileInfo([VarStruct("Translation", [1033, 1200])]),
        ],
    )
    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    out_path = BUILD_DIR / f"versioninfo_{Path(original_filename).stem}.txt"
    out_path.write_text(str(info), encoding="utf-8")
    return str(out_path)


VERSION_INFO_WINDOWED = _make_version_info("Jellyball Sports Proxy", "Jellyball.exe")
VERSION_INFO_CONSOLE = _make_version_info(
    "Jellyball Sports Proxy (Console)", "JellyballConsole.exe"
)

# ---------------------------------------------------------------------------
# Data files, hidden imports, collect_all
# ---------------------------------------------------------------------------
datas = [
    (str(path), "assets")
    for path in ASSETS_DIR.iterdir()
    if path.is_file()
]
binaries = []
hiddenimports = [
    "sports_catalog",
    "sports_matcher",
    "stream_extractor",
    "network_safety",
    "hls_session",
    "ts_normalize",
    "version",
    "main",
    "uvicorn.logging",
    "uvicorn.loops",
    "uvicorn.loops.auto",
    "uvicorn.loops.asyncio",
    "uvicorn.protocols",
    "uvicorn.protocols.http",
    "uvicorn.protocols.http.auto",
    "uvicorn.protocols.http.httptools_impl",
    "uvicorn.protocols.http.h11_impl",
    "uvicorn.lifespan",
    "uvicorn.lifespan.on",
    "pystray",
    "PIL",
    "bcrypt",
    "httptools",
    "h2",
    "lxml",
    "lxml.etree",
    "cryptography",
    # pywin32 service support (jellyball_launcher.py --service)
    "win32timezone",
    "win32serviceutil",
    "win32service",
    "win32event",
    "servicemanager",
    "pywintypes",
]

for package_name in [
    "tzdata",
    "playwright",
    "rapidfuzz",
    "thefuzz",
    "bs4",
    "httpx",
    "pystray",
    "PIL",
    "bcrypt",
    "h2",
    "lxml",
    "cryptography",
]:
    try:
        package_datas, package_binaries, package_hiddenimports = collect_all(package_name)
    except ImportError as exc:
        print(f"Optional package not collected: {package_name} ({exc})")
        continue
    datas += package_datas
    binaries += package_binaries
    hiddenimports += package_hiddenimports

# ---------------------------------------------------------------------------
# Playwright browsers: bundle only the Chromium build(s) this installed
# Playwright version actually expects, matched by revision from the
# playwright package's own driver/package/browsers.json. This avoids
# shipping stale/unrelated browser folders that may be sitting in the
# local ms-playwright cache (old revisions, Firefox/WebKit, etc.).
# ---------------------------------------------------------------------------
try:
    import playwright as _playwright_pkg
except ImportError as exc:
    raise SystemExit(f"jellyball.spec: playwright is not installed: {exc}")

_browsers_json_path = (
    Path(_playwright_pkg.__file__).resolve().parent / "driver" / "package" / "browsers.json"
)
if not _browsers_json_path.is_file():
    raise SystemExit(
        f"jellyball.spec: could not find playwright's browsers.json at {_browsers_json_path}"
    )
_browsers_data = json.loads(_browsers_json_path.read_text(encoding="utf-8"))
_pw_revisions = {
    entry["name"]: entry["revision"]
    for entry in _browsers_data.get("browsers", [])
    if entry.get("name") and entry.get("revision")
}

# Playwright's on-disk directory names replace hyphens with underscores in
# the browser name (browsers.json "chromium-headless-shell" -> directory
# "chromium_headless_shell-<revision>"), except for the trailing
# "-<revision>" separator itself.
# Only the headless shell: every launch in the app is headless=True, which
# Playwright serves from chromium_headless_shell (verified); bundling full
# Chromium as well added ~430 MB for nothing.
_WANTED_BROWSER_DIRS = {
    "chromium_headless_shell": _pw_revisions.get("chromium-headless-shell"),
    "ffmpeg": _pw_revisions.get("ffmpeg"),
}
_missing_revisions = [name for name, rev in _WANTED_BROWSER_DIRS.items() if not rev]
if _missing_revisions:
    raise SystemExit(
        "jellyball.spec: playwright's browsers.json has no revision for: "
        + ", ".join(_missing_revisions)
    )

browser_root_value = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "")
if not browser_root_value or browser_root_value == "0":
    browser_root_value = os.path.join(os.getenv("LOCALAPPDATA", str(Path.home())), "ms-playwright")
browser_root = Path(browser_root_value).expanduser()
if not browser_root.is_dir():
    raise SystemExit(
        "Playwright browser assets were not found at "
        f"{browser_root}. Run 'python -m playwright install chromium' before building."
    )

_bundled_browser_prefixes = set()
for browser_dir in sorted(browser_root.iterdir()):
    if not browser_dir.is_dir() or browser_dir.name == ".links":
        continue
    prefix, sep, revision = browser_dir.name.rpartition("-")
    if not sep:
        continue
    expected_revision = _WANTED_BROWSER_DIRS.get(prefix)
    if expected_revision and revision == expected_revision:
        datas.append((str(browser_dir), str(Path("playwright_browsers") / browser_dir.name)))
        _bundled_browser_prefixes.add(prefix)
        print(f"Bundling Playwright browser {browser_dir.name}")

_required_browser_prefixes = {"chromium_headless_shell"}
_missing_browser_prefixes = _required_browser_prefixes - _bundled_browser_prefixes
if _missing_browser_prefixes:
    _expected = {name: _WANTED_BROWSER_DIRS[name] for name in sorted(_missing_browser_prefixes)}
    raise SystemExit(
        "Required Playwright Chromium build(s) not found (matching this Playwright "
        f"version's expected revisions {_expected}) under {browser_root}. "
        "Run 'python -m playwright install chromium' before building."
    )
if "ffmpeg" not in _bundled_browser_prefixes:
    print(
        f"Playwright-managed ffmpeg build not found/matched under {browser_root}; "
        "continuing without it (this is Playwright's own ffmpeg, unrelated to the "
        "Multi-View ffmpeg.exe bundled into ffmpeg_bin/ below)."
    )

# ---------------------------------------------------------------------------
# Multi-View ffmpeg/ffprobe: bundle a portable ffmpeg.exe (and ffprobe.exe,
# if found beside it) so Multi-View channels work without a separate
# install. This is optional: the app falls back to a system-PATH "ffmpeg"
# (or a user-configured FFMPEG_PATH) at runtime and disables Multi-View
# with a dashboard warning if none is found, so a missing binary here does
# not fail the build.
# ---------------------------------------------------------------------------
ffmpeg_bundle_value = os.getenv("FFMPEG_BUNDLE_PATH", "")
if not ffmpeg_bundle_value:
    ffmpeg_bundle_value = os.path.join(os.getenv("LOCALAPPDATA", str(Path.home())), "ffmpeg", "bin", "ffmpeg.exe")
ffmpeg_bundle_path = Path(ffmpeg_bundle_value).expanduser()
if ffmpeg_bundle_path.is_file():
    datas.append((str(ffmpeg_bundle_path), "ffmpeg_bin"))
    print(f"Bundling ffmpeg from {ffmpeg_bundle_path}")
    ffprobe_name = "ffprobe.exe" if ffmpeg_bundle_path.suffix.lower() == ".exe" else "ffprobe"
    ffprobe_bundle_path = ffmpeg_bundle_path.with_name(ffprobe_name)
    if ffprobe_bundle_path.is_file():
        datas.append((str(ffprobe_bundle_path), "ffmpeg_bin"))
        print(f"Bundling ffprobe from {ffprobe_bundle_path}")
    else:
        print(
            f"ffprobe not found beside {ffmpeg_bundle_path}; continuing without it. "
            "Some Multi-View diagnostics may be unavailable."
        )
else:
    print(
        f"ffmpeg binary not found at {ffmpeg_bundle_path}; building without it. "
        "Multi-View will require a separate ffmpeg install (see README), or set "
        "FFMPEG_BUNDLE_PATH to an ffmpeg.exe before building to include one."
    )

# ---------------------------------------------------------------------------
# Analysis (shared by both executables) + two EXEs merged into one COLLECT
# so they share a single _internal payload.
# ---------------------------------------------------------------------------
a = Analysis(
    ["jellyball_launcher.py"],
    pathex=[SPECPATH],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe_windowed = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="Jellyball",
    icon=icon_file,
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    version=VERSION_INFO_WINDOWED,
)

exe_console = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="JellyballConsole",
    icon=icon_file,
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    version=VERSION_INFO_CONSOLE,
)

coll = COLLECT(
    exe_windowed,
    exe_console,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name="Jellyball",
)
