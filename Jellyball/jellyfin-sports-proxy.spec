# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path
from PyInstaller.utils.hooks import collect_all
import os

ROOT = Path(SPECPATH)
ASSETS_DIR = ROOT / "assets"
icon_file = str(ASSETS_DIR / "jellyball.ico")
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
    "uvicorn.logging",
    "uvicorn.loops",
    "uvicorn.loops.auto",
    "uvicorn.protocols",
    "uvicorn.protocols.http",
    "uvicorn.protocols.http.auto",
    "uvicorn.lifespan",
    "uvicorn.lifespan.on",
    "pystray",
    "PIL",
    "passlib",
    "passlib.handlers.bcrypt",
    "bcrypt",
]

for package_name in ["tzdata", "playwright", "rapidfuzz", "thefuzz", "bs4", "httpx", "pystray", "PIL", "passlib", "bcrypt"]:
    try:
        package_datas, package_binaries, package_hiddenimports = collect_all(package_name)
    except ImportError as exc:
        print(f"Optional package not collected: {package_name} ({exc})")
        continue
    datas += package_datas
    binaries += package_binaries
    hiddenimports += package_hiddenimports

browser_root_value = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "")
if not browser_root_value or browser_root_value == "0":
    browser_root_value = os.path.join(os.getenv("LOCALAPPDATA", str(Path.home())), "ms-playwright")
browser_root = Path(browser_root_value).expanduser()
if not browser_root.is_dir():
    raise SystemExit(
        "Playwright browser assets were not found at "
        f"{browser_root}. Run 'python -m playwright install chromium' before building."
    )

for browser_dir in browser_root.iterdir():
    if browser_dir.is_dir() and browser_dir.name != ".links":
        datas.append((str(browser_dir), str(Path("playwright_browsers") / browser_dir.name)))

# Bundle a portable ffmpeg.exe so Multi-View channels work without a separate
# install, mirroring the Playwright browser bundling above. Unlike Chromium,
# this is optional: the app already falls back to a system-PATH "ffmpeg" (or
# a user-configured FFMPEG_PATH) at runtime and disables Multi-View with a
# dashboard warning if none is found, so a missing binary here does not fail
# the build - it just produces an exe that needs ffmpeg installed separately.
ffmpeg_bundle_value = os.getenv("FFMPEG_BUNDLE_PATH", "")
if not ffmpeg_bundle_value:
    ffmpeg_bundle_value = os.path.join(os.getenv("LOCALAPPDATA", str(Path.home())), "ffmpeg", "bin", "ffmpeg.exe")
ffmpeg_bundle_path = Path(ffmpeg_bundle_value).expanduser()
if ffmpeg_bundle_path.is_file():
    datas.append((str(ffmpeg_bundle_path), "ffmpeg_bin"))
    print(f"Bundling ffmpeg from {ffmpeg_bundle_path}")
else:
    print(
        f"ffmpeg binary not found at {ffmpeg_bundle_path}; building without it. "
        "Multi-View will require a separate ffmpeg install (see README), or set "
        "FFMPEG_BUNDLE_PATH to an ffmpeg.exe before building to include one."
    )

a = Analysis(
    ['main.py'],
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

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='jellyfin-sports-proxy',
    icon=icon_file,
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    # UPX can trigger antivirus false positives and startup failures on Windows.
    upx=False,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

