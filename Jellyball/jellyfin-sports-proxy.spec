# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_all
import os

datas = []
binaries = []
hiddenimports = [
    'sports_matcher',
    'stream_extractor',
    'uvicorn.logging',
    'uvicorn.loops',
    'uvicorn.loops.auto',
    'uvicorn.protocols',
    'uvicorn.protocols.http',
    'uvicorn.protocols.http.auto',
    'uvicorn.lifespan',
    'uvicorn.lifespan.on',
    'pystray',
    'PIL'
]

for pkg in ['tzdata', 'playwright', 'rapidfuzz', 'thefuzz', 'bs4', 'httpx', 'pystray', 'PIL']:
    try:
        tmp_ret = collect_all(pkg)
        datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
    except Exception:
        pass

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

