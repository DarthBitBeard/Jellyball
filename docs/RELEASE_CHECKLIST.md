# Jellyball release checklist

Steps to cut a Jellyball release, in order. Run everything from a Windows
machine with Python 3.12, ffmpeg (`%LOCALAPPDATA%\ffmpeg\bin\ffmpeg.exe` or on
`PATH`/`FFMPEG_PATH`), and Inno Setup 6 available.

## 1. Bump the version

Edit `Jellyball/version.py` and bump `__version__` (this is the single source
used for the app, the PyInstaller `.exe` `VERSIONINFO`, and the Inno Setup
installer filename/`AppVersion`):

```python
__version__ = "X.Y.Z"
```

## 2. Update `CHANGELOG.md`

Add a dated `## X.Y.Z` entry at the top summarizing user-facing changes since
the last release (features, fixes, breaking config/behavior changes). Keep it
in the same style as prior entries.

## 3. Test

Run these from `Jellyball/`, in order, and confirm each one is clean before
moving on.

### 3a. Unit suite

```powershell
python -m unittest discover -p "test_*.py"
```

Expect `OK` (a couple of intentionally opt-in skips, e.g. load/soak tests and
the e2e wrappers below, are fine).

### 3b. End-to-end tool wrappers

Requires ffmpeg; this drives `tools/e2e_failover.py` and
`tools/e2e_multiview.py` as real subprocesses (fake HLS origins + real
ffmpeg, no internet needed):

```powershell
$env:JELLYBALL_E2E = "1"
python -m unittest test_e2e_tools -v
Remove-Item Env:\JELLYBALL_E2E
```

Both `test_e2e_failover` and `test_e2e_multiview` must pass. If either fails,
re-run the underlying script directly (e.g.
`python tools/e2e_failover.py --keep`) to inspect its kept scratch directory
and ffmpeg log.

### 3c. Soak test (30 minutes, 3 channels)

```powershell
python tools/soak.py --minutes 30 --channels 3
```

Watch for the final `PASS`/`FAIL` summary. This exercises hours-equivalent
churn (repeated failover flapping) in ~30 minutes; a `FAIL` here (consumer
restart, growing lag, rising RSS, or session `memory_mb` exceeding its cap)
is a stability regression and blocks the release until root-caused - don't
ship over it.

### 3d. CI green + Docker smoke

- [ ] GitHub Actions CI is green on the release commit (unit + ruff + e2e +
      docker-smoke jobs in `.github/workflows/ci.yml`).
- [ ] Locally (optional): `docker compose build && docker compose up -d`
      reaches a healthy `HEALTHCHECK` within ~60s.

## 4. Build the installer

```powershell
.\build-installer.ps1
```

Run from `Jellyball/`. This produces
`Jellyball/installer/Output/JellyballSetup-X.Y.Z.exe` (PyInstaller ONEDIR
build + Inno Setup). Confirm the version in the produced filename matches
step 1, and that the script printed both `Jellyball.exe` and
`JellyballConsole.exe` as built before it got to the Inno Setup step.

If code signing is configured (`SIGN_CERT_THUMBPRINT` or
`AZURE_SIGNING_DLIB` + `AZURE_SIGNING_METADATA`), confirm the script printed
`Signing ...` for both executables and the installer, then verify:

```powershell
Get-AuthenticodeSignature .\installer\Output\JellyballSetup-X.Y.Z.exe
```

Status should be `Valid`. Unsigned builds are acceptable for internal
testing but should not be the public GitHub release asset.

## 5. Upgrade-install on the server

Copy `JellyballSetup-X.Y.Z.exe` to the actual server this is deployed on (not
just a scratch VM) and run it there, over the existing install, then verify:

- [ ] **`.env` is kept.** `%ProgramData%\Jellyball\.env` still has the same
      content (port, dashboard credentials, network-access choice, any
      manually-added settings) as before the upgrade - the installer wizard
      should have skipped the configuration page entirely because `.env`
      already existed.
- [ ] **The database is kept.** `%ProgramData%\Jellyball\sports_proxy.db`
      (or wherever `DB_FILE` points) is untouched - existing channel
      state/config isn't reset to defaults.
- [ ] **The service restarts cleanly.** After the installer finishes, the
      `Jellyball` service (display name "Jellyball Sports Proxy") is
      `Running`:
      ```powershell
      Get-Service Jellyball
      ```
      and `%ProgramData%\Jellyball\jellyball.log` shows a fresh startup with
      no errors.
- [ ] **A normal channel plays in Jellyfin.** Add/refresh the M3U + XMLTV
      URLs in Jellyfin (Start Menu shortcut "Jellyball Dashboard" shows
      them), tune to at least one regular channel, and confirm it plays.
- [ ] **A Multi-View audio channel plays in Jellyfin.** Tune to one of the
      `audio-N.m3u8` per-member audio channels for an active Multi-View grid
      and confirm audio plays and matches the selected member.
- [ ] **`Stop-Service` is clean.**
      ```powershell
      Stop-Service Jellyball
      Get-Process Jellyball, JellyballConsole -ErrorAction SilentlyContinue
      ```
      The service reports `Stopped` promptly, no `Jellyball*` process is left
      running, and no ffmpeg child processes are orphaned
      (`Get-Process ffmpeg -ErrorAction SilentlyContinue`).

If any of the above fails, do not tag - fix forward and repeat step 5.

## 6. Tag and push

```powershell
git tag vX.Y.Z
git push origin vX.Y.Z
```

Pushing the tag triggers `.github/workflows/release.yml`, which, in order:

1. checks the tag matches `Jellyball/version.py` and that `CHANGELOG.md` has a
   `## [X.Y.Z]` section for it (fold the `changelog.d/` fragments in first
   with `python Jellyball/tools/changelog.py release X.Y.Z`), then runs the
   unit tests, ruff and mypy;
2. builds the installer on `windows-latest` (signed when the secrets are
   set) and writes `SHA256SUMS`;
3. builds and pushes `ghcr.io/darthbitbeard/jellyball:X.Y.Z` (linux/amd64,
   provenance and SBOM; `latest` and `X.Y` only for stable tags);
4. creates the GitHub release with `JellyballSetup-X.Y.Z.exe`, `SHA256SUMS`
   and the notes taken from the CHANGELOG section. A tag containing `-`
   (`vX.Y.Z-beta.1`, `-rc.1`) is published as a pre-release.

Confirm all four jobs succeed, the installer and `SHA256SUMS` are attached,
the checksum matches the downloaded installer, and
`docker pull ghcr.io/darthbitbeard/jellyball:X.Y.Z` works (the first push
creates a private package: make it public in the repository's package
settings) before announcing the release.
