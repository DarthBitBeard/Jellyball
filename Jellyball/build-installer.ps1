<#
.SYNOPSIS
    Builds the Jellyball Windows installer: PyInstaller ONEDIR build +
    Inno Setup service installer.

.DESCRIPTION
    1. Creates/reuses a build-only virtual environment (.venv-build) so this
       never pollutes (or depends on) whatever environment you normally run
       Jellyball from.
    2. Installs requirements-build.txt (runtime deps + PyInstaller + pywin32).
    3. Installs Playwright's Chromium build into the default per-user cache
       (%LOCALAPPDATA%\ms-playwright) - PLAYWRIGHT_BROWSERS_PATH is explicitly
       unset first so a developer's custom override doesn't leave Chromium
       somewhere jellyball.spec won't look.
    4. Runs PyInstaller against jellyball.spec (ONEDIR -> dist\Jellyball\).
    5. Verifies both Jellyball.exe and JellyballConsole.exe were produced.
    6. Locates ISCC.exe (Inno Setup 6) and compiles installer\jellyball.iss
       into installer\Output\JellyballSetup-<version>.exe.

    Stops immediately on any failure. If PyInstaller succeeds but ISCC.exe
    cannot be found, the script still exits non-zero (after printing install
    instructions) since no installer was produced - but dist\Jellyball\ is
    left in place either way.
#>

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectRoot

function Assert-Success {
    param([string]$Message)
    if ($LASTEXITCODE -ne 0) {
        throw "$Message (exit code $LASTEXITCODE)"
    }
}

# ---------------------------------------------------------------------------
# 1. Build venv
# ---------------------------------------------------------------------------
$VenvDir = Join-Path $ProjectRoot ".venv-build"
$VenvPython = Join-Path $VenvDir "Scripts\python.exe"

if (-not (Test-Path $VenvPython -PathType Leaf)) {
    Write-Host "Creating build virtual environment at $VenvDir"
    python -m venv $VenvDir
    Assert-Success "Failed to create virtual environment"
} else {
    Write-Host "Reusing existing build virtual environment at $VenvDir"
}

# ---------------------------------------------------------------------------
# 2. Install build + runtime dependencies
# ---------------------------------------------------------------------------
Write-Host "Installing build dependencies from requirements-build.txt"
& $VenvPython -m pip install --upgrade pip
Assert-Success "pip upgrade failed"
& $VenvPython -m pip install -r (Join-Path $ProjectRoot "requirements-build.txt")
Assert-Success "Dependency installation failed"

# ---------------------------------------------------------------------------
# 3. Playwright Chromium (into the default %LOCALAPPDATA%\ms-playwright
#    cache, which jellyball.spec reads from unless PLAYWRIGHT_BROWSERS_PATH
#    is set at build time)
# ---------------------------------------------------------------------------
Write-Host "Installing Playwright Chromium"
$PreviousPlaywrightBrowsersPath = $env:PLAYWRIGHT_BROWSERS_PATH
Remove-Item Env:\PLAYWRIGHT_BROWSERS_PATH -ErrorAction SilentlyContinue
try {
    & $VenvPython -m playwright install chromium --only-shell
    Assert-Success "Playwright Chromium installation failed"
} finally {
    if ($null -ne $PreviousPlaywrightBrowsersPath) {
        $env:PLAYWRIGHT_BROWSERS_PATH = $PreviousPlaywrightBrowsersPath
    }
}

# ---------------------------------------------------------------------------
# 4. PyInstaller (ONEDIR -> dist\Jellyball\)
# ---------------------------------------------------------------------------
Write-Host "Running PyInstaller"
& $VenvPython -m PyInstaller --clean --noconfirm jellyball.spec
Assert-Success "PyInstaller build failed"

# ---------------------------------------------------------------------------
# 5. Verify both executables were produced
# ---------------------------------------------------------------------------
$DistDir = Join-Path $ProjectRoot "dist\Jellyball"
$WindowedExe = Join-Path $DistDir "Jellyball.exe"
$ConsoleExe = Join-Path $DistDir "JellyballConsole.exe"

if (-not (Test-Path $WindowedExe -PathType Leaf)) {
    throw "Expected executable was not produced: $WindowedExe"
}
if (-not (Test-Path $ConsoleExe -PathType Leaf)) {
    throw "Expected executable was not produced: $ConsoleExe"
}
Write-Host "Built $WindowedExe"
Write-Host "Built $ConsoleExe"

# ---------------------------------------------------------------------------
# 6. Compile the Inno Setup installer
# ---------------------------------------------------------------------------
$VersionText = Get-Content (Join-Path $ProjectRoot "version.py") -Raw
if ($VersionText -notmatch '__version__\s*=\s*[''"]([^''"]+)[''"]') {
    throw "Could not parse __version__ from version.py"
}
$AppVersion = $Matches[1]
Write-Host "Jellyball version: $AppVersion"

$IsccCandidates = @()
$IsccOnPath = Get-Command "ISCC.exe" -ErrorAction SilentlyContinue
if ($IsccOnPath) { $IsccCandidates += $IsccOnPath.Source }
$IsccCandidates += Join-Path ${env:ProgramFiles(x86)} "Inno Setup 6\ISCC.exe"
$IsccCandidates += Join-Path $env:ProgramFiles "Inno Setup 6\ISCC.exe"
$IsccCandidates += Join-Path $env:LOCALAPPDATA "Programs\Inno Setup 6\ISCC.exe"

$IsccPath = $IsccCandidates | Where-Object { $_ -and (Test-Path $_ -PathType Leaf) } | Select-Object -First 1

if (-not $IsccPath) {
    Write-Host ""
    Write-Host "PyInstaller build succeeded, but Inno Setup's ISCC.exe was not found." -ForegroundColor Yellow
    Write-Host "Install Inno Setup 6 and re-run this script to produce the installer:" -ForegroundColor Yellow
    Write-Host "    winget install JRSoftware.InnoSetup" -ForegroundColor Yellow
    Write-Host "(or download it from https://jrsoftware.org/isinfo.php)" -ForegroundColor Yellow
    exit 1
}
Write-Host "Using Inno Setup compiler: $IsccPath"

$IssPath = Join-Path $ProjectRoot "installer\jellyball.iss"
$SourceDir = $DistDir

& $IsccPath "/DAppVersion=$AppVersion" "/DSourceDir=$SourceDir" $IssPath
Assert-Success "Inno Setup compilation failed"

$OutputExe = Join-Path $ProjectRoot "installer\Output\JellyballSetup-$AppVersion.exe"
if (-not (Test-Path $OutputExe -PathType Leaf)) {
    throw "Expected installer was not produced: $OutputExe"
}

Write-Host ""
Write-Host "Installer built: $OutputExe" -ForegroundColor Green
