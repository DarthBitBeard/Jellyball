$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectRoot

python -m pip install -r requirements.txt pyinstaller playwright
if ($LASTEXITCODE -ne 0) { throw "Dependency installation failed with exit code $LASTEXITCODE." }

python -m playwright install chromium
if ($LASTEXITCODE -ne 0) { throw "Playwright Chromium installation failed with exit code $LASTEXITCODE." }

python -m PyInstaller --clean --noconfirm jellyfin-sports-proxy.spec
if ($LASTEXITCODE -ne 0) { throw "PyInstaller failed with exit code $LASTEXITCODE." }

$artifact = Join-Path $projectRoot "dist\jellyfin-sports-proxy.exe"
if (-not (Test-Path $artifact -PathType Leaf)) { throw "Expected executable was not produced: $artifact" }

Write-Host "Built $artifact"
