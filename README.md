<div align="center">

<img src="Jellyball/assets/jellyball-logo.jpg" alt="Jellyball" width="200">

# Jellyball

**A live-sports tuner for Jellyfin, with failover that doesn't freeze your stream.**

[![CI](https://github.com/DarthBitBeard/Jellyball/actions/workflows/ci.yml/badge.svg)](https://github.com/DarthBitBeard/Jellyball/actions/workflows/ci.yml)
[![Latest release](https://img.shields.io/github/v/release/DarthBitBeard/Jellyball)](https://github.com/DarthBitBeard/Jellyball/releases/latest)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE.txt)
![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue)
![Windows | Docker | Linux](https://img.shields.io/badge/runs%20on-Windows%20%7C%20Docker%20%7C%20Linux-lightgrey)

</div>

Jellyball finds live sports streams from several sources and presents them to
Jellyfin as an ordinary **M3U tuner + XMLTV guide**. Pick your teams, add the
two URLs to Jellyfin Live TV, and games show up in the guide, ready to watch or
record with Jellyfin's own DVR.

Streams die mid-game. Jellyball keeps a ranked list of backups for every
channel and switches sources **without breaking playback**: your player sees one
continuous stream instead of a stall or a reconnect.

## Highlights

- **Seamless failover.** Every channel is a persistent session with its own HLS
  timeline. A source switch is signaled as an HLS discontinuity, so Jellyfin's
  ffmpeg keeps decoding straight through it.
- **Pick teams, not URLs.** Choose exact NFL, MLB, NHL, NBA and college teams
  from dashboard toggles. Identity-aware matching rejects look-alikes (Eastern
  Michigan vs. Michigan State, Rays vs. Buccaneers).
- **Multi-View.** Combine 2 or 4 channels into one grid (side-by-side or 2×2)
  with NVENC/QSV hardware encoding, and switch commentary audio per member.
- **Always-live channels.** ESPN, NFL RedZone, FOX Sports, CBS Sports Network
  and more, with stable `tvg-id`s for mapping to an external EPG.
- **A guide that stays correct.** The XMLTV guide runs about 10 days ahead, with
  realistic per-sport game lengths, so channels never show expired data.
- **Dashboard.** Channel control, live-editable tunables (no restart), stability
  metrics, logs, and Discord/Telegram alerts on failover and recovery.
- **Observable.** `/api/sessions`, `/api/status` and a Prometheus `/metrics`
  endpoint.
- **Secure by default.** Dashboard auth on any non-loopback bind, CSRF origin
  checks, HMAC-signed relay URLs, and SSRF guards on outbound requests.

## Install

### Windows installer (recommended)

1. Download `JellyballSetup-<version>.exe` from the
   [latest release](https://github.com/DarthBitBeard/Jellyball/releases/latest)
   and run it as administrator.
2. On first install, choose a port (default `8000`), a dashboard username and
   password, and whether other computers on your network may connect. These
   are stored in `%ProgramData%\Jellyball\.env` and are never overwritten by an
   upgrade.
3. Jellyball installs as a Windows service that starts automatically. Open
   **Jellyball Dashboard** from the Start Menu.

> The installer is not code-signed, so Windows SmartScreen may warn you.
> Choose **More info → Run anyway**.

### Docker

```bash
cp Jellyball/.env.example .env     # review DASHBOARD_PASSWORD
docker compose up -d
```

Open `http://<server-ip>:8000`. If you didn't set a password, Jellyball
generates one and saves it to `./data/dashboard-password.txt` (logs only
point at that file — they do not print the password).

### From source

Requires Python 3.12+.

```bash
cd Jellyball
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m playwright install chromium
cp .env.example .env
python main.py
```

On Linux, install the browser with `python -m playwright install --with-deps
chromium` instead (needs root): without the system libraries Chromium cannot
launch and Jellyball silently falls back to HTTP-only stream extraction.

## Connect Jellyfin

1. Open the Jellyball dashboard, go to **Channels & Streams**, expand a Sports
   Catalog category, tick your teams, and click **Apply Selected Teams**.
2. In Jellyfin, go to **Dashboard → Live TV → Tuner Devices → + → M3U Tuner**
   and use `http://<jellyball-host>:8000/playlist.m3u`.
3. Under **TV Guide Data Providers → + → XMLTV**, use
   `http://<jellyball-host>:8000/epg.xml`.
4. Refresh the guide. To record a game, open **Live TV → Guide**, select it,
   and click **Record**.

More detail (recording, stale entries, `tvg-id` mapping) is in the
[full documentation](Jellyball/README.md#jellyfin-setup).

## Documentation

| | |
| :--- | :--- |
| [Full documentation](Jellyball/README.md) | Every setting, Multi-View and ffmpeg setup, the security model, troubleshooting, architecture |
| [`.env.example`](Jellyball/.env.example) | Copy-pasteable configuration template |
| [Changelog](CHANGELOG.md) | What changed in each release |
| [Release checklist](docs/RELEASE_CHECKLIST.md) | How releases are built and verified |

## Security notes

Dashboard login protects the dashboard, **not** the playback endpoints
(`/playlist.m3u`, `/epg.xml`, `/stream/*`). Anyone who can reach the port can
watch your channels. Run Jellyball on a trusted LAN, behind a VPN, or behind a
TLS reverse proxy, and **do not expose it directly to the internet**. See the
[security model](Jellyball/README.md#security-model).

## Development

```bash
cd Jellyball
python -m unittest discover -p "test_*.py"   # unit suite (no ffmpeg or browsers needed)
ruff check .
```

CI runs the suite and lint on Windows and Linux. Pushing a `v*` tag builds the
installer and attaches it to a GitHub release. Building it yourself is covered
in the [full documentation](Jellyball/README.md#building-from-source-and-running-tests).

## Contributing

Community contributions are welcome via **fork → pull request** into `master`
(the protected default branch). See [CONTRIBUTING.md](CONTRIBUTING.md) for the
workflow and local test commands. You do not need write access to this repo to
propose changes.

## Disclaimer

Jellyball hosts, stores and distributes no media. It is a tool that finds and
relays publicly reachable streams published by third-party websites. You are
responsible for making sure your use complies with the laws of your country and
with the terms of the services involved. Jellyball is not affiliated with
Jellyfin, ESPN, or any league, team, broadcaster or stream provider; all names
and marks belong to their owners.

## License

[MIT](LICENSE.txt) © 2026 DarthBitBeard
