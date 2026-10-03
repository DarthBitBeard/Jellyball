# Troubleshooting

Start here when something does not work. Each entry says what to check first.
It describes behaviour that exists in the code today; a diagnostics bundle and
a first-run checklist are **planned** and not available yet.

## First: where are the logs and the data?

| Install | Data folder | Log |
| :--- | :--- | :--- |
| Windows service (installer) | `%ProgramData%\Jellyball` | `jellyball.log` in it |
| Windows tray app | `%LOCALAPPDATA%\Jellyball` | `jellyball.log` in it |
| Docker | `/app/data` (host `./data` with the shipped compose file) | `docker compose logs jellyball` and `jellyball.log` |
| Linux (systemd) | `/var/lib/jellyball` | `journalctl -u jellyball` and `jellyball.log` |

`JELLYBALL_DATA_DIR` overrides the folder. The log rotates at 10 MB (5 files);
the dashboard **Logs** tab and `/api/logs` show the tail. A quick health check
that needs no password: `http://<host>:8000/healthz` returns
`{"status":"ok", ...}` with the version.

## The dashboard

**I cannot sign in / do not know the password.** On a network bind (Docker,
Linux, or the installer's "allow network access") a password is required. If
you did not set `DASHBOARD_PASSWORD`, one was generated on first start and
saved in `dashboard-password.txt` in the data folder; the user name is
`admin` unless `DASHBOARD_USERNAME` says otherwise. The password is never
written to the log. To choose your own, set `DASHBOARD_PASSWORD` in `.env`
(or the environment) and restart.

**"Too many failed logins" (HTTP 429).** After 8 wrong attempts within five
minutes, that client address is locked out for five minutes. Behind a reverse
proxy every browser may share the proxy's address; see [DOCKER.md](DOCKER.md).

**"Dashboard is local-only" (HTTP 403).** The dashboard has no password and is
being opened by a non-local address or name. Without a password it works only
on a loopback bind and with a `localhost`/`127.0.0.1` address (or the
computer's own name). Set a password and bind to the network, or browse from
the same machine.

**Saving a setting fails with "Cross-site request rejected".** The browser's
`Origin` does not match the `Host` Jellyball received. This happens behind a
reverse proxy that rewrites `Host`. Make the proxy pass the original `Host`
header, or set `TRUST_X_FORWARDED_HOST=1` and send `X-Forwarded-Host` (only if
the proxy overwrites it).

## Starting up

**"Port 8000 on ... is in use; refusing to start on a different port".** Something
else owns the port, often a second Jellyball (the Windows service plus a
manually started `JellyballConsole.exe --console`, or a second container).
Stop one, or change `PORT`. Jellyball waits up to `PORT_BIND_WAIT_SECONDS`
(30) for the port to free up before giving up. The tray app, unlike the
service, may pick a different port for that session and says so.

**"Jellyball must run with a single worker".** `WEB_CONCURRENCY` is set to
something other than 1. Remove it; Jellyball keeps session state in memory and
cannot run with several workers.

**The Windows service will not start.** Stop the service, run
`JellyballConsole.exe --console` from an administrator console and read the
output (do not run both at once). Check `jellyball.log`, and that
`%ProgramData%\Jellyball` is writable by the service account (the installer
sets this; moving or re-permissioning the folder by hand can lock the service
out of its own database).

**It started but a provider search finds nothing and the log mentions
Playwright/Chromium.** Jellyball continues with HTTP-only extraction when
Chromium cannot start, which works for fewer sites. In Docker the image
contains Chromium; from source run `python -m playwright install chromium`;
on Linux without Docker see `deploy/linux/README.md` (`--with-deps` needs
root).

## Streams

**A channel shows "No Signal".** Either no candidate has been found yet (a
channel waits up to `STREAM_STARTUP_TIMEOUT` seconds, then shows the
placeholder), the game has not started, or every source failed and Jellyball
is probing for recovery. Check the channel card in the dashboard, then the log
for the provider names. Use **rescrape** on the channel card.

**Many channels have no sources at once.** A provider (or several) is
down, moved or newly blocked. See [PROVIDERS.md](PROVIDERS.md): update the
base URL in **Provider Domains**, look at the Performance tab and `/metrics`
breaker state, and consider `STREAM_PROVIDER_PRIORITY`.

**Playback stutters or freezes during a source switch.** Switches are signalled
as an HLS discontinuity; clients differ in how they handle that. See the
client matrix in [JELLYFIN.md](JELLYFIN.md) (all `unverified` today) and report
the client and version.

**A source keeps being skipped as "incompatible".** fMP4, demuxed-audio and
SAMPLE-AES sources cannot go through the main session engine and use the
legacy relay; Jellyball prefers a normal source when one exists and retries an
incompatible one after `INCOMPATIBLE_RETRY_SECONDS` (3600).

## Jellyfin

**Guide or channels do not update in Jellyfin.** Jellyfin reads XMLTV only when
its Refresh Guide task runs. Run **Refresh Guide Data** on the tuner and guide
provider, or configure the automatic refresh ([JELLYFIN.md](JELLYFIN.md)). If
duplicates linger, remove and re-add the tuner.

**Test Jellyfin Connection fails.** The card names the failing step:

| Step | Meaning | Fix |
| :--- | :--- | :--- |
| `config` | URL or API key missing, or key contains invalid characters | Enter both; paste the key without quotes. |
| `reach` | Cannot connect, timed out, or redirected | Check the URL, http vs https, the port; from Docker use an address the container can reach (not `localhost`). |
| `auth` | Key rejected (both header styles tried) | Create a new key in Jellyfin Dashboard, API Keys. |
| `discover` | No Refresh Guide task found | Set up Live TV in Jellyfin first. |
| `trigger` | The task would not start | Check the log line for the HTTP status. |

The Jellyfin URL may be a private (LAN) address; the Jellyfin client allows that
even though scraping targets must be public.

**The M3U links Jellyfin uses point at the wrong address (for example
`127.0.0.1`).** Links are built from the `Host` header of the request that
fetched the playlist. Fetch `/playlist.m3u` from the address Jellyfin should
use, or fix the proxy's `Host` header.

## Multi-View

**"No ffmpeg found" warning / Multi-View channels unavailable.** Install
ffmpeg or point `FFMPEG_PATH` (or `MULTIVIEW_FFMPEG_PATH`) at it. Resolution
order: `MULTIVIEW_FFMPEG_PATH`, `FFMPEG_PATH`, Jellyfin's own ffmpeg if it is
installed on the same machine, then the bundled one (Windows installer builds
that included it).

**High CPU while a grid runs.** The encoder fell back to software
(`libx264`). An NVENC failure switches to software for
`NVENC_FALLBACK_SECONDS` (600) and then retries hardware. Common causes: an
ffmpeg whose NVENC needs a newer NVIDIA driver than installed, or a container
without a GPU. Update the driver, use Jellyfin's ffmpeg, or set
`MULTIVIEW_HWACCEL=none` to accept the CPU cost. See [DOCKER.md](DOCKER.md)
for containers. `/api/ffmpeg-status` shows the process status, recent
ffmpeg log lines and the last error.

**The grid keeps restarting.** A "Multi-View Unstable" alert is sent when that
happens. Check `/api/ffmpeg-status`, and whether a member channel has no signal
(missing members are backfilled with the placeholder instead of stalling).

## Upgrades

See [UPGRADING.md](UPGRADING.md) for backups and rolling back.

## Reporting a problem

Open an issue with: Jellyball version (`/healthz`), how you run it (Windows
service, Docker, Linux), Jellyfin version and client, what you expected and
the relevant `jellyball.log` lines. Remove any URL that carries a token and
any API key or password before pasting.
