# Using Jellyball with Jellyfin

Jellyball looks like an M3U tuner plus an XMLTV guide to Jellyfin. This page
covers setup, the optional automatic guide refresh, what is known about
Jellyfin 12, and which Jellyfin clients have (and have not) been verified.
It describes the code as it is today; **planned** items are not implemented.

## Setup

Use the address of the machine running Jellyball (replace `<host>` and the
port if you changed it).

1. Jellyfin **Dashboard, Live TV, Tuner Devices, +**, **M3U Tuner**, File or
   URL: `http://<host>:8000/playlist.m3u`
2. **Dashboard, Live TV, TV Guide Data Providers, +**, **XMLTV**, File or URL:
   `http://<host>:8000/epg.xml`
3. **Dashboard, Playback, Streaming**: allow direct stream for Live TV so
   Jellyfin does not add an unnecessary transcode.
4. After changing which teams or channels are enabled in Jellyball, refresh the
   tuner and the guide in Jellyfin (the three-dots menu, Refresh Guide Data). If
   Jellyfin keeps stale duplicates, remove and re-add the tuner.
5. To record, open Live TV, Guide, pick an active game, Record. Jellyfin's
   scheduler records the proxied stream into your TV library.

The URLs in the playlist are built from the `Host` header of the request that
fetched it. Fetch the playlist using the address Jellyfin should use later (not
`localhost`), and see [DOCKER.md](DOCKER.md) if a reverse proxy sits in between.

Jellyball itself does not require anything from Jellyfin to work: playback,
failover and the guide are served to whichever client asks. The Jellyfin
connection below only automates the guide refresh.

## Automatic guide refresh (optional)

Jellyfin only re-reads an XMLTV source when its "Refresh Guide" scheduled task
runs. Jellyball can trigger that task when the channel list or schedule
changes.

1. In Jellyfin: **Dashboard, API Keys**, create a key for Jellyball.
2. In Jellyball, **Alerts & Integrations**: enter the Jellyfin URL
   (`JELLYFIN_URL`, e.g. `http://localhost:8096`) and the key
   (`JELLYFIN_API_KEY`), save. The "Refresh Guide" task id is detected
   automatically (`JELLYFIN_TASK_ID` overrides it).
3. **Test Jellyfin Connection** triggers a refresh and reports which step
   failed.

What the code does:

* It sends `Authorization: MediaBrowser Token="<key>"` first. If Jellyfin
  answers 401/403 it retries once with the legacy `X-Emby-Token` header and
  remembers which one worked (in memory, per Jellyfin host).
* It reads `/System/Info/Public` (no authentication needed) to learn the
  Jellyfin version and server name, then lists `/ScheduledTasks`, finds the
  task whose key is `RefreshGuide`, and starts it with
  `POST /ScheduledTasks/Running/<id>`. A stale saved task id (404) is looked up
  again once.
* It never follows redirects (the API key must not travel to another host) and
  never logs the key. A repeating failure is logged at most once an hour.
* The last attempt's outcome (time, OK or the failing step, HTTP status, which
  header worked) is stored and shown on the Alerts & Integrations card, and
  the Jellyfin version last seen appears at `/api/version` (behind dashboard
  auth). Automatic refreshes are spaced at least
  `JELLYFIN_AUTO_REFRESH_MIN_INTERVAL` seconds apart (default 600).

Failure steps you may see: `config` (URL or key missing), `reach` (cannot
connect, timed out, or redirected: check http/https and the port), `auth` (key
rejected, both header styles tried), `discover` (no Refresh Guide task: is Live
TV set up?), `trigger` (the task would not start).

## Jellyfin 12 notes

* **Authentication.** Jellyfin 12 deprecated the legacy authorization
  mechanisms. Jellyball up to 2.0.0 sent only `X-Emby-Token`, which stops the
  guide refresh working once a server rejects it; from 2.0.1 it sends the
  documented `MediaBrowser Token=` header first and keeps the legacy header
  as a fallback, so it works on both generations.
* **Tested range.** The dashboard warns when the server version is outside
  10.11 to 12.x (`TESTED_RANGE_TEXT` in `jellyfin_client.py`). That range is
  what the guide-refresh code and its tests (against a fake Jellyfin) cover;
  it has not been confirmed against a live Jellyfin 12 server by the
  maintainers, so treat 12.x as "expected to work".
* **M3U and XMLTV.** Both are plain standard formats and are unchanged by
  Jellyfin version. Channel ids and `tvg-id`s are stable across Jellyball
  versions so Jellyfin keeps its channel mappings.
* **Multi-View encoding.** Multi-View uses ffmpeg. Jellyball prefers
  Jellyfin's own ffmpeg when Jellyfin runs on the same machine, looking at the
  default Windows install location and `/usr/lib/jellyfin-ffmpeg/ffmpeg` or
  `/usr/share/jellyfin-ffmpeg/ffmpeg` on Linux. It does not ask the Jellyfin
  API where ffmpeg is, so a Jellyfin that installs ffmpeg elsewhere, or runs in
  another container, is not found: set `FFMPEG_PATH` (or
  `MULTIVIEW_FFMPEG_PATH`) instead. Whether Jellyfin 12 moved its ffmpeg is
  unchecked; if Multi-View falls back to software encoding after a Jellyfin
  upgrade, set the path explicitly. Asking Jellyfin for the path is **planned**.

## Planned for 2.1.0 (not in the code yet)

* A "Connect Jellyfin" button that registers the tuner and guide provider for
  you and adopts an existing tuner instead of duplicating it.
* Richer guide entries (sub-title, description, categories so games show under
  Jellyfin's Sports filter), and stable `ETag` headers on `/playlist.m3u` and
  `/epg.xml`.

## Client compatibility matrix

Jellyfin clients differ in how well they handle live HLS that changes source
(a failover is signalled as an HLS discontinuity) and in whether they expose
the extra audio channels of a Multi-View. **No client below has been verified
by the maintainers yet**; every cell says `unverified` until someone runs the
checklist and reports back (open an issue with the client, its version, the
Jellyfin version and the result).

| Client | Live TV playback | Failover without a stall | Multi-View audio channels | Recording playback |
| :--- | :--- | :--- | :--- | :--- |
| Jellyfin Web (browser) | unverified | unverified | unverified | unverified |
| Android / Android TV / Google TV | unverified | unverified | unverified | unverified |
| Fire TV | unverified | unverified | unverified | unverified |
| iOS / tvOS (Swiftfin) | unverified | unverified | unverified | unverified |
| Roku | unverified | unverified | unverified | unverified |
| Kodi (Jellyfin add-on) | unverified | unverified | unverified | unverified |
| Samsung (Tizen) / LG (webOS) | unverified | unverified | unverified | unverified |

### Verification checklist

For each client, with a channel that is live:

1. Tune the channel from the Guide; playback starts within about 30 seconds
   and has audio.
2. Leave it playing, then in the Jellyball dashboard force a failover on that
   channel (override or rescrape); playback continues without the client
   reporting an error.
3. Tune a Multi-View channel; switch to a `🔊` audio channel of a member and
   confirm the commentary changes.
4. Schedule a short recording of a live game and play it back.
5. Record the client name and version, the Jellyfin version, and whether
   Jellyfin transcoded (Dashboard, Activity), then update the table.

## Troubleshooting

See [TROUBLESHOOTING.md](TROUBLESHOOTING.md): guide not updating, channels
duplicated, Test Jellyfin Connection failing, no playback.
