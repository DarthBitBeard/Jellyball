# Upgrading and rolling back

Jellyball keeps your settings, channels and Multi-View setups in a data folder
that an upgrade does not replace. This page explains how to upgrade each kind
of install, what safety copy Jellyball makes for you, and how to go back.

What stays compatible across upgrades (and is checked by the project's
compatibility tests): `/playlist.m3u`, `/epg.xml`, `/stream/*`, channel ids,
`tvg-id`s, your `.env` and your database. Jellyfin should not need to re-map
channels after an upgrade. Read the release notes of each version on the
GitHub Releases page (and `CHANGELOG.md`) before upgrading; they list anything
that is not compatible.

## Before you upgrade: make a backup

Jellyball makes an automatic copy of the database, but only in one situation
(see below), so make your own as well. Stop Jellyball first so the files are not
in use, then copy the **whole data folder**:

| Install | Data folder |
| :--- | :--- |
| Windows service | `%ProgramData%\Jellyball` |
| Windows tray app | `%LOCALAPPDATA%\Jellyball` |
| Docker | the `./data` folder next to `docker-compose.yml` |
| Linux (systemd) | `/var/lib/jellyball` |

The files that matter are `.env`, `sports_proxy.db` (settings and channels; if
a `sports_proxy.db-wal` or `-shm` file exists copy it with the rest while the
service is stopped), `relay-signing.key` and `dashboard-password.txt` if
present.

## The automatic database backup

When an upgrade needs to change the database layout (a schema migration),
Jellyball copies the database to `sports_proxy.db.bak-<version>` next to it
before touching it. Details, all from the code:

* It happens once per app version: if `sports_proxy.db.bak-<version>` already
  exists it is kept and not overwritten.
* The newest three backups are kept; older ones are deleted.
* A brand-new install, or a database with nothing in it yet, makes none.
* If making the copy fails, the failure is logged and the upgrade continues
  (a service that will not start is considered worse than a missing copy), so
  do not rely on it as your only backup.
* An upgrade that changes no schema makes no copy.

## Upgrade

### Windows installer

1. Download the new `JellyballSetup-<version>.exe` from the
   [Releases](https://github.com/DarthBitBeard/Jellyball/releases) page, and
   optionally check it against the release's `SHA256SUMS` file:

   ```powershell
   (Get-FileHash .\JellyballSetup-X.Y.Z.exe -Algorithm SHA256).Hash.ToLower()
   Get-Content .\SHA256SUMS
   ```

   The two hashes must match. (An unsigned installer triggers a SmartScreen
   warning; that is separate from the checksum.)
2. Run it over the existing install, as administrator. It stops the service,
   replaces the program files, keeps your existing `.env` (the settings page
   is skipped) and your data folder, and starts the service again. Unattended
   installs can use `/VERYSILENT`; the `/PORT`, `/USER`, `/LAN` and `/DATADIR`
   switches matter for a fresh install: `/PORT`, `/USER` and `/LAN` are
   ignored when a `.env` already exists, and `/DATADIR` does not move existing
   data (it would start a new, empty data folder), so leave it out when
   upgrading.
3. Check `Get-Service Jellyball` is `Running` and that `http://localhost:<port>/healthz`
   reports the new version.

### Docker

```bash
docker compose pull          # fetches ghcr.io/darthbitbeard/jellyball:<tag>
docker compose up -d         # recreates the container; ./data is kept
```

With the default `latest` tag you follow stable releases. To control when you
move, pin `JELLYBALL_IMAGE=ghcr.io/darthbitbeard/jellyball:X.Y.Z` in `.env`
and change it deliberately. If you build locally, `git pull`/`git checkout
vX.Y.Z` and `docker compose build && docker compose up -d`.

### Linux (systemd)

Follow "Upgrading and rolling back" in
[`deploy/linux/README.md`](../deploy/linux/README.md).

### From source

Stop Jellyball, `git checkout vX.Y.Z`, `pip install -r requirements.txt`,
`python -m playwright install chromium` (Playwright's version pins the browser
build), start it again.

### Beta and release-candidate versions

A tag with a suffix (`v2.1.0-beta.1`, `v2.1.0-rc.1`) is published as a
GitHub **pre-release** and its Docker image gets only its exact tag, never
`latest`. The optional in-app update check follows GitHub's "latest release",
which ignores pre-releases, so you will not be offered one automatically.
Take a backup before trying a pre-release.

## Roll back

A newer version may change the database layout, and an older version does not
know how to read the new layout, so rolling back means restoring the old
database as well as the old program.

1. **Stop Jellyball** (stop the Windows service, `docker compose down`, or
   `systemctl stop jellyball`).
2. **Install the older version** (the older installer or image tag; on Docker set
   `JELLYBALL_IMAGE` to the old tag).
3. **Restore the database**, either your own copy of the data folder from
   before the upgrade or the automatic `sports_proxy.db.bak-<version>` (which
   holds the database as it was just before the migration):
   * delete `sports_proxy.db-wal` and `sports_proxy.db-shm` if they exist
     (stale ones next to a restored database can corrupt it);
   * copy the backup over `sports_proxy.db`.
4. **Keep your `.env`.** Restore it from your backup only if you changed it
   after the backup was taken.
5. Start Jellyball and check `/healthz` and the dashboard.

Changes made after the backup (new channels, setting changes) are lost with the
restored database.

## If an upgrade goes wrong

* The service will not start: stop it, run `JellyballConsole.exe --console`
  (Windows) or `docker compose logs` / `journalctl -u jellyball` and read the
  first error; then see [TROUBLESHOOTING.md](TROUBLESHOOTING.md).
* Jellyfin shows no channels: check the playlist opens at
  `http://<host>:<port>/playlist.m3u`, then refresh the tuner and guide in
  Jellyfin ([JELLYFIN.md](JELLYFIN.md)).
* Roll back as above and open an issue with the version numbers and the log
  lines (remove tokens and passwords first).
