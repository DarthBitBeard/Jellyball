# Running Jellyball on Linux with systemd

This runs Jellyball as a system service on a Debian/Ubuntu-style host without
Docker. If you are fine with containers, [`docs/DOCKER.md`](../../docs/DOCKER.md)
is simpler. Status: the unit file and these steps follow the code (headless
mode, data directory, password generation) but have not been run on a real host
by the maintainer yet; report anything that does not match.

You need Python 3.12 (3.13 is only tested as an early warning), `ffmpeg` (only
for Multi-View), and `git` or a release source archive.

## 1. Create the service user and folders

```bash
sudo useradd --system --home-dir /var/lib/jellyball --shell /usr/sbin/nologin jellyball
sudo mkdir -p /opt/jellyball /etc/jellyball
```

## 2. Get the source

Use the tag of the release you want (the `Jellyball/` folder inside the
repository is the application):

```bash
sudo git clone --branch vX.Y.Z --depth 1 https://github.com/DarthBitBeard/Jellyball.git /opt/jellyball/src
sudo ln -sfn /opt/jellyball/src/Jellyball /opt/jellyball/app
```

## 3. Python environment and Chromium

`PLAYWRIGHT_BROWSERS_PATH=0` installs Chromium inside the virtual environment,
which is where Jellyball looks for it when run from source. `--with-deps`
installs the system libraries Chromium needs and therefore must run as root.

```bash
sudo python3.12 -m venv /opt/jellyball/venv
sudo /opt/jellyball/venv/bin/pip install -r /opt/jellyball/app/requirements.txt
sudo env PLAYWRIGHT_BROWSERS_PATH=0 /opt/jellyball/venv/bin/python -m playwright install --with-deps chromium
sudo apt-get install -y --no-install-recommends ffmpeg   # Multi-View only
```

Playwright publishes Chromium for amd64 and arm64 Linux only.

## 4. Settings

Jellyball reads `KEY=value` lines from `/etc/jellyball/jellyball.env` (through
the unit's `EnvironmentFile=`). Every setting is listed, commented out at its
default, in `Jellyball/.env.example`.

```bash
sudo cp /opt/jellyball/app/.env.example /etc/jellyball/jellyball.env
sudo chmod 640 /etc/jellyball/jellyball.env
sudo chown root:jellyball /etc/jellyball/jellyball.env
sudoedit /etc/jellyball/jellyball.env
```

Things worth knowing:

* On Linux Jellyball listens on all interfaces (`0.0.0.0`) unless you set
  `JELLYBALL_HOST`. A network bind requires a dashboard password. If you do not
  set `DASHBOARD_PASSWORD`, a random one is generated on first start and saved
  to `/var/lib/jellyball/dashboard-password.txt` (it is never written to the
  log; the user name is `admin` unless you set `DASHBOARD_USERNAME`).
* To keep it private to the machine (for example behind a reverse proxy on the
  same host), set `JELLYBALL_HOST=127.0.0.1`.
* `PORT` defaults to 8000.
* Multi-View encoder: `MULTIVIEW_HWACCEL` defaults to `nvenc`. On a host
  without an NVIDIA GPU set `MULTIVIEW_HWACCEL=none` (software x264) or `qsv`
  (Intel). See `docs/DOCKER.md` for GPU notes.

## 5. Install and start the unit

```bash
sudo cp /opt/jellyball/src/deploy/linux/jellyball.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now jellyball
systemctl status jellyball
journalctl -u jellyball -f
curl -fsS http://localhost:8000/healthz
```

Then read the generated password if you did not set one:

```bash
sudo cat /var/lib/jellyball/dashboard-password.txt
```

Open `http://<host>:8000/` and sign in. In Jellyfin use
`http://<host>:8000/playlist.m3u` as the tuner and `http://<host>:8000/epg.xml`
as the guide source (see `docs/JELLYFIN.md`).

## Upgrading and rolling back

```bash
sudo systemctl stop jellyball
sudo cp -a /var/lib/jellyball /var/lib/jellyball.backup-$(date +%F)   # settings + database
sudo git -C /opt/jellyball/src fetch --tags
sudo git -C /opt/jellyball/src checkout vX.Y.Z
sudo /opt/jellyball/venv/bin/pip install -r /opt/jellyball/app/requirements.txt
sudo env PLAYWRIGHT_BROWSERS_PATH=0 /opt/jellyball/venv/bin/python -m playwright install --with-deps chromium
sudo systemctl start jellyball
```

To roll back, stop the service, check out the previous tag, and restore the
data folder copy. More in [`docs/UPGRADING.md`](../../docs/UPGRADING.md).

## Backups and disaster recovery

Everything Jellyball cannot rebuild lives in one directory:
`/var/lib/jellyball` (or `$JELLYBALL_DATA_DIR` if you set one). That folder
holds `sports_proxy.db` (channels, schedules, settings, provider history),
`dashboard-password.txt`, `relay-signing.key`, your `.env`, and
`jellyball.log`. Back up that single directory and you can rebuild from
scratch; lose it and you start over.

Back up while the service is stopped, or use SQLite's online backup against
the running database:

```bash
# Option 1: stopped-service copy (simplest, always consistent)
sudo systemctl stop jellyball
sudo cp -a /var/lib/jellyball "/root/jellyball-backup-$(date +%F)"
sudo systemctl start jellyball

# Option 2: live backup without stopping (safe on a running database)
sudo -u jellyball sqlite3 /var/lib/jellyball/sports_proxy.db \
  ".backup '/root/jellyball-backup-$(date +%F)/sports_proxy.db'"
sudo cp -a /var/lib/jellyball/dashboard-password.txt \
  /var/lib/jellyball/relay-signing.key /var/lib/jellyball/.env \
  "/root/jellyball-backup-$(date +%F)/"
```

Keep a few dated copies somewhere other than the same disk, and re-back-up
after changing channels, providers, or alerting settings. To restore: stop
the service, move the backup back into place with the `jellyball` user
owning the files (`sudo chown -R jellyball:nogroup /var/lib/jellyball`),
and start the service.

## Uninstall

```bash
sudo systemctl disable --now jellyball
sudo rm /etc/systemd/system/jellyball.service
sudo systemctl daemon-reload
# Optional, deletes your settings and database:
sudo rm -rf /opt/jellyball /etc/jellyball /var/lib/jellyball
sudo userdel jellyball
```

## Troubleshooting

* `Port 8000 on 0.0.0.0 is in use; refusing to start on a different port`
  in the journal: something else owns the port; change `PORT` or stop it.
* Service restarts in a loop: `journalctl -u jellyball -e`; the first error is
  usually a missing Python package, missing Chromium libraries (re-run the
  `playwright install --with-deps` line as root) or an unwritable data folder.
* More: [`docs/TROUBLESHOOTING.md`](../../docs/TROUBLESHOOTING.md).
