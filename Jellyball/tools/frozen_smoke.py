"""Start a built Jellyball (or any command that serves it) and check the
pages a packaging mistake would break: templates, static files, playlist, guide.

    python tools/frozen_smoke.py dist/Jellyball/JellyballConsole.exe --console
    python tools/frozen_smoke.py python jellyball_launcher.py --console

It uses a throwaway data directory and a free loopback port. Exit code 0 means
every check passed; the server's output is printed on failure.
"""

import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

STARTUP_SECONDS = 180


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _get(base: str, path: str):
    try:
        with urllib.request.urlopen(base + path, timeout=15) as response:
            return response.status, response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, ""
    except (urllib.error.URLError, OSError):
        return 0, ""


CHECKS = (
    ("/healthz", lambda body: '"app":"jellyball"' in body.replace(" ", "")),
    ("/", lambda body: "Jellyball Sports Manager" in body and "tab-channels" in body),
    ("/static/dashboard.js", lambda body: "function switchTab" in body),
    ("/static/dashboard.css", lambda body: len(body) > 100),
    ("/playlist.m3u", lambda body: body.startswith("#EXTM3U")),
    ("/epg.xml", lambda body: "<tv" in body),
)


def _stop(server: subprocess.Popen) -> None:
    """Stop the server and everything it started (the bundle spawns child processes
    that would otherwise keep the data directory open on Windows)."""
    if os.name == "nt":
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(server.pid)], capture_output=True)
    else:
        server.terminate()
    try:
        server.wait(timeout=30)
    except subprocess.TimeoutExpired:
        server.kill()


def main(argv) -> int:
    if not argv:
        print(__doc__)
        return 2
    port = _free_port()
    base = f"http://127.0.0.1:{port}"
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as data_dir:
        env = dict(os.environ, PORT=str(port), JELLYBALL_DATA_DIR=data_dir, JELLYBALL_HEADLESS="1",
                   DASHBOARD_PASSWORD="", PYTHONUTF8="1")
        log_path = os.path.join(data_dir, "server.out")
        with open(log_path, "wb") as log:
            server = subprocess.Popen(argv, env=env, stdout=log, stderr=subprocess.STDOUT)
            failures = []
            try:
                deadline = time.time() + STARTUP_SECONDS
                while time.time() < deadline and server.poll() is None:
                    if _get(base, "/healthz")[0] == 200:
                        break
                    time.sleep(1)
                for path, ok in CHECKS:
                    status, body = _get(base, path)
                    verdict = status == 200 and ok(body)
                    print(f"{'ok  ' if verdict else 'FAIL'} {path} (HTTP {status})")
                    if not verdict:
                        failures.append(path)
            finally:
                _stop(server)
        if failures:
            print("--- server output ---")
            with open(log_path, "rb") as log:
                print(log.read().decode("utf-8", "replace")[-4000:])
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
