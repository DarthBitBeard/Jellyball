"""Packaged entry point for Jellyball.

Modes:
  (no args)   Desktop tray app (per-user data in %LOCALAPPDATA%\\Jellyball).
  --console   Headless server in the foreground (debugging; use JellyballConsole.exe
              to see output).
  --service   Run as the "Jellyball" Windows service (started by the Service
              Control Manager; data in %ProgramData%\\Jellyball).

`main` is imported lazily on purpose. Its import-time constants (data dir,
database path, logging) must see the service's environment first, and the
Service Control Manager requires StartServiceCtrlDispatcher within ~30s -
importing FastAPI/Playwright/etc. (plus antivirus scanning a fresh install)
can take a noticeable part of that.
"""
from __future__ import annotations

import os
import shutil
import sys
import threading
from pathlib import Path

SERVICE_NAME = "Jellyball"
SERVICE_DISPLAY_NAME = "Jellyball Sports Proxy"
SERVICE_DESCRIPTION = "Jellyfin Live TV sports proxy: M3U/XMLTV tuner, stream failover and Multi-View."


def service_data_dir() -> Path:
    return Path(os.environ.get("ProgramData", r"C:\ProgramData")) / "Jellyball"


def _prepare_service_environment() -> None:
    data_dir = Path(os.environ.get("JELLYBALL_DATA_DIR") or service_data_dir())
    os.environ["JELLYBALL_DATA_DIR"] = str(data_dir)
    os.environ.setdefault("JELLYBALL_HEADLESS", "1")
    # Chromium temp profiles and other scratch files: keep them in our own
    # writable directory (a virtual service account's default TEMP may not be),
    # and clear leftovers from a previous run that was killed.
    tmp_dir = data_dir / "tmp"
    shutil.rmtree(tmp_dir, ignore_errors=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    os.environ["TEMP"] = os.environ["TMP"] = str(tmp_dir)


def _run_service() -> None:
    import servicemanager
    import win32event
    import win32service
    import win32serviceutil

    class JellyballService(win32serviceutil.ServiceFramework):
        _svc_name_ = SERVICE_NAME
        _svc_display_name_ = SERVICE_DISPLAY_NAME
        _svc_description_ = SERVICE_DESCRIPTION

        def __init__(self, args):
            super().__init__(args)
            self.stop_event = threading.Event()
            self.wait_handle = win32event.CreateEvent(None, 0, 0, None)

        def SvcStop(self):
            # uvicorn gets timeout_graceful_shutdown=10s, then lifespan shutdown
            # stops ffmpeg/sessions; ask the SCM for enough time.
            self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING, waitHint=45000)
            self.stop_event.set()
            win32event.SetEvent(self.wait_handle)

        def SvcDoRun(self):
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_INFORMATION_TYPE,
                servicemanager.PYS_SERVICE_STARTED,
                (self._svc_name_, ""),
            )
            try:
                import main

                exit_code = main.run_headless(stop_event=self.stop_event)
            except BaseException as exc:  # noqa: BLE001 - must reach the SCM
                servicemanager.LogErrorMsg(f"Jellyball failed: {type(exc).__name__}: {exc}")
                exit_code = 1
            if exit_code != 0 and not self.stop_event.is_set():
                # Exit the process abnormally so the SCM's failure actions
                # (restart after delay, configured by the installer) kick in.
                servicemanager.LogErrorMsg(f"Jellyball exited with code {exit_code}; requesting restart")
                os._exit(exit_code)

    _prepare_service_environment()
    servicemanager.Initialize()
    servicemanager.PrepareToHostSingle(JellyballService)
    servicemanager.StartServiceCtrlDispatcher()


def main_entry() -> None:
    args = sys.argv[1:]
    if "--service" in args:
        _run_service()
        return
    if "--console" in args:
        os.environ["JELLYBALL_HEADLESS"] = "1"
        import main

        sys.exit(main.run_headless())
    import main

    if main._wants_headless():
        sys.exit(main.run_headless())
    main.TrayApplication().run()


if __name__ == "__main__":
    main_entry()
