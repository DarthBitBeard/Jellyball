"""Provider plugin discovery, loading, and registry.

Scans ``builtin/`` and ``third_party/`` for plugin directories (each holding a
``manifest.json``), validates the manifests, imports the entry classes, and
registers the provider instances the engine consumes.

A plugin that fails to load is isolated: the failure is logged and recorded
for the dashboard, and every other plugin still loads. Startup never crashes
because of one bad plugin.
"""

import importlib
import importlib.util
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from config import LOGGER


PLUGIN_DIR = Path(__file__).resolve().parent
BUILTIN_DIR = PLUGIN_DIR / "builtin"
THIRD_PARTY_DIR = PLUGIN_DIR / "third_party"

# Deterministic load order for built-ins, matching the historical
# ACTIVE_PROVIDERS order so rankings and dashboard positions don't shift.
BUILTIN_LOAD_ORDER = [
    "thetvapp",
    "daddylive",
    "isportsurge",
    "mybuffstreams",
    "methstreams",
    "streameast",
    "footybite",
    "onestream",
    "streamedsu",
    "topstreams",
    "iptvorg",
]

_ENTRY_RE = re.compile(r"^([A-Za-z_][\w]*\.py):([A-Za-z_][\w]*)$")


@dataclass
class PluginRecord:
    """One successfully loaded plugin."""

    name: str
    version: str
    api_version: int
    builtin: bool
    linear: bool
    permissions: List[str]
    author: str
    description: str
    origin: str
    instance: Any
    cls: Any
    class_name: str
    module_name: str = ""


_PLUGIN_RECORDS: List[PluginRecord] = []
_PLUGIN_ERRORS: List[Dict[str, str]] = []
_RELOAD_COUNTER = 0


def get_plugin_records() -> List[PluginRecord]:
    return list(_PLUGIN_RECORDS)


def get_plugin_errors() -> List[Dict[str, str]]:
    return list(_PLUGIN_ERRORS)


def permitted_browser(provider: Any, browser: Any) -> Any:
    """Enforce the plugin's declared permissions.

    A plugin without the ``"browser"`` permission never sees the shared
    Playwright browser (it gets None instead). Providers that predate the
    permission system are unaffected.
    """
    perms = getattr(provider, "plugin_permissions", None)
    if perms is None:
        return browser
    return browser if "browser" in perms else None


def _record_error(slug: str, origin: str, error: str) -> None:
    _PLUGIN_ERRORS.append({"plugin": slug, "origin": origin, "error": error})
    LOGGER.warning("Plugin failed to load plugin=%s origin=%s error=%s", slug, origin, error)


def _validate_manifest(slug: str, origin: str, data: Any) -> Optional[Dict[str, Any]]:
    """Validate a manifest dict. Returns the normalized manifest, or records an
    error and returns None."""
    if not isinstance(data, dict):
        _record_error(slug, origin, "manifest.json is not a JSON object")
        return None
    name = data.get("name")
    version = data.get("version")
    api_version = data.get("api_version")
    entry = data.get("entry")
    if not isinstance(name, str) or not name.strip():
        _record_error(slug, origin, "manifest missing required 'name'")
        return None
    if not isinstance(version, str) or not version.strip():
        _record_error(slug, origin, "manifest missing required 'version'")
        return None
    if not isinstance(api_version, int) or api_version < 1:
        _record_error(slug, origin, "manifest 'api_version' must be a positive integer")
        return None
    from plugins import sdk

    if api_version > sdk.API_VERSION:
        _record_error(
            slug,
            origin,
            f"manifest api_version {api_version} is newer than supported API_VERSION {sdk.API_VERSION}",
        )
        return None
    if not isinstance(entry, str) or not _ENTRY_RE.match(entry):
        _record_error(slug, origin, "manifest 'entry' must look like 'provider.py:ClassName'")
        return None
    permissions = data.get("permissions", ["network"])
    if not isinstance(permissions, list) or not all(isinstance(p, str) for p in permissions):
        _record_error(slug, origin, "manifest 'permissions' must be a list of strings")
        return None
    return {
        "name": name.strip(),
        "version": version.strip(),
        "api_version": api_version,
        "entry": entry,
        "builtin": bool(data.get("builtin", False)),
        "linear": bool(data.get("linear", False)),
        "permissions": list(permissions),
        "author": str(data.get("author") or ""),
        "description": str(data.get("description") or ""),
    }


def _import_plugin_module(slug: str, origin: str, plugin_dir: Path, module_file: str):
    """Import the plugin's entry module. Built-ins are real subpackages;
    third-party plugins load from file so the directory needs no packaging."""
    if origin == "builtin":
        dotted = f"plugins.builtin.{slug}.{Path(module_file).stem}"
        return importlib.import_module(dotted)
    global _RELOAD_COUNTER
    _RELOAD_COUNTER += 1
    synthetic = f"_jellyball_plugin_{slug}_{_RELOAD_COUNTER}"
    spec = importlib.util.spec_from_file_location(synthetic, plugin_dir / module_file)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot create module spec for {module_file}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_one(slug: str, origin: str, plugin_dir: Path) -> Optional[PluginRecord]:
    from plugins import sdk

    manifest_path = plugin_dir / "manifest.json"
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        _record_error(slug, origin, f"cannot read manifest.json: {exc}")
        return None
    manifest = _validate_manifest(slug, origin, data)
    if manifest is None:
        return None
    module_file, class_name = _ENTRY_RE.match(manifest["entry"]).groups()  # type: ignore[union-attr]
    if not (plugin_dir / module_file).is_file():
        _record_error(slug, origin, f"entry module '{module_file}' not found")
        return None
    try:
        module = _import_plugin_module(slug, origin, plugin_dir, module_file)
    except Exception as exc:
        _record_error(slug, origin, f"entry import failed: {type(exc).__name__}: {exc}")
        return None
    cls = getattr(module, class_name, None)
    if not isinstance(cls, type) or not issubclass(cls, sdk.Provider):
        _record_error(slug, origin, f"entry '{class_name}' is not a plugins.sdk.Provider subclass")
        return None
    try:
        instance = cls()
    except Exception as exc:
        _record_error(slug, origin, f"provider instantiation failed: {type(exc).__name__}: {exc}")
        return None
    if instance.name != manifest["name"]:
        _record_error(
            slug,
            origin,
            f"manifest name '{manifest['name']}' does not match provider name '{instance.name}'",
        )
        return None
    instance.plugin_permissions = list(manifest["permissions"])
    instance.plugin_origin = origin
    return PluginRecord(
        name=manifest["name"],
        version=manifest["version"],
        api_version=manifest["api_version"],
        builtin=manifest["builtin"],
        linear=manifest["linear"],
        permissions=list(manifest["permissions"]),
        author=manifest["author"],
        description=manifest["description"],
        origin=origin,
        instance=instance,
        cls=cls,
        class_name=class_name,
        module_name=getattr(module, "__name__", ""),
    )


def _iter_plugin_dirs() -> List[tuple]:
    """(slug, origin, dir) in load order: builtins first (fixed order), then
    third-party plugins sorted by name."""
    ordered: List[tuple] = []
    if BUILTIN_DIR.is_dir():
        by_slug = {p.name: p for p in BUILTIN_DIR.iterdir() if p.is_dir() and (p / "manifest.json").is_file()}
        for slug in BUILTIN_LOAD_ORDER:
            if slug in by_slug:
                ordered.append((slug, "builtin", by_slug.pop(slug)))
        for slug in sorted(by_slug):
            LOGGER.warning("Builtin plugin directory '%s' is not in BUILTIN_LOAD_ORDER; loading last", slug)
            ordered.append((slug, "builtin", by_slug[slug]))
    if THIRD_PARTY_DIR.is_dir():
        for path in sorted(THIRD_PARTY_DIR.iterdir(), key=lambda p: p.name):
            if path.is_dir() and (path / "manifest.json").is_file():
                ordered.append((path.name, "third_party", path))
    return ordered


def load_plugins() -> List[PluginRecord]:
    """Discover, validate, and instantiate every plugin. Safe to call more than
    once; each call re-scans and rebuilds the registry (used by Reload)."""
    from plugins import sdk  # noqa: F401  (validates the SDK imports cleanly)

    _PLUGIN_RECORDS.clear()
    _PLUGIN_ERRORS.clear()
    seen_names: set = set()
    for slug, origin, plugin_dir in _iter_plugin_dirs():
        record = _load_one(slug, origin, plugin_dir)
        if record is None:
            continue
        if record.name in seen_names:
            _record_error(slug, origin, f"duplicate provider name '{record.name}'")
            continue
        seen_names.add(record.name)
        _PLUGIN_RECORDS.append(record)
        LOGGER.info(
            "Loaded plugin provider=%s version=%s origin=%s",
            record.name,
            record.version,
            record.origin,
        )
    return list(_PLUGIN_RECORDS)


def reload_plugins() -> List[PluginRecord]:
    """Re-scan and re-load every plugin (dashboard action). Built-in modules
    are reloaded in place so code changes take effect; third-party plugins
    always load fresh."""
    for record in _PLUGIN_RECORDS:
        if record.origin == "builtin" and record.module_name:
            module = sys.modules.get(record.module_name)
            if module is not None:
                try:
                    importlib.reload(module)
                except Exception as exc:
                    LOGGER.warning("Builtin plugin reload failed plugin=%s error=%s", record.name, exc)
    return load_plugins()
