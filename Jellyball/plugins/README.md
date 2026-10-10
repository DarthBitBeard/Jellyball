# Provider plugin SDK (3.0.0)

Stream providers are plugins. Each provider is a directory with a
`manifest.json` and a `provider.py` that defines one `plugins.sdk.Provider`
subclass. The engine discovers plugins in `builtin/` (shipped with Jellyball)
and `third_party/` (operator-installed), validates their manifests, loads them
with per-plugin failure isolation, and builds the `ACTIVE_PROVIDERS` list from
the results. A plugin that fails to load never crashes startup: the failure is
logged and shown on the dashboard's Providers card.

## SDK (`sdk.py`)

`API_VERSION = 1`. The stable surface a provider may use:

- `class Provider`: `name`, `base_url` property (honors the dashboard's domain
  overrides), `categories`, and
  `async search(query_or_terms, *, browser=None, http_client=None)` returning
  a list of `StreamDict`.
- `class HtmlAggregatorProvider(Provider)`: the shared logic for aggregators
  that expose linked event pages (`_fetch_html` with Playwright fallback,
  `_is_event_link`, `_parse_anchors`, and the full search pipeline).
- `StreamDict`: the contract for one stream. Fields: `url`, `referer`,
  `origin`, `provider`, `match_score`, `match_title`, `match_url`,
  `discovery_method`, `quality`.
- `fetch_text(client, url)` / `fetch_html(client, url)`: HTTP helpers built on
  `network_safety.validate_http_url_async`, so SSRF protection cannot be
  bypassed.
- `playwright_page(browser)`: async context manager yielding a Playwright page
  (or `None` when no browser is available).
- `get_setting(provider_name, key)`: read provider config (`"enabled"`,
  `"priority"`, `"base_url"`) from the existing `provider_settings` store.

## Manifest format

```json
{
  "name": "MyProvider",
  "version": "1.0.0",
  "api_version": 1,
  "entry": "provider.py:MyProviderScraper",
  "permissions": ["network", "browser"],
  "builtin": false,
  "linear": false
}
```

- `name`: must exactly match the provider class's `name` attribute.
- `version`: the plugin's own version, shown on the dashboard.
- `api_version`: the SDK version the plugin was written against. The loader
  accepts `api_version <= API_VERSION` and rejects anything newer.
- `entry`: `"<module>.py:<ClassName>"`, resolved inside the plugin directory.
- `permissions`: capability list. `"network"` is granted to every plugin;
  `"browser"` controls Playwright access.
- `linear`: marks a linear (24/7 channel) provider; replaces the old
  hard-coded `LINEAR_PROVIDERS` list.
- `builtin`: reserved for shipped providers.

## Permissions (v1)

v1 enforces browser access: a plugin without the `"browser"` permission gets
`browser=None` passed to its `search`, so Playwright-dependent code paths see
no browser. Network access always goes through the SDK helpers, so SSRF
validation applies to every plugin equally. All 11 built-in providers declare
`["network", "browser"]`, matching their historical behavior.

## Versioning policy

The SDK version (`API_VERSION`) bumps only on breaking changes to the stable
surface. Additive changes (new helpers, new optional `StreamDict` fields) do
not bump it. A plugin pins the SDK version it was written against in its
manifest; the loader refuses plugins that require a newer SDK than the engine
provides, and logs a warning when a plugin targets an older one.

## Third-party plugins

Drop a plugin directory into `third_party/` (see its README); it loads on the
next restart or immediately via the dashboard's **Reload plugins** action.

Third-party plugins are **trusted code**: they run in-process with the same
privileges as the built-in scrapers, exactly as all scrapers do today. Only
install plugins from sources you trust, the same way you would a browser
extension. The `third_party/` directory is gitignored so plugins are never
committed accidentally.
