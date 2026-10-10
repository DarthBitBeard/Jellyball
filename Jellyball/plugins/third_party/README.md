# Third-party plugins

Drop a provider plugin here as its own directory:

```
third_party/my-plugin/
    manifest.json
    provider.py
```

It is picked up on the next restart, or immediately via the dashboard's
**Reload plugins** action on the Providers card. See `../README.md` for the
manifest format and the SDK.

Third-party plugins are **trusted code**: they run in-process with the same
privileges as the built-in scrapers. Only install plugins from sources you
trust, the same way you would a browser extension.
