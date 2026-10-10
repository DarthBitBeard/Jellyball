Stream providers are now plugins: the 11 built-in scrapers live in
`Jellyball/plugins/builtin/`, one module each with a manifest, loaded by a
versioned SDK (`API_VERSION = 1`), and the Providers card shows each plugin's
version and origin with a "Reload plugins" action for the new
`plugins/third_party/` hot-load directory. Behavior of the built-ins is
unchanged; third-party plugins run in-process and are trusted code.
