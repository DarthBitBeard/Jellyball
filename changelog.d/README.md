# Changelog fragments

Add one file per user-visible change instead of editing `CHANGELOG.md`, so
parallel work never conflicts on it.

- Name: `<short-slug>.<type>.md`, where type is one of `added`, `changed`,
  `deprecated`, `removed`, `fixed`, `security`, `internal`.
- Content: the entry as plain Markdown, already wrapped. No leading `- ` and no
  heading; the tool adds the bullet.
- `python Jellyball/tools/changelog.py check` validates the files,
  `preview` prints what they add, and `release X.Y.Z` folds them (and the
  current `[Unreleased]` text) into a new version section and deletes them.
