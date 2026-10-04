# Parser fixtures

Stored samples of the third-party data Jellyball parses, used by
`test_fixtures_contract.py` so a refactor cannot silently change what the
parsers extract. Tests read only these files: no network, no real browser.
They are test data and are excluded from the Docker image.

## What is here

| Path | What it is | How it was made |
| --- | --- | --- |
| `espn/nfl_team_schedule_buf.json` | ESPN NFL team schedule for `buf` | Captured from the public API, trimmed |
| `espn/mls_team_schedule_inter_miami.json` | ESPN MLS (`usa.1`) schedule for team 20232 | Captured, trimmed |
| `espn/nfl_teams.json`, `espn/college_football_teams.json` | ESPN team directories (`?limit=1000`) | Captured, trimmed to 3 teams |
| `tvguide/schedule.json` | TVGuide schedule: ESPN and FS1 with 4 programmes each | Captured (`duration=1440`), 2 channels picked |
| `providers/aggregator/*` | Index, category, event, player and streamer pages for the `HtmlAggregatorScraper` subclasses | Hand-written |
| `providers/hls/*` | A live media playlist, a master playlist and a finished (VOD) playlist | Hand-written |
| `providers/iptv_org/sports.m3u` | Playlist in the iptv-org format | Hand-written |
| `providers/thetvapp/*`, `providers/daddylive/*` | Channel directory pages and scripted network logs | Hand-written |
| `providers/common/cloudflare_challenge.html` | The "Just a moment..." interstitial | Hand-written |

The ESPN and TVGuide files were captured once (6 requests in total) and cut with
`tools/capture_fixture.py json`. Every key of every kept item is preserved; only
list lengths were cut (`events`/`teams` to 3, other lists to 2, programmes to 4).
They still contain ESPN's own host names (`espn.com`, `a.espncdn.com`, and an
internal `sports.core.api.espn.pvt` that ESPN emits in `$ref` links).

## Provider fixtures are skeletons, not captures

The provider files were **authored from what the parsers expect**, not copied
from real sites (no provider or aggregator site was fetched). They prove the
parsers stay stable for those shapes; they do **not** prove a real site still
matches. All host names are `*.example.test`. The TheTVApp and DaddyLive network
logs are scripted request lists replayed by fake Playwright pages: they test our
capture and filtering code, not what a real player requests. `playwright_intercept_streams`
and the real player behaviour need a real browser and are not covered.

Refreshing against real pages is a maintainer job. Save a page, then:

```
python tools/capture_fixture.py html --input saved.html --out fixtures/providers/<name>/page.html
```

The tool drops scripts, styles, comments, handlers and iframe sources
(`--keep-iframe-src` keeps them for event pages), maps every external host to a
stable `site-N.example.test`, strips queries, fragments and token-like path
segments (`--keep-query-param id` keeps a structural parameter), shortens text
nodes over 120 characters and keeps tags, ids, classes and href paths.
A sanitiser cannot prove a page is clean: read the output and the stderr
warnings before committing. `--host-map FILE` keeps numbering stable across
runs; that file names real hosts, so never commit it. For the ESPN and TVGuide
files, see the examples in the tool's docstring (`json --keep`, `--select`, `--max-list`).
After a refresh, update the expected values in `test_fixtures_contract.py`.

## Sanitisation rules

- Provider fixtures: only `*.example.test` hosts (enforced by a test).
- Captured API fixtures: only the hosts the API itself returns (enforced by an allow-list test).
- No credentials, tokens or personal data; query strings and tokens stripped from captured pages.
