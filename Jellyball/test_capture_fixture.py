"""Tests for tools/capture_fixture.py, the maintainer tool that refreshes fixtures/.

The tool decides what ends up in a public repository, so these tests pin its
promises: no original hostname survives sanitising (in attributes, inline JSON,
text, comments' absence included), scripts and handlers are gone, the page
structure survives, and the output is deterministic. Nothing here touches the
network: --url is exercised with a patched httpx.get.
"""
import contextlib
import copy
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from bs4 import BeautifulSoup

TOOL_PATH = Path(__file__).resolve().parent / "tools" / "capture_fixture.py"
_spec = importlib.util.spec_from_file_location("capture_fixture", TOOL_PATH)
capture_fixture = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(capture_fixture)


def run_cli(*argv):
    """Run the tool's main(); return (exit status, captured stderr)."""
    stderr = io.StringIO()
    with contextlib.redirect_stderr(stderr):
        status = capture_fixture.main([str(part) for part in argv])
    return status, stderr.getvalue()


# --------------------------------------------------------------------------- #
# json subcommand
# --------------------------------------------------------------------------- #
def _schedule_document():
    return {
        "status": "success",
        "events": [
            {
                "id": str(number),
                "date": f"2026-09-{number:02d}T17:00Z",
                "links": [{"href": f"https://www.espn.com/{number}/{n}"} for n in range(5)],
                "competitions": [{"competitors": [{"team": {"logos": [{"href": f"logo-{n}"} for n in range(4)]}}]}],
            }
            for number in range(1, 8)
        ],
        "season": {"year": 2026},
    }


def _tvguide_document():
    return {
        "data": {
            "items": [
                {
                    "channel": {"sourceId": source_id, "name": name},
                    "programSchedules": [{"title": f"{name} {n}", "startTime": n} for n in range(6)],
                }
                for source_id, name in ((111, "ABC"), (222, "ESPN"), (333, "OTHER"), (444, "FS1"))
            ]
        }
    }


class JsonTrimTests(unittest.TestCase):
    def test_keep_cuts_the_named_list_and_every_key_of_each_kept_item_survives(self):
        original = _schedule_document()
        trimmed = capture_fixture.trim_json(copy.deepcopy(original), keep=["events=3"])

        self.assertEqual(len(trimmed["events"]), 3)
        for kept, source in zip(trimmed["events"], original["events"]):
            self.assertEqual(kept, source)  # only the list got shorter; items are untouched
        self.assertEqual(trimmed["season"], original["season"])
        self.assertEqual(set(trimmed), set(original))

    def test_wildcard_paths_reach_nested_lists_and_digits_index_lists(self):
        trimmed = capture_fixture.trim_json(
            copy.deepcopy(_tvguide_document()), keep=["data.items=2", "data.items.*.programSchedules=4"]
        )
        self.assertEqual([len(item["programSchedules"]) for item in trimmed["data"]["items"]], [4, 4])

        directory = {"sports": [{"leagues": [{"teams": [{"team": {"id": str(n)}} for n in range(10)]}]}]}
        trimmed = capture_fixture.trim_json(directory, keep=["sports.0.leagues.0.teams=3"])
        self.assertEqual([entry["team"]["id"] for entry in trimmed["sports"][0]["leagues"][0]["teams"]], ["0", "1", "2"])

    def test_max_list_caps_every_other_list_but_never_a_list_named_by_keep(self):
        trimmed = capture_fixture.trim_json(copy.deepcopy(_schedule_document()), keep=["events=3"], max_list=2)

        self.assertEqual(len(trimmed["events"]), 3)  # named by --keep, so exempt from the cap
        for event in trimmed["events"]:
            self.assertEqual(len(event["links"]), 2)
            self.assertEqual(len(event["competitions"][0]["competitors"][0]["team"]["logos"]), 2)
            self.assertEqual(set(event), {"id", "date", "links", "competitions"})  # no key was dropped

    def test_max_list_never_lengthens_a_short_list(self):
        trimmed = capture_fixture.trim_json({"a": [1], "b": []}, max_list=5)
        self.assertEqual(trimmed, {"a": [1], "b": []})

    def test_select_keeps_only_matching_items_in_their_original_order(self):
        trimmed = capture_fixture.trim_json(
            copy.deepcopy(_tvguide_document()),
            select=["data.items:channel.sourceId=444,222"],
            keep=["data.items.*.programSchedules=4"],
        )
        self.assertEqual([item["channel"]["name"] for item in trimmed["data"]["items"]], ["ESPN", "FS1"])
        self.assertEqual([len(item["programSchedules"]) for item in trimmed["data"]["items"]], [4, 4])

    def test_rules_that_match_nothing_fail_loudly(self):
        cases = [
            dict(keep=["missing=3"]),
            dict(keep=["season=3"]),  # a dict, not a list
            dict(keep=["events"]),  # no count
            dict(keep=["events=x"]),
            dict(select=["events:id=999"]),  # nothing matches
            dict(select=["missing:id=1"]),
            dict(select=["events"]),  # malformed
        ]
        for kwargs in cases:
            with self.subTest(**kwargs), self.assertRaises(capture_fixture.FixtureError):
                capture_fixture.trim_json(copy.deepcopy(_schedule_document()), **kwargs)

    def test_dump_is_pretty_utf8_and_deterministic(self):
        data = {"b": [1, 2], "a": {"name": "CF Montréal"}}
        first = capture_fixture.dump_json(data)
        self.assertEqual(first, capture_fixture.dump_json(copy.deepcopy(data)))
        self.assertTrue(first.endswith("}\n"))
        self.assertIn('\n  "b": [\n    1,', first)
        self.assertIn("Montréal", first)  # not \u-escaped
        self.assertEqual(list(json.loads(first)), ["b", "a"])  # key order preserved, not sorted

    def test_cli_trims_a_file_and_is_repeatable(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "raw.json"
            source.write_text(json.dumps(_schedule_document()), encoding="utf-8")
            out_a, out_b = Path(tmp) / "nested" / "a.json", Path(tmp) / "b.json"
            args = ["json", "--input", source, "--keep", "events=2", "--max-list", "1"]

            self.assertEqual(run_cli(*args, "--out", out_a)[0], 0)
            self.assertEqual(run_cli(*args, "--out", out_b)[0], 0)

            self.assertEqual(out_a.read_bytes(), out_b.read_bytes())
            self.assertNotIn(b"\r", out_a.read_bytes())
            written = json.loads(out_a.read_text(encoding="utf-8"))
            self.assertEqual(len(written["events"]), 2)
            self.assertEqual(len(written["events"][0]["links"]), 1)

    def test_cli_reports_a_bad_rule_with_status_2_and_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "raw.json"
            source.write_text(json.dumps(_schedule_document()), encoding="utf-8")
            out = Path(tmp) / "out.json"

            status, stderr = run_cli("json", "--input", source, "--keep", "nope=1", "--out", out)

            self.assertEqual(status, 2)
            self.assertIn("error:", stderr)
            self.assertFalse(out.exists())

    def test_cli_requires_exactly_one_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out.json"
            self.assertEqual(run_cli("json", "--out", out)[0], 2)
            self.assertEqual(run_cli("json", "--url", "https://x.example.test/a", "--input", "raw.json", "--out", out)[0], 2)
            self.assertEqual(run_cli("json", "--url", "ftp://x.example.test/a", "--out", out)[0], 2)

    def test_cli_url_mode_sends_the_capture_user_agent_and_extra_headers(self):
        seen = {}

        def fake_get(url, headers=None, **kwargs):
            seen.update(url=url, headers=headers)
            return httpx.Response(200, json=_schedule_document(), request=httpx.Request("GET", url))

        with tempfile.TemporaryDirectory() as tmp, patch.object(capture_fixture.httpx, "get", fake_get):
            out = Path(tmp) / "out.json"
            status, _ = run_cli(
                "json", "--url", "https://api.example.test/schedule", "--header", "Referer=https://www.example.test/",
                "--keep", "events=1", "--out", out,
            )

            self.assertEqual(status, 0)
            self.assertEqual(seen["url"], "https://api.example.test/schedule")
            self.assertEqual(seen["headers"]["User-Agent"], "Jellyball-fixture-capture")
            self.assertEqual(seen["headers"]["Referer"], "https://www.example.test/")
            self.assertEqual(len(json.loads(out.read_text(encoding="utf-8"))["events"]), 1)

    def test_cli_url_mode_reports_http_errors(self):
        def failing_get(url, headers=None, **kwargs):
            return httpx.Response(503, request=httpx.Request("GET", url))

        with tempfile.TemporaryDirectory() as tmp, patch.object(capture_fixture.httpx, "get", failing_get):
            out = Path(tmp) / "out.json"
            status, stderr = run_cli("json", "--url", "https://api.example.test/schedule", "--out", out)
            self.assertEqual(status, 2)
            self.assertIn("could not fetch", stderr)
            self.assertFalse(out.exists())


# --------------------------------------------------------------------------- #
# html subcommand
# --------------------------------------------------------------------------- #
# Every host below is an "original" one: none may appear in the output. The
# page is deliberately hostile: hosts hide in attributes, inline JSON (plain and
# with \/ escapes), text, srcset, inline style, user-info, an IP literal, an
# encoded URL in a path, a base64 attribute, and in parts the tool must drop
# entirely (script, style, noscript, comments, handlers).
SAMPLE_PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Real Streams - Gators vs Bulldogs on realstreams.ws</title>
<meta name="csrf-token" content="a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6">
<meta name="author" content="Someone">
<meta name="description" content="Watch at https://www.realstreams.ws/home?src=meta">
<link rel="canonical" href="https://www.realstreams.ws/watch/florida-gators-vs-georgia-bulldogs-live?utm_source=x#top">
<script src="https://www.tracker-one.io/analytics.js"></script>
<script>var cfg = {"api": "https:\/\/api.realstreams.ws\/v1\/events?token=abc123", "host": "cdn.hidden-host.net"};</script>
<style>.hero { background: url(//img.realstreams.ws/bg.png); }</style>
<!-- tracking pixel served from tracker.example-ads.com -->
</head>
<body class="home" onload="init('https://onload.realstreams.ws/')">
<noscript><img src="https://noscript.tracker-one.io/p.gif"></noscript>
<nav id="menu" class="menu main">
  <a href="/" class="home-link">Home</a>
  <a id="ev1" class="event-link big" href="/watch/florida-gators-vs-georgia-bulldogs-live" onclick="track('https://click.realstreams.ws/')">Florida Gators vs Georgia Bulldogs</a>
  <a id="chan" href="watch.php?id=44&amp;session=zzz">ESPN USA</a>
  <a id="hls" href="/hls/9f2c1d3b4a5e6f708192a3b4c5d6e7f8/index.m3u8?md5=abc&amp;expires=1">Stream</a>
  <a id="partner" href="https://partner.otherhost.com:8443/watch/x?ref=realstreams.ws#frag">Partner</a>
  <a id="private" href="https://user:pw@secret.stream-site.com/private/page">Private</a>
  <a id="byip" href="http://203.0.113.9:8080/live/index.m3u8?t=1">By IP</a>
  <a id="encoded" href="/redirect/https%3A%2F%2Fhidden-target.example-two.com%2Fx">Encoded</a>
  <a id="tracked" href="/go?to=https://tracker.example-ads.com/click">Tracked</a>
  <a id="js" href="javascript:doEvil()">Menu</a>
  <a id="mail" href="mailto:admin@realstreams.ws">Mail</a>
  <a id="top" href="#top">Top</a>
</nav>
<div id="cfg" data-config='{"embed":"https:\/\/embed.third-party.tv\/e\/123?auth=k","cdn":"//static.third-party.tv\/lib.js","mirror":"mirror.hidden-host.net"}' data-token="supersecret" data-b64="aHR0cHM6Ly9zZWNyZXQuc3RyZWFtLXNpdGUuY29tL2xpdmUvaW5kZXgubTN1OA==">Players</div>
<iframe id="player" src="https://player.embed-host.cc/embed/abc?x=1" width="640"></iframe>
<form id="search" action="https://form.realstreams.ws/search?q=1"><input type="hidden" name="csrf_token" value="deadbeefdeadbeefdeadbeef"><input name="q"></form>
<p id="seo" class="seo">Watch every game free on realstreams.ws and cdn.another-thing.io - this paragraph is deliberately very long so that it exceeds the one hundred and twenty character limit applied to text nodes.</p>
<p id="short">Short text stays.</p>
<img id="logo" src="/img/logo@2x.png" srcset="//img.realstreams.ws/a.png 1x, https://img2.realstreams.ws/b.png 2x">
<pre id="note">{"json": "in text https:\/\/text-host.example-one.com\/x\/y?z=1", "contact": "press@press-office.net"}</pre>
<ul id="clean"><li>already clean: <a href="https://site-9.example.test/watch/x">x</a></li></ul>
</body>
</html>
"""

ORIGINAL_HOSTS = (
    "realstreams.ws", "www.realstreams.ws", "api.realstreams.ws", "img.realstreams.ws", "img2.realstreams.ws",
    "form.realstreams.ws", "onload.realstreams.ws", "click.realstreams.ws", "cdn.hidden-host.net",
    "mirror.hidden-host.net", "partner.otherhost.com", "embed.third-party.tv", "static.third-party.tv",
    "player.embed-host.cc", "secret.stream-site.com", "text-host.example-one.com", "hidden-target.example-two.com",
    "tracker.example-ads.com", "www.tracker-one.io", "noscript.tracker-one.io", "cdn.another-thing.io",
    "press-office.net", "203.0.113.9",
)
# The distinctive second-level names: nothing may leak these even without the full host.
ORIGINAL_NAMES = (
    "realstreams", "hidden-host", "otherhost", "third-party", "embed-host", "stream-site", "example-one",
    "example-two", "example-ads", "tracker-one", "another-thing", "press-office",
)


def sanitize(page=SAMPLE_PAGE, **kwargs):
    output, _ = capture_fixture.sanitize_html(page, **kwargs)
    return output


def structure(markup):
    """(tag, id, classes) for every element: what 'tags, ids and classes survive' means."""
    soup = BeautifulSoup(markup, "lxml")
    return [(tag.name, tag.get("id"), tuple(tag.get("class") or ())) for tag in soup.find_all(True)]


class HtmlSanitiseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.output = sanitize()
        cls.lowered = cls.output.lower()
        cls.soup = BeautifulSoup(cls.output, "lxml")

    def test_no_original_hostname_survives_anywhere(self):
        for host in ORIGINAL_HOSTS:
            with self.subTest(host=host):
                self.assertNotIn(host, self.lowered)
        for name in ORIGINAL_NAMES:
            with self.subTest(name=name):
                self.assertNotIn(name, self.lowered)

    def test_hosts_inside_attributes_and_inline_json_become_placeholders(self):
        config = json.loads(self.soup.find(id="cfg")["data-config"])
        for value in config.values():
            host = value.replace("\\/", "/").replace("https://", "").replace("//", "").split("/")[0]
            self.assertRegex(host, r"^site-\d+\.example\.test$", value)
        self.assertIn("/e/123", config["embed"])  # the path survives; the host and query do not
        self.assertNotIn("auth=", config["embed"])
        note = json.loads(self.soup.find(id="note").get_text())
        self.assertRegex(note["json"], r"^in text https://site-\d+\.example\.test/x/y$")
        self.assertRegex(note["contact"], r"^user@site-\d+\.example\.test$")
        self.assertRegex(self.soup.find(id="logo")["srcset"], r"^//site-\d+\.example\.test/a\.png 1x, https://site-\d+\.example\.test/b\.png 2x$")

    def test_scripts_styles_comments_and_handlers_are_gone(self):
        self.assertEqual(self.soup.find_all(["script", "style", "noscript"]), [])
        self.assertNotIn("<!--", self.output)
        self.assertNotIn("doEvil()", self.output.replace("javascript:void(0)", ""))
        for tag in self.soup.find_all(True):
            self.assertFalse([name for name in tag.attrs if name.lower().startswith("on")], tag)
        self.assertNotIn("cfg =", self.output)  # script body
        self.assertNotIn(".hero", self.output)  # style body
        self.assertEqual(self.soup.find("body").attrs.get("class"), ["home"])

    def test_iframe_source_is_dropped_by_default_and_kept_but_mapped_on_request(self):
        self.assertFalse(self.soup.find("iframe").has_attr("src"))
        self.assertEqual(self.soup.find("iframe")["id"], "player")

        kept = BeautifulSoup(sanitize(keep_iframe_src=True), "lxml").find("iframe")
        self.assertRegex(kept["src"], r"^https://site-\d+\.example\.test/embed/abc$")

    def test_structure_survives(self):
        original = BeautifulSoup(SAMPLE_PAGE, "lxml")
        for tag in original.find_all(["script", "style", "noscript"]):
            tag.extract()
        expected = [(t.name, t.get("id"), tuple(t.get("class") or ())) for t in original.find_all(True)]
        self.assertEqual(structure(self.output), expected)

        event = self.soup.find(id="ev1")
        self.assertEqual(event["href"], "/watch/florida-gators-vs-georgia-bulldogs-live")  # href paths are kept
        self.assertEqual(event.get_text(strip=True), "Florida Gators vs Georgia Bulldogs")
        self.assertEqual(self.soup.find(id="chan").get_text(strip=True), "ESPN USA")
        self.assertEqual(self.soup.find(id="top")["href"], "#top")
        self.assertEqual(self.soup.find(id="short").get_text(strip=True), "Short text stays.")

    def test_query_strings_fragments_user_info_and_tokens_are_stripped(self):
        for fragment in ("token=abc123", "utm_source", "md5=", "expires=", "?ref=", "#frag", "user:pw", "9f2c1d3b4a5e6f70",
                         "supersecret", "a1b2c3d4e5f6a7b8", "deadbeefdead", "aHR0cHM6", "%2F"):
            with self.subTest(fragment=fragment):
                self.assertNotIn(fragment, self.output)
        self.assertEqual(self.soup.find(id="hls")["href"], "/hls/token/index.m3u8")
        self.assertRegex(self.soup.find(id="partner")["href"], r"^https://site-\d+\.example\.test:8443/watch/x$")
        self.assertRegex(self.soup.find(id="private")["href"], r"^https://site-\d+\.example\.test/private/page$")
        self.assertEqual(self.soup.find(id="encoded")["href"], "/redirect/token")
        self.assertEqual(self.soup.find(id="tracked")["href"], "/go")
        self.assertEqual(self.soup.find(id="js")["href"], "javascript:void(0)")
        self.assertRegex(self.soup.find(id="mail")["href"], r"^mailto:user@site-\d+\.example\.test$")
        self.assertEqual(self.soup.find("meta", attrs={"name": "csrf-token"})["content"], "token")
        self.assertEqual(self.soup.find("meta", attrs={"name": "author"})["content"], "Someone")  # not a secret
        self.assertEqual(self.soup.find("input", attrs={"name": "csrf_token"})["value"], "token")
        self.assertEqual(self.soup.find(id="cfg")["data-token"], "token")
        self.assertEqual(self.soup.find(id="cfg")["data-b64"], "token")

    def test_keep_query_param_keeps_only_the_named_structural_parameter(self):
        soup = BeautifulSoup(sanitize(keep_params=["id"]), "lxml")
        self.assertEqual(soup.find(id="chan")["href"], "watch.php?id=44")
        self.assertEqual(self.soup.find(id="chan")["href"], "watch.php")

    def test_long_text_nodes_are_shortened_and_short_ones_untouched(self):
        seo = self.soup.find(id="seo").get_text(strip=True)
        self.assertEqual(len(seo), capture_fixture.MAX_TEXT_LENGTH)
        self.assertTrue(seo.endswith("..."))
        self.assertTrue(seo.startswith("Watch every game free on "))
        self.assertNotIn("another-thing", seo)  # hosts are replaced before the text is cut

    def test_placeholders_are_numbered_by_first_appearance_and_reused(self):
        output, mapping = capture_fixture.sanitize_html(SAMPLE_PAGE)
        numbers = sorted(int(p.split("-")[1].split(".")[0]) for p in mapping.values())
        self.assertNotIn(9, numbers)  # site-9 is already used by the page itself
        self.assertEqual(numbers, [n for n in range(1, len(numbers) + 2) if n != 9][: len(numbers)])
        self.assertEqual(mapping["realstreams.ws"], "site-1.example.test")  # the <title> comes first
        self.assertEqual(mapping["www.realstreams.ws"], "site-2.example.test")
        # Same host, same placeholder, wherever it appears (title text and mailto link).
        self.assertIn("site-1.example.test", self.soup.find("title").get_text())
        self.assertIn("user@site-1.example.test", self.soup.find(id="mail")["href"])
        self.assertEqual(len(set(mapping.values())), len(mapping))
        self.assertTrue(all(real not in output.lower() for real in mapping))

    def test_output_is_deterministic_and_idempotent(self):
        self.assertEqual(sanitize(), sanitize())
        self.assertEqual(sanitize(), self.output)
        self.assertEqual(sanitize(self.output), self.output)
        compact = sanitize(pretty=False)
        self.assertEqual(sanitize(pretty=False), compact)
        self.assertNotEqual(compact, self.output)

    def test_placeholder_hosts_pass_through_and_keep_host_is_honoured(self):
        self.assertIn("https://site-9.example.test/watch/x", self.output)
        page = '<a href="https://a.espncdn.com/i/teamlogos/nfl/500/buf.png?v=1">logo</a><a href="https://other.ws/x">x</a>'
        soup = BeautifulSoup(sanitize(page, keep_hosts=["a.espncdn.com"]), "lxml")
        hrefs = [a["href"] for a in soup.find_all("a")]
        self.assertEqual(hrefs[0], "https://a.espncdn.com/i/teamlogos/nfl/500/buf.png")  # host kept, query still stripped
        self.assertRegex(hrefs[1], r"^https://site-\d+\.example\.test/x$")

    def test_an_existing_placeholder_number_is_never_reassigned(self):
        page = '<a href="https://site-1.example.test/a">a</a><a href="https://real-one.ws/b">b</a>'
        _, mapping = capture_fixture.sanitize_html(page)
        self.assertEqual(mapping, {"real-one.ws": "site-2.example.test"})

    def test_host_map_keeps_numbering_stable_across_runs(self):
        first_page = '<a href="https://alpha.ws/a">a</a><a href="https://beta.ws/b">b</a>'
        second_page = '<a href="https://gamma.ws/g">g</a><a href="https://beta.ws/b">b</a><a href="https://alpha.ws/a">a</a>'
        _, first_map = capture_fixture.sanitize_html(first_page)
        output, second_map = capture_fixture.sanitize_html(second_page, host_map=first_map)

        self.assertEqual(second_map["alpha.ws"], first_map["alpha.ws"])
        self.assertEqual(second_map["beta.ws"], first_map["beta.ws"])
        self.assertEqual(second_map["gamma.ws"], "site-3.example.test")
        self.assertNotIn("gamma.ws", output)

    def test_find_residual_hosts_flags_host_like_text_but_not_files_or_placeholders(self):
        text = "see foo.shop and index.html, player.js, a.m3u8, site-1.example.test, logo@2x.png, v1.2"
        self.assertEqual(capture_fixture.find_residual_hosts(text), ["foo.shop"])

    def test_cli_writes_the_file_prints_a_summary_and_is_repeatable(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "page.html"
            source.write_text(SAMPLE_PAGE, encoding="utf-8")
            first, second = Path(tmp) / "out" / "a.html", Path(tmp) / "b.html"
            host_map = Path(tmp) / "hosts.json"

            status, stderr = run_cli("html", "--input", source, "--out", first, "--host-map", host_map, "--show-map")
            self.assertEqual(status, 0)
            self.assertIn("host(s) replaced", stderr)
            self.assertIn("realstreams.ws -> site-1.example.test", stderr)
            self.assertEqual(first.read_text(encoding="utf-8"), self.output)
            self.assertEqual(json.loads(host_map.read_text(encoding="utf-8"))["realstreams.ws"], "site-1.example.test")

            self.assertEqual(run_cli("html", "--input", source, "--out", second, "--host-map", host_map)[0], 0)
            self.assertEqual(first.read_bytes(), second.read_bytes())
            self.assertNotIn(b"\r", first.read_bytes())

    def test_cli_warns_about_host_like_text_it_could_not_classify(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, out = Path(tmp) / "page.html", Path(tmp) / "out.html"
            source.write_text("<p>Mirror: sneaky-mirror.shop</p>", encoding="utf-8")
            status, stderr = run_cli("html", "--input", source, "--out", out)
            self.assertEqual(status, 0)
            self.assertIn("warning: host-like text left in output: sneaky-mirror.shop", stderr)

    def test_cli_url_mode_fetches_then_sanitises(self):
        def fake_get(url, headers=None, **kwargs):
            return httpx.Response(200, text='<a href="https://real-site.ws/watch/x?t=1">x</a>', request=httpx.Request("GET", url))

        with tempfile.TemporaryDirectory() as tmp, patch.object(capture_fixture.httpx, "get", fake_get):
            out = Path(tmp) / "out.html"
            self.assertEqual(run_cli("html", "--url", "https://listing.example.test/", "--out", out)[0], 0)
            written = out.read_text(encoding="utf-8")
            self.assertIn("https://site-1.example.test/watch/x", written)
            self.assertNotIn("real-site", written)


if __name__ == "__main__":
    unittest.main()
