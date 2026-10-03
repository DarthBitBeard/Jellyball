#!/usr/bin/env python3
"""Refresh Jellyball's parser fixtures (maintainer tool; not part of the app).

Two subcommands turn third-party data into small, safe, deterministic files for
Jellyball/fixtures/. Nothing here runs in CI or in production, and nothing is
fetched unless you pass --url.

json  Fetch a URL (--url) or read a file (--input), optionally pick list items
      (--select), keep only the first N items of named lists (--keep), cap every
      other list (--max-list), and write pretty JSON. Every key of every kept
      item is preserved; only list lengths shrink.

          python tools/capture_fixture.py json \\
              --url https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/buf/schedule \\
              --keep events=3 --max-list 2 --out fixtures/espn/nfl_team_schedule_buf.json

      Paths are dotted: "events", "sports.0.leagues.0.teams", "data.items.*.programSchedules".
      "*" means every element of a list (or every value of a dict); digits index a list.
      A rule that matches nothing is an error, so a changed API shape fails loudly
      instead of silently producing an untrimmed fixture.

html  Read a saved page (--input) or fetch one (--url) and write a sanitised copy:

        * script, style, noscript, comments, inline on* handlers, iframe src,
          nonce/integrity attributes are dropped;
        * every external hostname becomes a stable site-N.example.test (N follows
          first appearance in the document, or --host-map for stability across runs);
          this covers absolute and protocol-relative URLs, JSON-escaped URLs
          (https:\\/\\/host\\/x), URLs in attributes and inline JSON, e-mail
          addresses, and bare host names with a common TLD;
        * query strings, fragments, user-info and opaque path tokens are stripped
          (--keep-query-param NAME keeps a structural parameter such as id);
        * text nodes longer than 120 characters are shortened;
        * tags, ids, classes and href paths are kept.

          python tools/capture_fixture.py html --input saved_page.html \\
              --keep-iframe-src --out fixtures/providers/<name>/event.html

A sanitiser cannot prove a page is clean: read the output (and the stderr warnings
about residual host-like text) before committing it. The --host-map file maps real
host names to their placeholders, so it must never be committed.
"""
from __future__ import annotations

import argparse
import base64
import binascii
import json
import re
import sys
import urllib.parse
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import httpx
from bs4 import BeautifulSoup, CData, Comment, Declaration, Doctype, NavigableString, ProcessingInstruction, Tag

USER_AGENT = "Jellyball-fixture-capture"
PLACEHOLDER_SUFFIX = "example.test"
MAX_TEXT_LENGTH = 120


class FixtureError(Exception):
    """A usage or data problem the maintainer has to fix (exit status 2)."""


# --------------------------------------------------------------------------- #
# Fetching
# --------------------------------------------------------------------------- #
def _parse_headers(pairs: Iterable[str]) -> Dict[str, str]:
    headers = {"User-Agent": USER_AGENT, "Accept": "*/*"}
    for pair in pairs:
        name, sep, value = pair.partition("=")
        if not sep or not name.strip():
            raise FixtureError(f"--header expects NAME=VALUE, got {pair!r}")
        headers[name.strip()] = value.strip()
    return headers


def _fetch_text(url: str, headers: Dict[str, str]) -> str:
    if urllib.parse.urlsplit(url).scheme not in {"http", "https"}:
        raise FixtureError(f"--url must be http(s), got {url!r}")
    try:
        response = httpx.get(url, headers=headers, timeout=30.0, follow_redirects=True)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise FixtureError(f"could not fetch {url}: {type(exc).__name__}: {exc}") from exc
    return response.text


def _read_source(args: argparse.Namespace) -> str:
    if bool(args.url) == bool(args.input):
        raise FixtureError("pass exactly one of --url or --input")
    if args.url:
        return _fetch_text(args.url, _parse_headers(args.header))
    try:
        return Path(args.input).read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        raise FixtureError(f"could not read {args.input}: {exc}") from exc


def _write_text(path: str, text: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    # LF endings on every platform so a refresh never shows as a whole-file diff.
    target.write_text(text, encoding="utf-8", newline="\n")


# --------------------------------------------------------------------------- #
# json subcommand
# --------------------------------------------------------------------------- #
def _split_path(path: str) -> List[str]:
    segments = [segment for segment in path.split(".") if segment]
    if not segments:
        raise FixtureError(f"empty path {path!r}")
    return segments


def _child_keys(node: Any, segment: str) -> List[Any]:
    if segment == "*":
        if isinstance(node, list):
            return list(range(len(node)))
        if isinstance(node, dict):
            return list(node)
        return []
    if isinstance(node, dict):
        return [segment] if segment in node else []
    if isinstance(node, list) and segment.isdigit() and int(segment) < len(node):
        return [int(segment)]
    return []


def _resolve(root: Any, segments: Sequence[str]) -> List[Any]:
    nodes = [root]
    for segment in segments:
        nodes = [node[key] for node in nodes for key in _child_keys(node, segment)]
    return nodes


def _lookup(item: Any, segments: Sequence[str]) -> Any:
    nodes = _resolve(item, segments)
    return nodes[0] if nodes else None


def apply_select(data: Any, spec: str) -> None:
    """--select LISTPATH:FIELD=v1,v2 keeps only list items whose FIELD (a dotted
    path inside the item, compared as text) is one of the values."""
    list_path, colon, rest = spec.partition(":")
    field, equals, values = rest.partition("=")
    if not (colon and equals and list_path and field and values):
        raise FixtureError(f"--select expects LISTPATH:FIELD=v1,v2, got {spec!r}")
    wanted = {value.strip() for value in values.split(",") if value.strip()}
    field_segments = _split_path(field)
    lists = [node for node in _resolve(data, _split_path(list_path)) if isinstance(node, list)]
    if not lists:
        raise FixtureError(f"--select {spec!r}: no list at {list_path!r}")
    kept = 0
    for items in lists:
        items[:] = [item for item in items if str(_lookup(item, field_segments)) in wanted]
        kept += len(items)
    if not kept:
        raise FixtureError(f"--select {spec!r}: no item matched")


def apply_keep(data: Any, path: str, count: int) -> List[list]:
    """Truncate every list addressed by `path` to its first `count` items."""
    lists = [node for node in _resolve(data, _split_path(path)) if isinstance(node, list)]
    if not lists:
        raise FixtureError(f"--keep {path}: no list found at that path")
    for items in lists:
        del items[count:]
    return lists


def cap_lists(node: Any, limit: int, pinned: Set[int]) -> None:
    """Shorten every list not named by --keep to `limit` items (never lengthens)."""
    if isinstance(node, list):
        if id(node) not in pinned:
            del node[limit:]
        children: Iterable[Any] = node
    elif isinstance(node, dict):
        children = node.values()
    else:
        return
    for child in children:
        cap_lists(child, limit, pinned)


def trim_json(
    data: Any,
    keep: Sequence[str] = (),
    select: Sequence[str] = (),
    max_list: Optional[int] = None,
) -> Any:
    """Apply --select, then --keep, then --max-list to `data` (in place)."""
    for spec in select:
        apply_select(data, spec)
    pinned: Set[int] = set()
    for rule in keep:
        path, equals, number = rule.rpartition("=")
        if not equals or not number.isdigit():
            raise FixtureError(f"--keep expects PATH=N, got {rule!r}")
        pinned.update(id(items) for items in apply_keep(data, path, int(number)))
    if max_list is not None:
        cap_lists(data, max_list, pinned)
    return data


def dump_json(data: Any) -> str:
    return json.dumps(data, indent=2, ensure_ascii=False) + "\n"


def run_json(args: argparse.Namespace) -> int:
    try:
        data = json.loads(_read_source(args))
    except json.JSONDecodeError as exc:
        raise FixtureError(f"input is not valid JSON: {exc}") from exc
    trim_json(data, keep=args.keep, select=args.select, max_list=args.max_list)
    text = dump_json(data)
    _write_text(args.out, text)
    print(f"wrote {args.out} ({len(text.encode('utf-8'))} bytes)", file=sys.stderr)
    return 0


# --------------------------------------------------------------------------- #
# html subcommand: host mapping
# --------------------------------------------------------------------------- #
_PLACEHOLDER_RE = re.compile(r"^site-(\d+)\." + re.escape(PLACEHOLDER_SUFFIX) + "$")
_EXISTING_PLACEHOLDER_RE = re.compile(r"\bsite-(\d+)\." + re.escape(PLACEHOLDER_SUFFIX) + r"\b", re.IGNORECASE)

# TLDs recognised for bare host names ("Powered by example.ws"). Hosts that appear
# inside a URL are mapped whatever their TLD; this list only widens detection.
_TLDS = (
    "com", "net", "org", "edu", "gov", "info", "biz", "io", "co", "tv", "me", "cc", "ws", "su", "pk", "im",
    "to", "sx", "xyz", "top", "live", "click", "plus", "site", "online", "stream", "sport", "sports", "app",
    "dev", "club", "vip", "fun", "one", "pro", "icu", "cloud", "link", "lol", "gg", "ly", "fm", "ai", "so",
    "la", "gl", "st", "nu", "tk", "ml", "ga", "cf", "gq", "ru", "de", "fr", "es", "it", "uk", "us", "ca",
    "au", "in", "br", "mx", "nl", "pl", "cz", "tr", "ua", "eu", "be", "ch", "se", "no", "dk", "fi", "gr",
)


class HostMapper:
    """Real host name -> stable site-N.example.test placeholder."""

    def __init__(self, keep_hosts: Iterable[str] = (), initial: Optional[Dict[str, str]] = None):
        self.mapping: Dict[str, str] = {k.lower(): v for k, v in (initial or {}).items()}
        self.keep = {host.strip().lower() for host in keep_hosts if host.strip()}
        self._used = {int(m.group(1)) for m in map(_PLACEHOLDER_RE.match, self.mapping.values()) if m}

    def is_placeholder(self, host: str) -> bool:
        return host == PLACEHOLDER_SUFFIX or host.endswith("." + PLACEHOLDER_SUFFIX)

    def reserve(self, text: str) -> None:
        """Never hand out a site-N that the document already uses for something else."""
        self._used.update(int(number) for number in _EXISTING_PLACEHOLDER_RE.findall(text))

    def map(self, host: str) -> str:
        key = host.strip().strip(".").lower()
        if self.is_placeholder(key) or key in self.keep:
            return key
        if key not in self.mapping:
            number = 1
            while number in self._used:
                number += 1
            self._used.add(number)
            self.mapping[key] = f"site-{number}.{PLACEHOLDER_SUFFIX}"
        return self.mapping[key]


# --------------------------------------------------------------------------- #
# html subcommand: string scrubbing
# --------------------------------------------------------------------------- #
# An absolute (scheme://host/...) or protocol-relative (//host/...) URL, in plain or
# JSON-escaped (https:\/\/host\/...) form. The path alternation is unambiguous
# (a backslash is only allowed as part of "\/"), so matching stays linear.
_URL_RE = re.compile(
    r"(?:(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*:)(?P<s1>(?:\\?/){2})|(?<![\w:/\\.-])(?P<s2>(?:\\?/){2}))"
    r"(?:[^\s/\\\"'<>@]+@)?"
    r"(?P<host>(?:[A-Za-z0-9-]+\.)+[A-Za-z0-9-]+|\[[0-9A-Fa-f:.]+\])"
    r"(?::(?P<port>\d{1,5}))?"
    r"(?P<path>(?:\\?/(?:[^\s\"'<>?#)\\]|\\/)*)?)"
    r"(?P<tail>[?#][^\s\"'<>)]*)?"
)
_TLD_ALTERNATION = "|".join(_TLDS)
_EMAIL_RE = re.compile(
    r"[A-Za-z0-9._%+-]+@((?:[A-Za-z0-9-]+\.)+(?:" + _TLD_ALTERNATION + r"))(?![\w-])", re.IGNORECASE
)
_BARE_HOST_RE = re.compile(
    r"(?<![\w@.-])((?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+(?:" + _TLD_ALTERNATION + r"))(?![\w-]|\.\w)",
    re.IGNORECASE,
)
_B64_RE = re.compile(r"(?<![A-Za-z0-9+/=_-])[A-Za-z0-9+/_-]{24,}={0,2}(?![A-Za-z0-9+/=_-])")
_JWT_RE = re.compile(r"^[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}$")
_HEX_RE = re.compile(r"^[0-9a-fA-F]{16,}$")
_OPAQUE_RE = re.compile(r"^[A-Za-z0-9_=+-]{24,}$")
_SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+){2,}$")
_SENSITIVE_NAME_RE = re.compile(r"token|csrf|nonce|secret|passw|auth(?!or)|session|signature|api[-_]?key", re.IGNORECASE)
_URL_ATTRS = frozenset({
    "href", "src", "data-src", "data-href", "data-url", "data-link", "action", "formaction", "poster", "cite",
})
_DROP_ATTRS = frozenset({"nonce", "integrity"})
_REMOVE_TAGS = ("script", "style", "noscript")
_DROP_STRING_TYPES = (Comment, CData, ProcessingInstruction)
_KEEP_STRING_TYPES = (Doctype, Declaration)


def _is_token_like(stem: str) -> bool:
    if _HEX_RE.match(stem):
        return True
    return bool(
        _OPAQUE_RE.match(stem)
        and any(c.isdigit() for c in stem)
        and any(c.isalpha() for c in stem)
        and not _SLUG_RE.match(stem)
    )


def _clean_segment(segment: str) -> str:
    """Replace an opaque path segment (hash, JWT, encoded URL) with "token"."""
    if not segment:
        return segment
    lowered = segment.lower()
    if "%2f" in lowered or "%3a" in lowered or _JWT_RE.match(segment):
        return "token"
    stem, dot, extension = segment.rpartition(".")
    if not dot or not (1 <= len(extension) <= 5 and extension.isalnum()):
        stem, extension = segment, ""
    if _is_token_like(stem):
        return "token" + ("." + extension if extension else "")
    return segment


def _kept_query(query: str, keep_params: Sequence[str]) -> str:
    if not query or not keep_params:
        return ""
    pairs = [
        (name, "token" if _is_token_like(value) else value)
        for name, value in urllib.parse.parse_qsl(query, keep_blank_values=True)
        if name in keep_params
    ]
    return urllib.parse.urlencode(pairs)


class Sanitizer:
    def __init__(self, hosts: HostMapper, keep_params: Sequence[str] = (), keep_iframe_src: bool = False):
        self.hosts = hosts
        self.keep_params = tuple(keep_params)
        self.keep_iframe_src = keep_iframe_src
        self.removed = 0

    # --- string level --------------------------------------------------- #
    def _clean_path(self, path: str) -> str:
        cleaned = "/".join(_clean_segment(segment.split(";", 1)[0]) for segment in path.split("/"))
        return _BARE_HOST_RE.sub(lambda m: self.hosts.map(m.group(1)), cleaned)

    def _replace_url(self, match: "re.Match[str]") -> str:
        raw_path = match.group("path") or ""
        escaped = "\\/" in raw_path
        path = self._clean_path(raw_path.replace("\\/", "/"))
        tail = match.group("tail") or ""
        query = _kept_query(tail[1:].split("#", 1)[0], self.keep_params) if tail.startswith("?") else ""
        if query:
            path += "?" + query
        if escaped:
            path = path.replace("/", "\\/")
        port = f":{match.group('port')}" if match.group("port") else ""
        slashes = match.group("s1") or match.group("s2")
        return f"{match.group('scheme') or ''}{slashes}{self.hosts.map(match.group('host'))}{port}{path}"

    def _replace_base64(self, match: "re.Match[str]") -> str:
        blob = match.group(0)
        try:
            padded = blob.replace("-", "+").replace("_", "/")
            decoded = base64.b64decode(padded + "=" * (-len(padded) % 4), validate=True).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError, ValueError):
            return blob
        looks_like_url = "://" in decoded or bool(_BARE_HOST_RE.search(decoded))
        return "token" if decoded.isprintable() and looks_like_url else blob

    def scrub(self, value: str) -> str:
        value = _EMAIL_RE.sub(lambda m: "user@" + self.hosts.map(m.group(1)), value)
        value = _URL_RE.sub(self._replace_url, value)
        value = _B64_RE.sub(self._replace_base64, value)
        return _BARE_HOST_RE.sub(lambda m: self.hosts.map(m.group(1)), value)

    def clean_reference(self, ref: str) -> str:
        """Clean one attribute value that is a URL reference."""
        lowered = ref.strip().lower()
        if lowered.startswith("javascript:"):
            return "javascript:void(0)"
        if lowered.startswith("data:"):
            return "data:,"
        if not ref.strip() or ref.startswith("#") or lowered.startswith(("mailto:", "tel:")):
            return ref
        parts = urllib.parse.urlsplit(ref)
        if parts.scheme or parts.netloc:
            return ref  # absolute or protocol-relative: already handled by the URL pass
        path = _BARE_HOST_RE.sub(lambda m: self.hosts.map(m.group(1)), "/".join(
            _clean_segment(segment.split(";", 1)[0]) for segment in parts.path.split("/")
        ))
        return urllib.parse.urlunsplit(("", "", path, _kept_query(parts.query, self.keep_params), ""))

    def shorten(self, text: str) -> str:
        """Cut a long text node to MAX_TEXT_LENGTH characters, ignoring (and keeping)
        its surrounding whitespace so that re-running on indented output is a no-op."""
        core = text.strip()
        if len(core) <= MAX_TEXT_LENGTH:
            return text
        lead = text[: len(text) - len(text.lstrip())]
        trail = text[len(text.rstrip()):]
        return lead + core[: MAX_TEXT_LENGTH - 3].rstrip() + "..." + trail

    # --- document level ------------------------------------------------- #
    def _clean_tag(self, tag: Tag) -> None:
        if tag.name == "iframe" and not self.keep_iframe_src:
            for attr in ("src", "data-src", "srcdoc"):
                tag.attrs.pop(attr, None)
        if tag.name in {"input", "meta"}:
            ident = " ".join(str(tag.get(attr, "")) for attr in ("name", "id", "http-equiv"))
            if _SENSITIVE_NAME_RE.search(ident):
                for attr in ("value", "content"):
                    if tag.get(attr):
                        tag[attr] = "token"
        for name in list(tag.attrs):
            value = tag.attrs[name]
            lowered = name.lower()
            if lowered in _DROP_ATTRS or (lowered.startswith("on") and len(lowered) > 2):
                del tag.attrs[name]
            elif not isinstance(value, str):
                continue
            elif _SENSITIVE_NAME_RE.search(lowered) and value:
                tag.attrs[name] = "token"
            else:
                # Relative references lose their query before scrubbing, so a host that only
                # appears in a discarded query never claims a placeholder number.
                if lowered in _URL_ATTRS:
                    value = self.clean_reference(value)
                tag.attrs[name] = self.scrub(value)

    def sanitize(self, html_text: str, pretty: bool = True) -> str:
        self.hosts.reserve(html_text or "")
        soup = BeautifulSoup(html_text or "", "lxml")
        for tag in soup.find_all(list(_REMOVE_TAGS)):
            tag.extract()
            self.removed += 1
        for node in soup.find_all(string=lambda s: isinstance(s, _DROP_STRING_TYPES)):
            node.extract()
            self.removed += 1
        for node in list(soup.descendants):
            if isinstance(node, Tag):
                self._clean_tag(node)
            elif isinstance(node, NavigableString) and not isinstance(node, _KEEP_STRING_TYPES):
                if node.strip():
                    node.replace_with(self.shorten(self.scrub(str(node))))
        rendered = soup.prettify() if pretty else str(soup)
        return rendered.rstrip("\n") + "\n"


def sanitize_html(
    html_text: str,
    *,
    keep_hosts: Iterable[str] = (),
    keep_params: Sequence[str] = (),
    keep_iframe_src: bool = False,
    host_map: Optional[Dict[str, str]] = None,
    pretty: bool = True,
) -> Tuple[str, Dict[str, str]]:
    """Sanitise `html_text`; return (html, real-host -> placeholder mapping)."""
    hosts = HostMapper(keep_hosts, host_map)
    sanitizer = Sanitizer(hosts, keep_params, keep_iframe_src)
    return sanitizer.sanitize(html_text, pretty=pretty), hosts.mapping


_RESIDUAL_RE = re.compile(r"(?<![\w@./-])(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}(?![\w-])")
_FILE_EXTENSIONS = frozenset({
    "html", "htm", "php", "asp", "aspx", "jsp", "js", "css", "json", "xml", "txt", "png", "jpg", "jpeg", "gif",
    "svg", "webp", "ico", "m3u8", "mp4", "ts", "woff", "woff2", "ttf", "test", "webm", "mp3", "aac", "m4s",
})


def find_residual_hosts(text: str) -> List[str]:
    """Host-looking tokens left in sanitised output (a review aid, not a guarantee)."""
    found = []
    for match in _RESIDUAL_RE.finditer(text):
        token = match.group(0)
        if token.rsplit(".", 1)[-1].lower() not in _FILE_EXTENSIONS and token.lower() not in found:
            found.append(token.lower())
    return found


def run_html(args: argparse.Namespace) -> int:
    host_map: Dict[str, str] = {}
    if args.host_map and Path(args.host_map).is_file():
        try:
            host_map = json.loads(Path(args.host_map).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise FixtureError(f"could not read --host-map {args.host_map}: {exc}") from exc
    output, mapping = sanitize_html(
        _read_source(args),
        keep_hosts=args.keep_host,
        keep_params=args.keep_query_param,
        keep_iframe_src=args.keep_iframe_src,
        host_map=host_map,
        pretty=args.pretty,
    )
    _write_text(args.out, output)
    if args.host_map:
        _write_text(args.host_map, json.dumps(mapping, indent=2, sort_keys=True) + "\n")
    print(f"wrote {args.out} ({len(output.encode('utf-8'))} bytes); {len(mapping)} host(s) replaced", file=sys.stderr)
    if args.show_map:
        for real, fake in mapping.items():
            print(f"  {real} -> {fake}", file=sys.stderr)
    for token in find_residual_hosts(output):
        print(f"warning: host-like text left in output: {token}", file=sys.stderr)
    return 0


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="capture_fixture.py", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    def add_source(p: argparse.ArgumentParser) -> None:
        p.add_argument("--url", help="fetch this http(s) URL")
        p.add_argument("--input", help="read this local file instead of fetching")
        p.add_argument("--header", action="append", default=[], metavar="NAME=VALUE", help="extra request header for --url")
        p.add_argument("--out", required=True, help="file to write (parent directories are created)")

    json_parser = sub.add_parser("json", help="trim a JSON document to a small fixture")
    add_source(json_parser)
    json_parser.add_argument("--select", action="append", default=[], metavar="LISTPATH:FIELD=v1,v2",
                             help="keep only list items whose FIELD equals one of the values")
    json_parser.add_argument("--keep", action="append", default=[], metavar="PATH=N",
                             help="keep the first N items of the list at PATH")
    json_parser.add_argument("--max-list", type=int, metavar="N", help="cap every list not named by --keep at N items")
    json_parser.set_defaults(func=run_json)

    html_parser = sub.add_parser("html", help="sanitise an HTML page into a fixture")
    add_source(html_parser)
    html_parser.add_argument("--keep-host", action="append", default=[], metavar="HOST",
                             help="leave this host name alone (e.g. a public logo CDN)")
    html_parser.add_argument("--keep-query-param", action="append", default=[], metavar="NAME",
                             help="keep this structural query parameter (e.g. id) when stripping query strings")
    html_parser.add_argument("--keep-iframe-src", action="store_true",
                             help="keep iframe src/data-src (still host-mapped and stripped); needed for event pages")
    html_parser.add_argument("--host-map", metavar="FILE",
                             help="JSON file of real host -> placeholder, read and updated for stable numbering; never commit it")
    html_parser.add_argument("--show-map", action="store_true", help="print the host mapping to stderr")
    html_parser.add_argument("--pretty", action=argparse.BooleanOptionalAction, default=True,
                             help="re-indent the output (default) or keep the parser's compact form")
    html_parser.set_defaults(func=run_html)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except FixtureError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
