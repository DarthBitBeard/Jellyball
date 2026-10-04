"""Changelog fragments: every change adds its own file under changelog.d/
instead of editing CHANGELOG.md, so parallel work never conflicts on it.

    python tools/changelog.py check               validate the fragments
    python tools/changelog.py preview             print the entries they add
    python tools/changelog.py release 2.1.0       fold them into CHANGELOG.md
    python tools/changelog.py notes 2.1.0         print one version's section

A fragment is changelog.d/<slug>.<type>.md holding the entry as plain Markdown
(no leading "- "), already wrapped; type is one of TYPES. `release` moves the
current [Unreleased] text and every fragment into a new "## [version] - date"
section, empties [Unreleased], and deletes the fragments.
"""

import argparse
import datetime
import re
import sys
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
CHANGELOG = ROOT / "CHANGELOG.md"
FRAGMENTS = ROOT / "changelog.d"

TYPES = ("added", "changed", "deprecated", "removed", "fixed", "security", "internal")
HEADINGS = {kind: kind.capitalize() for kind in TYPES}
_FILENAME = re.compile(r"^[a-z0-9][a-z0-9_-]*\.(" + "|".join(TYPES) + r")\.md$")
_UNRELEASED = "## [Unreleased]"


class Fragment(NamedTuple):
    name: str
    kind: str
    text: str


def load_fragments(directory: Path = FRAGMENTS) -> List[Fragment]:
    """Valid fragments in a stable order (type, then file name); raises ValueError listing every problem."""
    problems: List[str] = []
    fragments: List[Fragment] = []
    for path in sorted(directory.glob("*.md")) if directory.is_dir() else []:
        if path.name.lower() == "readme.md":
            continue
        match = _FILENAME.match(path.name)
        if not match:
            problems.append(f"{path.name}: name must be <slug>.<{'|'.join(TYPES)}>.md")
            continue
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            problems.append(f"{path.name}: empty")
        elif text[0] in "-*#":
            problems.append(f"{path.name}: write the entry text only (no leading bullet or heading)")
        else:
            fragments.append(Fragment(path.name, match.group(1), text))
    if problems:
        raise ValueError("\n".join(problems))
    return sorted(fragments, key=lambda f: (TYPES.index(f.kind), f.name))


def _bullet(text: str) -> str:
    first, *rest = text.splitlines()
    return "\n".join([f"- {first}", *[f"  {line}" if line.strip() else "" for line in rest]])


def render_entries(fragments: List[Fragment]) -> Dict[str, str]:
    """{type: the type's bullets, ready to place under its heading}."""
    grouped: Dict[str, List[str]] = {}
    for fragment in fragments:
        grouped.setdefault(fragment.kind, []).append(_bullet(fragment.text))
    return {kind: "\n".join(bullets) for kind, bullets in grouped.items()}


def _split_unreleased(changelog: str) -> Tuple[str, str, str]:
    """(text up to and including the [Unreleased] heading, its body, text from the next release heading)."""
    start = changelog.find(_UNRELEASED)
    if start < 0:
        raise ValueError("CHANGELOG.md has no '## [Unreleased]' heading")
    body_start = start + len(_UNRELEASED)
    following = re.search(r"(?m)^## \[", changelog[body_start:])
    body_end = body_start + following.start() if following else len(changelog)
    return changelog[:body_start], changelog[body_start:body_end], changelog[body_end:]


def _parse_body(body: str) -> Tuple[str, Dict[str, str]]:
    """(intro text before the first '### ', {heading: text under it}), headings in file order."""
    parts = re.split(r"(?m)^### (.+?)\s*$", body)
    sections = {parts[i].strip(): parts[i + 1].strip("\n") for i in range(1, len(parts), 2)}
    return parts[0].strip("\n"), sections


def _compose(intro: str, sections: Dict[str, str]) -> str:
    known = [HEADINGS[kind] for kind in TYPES if HEADINGS[kind] in sections]
    ordered = known + [heading for heading in sections if heading not in known]
    blocks = ([intro] if intro.strip() else []) + [f"### {heading}\n\n{sections[heading]}" for heading in ordered]
    return "\n\n".join(blocks)


def preview(fragments: List[Fragment]) -> str:
    entries = render_entries(fragments)
    return _compose("", {HEADINGS[kind]: entries[kind] for kind in TYPES if kind in entries})


def release(changelog: str, fragments: List[Fragment], version: str, date: str) -> str:
    """CHANGELOG.md text with [Unreleased] and the fragments moved into a new version section."""
    if re.search(rf"(?m)^## \[{re.escape(version)}\]", changelog):
        raise ValueError(f"CHANGELOG.md already has a section for {version}")
    head, body, tail = _split_unreleased(changelog)
    intro, sections = _parse_body(body)
    for kind, text in render_entries(fragments).items():
        heading = HEADINGS[kind]
        sections[heading] = f"{sections[heading]}\n{text}" if heading in sections else text
    released = _compose(intro, sections)
    if not released.strip():
        raise ValueError("nothing to release: [Unreleased] is empty and there are no fragments")
    return f"{head}\n\n## [{version}] - {date}\n\n{released}\n\n{tail.lstrip(chr(10))}"


def notes(changelog: str, version: str) -> str:
    """The body of one section, for a GitHub release description. `Unreleased` selects the pending one."""
    if version.lower() == "unreleased":
        _, body, _ = _split_unreleased(changelog)
        return body.strip("\n")
    match = re.search(rf"(?m)^## \[{re.escape(version)}\][^\n]*\n", changelog)
    if not match:
        raise ValueError(f"CHANGELOG.md has no section for {version}")
    following = re.search(r"(?m)^## \[", changelog[match.end():])
    end = match.end() + following.start() if following else len(changelog)
    return changelog[match.end():end].strip("\n")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("check")
    commands.add_parser("preview")
    release_cmd = commands.add_parser("release")
    release_cmd.add_argument("version")
    release_cmd.add_argument("--date", default=datetime.date.today().isoformat())
    release_cmd.add_argument("--dry-run", action="store_true")
    notes_cmd = commands.add_parser("notes")
    notes_cmd.add_argument("version")
    args = parser.parse_args(argv)

    try:
        if args.command == "notes":
            print(notes(CHANGELOG.read_text(encoding="utf-8"), args.version))
            return 0
        fragments = load_fragments()
        if args.command == "check":
            print(f"{len(fragments)} changelog fragment(s) OK")
        elif args.command == "preview":
            print(preview(fragments))
        else:
            updated = release(CHANGELOG.read_text(encoding="utf-8"), fragments, args.version, args.date)
            if args.dry_run:
                print(updated)
            else:
                CHANGELOG.write_text(updated, encoding="utf-8", newline="\n")
                for fragment in fragments:
                    (FRAGMENTS / fragment.name).unlink()
                print(f"CHANGELOG.md: released {args.version} ({len(fragments)} fragment(s) folded in)")
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
