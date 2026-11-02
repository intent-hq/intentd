#!/usr/bin/env python3
"""Fail when a scripts/test-* suite is run by nothing.

A suite counts as wired when its file name appears outside a comment in
.github/workflows or .github/actions, the Makefile, or another wired
scripts/test-* file (a harness that runs it). A mention from a suite that is
itself unwired or manual-only does not count, and neither does a whole-line
`#` comment. Otherwise the suite must be on MANUAL_ONLY below, with the
reason it stays out of CI. An unwired suite breaks silently: the review runtime suite went
unrun until intentd#2921, and its scratch-copy file list lost two files that
nobody saw until intentd#2911 fixed it by hand.

A MANUAL_ONLY entry for a suite that no longer exists, or that is now wired,
is also an error, so the list cannot go stale.

Usage: python3 -I -B scripts/check-test-scripts-wired.py [repo-dir]
Exit 0 when every suite is wired or listed, 1 when not (one ::error line per
problem), 2 when the repo layout cannot be read.
"""
from pathlib import Path
import re
import sys

# name -> why it stays out of CI. Keep reasons concrete: what it needs that a
# CI runner does not have, or what already covers it.
MANUAL_ONLY = {
    "test-transfer-selection-contract.sh":
        "entry script for exporting or regenerating the transfer-selection fixtures; "
        "the contract test it wraps (public_import_matches_contract) runs in the nextest suite",
    "test-without-provider-clis.sh":
        "local wrapper that runs another command with provider CLIs hidden, as on a CI runner "
        "(the monorepo Makefile's HERMETIC=1); CI itself has no provider CLIs to hide",
}

REFERENCE_DIRS = (".github/workflows", ".github/actions")
REFERENCE_FILES = ("Makefile",)


def suites(root):
    return sorted(p.name for p in (root / "scripts").glob("test-*") if p.is_file())


def mentions(name):
    return re.compile(r"(?<![\w.-])" + re.escape(name) + r"(?![\w.-])")


def code(text):
    """`text` without whole-line `#` comments, so a note naming a suite is not wiring."""
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def read(path):
    return code(path.read_text(errors="replace"))


def root_texts(root):
    """(label, text) for every file outside scripts/ that can wire a suite."""
    texts = []
    for directory in REFERENCE_DIRS:
        base = root / directory
        if base.is_dir():
            for path in sorted(base.rglob("*")):
                if path.is_file():
                    texts.append((str(path.relative_to(root)), read(path)))
    for name in REFERENCE_FILES:
        path = root / name
        if path.is_file():
            texts.append((name, read(path)))
    return texts


def wiring(root, names, manual_only):
    """name -> the first file that wires it. Suites propagate only once wired
    themselves; a manual-only suite never wires another."""
    suite_texts = {name: read(root / "scripts" / name) for name in names}
    patterns = {name: mentions(name) for name in names}
    wired = {}
    for label, text in root_texts(root):
        for name in names:
            if name not in wired and patterns[name].search(text):
                wired[name] = label
    frontier = [name for name in names if name in wired and name not in manual_only]
    while frontier:
        caller = frontier.pop()
        for name in names:
            if name not in wired and name != caller and patterns[name].search(suite_texts[caller]):
                wired[name] = "scripts/" + caller
                if name not in manual_only:
                    frontier.append(name)
    return wired


def problems(root, manual_only=MANUAL_ONLY):
    names = suites(root)
    wired = wiring(root, names, manual_only)
    found = []
    for name in names:
        if name in wired and name in manual_only:
            found.append((name, f"listed as manual-only but referenced by {wired[name]}; "
                                "remove its MANUAL_ONLY entry"))
        elif name not in wired and name not in manual_only:
            found.append((name, "no workflow, Makefile target or other wired test script runs it; "
                                "wire it into CI or add it to MANUAL_ONLY in "
                                "scripts/check-test-scripts-wired.py with a reason"))
    for name in sorted(set(manual_only) - set(names)):
        found.append((name, "listed as manual-only but scripts/" + name
                            + " does not exist; remove its MANUAL_ONLY entry"))
    return found


def main(argv):
    if len(argv) > 2:
        print(__doc__.split("\n\n")[2], file=sys.stderr)
        return 2
    root = Path(argv[1]) if len(argv) == 2 else Path(__file__).resolve().parents[1]
    if not (root / "scripts").is_dir():
        print(f"error: {root}/scripts is not a directory", file=sys.stderr)
        return 2
    found = problems(root)
    for name, message in found:
        print(f"::error file=scripts/{name}::scripts/{name}: {message}")
    if found:
        return 1
    print(f"ok: {len(suites(root))} scripts/test-* suites are wired or listed as manual-only")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
