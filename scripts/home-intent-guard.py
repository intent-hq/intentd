#!/usr/bin/env python3
"""CI backstop: fail when a test run changed the runner's real ``~/intent``.

``intent_core::test_isolation`` redirects default secrets stores away from
the real home and aborts explicit secrets paths or the default workspaces
root inside it. This script catches every *other* writer: run ``snapshot`` before the test step and ``verify``
after it. ``verify`` exits 1 and lists each created, removed or modified
entry.

The snapshot records every entry below ``<home>/intent`` (type, size, mtime;
a symlinked ``~/intent`` is followed), except the contents of checkouts: for
``workspaces/<id>`` and ``worktrees/<id>`` only the ``<id>`` directory's
existence is recorded, because developer checkouts (and their build dirs)
legitimately live there. A new ``<id>`` still shows up as a created entry.
A concurrent legitimate writer (a live Intent install on a self-hosted
runner) is indistinguishable from a test and will be reported. The home is
the user-database home (``pwd``), not ``$HOME``, matching the Rust guard.

Usage: home-intent-guard.py {snapshot,verify} STATE_FILE [--home DIR]
"""

import argparse
import json
import os
import pwd
import sys
from pathlib import Path

CHECKOUT_ROOTS = ("workspaces", "worktrees")


def real_home():
    return Path(pwd.getpwuid(os.getuid()).pw_dir)


def scan(home):
    """Map relative path -> [kind, size, mtime_ns] for ``home/intent``."""
    link = Path(home) / "intent"
    if not link.exists() and not link.is_symlink():
        return {}
    entries = {}
    if link.is_symlink():
        # Record the link itself, then scan what it points at.
        entries["@link"] = ["link", 0, os.readlink(link)]
    root = link.resolve()
    if not root.exists():
        return entries

    def record(path):
        rel = path.relative_to(root).as_posix() if path != root else "."
        st = path.lstat()
        kind = "dir" if path.is_dir() and not path.is_symlink() else (
            "link" if path.is_symlink() else "file")
        parts = path.relative_to(root).parts if path != root else ()
        if kind == "dir" and len(parts) == 2 and parts[0] in CHECKOUT_ROOTS:
            # A checkout's <id> dir: record only that it exists. Writes inside
            # it are allowed (the Rust guard permits <root>/<id>/...).
            entries[rel] = [kind, 0, 0]
            return kind
        size = 0 if kind == "dir" else st.st_size
        entries[rel] = [kind, size, st.st_mtime_ns]
        return kind

    record(root)
    if not root.is_dir():
        return entries
    stack = [root]
    while stack:
        current = stack.pop()
        for child in sorted(current.iterdir()):
            kind = record(child)
            if kind != "dir":
                continue
            rel = child.relative_to(root).parts
            # Record a checkout's <id> dir but never descend into it.
            if len(rel) == 2 and rel[0] in CHECKOUT_ROOTS:
                continue
            stack.append(child)
    return entries


def diff(before, after):
    problems = []
    for rel in sorted(set(before) | set(after)):
        if rel not in before:
            problems.append(f"created  {rel}")
        elif rel not in after:
            problems.append(f"removed  {rel}")
        elif before[rel] != after[rel]:
            problems.append(f"modified {rel}")
    return problems


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("action", choices=("snapshot", "verify"))
    parser.add_argument("state", type=Path)
    parser.add_argument("--home", type=Path, default=None,
                        help="home to check (default: the user-database home)")
    args = parser.parse_args(argv)
    home = args.home or real_home()
    current = scan(home)
    if args.action == "snapshot":
        args.state.write_text(json.dumps(current, sort_keys=True))
        print(f"home-intent-guard: recorded {len(current)} entries under {home}/intent")
        return 0
    before = json.loads(args.state.read_text())
    problems = diff(before, current)
    if problems:
        print(f"home-intent-guard: the test run changed {home}/intent "
              "(tests must never touch the real home; see "
              "intent_core::test_isolation):", file=sys.stderr)
        for line in problems:
            print(f"  {line}", file=sys.stderr)
        return 1
    print(f"home-intent-guard: {home}/intent untouched ({len(current)} entries)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
