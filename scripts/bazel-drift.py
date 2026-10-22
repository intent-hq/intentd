#!/usr/bin/env python3
"""Fail when the Bazel build has drifted from the Cargo workspace.

Cargo is the source of truth. The Bazel side mirrors it in three places, and
each must be refreshed when Cargo.toml, Cargo.lock or rust-toolchain.toml
change:

  * crates/*/BUILD.bazel and bazel/generated.MODULE.bazel, written by
    scripts/bazel-gen.py from `cargo metadata`;
  * MODULE.bazel.lock, which records crate_universe's resolution of Cargo.toml
    and Cargo.lock (Bazel compares the recorded file digests, so a dependency
    bump without a lock refresh fails `--lockfile_mode=error`);
  * that lock must not record host-absolute paths, or it only works in the
    checkout that wrote it.

    python3 scripts/bazel-drift.py         # check; exit 1 and print the fix
    python3 scripts/bazel-drift.py --fix   # regenerate, refresh the lock, recheck

Set BAZEL to choose the Bazel launcher (default: bazelisk, else bazel).
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOCK = ROOT / "MODULE.bazel.lock"
FIX_COMMAND = "python3 scripts/bazel-drift.py --fix"
# Paths that only exist on the machine that wrote the lock.
HOST_PATH = re.compile(r'(?<![\w.-])/(?:home|Users|root|tmp|private|var/folders)/[^"\s\\]*')
BAZEL_FLAGS = ["--curses=no", "--color=no"]


def bazel() -> str:
    explicit = os.environ.get("BAZEL")
    if explicit:
        return explicit
    for name in ("bazelisk", "bazel"):
        found = shutil.which(name)
        if found:
            return found
    raise SystemExit("bazel-drift: neither bazelisk nor bazel is on PATH (or set BAZEL)")


def run(cmd: list[str]) -> int:
    print("+ " + " ".join(cmd), flush=True)
    return subprocess.run(cmd, cwd=ROOT).returncode


def host_paths(text: str, roots: list[str]) -> list[str]:
    """Host-absolute paths in the lock text: the given roots, then common home/tmp prefixes."""
    found = [root for root in roots if root and root != "/" and root in text]
    found += [m.group(0) for m in HOST_PATH.finditer(text)]
    return sorted(set(found))


def check_lock_paths() -> list[str]:
    if not LOCK.exists():
        return [f"{LOCK.name} is missing"]
    hits = host_paths(LOCK.read_text(encoding="utf-8"), [str(ROOT), str(Path.home())])
    return [f"{LOCK.name} records host path {hit}" for hit in hits[:5]]


def check(launcher: str) -> list[str]:
    problems = []
    if run([sys.executable, "scripts/bazel-gen.py", "--check"]) != 0:
        problems.append("generated BUILD or MODULE files are stale (scripts/bazel-gen.py --check)")
    problems += check_lock_paths()
    # `mod deps` evaluates the module extensions, so crate_universe's recorded
    # Cargo.toml/Cargo.lock digests are compared against the working tree.
    if not problems and run([launcher, "mod", "deps", "--lockfile_mode=error", *BAZEL_FLAGS]) != 0:
        problems.append(f"{LOCK.name} is out of date with MODULE.bazel or the Cargo manifests/lockfile")
    return problems


def fix(launcher: str) -> int:
    if run([sys.executable, "scripts/bazel-gen.py"]) != 0:
        return 1
    return run([launcher, "mod", "deps", "--lockfile_mode=update", *BAZEL_FLAGS])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--fix", action="store_true", help="regenerate Bazel files and refresh MODULE.bazel.lock")
    args = parser.parse_args(argv)
    launcher = bazel()
    if args.fix and fix(launcher) != 0:
        print("bazel-drift: refresh failed; see the output above", file=sys.stderr)
        return 1
    problems = check(launcher)
    if problems:
        print("\nbazel-drift: Bazel is out of sync with Cargo:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        print(f"\nFix: run `{FIX_COMMAND}` and commit the result.", file=sys.stderr)
        return 1
    print("bazel-drift: Bazel files match the Cargo workspace.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
