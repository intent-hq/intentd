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

Release bumps need no Bazel action. release-plz changes only the versions of
workspace crates; when that is the whole difference (in the generated files,
or between the Cargo inputs the lock recorded and today's), the check passes.
Any third-party, feature, path or crate-set change still fails.

    python3 scripts/bazel-drift.py         # check; exit 1 and print the fix
    python3 scripts/bazel-drift.py --fix   # regenerate, refresh the lock, recheck

Set BAZEL to choose the Bazel launcher (default: bazelisk, else bazel). The
release tolerance reads the commit that last changed MODULE.bazel.lock, so CI
needs the git history (fetch-depth: 0).
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOCK = ROOT / "MODULE.bazel.lock"
FIX_COMMAND = "python3 scripts/bazel-drift.py --fix"
# Paths that only exist on the machine that wrote the lock.
HOST_PATH = re.compile(r'(?<![\w.-])/(?:home|Users|root|tmp|private|var/folders)/[^"\s\\]*')
BAZEL_FLAGS = ["--curses=no", "--color=no"]
# A crate's own version line in a generated BUILD file.
VERSION_LINE = re.compile(r'^\s*version = "[^"]*",\n', re.M)
CRATE_EXTENSION = "@@rules_rust+//crate_universe:extensions.bzl%crate"
DEP_TABLES = ("dependencies", "dev-dependencies", "build-dependencies")


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


# --- release-bump tolerance -------------------------------------------------


def _strip_member_versions(deps: dict) -> dict:
    out = {}
    for name, spec in deps.items():
        if isinstance(spec, dict) and "path" in spec:
            spec = {k: v for k, v in spec.items() if k != "version"}
        out[name] = spec
    return out


def normalize_manifest(text: str) -> dict:
    """A Cargo.toml without workspace-crate versions: the crate's own and its path deps'."""
    doc = tomllib.loads(text)
    package = doc.get("package")
    if isinstance(package, dict):
        package.pop("version", None)
    tables = [doc, doc.get("workspace", {})] + list(doc.get("target", {}).values())
    for table in tables:
        for key in DEP_TABLES:
            if isinstance(table.get(key), dict):
                table[key] = _strip_member_versions(table[key])
    return doc


def normalize_cargo_lock(text: str) -> dict:
    """A Cargo.lock without the versions of local (source-less) packages."""
    doc = tomllib.loads(text)
    packages = doc.get("package", [])
    local = {p["name"] for p in packages if "source" not in p}
    for package in packages:
        if package["name"] in local:
            package.pop("version", None)
        package["dependencies"] = [
            dep.split(" ")[0] if dep.split(" ")[0] in local else dep
            for dep in package.get("dependencies", [])
        ]
    return doc


def release_only_change(path: str, old: str, new: str) -> bool:
    """True when `old` and `new` differ only in workspace-crate versions."""
    try:
        if Path(path).name == "Cargo.lock":
            return normalize_cargo_lock(old) == normalize_cargo_lock(new)
        if Path(path).name == "Cargo.toml":
            return normalize_manifest(old) == normalize_manifest(new)
    except (tomllib.TOMLDecodeError, KeyError, AttributeError):
        return False
    return False


def recorded_crate_inputs(lock_text: str) -> dict[str, str]:
    """Workspace files crate_universe recorded in the lock, mapped to their sha256."""
    lock = json.loads(lock_text)
    inputs = {}
    for entry in lock.get("moduleExtensions", {}).get(CRATE_EXTENSION, {}).values():
        for item in entry.get("recordedInputs", []):
            if item.startswith("FILE:@@//"):
                path, digest = item[len("FILE:@@//"):].rsplit(" ", 1)
                inputs[path] = digest
    return inputs


def git_show(rev: str, path: str) -> str | None:
    out = subprocess.run(["git", "show", f"{rev}:{path}"], cwd=ROOT, capture_output=True, text=True)
    return out.stdout if out.returncode == 0 else None


def lock_drift_is_release_only() -> tuple[bool, str]:
    """Whether every Cargo input that changed since the lock was written changed only versions."""
    rev = subprocess.run(["git", "log", "-1", "--format=%H", "--", LOCK.name],
                         cwd=ROOT, capture_output=True, text=True).stdout.strip()
    if not rev:
        return False, "no commit changed MODULE.bazel.lock (shallow clone?)"
    recorded = recorded_crate_inputs(LOCK.read_text(encoding="utf-8"))
    if not recorded:
        return False, "the lock records no crate_universe inputs"
    changed = []
    for path, digest in sorted(recorded.items()):
        current = ROOT / path
        if current.exists() and hashlib.sha256(current.read_bytes()).hexdigest() == digest:
            continue
        old = git_show(rev, path)
        if old is None or hashlib.sha256(old.encode()).hexdigest() != digest:
            return False, f"cannot recover the {path} the lock recorded"
        if not current.exists() or not release_only_change(path, old, current.read_text(encoding="utf-8")):
            return False, f"{path} changed beyond workspace-crate versions"
        changed.append(path)
    if not changed:
        return False, "the recorded Cargo inputs are unchanged, so MODULE.bazel or the lock itself drifted"
    return True, f"only workspace-crate versions changed in {len(changed)} Cargo file(s)"


def load_generator():
    spec = importlib.util.spec_from_file_location("bazel_gen", ROOT / "scripts/bazel-gen.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def generated_drift_is_release_only() -> bool:
    """Whether the stale generated files differ from the generator's output only in version lines."""
    try:
        expected = load_generator().generated_files()
    except (SystemExit, Exception):  # noqa: BLE001 - any generator failure means real drift
        return False
    stale = False
    for path, content in expected.items():
        current = path.read_text(encoding="utf-8") if path.exists() else None
        if current == content:
            continue
        if current is None or VERSION_LINE.sub("", current) != VERSION_LINE.sub("", content):
            return False
        stale = True
    orphans = {p for p in ROOT.glob("crates/*/BUILD.bazel")} - set(expected)
    return stale and not orphans


# --- check / fix --------------------------------------------------------------


def check(launcher: str) -> tuple[list[str], list[str]]:
    """Return (problems, notes)."""
    problems, notes = [], []
    if run([sys.executable, "scripts/bazel-gen.py", "--check"]) != 0:
        if generated_drift_is_release_only():
            notes.append("generated files differ only in workspace-crate versions (release bump)")
        else:
            problems.append("generated BUILD or MODULE files are stale (scripts/bazel-gen.py --check)")
    problems += check_lock_paths()
    # `mod deps` evaluates the module extensions, so crate_universe's recorded
    # Cargo.toml/Cargo.lock digests are compared against the working tree.
    if not problems and run([launcher, "mod", "deps", "--lockfile_mode=error", *BAZEL_FLAGS]) != 0:
        release_only, why = lock_drift_is_release_only()
        if release_only:
            notes.append(f"{LOCK.name} is behind a release bump: {why}")
        else:
            problems.append(f"{LOCK.name} is out of date with MODULE.bazel or the Cargo manifests/lockfile ({why})")
    return problems, notes


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
    problems, notes = check(launcher)
    for note in notes:
        print(f"bazel-drift: tolerated: {note}. No action needed; `{FIX_COMMAND}` refreshes it.")
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
