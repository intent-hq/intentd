#!/usr/bin/env python3
"""Fail when changed-tests.sh's dependency scanner disagrees with cargo metadata.

`scripts/changed-tests.sh` (`make test-changed`, CI coverage-changed) selects
every crate depending on intent-core when intent-core changes. Selection runs
before a Rust toolchain is installed, so it reads crates/*/Cargo.toml with a
hand-written line scanner (`workspace_edges`) instead of `cargo metadata`. A
manifest style the scanner misses drops crates from the run without a sound,
and the merge queue then fails on a test the PR never ran (intentd#2698). The
first such gap, a `package =` rename with no path, was found only in review
(intentd#2704, fixed in c0ac2a532).

This check compares the scanner's view with cargo's:

1. Every workspace dependency edge. cargo's edges are member -> member
   dependencies whose `path` is the other member's directory, of every kind
   (normal, dev, build) and for every target. Crates are named by their
   crates/<dir> directory, the scanner's vocabulary.
2. For each root the script propagates from (`--print-rdeps`, today only
   intent-core), its transitive reverse dependencies.

Usage: python3 -I -B scripts/check-rdeps-parity.py [manifest-path]
  default manifest: this repo's Cargo.toml. The scanner run is
  <manifest dir>/scripts/changed-tests.sh, which reads that workspace's crates.
  CARGO_METADATA_FLAGS: extra cargo flags (default --locked).
Exit 0 on parity, 1 on any difference (one ::error line per missing or extra
edge, member and reverse dependency), 2 when the metadata or the scanner
output cannot be read. CI (ci.yml check job) runs it after
scripts/test-check-rdeps-parity.sh.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

HINT = "fix workspace_edges in scripts/changed-tests.sh so it reads this manifest style"


def run(argv: list[str], cwd: Path) -> tuple[int, str, str]:
    try:
        proc = subprocess.run(argv, cwd=cwd, capture_output=True, text=True, check=False)
    except OSError as err:
        return 127, "", str(err)
    return proc.returncode, proc.stdout, proc.stderr


def cargo_edges(manifest: Path) -> tuple[dict[tuple[str, str], set[str]], list[str]] | str:
    """Returns ({(dependent, dependency): kinds}, members outside crates/) or an error."""
    flags = shlex.split(os.environ.get("CARGO_METADATA_FLAGS", "--locked"))
    # Run from the manifest's directory: cargo reads .cargo/config.toml from the
    # invocation directory upward, and another workspace's config must not leak
    # into this one.
    code, out, err = run(
        ["cargo", "metadata", *flags, "--format-version", "1", "--no-deps", "--manifest-path", str(manifest)],
        manifest.parent,
    )
    if code != 0:
        return f"cannot read the workspace metadata of {manifest}:\n{err}"
    try:
        packages = json.loads(out)["packages"]
    except (ValueError, KeyError) as err:
        return f"unexpected cargo metadata output: {err}"

    crates_dir = manifest.parent / "crates"
    dirs: dict[Path, str] = {}
    outside = []
    for package in packages:
        crate_dir = Path(package["manifest_path"]).resolve().parent
        if crate_dir.parent == crates_dir:
            dirs[crate_dir] = crate_dir.name
        else:
            outside.append(f"{package['name']} ({crate_dir})")

    edges: dict[tuple[str, str], set[str]] = {}
    for package in packages:
        src = dirs.get(Path(package["manifest_path"]).resolve().parent)
        if src is None:
            continue
        for dep in package["dependencies"]:
            path = dep.get("path")
            dst = dirs.get(Path(path).resolve()) if path else None
            if dst is None or dst == src:
                continue
            kind = dep.get("kind") or "normal"
            if dep.get("target"):
                kind += f" ({dep['target']})"
            edges.setdefault((src, dst), set()).add(kind)
    return edges, outside


def scanner(script: Path, mode: str) -> list[list[str]] | str:
    code, out, err = run(["bash", str(script), f"--print-{mode}"], script.parent.parent)
    if code != 0:
        return f"{script} --print-{mode} exited {code}:\n{err}"
    return [line.split("\t") for line in out.splitlines() if line]


def reverse_dependents(root: str, edges: set[tuple[str, str]]) -> set[str]:
    found: set[str] = set()
    frontier = {root}
    while frontier:
        frontier = {src for src, dst in edges if dst in frontier and src != root and src not in found}
        found |= frontier
    return found


def names(crates: set[str]) -> str:
    return ", ".join(sorted(crates))


def main(argv: list[str]) -> int:
    repo = Path(__file__).resolve().parent.parent
    manifest = (Path(argv[1]) if len(argv) > 1 else repo / "Cargo.toml").resolve()
    if not manifest.parent.is_dir():
        print(f"error: cannot read the workspace metadata: no directory for {manifest}", file=sys.stderr)
        return 2
    script = manifest.parent / "scripts" / "changed-tests.sh"

    truth = cargo_edges(manifest)
    if isinstance(truth, str):
        print(f"error: {truth}", file=sys.stderr)
        return 2
    cargo, outside = truth
    printed_edges = scanner(script, "edges")
    if isinstance(printed_edges, str):
        print(f"error: {printed_edges}", file=sys.stderr)
        return 2
    printed_rdeps = scanner(script, "rdeps")
    if isinstance(printed_rdeps, str):
        print(f"error: {printed_rdeps}", file=sys.stderr)
        return 2
    if any(len(fields) != 2 for fields in printed_edges + printed_rdeps):
        print(f"error: unexpected {script} --print-edges/--print-rdeps output", file=sys.stderr)
        return 2

    errors = []
    for member in outside:
        errors.append(f"workspace member {member} is not under crates/: changed-tests.sh cannot see it")
    scanned = {(src, dst) for src, dst in printed_edges}
    for src, dst in sorted(set(cargo) - scanned):
        kinds = ", ".join(sorted(cargo[(src, dst)]))
        errors.append(f"{src} -> {dst} ({kinds} dependency) is missing from the scanner: {HINT}")
    for src, dst in sorted(scanned - set(cargo)):
        errors.append(f"{src} -> {dst} is reported by the scanner but is not a path dependency in cargo metadata")

    summary = []
    for root, listed in printed_rdeps:
        expected = reverse_dependents(root, set(cargo))
        got = set(listed.split())
        missing, extra = expected - got, got - expected
        if missing or extra:
            parts = []
            if missing:
                parts.append(f"missing {names(missing)} (an {root} change would not run their tests)")
            if extra:
                parts.append(f"extra {names(extra)}")
            errors.append(f"{root} reverse dependencies differ from cargo metadata: {'; '.join(parts)}")
        summary.append(f"{root}: {len(expected)} reverse dependencies")

    if not errors:
        print(f"ok: changed-tests.sh matches cargo metadata ({len(cargo)} workspace dependency edges; {'; '.join(summary)})")
        return 0
    for error in errors:
        print(f"::error::{error}")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
