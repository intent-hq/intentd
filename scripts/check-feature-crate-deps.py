#!/usr/bin/env python3
"""Fail when a workspace crate depends on a services feature crate the wrong way.

intent-services is being split into feature crates (`intent-svc-*`) that sit
above it, with the thin `intent-app` composition root on top. The layering only
holds if nothing below a feature reaches up into it:

- intent-services (core) and intent-transport never depend on a feature crate
  or on intent-app, through any kind of edge. A dev-dependency from core on a
  feature would also compile two copies of core into core's own tests.
- Every other edge into an `intent-svc-*` crate or intent-app must be listed in
  ALLOWED below: intent-app composes every feature, intentd depends on
  intent-app, and feature-to-feature edges are the ones in the split plan
  (agents -> providers, scm, runtime; transfer -> agents).

Normal, build and dev-dependencies are all checked, for every target platform:
a dev-dependency that runs the wrong way creates the same cycle in tests.

To add a new feature crate, name it `intent-svc-<domain>`; intent-app may
depend on it right away. An edge between two features needs a row in ALLOWED
(and in packages/intentd/AGENTS.md "Where to look" -> new feature crate).

Usage: python3 -I -B scripts/check-feature-crate-deps.py [manifest-path]
  default manifest: this repo's Cargo.toml.
  CARGO_METADATA_FLAGS: extra cargo flags (default --locked).
Exit 0 when every edge is allowed, 1 when one is not (one ::error line per
edge), 2 when the workspace metadata cannot be read. CI (ci.yml check job)
runs it after scripts/test-check-feature-crate-deps.sh.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

FEATURE_PREFIX = "intent-svc-"
APP = "intent-app"
# Crates below every feature: they may not depend on a feature or on intent-app.
BELOW_FEATURES = ("intent-services", "intent-transport")

# Allowed edges into a feature crate or intent-app, as (from, to). A `to` of
# FEATURE_PREFIX + "*" matches every feature crate.
ALLOWED = {
    (APP, FEATURE_PREFIX + "*"),
    ("intentd", APP),
    ("intent-svc-agents", "intent-svc-providers"),
    ("intent-svc-agents", "intent-svc-scm"),
    ("intent-svc-agents", "intent-svc-runtime"),
    ("intent-svc-transfer", "intent-svc-agents"),
}


def is_upper(name: str) -> bool:
    return name == APP or name.startswith(FEATURE_PREFIX)


def allowed(src: str, dst: str) -> bool:
    if src in BELOW_FEATURES:
        return False
    if (src, dst) in ALLOWED:
        return True
    return dst.startswith(FEATURE_PREFIX) and (src, FEATURE_PREFIX + "*") in ALLOWED


def edge_kind(dep: dict) -> str:
    kind = dep.get("kind") or "normal"
    target = dep.get("target")
    return f"{kind} ({target})" if target else kind


def main(argv: list[str]) -> int:
    repo = Path(__file__).resolve().parent.parent
    manifest = Path(argv[1]) if len(argv) > 1 else repo / "Cargo.toml"
    manifest = manifest.resolve()
    if not manifest.parent.is_dir():
        print(f"error: cannot read the workspace metadata: no directory for {manifest}", file=sys.stderr)
        return 2
    flags = shlex.split(os.environ.get("CARGO_METADATA_FLAGS", "--locked"))
    # Run from the manifest's directory: cargo reads .cargo/config.toml from the
    # invocation directory upward, and another workspace's config must not leak
    # into this one.
    try:
        proc = subprocess.run(
            ["cargo", "metadata", *flags, "--format-version", "1", "--no-deps", "--manifest-path", str(manifest)],
            cwd=manifest.parent,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as err:
        print(f"error: cannot run cargo metadata: {err}", file=sys.stderr)
        return 2
    if proc.returncode != 0:
        print(f"error: cannot read the workspace metadata of {manifest}:\n{proc.stderr}", file=sys.stderr)
        return 2
    try:
        packages = json.loads(proc.stdout)["packages"]
    except (ValueError, KeyError) as err:
        print(f"error: unexpected cargo metadata output: {err}", file=sys.stderr)
        return 2

    members = {p["name"] for p in packages}
    violations = []
    for package in sorted(packages, key=lambda p: p["name"]):
        src = package["name"]
        for dep in package["dependencies"]:
            dst = dep["name"]
            if dst not in members or not is_upper(dst) or allowed(src, dst):
                continue
            violations.append((src, dst, edge_kind(dep)))

    if not violations:
        print(f"ok: every dependency on a services feature crate or {APP} is allowed")
        return 0
    for src, dst, kind in sorted(set(violations)):
        if src in BELOW_FEATURES:
            why = f"{src} sits below every feature crate and may not depend on {dst}"
        else:
            why = f"{src} -> {dst} is not an allowed edge"
        print(
            f"::error::{why} ({kind} dependency). Reach the feature through a port in "
            f"intent-services, or add the edge to ALLOWED in scripts/check-feature-crate-deps.py "
            f"if the split plan allows it"
        )
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
