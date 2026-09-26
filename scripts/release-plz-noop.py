#!/usr/bin/env python3
"""Expose a confirmed release-plz no-op as GitHub Actions step outputs.

The release-pr action supplies RELEASE_PLZ_OUTCOME and RELEASE_PLZ_PRS;
GITHUB_SHA identifies its checkout. Redirect stdout to GITHUB_OUTPUT. Any
unconfirmed state emits only no_release_needed=false, leaving the frontend's
existing dependency guard in place. This does not decide what needs a release.
"""

import json
import os
import re
import subprocess
import sys
import tomllib


MANIFEST = "crates/intentd/Cargo.toml"
SHA = re.compile(r"[0-9a-f]{40}")
VERSION = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")


def git(*args):
    return subprocess.check_output(
        ["git", *args], text=True, stderr=subprocess.PIPE, timeout=10,
    ).strip()


def package_version(commit):
    manifest = tomllib.loads(git("show", f"{commit}:{MANIFEST}"))
    version = manifest["package"]["version"]
    # Intentd uses plain vX.Y.Z tags; alpha/beta are publication channels.
    # Unsupported version formats cannot form a trustworthy baseline.
    if not isinstance(version, str) or not VERSION.fullmatch(version):
        raise ValueError("unsupported intentd version")
    return version


def baseline():
    if os.environ.get("RELEASE_PLZ_OUTCOME") != "success":
        return None
    if json.loads(os.environ.get("RELEASE_PLZ_PRS", "")) != []:
        return None

    head = os.environ.get("GITHUB_SHA", "")
    if not SHA.fullmatch(head) or git("rev-parse", "--verify", "HEAD") != head:
        return None

    # Read the assessed commit, not files release-plz may have touched locally.
    # Requiring its version's tag rules out a manual bump or a release merge
    # that is still waiting for its tag, even when release-pr returns [].
    version = package_version(head)
    tag = f"v{version}"
    commit = git("rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}")
    if not SHA.fullmatch(commit):
        return None
    git("merge-base", "--is-ancestor", commit, head)
    if package_version(commit) != version:
        return None
    return tag, commit


def main():
    try:
        release = baseline()
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError):
        # No proof is safer than turning metadata/read errors into a release
        # exemption. Do not print untrusted action output or git diagnostics.
        print("Release-plz no-op baseline could not be confirmed.", file=sys.stderr)
        release = None
    if release is None:
        print("no_release_needed=false")
    else:
        tag, commit = release
        print(f"no_release_needed=true\nbaseline_tag={tag}\nbaseline_sha={commit}")


if __name__ == "__main__":
    main()
