#!/usr/bin/env python3
"""Prepare a temporary release-pr config for new libraries' first shared release.

release-plz 0.3.162 errors when a git-only package is missing from the latest
shared tag (#6163; recurred for intent-svc-transfer in #7760). Each library
confirmed absent from that tag's workspace, present in the current workspace,
and carrying exactly the shared library release policy gets a reserved,
never-created tag lookup so the tool takes its initial-release path. Anything
else missing (intentd itself, a non-member, a different policy) fails closed.
Once the shared baseline contains every package, copy the original bytes.
Never use this configuration for `release`: the publication job stays unchanged.

release-plz also runs `cargo package --workspace` at the latest tag, so a tag
whose manifests cargo rejects blocks every release-pr run until a newer tag
exists (v0.11.321 shipped intentd#3867's unversioned optional dependency).
A tag recorded in REPAIRS, while it is the latest, has its LOCAL ref pointed at
a deterministic child commit: the tagged tree plus the recorded fix commit's
diff. Commits up to the tag stay "too old" for the history walk, release-pr
pushes only its branch, and the record retires once a newer tag exists. A moved
tag, a fix outside HEAD's history or a diff that does not apply fails closed.

Tag selection mirrors that pinned version's release_regex.rs and
command/release_pr/git.rs: anchored v<major>.<minor>.<patch>, valid Rust semver
(u64 components, no leading zeroes), highest version across ALL local tags.
The workflow must fetch full history and tags before running this script.
"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import tomllib

SHARED = "v{{ version }}"
RESERVED = "__initial_"
# The only policy that may take the initial-release path: an untagged,
# unpublished, changelog-less library riding intentd's shared version.
LIBRARY = {"release": True, "git_only": True, "publish": False,
           "git_release_enable": False, "git_tag_enable": False,
           "changelog_update": False, "version_group": "intentd",
           "git_tag_name": SHARED}
# Keys an introduced package's own table may set. git_tag_name is excluded so
# the override never duplicates an explicit key (it inherits SHARED instead).
ALLOWED_KEYS = {"name"} | set(LIBRARY) - {"git_tag_name"}
VERSION = re.compile(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")
REPAIRS = "scripts/release-plz-baseline-repairs.toml"
REPAIR_KEYS = {"tag", "commit", "fix", "reason"}
SHA = re.compile(r"[0-9a-f]{40}")


def run(repo, *args):
    return subprocess.check_output(args, cwd=repo, text=True, stderr=subprocess.PIPE)


def latest_tag(tags):
    candidates = []
    for tag in tags:
        match = VERSION.fullmatch(tag)
        if match:
            version = tuple(map(int, match.groups()))
            if all(part <= 2**64 - 1 for part in version):
                candidates.append((version, tag))
    return max(candidates)[1] if candidates else None


def git(repo, *args, env=None, stdin=None):
    """git with an exit-status result instead of an exception, for expected refusals."""
    return subprocess.run(["git", *args], cwd=repo, text=True, capture_output=True, input=stdin,
                          env={**os.environ, **(env or {})})


def repairs(repo):
    path = repo / REPAIRS
    if not path.exists():
        return {}
    records = {}
    for record in tomllib.loads(path.read_text()).get("repair", []):
        if set(record) != REPAIR_KEYS or not all(SHA.fullmatch(record[k]) for k in ("commit", "fix")) \
                or not VERSION.fullmatch(record["tag"]) or record["tag"] in records:
            raise ValueError(f"malformed baseline repair record in {REPAIRS}: {record}")
        records[record["tag"]] = record
    return records


def repaired_commit(repo, record):
    """The tagged tree plus the fix diff, as a child of the tag commit; same inputs, same SHA."""
    date = run(repo, "git", "show", "-s", "--format=%cI", record["commit"]).strip()
    with tempfile.TemporaryDirectory(prefix="release-plz-repair-") as temp:
        env = {"GIT_INDEX_FILE": str(Path(temp) / "index"),
               "GIT_AUTHOR_NAME": "release-plz baseline repair", "GIT_AUTHOR_DATE": date,
               "GIT_AUTHOR_EMAIL": "release-plz-baseline@invalid",
               "GIT_COMMITTER_NAME": "release-plz baseline repair", "GIT_COMMITTER_DATE": date,
               "GIT_COMMITTER_EMAIL": "release-plz-baseline@invalid"}
        if git(repo, "read-tree", record["commit"], env=env).returncode:
            raise ValueError(f"cannot read {record['tag']} commit {record['commit']}")
        patch = run(repo, "git", "diff", "--binary", f"{record['fix']}^", record["fix"])
        applied = git(repo, "apply", "--cached", env=env, stdin=patch)
        if applied.returncode:
            raise ValueError(f"fix {record['fix']} does not apply to {record['tag']}: {applied.stderr.strip()}")
        tree = git(repo, "write-tree", env=env).stdout.strip()
        message = f"Local release-plz baseline: {record['tag']} + {record['fix']}\n\n{record['reason']}\n"
        made = git(repo, "commit-tree", "--no-gpg-sign", tree, "-p", record["commit"], env=env, stdin=message)
        if made.returncode or not SHA.fullmatch(made.stdout.strip()):
            raise ValueError(f"cannot create the repaired {record['tag']} commit: {made.stderr.strip()}")
        return made.stdout.strip()


def repair_baseline(repo, baseline, commit):
    """Point the local `baseline` tag at its repaired commit when REPAIRS records it."""
    record = repairs(repo).get(baseline)
    if record is None:
        return commit
    # An earlier preparation (the retry path) may already have moved the local tag.
    parent = git(repo, "rev-parse", "--verify", f"{commit}^").stdout.strip()
    if record["commit"] not in (commit, parent):
        raise ValueError(f"{baseline} points at {commit}, not the recorded {record['commit']}")
    if git(repo, "merge-base", "--is-ancestor", record["fix"], "HEAD").returncode:
        raise ValueError(f"repair fix {record['fix']} for {baseline} is not in the history of HEAD")
    repaired = repaired_commit(repo, record)
    if commit not in (record["commit"], repaired):
        raise ValueError(f"{baseline} points at {commit}, not the recorded {record['commit']}")
    run(repo, "git", "update-ref", f"refs/tags/{baseline}", repaired)
    print(f"Repaired baseline {baseline}: {record['commit']} + {record['fix']} -> {repaired} "
          "(local tag only)", file=sys.stderr)
    return repaired


def members(repo):
    metadata = json.loads(run(repo, "cargo", "metadata", "--no-deps", "--format-version", "1"))
    ids = set(metadata["workspace_members"])
    packages = {p["name"] for p in metadata["packages"] if p["id"] in ids}
    if not packages or len(packages) != len(ids):
        raise ValueError("incomplete cargo workspace metadata")
    return packages


def prepare(repo):
    original = (repo / "release-plz.toml").read_bytes()
    config = tomllib.loads(original.decode())
    defaults = config["workspace"]
    packages = {p["name"]: {**defaults, **p} for p in config["package"]}
    if len(packages) != len(config["package"]):
        raise ValueError("duplicate release-plz package tables")
    if packages["intentd"].get("git_tag_name") != SHARED:
        raise ValueError("intentd shared-tag policy changed")
    if run(repo, "git", "rev-parse", "--is-shallow-repository").strip() != "false":
        raise ValueError("shallow history cannot establish the release baseline; fetch full history and tags")
    tags = run(repo, "git", "tag", "--list").splitlines()
    if any(tag.startswith(RESERVED) for tag in tags):
        raise ValueError("reserved initial-release tag collision")
    current = members(repo)
    shared = {name for name, p in packages.items()
              if p.get("git_only") is True and p.get("git_tag_name") == SHARED}
    strays = sorted(shared - current)
    if strays:
        raise ValueError(f"configured packages are not current workspace members: {strays}")
    baseline = latest_tag(tags)
    if baseline is None:
        return original
    commit = run(repo, "git", "rev-parse", "--verify", f"refs/tags/{baseline}^{{commit}}").strip()
    commit = repair_baseline(repo, baseline, commit)
    with tempfile.TemporaryDirectory(prefix="release-plz-baseline-") as temp:
        tree = Path(temp) / "baseline"
        run(repo, "git", "worktree", "add", "--detach", str(tree), commit)
        try:
            released = members(tree)
        finally:
            run(repo, "git", "worktree", "remove", "--force", str(tree))
    introduced = sorted(shared - released)
    if "intentd" in introduced:
        raise ValueError(f"intentd is absent from {baseline}; the shared baseline is unusable")
    # Only libraries matching the shared policy exactly may be introduced: the
    # effective values must equal LIBRARY and the table may set nothing else.
    tables = {p["name"]: p for p in config["package"]}
    unexpected = [name for name in introduced
                  if any(packages[name].get(key) != value for key, value in LIBRARY.items())
                  or not set(tables[name]) <= ALLOWED_KEYS]
    if unexpected:
        raise ValueError(f"unexpected packages absent from {baseline} without the shared "
                         f"library release policy: {unexpected}")
    print(f"Shared baseline {baseline}@{commit}; introduced packages: {introduced}",
          file=sys.stderr)
    if not introduced:
        return original
    # Keep all formatting and every other policy untouched; reparse to prove the
    # only semantic changes landed inside the introduced packages' existing tables.
    changed = original.decode()
    for name in introduced:
        reserved = f"{RESERVED}{name}-v{{{{ version }}}}"
        header = f'name = "{name}"\ngit_tag_name = "{reserved}"'
        changed, count = re.subn(rf'(?m)^name = "{re.escape(name)}"[ \t]*$',
                                 lambda _, header=header: header, changed)
        if count != 1:
            raise ValueError(f"cannot locate exactly one {name} package table")
        for package in config["package"]:
            if package["name"] == name:
                package["git_tag_name"] = reserved
    if tomllib.loads(changed) != config:
        raise ValueError("temporary configuration changed unrelated release policy")
    return changed.encode()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path,
                        help="new config path outside the checkout, e.g. RUNNER_TEMP")
    args = parser.parse_args()
    try:
        repo = Path(run(Path.cwd(), "git", "rev-parse", "--show-toplevel").strip()).resolve()
        output = args.output.resolve()
        if output.is_relative_to(repo):
            raise ValueError("temporary config must be outside the checkout")
        if output.exists():
            raise ValueError("output already exists; refusing to overwrite it")
        contents = prepare(repo)
        with output.open("xb") as target:
            target.write(contents)
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError) as error:
        print(f"Release baseline preparation failed: {error}", file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError) and error.stderr:
            print(error.stderr, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
