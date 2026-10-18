#!/usr/bin/env python3
"""Fail when a release-plz version_group can drift out of lockstep.

release-plz.toml puts intentd and its library crates in `version_group =
"intentd"` so they release together. Library code reads its own
`env!("CARGO_PKG_VERSION")` (MCP/ACP client info, user agents, transfer
manifests), so a release that bumps only some members ships a daemon that
misreports its version. Release intent-hq/intentd#2461 bumped intentd alone
(0.11.4 -> 0.11.5) because sitter, a dependency outside the group, was released:
release-plz 0.3.162's dependent-package pass ignores version_group.

Checks, per group:
  lockstep   every member has the same package version, and every path
             dependency on a member (any crate, any [target.*] table) pins that
             version exactly.
  precision  a member's path dependency on a release-managed crate outside the
             group has no patch component (e.g. "0.1"), so that crate's patch
             releases never trigger the group-blind dependent bump.

Run:  python3 -S scripts/test-version-group.py            # unit tests + this tree
      python3 -S scripts/test-version-group.py --rev REV  # check a commit
      python3 -S scripts/test-version-group.py --rev REV --only lockstep
"""

import argparse
import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib
import unittest


ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("release_pr_fast_path", ROOT / "scripts/release-pr-fast-path.py")
_fast_path = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_fast_path)
dependency_tables = _fast_path.dependency_tables
CHECKS = ("lockstep", "precision")


class Tree:
    """Read files from the working tree or, with `rev`, from a git commit."""

    def __init__(self, root, rev=None):
        self.root, self.rev = Path(root), rev

    def read(self, path):
        if self.rev is None:
            return (self.root / path).read_text()
        return subprocess.check_output(["git", "show", f"{self.rev}:{path}"], cwd=self.root, text=True)

    def manifest_paths(self):
        if self.rev is None:
            return sorted(p.relative_to(self.root).as_posix() for p in self.root.glob("crates/*/Cargo.toml"))
        listing = subprocess.check_output(
            ["git", "ls-tree", "-r", "--name-only", self.rev, "--", "crates"], cwd=self.root, text=True)
        return sorted(p for p in listing.splitlines() if p.count("/") == 2 and p.endswith("/Cargo.toml"))


def path_dependencies(doc):
    """(table label, package name, requirement) for each versioned path dependency."""
    keys = ("dependencies", "dev-dependencies", "build-dependencies")
    labels = [*(f"[{k}]" for k in keys),
              *(f"[target.{t}.{k}]" for t in doc.get("target", {}) for k in keys)]
    # dependency_tables yields the same order: top level, then each target.
    for label, table in zip(labels, dependency_tables(doc), strict=True):
        for name, spec in table.items():
            if isinstance(spec, dict) and "path" in spec and "version" in spec:
                yield label, spec.get("package", name), spec["version"]


def version_group_errors(tree, only=CHECKS):
    config = tomllib.loads(tree.read("release-plz.toml"))
    defaults = config.get("workspace", {})
    policies = {p["name"]: {**defaults, **p} for p in config.get("package", [])}
    groups = {}
    for name, policy in policies.items():
        if policy.get("version_group"):
            groups.setdefault(policy["version_group"], []).append(name)
    crates = {}
    for path in tree.manifest_paths():
        doc = tomllib.loads(tree.read(path))
        if doc.get("package", {}).get("name"):
            crates[doc["package"]["name"]] = (path, doc)
    errors = []
    for group, members in sorted(groups.items()):
        errors.extend(f"{group}: {m} is in release-plz.toml but has no crates/*/Cargo.toml"
                      for m in sorted(members) if m not in crates)
        versions = {m: crates[m][1]["package"].get("version") for m in members if m in crates}
        if "lockstep" in only and len(set(versions.values())) > 1:
            listing = ", ".join(f"{m}={v}" for m, v in sorted(versions.items()))
            errors.append(f"{group}: members disagree on version ({listing})")
        for name, (path, doc) in sorted(crates.items()):
            for table, dep, requirement in path_dependencies(doc):
                if "lockstep" in only and dep in versions and len(set(versions.values())) == 1 \
                        and requirement != versions[dep]:
                    errors.append(f"{group}: {path} pins {dep} {requirement} in {table}, "
                                  f"expected {versions[dep]}")
                managed = policies.get(dep, {}).get("release", False)
                if "precision" in only and name in versions and dep not in versions and managed \
                        and len(requirement.lstrip("^=~ ").split(".")) > 2:
                    errors.append(f"{group}: {path} requires {dep} \"{requirement}\" in {table}; use "
                                  f"major.minor (\"{'.'.join(requirement.lstrip('^=~ ').split('.')[:2])}\") "
                                  "so its patch releases do not bump this member outside the group")
    return errors


class VersionGroupTests(unittest.TestCase):
    def test_repository_version_groups_are_in_lockstep(self):
        self.assertEqual(version_group_errors(Tree(ROOT)), [])

    def fixture(self, versions, pin="1.0.0", sitter_req="0.1", extra=""):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        (root / "release-plz.toml").write_text(
            '[workspace]\nrelease = false\n\n'
            + "".join(f'[[package]]\nname = "{n}"\nrelease = true\nversion_group = "daemon"\n\n' for n in versions)
            + '[[package]]\nname = "sitter"\nrelease = true\n')
        for name, version in {**versions, "sitter": "0.1.5", "voice": "0.4.1"}.items():
            crate = root / "crates" / name
            crate.mkdir(parents=True)
            deps = ""
            if name == "daemon":
                deps = (f'[dependencies]\ncore = {{ path = "../core", version = "{pin}" }}\n'
                        f'sitter = {{ path = "../sitter", version = "{sitter_req}" }}\n'
                        'voice = { path = "../voice", version = "0.4.1" }\n'
                        'test-support = { path = "../test-support" }\n' + extra)
            (crate / "Cargo.toml").write_text(f'[package]\nname = "{name}"\nversion = "{version}"\n{deps}')
        return Tree(root)

    def test_lockstep_group_passes_and_unmanaged_crates_are_ignored(self):
        self.assertEqual(version_group_errors(self.fixture({"daemon": "1.0.0", "core": "1.0.0"})), [])

    def test_drifted_member_is_reported(self):
        errors = version_group_errors(self.fixture({"daemon": "1.0.1", "core": "1.0.0"}))
        self.assertEqual(errors, ["daemon: members disagree on version (core=1.0.0, daemon=1.0.1)"])

    def test_stale_internal_pin_is_reported(self):
        errors = version_group_errors(self.fixture({"daemon": "1.0.1", "core": "1.0.1"}, pin="1.0.0"))
        self.assertEqual(errors, ["daemon: crates/daemon/Cargo.toml pins core 1.0.0 in [dependencies], "
                                  "expected 1.0.1"])

    def test_stale_target_table_pin_is_reported(self):
        extra = '[target.\'cfg(windows)\'.dependencies]\ncore2 = { package = "core", path = "../core", version = "1.0.0" }\n'
        errors = version_group_errors(self.fixture({"daemon": "1.0.1", "core": "1.0.1"}, pin="1.0.1", extra=extra))
        self.assertEqual(errors, ["daemon: crates/daemon/Cargo.toml pins core 1.0.0 in "
                                  "[target.cfg(windows).dependencies], expected 1.0.1"])

    def test_exact_pin_on_managed_crate_outside_group_is_reported(self):
        errors = version_group_errors(self.fixture({"daemon": "1.0.0", "core": "1.0.0"}, sitter_req="0.1.5"))
        self.assertEqual(len(errors), 1, errors)
        self.assertIn('requires sitter "0.1.5" in [dependencies]; use major.minor ("0.1")', errors[0])
        tree = self.fixture({"daemon": "1.0.0", "core": "1.0.0"}, sitter_req="0.1.5")
        self.assertEqual(version_group_errors(tree, only=("lockstep",)), [])

    def test_group_member_without_a_crate_is_reported(self):
        tree = self.fixture({"daemon": "1.0.0", "core": "1.0.0"})
        config = tree.root / "release-plz.toml"
        config.write_text(config.read_text() + '\n[[package]]\nname = "gone"\nversion_group = "daemon"\n')
        self.assertIn("daemon: gone is in release-plz.toml but has no crates/*/Cargo.toml",
                      version_group_errors(tree))

    def test_rev_mode_reads_the_commit_not_the_working_tree(self):
        tree = self.fixture({"daemon": "1.0.1", "core": "1.0.0"}, pin="1.0.1")
        env = {"GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1", "GIT_AUTHOR_NAME": "t",
               "GIT_AUTHOR_EMAIL": "t@example.com", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.com"}
        for args in (["init", "-q"], ["add", "."], ["commit", "-qm", "drift"]):
            subprocess.run(["git", *args], cwd=tree.root, env=env, check=True)
        manifest = tree.root / "crates/core/Cargo.toml"
        manifest.write_text(manifest.read_text().replace("1.0.0", "1.0.1"))
        self.assertEqual(version_group_errors(tree), [])
        self.assertEqual(len(version_group_errors(Tree(tree.root, "HEAD"))), 1)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rev", help="check this commit instead of running the unit tests")
    parser.add_argument("--only", choices=CHECKS, action="append", help="restrict --rev to these checks")
    args, rest = parser.parse_known_args()
    if args.rev is None:
        unittest.main(argv=[sys.argv[0], *rest])
    errors = version_group_errors(Tree(ROOT, args.rev), tuple(args.only or CHECKS))
    for error in errors:
        print(f"error: {error}", file=sys.stderr)
    print(f"{args.rev}: {'FAIL' if errors else 'ok'} (checked: {', '.join(args.only or CHECKS)})")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
