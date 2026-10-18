#!/usr/bin/env python3
"""Fail when crates in one release-plz version_group disagree on their version.

release-plz.toml puts intentd and its library crates in `version_group =
"intentd"` so they release in lockstep. Library code reads its own
`env!("CARGO_PKG_VERSION")` for the daemon version (client.hello, MCP/ACP
client info, user agents, transfer manifests), so a release that bumps only
some group members ships a daemon that misreports its version. Releases #2461
and #2464 left intentd at 0.11.6 with the libraries at 0.11.5: v0.11.6 told
clients it was 0.11.5, and the merge queue failed on version-sensitive tests.

This check runs in the pr-static job, which also runs on release PRs, so a
drifting release PR fails before it merges. It also rejects stale internal
dependency pins (`intent-core = { path = ..., version = "<old>" }`) on group
members.
"""

from pathlib import Path
import tempfile
import tomllib
import unittest


ROOT = Path(__file__).resolve().parents[1]


def manifests(root):
    """Package name -> (manifest path, parsed manifest) for every workspace crate."""
    out = {}
    for path in sorted((root / "crates").glob("*/Cargo.toml")):
        data = tomllib.loads(path.read_text())
        name = data.get("package", {}).get("name")
        if name:
            out[name] = (path, data)
    return out


def version_group_errors(root):
    config = tomllib.loads((root / "release-plz.toml").read_text())
    groups = {}
    for package in config.get("package", []):
        if package.get("version_group"):
            groups.setdefault(package["version_group"], []).append(package["name"])
    crates = manifests(root)
    errors = []
    for group, members in sorted(groups.items()):
        missing = [m for m in members if m not in crates]
        errors.extend(f"{group}: {m} is in release-plz.toml but has no crates/*/Cargo.toml" for m in missing)
        versions = {m: crates[m][1]["package"].get("version") for m in members if m in crates}
        if len(set(versions.values())) > 1:
            listing = ", ".join(f"{m}={v}" for m, v in sorted(versions.items()))
            errors.append(f"{group}: members disagree on version ({listing})")
            continue
        if not versions:
            continue
        expected = next(iter(versions.values()))
        for name, (path, data) in sorted(crates.items()):
            for table in ("dependencies", "dev-dependencies", "build-dependencies"):
                for dep, spec in data.get(table, {}).items():
                    if dep in versions and isinstance(spec, dict) and "path" in spec and "version" in spec \
                            and spec["version"] != expected:
                        errors.append(f"{group}: {path.relative_to(root)} pins {dep} {spec['version']} "
                                      f"in [{table}], expected {expected}")
    return errors


class VersionGroupTests(unittest.TestCase):
    def test_repository_version_groups_are_in_lockstep(self):
        self.assertEqual(version_group_errors(ROOT), [])

    def fixture(self, versions, pin="1.0.0", sitter="0.1.0"):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        (root / "release-plz.toml").write_text(
            "".join(f'[[package]]\nname = "{n}"\nversion_group = "daemon"\n\n' for n in versions)
            + '[[package]]\nname = "sitter"\n')
        for name, version in {**versions, "sitter": sitter}.items():
            crate = root / "crates" / name
            crate.mkdir(parents=True)
            deps = "" if name == "core" else f'[dependencies]\ncore = {{ path = "../core", version = "{pin}" }}\n'
            (crate / "Cargo.toml").write_text(f'[package]\nname = "{name}"\nversion = "{version}"\n{deps}')
        return root

    def test_lockstep_group_passes_and_independent_crates_are_ignored(self):
        self.assertEqual(version_group_errors(self.fixture({"daemon": "1.0.0", "core": "1.0.0"}, sitter="0.4.2")), [])

    def test_drifted_member_is_reported(self):
        errors = version_group_errors(self.fixture({"daemon": "1.0.1", "core": "1.0.0"}))
        self.assertEqual(errors, ["daemon: members disagree on version (core=1.0.0, daemon=1.0.1)"])

    def test_stale_internal_pin_is_reported(self):
        errors = version_group_errors(self.fixture({"daemon": "1.0.1", "core": "1.0.1"}, pin="1.0.0"))
        self.assertTrue(any("pins core 1.0.0" in e and "expected 1.0.1" in e for e in errors), errors)

    def test_group_member_without_a_crate_is_reported(self):
        root = self.fixture({"daemon": "1.0.0", "core": "1.0.0"})
        (root / "release-plz.toml").write_text(
            (root / "release-plz.toml").read_text() + '[[package]]\nname = "gone"\nversion_group = "daemon"\n')
        self.assertIn("daemon: gone is in release-plz.toml but has no crates/*/Cargo.toml",
                      version_group_errors(root))


if __name__ == "__main__":
    unittest.main()
