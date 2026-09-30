#!/usr/bin/env python3
"""Functional changelog checks using release-plz update in disposable git repos.

Run: RELEASE_PLZ=/path/to/release-plz python3 -S scripts/test-release-plz-changelog.py
Requires Python 3.11+, cargo, git, and release-plz (CI pins its version).
Uses the real release configuration with tiny crates and local dependencies; no tokens,
registry access, release publication, or changes to the source checkout.
"""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib
import unittest


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "release-plz.toml"
DAEMON_VERSION = "0.9.0"
SITTER_VERSION = "0.1.0"
INDEPENDENT_LIBRARY = "fixture-helper"


class ReleasePlzChangelogTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="test-release-plz-changelog-")
        self.addCleanup(temp.cleanup)
        self.repo = Path(temp.name) / "repo"
        self.repo.mkdir()
        self.env = {
            **os.environ,
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_COUNT": "0",
            "GIT_AUTHOR_NAME": "Test User",
            "GIT_COMMITTER_NAME": "Test User",
            "GIT_AUTHOR_EMAIL": "test@example.com",
            "GIT_COMMITTER_EMAIL": "test@example.com",
            "CARGO_NET_OFFLINE": "true",
            "CARGO_TARGET_DIR": str(Path(temp.name) / "target"),
            "CARGO_TERM_PROGRESS_WHEN": "never",
        }
        for key in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GITHUB_TOKEN",
                    "GH_TOKEN", "GIT_TOKEN", "GIT_CLIFF_CONFIG", "RUSTC_WRAPPER"):
            self.env.pop(key, None)
        self.run_command("git", "init", "-q", "-b", "main")
        shutil.copyfile(CONFIG, self.repo / "release-plz.toml")
        self.config = tomllib.loads(CONFIG.read_text())
        self.packages = {p["name"]: p for p in self.config["package"]}
        self.libraries = [name for name in self.packages
                          if name not in ("intentd", "intentd-sitter")]
        # Production libraries aggregate their commits into daemon notes before
        # dependency propagation. An independent library exercises the generated
        # mixed dependency summaries without changing the production policy.
        self.packages[INDEPENDENT_LIBRARY] = {"name": INDEPENDENT_LIBRARY}
        with (self.repo / "release-plz.toml").open("a") as config:
            config.write(
                f'\n[[package]]\nname = "{INDEPENDENT_LIBRARY}"\n'
                'release = true\ngit_only = true\n'
            )
        (self.repo / "Cargo.toml").write_text(
            '[workspace]\nmembers = ["crates/*"]\nresolver = "2"\n'
        )
        (self.repo / ".gitignore").write_text("target/\n")
        for name in self.packages:
            crate = self.repo / "crates" / name
            (crate / "src").mkdir(parents=True)
            version = SITTER_VERSION if name == "intentd-sitter" else DAEMON_VERSION
            (crate / "Cargo.toml").write_text(
                f'[package]\nname = "{name}"\nversion = "{version}"\n'
                'edition = "2021"\nlicense = "MIT"\ndescription = "Fixture"\n'
            )
            (crate / "src/lib.rs").write_text("pub fn value() -> u32 { 0 }\n")
        daemon = self.repo / "crates/intentd"
        with (daemon / "Cargo.toml").open("a") as manifest:
            manifest.write(
                '\n[dependencies]\n'
                f'intentd-sitter = {{ path = "../intentd-sitter", version = "{SITTER_VERSION}" }}\n'
                f'intent-core = {{ path = "../intent-core", version = "{DAEMON_VERSION}" }}\n'
                f'{INDEPENDENT_LIBRARY} = {{ path = "../{INDEPENDENT_LIBRARY}", version = "{DAEMON_VERSION}" }}\n'
            )
        (daemon / "src/main.rs").write_text(
            'fn main() { println!("{}", intentd_sitter::value() + intent_core::value()); }\n'
        )
        self.run_command("cargo", "generate-lockfile", "--offline")
        self.commit("chore: fixture baseline")
        self.run_command("git", "tag", f"v{DAEMON_VERSION}")
        self.run_command("git", "tag", f"v{SITTER_VERSION}")

    def run_command(self, *args):
        result = subprocess.run(args, cwd=self.repo, env=self.env, text=True,
                                capture_output=True, timeout=120)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout

    def commit(self, message):
        self.run_command("git", "add", ".")
        self.run_command("git", "commit", "-qm", message)

    def change(self, names, message, value=1):
        for name in names:
            (self.repo / "crates" / name / "src/lib.rs").write_text(
                f"pub fn value() -> u32 {{ {value} }}\n"
            )
        self.commit(message)

    def update(self):
        self.run_command(os.environ.get("RELEASE_PLZ", "release-plz"), "update",
                         "--repo-url", "https://github.com/example/changelog-fixture")

    def version(self, name):
        return tomllib.loads(
            (self.repo / "crates" / name / "Cargo.toml").read_text()
        )["package"]["version"]

    def test_sitter_only_bumps_sitter_without_daemon_notes(self):
        self.change(["intentd-sitter"], "fix: sitter exclusive repair")
        self.update()
        changelog = self.repo / "CHANGELOG.md"
        notes = changelog.read_text() if changelog.exists() else ""
        self.assertNotIn("Sitter exclusive repair", notes)
        self.assertNotIn("Updated the following local packages: intentd-sitter", notes)
        self.assertNotIn("###", notes)
        # Dependency propagation still bumps the daemon; this is a notes policy.
        self.assertEqual(self.version("intentd"), "0.9.1")
        self.assertEqual(self.version("intentd-sitter"), "0.1.1")
        daemon = tomllib.loads((self.repo / "crates/intentd/Cargo.toml").read_text())
        self.assertEqual(daemon["dependencies"]["intentd-sitter"]["version"], "0.1.1")
        self.assertFalse((self.repo / "crates/intentd-sitter/CHANGELOG.md").exists())

    def test_daemon_libraries_and_mixed_changes_remain_visible(self):
        self.change(["intentd-sitter"], "fix: sitter exclusive repair")
        # A sitter scope must not hide a daemon change: filtering is by package.
        self.change(["intentd"], "fix(sitter): daemon integration repair")
        for name in self.libraries:
            self.change([name], f"fix: {name} library repair")
        self.change(["intentd", "intentd-sitter"], "fix: mixed component repair", 2)
        self.update()
        notes = (self.repo / "CHANGELOG.md").read_text()
        self.assertNotIn("Sitter exclusive repair", notes)
        self.assertIn("Daemon integration repair", notes)
        self.assertEqual(notes.count("Mixed component repair"), 1)
        for name in self.libraries:
            with self.subTest(package=name):
                self.assertIn(f"{name.capitalize()} library repair", notes)
        self.assertEqual(self.version("intentd"), "0.9.1")
        self.assertEqual(self.version("intentd-sitter"), "0.1.1")

    def test_library_only_change_reaches_daemon_notes(self):
        self.change(["intent-core"], "fix: shared library repair")
        self.update()
        self.assertIn("Shared library repair", (self.repo / "CHANGELOG.md").read_text())
        self.assertEqual(self.version("intentd"), "0.9.1")
        self.assertEqual(self.version("intentd-sitter"), SITTER_VERSION)

    def test_generated_mixed_dependency_summary_remains_visible(self):
        self.change(["intentd-sitter", INDEPENDENT_LIBRARY], "fix: independent repairs")
        self.update()
        notes = (self.repo / "CHANGELOG.md").read_text()
        self.assertIn("Updated the following local packages:", notes)
        self.assertIn(INDEPENDENT_LIBRARY, notes)
        self.assertIn("intentd-sitter", notes)
        self.assertEqual(self.version("intentd-sitter"), "0.1.1")
        self.assertEqual(self.version(INDEPENDENT_LIBRARY), "0.9.1")

    def test_generated_library_dependency_summary_remains_visible(self):
        self.change([INDEPENDENT_LIBRARY], "fix: independent library repair")
        self.update()
        notes = (self.repo / "CHANGELOG.md").read_text()
        self.assertIn(f"Updated the following local packages: {INDEPENDENT_LIBRARY}", notes)
        self.assertEqual(self.version("intentd-sitter"), SITTER_VERSION)

    def test_real_commit_matching_generated_message_remains_visible(self):
        self.change(["intentd"], "chore: updated the following local packages: intentd-sitter")
        self.update()
        notes = (self.repo / "CHANGELOG.md").read_text()
        self.assertIn("Updated the following local packages: intentd-sitter", notes)

    def test_sitter_keeps_independent_version_and_publication_policy(self):
        policy = {**self.config["workspace"], **self.packages["intentd-sitter"]}
        self.assertTrue(policy["release"])
        self.assertTrue(policy["git_only"])
        self.assertNotIn("version_group", policy)
        self.assertFalse(policy["changelog_update"])
        self.assertFalse(policy["git_tag_enable"])
        self.assertFalse(policy["git_release_enable"])
        self.assertFalse(policy["publish"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
