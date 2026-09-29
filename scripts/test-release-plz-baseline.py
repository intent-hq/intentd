#!/usr/bin/env python3
"""Actual release-plz 0.3.162 regressions; disposable local histories, no publication.

RELEASE_PLZ=/path/to/release-plz python3 -S scripts/test-release-plz-baseline.py
The two-cycle fixture comes from the verified #6163 investigation. No token or
remote is needed: only `update` is executed, never `release-pr` or `release`.
"""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib
import unittest

ROOT = Path(__file__).resolve().parents[1]
PREPARE = ROOT / "scripts/release-plz-baseline.py"
BINARY = os.environ.get("RELEASE_PLZ", "release-plz")
RESERVED = "__initial_intent-nodes-v"
CONFIG = '''[workspace]
release = false
publish = false
git_release_enable = false
git_tag_name = "v{{ version }}"
git_tag_enable = false
changelog_update = false
semver_check = false
[[package]]
name = "intentd"
release = true
git_only = true
git_tag_enable = true
changelog_update = true
changelog_include = ["intent-nodes"]
version_group = "intentd"
[[package]]
name = "intent-nodes"
release = true
git_only = true
version_group = "intentd"
[[package]]
name = "intentd-sitter"
release = true
git_only = true
'''


class BaselineTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="release-plz-baseline-test-")
        self.addCleanup(temp.cleanup)
        self.repo = Path(temp.name) / "repo"
        self.repo.mkdir()
        self.output = Path(temp.name) / "planning.toml"
        self.env = {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull,
                    "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_COUNT": "0",
                    "GIT_AUTHOR_NAME": "Fixture", "GIT_COMMITTER_NAME": "Fixture",
                    "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
                    "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
                    "CARGO_NET_OFFLINE": "true", "CARGO_TERM_PROGRESS_WHEN": "never",
                    "CARGO_TARGET_DIR": str(Path(temp.name) / "target")}
        for key in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GITHUB_TOKEN",
                    "GH_TOKEN", "GIT_TOKEN", "GIT_CLIFF_CONFIG", "RUSTC_WRAPPER"):
            self.env.pop(key, None)
        self.run_ok("git", "init", "-q", "-b", "main")
        self.put(".gitignore", "target/\n")
        self.put("Cargo.toml", '[workspace]\nmembers = ["crates/*"]\nresolver = "2"\n')
        self.package("intentd", "0.9.114")
        self.package("intentd-sitter", "0.1.0")
        self.put("release-plz.toml", CONFIG)
        self.run_ok("cargo", "generate-lockfile")
        self.commit("chore: fixture baseline")
        self.run_ok("git", "tag", "v0.9.114")
        self.package("intent-nodes", "0.9.113")
        self.run_ok("cargo", "generate-lockfile")
        self.commit("feat: add nodes")

    def command(self, *args):
        return subprocess.run(args, cwd=self.repo, env=self.env, text=True,
                              capture_output=True, timeout=120)

    def run_ok(self, *args):
        result = self.command(*args)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout

    def put(self, name, text):
        path = self.repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def package(self, name, version):
        self.put(f"crates/{name}/Cargo.toml", f'[package]\nname = "{name}"\n'
                 f'version = "{version}"\nedition = "2021"\nlicense = "MIT"\n'
                 'description = "Release baseline fixture"\n')
        self.put(f"crates/{name}/src/lib.rs", "pub fn value() {}\n")

    def commit(self, message):
        self.run_ok("git", "add", ".")
        self.run_ok("git", "commit", "-qm", message)

    def prepare(self, success=True):
        result = self.command(sys.executable, "-S", str(PREPARE), "--output", str(self.output))
        if success:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertFalse(self.output.exists(), "failed preparation left usable config")
        self.assertEqual(self.run_ok("git", "worktree", "list", "--porcelain").count("worktree "), 1)
        return result

    def update(self, prepared=True, success=True):
        args = ("--config", str(self.output)) if prepared else ()
        result = self.command(BINARY, "update", "--repo-url",
                              "https://github.com/example/baseline-fixture", *args)
        if success:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0)
        return result

    def version(self, name):
        return tomllib.loads((self.repo / f"crates/{name}/Cargo.toml").read_text())["package"]["version"]

    def test_unmodified_tool_reproduces_missing_package(self):
        self.assertEqual(self.run_ok(BINARY, "--version").strip(), "release-plz 0.3.162")
        result = self.update(prepared=False, success=False)
        self.assertIn('Failed to find package "intent-nodes"', result.stdout + result.stderr)
        self.assertEqual(self.run_ok("git", "status", "--porcelain"), "")

    def test_introduction_then_shared_baseline_retires_override(self):
        original = (self.repo / "release-plz.toml").read_bytes()
        tags = self.run_ok("git", "tag")
        self.prepare()
        config = tomllib.loads(self.output.read_text())
        expected = tomllib.loads(CONFIG)
        expected["package"][1]["git_tag_name"] = RESERVED + "{{ version }}"
        self.assertEqual(config, expected)
        self.update()
        self.assertEqual(self.version("intentd"), "0.9.115")
        self.assertEqual(self.version("intent-nodes"), "0.9.115")
        self.assertEqual(self.version("intentd-sitter"), "0.1.0")
        self.assertEqual(self.run_ok("git", "tag"), tags)
        self.commit("chore: release v0.9.115")
        self.run_ok("git", "tag", "-am", "shared release", "v0.9.115")
        self.put("crates/intent-nodes/src/lib.rs", "pub fn repair() {}\n")
        self.commit("fix: repair nodes")
        self.output.unlink()
        self.prepare()
        self.assertEqual(self.output.read_bytes(), original)
        self.update()
        self.assertEqual(self.version("intentd"), "0.9.116")
        self.assertEqual(self.version("intent-nodes"), "0.9.116")
        notes = (self.repo / "crates/intentd/CHANGELOG.md").read_text()
        self.assertEqual(notes.count("- add nodes"), 1)
        self.assertEqual(notes.count("- repair nodes"), 1)
        self.assertNotIn("add nodes", notes.split("## [0.9.115]")[0])
        self.assertEqual((self.repo / "release-plz.toml").read_bytes(), original)
        self.commit("chore: release v0.9.116")
        self.run_ok("git", "tag", "v0.9.116")
        self.output.unlink()
        self.prepare()
        self.update()
        self.assertEqual(self.run_ok("git", "status", "--porcelain"), "")

    def test_numeric_tag_selection_matches_tool_and_ignores_other_patterns(self):
        for tag in ("v0.9.9", "v999.0.0-alpha.1", "sitter-v999.0.0", "v01.0.0",
                    "v18446744073709551616.0.0"):
            self.run_ok("git", "tag", tag)
        self.prepare()
        self.assertIn(RESERVED, self.output.read_text())
        self.update()
        self.assertEqual(self.version("intentd"), "0.9.115")

    def test_no_tags_leaves_original_config(self):
        self.run_ok("git", "tag", "-d", "v0.9.114")
        self.prepare()
        self.assertEqual(self.output.read_text(), CONFIG)
        self.update()

    def test_reserved_tag_collision_fails(self):
        self.run_ok("git", "tag", RESERVED + "0.9.115")
        result = self.prepare(success=False)
        self.assertIn("reserved", result.stderr)

    def test_unsafe_policy_fails(self):
        self.put("release-plz.toml", CONFIG.replace('name = "intent-nodes"',
                  'name = "intent-nodes"\ngit_tag_enable = true'))
        self.assertIn("policy", self.prepare(success=False).stderr)

    def test_unexpected_missing_package_fails(self):
        self.package("another-new-crate", "0.9.114")
        self.put("release-plz.toml", CONFIG + '\n[[package]]\nname = "another-new-crate"\n'
                 'release = true\ngit_only = true\nversion_group = "intentd"\n')
        self.assertIn("another-new-crate", self.prepare(success=False).stderr)

    def test_malformed_current_metadata_fails(self):
        self.put("crates/intent-nodes/Cargo.toml", "not valid toml {")
        self.assertIn("cargo", self.prepare(success=False).stderr)

    def test_malformed_baseline_metadata_fails(self):
        self.run_ok("git", "checkout", "--detach", "v0.9.114")
        self.put("crates/intentd/Cargo.toml", "not valid toml {")
        self.commit("chore: corrupt baseline fixture")
        self.run_ok("git", "tag", "v0.9.115")
        self.run_ok("git", "checkout", "main")
        self.assertIn("cargo", self.prepare(success=False).stderr)

    def test_unreadable_tag_is_not_absence(self):
        blob = self.run_ok("git", "rev-parse", "HEAD:Cargo.toml").strip()
        self.run_ok("git", "tag", "v0.9.115", blob)
        self.assertIn("git", self.prepare(success=False).stderr)

    def test_baseline_build_error_stays_error_in_real_tool(self):
        self.run_ok("git", "checkout", "--detach", "v0.9.114")
        self.put("crates/intentd/src/lib.rs", 'compile_error!("baseline build broken");\n')
        self.commit("chore: broken baseline fixture")
        self.run_ok("git", "tag", "v0.9.115")
        self.run_ok("git", "checkout", "main")
        self.prepare()
        result = self.update(success=False)
        self.assertIn("baseline build broken", result.stdout + result.stderr)

    def test_shallow_history_fails(self):
        clone = self.repo.parent / "shallow"
        self.run_ok("git", "clone", "--depth=1", self.repo.as_uri(), str(clone))
        self.repo = clone
        self.assertIn("shallow", self.prepare(success=False).stderr)

    def test_refuses_output_inside_checkout(self):
        self.output = self.repo / "generated.toml"
        self.assertIn("outside", self.prepare(success=False).stderr)

    def test_real_repository_config_changes_only_nodes_tag_lookup(self):
        original = (ROOT / "release-plz.toml").read_bytes()
        config = tomllib.loads(original.decode())
        self.run_ok("git", "checkout", "--detach", "v0.9.114")
        for package in config["package"]:
            name = package["name"]
            if name not in ("intent-nodes", "intentd", "intentd-sitter"):
                self.package(name, "0.9.114")
        self.put("release-plz.toml", original.decode())
        self.run_ok("cargo", "generate-lockfile")
        self.commit("chore: production configuration baseline")
        self.run_ok("git", "tag", "v0.9.115")
        self.package("intent-nodes", "0.9.113")
        self.run_ok("cargo", "generate-lockfile")
        self.commit("feat: add nodes")
        self.run_ok("git", "switch", "-c", "production-config-fixture")
        self.prepare()
        expected = tomllib.loads(original.decode())
        for package in expected["package"]:
            if package["name"] == "intent-nodes":
                package["git_tag_name"] = RESERVED + "{{ version }}"
        self.assertEqual(tomllib.loads(self.output.read_text()), expected)
        self.update()
        self.assertIn("Add nodes", (self.repo / "CHANGELOG.md").read_text())
        self.assertEqual((self.repo / "release-plz.toml").read_bytes(), original)
        self.assertEqual(self.version("intentd-sitter"), "0.1.0")

    def test_workflow_limits_temporary_config_to_pinned_planning_action(self):
        workflow = (ROOT / ".github/workflows/release-plz.yml").read_text()
        release, planning = workflow.split("  release-plz-pr:\n", 1)
        self.assertNotIn("release-plz-planning.toml", release)
        self.assertIn("fetch-depth: 0", planning)
        self.assertIn('python3 -S scripts/release-plz-baseline.py --output "$RUNNER_TEMP/release-plz-planning.toml"', planning)
        self.assertIn("version: 0.3.162", planning)
        self.assertIn("config: ${{ runner.temp }}/release-plz-planning.toml", planning)
        self.assertIn("command: release-pr", planning)
        self.assertIn('run: rm -f "$RUNNER_TEMP/release-plz-planning.toml"', planning)
        ci = (ROOT / ".github/workflows/ci.yml").read_text()
        self.assertIn("tool: release-plz@0.3.162", ci)
        self.assertIn("python3 -S scripts/test-release-plz-baseline.py", ci)

    def test_malformed_configuration_fails(self):
        self.put("release-plz.toml", "[broken")
        self.prepare(success=False)

    def test_missing_baseline_member_manifest_fails(self):
        self.run_ok("git", "checkout", "--detach", "v0.9.114")
        self.put("Cargo.toml", '[workspace]\nmembers = ["crates/intentd", "missing"]\n')
        self.commit("chore: missing manifest baseline")
        self.run_ok("git", "tag", "v0.9.115")
        self.run_ok("git", "checkout", "main")
        self.assertIn("cargo", self.prepare(success=False).stderr)

    def test_does_not_overwrite_existing_output(self):
        self.output.write_text("existing")
        result = self.command(sys.executable, "-S", str(PREPARE), "--output", str(self.output))
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.output.read_text(), "existing")


if __name__ == "__main__":
    unittest.main(verbosity=2)
