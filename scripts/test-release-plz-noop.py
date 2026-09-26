#!/usr/bin/env python3
"""Offline functional tests: python3 -S scripts/test-release-plz-noop.py.

Run the real helper with release-plz outputs and temporary git histories.
No release-plz execution, GitHub credentials, network, or publication.
"""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().with_name("release-plz-noop.py")
MANIFEST = "crates/intentd/Cargo.toml"
VERSION = "0.9.110"
TAG = f"v{VERSION}"


class ReleasePlzNoopTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="test-release-plz-noop-")
        self.addCleanup(temp.cleanup)
        self.repo = Path(temp.name)
        self.env = {
            **os.environ,
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_COUNT": "0",
            "GIT_AUTHOR_NAME": "Test User",
            "GIT_COMMITTER_NAME": "Test User",
            "GIT_AUTHOR_EMAIL": "test@example.com",
            "GIT_COMMITTER_EMAIL": "test@example.com",
            "RELEASE_PLZ_OUTCOME": "success",
            "RELEASE_PLZ_PRS": "[]",
        }
        for key in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE"):
            self.env.pop(key, None)
        self.git("init", "-q", "-b", "main")
        self.manifest = self.repo / MANIFEST
        self.manifest.parent.mkdir(parents=True)
        self.manifest.write_text(f'[package]\nname = "intentd"\nversion = "{VERSION}"\n')
        self.base = self.commit("chore: release")
        self.git("tag", TAG)
        scripts = self.repo / "scripts"
        scripts.mkdir()
        (scripts / "release-helper.sh").write_text("#!/bin/sh\ntrue\n")
        self.head = self.commit("fix: release helper")
        self.env["GITHUB_SHA"] = self.head

    def git(self, *args):
        return subprocess.check_output(
            ["git", "-C", str(self.repo), *args], env=self.env,
            text=True, stderr=subprocess.PIPE,
        ).strip()

    def commit(self, message):
        self.git("add", ".")
        self.git("commit", "-qm", message)
        return self.git("rev-parse", "HEAD")

    def invoke(self):
        result = subprocess.run(
            [sys.executable, "-S", str(SCRIPT)], cwd=self.repo,
            env=self.env, text=True, capture_output=True, timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return dict(line.split("=", 1) for line in result.stdout.splitlines())

    def assert_noop(self, base=None):
        self.assertEqual(self.invoke(), {
            "no_release_needed": "true", "baseline_tag": TAG,
            "baseline_sha": base or self.base,
        })

    def assert_unconfirmed(self):
        self.assertEqual(self.invoke(), {"no_release_needed": "false"})

    def test_scripts_only_delta_with_empty_prs_exposes_released_baseline(self):
        self.assertNotEqual(self.head, self.base)
        self.assert_noop()

    def test_json_whitespace_is_allowed(self):
        self.env["RELEASE_PLZ_PRS"] = " \n[ \n ]\t"
        self.assert_noop()

    def test_pending_release_prs_do_not_emit_a_baseline(self):
        for prs in ('[{"number": 123}]', '[{}]', '[null]'):
            with self.subTest(prs=prs):
                self.env["RELEASE_PLZ_PRS"] = prs
                self.assert_unconfirmed()

    def test_missing_or_malformed_prs_cannot_mean_no_release(self):
        for prs in ("", "null", "false", "0", "{}", '{"prs":[]}',
                    '"[]"', "[", "[] []", "[]\ninvalid", "NaN"):
            with self.subTest(prs=prs):
                self.env["RELEASE_PLZ_PRS"] = prs
                self.assert_unconfirmed()
        del self.env["RELEASE_PLZ_PRS"]
        self.assert_unconfirmed()

    def test_failed_skipped_or_incomplete_action_cannot_emit_noop(self):
        for outcome in ("failure", "skipped", "cancelled", "timed_out", "", "pending"):
            with self.subTest(outcome=outcome):
                self.env["RELEASE_PLZ_OUTCOME"] = outcome
                self.assert_unconfirmed()
        del self.env["RELEASE_PLZ_OUTCOME"]
        self.assert_unconfirmed()

    def test_head_must_match_full_assessed_sha(self):
        for sha in ("", self.head[:7], self.base, "f" * 40, "HEAD", self.head + "\n"):
            with self.subTest(sha=sha):
                self.env["GITHUB_SHA"] = sha
                self.assert_unconfirmed()
        del self.env["GITHUB_SHA"]
        self.assert_unconfirmed()

    def test_missing_baseline_tag_cannot_emit_noop(self):
        self.git("tag", "-d", TAG)
        self.assert_unconfirmed()

    def test_version_bump_awaiting_its_tag_cannot_reuse_old_baseline(self):
        self.manifest.write_text('[package]\nname = "intentd"\nversion = "0.9.111"\n')
        self.env["GITHUB_SHA"] = self.commit("chore: release")
        self.assert_unconfirmed()

    def test_annotated_tag_reports_commit_sha_instead_of_tag_object(self):
        self.git("tag", "-d", TAG)
        self.git("tag", "-a", TAG, self.base, "-m", "Released version")
        self.assertNotEqual(self.git("rev-parse", TAG), self.base)
        self.assert_noop()

    def test_baseline_tag_on_head_is_valid(self):
        self.git("tag", "-f", TAG, self.head)
        self.assert_noop(base=self.head)

    def test_non_ancestor_tag_cannot_emit_noop(self):
        self.git("checkout", "-q", "-b", "other", self.base)
        (self.repo / "other.txt").write_text("Another history\n")
        other = self.commit("fix: another branch")
        self.git("tag", "-f", TAG, other)
        self.git("checkout", "-q", "main")
        self.assert_unconfirmed()

    def test_tag_must_resolve_to_commit(self):
        blob = self.git("rev-parse", f"HEAD:{MANIFEST}")
        self.git("tag", "-f", TAG, blob)
        self.assert_unconfirmed()

    def test_tag_manifest_must_match_version(self):
        self.manifest.write_text('[package]\nname = "intentd"\nversion = "0.9.109"\n')
        wrong_version = self.commit("chore: fixture version")
        self.git("tag", "-f", TAG, wrong_version)
        self.manifest.write_text(f'[package]\nname = "intentd"\nversion = "{VERSION}"\n')
        self.env["GITHUB_SHA"] = self.commit("chore: restore version")
        self.assert_unconfirmed()

    def test_invalid_or_unsupported_package_version_declines_proof(self):
        for manifest in (
            "not TOML", "[package]\n", '[package]\nversion.workspace = true\n',
            '[package]\nversion = 110\n', '[package]\nversion = "0.9.111-alpha.1"\n',
            '[package]\nversion = "0.9.111+build.1"\n',
            '[package]\nversion = "00.9.110"\n',
            '[package]\nversion = "0.9.110\\nbaseline_sha=forged"\n',
        ):
            with self.subTest(manifest=manifest):
                self.manifest.write_text(manifest)
                self.env["GITHUB_SHA"] = self.commit("chore: invalid fixture version")
                self.assert_unconfirmed()

    def test_missing_manifest_declines_proof(self):
        self.manifest.unlink()
        self.env["GITHUB_SHA"] = self.commit("chore: missing fixture manifest")
        self.assert_unconfirmed()

    def test_baseline_comes_from_assessed_commit(self):
        self.manifest.write_text('[package]\nname = "intentd"\nversion = "0.9.111"\n')
        self.assert_noop()


if __name__ == "__main__":
    unittest.main(verbosity=2)
