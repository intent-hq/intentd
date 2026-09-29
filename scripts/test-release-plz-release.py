#!/usr/bin/env python3
"""Offline controls: python3 -S scripts/test-release-plz-release.py.

The sole publication executable is a fixture on a private PATH. No real
release-plz, cargo, git, gh, network, or credentials are available to it.
RELEASE_PLZ_TEST_RUNNER may select the saved pre-fix action command for RED.
"""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib
import unittest


ROOT = Path(__file__).resolve().parents[1]
RUNNER = Path(os.environ.get("RELEASE_PLZ_TEST_RUNNER", ROOT / "scripts/release-plz-release.py"))
QUOTA = 'Response body:\n' + json.dumps({
    "message": "API rate limit exceeded for user ID 526899.", "status": "403",
}) + '\nCaused by:\n    HTTP status client error (403 Forbidden) for url (https://api.github.com/repos/intent-hq/intentd/commits/abc/pulls)\n'


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="release-quota-test-")
        self.addCleanup(temp.cleanup)
        self.repo = Path(temp.name)
        self.bin = self.repo / "bin"
        self.bin.mkdir()
        self.summary = self.repo / "summary"
        self.outputs = self.repo / "outputs"
        self.env = {
            "PATH": str(self.bin), "HOME": str(self.repo),
            "GITHUB_TOKEN": "fixture-token-not-a-credential",
            "GITHUB_REPOSITORY": "intent-hq/intentd", "GITHUB_RUN_ID": "123",
            "GITHUB_STEP_SUMMARY": str(self.summary),
            "GITHUB_OUTPUT": str(self.outputs),
            "PYTHONDONTWRITEBYTECODE": "1", "FIXTURE_ROOT": str(self.repo),
        }
        stub = self.bin / "release-plz"
        stub.write_text(f"#!{sys.executable} -S\n" + '''import json, os, pathlib, sys
root = pathlib.Path(os.environ["FIXTURE_ROOT"])
with (root / "calls").open("a") as log:
    log.write(json.dumps(sys.argv[1:]) + "\\n")
fixture = json.loads((root / "fixture").read_text())
if fixture.get("partial"):
    (root / "remote-tag").write_text("already-pushed")
print(fixture.get("stdout", '{"releases": []}'))
print(fixture.get("stderr", ""), file=sys.stderr)
sys.exit(fixture.get("exit", 0))
''')
        stub.chmod(0o700)
        for name in ("gh", "cargo", "git", "curl", "sleep"):
            guard = self.bin / name
            guard.write_text(f"#!{sys.executable} -S\nraise SystemExit('unexpected external command: {name}')\n")
            guard.chmod(0o700)

    def invoke(self, **fixture):
        (self.repo / "fixture").write_text(json.dumps(fixture))
        result = subprocess.run(
            [sys.executable, "-I", "-S", "-B", str(RUNNER)],
            cwd=self.repo, env=self.env, capture_output=True, text=True, timeout=10,
        )
        self.calls = [json.loads(line) for line in (self.repo / "calls").read_text().splitlines()]
        self.assertEqual(self.calls[-1], ["release", "--git-token", self.env["GITHUB_TOKEN"], "--forge", "github", "-o", "json"])
        return result

    def assert_failure(self, result, state, code=1):
        self.assertEqual(result.returncode, code, result.stdout + result.stderr)
        self.assertEqual(len(self.calls), 1, "Publication must never be retried automatically")
        self.assertTrue(self.summary.exists(), "Missing actionable release failure summary")
        self.assertTrue(self.outputs.exists(), "Missing classified release failure state")
        self.assertIn(f"release_state={state}", self.outputs.read_text())
        self.assertNotIn("releases_created=true", self.outputs.read_text())
        summary = self.summary.read_text()
        self.assertIn("remote tags", summary)
        self.assertIn("partial", summary)
        self.assertIn("Release PR remains blocked", summary)
        self.assertNotIn(self.env["GITHUB_TOKEN"], result.stdout + result.stderr + summary)
        return summary

    def test_reported_quota_retains_failure_and_actionable_reset(self):
        result = self.invoke(exit=1, stderr=QUOTA)
        summary = self.assert_failure(result, "quota_blocked")
        self.assertIn("RELEASE_PLZ_TOKEN", summary)
        self.assertIn("reset time was not supplied", summary)
        self.assertIn("gh run rerun 123 --repo intent-hq/intentd --failed", summary)
        self.assertIn("one manual", summary)
        self.assertNotIn("seconds", summary)
        self.assertNotIn("minute", summary)

    def test_unassociated_headers_cannot_supply_reset_timing(self):
        headers = 'X-RateLimit-Reset: 1\nRetry-After: 120\n'
        separator = '\nEND OF RESPONSE\nUnrelated log record\n'
        errors = {
            "preceding": headers + separator + QUOTA,
            "following": QUOTA + separator + headers,
            "adjacent_without_envelope": QUOTA + headers,
            "reset_only": QUOTA + 'X-RateLimit-Reset: 1\n',
            "retry_only": QUOTA + 'Retry-After: 120\n',
            "conflicting": QUOTA + headers + separator + 'X-RateLimit-Reset: 9999999999\nRetry-After: 600\n',
            "duplicate_responses": QUOTA + headers + separator + QUOTA,
        }
        for boundary, error in errors.items():
            with self.subTest(boundary=boundary):
                for file in (self.repo / "calls", self.outputs, self.summary):
                    file.unlink(missing_ok=True)
                result = self.invoke(exit=7, stderr=error)
                summary = self.assert_failure(result, "quota_blocked", 7)
                self.assertIn("reset time was not supplied", summary)
                self.assertNotIn("Unix time", summary)
                self.assertNotIn("seconds", summary)

    def test_unsubstantiated_secondary_error_does_not_get_a_retry_command(self):
        result = self.invoke(exit=1, stderr=QUOTA.replace("API rate limit exceeded for user ID 526899.", "You have exceeded a secondary rate limit. Please wait a few minutes before you try again."))
        summary = self.assert_failure(result, "failed")
        self.assertNotIn("gh run rerun", summary)

    def test_malformed_reset_is_not_treated_as_a_known_deadline(self):
        result = self.invoke(exit=1, stderr=QUOTA + '\nX-RateLimit-Reset: tomorrow\nRetry-After: -1\n')
        summary = self.assert_failure(result, "quota_blocked")
        self.assertIn("reset time was not supplied", summary)
        self.assertNotIn("tomorrow", summary)

    def test_ansi_colored_error_is_classified(self):
        result = self.invoke(exit=1, stderr='\x1b[31m' + QUOTA + '\x1b[0m')
        self.assert_failure(result, "quota_blocked")

    def test_permanent_403_is_not_quota(self):
        result = self.invoke(exit=4, stderr=QUOTA.replace("API rate limit exceeded for user ID 526899.", "Resource not accessible by personal access token"))
        summary = self.assert_failure(result, "failed", 4)
        self.assertIn("authentication", summary)
        self.assertNotIn("gh run rerun", summary)

    def test_unknown_failures_are_not_quota(self):
        for error in (
            "403 Forbidden", "API rate limit exceeded",
            QUOTA.replace('"403"', '"500"'), QUOTA.replace('"403"', '"429"'),
            QUOTA.replace("api.github.com", "example.invalid"),
            QUOTA.replace("403 Forbidden", "500 Internal Server Error"),
            QUOTA.replace("/commits/abc/pulls", "/releases"),
            QUOTA.replace("\nCaused by:", "\nUnrelated log message\nCaused by:"),
        ):
            with self.subTest(error=error):
                for file in (self.repo / "calls", self.outputs, self.summary):
                    file.unlink(missing_ok=True)
                result = self.invoke(exit=9, stderr=error)
                self.assert_failure(result, "failed", 9)

    def test_partial_publication_is_never_replayed_or_declared_absent(self):
        result = self.invoke(exit=1, partial=True, stderr=QUOTA)
        self.assert_failure(result, "quota_blocked")
        self.assertEqual((self.repo / "remote-tag").read_text(), "already-pushed")
        self.assertIn("Do not delete or move", self.summary.read_text())

    def test_repeated_quota_stays_failed_without_an_internal_retry(self):
        for attempt in (1, 2):
            result = self.invoke(exit=1, stderr=QUOTA)
            self.assertEqual(result.returncode, 1)
            self.assertEqual(len(self.calls), attempt)
        self.assertIn("If quota persists, stop", self.summary.read_text())
        self.assertNotIn("releases_created=true", self.outputs.read_text())

    def test_success_and_already_published_noop_preserve_action_outputs(self):
        for releases in ([], [{"package_name": "intentd", "version": "0.9.1"}]):
            with self.subTest(releases=releases):
                for file in (self.repo / "calls", self.outputs, self.summary):
                    file.unlink(missing_ok=True)
                (self.repo / "remote-tag").write_text("existing-tag")
                result = self.invoke(stdout=json.dumps({"releases": releases}))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(len(self.calls), 1)
                self.assertEqual((self.repo / "remote-tag").read_text(), "existing-tag")
                output = dict(line.split("=", 1) for line in self.outputs.read_text().splitlines())
                self.assertEqual(json.loads(output["releases"]), releases)
                self.assertEqual(output["releases_created"], str(bool(releases)).lower())
                self.assertEqual(output["release_state"], "success")

    def test_successful_command_with_unusable_output_does_not_unlock_pr(self):
        for output in ("invalid", "{}", '{"releases": null}'):
            with self.subTest(output=output):
                for file in (self.repo / "calls", self.outputs, self.summary):
                    file.unlink(missing_ok=True)
                result = self.invoke(stdout=output)
                self.assert_failure(result, "failed")

    def test_logged_token_is_redacted(self):
        result = self.invoke(exit=1, stderr=QUOTA + self.env["GITHUB_TOKEN"])
        self.assert_failure(result, "quota_blocked")

    def test_diagnostic_write_failure_cannot_replace_original_exit(self):
        self.env["GITHUB_STEP_SUMMARY"] = str(self.repo)
        self.env["GITHUB_OUTPUT"] = str(self.repo)
        result = self.invoke(exit=7, stderr=QUOTA)
        self.assertEqual(result.returncode, 7, result.stderr)
        self.assertEqual(len(self.calls), 1)
        self.assertIn("Release quota blocked", result.stderr)
        self.assertIn("Could not write release failure summary", result.stderr)
        self.assertIn("Could not write release failure output", result.stderr)


class WorkflowTests(unittest.TestCase):
    def test_release_dependency_concurrency_and_existing_tag_policy(self):
        workflow = (ROOT / ".github/workflows/release-plz.yml").read_text()
        release, pr = workflow.split("  release-plz-pr:\n", 1)
        self.assertIn("python3 -S scripts/release-plz-release.py", release)
        self.assertNotIn("continue-on-error:", workflow)
        self.assertIn("    needs: release-plz-release\n", pr)
        self.assertIn("if: ${{ github.repository_owner == 'intent-hq' }}", pr)
        self.assertEqual(sum(line.strip() == "cancel-in-progress: false"
                             for line in workflow.splitlines()), 2)
        self.assertIn("'release-merge' || 'push'", release)
        self.assertIn("group: release-plz-pr-${{ github.ref }}", pr)
        self.assertIn("release-plz/action@aec534bbd8631793b9b3b8f1ee6cd886c322e17f", pr)
        self.assertIn("command: release-pr", pr)
        config = tomllib.loads((ROOT / "release-plz.toml").read_text())
        for key in ("release_always", "publish", "git_release_enable", "git_tag_enable"):
            self.assertFalse(config["workspace"][key])
        self.assertEqual(config["workspace"]["git_tag_name"], "v{{ version }}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
