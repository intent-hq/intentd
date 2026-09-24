#!/usr/bin/env python3
"""Offline functional tests: python3 -S scripts/test-auto-pin-codex-acp.py.

Execute the real CLI with mocked curl/gh and real temporary git repositories.
The git shim only injects races/failures; it forwards operations to real git.
No credentials, network, Rust builds, or changes to the checked-out pin.
Use -S to exclude host-installed site hooks (for example test telemetry).
"""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/auto-pin-codex-acp.py"
CONFIG = "crates/intent-providers/src/config.rs"
BRANCH = "auto/codex-acp-pin"
REF = f"refs/heads/{BRANCH}"
REPO = "intent-hq/intentd"
ORIGIN = f"https://github.com/{REPO}.git"
REAL_GIT = shutil.which("git")
FIXTURE = '''// Keep all surrounding text and other provider pins unchanged.
pub const CLAUDE_AGENT_ACP_NPX_PACKAGE: &str = "claude-acp@0.8.1";
pub const CODEX_ACP_NPX_PACKAGE: &str = "@agentclientprotocol/codex-acp@VERSION";
pub const PI_ACP_NPX_PACKAGE: &str = "pi-acp@0.0.33";
'''

# The mock rejects unexpected calls, checks the real server-side query scope,
# records mutations, and materializes branch heads from the real bare remote.
MOCK = r'''
import json, os, pathlib, subprocess, sys
state_path = pathlib.Path(os.environ["MOCK_STATE"])
state = json.loads(state_path.read_text())
args = sys.argv[1:]
tool = pathlib.Path(sys.argv[0]).name
state["calls"].append([tool, *args])
def save():
    state_path.write_text(json.dumps(state))
def real_git(*argv):
    return subprocess.check_output([os.environ["REAL_GIT"], *argv], text=True).strip()
def remote_head():
    return real_git("--git-dir", state["remote"], "rev-parse", "refs/heads/auto/codex-acp-pin")
def fail():
    save()
    print("mock operation failed", file=sys.stderr)
    sys.exit(1)
save()
if tool == "curl":
    assert args == ["--fail", "--silent", "--show-error", "--location", "--max-time", "60",
                    "--retry", "2", "https://registry.npmjs.org/@agentclientprotocol%2fcodex-acp/latest"]
    if state.get("registry_error"): fail()
    print(state.get("registry_raw", json.dumps(state["manifest"])))
elif tool == "git":
    if args[:1] == ["fetch"] and args[-1] == "refs/heads/main":
        state["main_fetches"] += 1
        if state.get("main_fetch_error") == state["main_fetches"]: fail()
        if state["main_fetches"] == 2 and state.get("live_main"):
            real_git("--git-dir", state["remote"], "update-ref", "refs/heads/main", state["live_main"])
            if state.get("delete_branch_on_merge"):
                real_git("--git-dir", state["remote"], "update-ref", "-d", "refs/heads/auto/codex-acp-pin")
    if args[:1] == ["ls-remote"] and state.get("branch_lookup_error"): fail()
    if args[:1] == ["push"]:
        if state.get("push_error"): fail()
        if state.get("concurrent_head"):
            real_git("--git-dir", state["remote"], "update-ref", "refs/heads/auto/codex-acp-pin", state["concurrent_head"])
    save()
    os.execv(os.environ["REAL_GIT"], [os.environ["REAL_GIT"], *args])
elif tool == "gh":
    if args[:2] == ["api", "user"]:
        if state.get("identity_error"): fail()
        print(json.dumps({"login": "pin-bot"}))
    elif args[:4] == ["api", "--method", "GET", "repos/intent-hq/intentd/pulls"]:
        assert args[4:] == ["-f", "state=all", "-f", "head=intent-hq:auto/codex-acp-pin",
                           "-f", "base=main", "-f", "per_page=100", "--paginate", "--slurp"]
        state["pr_lookups"] += 1
        if state.get("pr_lookup_error") == state["pr_lookups"]: fail()
        if state.get("change_pr_on_recheck") and state["pr_lookups"] == 2:
            state["pr"]["body"] += "Human note during this run."
        prs = list(state.get("extra_prs", []))
        if state.get("pr"):
            state["pr"]["head"]["sha"] = remote_head()
            prs.append(state["pr"])
        save()
        print(state.get("pr_raw", json.dumps([prs])))
    elif args[:2] in (["pr", "create"], ["pr", "edit"]):
        def flag(name): return args[args.index(name) + 1]
        assert flag("--repo") == "intent-hq/intentd"
        if state.get("pr_write_error"): fail()
        if args[1] == "create":
            assert state.get("pr") is None
            assert flag("--base") == "main" and flag("--head") == "auto/codex-acp-pin"
            state["pr"] = {
                "number": 12, "state": "open", "merged_at": None, "draft": False,
                "head": {"ref": "auto/codex-acp-pin", "sha": remote_head(),
                         "repo": {"full_name": "intent-hq/intentd"}},
                "base": {"ref": "main", "repo": {"full_name": "intent-hq/intentd"}},
                "user": {"login": "pin-bot"}, "labels": [],
            }
        else:
            assert args[2] == str(state["pr"]["number"])
        state["pr"]["title"] = flag("--title")
        state["pr"]["body"] = pathlib.Path(flag("--body-file")).read_text()
        state["writes"].append(args[1])
        save()
        print("https://github.com/intent-hq/intentd/pull/12")
    else:
        raise AssertionError("Unexpected gh operation: " + repr(args))
else:
    raise AssertionError(tool)
'''


class PinAutomationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="test-codex-pin-")
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.remote = self.directory / "remote.git"
        self.repo = self.directory / "checkout"
        self.bin = self.directory / "bin"
        self.bin.mkdir()
        self.state_path = self.directory / "state.json"
        self.env = {
            **os.environ,
            "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_TERMINAL_PROMPT": "0", "GIT_CONFIG_COUNT": "0",
            "GIT_AUTHOR_NAME": "Test User", "GIT_COMMITTER_NAME": "Test User",
            "GIT_AUTHOR_EMAIL": "test@example.com", "GIT_COMMITTER_EMAIL": "test@example.com",
            "GIT_AUTHOR_DATE": "2026-01-01T00:00:00Z",
            "GIT_COMMITTER_DATE": "2026-01-01T00:00:00Z",
            "GH_TOKEN": "offline-test-token-never-log", "GITHUB_REPOSITORY": REPO,
            "MOCK_STATE": str(self.state_path), "REAL_GIT": REAL_GIT,
        }
        for key in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GITHUB_TOKEN"):
            self.env.pop(key, None)
        subprocess.run([REAL_GIT, "init", "-q", "--bare", str(self.remote)],
                       env=self.env, check=True)
        subprocess.run([REAL_GIT, "init", "-q", "-b", "main", str(self.repo)],
                       env=self.env, check=True)
        self.git("remote", "add", "origin", ORIGIN)
        # Test the real repository identity guard, but route transport offline.
        self.git("config", f"url.{self.remote}.insteadOf", ORIGIN)
        self.pin_file = self.repo / CONFIG
        self.pin_file.parent.mkdir(parents=True)
        self.pin_file.write_text(FIXTURE.replace("VERSION", "1.9.0"))
        (self.repo / "other.txt").write_text("Unrelated content\n")
        self.git("add", ".")
        self.git("commit", "-qm", "chore: fixture")
        self.git("push", "-q", "origin", "main")
        self.base = self.git("rev-parse", "HEAD")
        for name in ("curl", "gh", "git"):
            executable = self.bin / name
            executable.write_text(f"#!{sys.executable} -S\n" + MOCK)
            executable.chmod(0o755)
        self.env["PATH"] = f"{self.bin}:{os.environ['PATH']}"
        self.state = {
            "manifest": {"name": "@agentclientprotocol/codex-acp", "version": "1.13.1"},
            "remote": str(self.remote), "calls": [], "writes": [],
            "main_fetches": 0, "pr_lookups": 0, "pr": None,
        }

    def git(self, *args):
        return subprocess.check_output([REAL_GIT, "-C", str(self.repo), *args],
                                       env=self.env, text=True, stderr=subprocess.PIPE).strip()

    def head(self):
        refs = self.git("ls-remote", "--heads", "origin", REF)
        return refs.split()[0] if refs else None

    def invoke(self, *args, ok=True):
        self.state_path.write_text(json.dumps(self.state))
        result = subprocess.run([sys.executable, "-S", str(SCRIPT), *args], cwd=self.repo,
                                env=self.env, text=True, capture_output=True, timeout=30)
        self.state = json.loads(self.state_path.read_text())
        output = result.stdout + result.stderr
        self.assertNotIn("offline-test-token-never-log", output)
        self.assertEqual(result.returncode, 0 if ok else 1, output)
        for call in self.state["calls"]:
            if call[0] == "gh":
                self.assertNotIn("merge", call)
                self.assertNotIn("--auto", call)
        return output

    def clear_calls(self):
        self.state.update(calls=[], writes=[], main_fetches=0, pr_lookups=0)

    def assert_no_publication(self):
        self.assertEqual(self.state["writes"], [])
        self.assertFalse(any(call[:2] == ["git", "push"] or "commit-tree" in call
                             for call in self.state["calls"]))

    def main_commit(self, version, other="Unrelated content\n"):
        self.pin_file.write_text(FIXTURE.replace("VERSION", version))
        (self.repo / "other.txt").write_text(other)
        self.git("add", ".")
        self.git("commit", "-qm", "fix: another main change")
        commit = self.git("rev-parse", "HEAD")
        self.git("push", "-q", "origin", f"{commit}:refs/heads/fixture-live-main")
        return commit

    def test_stable_upgrade_changes_only_the_named_constant(self):
        self.invoke()
        head = self.head()
        self.assertEqual(self.git("show", f"{head}:{CONFIG}"),
                         FIXTURE.replace("VERSION", "1.13.1").strip())
        self.assertEqual(self.git("diff", "--name-only", self.base, head), CONFIG)
        self.assertEqual(self.git("rev-parse", "HEAD"), self.base)
        self.assertEqual(self.git("status", "--porcelain"), "")
        self.assertEqual(self.state["writes"], ["create"])
        self.assertTrue(self.state["pr"]["title"].startswith("fix: "))
        self.assertIn("https://github.com/agentclientprotocol/codex-acp/releases/tag/v1.13.1",
                      self.state["pr"]["body"])
        self.assertIn(f"--force-with-lease={REF}:", next(
            call for call in self.state["calls"] if call[:2] == ["git", "push"]))

    def test_numeric_version_ordering(self):
        for target in ("1.10.0", "1.100.0", "2.0.0", "10.0.0"):
            with self.subTest(target=target):
                self.state["manifest"]["version"] = target
                self.assertIn("would propose 1.9.0 -> " + target, self.invoke("--dry-run"))
                self.assert_no_publication()

    def test_malformed_and_prerelease_versions_are_rejected(self):
        for target in ("1.14.0-beta.1", "1.14.0+build.1", "v1.14.0", "01.14.0",
                       "1.014.0", "1.14.00", "1.14", "latest", "1.14.0\n",
                       "1.14.0; touch /tmp/bad", "", None, 1140, ["1.14.0"]):
            with self.subTest(target=target):
                self.state["manifest"]["version"] = target
                self.invoke(ok=False)
                self.assert_no_publication()

    def test_equal_older_and_numeric_older_versions_are_noops(self):
        for target in ("1.9.0", "1.8.99", "1.2.100", "0.99.99"):
            with self.subTest(target=target):
                self.state["manifest"]["version"] = target
                self.assertIn("nothing to do", self.invoke())
                self.assert_no_publication()

    def test_existing_pr_updates_on_the_same_branch(self):
        self.invoke()
        old = self.head()
        self.clear_calls()
        self.state["manifest"]["version"] = "1.14.0"
        self.invoke()
        self.assertNotEqual(self.head(), old)
        self.assertEqual(self.state["writes"], ["edit"])
        self.assertEqual(self.state["pr"]["number"], 12)
        self.assertIn("v1.14.0", self.state["pr"]["title"])

    def test_repeat_run_does_not_create_commits_push_or_edit_pr(self):
        self.invoke()
        old = self.head()
        self.clear_calls()
        self.assertIn("no commit, push, or PR edit", self.invoke())
        self.assertEqual(self.head(), old)
        self.assert_no_publication()

    def test_repeat_run_ignores_unrelated_main_advancement(self):
        self.invoke()
        old = self.head()
        self.main_commit("1.9.0", "New main content\n")
        self.git("push", "-q", "origin", "main")
        self.clear_calls()
        self.invoke()
        self.assertEqual(self.head(), old)
        self.assert_no_publication()

    def test_existing_newer_proposal_is_never_downgraded(self):
        self.invoke()
        self.clear_calls()
        self.state["manifest"]["version"] = "1.10.0"
        self.assertIn("already proposes newer", self.invoke())
        self.assert_no_publication()

    def test_dry_run_with_and_without_token_has_no_mutations(self):
        for token in ("offline-test-token-never-log", ""):
            with self.subTest(token_present=bool(token)):
                self.env["GH_TOKEN"] = token
                self.assertIn("dry-run: would propose", self.invoke("--dry-run"))
                self.assert_no_publication()
                self.assertIsNone(self.head())

    def test_missing_token_warns_without_reading_or_publishing(self):
        self.env.pop("GH_TOKEN")
        self.assertIn("RELEASE_PLZ_TOKEN is not set", self.invoke())
        self.assertEqual(self.state["calls"], [])
        self.assertIsNone(self.head())

    def test_registry_and_github_failures_stop_before_publication(self):
        for flag, value in (("registry_error", True), ("registry_raw", "not json"),
                            ("identity_error", True), ("pr_lookup_error", 1),
                            ("pr_lookup_error", 2), ("main_fetch_error", 1),
                            ("main_fetch_error", 2), ("branch_lookup_error", True),
                            ("pr_raw", '{}'), ("pr_raw", '[[{}]]')):
            with self.subTest(flag=flag, value=value):
                self.clear_calls()
                self.state[flag] = value
                self.invoke(ok=False)
                self.assert_no_publication()
                del self.state[flag]

    def test_wrong_package_is_rejected(self):
        self.state["manifest"]["name"] = "another-package"
        self.invoke(ok=False)
        self.assert_no_publication()

    def test_invalid_or_duplicate_pin_is_rejected(self):
        for text in (FIXTURE.replace("VERSION", "latest"),
                     FIXTURE.replace("CODEX_ACP_NPX_PACKAGE", "REMOVED_PIN"),
                     FIXTURE.replace("VERSION", "1.9.0") * 2):
            with self.subTest(text=text):
                self.pin_file.write_text(text)
                self.git("add", CONFIG)
                self.git("commit", "-qm", "chore: unexpected pin fixture")
                self.git("push", "-q", "origin", "main")
                self.invoke(ok=False)
                self.assert_no_publication()

    def test_wrong_origin_or_repository_is_rejected(self):
        self.env["GITHUB_REPOSITORY"] = "someone/intentd"
        self.invoke(ok=False)
        self.assert_no_publication()
        self.env["GITHUB_REPOSITORY"] = REPO
        self.git("remote", "set-url", "origin", "https://github.com/someone/intentd.git")
        self.invoke(ok=False)
        self.assert_no_publication()

    def test_live_main_landed_pin_does_not_recreate_update(self):
        self.state["live_main"] = self.main_commit("1.13.1")
        self.assertIn("Live main changed (pin 1.13.1)", self.invoke())
        self.assert_no_publication()
        self.assertIsNone(self.head())

    def test_live_main_other_pin_or_unrelated_changes_stop_stale_publication(self):
        for pin in ("1.10.0", "1.14.0", "1.9.0"):
            with self.subTest(pin=pin):
                self.git("push", "-q", "--force", "origin", f"{self.base}:refs/heads/main")
                self.clear_calls()
                self.state["live_main"] = self.main_commit(pin, f"Change {pin}\n")
                self.assertIn("Live main changed", self.invoke())
                self.assert_no_publication()

    def test_concurrent_merge_and_branch_deletion_does_not_recreate_pr(self):
        self.invoke()
        self.clear_calls()
        self.state["manifest"]["version"] = "1.14.0"
        self.state["live_main"] = self.main_commit("1.13.1")
        self.state["delete_branch_on_merge"] = True
        self.assertIn("Live main changed", self.invoke())
        self.assert_no_publication()
        self.assertIsNone(self.head())

    def test_human_branch_commit_is_preserved(self):
        self.invoke()
        self.git("checkout", "-q", "--detach", self.head())
        (self.repo / "other.txt").write_text("Human change\n")
        self.git("add", "other.txt")
        self.git("commit", "-qm", "fix: human work")
        self.git("push", "-q", "origin", f"HEAD:{REF}")
        human_head = self.head()
        self.clear_calls()
        self.state["manifest"]["version"] = "1.14.0"
        self.invoke(ok=False)
        self.assertEqual(self.head(), human_head)
        self.assert_no_publication()

    def test_human_pr_metadata_and_hold_label_are_preserved(self):
        self.invoke()
        original = json.loads(json.dumps(self.state["pr"]))
        self.state["manifest"]["version"] = "1.14.0"
        for field, value in (("title", "My proposal"), ("body", "Human notes"),
                             ("user", {"login": "someone-else"}), ("draft", True),
                             ("labels", [{"name": "hold-release"}])):
            with self.subTest(field=field):
                self.clear_calls()
                self.state["pr"] = {**original, field: value}
                self.invoke(ok=field == "labels")
                self.assert_no_publication()

    def test_closed_unmerged_pr_is_not_reopened(self):
        self.invoke()
        self.clear_calls()
        self.state["pr"]["state"] = "closed"
        self.assertIn("closed without merging", self.invoke(ok=False))
        self.assert_no_publication()

    def test_fork_and_unrelated_prs_are_never_edited(self):
        self.invoke()
        template = json.loads(json.dumps(self.state["pr"]))
        self.state["pr"] = None
        for field, value in (("repo", {"full_name": "someone/intentd"}),
                             ("ref", "unrelated-branch")):
            other = json.loads(json.dumps(template))
            other["number"] = 99
            other["head"][field] = value
            self.state.setdefault("extra_prs", []).append(other)
        self.clear_calls()
        self.invoke()
        self.assertEqual(self.state["writes"], ["create"])
        self.assertEqual(self.state["pr"]["number"], 12)
        self.assertFalse(any("commit-tree" in call for call in self.state["calls"]))

    def test_pr_edit_during_run_stops_publication(self):
        self.invoke()
        self.clear_calls()
        self.state["manifest"]["version"] = "1.14.0"
        self.state["change_pr_on_recheck"] = True
        self.assertIn("PR changed during this run", self.invoke(ok=False))
        self.assert_no_publication()

    def test_push_lease_preserves_concurrent_human_branch(self):
        self.invoke()
        original = self.head()
        self.state["concurrent_head"] = self.main_commit("1.12.0")
        self.clear_calls()
        self.state["manifest"]["version"] = "1.14.0"
        self.invoke(ok=False)
        self.assertEqual(self.head(), self.state["concurrent_head"])
        self.assertEqual(self.state["writes"], [])
        push = next(call for call in self.state["calls"] if call[:2] == ["git", "push"])
        self.assertIn(f"--force-with-lease={REF}:{original}", push)

    def test_pr_creation_failure_can_recover_without_another_commit(self):
        self.state["pr_write_error"] = True
        self.invoke(ok=False)
        original = self.head()
        self.assertIsNotNone(original)
        self.clear_calls()
        del self.state["pr_write_error"]
        self.invoke()
        self.assertEqual(self.head(), original)
        self.assertEqual(self.state["writes"], ["create"])
        self.assertFalse(any("commit-tree" in call or call[:2] == ["git", "push"]
                             for call in self.state["calls"]))

    def test_failed_push_never_creates_a_pr(self):
        self.state["push_error"] = True
        self.invoke(ok=False)
        self.assertIsNone(self.head())
        self.assertEqual(self.state["writes"], [])

    def test_checked_out_human_changes_and_index_are_untouched(self):
        self.pin_file.write_text("Uncommitted user work\n")
        (self.repo / "other.txt").write_text("Staged user work\n")
        self.git("add", "other.txt")
        status = self.git("status", "--porcelain")
        self.invoke()
        self.assertEqual(self.pin_file.read_text(), "Uncommitted user work\n")
        self.assertEqual(self.git("status", "--porcelain"), status)
        self.assertEqual(self.git("show", ":other.txt"), "Staged user work")


if __name__ == "__main__":
    unittest.main(verbosity=2)
