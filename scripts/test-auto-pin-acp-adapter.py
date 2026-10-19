#!/usr/bin/env python3
"""Offline functional tests: python3 -S scripts/test-auto-pin-acp-adapter.py.

Execute the real CLI with mocked curl/gh and real temporary git repositories.
The git shim only injects races/failures; it forwards operations to real git.
No credentials, network, Rust builds, or changes to the checked-out pin.
The full suite runs for Codex; a representative subset plus Claude-only
guards (engines.node, macro pin, branch isolation) runs for claude-code.
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
SCRIPT = ROOT / "scripts/auto-pin-acp-adapter.py"
WORKFLOW = ROOT / ".github/workflows/auto-pin-acp-adapters.yml"
CONFIG = "crates/intent-providers/src/config.rs"
METADATA = "crates/intent-providers/data/adapter-cli-packages.json"
REPO = "intent-hq/intentd"
ORIGIN = f"https://github.com/{REPO}.git"
REAL_GIT = shutil.which("git")
# Mirrors the real config.rs shapes: the Claude pin is a macro literal.
FIXTURE = '''// Keep all surrounding text and other provider pins unchanged.
macro_rules! claude_agent_acp_version {
    () => {
        "<claude-version>"
    };
}
pub const CLAUDE_AGENT_ACP_NPX_PACKAGE: &str = concat!(
    "@agentclientprotocol/claude-agent-acp@",
    claude_agent_acp_version!()
);
pub const CODEX_ACP_NPX_PACKAGE: &str = "@agentclientprotocol/codex-acp@<codex-version>";
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
    return real_git("--git-dir", state["remote"], "rev-parse", "refs/heads/" + state["branch"])
def fail():
    save()
    print("mock operation failed", file=sys.stderr)
    sys.exit(1)
save()
if tool == "curl":
    assert args == ["--fail", "--silent", "--show-error", "--location", "--max-time", "60",
                    "--retry", "2", state["registry"]], args
    if state.get("registry_error"): fail()
    print(state.get("registry_raw", json.dumps(state["manifest"])))
elif tool == "git":
    if args[:1] == ["fetch"] and args[-1] == "refs/heads/main":
        state["main_fetches"] += 1
        if state.get("main_fetch_error") == state["main_fetches"]: fail()
        if state["main_fetches"] == 2 and state.get("live_main"):
            real_git("--git-dir", state["remote"], "update-ref", "refs/heads/main", state["live_main"])
            if state.get("delete_branch_on_merge"):
                real_git("--git-dir", state["remote"], "update-ref", "-d", "refs/heads/" + state["branch"])
    if args[:1] == ["ls-remote"] and state.get("branch_lookup_error"): fail()
    if args[:1] == ["push"]:
        if state.get("push_error"): fail()
        if state.get("concurrent_head"):
            real_git("--git-dir", state["remote"], "update-ref", "refs/heads/" + state["branch"], state["concurrent_head"])
    save()
    os.execv(os.environ["REAL_GIT"], [os.environ["REAL_GIT"], *args])
elif tool == "gh":
    if args[:2] == ["api", "user"]:
        if state.get("identity_error"): fail()
        print(json.dumps({"login": "pin-bot"}))
    elif args[:4] == ["api", "--method", "GET", "repos/intent-hq/intentd/pulls"]:
        assert args[4:] == ["-f", "state=all", "-f", "head=intent-hq:" + state["branch"],
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
            assert flag("--base") == "main" and flag("--head") == state["branch"]
            state["pr"] = {
                "number": 12, "state": "open", "merged_at": None, "draft": False,
                "head": {"ref": state["branch"], "sha": remote_head(),
                         "repo": {"full_name": "intent-hq/intentd"}},
                "base": {"ref": "main", "repo": {"full_name": "intent-hq/intentd"}},
                "user": {"login": "pin-bot"}, "labels": [], "auto_merge": None,
            }
        else:
            assert args[2] == str(state["pr"]["number"])
        state["pr"]["title"] = flag("--title")
        state["pr"]["body"] = pathlib.Path(flag("--body-file")).read_text()
        state["writes"].append(args[1])
        save()
        print(state.get("pr_url", "https://github.com/intent-hq/intentd/pull/12"))
    elif args[:2] == ["pr", "merge"]:
        assert args[2] == str(state["pr"]["number"])
        assert args[3:5] == ["--repo", "intent-hq/intentd"]
        if args[5:] == ["--disable-auto"]:
            if state.get("disarm_error"): fail()
            state["pr"]["auto_merge"] = None
            state["writes"].append("disarm")
        else:
            assert args[5:8] == ["--squash", "--auto", "--match-head-commit"], args
            assert len(args) == 9 and args[8] == remote_head(), (args, remote_head())
            if state.get("arm_error"): fail()
            if state.get("arm_failures", 0) > 0:
                state["arm_failures"] -= 1
                fail()
            state["pr"]["auto_merge"] = {"enabled_by": {"login": "pin-bot"},
                                         "merge_method": "squash"}
            state["armed_heads"] = state.get("armed_heads", []) + [args[8]]
            state["writes"].append("arm")
        save()
    else:
        raise AssertionError("Unexpected gh operation: " + repr(args))
else:
    raise AssertionError(tool)
'''


class PinAutomationTests(unittest.TestCase):
    """The full rule suite, run against Codex (the original managed adapter)."""

    ADAPTER = "codex"
    META_KEY = "codex"
    PACKAGE = "@agentclientprotocol/codex-acp"
    BRANCH = "auto/codex-acp-pin"
    REGISTRY = "https://registry.npmjs.org/@agentclientprotocol%2fcodex-acp/latest"
    RELEASES = "https://github.com/agentclientprotocol/codex-acp/releases/tag/"
    PIN_TOKEN = "CODEX_ACP_NPX_PACKAGE"
    CLI_DEPENDENCY = "@openai/codex"
    MANIFEST_EXTRA = {}
    # None runs every test; a subclass may name the subset it repeats.
    ONLY = None

    @property
    def REF(self):
        return f"refs/heads/{self.BRANCH}"

    def fixture(self, version):
        other = {"codex": "2.2.2", "claude-code": "0.8.1"}
        text = FIXTURE.replace("<codex-version>",
                               version if self.ADAPTER == "codex" else other["codex"])
        return text.replace("<claude-version>",
                            version if self.ADAPTER == "claude-code" else other["claude-code"])

    def setUp(self):
        if self.ONLY is not None and self._testMethodName not in self.ONLY:
            self.skipTest(f"covered by the Codex suite; not repeated for {self.ADAPTER}")
        self.temp = tempfile.TemporaryDirectory(prefix="test-acp-pin-")
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
            "AUTO_PIN_ARM_RETRY_SECONDS": "0",
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
        self.pin_file.write_text(self.fixture("1.9.0"))
        metadata_path = self.repo / METADATA
        metadata_path.parent.mkdir(parents=True)
        self.metadata = {
            "codex": {"cliPackage":"@openai/codex", "cliCommand":"codex", "pinConstant":"CODEX_ACP_NPX_PACKAGE", "package":{"name":"@agentclientprotocol/codex-acp", "version":"1.9.0" if self.ADAPTER == "codex" else "2.2.2"}},
            "claude-code": {"cliPackage":"@anthropic-ai/claude-code", "cliCommand":"claude", "pinConstant":"CLAUDE_AGENT_ACP_NPX_PACKAGE", "package":{"name":"@agentclientprotocol/claude-agent-acp", "version":"1.9.0" if self.ADAPTER == "claude-code" else "0.8.1"}},
        }
        metadata_path.write_text(json.dumps(self.metadata))
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
            "manifest": {"name": self.PACKAGE, "version": "1.13.1",
                         "dependencies": {self.CLI_DEPENDENCY: "^0.159.1"},
                         **json.loads(json.dumps(self.MANIFEST_EXTRA))},
            "branch": self.BRANCH, "registry": self.REGISTRY,
            "remote": str(self.remote), "calls": [], "writes": [],
            "main_fetches": 0, "pr_lookups": 0, "pr": None,
        }

    def git(self, *args):
        return subprocess.check_output([REAL_GIT, "-C", str(self.repo), *args],
                                       env=self.env, text=True, stderr=subprocess.PIPE).strip()

    def head(self):
        refs = self.git("ls-remote", "--heads", "origin", self.REF)
        return refs.split()[0] if refs else None

    def invoke(self, *args, ok=True):
        self.state_path.write_text(json.dumps(self.state))
        result = subprocess.run([sys.executable, "-S", str(SCRIPT), "--adapter", self.ADAPTER,
                                 *args], cwd=self.repo,
                                env=self.env, text=True, capture_output=True, timeout=30)
        self.state = json.loads(self.state_path.read_text())
        output = result.stdout + result.stderr
        self.assertNotIn("offline-test-token-never-log", output)
        self.assertEqual(result.returncode, 0 if ok else 1, output)
        for call in self.state["calls"]:
            if call[0] == "gh":
                self.assertNotIn("--admin", call)
                if "merge" in call:
                    self.assertEqual(call[1:3], ["pr", "merge"])
                    self.assertTrue("--auto" in call or "--disable-auto" in call, call)
        return output

    def clear_calls(self):
        self.state.update(calls=[], writes=[], main_fetches=0, pr_lookups=0)

    def assert_no_merge_calls(self):
        self.assertFalse(any(call[:3] == ["gh", "pr", "merge"] for call in self.state["calls"]))

    def assert_no_publication(self):
        self.assertEqual(self.state["writes"], [])
        self.assertFalse(any(call[:2] == ["git", "push"] or "commit-tree" in call
                             for call in self.state["calls"]))

    def main_commit(self, version, other="Unrelated content\n"):
        self.pin_file.write_text(self.fixture(version))
        (self.repo / "other.txt").write_text(other)
        self.git("add", ".")
        self.git("commit", "-qm", "fix: another main change")
        commit = self.git("rev-parse", "HEAD")
        self.git("push", "-q", "origin", f"{commit}:refs/heads/fixture-live-main")
        return commit

    def test_stable_upgrade_changes_only_pin_and_adapter_metadata(self):
        self.invoke()
        head = self.head()
        self.assertEqual(self.git("show", f"{head}:{CONFIG}"),
                         self.fixture("1.13.1").strip())
        self.assertEqual(self.git("diff", "--name-only", self.base, head), "\n".join(sorted([CONFIG, METADATA])))
        metadata = json.loads(self.git("show", f"{head}:{METADATA}"))
        self.assertEqual(metadata[self.META_KEY]["package"]["version"], self.state["manifest"]["version"])
        self.assertEqual(metadata[self.META_KEY]["package"]["dependencies"], self.state["manifest"]["dependencies"])
        for key, entry in self.metadata.items():
            if key != self.META_KEY:
                self.assertEqual(metadata[key], entry)
        self.assertEqual(self.git("rev-parse", "HEAD"), self.base)
        self.assertEqual(self.git("status", "--porcelain"), "")
        self.assertEqual(self.state["writes"], ["create", "arm"])
        self.assertEqual(self.state["armed_heads"], [head])
        self.assertTrue(self.state["pr"]["title"].startswith("fix: "))
        self.assertIn(self.RELEASES + "v1.13.1",
                      self.state["pr"]["body"])
        self.assertIn(f"--force-with-lease={self.REF}:", next(
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
        self.assertEqual(len(self.state["armed_heads"]), 1)
        self.assertEqual(self.state["pr"]["number"], 12)
        self.assertIn("v1.14.0", self.state["pr"]["title"])

    def test_next_pin_refreshes_declared_cli_requirement_automatically(self):
        self.invoke()
        self.state["manifest"]["version"] = "1.14.0"
        self.state["manifest"]["dependencies"][self.CLI_DEPENDENCY] = "^0.160.0"
        self.invoke()
        metadata = json.loads(self.git("show", f"{self.head()}:{METADATA}"))
        self.assertEqual(metadata[self.META_KEY]["package"]["dependencies"][self.CLI_DEPENDENCY], "^0.160.0")

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
        self.assertEqual(self.state["writes"], ["disarm"])
        self.state["writes"] = []
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
        for text in (self.fixture("latest"),
                     self.fixture("1.9.0").replace(self.PIN_TOKEN, "REMOVED_PIN"),
                     self.fixture("1.9.0") * 2):
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
        self.git("push", "-q", "origin", f"HEAD:{self.REF}")
        human_head = self.head()
        self.clear_calls()
        self.state["manifest"]["version"] = "1.14.0"
        self.invoke(ok=False)
        self.assertEqual(self.head(), human_head)
        self.assertEqual(self.state["writes"], ["disarm"])
        self.state["writes"] = []
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
                self.assertEqual(self.state["writes"], ["disarm"])
                self.assertIsNone(self.state["pr"]["auto_merge"])
                self.state["writes"] = []
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
        self.assertEqual(self.state["writes"], ["create", "arm"])
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
        self.assertIn(f"--force-with-lease={self.REF}:{original}", push)

    def test_pr_creation_failure_can_recover_without_another_commit(self):
        self.state["pr_write_error"] = True
        self.invoke(ok=False)
        original = self.head()
        self.assertIsNotNone(original)
        self.clear_calls()
        del self.state["pr_write_error"]
        self.invoke()
        self.assertEqual(self.head(), original)
        self.assertEqual(self.state["writes"], ["create", "arm"])
        self.assertFalse(any("commit-tree" in call or call[:2] == ["git", "push"]
                             for call in self.state["calls"]))

    def test_failed_existing_pr_edit_recovers_without_commit_or_push(self):
        self.invoke()
        previous_head = self.head()
        self.state["manifest"]["version"] = "1.14.0"
        self.state["pr_write_error"] = True
        self.invoke(ok=False)
        pushed_head = self.head()
        self.assertNotEqual(pushed_head, previous_head)
        self.assertIn("v1.13.1", self.state["pr"]["title"])

        # A later workflow run starts in a fresh clone, without local recovery
        # state or the previous branch's now-unreachable commit.
        self.repo = self.directory / "retry-checkout"
        subprocess.run([REAL_GIT, "clone", "-q", "--no-local", "--branch", "main",
                        str(self.remote), str(self.repo)], env=self.env, check=True)
        self.git("remote", "set-url", "origin", ORIGIN)
        self.git("config", f"url.{self.remote}.insteadOf", ORIGIN)
        self.clear_calls()
        self.assertIsNotNone(self.state["pr"]["auto_merge"])
        output = self.invoke("--dry-run")
        self.assertIn("would repair PR metadata for 1.14.0; no publication", output)
        self.assertNotIn("arm auto-merge", output)
        self.assert_no_publication()
        self.clear_calls()
        self.invoke(ok=False)
        self.assertEqual(self.head(), pushed_head)
        self.assert_no_publication()
        self.clear_calls()
        del self.state["pr_write_error"]
        self.invoke()
        self.assertEqual(self.head(), pushed_head)
        self.assertEqual(self.state["writes"], ["edit"])
        self.assertIn("v1.14.0", self.state["pr"]["title"])
        self.assertIn("releases/tag/v1.14.0", self.state["pr"]["body"])
        self.assertFalse(any("commit-tree" in call or call[:2] == ["git", "push"]
                             for call in self.state["calls"]))

        self.clear_calls()
        self.invoke()
        self.assertEqual(self.head(), pushed_head)
        self.assert_no_publication()

    def test_recovery_preserves_human_edits_to_stale_metadata(self):
        self.invoke()
        self.state["manifest"]["version"] = "1.14.0"
        self.state["pr_write_error"] = True
        self.invoke(ok=False)
        pushed_head = self.head()
        stale = json.loads(json.dumps(self.state["pr"]))
        del self.state["pr_write_error"]
        for field, value in (("title", stale["title"] + " (human edit)"),
                             ("body", stale["body"] + "Human review notes\n"),
                             ("number", 99), ("user", {"login": "someone-else"}),
                             ("draft", True), ("labels", [{"name": "hold-release"}])):
            with self.subTest(field=field):
                self.clear_calls()
                self.state["pr"] = {**stale, field: value}
                self.invoke(ok=field == "labels")
                self.assertEqual(self.head(), pushed_head)
                self.assertEqual(self.state["pr"][field], value)
                self.assertEqual(self.state["writes"], ["disarm"])
                self.state["writes"] = []
                self.assert_no_publication()

    def test_recovery_rejects_unrecorded_generated_older_metadata(self):
        self.invoke()
        older_metadata = json.loads(json.dumps(self.state["pr"]))
        self.state["manifest"]["version"] = "1.14.0"
        self.invoke()
        self.state["manifest"]["version"] = "1.15.0"
        self.state["pr_write_error"] = True
        self.invoke(ok=False)
        recorded_metadata = json.loads(json.dumps(self.state["pr"]))
        del self.state["pr_write_error"]
        self.state["pr"] = older_metadata
        self.clear_calls()
        self.invoke(ok=False)
        self.assertEqual(self.state["writes"], ["disarm"])
        self.state["writes"] = []
        self.assert_no_publication()
        # Positive control: only the immediately recorded generated state
        # remains eligible for automatic repair on this branch commit.
        self.state["pr"] = recorded_metadata
        self.invoke()
        self.assertEqual(self.state["writes"], ["edit"])

    def test_failed_push_never_creates_a_pr(self):
        self.state["push_error"] = True
        self.invoke(ok=False)
        self.assertIsNone(self.head())
        self.assertEqual(self.state["writes"], [])

    def test_body_describes_auto_merge_and_hold(self):
        self.invoke()
        body = self.state["pr"]["body"]
        self.assertIn("auto-merge", body)
        self.assertIn("hold-release", body)
        self.assertNotIn("never merges", body)
        self.assertNotIn("human must approve", body)

    def test_unarmed_current_pr_is_armed_without_publication(self):
        self.invoke()
        head = self.head()
        self.state["pr"]["auto_merge"] = None
        self.clear_calls()
        self.assertIn("would arm auto-merge for #12", self.invoke("--dry-run"))
        self.assert_no_merge_calls()
        self.clear_calls()
        self.invoke()
        self.assertEqual(self.state["writes"], ["arm"])
        self.assertEqual(self.state["armed_heads"][-1], head)
        self.assertEqual(self.head(), head)
        self.assertFalse(any(call[:2] == ["git", "push"] or "commit-tree" in call
                             for call in self.state["calls"]))

    def test_already_armed_pr_makes_no_merge_call(self):
        self.invoke()
        self.clear_calls()
        self.invoke()
        self.assert_no_merge_calls()
        self.assert_no_publication()

    def test_pre_auto_merge_description_is_treated_as_a_human_edit(self):
        self.invoke()
        armed = json.loads(json.dumps(self.state["pr"]))
        head = self.head()
        old_policy = ("Normal PR CI must pass, and a human must approve the merge. "
                      "This automation never merges PRs.\n\n")
        old_body = armed["body"].split("This automation arms", 1)[0] + old_policy
        for target in ("1.13.1", "1.14.0"):
            with self.subTest(target=target):
                self.state["pr"] = {**armed, "body": old_body}
                self.state["manifest"]["version"] = target
                self.clear_calls()
                self.assertIn("unexpected ownership or edits", self.invoke(ok=False))
                self.assertEqual(self.state["writes"], ["disarm"])
                self.assertEqual(self.head(), head)

    def test_hold_release_on_unarmed_pr_makes_no_merge_call(self):
        self.invoke()
        self.state["pr"]["auto_merge"] = None
        self.state["pr"]["labels"] = [{"name": "hold-release"}]
        self.state["manifest"]["version"] = "1.14.0"
        self.clear_calls()
        self.assertIn("hold-release", self.invoke())
        self.assert_no_merge_calls()
        self.assert_no_publication()

    def test_hold_release_disarms_even_when_a_human_armed_it(self):
        self.invoke()
        self.state["pr"]["auto_merge"] = {"enabled_by": {"login": "a-human"}}
        self.state["pr"]["labels"] = [{"name": "hold-release"}]
        self.state["manifest"]["version"] = "1.14.0"
        self.clear_calls()
        self.assertIn("would disable auto-merge", self.invoke("--dry-run"))
        self.assert_no_merge_calls()
        self.clear_calls()
        self.invoke()
        self.assertEqual(self.state["writes"], ["disarm"])

    def test_human_edit_keeps_a_humans_own_arming(self):
        self.invoke()
        self.state["pr"]["title"] = "My proposal"
        self.state["pr"]["auto_merge"] = {"enabled_by": {"login": "a-human"}}
        self.state["manifest"]["version"] = "1.14.0"
        self.clear_calls()
        self.invoke(ok=False)
        self.assert_no_merge_calls()
        self.assert_no_publication()

    def test_human_edit_in_dry_run_does_not_disarm(self):
        self.invoke()
        self.state["pr"]["title"] = "My proposal"
        self.state["manifest"]["version"] = "1.14.0"
        self.clear_calls()
        self.invoke("--dry-run", ok=False)
        self.assert_no_merge_calls()
        self.assertIsNotNone(self.state["pr"]["auto_merge"])

    def test_disarm_failure_still_refuses_and_preserves_the_pr(self):
        self.invoke()
        self.state["pr"]["title"] = "My proposal"
        self.state["disarm_error"] = True
        self.state["manifest"]["version"] = "1.14.0"
        old = self.head()
        self.clear_calls()
        self.assertIn("Could not disable auto-merge", self.invoke(ok=False))
        self.assertEqual(self.head(), old)
        self.assert_no_publication()

    def test_dry_run_reports_arming_without_merge_calls(self):
        self.assertIn("and arm auto-merge", self.invoke("--dry-run"))
        self.assert_no_merge_calls()

    def test_arm_failure_warns_and_succeeds_then_next_run_arms(self):
        self.state["arm_error"] = True
        output = self.invoke()
        self.assertIn("Could not arm auto-merge for #12", output)
        self.assertEqual(self.state["writes"], ["create"])
        self.assertIsNone(self.state["pr"]["auto_merge"])
        head = self.head()
        del self.state["arm_error"]
        self.clear_calls()
        self.invoke()
        self.assertEqual(self.state["writes"], ["arm"])
        self.assertEqual(self.state["armed_heads"][-1], head)

    def test_unexpected_pr_url_skips_arming_until_next_run(self):
        self.state["pr_url"] = "https://example.com/not-a-pr"
        self.assertIn("Unexpected PR URL", self.invoke())
        self.assertEqual(self.state["writes"], ["create"])
        del self.state["pr_url"]
        self.clear_calls()
        self.invoke()
        self.assertEqual(self.state["writes"], ["arm"])

    def test_failed_push_or_pr_write_never_arms(self):
        for flag in ("push_error", "pr_write_error"):
            with self.subTest(flag=flag):
                self.state[flag] = True
                self.invoke(ok=False)
                self.assert_no_merge_calls()
                del self.state[flag]

    def test_transient_arm_failure_is_retried_in_the_same_run(self):
        self.state["arm_failures"] = 2
        self.invoke()
        self.assertEqual(self.state["writes"], ["create", "arm"])
        self.assertEqual(self.state["armed_heads"], [self.head()])

    def test_bot_push_never_overrides_an_existing_arming(self):
        self.invoke()
        self.state["pr"]["auto_merge"] = {"enabled_by": {"login": "a-human"},
                                          "merge_method": "rebase"}
        self.state["manifest"]["version"] = "1.14.0"
        self.clear_calls()
        self.invoke()
        self.assertEqual(self.state["writes"], ["edit"])
        self.assertEqual(self.state["pr"]["auto_merge"]["merge_method"], "rebase")

    def test_npm_latest_not_newer_than_main_disarms_open_pr(self):
        self.invoke()
        self.state["manifest"]["version"] = "1.9.0"
        self.clear_calls()
        self.assertIn("would disable auto-merge", self.invoke("--dry-run"))
        self.assert_no_merge_calls()
        self.clear_calls()
        self.assertIn("nothing to do", self.invoke())
        self.assertEqual(self.state["writes"], ["disarm"])
        self.clear_calls()
        self.invoke()
        self.assert_no_merge_calls()

    def test_npm_latest_not_newer_keeps_a_humans_arming_and_tolerates_lookup_errors(self):
        self.invoke()
        self.state["pr"]["auto_merge"] = {"enabled_by": {"login": "a-human"}}
        self.state["manifest"]["version"] = "1.9.0"
        self.clear_calls()
        self.invoke()
        self.assert_no_merge_calls()
        self.state["pr"]["state"] = "closed"
        self.clear_calls()
        self.assertIn("nothing to do", self.invoke())

    def test_human_branch_push_dry_run_reports_disarm(self):
        self.invoke()
        self.git("checkout", "-q", "--detach", self.head())
        (self.repo / "other.txt").write_text("Human change\n")
        self.git("commit", "-qam", "fix: human work")
        self.git("push", "-q", "origin", f"HEAD:{self.REF}")
        self.clear_calls()
        self.assertIn("would disable auto-merge for #12 (human branch changes)",
                      self.invoke("--dry-run", ok=False))
        self.assert_no_merge_calls()

    def test_human_metadata_dry_run_reports_disarm(self):
        self.invoke()
        self.state["pr"]["title"] = "My proposal"
        self.state["manifest"]["version"] = "1.14.0"
        self.clear_calls()
        self.assertIn("would disable auto-merge for #12 (human edits)",
                      self.invoke("--dry-run", ok=False))

    def test_checked_out_human_changes_and_index_are_untouched(self):
        self.pin_file.write_text("Uncommitted user work\n")
        (self.repo / "other.txt").write_text("Staged user work\n")
        self.git("add", "other.txt")
        status = self.git("status", "--porcelain")
        self.invoke()
        self.assertEqual(self.pin_file.read_text(), "Uncommitted user work\n")
        self.assertEqual(self.git("status", "--porcelain"), status)
        self.assertEqual(self.git("show", ":other.txt"), "Staged user work")


    def test_codex_generated_text_is_byte_identical_to_the_original_automation(self):
        # Open rolling PRs and commits from before the generalization must
        # still verify as the automation's own; any drift would disarm them.
        if self.ADAPTER != "codex":
            self.skipTest("Codex-only compatibility golden")
        self.invoke()
        head = self.head()
        self.assertEqual(self.state["pr"]["title"], "fix: bump Codex ACP fallback to v1.13.1")
        self.assertTrue(self.state["pr"]["body"].startswith(
            "<!-- auto-pin-codex-acp: v1 -->\n"
            "Update `@agentclientprotocol/codex-acp` from `1.9.0` to `1.13.1` in "
            "`CODEX_ACP_NPX_PACKAGE`. The fallback stays pinned to an exact version.\n\n"
            "[Upstream release v1.13.1]"
            "(https://github.com/agentclientprotocol/codex-acp/releases/tag/v1.13.1).\n\n"
            "This automation arms squash auto-merge"))
        message = self.git("show", "-s", "--format=%B", head).splitlines()
        self.assertEqual(message[:4], ["fix: bump Codex ACP fallback to v1.13.1", "",
                                       "Managed Codex ACP pin: 1.9.0 -> 1.13.1.",
                                       "Auto-pin-Codex-ACP: v1"])
        self.assertEqual(self.git("show", "-s", "--format=%an <%ae>", head),
                         "github-actions[bot] <41898282+github-actions[bot]@users.noreply.github.com>")


class ClaudeAgentAcpPinTests(PinAutomationTests):
    """claude-code: the same rules on its own branch, plus its extra guards."""

    ADAPTER = "claude-code"
    META_KEY = "claude-code"
    PACKAGE = "@agentclientprotocol/claude-agent-acp"
    BRANCH = "auto/claude-agent-acp-pin"
    REGISTRY = "https://registry.npmjs.org/@agentclientprotocol%2fclaude-agent-acp/latest"
    RELEASES = "https://github.com/agentclientprotocol/claude-agent-acp/releases/tag/"
    PIN_TOKEN = "claude_agent_acp_version"
    CLI_DEPENDENCY = "@anthropic-ai/claude-agent-sdk"
    MANIFEST_EXTRA = {"engines": {"node": ">=22"}}
    # The shared rules are adapter-independent code; repeat the ones that
    # exercise adapter-specific values (pin, metadata key, branch, PR text).
    ONLY = {
        "test_stable_upgrade_changes_only_pin_and_adapter_metadata",
        "test_numeric_version_ordering",
        "test_equal_older_and_numeric_older_versions_are_noops",
        "test_existing_pr_updates_on_the_same_branch",
        "test_next_pin_refreshes_declared_cli_requirement_automatically",
        "test_repeat_run_does_not_create_commits_push_or_edit_pr",
        "test_existing_newer_proposal_is_never_downgraded",
        "test_wrong_package_is_rejected",
        "test_invalid_or_duplicate_pin_is_rejected",
        "test_human_branch_commit_is_preserved",
        "test_human_pr_metadata_and_hold_label_are_preserved",
        "test_failed_existing_pr_edit_recovers_without_commit_or_push",
        "test_body_describes_auto_merge_and_hold",
        "test_npm_latest_not_newer_than_main_disarms_open_pr",
        "test_checked_out_human_changes_and_index_are_untouched",
        "test_claude_generated_text_names_its_own_adapter",
        "test_engines_node_change_refuses_before_lookup_or_publication",
        "test_engines_node_change_leaves_the_open_proposal_unchanged",
        "test_codex_rolling_pr_is_never_touched",
    }

    def test_claude_generated_text_names_its_own_adapter(self):
        self.invoke()
        head = self.head()
        self.assertEqual(self.state["pr"]["title"], "fix: bump claude-agent-acp pin to v1.13.1")
        body = self.state["pr"]["body"]
        self.assertTrue(body.startswith("<!-- auto-pin-claude-agent-acp: v1 -->\n"), body)
        self.assertIn("`@agentclientprotocol/claude-agent-acp` from `1.9.0` to `1.13.1`", body)
        self.assertIn("`claude_agent_acp_version!`", body)
        self.assertNotIn("codex", body.lower())
        message = self.git("show", "-s", "--format=%B", head).splitlines()
        self.assertEqual(message[:4], ["fix: bump claude-agent-acp pin to v1.13.1", "",
                                       "Managed claude-agent-acp pin: 1.9.0 -> 1.13.1.",
                                       "Auto-pin-Claude-Agent-ACP: v1"])
        # Only the macro literal changes; the concat! spec follows from it.
        diff = self.git("diff", "-U0", self.base, head, "--", CONFIG)
        self.assertEqual([line for line in diff.splitlines()
                          if line[:1] in "+-" and line[:3] not in ("+++", "---")],
                         ['-        "1.9.0"', '+        "1.13.1"'])

    def test_engines_node_change_refuses_before_lookup_or_publication(self):
        for engines in ({"node": ">=24"}, {"node": ">=22.5"}, {}, None, "node >=22"):
            for args in ((), ("--dry-run",)):
                with self.subTest(engines=engines, args=args):
                    self.clear_calls()
                    if engines is None:
                        self.state["manifest"].pop("engines", None)
                    else:
                        self.state["manifest"]["engines"] = engines
                    self.assertIn("engines.node", self.invoke(*args, ok=False))
                    self.assert_no_publication()
                    self.assertFalse(any(call[0] == "gh" for call in self.state["calls"]))
                    self.assertIsNone(self.head())

    def test_engines_node_change_leaves_the_open_proposal_unchanged(self):
        self.invoke()
        head = self.head()
        self.clear_calls()
        self.state["manifest"].update(version="1.14.0", engines={"node": ">=24"})
        self.invoke(ok=False)
        self.assertEqual(self.head(), head)
        self.assertIn("v1.13.1", self.state["pr"]["title"])
        self.assert_no_publication()
        self.assert_no_merge_calls()

    def test_codex_rolling_pr_is_never_touched(self):
        codex = {
            "number": 7, "state": "open", "merged_at": None, "draft": False,
            "title": "fix: bump Codex ACP fallback to v9.9.9", "body": "codex",
            "head": {"ref": "auto/codex-acp-pin", "sha": "0" * 40,
                     "repo": {"full_name": REPO}},
            "base": {"ref": "main", "repo": {"full_name": REPO}},
            "user": {"login": "pin-bot"}, "labels": [{"name": "hold-release"}],
            "auto_merge": {"enabled_by": {"login": "pin-bot"}},
        }
        self.state["extra_prs"] = [codex]
        self.invoke()
        self.assertEqual(self.state["writes"], ["create", "arm"])
        self.assertEqual(self.state["pr"]["number"], 12)
        self.assertEqual(self.state["extra_prs"], [codex])
        self.assertIsNone(self.git("ls-remote", "--heads", "origin",
                                   "refs/heads/auto/codex-acp-pin") or None)
        for call in self.state["calls"]:
            self.assertNotIn("auto/codex-acp-pin", " ".join(call))


class CliAndWorkflowTests(unittest.TestCase):
    def test_adapter_is_required_and_restricted(self):
        for args in ([], ["--adapter", "pi"], ["--adapter", "unknown"]):
            with self.subTest(args=args):
                result = subprocess.run([sys.executable, "-S", str(SCRIPT), "--dry-run", *args],
                                        text=True, capture_output=True, timeout=30,
                                        env={**os.environ, "PATH": "/nonexistent"})
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn("--adapter", result.stderr)

    def test_workflow_pins_each_adapter_from_trusted_main(self):
        text = WORKFLOW.read_text()
        self.assertFalse((ROOT / ".github/workflows/auto-pin-codex-acp.yml").exists())
        pin = text.split("\n  pin:\n", 1)[1].split("\n  stand-down:\n", 1)[0]
        for needle in ("github.event_name != 'pull_request'",
                       "github.repository == 'intent-hq/intentd' && github.ref == 'refs/heads/main'",
                       "fail-fast: false", "adapter: [codex, claude-code]",
                       "ref: main", "secrets.RELEASE_PLZ_TOKEN",
                       'scripts/auto-pin-acp-adapter.py --adapter "$ADAPTER"'):
            self.assertIn(needle, pin)

    def test_workflow_stands_down_on_hold_or_foreign_change_without_secrets(self):
        text = WORKFLOW.read_text()
        self.assertIn("types: [labeled, synchronize, edited]", text)
        route = text.split("\n  runner-route:\n", 1)[1].split("\n  pin:\n", 1)[0]
        job = text.split("\n  stand-down:\n", 1)[1]
        for branch in ("auto/codex-acp-pin", "auto/claude-agent-acp-pin"):
            self.assertIn(f"github.event.pull_request.head.ref == '{branch}'", route)
            self.assertIn(f"github.event.pull_request.head.ref == '{branch}'", job)
        for needle in ("github.event.pull_request.head.repo.full_name == github.repository",
                       "github.event.label.name == 'hold-release'",
                       "github.actor != github.event.pull_request.user.login",
                       "GH_TOKEN: ${{ github.token }}", "--disable-auto"):
            self.assertIn(needle, job)
        self.assertNotIn("secrets.", job)
        self.assertNotIn("scripts/", job)


if __name__ == "__main__":
    unittest.main(verbosity=2)
