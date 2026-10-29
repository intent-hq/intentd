#!/usr/bin/env python3
"""Offline tests for scripts/home-intent-guard.py."""

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent / "home-intent-guard.py"


def run(action, state, home):
    return subprocess.run(
        [sys.executable, "-I", "-B", str(SCRIPT), action, str(state), "--home", str(home)],
        capture_output=True, text=True, check=False)


class HomeIntentGuardTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="home-intent-guard-")
        self.home = Path(self._tmp.name) / "home"
        self.intent = self.home / "intent"
        (self.intent / "workspaces" / "ws" / "repo").mkdir(parents=True)
        (self.intent / "logs").mkdir()
        (self.intent / ".secrets.json").write_text("{}")
        self.state = Path(self._tmp.name) / "state.json"

    def tearDown(self):
        self._tmp.cleanup()

    def snapshot(self):
        result = run("snapshot", self.state, self.home)
        self.assertEqual(result.returncode, 0, result.stderr)

    def assert_flags(self, needle):
        result = run("verify", self.state, self.home)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn(needle, result.stderr)

    def test_untouched_home_passes(self):
        self.snapshot()
        result = run("verify", self.state, self.home)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_checkout_contents_are_ignored(self):
        self.snapshot()
        (self.intent / "workspaces" / "ws" / "repo" / "target").mkdir()
        (self.intent / "workspaces" / "ws" / "repo" / "target" / "x").write_text("build")
        result = run("verify", self.state, self.home)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_secrets_write_is_flagged(self):
        self.snapshot()
        path = self.intent / ".secrets.json"
        path.write_text('{"test-only": "leak"}')
        stat = path.stat()
        os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
        self.assert_flags("modified .secrets.json")

    def test_created_collaboration_store_is_flagged(self):
        self.snapshot()
        (self.intent / ".secrets.json.collaboration").mkdir()
        self.assert_flags("created  .secrets.json.collaboration")

    def test_legacy_migration_is_flagged(self):
        (self.intent / "secrets.json").write_text("{}")
        self.snapshot()
        (self.intent / "secrets.json").unlink()
        self.assert_flags("removed  secrets.json")

    def test_new_workspace_dir_is_flagged(self):
        self.snapshot()
        (self.intent / "workspaces" / "leaked-ws").mkdir()
        self.assert_flags("created  workspaces/leaked-ws")

    def test_creating_missing_intent_dir_is_flagged(self):
        empty = Path(self._tmp.name) / "empty-home"
        empty.mkdir()
        self.assertEqual(run("snapshot", self.state, empty).returncode, 0)
        (empty / "intent").mkdir()
        result = run("verify", self.state, empty)
        self.assertEqual(result.returncode, 1)
        self.assertIn("created  .", result.stderr)


if __name__ == "__main__":
    unittest.main()
