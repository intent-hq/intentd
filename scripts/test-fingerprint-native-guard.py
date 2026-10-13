#!/usr/bin/env python3
"""Lightweight functional tests for the temporary diagnostic resource guard."""

import importlib.util
import json
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "guard", Path(__file__).with_name("fingerprint-native-guard.py"))
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


class GuardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def run_child(self, code, *, seconds=2, free=None):
        with patch.object(guard, "free_bytes", side_effect=free,
                          return_value=60 * guard.GIB):
            return guard.supervise([sys.executable, "-c", code], self.root,
                                   self.root, seconds, interval=0.01)

    def test_preserves_success_and_failure_and_output(self):
        self.assertEqual(self.run_child("print('retained'); exit(7)"), 7)
        self.assertEqual((self.root / "native-triage.log").read_text(), "retained\n")
        self.assertEqual(self.run_child("exit(0)"), 0)

    def test_low_initial_disk_does_not_launch(self):
        with patch.object(guard, "free_bytes", return_value=49 * guard.GIB), \
                patch.object(guard.subprocess, "Popen") as launch:
            with self.assertRaises(RuntimeError):
                guard.supervise(["unused"], self.root, self.root, 1)
            launch.assert_not_called()

    def test_runtime_limit_stops_child_and_retains_reason(self):
        self.assertEqual(self.run_child("import time; time.sleep(30)", seconds=0.03), 124)
        result = json.loads((self.root / "supervisor-result.json").read_text())
        self.assertEqual(result["reason"], "runtime-limit")

    def test_disk_limit_stops_child_and_retains_reason(self):
        self.assertEqual(self.run_child("import time; time.sleep(30)",
                                       free=[60 * guard.GIB, 9 * guard.GIB]), 125)
        result = json.loads((self.root / "supervisor-result.json").read_text())
        self.assertEqual(result["reason"], "disk-below-10-GiB")

    def test_cleanup_includes_group_when_leader_has_exited(self):
        from unittest.mock import Mock
        child = Mock(pid=123456, returncode=0)
        with patch.object(guard.os, "killpg") as kill:
            guard.stop_group(child)
        self.assertEqual(kill.call_args_list[0].args, (123456, guard.signal.SIGTERM))
        self.assertEqual(kill.call_args_list[1].args, (123456, guard.signal.SIGKILL))

    def test_pinned_nextest_accepts_multiline_version_and_rejects_wrong_version(self):
        script = Path(__file__).with_name("fingerprint-native-run.sh").read_text()
        assertion = next(line for line in script.splitlines()
                         if line.startswith('test "$(cargo nextest --version'))
        for version, expected in [("0.9.143", 0), ("0.9.144", 1)]:
            output = (f"cargo-nextest {version} (60fa45f63 2026-08-04)\n"
                      f"release: {version}\ncommit-hash: 60fa45f63\n"
                      "commit-date: 2026-08-04\nhost: x86_64-unknown-linux-gnu\n")
            command = "cargo() { printf '%s' " + shlex.quote(output) + "; }; " + assertion
            result = subprocess.run(["bash", "-euo", "pipefail", "-c", command], check=False)
            self.assertEqual(result.returncode, expected, version)

    def test_exact_filter_and_receipt_membership(self):
        targets = json.loads(Path(__file__).with_name("fingerprint-native-targets.json").read_text())
        self.assertEqual(len(targets), 13)
        self.assertIn("binary_id(=intent-js::guarded_host)", guard.selection(targets))
        with self.assertRaises(ValueError):
            guard.selection(targets[:-1])
        suites = {}
        for binary, name in targets:
            suite = suites.setdefault(binary, {"testcases": {}})
            suite["testcases"][name] = {"ignored": False, "filter-match": {"status": "matches"}}
        listing = self.root / "gate-runs" / "attempt" / "listing-1.json"
        listing.parent.mkdir(parents=True)
        listing.write_text(json.dumps({"rust-suites": suites}))
        guard.verify_listing(self.root, targets)
        binary, name = targets[0]
        suites[binary]["testcases"][name]["ignored"] = True
        listing.write_text(json.dumps({"rust-suites": suites}))
        with self.assertRaises(RuntimeError):
            guard.verify_listing(self.root, targets)


if __name__ == "__main__":
    unittest.main()
