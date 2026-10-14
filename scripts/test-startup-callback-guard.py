#!/usr/bin/env python3
"""Lightweight functional tests for the temporary diagnostic resource guard."""

import importlib.util
import copy
import json
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "guard", Path(__file__).with_name("startup-callback-guard.py"))
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
        self.assertEqual((self.root / "native-validation.log").read_text(), "retained\n")
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
        script = Path(__file__).with_name("startup-callback-run.sh").read_text()
        assertion = next(line for line in script.splitlines()
                         if line.startswith('test "$(cargo nextest --version'))
        for version, expected in [("0.9.148", 0), ("0.9.149", 1)]:
            output = (f"cargo-nextest {version} (60fa45f63 2026-08-04)\n"
                      f"release: {version}\ncommit-hash: 60fa45f63\n"
                      "commit-date: 2026-08-04\nhost: x86_64-unknown-linux-gnu\n")
            command = "cargo() { printf '%s' " + shlex.quote(output) + "; }; " + assertion
            result = subprocess.run(["bash", "-euo", "pipefail", "-c", command], check=False)
            self.assertEqual(result.returncode, expected, version)

    def write_selection(self, names):
        (self.root / "expected-test.txt").write_text("expected")
        suites = {str(i): {"testcases": {name: {"filter-match": {"status": "matches"}}}}
                  for i, name in enumerate(names)}
        (self.root / "listing.json").write_text(json.dumps({"rust-suites": suites}))

    def test_selection_rejects_absent_wrong_duplicate_and_extra(self):
        self.write_selection(["expected"])
        guard.verify_selection(self.root)
        for names in ([], ["wrong"], ["expected", "extra"], ["expected", "expected"]):
            self.write_selection(names)
            with self.assertRaises(RuntimeError):
                guard.verify_selection(self.root)

    def test_result_requires_single_fresh_pass_and_exit_zero(self):
        self.write_selection(["expected"])
        (self.root / "test-exit-code.txt").write_text("0")
        def save(cases):
            if cases == '<testcase name="expected"/>':
                cases = '<testcase name="expected"><system-err>' + self.stage_lines() + '</system-err></testcase>'
            (self.root / "junit.xml").write_text("<testsuites><testsuite>" + cases + "</testsuite></testsuites>")
        save('<testcase name="expected"/>')
        guard.verify_result(self.root)
        for tag in ("failure", "error", "skipped", "rerunFailure", "rerunError", "flakyFailure", "flakyError"):
            save(f'<testcase name="expected"><{tag}/></testcase>')
            with self.assertRaises(RuntimeError):
                guard.verify_result(self.root)
        for cases in ('', '<testcase name="wrong"/>', '<testcase name="expected"/><testcase name="expected"/>'):
            save(cases)
            with self.assertRaises(RuntimeError):
                guard.verify_result(self.root)
        save('<testcase name="expected"/>')
        (self.root / "test-exit-code.txt").write_text("101")
        with self.assertRaises(RuntimeError):
            guard.verify_result(self.root)

    def test_result_rejects_missing_admission_stage_evidence(self):
        self.write_selection(["expected"])
        (self.root / "test-exit-code.txt").write_text("0")
        (self.root / "junit.xml").write_text(
            '<testsuites><testsuite><testcase name="expected"/></testsuite></testsuites>')
        with self.assertRaisesRegex(RuntimeError, "stage evidence"):
            guard.verify_result(self.root)

    def stage_traces(self, path="admit_optional"):
        stages = [{"path": path, "stage": stage, "records": 64, "elapsed_ms": elapsed}
                  for stage, elapsed in zip(
                      ["retain-start", "retain-complete", "original-start", "original-complete"],
                      [0, 120, 121, 450])]
        return [{"subcase": "mixed-alias-budget", "iterations": count,
                 "returned": True, "unwinding": False, "stages": stages if count == 63 else []}
                for count in [63, 64]]

    def stage_lines(self, traces=None):
        return "\n".join("budget-admission-trace " + json.dumps(t)
                         for t in (self.stage_traces() if traces is None else traces))

    def test_both_paths_report_actual_path_and_separate_stage_costs(self):
        for path in ("admit", "admit_optional"):
            result = guard.verify_stages(self.stage_lines(self.stage_traces(path)))
            self.assertEqual(result["path"], path)
            self.assertEqual(result["fixture_validation_ms"], 120)
            self.assertEqual(result["original_admission_ms"], 329)

    def test_rejects_partial_duplicate_wrong_path_or_malformed_stage_evidence(self):
        valid = self.stage_traces()
        bad = [[], valid[:1], valid + valid, list(reversed(valid))]
        for field, value in [("path", "unknown"), ("records", 63),
                             ("stage", "original-start"), ("elapsed_ms", -1),
                             ("elapsed_ms", True), ("elapsed_ms", 500)]:
            traces = copy.deepcopy(valid)
            traces[0]["stages"][1][field] = value
            bad.append(traces)
        for path in ("admit", "admit_optional"):
            traces = copy.deepcopy(valid)
            traces[0]["stages"][0]["path"] = path
            traces[0]["stages"][1]["path"] = "admit" if path == "admit_optional" else "admit_optional"
            bad.append(traces)
        for kind in ("partial", "unexpected-quota-admission", "unwinding", "not-returned"):
            traces = copy.deepcopy(valid)
            if kind == "partial":
                traces[0]["stages"].pop()
            elif kind == "unexpected-quota-admission":
                traces[1]["stages"] = traces[0]["stages"]
            elif kind == "unwinding":
                traces[0]["unwinding"] = True
            else:
                traces[0]["returned"] = False
            bad.append(traces)
        for traces in bad:
            with self.subTest(traces=traces), self.assertRaises(RuntimeError):
                guard.verify_stages(self.stage_lines(traces))


if __name__ == "__main__":
    unittest.main()
