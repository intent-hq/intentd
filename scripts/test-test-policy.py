#!/usr/bin/env python3
"""Fast child-process contracts for the test-only caller policy (no Rust build)."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


SCRIPTS = Path(__file__).resolve().parent
WRAPPER = SCRIPTS / "with-test-policy.sh"


class PolicyTests(unittest.TestCase):
    def test_wrapper_preserves_process_contract(self):
        for inherited in (None, "", "0", "false", "1"):
            with self.subTest(inherited=inherited):
                env = dict(os.environ)
                env.pop("INTENTD_ASSERT_BOUND_CALLER", None)
                if inherited is not None:
                    env["INTENTD_ASSERT_BOUND_CALLER"] = inherited
                env["INTENTD_TEST_TIMEOUT_MULTIPLIER"] = "7"
                code = (
                    "import json,os,sys; "
                    "print(json.dumps([os.getcwd(),sys.argv[1:],sys.stdin.read(),"
                    "os.environ['INTENTD_ASSERT_BOUND_CALLER'],"
                    "os.environ['INTENTD_TEST_TIMEOUT_MULTIPLIER']])); "
                    "print('child stderr',file=sys.stderr); sys.exit(37)"
                )
                result = subprocess.run(
                    ["bash", str(WRAPPER), sys.executable, "-S", "-c", code, "two words", "", "*", "a'b"],
                    input="caller input\n", text=True, capture_output=True, env=env,
                    cwd=SCRIPTS, timeout=5,
                )
                self.assertEqual(result.returncode, 37, result.stderr)
                self.assertEqual(result.stderr, "child stderr\n")
                self.assertEqual(json.loads(result.stdout), [str(SCRIPTS), ["two words", "", "*", "a'b"], "caller input\n", "1", "7"])

    def test_exact_child_can_remove_guard(self):
        code = (
            "import os,subprocess,sys; "
            "assert os.environ['INTENTD_ASSERT_BOUND_CALLER']=='1'; "
            "env=dict(os.environ); env.pop('INTENTD_ASSERT_BOUND_CALLER'); "
            "subprocess.run([sys.executable,'-S','-c',"
            "\"import os; assert 'INTENTD_ASSERT_BOUND_CALLER' not in os.environ\"],env=env,check=True)"
        )
        result = subprocess.run(["bash", str(WRAPPER), sys.executable, "-S", "-c", code], capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_wrapper_does_not_change_parent_or_later_launches(self):
        code = 'bash "$1" sh -c \'test "$INTENTD_ASSERT_BOUND_CALLER" = 1\' && sh -c \'test "$INTENTD_ASSERT_BOUND_CALLER" = 0\''
        result = subprocess.run(["bash", "-c", code, "test", str(WRAPPER)], env={**os.environ, "INTENTD_ASSERT_BOUND_CALLER": "0"}, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_wrapper_requires_command(self):
        result = subprocess.run(["bash", str(WRAPPER)], capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 2)
        self.assertIn("Usage:", result.stderr)

    def test_full_coverage_children(self):
        for script in ("coverage-e2e.sh", "coverage-all.sh"):
            for inherited in (None, "0", "false", "1"):
                for child_exit in (0, 42):
                    with self.subTest(script=script, inherited=inherited, child_exit=child_exit), tempfile.TemporaryDirectory() as tmp:
                        root = Path(tmp)
                        scripts, bin_dir = root / "scripts", root / "bin"
                        scripts.mkdir()
                        bin_dir.mkdir()
                        shutil.copy(SCRIPTS / script, scripts)
                        if WRAPPER.exists():
                            shutil.copy(WRAPPER, scripts)
                        cargo = bin_dir / "cargo"
                        cargo.write_text(
                            f"#!{sys.executable} -S\n"
                            "import json,os,sys\n"
                            "with open(os.environ['CHILD_LOG'],'a') as f:\n"
                            " f.write(json.dumps([sys.argv[1:],os.environ.get('INTENTD_ASSERT_BOUND_CALLER'),os.environ.get('INTENTD_TEST_TIMEOUT_MULTIPLIER')])+'\\n')\n"
                            "if 'nextest' in sys.argv: sys.exit(int(os.environ['CHILD_EXIT']))\n"
                            "print('TOTAL 100%')\n"
                        )
                        cargo.chmod(0o755)
                        for name in ("cargo-llvm-cov", "cargo-nextest", "rustup"):
                            path = bin_dir / name
                            path.write_text("#!/bin/sh\necho llvm-tools\n")
                            path.chmod(0o755)
                        env = {**os.environ, "PATH": str(bin_dir) + os.pathsep + os.environ["PATH"], "CHILD_LOG": str(root / "children.jsonl"), "CHILD_EXIT": str(child_exit), "GENERATE_LCOV": "1", "INTENTD_TEST_TIMEOUT_MULTIPLIER": "9"}
                        env.pop("INTENTD_ASSERT_BOUND_CALLER", None)
                        if inherited is not None:
                            env["INTENTD_ASSERT_BOUND_CALLER"] = inherited
                        result = subprocess.run(["bash", str(scripts / script), "40"], capture_output=True, text=True, env=env, timeout=5)
                        self.assertEqual(result.returncode, child_exit, result.stderr)
                        calls = [json.loads(line) for line in (root / "children.jsonl").read_text().splitlines()]
                        tests = [call for call in calls if "nextest" in call[0]]
                        self.assertEqual(len(tests), 1)
                        self.assertEqual(tests[0][1:], ["1", "3"])
                        expected = ["llvm-cov", "--no-report", "nextest"]
                        expected += (["--workspace"] if script == "coverage-all.sh" else ["-p", "intentd", "-E", "kind(test) and not binary(intentd) and not binary(auggie_context_e2e)"])
                        self.assertEqual(tests[0][0], expected)
                        for args, policy, multiplier in calls:
                            if "nextest" not in args:
                                self.assertEqual([policy, multiplier], [inherited, "9"])
                        if child_exit:
                            self.assertEqual(len(calls), 2, "failed test must prevent reports")
                        else:
                            self.assertIn(["llvm-cov", "report", "--lcov", "--output-path", "lcov.info"], [call[0] for call in calls])
                            self.assertEqual(calls[-1][0], ["llvm-cov", "report", "--summary-only", "--fail-under-lines", "40"])


if __name__ == "__main__":
    unittest.main()
