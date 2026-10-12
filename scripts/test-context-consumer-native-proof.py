#!/usr/bin/env python3
"""Offline regressions for native context discovery, execution and evidence."""
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("proof", Path(__file__).with_name("test-context-consumer-native.py"))
proof = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proof)
NAMES = sorted(proof.REQUIRED)
LISTING = "\n".join(name + ": test" for name in NAMES) + "\n"
EXECUTION = "\n".join("test " + name + " ... ok" for name in NAMES) + (
    f"\ntest result: ok. {len(NAMES)} passed; 0 failed; 0 ignored; 100 filtered out\n"
)


class ProofTests(unittest.TestCase):
    def test_every_required_behavior_must_be_discovered(self):
        self.assertEqual(proof.discover(LISTING), NAMES)
        for name in NAMES:
            with self.subTest(missing=name), self.assertRaises(RuntimeError):
                proof.discover(LISTING.replace(name + ": test\n", ""))
        for output in ("", LISTING + LISTING, LISTING + "foreign: test\n"):
            with self.subTest(output=output), self.assertRaises(RuntimeError):
                proof.discover(output)

    def test_new_selected_tests_also_require_execution(self):
        extra = proof.PREFIX + "future_regression"
        names = proof.discover(LISTING + extra + ": test\n")
        with self.assertRaises(RuntimeError):
            proof.require_execution(EXECUTION, names)

    def test_zero_missing_duplicate_failed_and_ignored_cannot_pass(self):
        proof.require_execution(EXECUTION, NAMES)
        for output in (
            "test result: ok. 0 passed; 0 failed; 0 ignored; 9 filtered out\n",
            EXECUTION.replace("test " + NAMES[0] + " ... ok\n", ""),
            EXECUTION + EXECUTION,
            EXECUTION.replace(" ... ok", " ... ignored", 1),
            EXECUTION.replace(" ... ok", " ... FAILED", 1),
            EXECUTION.replace("0 ignored", "1 ignored"),
            EXECUTION.replace("0 failed", "1 failed"),
        ):
            with self.subTest(output=output), self.assertRaises(RuntimeError):
                proof.require_execution(output, NAMES)

    def test_command_failure_retains_log_and_exit_code(self):
        with tempfile.TemporaryDirectory() as root, patch("sys.stdout", io.StringIO()):
            log = Path(root) / "failure.log"
            with self.assertRaises(subprocess.CalledProcessError) as error:
                proof.run([sys.executable, "-c", "print('native failure'); raise SystemExit(7)"], os.environ, log)
            self.assertEqual(error.exception.returncode, 7)
            self.assertIn("native failure\nexit_code=7", log.read_text())

    def test_native_commands_policy_and_failure_receipt(self):
        for platform, host in (("Windows", "x86_64-pc-windows-msvc"), ("macOS", "aarch64-apple-darwin")):
            for failure in (False, True):
                with self.subTest(platform=platform, failure=failure), tempfile.TemporaryDirectory() as root:
                    directory = Path(root) / "evidence with spaces"
                    calls = []

                    def run(args, env, log):
                        calls.append(args)
                        self.assertEqual(env["INTENTD_ASSERT_BOUND_CALLER"], "1")
                        self.assertEqual(env["INTENTD_TEST_TIMEOUT_MULTIPLIER"], "1")
                        if args == ["git", "rev-parse", "HEAD"]:
                            return "source-sha\n"
                        if args == ["rustc", "-vV"]:
                            return "host: " + host + "\n"
                        if "--list" in args:
                            return LISTING
                        if "--test-threads=1" in args:
                            if failure:
                                raise subprocess.CalledProcessError(101, args, "native failure")
                            return EXECUTION
                        return "git version fixture\n"

                    env = dict(CONTEXT_CONSUMER_EVIDENCE=str(directory), RUNNER_OS=platform,
                               CARGO_BUILD_TARGET=host, INTENTD_ASSERT_BOUND_CALLER="0",
                               INTENTD_TEST_TIMEOUT_MULTIPLIER="9")
                    with patch.dict(os.environ, env), patch.object(proof, "run", run), patch("sys.stdout", io.StringIO()):
                        if failure:
                            with self.assertRaises(subprocess.CalledProcessError):
                                proof.main()
                        else:
                            proof.main()
                    receipt = json.loads((directory / "receipt.json").read_text())
                    self.assertEqual(receipt["success"], not failure)
                    self.assertEqual(receipt["target"], host)
                    self.assertEqual(receipt["source_sha"], "source-sha")
                    self.assertEqual(receipt["discovered"], NAMES)
                    self.assertEqual(calls[-2], proof.COMMAND + ["--", "--list", "--format=terse"])
                    self.assertEqual(calls[-1], proof.COMMAND + ["--", "--test-threads=1", "--format=pretty", "--color=never"])


if __name__ == "__main__":
    unittest.main()
