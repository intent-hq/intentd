#!/usr/bin/env python3
"""Ensure native lifecycle evidence cannot silently pass with zero/skipped tests."""
import importlib.util
import io
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("lifecycle", Path(__file__).with_name("test-daemon-lifecycle.py"))
lifecycle = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lifecycle)


class EvidenceTests(unittest.TestCase):
    def test_nonzero_command_preserves_output_and_fails(self):
        with patch("sys.stdout", io.StringIO()), self.assertRaises(subprocess.CalledProcessError) as raised:
            lifecycle.run([sys.executable, "-c", "print('failure evidence'); raise SystemExit(7)"])
        self.assertEqual(raised.exception.returncode, 7)
        self.assertEqual(raised.exception.output, "failure evidence\n")

    def test_progress_is_visible_before_command_exits(self):
        with tempfile.TemporaryDirectory() as root:
            receipt = Path(root) / "observed"

            class Observer(io.StringIO):
                def write(self, text):
                    if text.startswith("phase ready\n"):
                        receipt.touch()
                    return super().write(text)

            # The child exits successfully only when its progress has already
            # reached the caller. Buffered-until-exit capture fails this handshake.
            code = "\n".join([
                "from pathlib import Path",
                "import sys, time",
                "print('phase ready', flush=True)",
                "deadline = time.monotonic() + 3",
                "while not Path(sys.argv[1]).exists() and time.monotonic() < deadline:",
                "    time.sleep(0.01)",
                "sys.exit(0 if Path(sys.argv[1]).exists() else 9)",
            ])
            with patch("sys.stdout", Observer()):
                output = lifecycle.run([sys.executable, "-c", code, str(receipt)])
            self.assertEqual(output, "phase ready\n")

    def test_requires_executed_success(self):
        lifecycle.require_passed("test result: ok. 1 passed; 0 failed; 0 ignored; 5 filtered out")
        for result in ["", "test result: ok. 0 passed; 0 failed; 0 ignored", "test result: ok. 0 passed; 0 failed; 1 ignored", "test result: ok. 1 passed; 0 failed; 1 ignored", "test result: FAILED. 0 passed; 1 failed; 0 ignored"]:
            with self.subTest(result=result), self.assertRaises(RuntimeError):
                lifecycle.require_passed(result)


if __name__ == "__main__":
    unittest.main()
