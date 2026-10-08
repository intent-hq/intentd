#!/usr/bin/env python3
"""Ensure native lifecycle evidence cannot silently pass with zero/skipped tests."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("lifecycle", Path(__file__).with_name("test-daemon-lifecycle.py"))
lifecycle = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lifecycle)


class EvidenceTests(unittest.TestCase):
    def test_requires_executed_success(self):
        lifecycle.require_passed("test result: ok. 1 passed; 0 failed; 0 ignored; 5 filtered out")
        for result in ["", "test result: ok. 0 passed; 0 failed; 0 ignored", "test result: ok. 0 passed; 0 failed; 1 ignored", "test result: ok. 1 passed; 0 failed; 1 ignored", "test result: FAILED. 0 passed; 1 failed; 0 ignored"]:
            with self.subTest(result=result), self.assertRaises(RuntimeError):
                lifecycle.require_passed(result)


if __name__ == "__main__":
    unittest.main()
