#!/usr/bin/env python3
"""Offline regression controls for callback compilation scope selection."""
import json
import re
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("callback-build-scope.py")
HEAD = "a" * 40
FALLBACK = ["-p", "intent-acp", "-p", "intent-services", "--lib"]
SHARED = ["-p", "intent-core", "-p", "intent-services", "-p", "intent-transport",
          "--lib", "--bins", "--tests"]


class ScopeTests(unittest.TestCase):
    def select(self, plans, *, head=HEAD, version=1, expected_head=HEAD):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "plan.json"
            path.write_text(json.dumps({"version": version, "head": head,
                                        "mergeBase": "b" * 40, "plans": plans}))
            return subprocess.run([sys.executable, "-I", "-B", "-S", str(SCRIPT),
                                   str(path), expected_head], capture_output=True, text=True)

    def assert_scope(self, plans, expected):
        result = self.select(plans)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), expected)

    def test_ci_callback_inventory_names_existing_source_tests(self):
        root = SCRIPT.parent.parent
        workflow = (root / ".github/workflows/ci.yml").read_text()
        blocks = re.findall(r"^      CALLBACK_FIXTURE_TESTS: \|\n((?:        \S[^\n]*\n)+)",
                            workflow, re.M)
        self.assertEqual(len(blocks), 1)
        names = blocks[0].split()
        self.assertEqual(len(names), 6)
        self.assertEqual(len(set(names)), 6)
        owners = {
            "callback_registration::tests":
                "crates/intent-acp/src/callback_registration/tests.rs",
            "agent_manager::repository_origin::callback_delivery::tests::live_context":
                "crates/intent-services/src/agent_manager/repository_origin/callback_delivery/tests.rs",
        }
        # A fast source-name drift check. CI's compiled nextest inventory remains
        # authoritative for full module paths, cfg selection and execution.
        for name in names:
            with self.subTest(name=name):
                module, function = name.rsplit("::", 1)
                self.assertIn(module, owners)
                source = (root / owners[module]).read_text()
                self.assertIsNotNone(
                    re.search(r"(?m)^\s*(?:async )?fn " + re.escape(function) +
                              r"\s*\(", source),
                    f"CI callback test is absent from {owners[module]}: {name}",
                )

    def test_shared_graph_keeps_all_original_packages_and_targets(self):
        self.assert_scope([SHARED], SHARED + ["-p", "intent-acp"])

    def test_acp_already_selected_is_not_duplicated(self):
        shared = SHARED + ["-p", "intent-acp"]
        self.assert_scope([shared], shared)

    def test_deterministic_first_eligible_plan(self):
        self.assert_scope([["-p", "intent-store"], SHARED,
                           ["-p", "intent-services", "--lib"]],
                          SHARED + ["-p", "intent-acp"])

    def test_no_eligible_plan_keeps_existing_scope(self):
        for plans in ([], [["-p", "intent-core", "--lib"]],
                      [["-p", "intent-services", "--test", "smoke"]]):
            with self.subTest(plans=plans):
                self.assert_scope(plans, FALLBACK)

    def test_malformed_or_unsupported_plan_never_falls_back(self):
        for plans in (None, "shell text", [None], [[]], [["-p"]],
                      [["-p", "intent-services\n-p intent-acp"]],
                      [["-p", "intent-services", "--features", "unexpected"]],
                      [["-p", "intent-services", "--lib", "--workspace"]],
                      [["-p", "intent-services", "-p", "intent-services"]],
                      [SHARED, ["--release"]]):
            with self.subTest(plans=plans):
                result = self.select(plans)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")

    def test_stale_or_unknown_schema_is_rejected(self):
        for kwargs in ({"head": "c" * 40}, {"version": 2}, {"head": "bad"}):
            with self.subTest(kwargs=kwargs):
                result = self.select([SHARED], **kwargs)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")


if __name__ == "__main__":
    unittest.main()
