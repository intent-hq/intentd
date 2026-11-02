#!/usr/bin/env python3
"""Tests for check-test-scripts-wired.py against throwaway repo trees, plus a
run of the real check on this repo (which must pass)."""
import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
CHECK = HERE / "check-test-scripts-wired.py"
SPEC = importlib.util.spec_from_file_location("wired_check", CHECK)
check = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(check)


class FixtureTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="wired-check-")
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        (self.root / "scripts").mkdir()
        (self.root / ".github/workflows").mkdir(parents=True)

    def write(self, relative, text=""):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def names(self, manual_only=None):
        return [name for name, _ in check.problems(self.root, manual_only or {})]

    def test_each_reference_source_wires_a_suite(self):
        self.write("scripts/test-a.py")
        self.write("scripts/test-b.sh")
        self.write("scripts/test-c.py", "subprocess.run(['bash', 'scripts/test-b.sh'])")
        self.write("scripts/test-d.py")
        self.write(".github/workflows/ci.yml", "run: python3 -I -B scripts/test-a.py\n")
        self.write(".github/actions/x/action.yml", "run: ./scripts/test-c.py\n")
        self.write("Makefile", "t:\n\tpython3 scripts/test-d.py\n")
        self.assertEqual(self.names(), [])

    def test_an_unreferenced_suite_fails(self):
        self.write("scripts/test-a.py")
        self.write("scripts/test-orphan.py")
        self.write(".github/workflows/ci.yml", "run: python3 scripts/test-a.py\n")
        self.assertEqual(self.names(), ["test-orphan.py"])

    def test_a_suite_naming_itself_is_not_wired(self):
        self.write("scripts/test-self.py", "# Run directly: ./scripts/test-self.py\n")
        self.assertEqual(self.names(), ["test-self.py"])

    def test_a_longer_name_does_not_wire_its_prefix(self):
        self.write("scripts/test-x.py")
        self.write("scripts/test-x.py-extra")
        self.write("scripts/test-x-caller.py")
        self.write(".github/workflows/ci.yml",
                   "run: |\n  ./scripts/test-x.py-extra\n  ./scripts/test-x-caller.py\n  ./scripts/pre-test-x.py\n")
        self.assertEqual(self.names(), ["test-x.py"])

    def test_manual_only_entry_excuses_an_orphan(self):
        self.write("scripts/test-manual.sh")
        self.assertEqual(self.names({"test-manual.sh": "needs real artifacts"}), [])

    def test_stale_manual_only_entries_fail(self):
        self.write("scripts/test-wired.py")
        self.write("Makefile", "\tpython3 scripts/test-wired.py\n")
        found = check.problems(self.root, {"test-wired.py": "r", "test-gone.py": "r"})
        self.assertEqual([name for name, _ in found], ["test-wired.py", "test-gone.py"])
        self.assertIn("referenced by Makefile", found[0][1])
        self.assertIn("does not exist", found[1][1])

    def test_cli_reports_orphans_and_exits_1(self):
        self.write("scripts/test-orphan.py")
        run = subprocess.run([sys.executable, "-I", "-B", str(CHECK), str(self.root)],
                             capture_output=True, text=True)
        self.assertEqual(run.returncode, 1, run.stdout + run.stderr)
        self.assertIn("::error file=scripts/test-orphan.py::", run.stdout)

    def test_cli_rejects_a_tree_without_scripts(self):
        run = subprocess.run([sys.executable, "-I", "-B", str(CHECK), str(self.root / "missing")],
                             capture_output=True, text=True)
        self.assertEqual(run.returncode, 2)


class RepoTests(unittest.TestCase):
    def test_this_repo_passes(self):
        run = subprocess.run([sys.executable, "-I", "-B", str(CHECK)], capture_output=True, text=True)
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)

    def test_without_manual_only_its_entries_are_the_orphans(self):
        # With no MANUAL_ONLY list, exactly the listed suites are reported.
        found = check.problems(HERE.parent, {})
        self.assertEqual([name for name, _ in found], sorted(check.MANUAL_ONLY))


if __name__ == "__main__":
    unittest.main(verbosity=2)
