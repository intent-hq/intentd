#!/usr/bin/env python3
"""Tests for script_helpers.py, the derived copy list of the fixture harnesses.

scripts/test-changed-tests.sh and scripts/test-test-policy.py run scripts in a
scratch checkout and copy each script's helpers there. intentd#2896 showed a
hand-kept copy list breaking with exit 127 only in the full release-scripts
run; these cases pin the derived list and the fail-fast error.
"""

import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest


SCRIPTS = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location("script_helpers", SCRIPTS / "script_helpers.py")
script_helpers = importlib.util.module_from_spec(_spec)
sys.dont_write_bytecode = True
_spec.loader.exec_module(script_helpers)


def write(directory, name, text):
    path = Path(directory) / name
    path.write_text(text)
    return path


class ClosureTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def closure(self, *roots):
        return script_helpers.closure(roots, self.dir)

    def test_reference_forms_and_transitive_calls(self):
        write(self.dir, "main.sh", "\n".join([
            '#!/usr/bin/env bash',
            'cd "$(dirname "$0")/.."',
            '"$BASH" scripts/a.sh',
            'run=("$BASH" "$repo_root/scripts/b.sh")',
            '"$here/c.sh" x',
            '"${script_dir}/d.py"',
            '',
        ]))
        write(self.dir, "a.sh", 'python3 -I "$repo_root/scripts/e.py"\n')
        for name in ("b.sh", "c.sh", "d.py", "e.py"):
            write(self.dir, name, "true\n")
        write(self.dir, "unrelated.sh", "true\n")
        self.assertEqual(self.closure("main.sh"), ["a.sh", "b.sh", "c.sh", "d.py", "e.py", "main.sh"])

    def test_cycles_terminate(self):
        write(self.dir, "a.sh", "scripts/b.sh\n")
        write(self.dir, "b.sh", "scripts/a.sh\n")
        self.assertEqual(self.closure("a.sh"), ["a.sh", "b.sh"])

    def test_comment_lines_are_not_calls(self):
        write(self.dir, "main.sh", "# see scripts/gone.sh and scripts/other.sh\n  # scripts/gone.py\ntrue\n")
        write(self.dir, "other.sh", "true\n")
        self.assertEqual(self.closure("main.sh"), ["main.sh"])

    def test_variable_path_outside_scripts_is_not_a_helper(self):
        write(self.dir, "main.sh", 'bash "$temp_dir/fixture.sh"\n')
        self.assertEqual(self.closure("main.sh"), ["main.sh"])

    def test_missing_literal_helper_names_caller_line_and_helper(self):
        write(self.dir, "main.sh", 'true\n"$BASH" "$repo_root/scripts/scratch-helper.sh"\n')
        with self.assertRaises(script_helpers.MissingHelper) as raised:
            self.closure("main.sh")
        self.assertIn("main.sh:2 calls scripts/scratch-helper.sh", str(raised.exception))

    def test_missing_transitive_helper_names_its_caller(self):
        write(self.dir, "main.sh", "scripts/mid.sh\n")
        write(self.dir, "mid.sh", "scripts/deep.py\n")
        with self.assertRaises(script_helpers.MissingHelper) as raised:
            self.closure("main.sh")
        self.assertIn("mid.sh:1 calls scripts/deep.py", str(raised.exception))

    def test_missing_root(self):
        with self.assertRaises(script_helpers.MissingHelper) as raised:
            self.closure("absent.sh")
        self.assertIn("scripts/absent.sh does not exist", str(raised.exception))

    def test_copy_into_copies_the_closure(self):
        write(self.dir, "main.sh", "scripts/a.sh\n")
        write(self.dir, "a.sh", "true\n")
        with tempfile.TemporaryDirectory() as dest:
            names = script_helpers.copy_into(["main.sh"], dest, self.dir)
            self.assertEqual(names, ["a.sh", "main.sh"])
            self.assertEqual(sorted(os.listdir(dest)), names)

    def test_cli(self):
        write(self.dir, "main.sh", "scripts/a.sh\n")
        write(self.dir, "a.sh", "true\n")
        write(self.dir, "broken.sh", "scripts/nope.sh\n")
        shutil.copy(SCRIPTS / "script_helpers.py", self.dir)
        cli = [sys.executable, "-I", "-B", str(self.dir / "script_helpers.py")]
        ok = subprocess.run(cli + ["main.sh"], capture_output=True, text=True, timeout=10)
        self.assertEqual((ok.returncode, ok.stdout, ok.stderr), (0, "a.sh\nmain.sh\n", ""))
        bad = subprocess.run(cli + ["broken.sh"], capture_output=True, text=True, timeout=10)
        self.assertEqual(bad.returncode, 1)
        self.assertIn("error: broken.sh:1 calls scripts/nope.sh", bad.stderr)
        usage = subprocess.run(cli, capture_output=True, text=True, timeout=10)
        self.assertEqual(usage.returncode, 2)


class RepositoryTests(unittest.TestCase):
    """The real scripts the harnesses run resolve to their current helpers."""

    def test_harnessed_scripts_resolve(self):
        changed = script_helpers.closure(["changed-tests.sh"])
        for helper in ("ci-killed-rustc-retry.sh", "coverage-storage.py", "with-test-policy.sh"):
            self.assertIn(helper, changed)
        for script in ("coverage-e2e.sh", "coverage-all.sh"):
            closure = script_helpers.closure([script])
            self.assertIn("ci-killed-rustc-retry.sh", closure)
            self.assertIn("with-test-policy.sh", closure)

    def test_changed_tests_harness_fails_fast_naming_a_missing_helper(self):
        # The proof the issue asked for: a new helper call that the fixture
        # cannot satisfy stops test-changed-tests.sh before its first case,
        # naming the helper, instead of exit 127 deep inside a case.
        with tempfile.TemporaryDirectory() as tmp:
            scripts = Path(tmp) / "scripts"
            scripts.mkdir()
            for name in script_helpers.closure(["changed-tests.sh"]) + ["test-changed-tests.sh", "script_helpers.py"]:
                shutil.copy2(SCRIPTS / name, scripts / name)
            changed = scripts / "changed-tests.sh"
            lines = changed.read_text().splitlines(keepends=True)
            lines.insert(1, '"$BASH" "$repo_root/scripts/scratch-helper.sh"\n')
            changed.write_text("".join(lines))
            started = time.monotonic()
            result = subprocess.run(
                ["bash", str(scripts / "test-changed-tests.sh")],
                capture_output=True, text=True, timeout=60,
            )
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            self.assertIn("changed-tests.sh:2 calls scripts/scratch-helper.sh", result.stderr)
            self.assertIn("cannot list the helpers changed-tests.sh calls", result.stderr)
            self.assertLess(time.monotonic() - started, 30, "must fail before running cases")


if __name__ == "__main__":
    unittest.main()
