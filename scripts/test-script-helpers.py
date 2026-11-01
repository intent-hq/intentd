#!/usr/bin/env python3
"""Tests for script_helpers.py, the derived copy list of the fixture harnesses.

scripts/test-changed-tests.sh, scripts/test-test-policy.py,
scripts/test-optional-intel-release.py, scripts/test-optional-intel-dist.py and
scripts/test-prepare-claude-review-runtime.py run scripts in a scratch copy and
copy each script's helpers there. intentd#2896 showed a hand-kept copy list
breaking with exit 127 only in the full release-scripts run; these cases pin
the derived list (shell calls and Python sibling loads) and the fail-fast
error.
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

    def test_python_sibling_loads_and_data_files(self):
        write(self.dir, "main.py", "\n".join([
            'from pathlib import Path',
            'HERE = Path(__file__).resolve().parent',
            'SCRIPTS = Path(__file__).parent  # the scripts directory',
            'spec = load(Path(__file__).with_name("a.py"))',
            "policy = Path(__file__).resolve().with_name('b-policy.json')",
            'helper = HERE / "c.py"',
            "data = SCRIPTS/'d.patch'",
            'inline = Path(__file__).resolve().parent / "e.mjs"',
            '',
        ]))
        write(self.dir, "a.py", "DESCRIPTOR = Path(__file__).with_name('f.json')\n")
        for name in ("b-policy.json", "c.py", "e.mjs", "f.json"):
            write(self.dir, name, "{}\n")
        write(self.dir, "d.patch", "+scripts/not-a-call.sh\n")
        self.assertEqual(self.closure("main.py"),
                         ["a.py", "b-policy.json", "c.py", "d.patch", "e.mjs", "f.json", "main.py"])

    def test_python_paths_outside_the_script_directory_are_not_siblings(self):
        write(self.dir, "main.py", "\n".join([
            'ROOT = Path(__file__).resolve().parent.parent',
            'TOP = Path(__file__).resolve().parents[1]',
            'a = ROOT / "gone.py"',
            'b = TOP / "dist-workspace.toml"',
            'c = archive.with_name("gone.sha256")',
            'd = other / "gone.json"',
            'e = MYHERE / "gone.py"',
            'f = HERE / f"{name}.py"',
            '# HERE = Path(__file__).parent',
            '# spec = Path(__file__).with_name("gone.py")',
            '',
        ]))
        self.assertEqual(self.closure("main.py"), ["main.py"])

    def test_data_file_roots_are_copied_but_not_scanned(self):
        write(self.dir, "install.ps1", "& scripts/gone.sh\n")
        (self.dir / "blob.json").write_bytes(b"\xff\xfe scripts/gone.py")
        self.assertEqual(self.closure("install.ps1", "blob.json"), ["blob.json", "install.ps1"])

    def test_missing_python_sibling_names_caller_line_and_file(self):
        write(self.dir, "main.py", "import x\nspec = Path(__file__).with_name('missing.py')\n")
        with self.assertRaises(script_helpers.MissingHelper) as raised:
            self.closure("main.py")
        self.assertIn("main.py:2 loads missing.py", str(raised.exception))
        write(self.dir, "data.py", "HERE = Path(__file__).resolve().parent\npath = HERE / 'gone.json'\n")
        with self.assertRaises(script_helpers.MissingHelper) as raised:
            self.closure("data.py")
        self.assertIn("data.py:2 loads gone.json", str(raised.exception))

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

    def test_script_helpers_itself_resolves(self):
        # Every harness that uses copy_into loads script_helpers.py, so any
        # closure reaching a harness reaches it; its own text must not read
        # as a call to a missing helper.
        self.assertIn("script_helpers.py", script_helpers.closure(["script_helpers.py"]))

    def test_python_harnessed_scripts_resolve(self):
        release = script_helpers.closure(["reconcile-sitter-intel.py"])
        self.assertEqual(release, ["optional-intel-release.py", "reconcile-sitter-intel.py"])
        review = script_helpers.closure(["prepare-claude-review-runtime.py"])
        for name in ("prepare-claude-callback-runtime.py", "claude-review-runtime.json",
                     "claude-review-runtime.patch", "claude-callback-runtime.json"):
            self.assertIn(name, review)
        dist = script_helpers.closure(["configure-optional-intel-release.py", "configure-release-host-network.py"])
        for name in ("optional-intel-release.py", "fetch-locked-crates.py"):
            self.assertIn(name, dist)

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
