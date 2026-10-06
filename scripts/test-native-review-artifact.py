#!/usr/bin/env python3
"""Offline CI contract plus real setup/validation probes (requires PyYAML).

Rust's native_review_startup_failure_* regressions test producer sanitization,
ownership and saturation. These checks pin its limits at the upload boundary.
They do not pretend to run the remote upload-artifact service.
"""

import copy
import importlib.util
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/native-review-artifact.py"
spec = importlib.util.spec_from_file_location("artifact", SCRIPT)
artifact = importlib.util.module_from_spec(spec)
spec.loader.exec_module(artifact)
PREPARE = "Prepare native startup evidence"
VALIDATE = "Validate native startup evidence"
UPLOAD = "Upload native startup evidence"
PATHS = "\n".join(
    "${{ steps.native-evidence.outputs.directory }}/" + pattern
    for pattern in ("startup-failure-0[0-9].json", "startup-failure-1[0-5].json")
)


def contract(workflow):
    steps = workflow["jobs"]["coverage-e2e"]["steps"]
    named = {s.get("name"): s for s in steps}
    prepare, check, upload = (named[name] for name in (PREPARE, VALIDATE, UPLOAD))
    assert prepare["id"] == "native-evidence"
    assert "if" not in prepare and not prepare.get("continue-on-error")
    run = named["Run e2e coverage"]
    assert run["env"]["NATIVE_REVIEW_FAILURE_DIR"] == "${{ steps.native-evidence.outputs.directory }}"
    assert not run.get("continue-on-error")
    assert steps.index(prepare) < steps.index(run) < steps.index(check) < steps.index(upload)
    assert check["id"] == "native-evidence-check"
    assert check["if"] == "${{ failure() && steps.native-evidence.outputs.directory != '' }}"
    assert check["env"]["NATIVE_REVIEW_FAILURE_DIR"] == run["env"]["NATIVE_REVIEW_FAILURE_DIR"]
    assert check["run"].strip() == 'python3 -I -B scripts/native-review-artifact.py "$NATIVE_REVIEW_FAILURE_DIR" >> "$GITHUB_OUTPUT"'
    assert upload["if"] == "${{ failure() && steps.native-evidence-check.outputs.ready == 'true' }}"
    assert upload["uses"] == "actions/upload-artifact@v7"
    assert upload["continue-on-error"] is True
    assert upload["with"] == {
        "name": "native-review-startup-failures", "path": PATHS + "\n",
        "if-no-files-found": "ignore", "retention-days": 3,
    }
    regressions = workflow["jobs"]["release-scripts"]["steps"]
    wired = [s for s in regressions if "python3 -I -B scripts/test-native-review-artifact.py" in s.get("run", "").splitlines()]
    assert len(wired) == 1 and not wired[0].get("if") and not wired[0].get("continue-on-error")
    assert "release-scripts" in workflow["jobs"]["gate"]["needs"]


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name).resolve()
        self.workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())

    def step(self, name):
        return next(s for s in self.workflow["jobs"]["coverage-e2e"]["steps"] if s.get("name") == name)

    def report(self, slot=0, size=None):
        data = json.dumps({"version": 1, "success": False, "nativeCompletion": "not-established"}).encode()
        if size:
            data += b" " * (size - len(data))
        path = self.root / f"startup-failure-{slot:02}.json"
        path.write_bytes(data)
        path.chmod(0o600)
        return path

    def test_workflow_contract(self):
        contract(self.workflow)

    def test_workflow_rejects_weakened_upload(self):
        for field, value in (("if", "always()"), ("if", "${{ failure() && hashFiles('lcov.info') != '' }}"),
                             ("path", "${{ runner.temp }}/**"), ("retention-days", 30),
                             ("if-no-files-found", "error")):
            with self.subTest(field=field):
                workflow = copy.deepcopy(self.workflow)
                upload = next(s for s in workflow["jobs"]["coverage-e2e"]["steps"] if s.get("name") == UPLOAD)
                (upload if field == "if" else upload["with"])[field] = value
                with self.assertRaises(AssertionError):
                    contract(workflow)

    def test_workflow_requires_validation_and_regression_wiring(self):
        for mutation in ("lcov-condition", "skip-validation", "allow-test-failure", "skip-contract"):
            with self.subTest(mutation=mutation):
                workflow = copy.deepcopy(self.workflow)
                steps = workflow["jobs"]["coverage-e2e"]["steps"]
                named = {s.get("name"): s for s in steps}
                if mutation == "lcov-condition":
                    named[VALIDATE]["if"] += " && hashFiles('lcov.info') != ''"
                elif mutation == "skip-validation":
                    named[VALIDATE]["run"] = "echo ready=true >> \"$GITHUB_OUTPUT\""
                elif mutation == "allow-test-failure":
                    named["Run e2e coverage"]["continue-on-error"] = True
                else:
                    for step in workflow["jobs"]["release-scripts"]["steps"]:
                        if "test-native-review-artifact.py" in step.get("run", ""):
                            step["if"] = "false"
                with self.assertRaises(AssertionError):
                    contract(workflow)

    def test_setup_is_fresh_canonical_private_and_ignores_inherited_destination(self):
        stale = self.root / "native-review-startup-failures"
        stale.mkdir()
        (stale / "startup-failure-00.json").write_text("unrelated secret")
        alias = self.root / "alias"
        alias.symlink_to(self.root, target_is_directory=True)
        directories = []
        for index in range(2):
            output = self.root / f"output-{index}"
            result = subprocess.run(
                ["bash", "-e", "-o", "pipefail", "-c", self.step(PREPARE)["run"]],
                env={**os.environ, "RUNNER_TEMP": str(alias), "GITHUB_OUTPUT": str(output),
                     "NATIVE_REVIEW_FAILURE_DIR": str(stale)},
                capture_output=True, text=True, timeout=5,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            key, value = output.read_text().strip().split("=", 1)
            self.assertEqual(key, "directory")
            path = Path(value)
            self.assertTrue(path.is_absolute())
            self.assertEqual(path.resolve(), path)
            self.assertEqual(path.parent, self.root)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o700)
            self.assertEqual(list(path.iterdir()), [])
            directories.append(path)
        self.assertNotEqual(*directories)
        self.assertEqual((stale / "startup-failure-00.json").read_text(), "unrelated secret")

    def test_fixed_slots_and_byte_bounds_match_writer(self):
        source = (ROOT / "crates/intentd/tests/e2e_native_review_wire.rs").read_text()
        self.assertIn("const OUTPUT_BYTES: usize = 128 * 1024;", source)
        self.assertIn("const OUTPUT_FILES: usize = 16;", source)
        self.assertEqual(artifact.MAX_FILES * artifact.MAX_BYTES, 2 * 1024 * 1024)
        for slot in range(16):
            self.report(slot, 128 * 1024)
        self.assertTrue(artifact.validate(self.root))
        patterns = [line.rsplit("/", 1)[1] for line in self.step(UPLOAD)["with"]["path"].splitlines()]
        self.assertEqual({p.name for pattern in patterns for p in self.root.glob(pattern)}, artifact.SLOTS)
        self.report(0, 128 * 1024 + 1)
        with self.assertRaises(ValueError):
            artifact.validate(self.root)
        self.report(0)
        self.report(16)
        with self.assertRaises(ValueError):
            artifact.validate(self.root)

    def test_rejects_unrelated_raw_invalid_or_nonprivate_files(self):
        cases = ("raw-log", "invalid-json", "wrong-report", "symlink", "hardlink", "public-file", "public-directory", "fifo")
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp).resolve()
                path = root / "startup-failure-00.json"
                path.write_text(json.dumps({"version": 1, "success": False, "nativeCompletion": "not-established"}))
                path.chmod(0o600)
                if case == "raw-log":
                    (root / "child.log").write_text("private raw log")
                elif case == "invalid-json":
                    path.write_text("{")
                elif case == "wrong-report":
                    path.write_text('{"success":true}')
                elif case in ("symlink", "hardlink", "fifo"):
                    path.unlink()
                    foreign = self.report()
                    if case == "symlink":
                        path.symlink_to(foreign)
                    elif case == "hardlink":
                        os.link(foreign, path)
                    else:
                        os.mkfifo(path, 0o600)
                elif case == "public-file":
                    path.chmod(0o644)
                elif case == "public-directory":
                    root.chmod(0o755)
                with self.assertRaises((ValueError, OSError)):
                    artifact.validate(root)

    def test_validation_step_handles_absent_output_without_lcov(self):
        for directory in (self.root / "missing", self.root):
            with self.subTest(directory=directory):
                output = self.root.parent / (self.root.name + "-output")
                self.addCleanup(output.unlink, missing_ok=True)
                output.unlink(missing_ok=True)
                result = subprocess.run(
                    ["bash", "-e", "-o", "pipefail", "-c", self.step(VALIDATE)["run"]], cwd=ROOT,
                    env={**os.environ, "NATIVE_REVIEW_FAILURE_DIR": str(directory), "GITHUB_OUTPUT": str(output)},
                    capture_output=True, text=True, timeout=5,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(output.read_text(), "ready=false\n")
        self.report()
        result = subprocess.run([sys.executable, "-I", "-B", str(SCRIPT), str(self.root)], capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "ready=true\n")
        self.assertFalse((self.root / "lcov.info").exists())

    def test_invalid_output_never_emits_upload_permission_or_raw_content(self):
        self.report().write_text("private raw secret")
        result = subprocess.run([sys.executable, "-I", "-B", str(SCRIPT), str(self.root)], capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertNotIn("private raw secret", result.stderr)


if __name__ == "__main__":
    unittest.main()
