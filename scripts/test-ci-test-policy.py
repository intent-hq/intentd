#!/usr/bin/env python3
"""Required CI Rust test steps must match the local wrapper's caller policy.

Reads real YAML (PyYAML, already installed in release-scripts). The command
scan is intentionally bounded to executable shell lines, not echoed plans.
Mutation tests ensure removed/disabled step policy and missing wiring fail.
"""

import copy
import os
from pathlib import Path
import re
import shlex
import subprocess
import unittest

import yaml


ROOT = Path(__file__).resolve().parent.parent
JOBS = ("check", "coverage-e2e", "coverage-all", "coverage-changed")
COMMAND = re.compile(
    r"^(?:[A-Z_]+=\S+\s+)*(?:cargo\s+(?:test\b|nextest\s+run\b|llvm-cov\b[^\n]*\bnextest\b)"
    r"|\./scripts/(?:coverage-e2e|coverage-all|changed-tests)\.sh\b)"
)


def test_steps(workflow):
    for name in JOBS:
        job = workflow["jobs"][name]
        for step in job["steps"]:
            run = str(step.get("run", ""))
            if any(COMMAND.search(line.strip()) and "--dry-run" not in line for line in run.splitlines()):
                yield name, job, step


def violations(workflow, policy):
    errors = []
    found = set()
    for name, job, step in test_steps(workflow):
        found.add(name)
        env = {**workflow.get("env", {}), **job.get("env", {}), **step.get("env", {})}
        command_policies = []
        for line in str(step.get("run", "")).splitlines():
            if COMMAND.search(line.strip()) and "--dry-run" not in line:
                command_env = dict(env)
                # Leading shell assignments take precedence over the YAML env.
                prefix = re.match(r"^(?:[A-Z_]+=\S+\s+)*", line.strip()).group()
                for assignment in shlex.split(prefix):
                    key, value = assignment.split("=", 1)
                    command_env[key] = value
                command_policies.append(str(command_env.get("INTENTD_ASSERT_BOUND_CALLER", "")))
        if any(value != policy for value in command_policies):
            errors.append(f"{name}: {step.get('name')} must arm INTENTD_ASSERT_BOUND_CALLER={policy}")
        if job.get("continue-on-error") or step.get("continue-on-error"):
            errors.append(f"{name}: test policy must remain required")
    if found != set(JOBS):
        errors.append(f"missing test jobs: {set(JOBS) - found}")
    source_lints = workflow["jobs"]["check"]["steps"]
    if not any(step.get("run") == "cargo test --workspace --test '*_lint'" for step in source_lints):
        errors.append("source lint discovery must retain its literal glob invocation")
    gate = workflow["jobs"]["gate"]
    if not set((*JOBS, "release-scripts")).issubset(gate["needs"]):
        errors.append("CI Gate must require test jobs and policy regressions")
    steps = workflow["jobs"]["release-scripts"]["steps"]
    for script in ("test-test-policy.py", "test-ci-test-policy.py"):
        matches = [step for step in steps if re.search(r"^python3 (?:-[IBS] )*scripts/" + re.escape(script) + r"$", str(step.get("run", "")), re.M)]
        if not matches or any(step.get("if") or step.get("continue-on-error") for step in matches):
            errors.append(f"release-scripts must run {script} unconditionally")
    return errors


class CiPolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())

    def test_required_ci_matches_wrapper(self):
        result = subprocess.run(
            ["bash", str(ROOT / "scripts/with-test-policy.sh"), "sh", "-c", 'printf "%s" "$INTENTD_ASSERT_BOUND_CALLER"'],
            env={**os.environ, "INTENTD_ASSERT_BOUND_CALLER": "0"}, capture_output=True, text=True, timeout=5,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "1")
        self.assertEqual(violations(self.workflow, result.stdout), [])

    def test_each_required_test_step_rejects_missing_or_disabled_policy(self):
        for name, _, step in test_steps(self.workflow):
            for value in (None, "0", "false"):
                with self.subTest(job=name, step=step["name"], value=value):
                    workflow = copy.deepcopy(self.workflow)
                    mutated = next(s for s in workflow["jobs"][name]["steps"] if s.get("name") == step["name"])
                    env = mutated.setdefault("env", {})
                    if value is None:
                        env.pop("INTENTD_ASSERT_BOUND_CALLER", None)
                    else:
                        env["INTENTD_ASSERT_BOUND_CALLER"] = value
                    self.assertTrue(violations(workflow, "1"))

    def test_unarmed_new_test_step_is_discovered(self):
        workflow = copy.deepcopy(self.workflow)
        workflow["jobs"]["check"]["steps"].append({"name": "New Rust tests", "run": "cargo test -p new-crate"})
        self.assertTrue(any("New Rust tests" in error for error in violations(workflow, "1")))

    def test_shell_assignment_cannot_disable_ci_policy(self):
        workflow = copy.deepcopy(self.workflow)
        workflow["jobs"]["check"]["steps"].append({
            "name": "Disabled Rust tests", "env": {"INTENTD_ASSERT_BOUND_CALLER": "1"},
            "run": "INTENTD_ASSERT_BOUND_CALLER=0 cargo test -p new-crate",
        })
        self.assertTrue(any("Disabled Rust tests" in error for error in violations(workflow, "1")))

    def test_missing_regression_wiring_is_rejected(self):
        for script in ("test-test-policy.py", "test-ci-test-policy.py"):
            workflow = copy.deepcopy(self.workflow)
            steps = workflow["jobs"]["release-scripts"]["steps"]
            workflow["jobs"]["release-scripts"]["steps"] = [s for s in steps if script not in s.get("run", "")]
            self.assertTrue(any(script in error for error in violations(workflow, "1")))


if __name__ == "__main__":
    unittest.main()
