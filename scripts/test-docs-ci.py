#!/usr/bin/env python3
"""Exercise docs job routing, gate failures and the actual focused Cargo runner."""

import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import yaml


ROOT = Path(__file__).resolve().parents[1]


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ci = load("docs_ci", "docs-ci.py")
routing = load("routing", "test-ci-native-routing.py")
COMPILED = {"tier": "compiled", "packages": ["intent-acp", "intent-services"],
            "tests": [{"package": "intentd", "target": "e2e_wss_system_prompt_fallback"}]}


class RoutingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())

    def test_docs_job_selection_and_gate(self):
        for event in ("pull_request", "merge_group"):
            for tier in ("docs", "compiled"):
                with self.subTest(event=event, tier=tier):
                    jobs = routing.scheduled(self.workflow, event, docs_tier=tier)
                    expected = {"docs-scope", "docs-lint", "gate"}
                    if event == "pull_request":
                        expected |= routing.PR_METADATA
                    if tier == "compiled":
                        expected |= {"docs-compiled", "monorepo-consumer-checks"}
                    self.assertEqual({name for name, result in jobs.items() if result == "success"}, expected)
                    self.assertTrue(routing.gate_passes(self.workflow, event, jobs, tier))
                    for name in self.workflow["jobs"]["gate"]["needs"]:
                        for result in ("skipped", "failure", "cancelled"):
                            mutated = {**jobs, name: result}
                            accepted = result == "skipped" and name not in expected
                            self.assertEqual(routing.gate_passes(self.workflow, event, mutated, tier), accepted,
                                             (event, tier, name, result))

    def test_classifier_failure_and_unknown_outputs_run_full_ci(self):
        for event in ("pull_request", "merge_group"):
            baseline = routing.scheduled(self.workflow, event)
            for tier, result in (("", "success"), ("unknown", "success"), ("full", "success"),
                                 ("", "failure"), ("", "skipped"), ("docs", "failure"),
                                 ("compiled", "failure")):
                with self.subTest(event=event, tier=tier, result=result):
                    jobs = routing.scheduled(self.workflow, event, docs_tier=tier, docs_result=result)
                    for name in baseline.keys() - {"docs-scope"}:
                        self.assertEqual(jobs[name], baseline[name], name)
                    self.assertTrue(routing.gate_passes(self.workflow, event, jobs, tier))
                    for name in routing.REQUIRED[event]:
                        self.assertFalse(routing.gate_passes(self.workflow, event, {**jobs, name: "skipped"}, tier))

    def test_push_and_fork_invariance(self):
        expected = routing.scheduled(self.workflow, "push")
        for tier in ("docs", "compiled", "full", ""):
            jobs = routing.scheduled(self.workflow, "push", docs_tier=tier)
            self.assertEqual(jobs, expected)
            self.assertTrue(routing.gate_passes(self.workflow, "push", jobs, tier))
            fork = routing.scheduled(self.workflow, "pull_request", fork=True, docs_tier=tier)
            self.assertEqual(set(fork.values()), {"skipped"})

    def test_unproven_tier_cannot_authorize_skips(self):
        jobs = routing.scheduled(self.workflow, "merge_group", docs_tier="docs")
        for tier in ("", "full", "unknown"):
            self.assertFalse(routing.gate_passes(self.workflow, "merge_group", jobs, tier))
        for result in ("failure", "skipped", "cancelled"):
            self.assertFalse(routing.gate_passes(self.workflow, "merge_group",
                                                {**jobs, "docs-scope": result}, "docs"))

    def test_gate_mutation_cannot_ignore_focused_failure(self):
        workflow = copy.deepcopy(self.workflow)
        gate = next(step for step in workflow["jobs"]["gate"]["steps"] if step.get("name") == "Check results")
        gate["run"] = gate["run"].replace("${{ needs.docs-compiled.result }}", "success")
        jobs = routing.scheduled(workflow, "merge_group", docs_tier="compiled")
        jobs["docs-compiled"] = "failure"
        self.assertFalse(routing.gate_passes(self.workflow, "merge_group", jobs, "compiled"))
        self.assertTrue(routing.gate_passes(workflow, "merge_group", jobs, "compiled"))

    def test_classify_and_lint_use_event_base_and_whole_group_head(self):
        fixtures = [
            {"github.event.pull_request.base.sha": "pr-base", "github.sha": "pr-merged-head"},
            {"github.event.merge_group.base_sha": "group-base", "github.event.merge_group.head_sha": "whole-group-head",
             "github.sha": "unrelated-default", "github.event.pull_request.base.sha": "wrong-pr-base"},
        ]
        for fields, expected in zip(fixtures, [("pr-base", "pr-merged-head"), ("group-base", "whole-group-head")]):
            for name in ("docs-scope", "docs-lint"):
                step = next(s for s in self.workflow["jobs"][name]["steps"] if "BASE_SHA" in s.get("env", {}))
                resolved = []
                for key in ("BASE_SHA", "HEAD_SHA"):
                    expression = step["env"][key].removeprefix("${{").removesuffix("}}")
                    resolved.append(next((fields.get(term.strip()) for term in expression.split("||")
                                          if fields.get(term.strip())), ""))
                self.assertEqual(tuple(resolved), expected)
        events = self.workflow.get("on", self.workflow.get(True))
        for event in ("pull_request", "merge_group"):
            self.assertNotIn("paths-ignore", events[event] or {})
            self.assertNotIn("paths", events[event] or {})


class RunnerTests(unittest.TestCase):
    def test_classifier_errors_degrade_to_full(self):
        for error in (OSError("missing git"), subprocess.CalledProcessError(1, "git"),
                      subprocess.TimeoutExpired("git", 60)):
            with self.subTest(error=error), patch.object(ci.subprocess, "run", side_effect=error):
                self.assertEqual(ci.classify("base", "head"), ci.FULL)
        invalid = ["bad json", '{"tier":"docs"}',
                   json.dumps({**ci.FULL, "tier": "compiled"}),
                   json.dumps({**COMPILED, "packages": ["--workspace"]}),
                   json.dumps({**COMPILED, "tests": [{"package": "intentd", "target": "*"}]}),
                   json.dumps({**COMPILED, "tier": "docs"})]
        for output in invalid:
            with self.subTest(output=output), patch.object(ci.subprocess, "run", return_value=
                    subprocess.CompletedProcess([], 0, stdout=output, stderr="")):
                self.assertEqual(ci.classify("base", "head"), ci.FULL)

    def test_fetch_and_classifier_receive_exact_revisions(self):
        with patch.object(ci.subprocess, "run", return_value=subprocess.CompletedProcess(
                [], 0, stdout=json.dumps(COMPILED), stderr="")) as run:
            self.assertEqual(ci.classify("group-base", "group-head"), COMPILED)
        self.assertEqual(run.call_args_list[0].args[0],
                         ["git", "fetch", "--no-tags", "--depth=1", "origin", "group-base", "group-head"])
        self.assertEqual(run.call_args_list[1].args[0],
                         ["bash", "scripts/docs-ci-scope.sh", "group-base", "group-head"])

    def run_focused(self, fail_at=""):
        with tempfile.TemporaryDirectory(prefix="docs-ci-runner-") as tmp:
            root = Path(tmp)
            log = root / "calls"
            cargo = root / "cargo"
            cargo.write_text('#!' + sys.executable + '\nimport json, os, sys\n'
                             'with open(os.environ["CALL_LOG"], "a") as log:\n'
                             ' log.write(json.dumps([sys.argv[1:], os.environ.get("INTENTD_ASSERT_BOUND_CALLER")]) + "\\n")\n'
                             'sys.exit(29 if sys.argv[1] == os.environ.get("FAIL_AT") else 0)\n')
            cargo.chmod(0o755)
            result = subprocess.run([sys.executable, "-I", "-B", str(ROOT / "scripts/docs-ci.py"), "compiled"],
                                    env={**os.environ, "PATH": tmp + os.pathsep + os.environ["PATH"],
                                         "CALL_LOG": str(log), "FAIL_AT": fail_at,
                                         "DOCS_SCOPE": json.dumps(COMPILED), "INTENTD_ASSERT_BOUND_CALLER": "0"},
                                    capture_output=True, text=True, timeout=10)
            return result, [json.loads(line) for line in log.read_text().splitlines()]

    def test_focused_commands_run_only_owners_and_prompt_target(self):
        result, calls = self.run_focused()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([args[0] for args, policy in calls], ["check", "clippy", "test"] * 2)
        for args, policy in calls:
            self.assertEqual(policy, "1")
            self.assertNotIn("--workspace", args)
            if "intentd" in args:
                self.assertEqual(args[args.index("--test") + 1], "e2e_wss_system_prompt_fallback")
            else:
                self.assertIn("intent-acp", args)
                self.assertIn("intent-services", args)
            self.assertIn("--locked", args)

    def test_focused_child_failure_is_fatal(self):
        result, calls = self.run_focused("clippy")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(calls), 2)


if __name__ == "__main__":
    unittest.main()
