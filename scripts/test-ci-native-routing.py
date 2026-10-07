#!/usr/bin/env python3
"""Exercise native CI event routing and the real CI Gate shell body offline.

The small expression interpreter supports only the boolean/context subset used
by these job and native-step conditions; unknown syntax fails closed. This is
not an Actions runner: matrix scheduling and implicit success are modeled,
while the gate's actual shell is executed with substituted result fixtures.
"""

import ast
import copy
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]
RESULTS = ("success", "skipped", "failure", "cancelled")
ALWAYS_REQUIRED = {"deb-packaging", "release-scripts", "install-ps1", "pi-session-paths"}
REQUIRED = {
    "pull_request": ALWAYS_REQUIRED | {"check", "coverage-changed", "monorepo-consumer-checks"},
    "merge_group": ALWAYS_REQUIRED | {"check", "build", "coverage-e2e", "coverage-all", "monorepo-consumer-checks"},
    "push": ALWAYS_REQUIRED | {"build"},
}
PR_METADATA = {"pr-title", "conflict-markers"}
NATIVE_STEPS = {
    "Check (native production configuration)": {"macOS", "Windows"},
    "Clippy (target-gated code)": {"macOS", "Windows"},
    "Tunnel deadline and TLS controls (macOS runtime)": {"macOS"},
    "Codex diagnostic process ownership (Windows runtime)": {"Windows"},
    "Codex focused diagnostics (macOS runtime)": {"macOS"},
    "Retained workspace CoW fixtures (macOS runtime)": {"macOS"},
}
GITLAB_STEP = "GitLab auth, invitations and checkout over real sockets with explicit test transports"
GITLAB_COMMAND = (
    "cargo test --locked -p intentd --features repository-test-fixtures "
    "--test e2e_wss_gitlab_auth --test e2e_wss_invite_join_gitlab "
    "--test e2e_repository_resource_read -- --test-threads=2"
)


def condition(value, context, *, success=True, cancelled=False):
    if isinstance(value, bool):
        return success and value
    expression = str(value).removeprefix("${{").removesuffix("}}").strip()
    has_status = bool(re.search(r"\b(always|success|failure|cancelled)\(\)", expression))
    statuses = {"always": True, "success": success, "failure": not success, "cancelled": cancelled}
    expression = re.sub(r"\b(always|success|failure|cancelled)\(\)", lambda m: repr(statuses[m[1]]), expression)
    expression = re.sub(r"\b(?:github|needs|runner)\.[\w.-]+", lambda m: repr(context.get(m[0], "")), expression)
    expression = expression.replace("&&", " and ").replace("||", " or ")
    expression = re.sub(r"!(?!=)", " not ", expression).strip()
    expression = re.sub(r"\btrue\b|\bfalse\b", lambda m: m[0].title(), expression)
    tree = ast.parse(expression, mode="eval")
    allowed = (ast.Expression, ast.BoolOp, ast.UnaryOp, ast.Compare, ast.Constant,
               ast.And, ast.Or, ast.Not, ast.Eq, ast.NotEq)
    if any(not isinstance(node, allowed) for node in ast.walk(tree)):
        raise ValueError(f"unsupported Actions condition: {value}")
    return (has_status or success) and bool(eval(compile(tree, "<condition>", "eval"), {"__builtins__": {}}))


def context(event, *, fork=False, fast_path="", platform="macOS"):
    return {
        "github.event_name": event,
        "github.repository": "intent-hq/intentd",
        "github.event.pull_request.head.repo.full_name": "outside/intentd" if fork else "intent-hq/intentd",
        "needs.release-fast-path.outputs.fast_path": fast_path,
        "runner.os": platform,
    }


def scheduled(workflow, event, *, fork=False, fast_path="", route="success", cancelled=False):
    results = {}
    for name, job in workflow["jobs"].items():
        needs = job.get("needs", [])
        if isinstance(needs, str):
            needs = [needs]
        values = context(event, fork=fork, fast_path=fast_path)
        values.update({f"needs.{key}.result": value for key, value in results.items()})
        runs = condition(job.get("if", True), values,
                         success=all(results[n] == "success" for n in needs), cancelled=cancelled)
        results[name] = (route if name == "route" else "success") if runs else "skipped"
    return results


def gate_passes(workflow, event, results):
    body = next(s["run"] for s in workflow["jobs"]["gate"]["steps"] if s.get("name") == "Check results")
    values = {"github.event_name": event, **{f"needs.{k}.result": v for k, v in results.items()}}
    body = re.sub(r"\$\{\{\s*(.*?)\s*\}\}", lambda m: values[m[1]], body)
    result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", body], capture_output=True, text=True, timeout=5)
    return result.returncode == 0


def gate_errors(workflow):
    errors = []
    gate = workflow["jobs"]["gate"]
    step = next(s for s in gate["steps"] if s.get("name") == "Check results")
    if gate.get("continue-on-error") or step.get("continue-on-error") or step.get("if"):
        errors.append("gate result check must be unconditional and fatal")
    needs = set(workflow["jobs"]["gate"]["needs"])
    expected = set.union(*REQUIRED.values(), PR_METADATA)
    if needs != expected:
        errors.append("gate dependencies changed")
    for event, required in REQUIRED.items():
        baseline = {name: "success" if name in required or (event == "pull_request" and name in PR_METADATA)
                    else "skipped" for name in expected}
        if not gate_passes(workflow, event, baseline):
            errors.append(f"{event}: intended skips rejected")
        for name in expected:
            for result in RESULTS:
                accepted = result == "success" or (result == "skipped" and name not in required)
                if gate_passes(workflow, event, {**baseline, name: result}) != accepted:
                    errors.append(f"{event}: {name}={result}")
    return errors


def routing_errors(workflow):
    errors = []
    for event in REQUIRED:
        for fast_path in ("", "false", "true") if event == "pull_request" else ("",):
            jobs = scheduled(workflow, event, fast_path=fast_path)
            expected = REQUIRED[event] | {"route", "gate"}
            if event == "pull_request":
                expected |= PR_METADATA | {"release-fast-path"}
            actual = {name for name, result in jobs.items() if result == "success"}
            if actual != expected:
                errors.append(f"{event}/{fast_path}: wrong jobs {actual ^ expected}")
    if any(value != "skipped" for value in scheduled(workflow, "pull_request", fork=True).values()):
        errors.append("fork PR allocated runners")
    for event in REQUIRED:
        for route in ("failure", "skipped", "cancelled"):
            jobs = scheduled(workflow, event, route=route)
            if event != "push" and gate_passes(workflow, event, jobs):
                errors.append(f"{event}: gate accepted route={route}")
            # Native routing deliberately falls back to hosted Windows on a
            # failed/empty probe. Linux required checks still fail the gate.
            if event != "pull_request" and jobs["build"] != "success":
                errors.append(f"{event}: missing native fallback for route={route}")
    return errors


def native_errors(workflow):
    errors = []
    build = workflow["jobs"]["build"]
    steps = {step.get("name"): step for step in build["steps"]}
    if build.get("continue-on-error") or build["strategy"]["fail-fast"]:
        errors.append("native matrix must be required and independent")
    targets = {entry["target"] for entry in build["strategy"]["matrix"]["include"]}
    if targets != {"aarch64-apple-darwin", "x86_64-pc-windows-msvc"}:
        errors.append("missing native target")
    for name, platforms in NATIVE_STEPS.items():
        step = steps.get(name)
        if not step or step.get("continue-on-error") or not step.get("run"):
            errors.append(f"{name}: missing required proof")
            continue
        for event in ("merge_group", "push"):
            for platform in ("macOS", "Windows"):
                expected = event == "merge_group" and platform in platforms
                if condition(step.get("if", True), context(event, platform=platform)) != expected:
                    errors.append(f"{name}: wrong selection for {event}/{platform}")
    clippy = steps.get("Clippy (target-gated code)", {})
    if clippy.get("run") != "cargo clippy --workspace --all-targets --target ${{ matrix.target }} -- -D warnings":
        errors.append("native Clippy must lint all workspace targets")
    if clippy.get("env") or clippy.get("working-directory"):
        errors.append("native Clippy must inherit the job compilation configuration")
    for name, command in (("Build (release)", "cargo build --workspace --release --target ${{ matrix.target }}"),
                          ("Warm merge queue check cache", "cargo check --workspace --target ${{ matrix.target }}")):
        step = steps.get(name)
        if not step or step.get("run") != command or step.get("continue-on-error"):
            errors.append(f"{name}: missing push compilation")
        elif any(condition(step.get("if", True), context(event)) != (event == "push") for event in REQUIRED):
            errors.append(f"{name}: must be push-only")
    # --all-targets unifies dev-dependency fixture features into intent-services
    # and omits some feature-off production arms. Keep the non-test check too.
    check = steps.get("Check (native production configuration)", {})
    if check.get("run") != "cargo check --workspace --target ${{ matrix.target }}":
        errors.append("native check must compile the non-test production configuration")
    if check.get("env") or check.get("working-directory"):
        errors.append("native check must inherit the job compilation configuration")
    tests = workflow["jobs"]["release-scripts"]
    if tests.get("continue-on-error") or not any(
        re.search(r"^python3 (?:-[IB] )*scripts/test-ci-native-routing.py$", step.get("run", ""), re.M)
        and not step.get("if") and not step.get("continue-on-error") for step in tests["steps"]
    ):
        errors.append("native routing regression suite must run in CI")
    return errors


def gitlab_errors(workflow):
    errors = []
    step = next((s for s in workflow["jobs"]["check"]["steps"] if s.get("name") == GITLAB_STEP), None)
    if not step:
        return ["missing GitLab feature suite"]
    for event in REQUIRED:
        if condition(step.get("if", True), context(event)) != (event == "merge_group"):
            errors.append(f"GitLab suite selected incorrectly on {event}")
    if step.get("run") != GITLAB_COMMAND:
        errors.append("GitLab targets, feature, lockfile and thread limit must be retained")
    if step.get("continue-on-error") or workflow["jobs"]["check"].get("continue-on-error"):
        errors.append("GitLab failures must remain fatal")
    if step.get("env", {}).get("INTENTD_ASSERT_BOUND_CALLER") != "1":
        errors.append("GitLab caller policy must remain armed")
    return errors


class NativeRoutingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())

    def test_job_selection_and_route_failures(self):
        self.assertEqual(routing_errors(self.workflow), [])

    def test_gate_result_truth_tables(self):
        self.assertEqual(gate_errors(self.workflow), [])

    def test_native_proofs_and_push_release_builds(self):
        self.assertEqual(native_errors(self.workflow), [])

    def test_gitlab_feature_suite_is_required_only_in_queue(self):
        self.assertEqual(gitlab_errors(self.workflow), [])

    def test_gitlab_mutations_cannot_drop_required_coverage(self):
        for mutation in ("remove", "skip", "pr-only", "optional", "feature", "target", "threads", "caller"):
            with self.subTest(mutation=mutation):
                workflow = copy.deepcopy(self.workflow)
                steps = workflow["jobs"]["check"]["steps"]
                step = next(s for s in steps if s.get("name") == GITLAB_STEP)
                if mutation == "remove":
                    steps.remove(step)
                elif mutation == "skip":
                    step["if"] = False
                elif mutation == "pr-only":
                    step["if"] = "github.event_name == 'pull_request'"
                elif mutation == "optional":
                    step["continue-on-error"] = True
                elif mutation == "caller":
                    step["env"]["INTENTD_ASSERT_BOUND_CALLER"] = "0"
                else:
                    removed = {"feature": "--features repository-test-fixtures", "target": "--test e2e_repository_resource_read", "threads": "--test-threads=2"}[mutation]
                    step["run"] = step["run"].replace(removed, "")
                self.assertTrue(gitlab_errors(workflow))

    def test_gitlab_child_failure_propagates(self):
        step = next(s for s in self.workflow["jobs"]["check"]["steps"] if s.get("name") == GITLAB_STEP)
        with tempfile.TemporaryDirectory() as tmp:
            stub = Path(tmp) / "cargo"
            stub.write_text("#!/bin/sh\nexit 29\n")
            stub.chmod(0o755)
            body = "PATH=" + shlex.quote(tmp) + "\n" + step["run"]
            result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", body], capture_output=True, timeout=5)
            self.assertEqual(result.returncode, 29)

    def test_trigger_and_gate_contract(self):
        events = self.workflow.get("on", self.workflow.get(True))
        self.assertEqual(set(events), set(REQUIRED))
        self.assertEqual(events["push"]["branches"], ["main"])
        gate = self.workflow["jobs"]["gate"]
        self.assertEqual(gate["name"], "CI Gate")
        for event in REQUIRED:
            for failed in (False, True):
                self.assertTrue(condition(gate["if"], context(event), success=not failed))
        self.assertFalse(condition(gate["if"], context("pull_request", fork=True), success=False))
        self.assertFalse(condition(self.workflow["jobs"]["build"]["if"], context("merge_group"), cancelled=True))

    def test_mutations_cannot_disable_native_queue_steps(self):
        for name in NATIVE_STEPS:
            for mutation in ("remove", "skip", "pr-only", "optional", "empty"):
                with self.subTest(step=name, mutation=mutation):
                    workflow = copy.deepcopy(self.workflow)
                    steps = workflow["jobs"]["build"]["steps"]
                    step = next(s for s in steps if s.get("name") == name)
                    if mutation == "remove":
                        steps.remove(step)
                    elif mutation == "skip":
                        step["if"] = False
                    elif mutation == "pr-only":
                        step["if"] = "github.event_name == 'pull_request'"
                    elif mutation == "optional":
                        step["continue-on-error"] = True
                    else:
                        step["run"] = ""
                    self.assertTrue(any(name in error for error in native_errors(workflow)))

    def test_mutations_cannot_skip_queue_or_enable_pr_matrix(self):
        for expression in ("github.event_name == 'push'", "always()"):
            with self.subTest(expression=expression):
                workflow = copy.deepcopy(self.workflow)
                workflow["jobs"]["build"]["if"] = expression
                self.assertTrue(any("wrong jobs" in error for error in routing_errors(workflow)))

    def test_mutations_cannot_replace_production_check_with_fixture_configuration(self):
        for arguments in ("--all-targets", "--tests", "--features intent-services/repository-test-fixtures"):
            with self.subTest(arguments=arguments):
                workflow = copy.deepcopy(self.workflow)
                step = next(s for s in workflow["jobs"]["build"]["steps"]
                            if s.get("name") == "Check (native production configuration)")
                step["run"] += " " + arguments
                self.assertIn("native check must compile the non-test production configuration", native_errors(workflow))

    def test_mutation_cannot_accept_skipped_queue_build(self):
        workflow = copy.deepcopy(self.workflow)
        step = next(s for s in workflow["jobs"]["gate"]["steps"] if s.get("name") == "Check results")
        step["run"] = step["run"].replace('${{ needs.build.result }}', "success")
        self.assertIn("merge_group: build=skipped", gate_errors(workflow))
        self.assertIn("push: build=skipped", gate_errors(workflow))

    def test_mutation_cannot_drop_gate_dependency(self):
        workflow = copy.deepcopy(self.workflow)
        workflow["jobs"]["gate"]["needs"].remove("build")
        self.assertIn("gate dependencies changed", gate_errors(workflow))

    def test_expression_status_and_unknown_syntax(self):
        self.assertFalse(condition("github.event_name != 'push'", context("pull_request"), success=False))
        self.assertTrue(condition("!cancelled()", {}, success=False))
        self.assertFalse(condition("!cancelled()", {}, cancelled=True))
        self.assertTrue(condition("needs.release-fast-path.outputs.fast_path != 'true'", context("merge_group")))
        self.assertFalse(condition("needs.release-fast-path.outputs.fast_path != 'true'", context("pull_request", fast_path="true")))
        with self.assertRaises(ValueError):
            condition("unknown()", {})


if __name__ == "__main__":
    unittest.main()
