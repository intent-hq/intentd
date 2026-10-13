#!/usr/bin/env python3
"""Exercise the real runner probe, native event routing and CI Gate offline.

The small expression interpreter supports only the boolean/context subset used
by these job and native-step conditions; unknown syntax fails closed. This is
not an Actions runner: matrix expressions and implicit success are modeled,
while the actual probe JS and gate shell execute against API/result fixtures.
"""

import ast
import copy
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]

_workflow_spec = importlib.util.spec_from_file_location('workflow_tests', ROOT / 'scripts/ci-workflow-tests.py')
_workflow_tests = importlib.util.module_from_spec(_workflow_spec)
_workflow_spec.loader.exec_module(_workflow_tests)

RESULTS = ("success", "skipped", "failure", "cancelled")
ALWAYS_REQUIRED = {"deb-packaging", "release-scripts", "install-ps1", "pi-session-paths"}
REQUIRED = {
    "pull_request": ALWAYS_REQUIRED | {"daemon-lifecycle", "check", "coverage-changed", "monorepo-consumer-checks"},
    "merge_group": ALWAYS_REQUIRED | {"daemon-lifecycle", "check", "coverage-e2e", "coverage-all", "monorepo-consumer-checks"},
    "push": ALWAYS_REQUIRED | {"build"},
}
PR_METADATA = {"pr-title", "conflict-markers"}
NATIVE_STEPS = {
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
    expression = re.sub(r"contains\(matrix.os, 'self-hosted'\)",
                        lambda _: repr("self-hosted" in context.get("matrix.os", [])), expression)
    expression = re.sub(r"\b(?:github|needs|runner|env|steps)\.[\w.-]+", lambda m: repr(context.get(m[0], "")), expression)
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
        "steps.native-scope.outputs.event": event,
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


def gate_passes(workflow, event, results, full_coverage="false"):
    body = next(s["run"] for s in workflow["jobs"]["gate"]["steps"] if s.get("name") == "Check results")
    values = {"github.event_name": event, "needs.check.outputs.full_coverage": full_coverage, **{f"needs.{k}.result": v for k, v in results.items()}}
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
            if event == "push" and jobs["build"] != "success":
                errors.append(f"{event}: missing native fallback for route={route}")
    return errors


def native_errors(workflow):
    errors = []
    build = workflow["jobs"]["build"]
    steps = {step.get("name"): step for step in workflow["jobs"]["daemon-lifecycle"]["steps"]}
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
    for name, command in (("Build (release)", "cargo build --workspace --release --target ${{ matrix.target }}"),
                          ("Warm merge queue check cache", "cargo check --workspace --target ${{ matrix.target }}")):
        step = next((s for s in build["steps"] if s.get("name") == name), None)
        if not step or step.get("run") != command or step.get("continue-on-error"):
            errors.append(f"{name}: missing push compilation")
        elif any(condition(step.get("if", True), context(event)) != (event == "push") for event in REQUIRED):
            errors.append(f"{name}: must be push-only")
    producer = steps.get("Produce native archives in one shared target", {})
    if producer.get("run") != 'python3 -I -B scripts/ci-native-artifacts.py produce --platform "$NATIVE_PLATFORM" --event "$NATIVE_SCOPE" --reuse --archives-only' or producer.get('if') or producer.get('continue-on-error'):
        errors.append('native producer must build every demanded variant')
    lint = steps.get('Production check and Clippy in shared target', {})
    if lint.get('run') != 'python3 -I -B scripts/ci-native-artifacts.py lint --platform "$NATIVE_PLATFORM" --event "$NATIVE_SCOPE"' or lint.get('if') or lint.get('continue-on-error'):
        errors.append('native production check and Clippy must remain fatal')
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
    if step.get("run") != "python3 -I -B scripts/ci-linux-artifacts.py test -- " + GITLAB_COMMAND:
        errors.append("GitLab targets, feature, lockfile and thread limit must be retained")
    if step.get("continue-on-error") or workflow["jobs"]["check"].get("continue-on-error"):
        errors.append("GitLab failures must remain fatal")
    if step.get("env", {}).get("INTENTD_ASSERT_BOUND_CALLER") != "1":
        errors.append("GitLab caller policy must remain armed")
    return errors


def run_route(workflow, **fixture):
    """Execute the workflow's actual github-script body with offline API fixtures."""
    step = next(s for s in workflow["jobs"]["route"]["steps"] if s.get("id") == "decide")
    harness = r"""
const { script, fixture } = JSON.parse(require('node:fs').readFileSync(0, 'utf8'));
const outputs = {}, calls = [], logs = [];
const github = {
  rest: { actions: {
    listSelfHostedRunnersForOrg: 'org', listSelfHostedRunnersForRepo: 'repo',
    async listWorkflowRunsForRepo(args) {
      if ((fixture.queueErrors || []).includes(args.repo)) throw new Error('queue unavailable');
      return { data: { workflow_runs: args.repo === 'intentd' && args.status === 'queued' ? [{ id: 1 }] : [] } };
    },
    async listJobsForWorkflowRun() { return { data: { jobs: fixture.jobs || [] } }; },
  } },
  async paginate(endpoint, args) {
    calls.push({ endpoint, args });
    if (fixture[endpoint + 'Error']) throw new Error(fixture[endpoint + 'Error']);
    return fixture[endpoint] ?? [];
  },
};
const core = { info: message => logs.push(message), setOutput: (name, value) => { outputs[name] = value; } };
const context = { repo: { owner: 'intent-hq', repo: 'intentd' }, runId: 99 };
process.env.THRESHOLD = '8';
const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
new AsyncFunction('github', 'core', 'context', script)(github, core, context)
  .then(() => process.stdout.write(JSON.stringify({ outputs, calls, logs })))
  .catch(error => { console.error(error); process.exitCode = 1; });
"""
    result = subprocess.run(["node", "-e", harness],
                            input=json.dumps({"script": step["with"]["script"], "fixture": fixture}),
                            env={k: v for k, v in os.environ.items() if k != "NODE_OPTIONS"},
                            capture_output=True, text=True, timeout=10, check=True)
    return json.loads(result.stdout)


def route_outputs(workflow, outputs):
    result = {}
    for key, expression in workflow["jobs"]["route"]["outputs"].items():
        match = re.fullmatch(r"\$\{\{ steps\.decide\.outputs\.(\w+) \}\}", expression)
        if not match:
            raise ValueError(f"unsupported job output: {expression}")
        result[key] = outputs.get(match[1], "")
    return result


def matrix_labels(value, outputs):
    """Evaluate only the actual matrix's output-or-JSON-fallback expression."""
    if isinstance(value, list):
        return value
    match = re.fullmatch(r"\$\{\{ fromJSON\(needs\.route\.outputs\.(\w+) \|\| '([^']+)'\) \}\}", value)
    if not match:
        raise ValueError(f"unsupported runner expression: {value}")
    return json.loads(outputs.get(match[1], "") or match[2])


def runner(platform="macOS", arch="ARM64", *, status="online", busy=False):
    return {"status": status, "busy": busy,
            "labels": [{"name": name} for name in ("self-hosted", platform, arch)]}


class RunnerAvailabilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = _workflow_tests.load_ci(ROOT)

    def assert_mac_route(self, fixture, expected):
        result = run_route(self.workflow, **fixture)
        outputs = route_outputs(self.workflow, result["outputs"])
        self.assertEqual(json.loads(outputs.get("macos_labels", "null")), expected)
        self.assertEqual(outputs.get("macos_burst"), str(expected == ["macos-15"]).lower())
        for job_id, field, value in (("build", "target", "aarch64-apple-darwin"),
                                     ("daemon-lifecycle", "platform", "macos")):
            job = self.workflow["jobs"][job_id]
            self.assertIn("route", job["needs"])
            self.assertEqual(job["runs-on"], "${{ matrix.os }}")
            row = next(r for r in job["strategy"]["matrix"]["include"] if r[field] == value)
            self.assertEqual(matrix_labels(row["os"], outputs), expected)
        return result

    def test_macos_requires_positive_idle_matching_availability(self):
        missing_busy = runner()
        missing_busy.pop("busy")
        for fleet in ([], [runner(status="offline")], [runner(busy=True)], [missing_busy],
                      [runner(busy=None)], [runner(busy="false")], [runner(busy=0)],
                      [runner(arch="X64")], [runner(platform="Linux")],
                      [{**runner(), "labels": []}], [{**runner(), "labels": None}],
                      [{**runner(), "labels": [{"name": "macOS"}, {"name": "ARM64"}]}],
                      [runner(busy=True), runner(status="offline"), runner(arch="X64")]):
            with self.subTest(fleet=fleet):
                self.assert_mac_route({"org": fleet}, ["macos-15"])
        for fleet in ([runner()], [runner(platform="MACos", arch="arm64")],
                      [{**runner(), "labels": [{"name": n} for n in ("SELF-HOSTED", "macos", "arm64")]}],
                      [runner(busy=True), runner(status="offline"), runner(), runner(arch="X64")]):
            with self.subTest(fleet=fleet):
                self.assert_mac_route({"org": fleet}, ["self-hosted", "macOS", "ARM64"])

    def test_org_lookup_and_repo_fallback_are_shared_and_fail_to_hosted(self):
        cases = [({}, ["macos-15"], ["org"]),
                 ({"orgError": "403 missing permission", "repo": [runner()]},
                  ["self-hosted", "macOS", "ARM64"], ["org", "repo"]),
                 ({"orgError": "403", "repo": [runner(busy=True)]}, ["macos-15"], ["org", "repo"]),
                 ({"orgError": "403", "repo": []}, ["macos-15"], ["org", "repo"]),
                 ({"orgError": "403", "repoError": "403 missing permission"}, ["macos-15"], ["org", "repo"]),
                 ({"orgError": "network timeout", "repoError": "503 unavailable"}, ["macos-15"], ["org", "repo"]),
                 ({"org": {"unexpected": "response"}}, ["macos-15"], ["org"]),
                 ({"org": [], "repo": [runner()]}, ["macos-15"], ["org"])]
        for fixture, expected, endpoints in cases:
            with self.subTest(fixture=fixture):
                result = self.assert_mac_route(fixture, expected)
                self.assertEqual([call["endpoint"] for call in result["calls"]], endpoints)
                for call in result["calls"]:
                    self.assertEqual(call["args"], {"org": "intent-hq", "per_page": 100} if call["endpoint"] == "org"
                                     else {"owner": "intent-hq", "repo": "intentd", "per_page": 100})

    def test_linux_and_windows_keep_existing_queue_and_online_semantics(self):
        fleet = [runner("Linux", "X64", busy=True), runner("Windows", "X64", busy=True), runner()]
        for depth, queue_errors, linux_burst in ((8, [], False), (9, [], True),
                                               (0, ["intentd"], True), (0, ["cloudlands-fe", "monorepo"], False)):
            with self.subTest(depth=depth, queue_errors=queue_errors):
                jobs = [{"status": "queued" if i % 2 else "in_progress", "labels": ["self-hosted", "Linux"]}
                        for i in range(depth)]
                jobs += [{"status": "in_progress", "labels": ["gh-linux-16x"]}] * 20
                result = self.assert_mac_route({"org": fleet, "jobs": jobs, "queueErrors": queue_errors},
                                              ["self-hosted", "macOS", "ARM64"])
                outputs = result["outputs"]
                self.assertEqual(json.loads(outputs["linux_labels"]), ["gh-linux-16x"] if linux_burst else ["self-hosted", "Linux", "X64"])
                self.assertEqual(json.loads(outputs["check_labels"]), ["gh-linux-16x"] if linux_burst else ["self-hosted", "Linux", "X64"])
                self.assertEqual(outputs["linux_burst"], str(linux_burst).lower())
                self.assertEqual(json.loads(outputs["windows_labels"]), ["self-hosted", "Windows", "X64"])
                self.assertEqual(outputs["windows_burst"], "false")
        for fixture in ({}, {"orgError": "403", "repoError": "403"},
                        {"org": [runner("Linux", "X64", status="offline"), runner("Windows", "X64", status="offline")]},
                        {"org": [runner("linux", "x64"), runner("windows", "x64")]}):
            with self.subTest(fixture=fixture):
                outputs = run_route(self.workflow, **fixture)["outputs"]
                self.assertEqual(json.loads(outputs["linux_labels"]), ["gh-linux-16x"])
                self.assertEqual(json.loads(outputs["check_labels"]), ["gh-linux-16x"])
                self.assertEqual(json.loads(outputs["windows_labels"]), ["gh-windows-16x"])
                self.assertEqual(outputs["linux_burst"], "true")
                self.assertEqual(outputs["windows_burst"], "true")

    def test_empty_outputs_use_hosted_macos_in_both_matrices(self):
        for job_id in ("build", "daemon-lifecycle"):
            for row in self.workflow["jobs"][job_id]["strategy"]["matrix"]["include"]:
                expected = ["macos-15"] if row.get("target") == "aarch64-apple-darwin" or row.get("platform") == "macos" else (
                    ["gh-linux-16x"] if row.get("platform") == "linux" else ["gh-windows-16x"])
                for outputs in ({}, {"macos_labels": "", "windows_labels": "", "linux_labels": ""}):
                    with self.subTest(job=job_id, row=row, outputs=outputs):
                        self.assertEqual(matrix_labels(row["os"], outputs), expected)
        mac = self.workflow["jobs"]["build"]["strategy"]["matrix"]["include"][0]
        self.assertEqual(mac["name"], "build (aarch64-apple-darwin, macos-14)")
        self.assertEqual(self.workflow["jobs"]["daemon-lifecycle"]["name"], "Daemon lifecycle (${{ matrix.platform }})")

    def test_hosted_macos_skips_persistent_cleanup_and_uses_cache(self):
        steps = {s.get("name"): s for s in self.workflow["jobs"]["build"]["steps"]}
        for labels in (["macos-15"], ["self-hosted", "macOS", "ARM64"]):
            for fast_path in ("", "true"):
                values = {**context("merge_group", fast_path=fast_path), "matrix.os": labels}
                for name in ("Ensure macOS CI disk headroom", "Point CARGO_TARGET_DIR outside the work dir (self-hosted)"):
                    self.assertEqual(condition(steps[name]["if"], values), fast_path != "true" and "self-hosted" in labels)
                self.assertEqual(condition(steps["Rust cache (GitHub-hosted runner only)"]["if"], values),
                                 fast_path != "true" and "self-hosted" not in labels)


class LifecycleCacheTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.job = _workflow_tests.load_ci(ROOT)["jobs"]["daemon-lifecycle"]
        cls.steps = cls.job["steps"]

    def step(self, name):
        matches = [s for s in self.steps if s.get("name") == name]
        self.assertEqual(len(matches), 1, name)
        return matches[0]

    def test_remote_cache_runs_only_on_hosted_runners(self):
        cache = next(s for s in self.steps if s.get("uses", "").startswith("Swatinem/rust-cache@"))
        self.assertEqual(cache["with"]["shared-key"], "daemon-lifecycle-${{ matrix.platform }}")
        for platform in ("Linux", "macOS", "Windows"):
            for hosted in (False, True):
                values = {**context("pull_request", platform=platform),
                          "matrix.os": [platform] if hosted else ["self-hosted", platform]}
                with self.subTest(platform=platform, hosted=hosted):
                    self.assertEqual(condition(cache.get("if", True), values), hosted)

    def test_persistent_paths_are_platform_scoped_and_precede_compilation(self):
        names = {"Point lifecycle target outside checkout (Linux)": {"Linux"},
                 "Point lifecycle target outside checkout (macOS and Windows)": {"macOS", "Windows"},
                 "Disk-space preflight (tinybox only)": {"Linux"},
                 "Ensure macOS CI disk headroom": {"macOS"}}
        toolchain = self.steps.index(self.step("Resolve pinned Rust channel"))
        for name, platforms in names.items():
            step = self.step(name)
            self.assertLess(self.steps.index(step), toolchain)
            self.assertFalse(step.get("continue-on-error", False))
            for platform in ("Linux", "macOS", "Windows"):
                for hosted in (False, True):
                    values = {**context("merge_group", platform=platform),
                              "matrix.os": [platform] if hosted else ["self-hosted", platform]}
                    self.assertEqual(condition(step.get("if", True), values), not hosted and platform in platforms)
        self.assertLess(self.steps.index(self.step("Disk-space preflight (tinybox only)")),
                        self.steps.index(self.step("Point lifecycle target outside checkout (Linux)")))

    def test_linux_slot_targets_survive_checkout_and_release_after_failure(self):
        step = self.step("Point lifecycle target outside checkout (Linux)")
        self.assertIn('/raid/ci-target/intentd/$RUNNER_NAME/daemon-lifecycle', step["run"])
        release = self.step("Release lifecycle target dir")
        self.assertEqual(self.steps[-1], release)
        for platform in ("Linux", "macOS", "Windows"):
            for marked in (False, True):
                values = {**context("pull_request", platform=platform),
                          "matrix.os": ["self-hosted", platform],
                          "env.CARGO_TARGET_DIR": "/fixture/target" if marked else ""}
                for success in (False, True):
                    self.assertEqual(condition(release["if"], values, success=success, cancelled=True),
                                     platform == "Linux" and marked)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            checkout = root / "checkout"
            checkout.mkdir()
            subprocess.run(["git", "init", "-q", str(checkout)], check=True)
            targets = []
            for slot in ("slot1", "slot2"):
                envfile = root / (slot + ".env")
                body = step["run"].replace("/raid/ci-target", str(root / "ci-target"))
                result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", body], cwd=ROOT,
                                        env={**os.environ, "RUNNER_NAME": slot, "GITHUB_ENV": str(envfile)},
                                        capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 0, result.stderr)
                target = Path(envfile.read_text().strip().removeprefix("CARGO_TARGET_DIR="))
                self.assertEqual(target, root / "ci-target/intentd" / slot / "daemon-lifecycle")
                self.assertTrue((target / ".in-use").is_file())
                artifact = target / "warm-artifact"
                artifact.touch()
                disposable = checkout / "target"
                disposable.mkdir()
                (disposable / "cold-artifact").touch()
                subprocess.run(["git", "-C", str(checkout), "clean", "-ffdx"], check=True, capture_output=True)
                self.assertFalse(disposable.exists())
                self.assertTrue(artifact.exists())
                result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", release["run"]], cwd=ROOT,
                                        env={**os.environ, "CARGO_TARGET_DIR": str(target)}, capture_output=True, timeout=5)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertFalse((target / ".in-use").exists())
                self.assertTrue(artifact.exists())
                targets.append(target)
            self.assertNotEqual(*targets)

    def test_macos_and_windows_keep_native_persistent_paths(self):
        step = self.step("Point lifecycle target outside checkout (macOS and Windows)")
        self.assertEqual(step["shell"], "bash")
        bash = self.step("Put Git Bash first on PATH (Windows)")
        self.assertLess(self.steps.index(bash), self.steps.index(step))
        for platform, profile in (("macos", ""), ("windows", "C:\\Users\\CI User")):
            with tempfile.TemporaryDirectory() as tmp:
                envfile = Path(tmp) / "env"
                body = step["run"].replace("${{ matrix.platform }}", platform)
                result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", body], cwd=ROOT,
                                        env={**os.environ, "USERPROFILE": profile, "GITHUB_ENV": str(envfile)},
                                        capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(envfile.read_text().strip(),
                                 f"CARGO_TARGET_DIR={profile or os.environ['HOME']}/ci-target/intentd/daemon-lifecycle-{platform}")

    def test_linux_preflight_executes_only_safe_fixture_evictions(self):
        step = self.step("Disk-space preflight (tinybox only)")
        for scenario, age_minutes in (("adequate", 5000), ("stale", 5000), ("lru", 60), ("exhausted", 0)):
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                raid = root / "raid"
                targets = raid / "ci-target/intentd"
                own = targets / "self/daemon-lifecycle"
                live = targets / "other/daemon-lifecycle"
                idle = targets / "idle/daemon-lifecycle"
                for target in (own, live, idle):
                    (target / "debug/deps").mkdir(parents=True)
                    (target / "debug/deps/artifact").touch()
                    for entry in sorted(target.rglob("*"), reverse=True):
                        os.utime(entry, (1, 1))
                    os.utime(target, (1, 1))
                (live / ".in-use").touch()
                now = (live / ".in-use").stat().st_mtime
                for entry in [idle, *idle.rglob("*")]:
                    os.utime(entry, (now - age_minutes * 60,) * 2)
                stub = root / "df"
                available = 20 if scenario == "adequate" else 5
                stub.write_text("#!/bin/sh\nblocks=" + str(available * 1048576) +
                                "\n[ -d " + shlex.quote(str(idle)) + " ] || blocks=20971520\n" +
                                "printf 'Filesystem 1024-blocks Used Available Capacity Mounted\nfixture 99999999 0 %s 0%% /fixture\n' \"$blocks\"\n")
                stub.chmod(0o755)
                # Rewrite every absolute production root before running deletion.
                body = step["run"].replace("/raid", str(raid))
                self.assertNotIn("/raid/ci-target", body.replace(str(raid), "FIXTURE"))
                result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", body], cwd=ROOT,
                                        env={**os.environ, **{k: str(v) for k, v in step["env"].items()},
                                             "RUNNER_NAME": "self", "PATH": str(root) + os.pathsep + os.environ["PATH"]},
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 1 if scenario == "exhausted" else 0, result.stdout + result.stderr)
                self.assertEqual(idle.exists(), scenario in ("adequate", "exhausted"))
                self.assertTrue((own / "debug/deps/artifact").exists())
                self.assertTrue((live / "debug/deps/artifact").exists())

    def test_linux_preflight_keeps_existing_prune_rails(self):
        step = self.step("Disk-space preflight (tinybox only)")
        self.assertEqual(step["env"]["JOB_TARGET_SUBDIR"], "daemon-lifecycle")
        self.assertGreaterEqual(int(step["env"]["NEED_GIB"]), 15)
        body = step["run"]
        for required in ('/raid/ci-target/intentd/$RUNNER_NAME/$JOB_TARGET_SUBDIR',
                         'is-purgeable "$dir" --quiet-min "$quiet_min"',
                         'purge-candidates /raid/ci-target',
                         'evict_until_free pruning 2880 --skip-dir "$own_dir"',
                         'evict_until_free evicting 15 --skip-slot "$RUNNER_NAME"', 'exit 1'):
            self.assertIn(required, body)
        self.assertLess(body.index('is-purgeable'), body.index('rm -rf'))


class NativeRoutingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = _workflow_tests.load_ci(ROOT)

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
            stub = Path(tmp) / "python3"
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
                    steps = workflow["jobs"]["daemon-lifecycle"]["steps"]
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
        for expression in ("github.event_name == 'merge_group'", "always()"):
            with self.subTest(expression=expression):
                workflow = copy.deepcopy(self.workflow)
                workflow["jobs"]["build"]["if"] = expression
                self.assertTrue(any("wrong jobs" in error for error in routing_errors(workflow)))

    def test_producer_preserves_feature_off_check_and_full_clippy(self):
        spec = importlib.util.spec_from_file_location('native', ROOT / 'scripts/ci-native-artifacts.py')
        native = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(native)
        for platform in ('macos', 'windows'):
            commands = native.check_commands(platform, 'merge_group', 'native-triple', '/target')
            self.assertEqual(commands, [
                ['cargo', 'check', '--workspace', '--locked', '--target', 'native-triple', '--target-dir', '/target'],
                ['cargo', 'clippy', '--workspace', '--all-targets', '--locked', '--target', 'native-triple', '--target-dir', '/target', '--', '-D', 'warnings'],
            ])
            self.assertEqual(len(native.check_commands(platform, 'pull_request', 'native-triple', '/target')), 1)

    def test_mutation_cannot_accept_skipped_queue_build(self):
        workflow = copy.deepcopy(self.workflow)
        step = next(s for s in workflow["jobs"]["gate"]["steps"] if s.get("name") == "Check results")
        step["run"] = step["run"].replace('${{ needs.build.result }}', "success")
        lifecycle = copy.deepcopy(self.workflow)
        lifecycle["jobs"]["gate"]["steps"][0]["run"] = step["run"].replace("${{ needs.daemon-lifecycle.result }}", "success")
        self.assertIn("merge_group: daemon-lifecycle=skipped", gate_errors(lifecycle))
        self.assertIn("push: build=skipped", gate_errors(workflow))

    def test_mutation_cannot_drop_gate_dependency(self):
        workflow = copy.deepcopy(self.workflow)
        workflow["jobs"]["gate"]["needs"].remove("build")
        self.assertIn("gate dependencies changed", gate_errors(workflow))

    def test_lifecycle_matrix_is_required_and_runs_real_proof(self):
        job = self.workflow["jobs"]["daemon-lifecycle"]
        self.assertEqual({row["platform"] for row in job["strategy"]["matrix"]["include"]}, {"macos", "windows"})
        linux = self.workflow["jobs"]["check"]
        self.assertTrue(any(s.get("name") == "Linux lifecycle runtime proof" and "test-daemon-lifecycle.py --archives" in s.get("run", "") for s in linux["steps"]))
        self.assertFalse(job["strategy"]["fail-fast"])
        step = next(step for step in job["steps"] if step.get("name") == "Native lifecycle runtime proof")
        self.assertIn("python3 -I -B scripts/test-daemon-lifecycle.py", step["run"])
        self.assertIn('--archives "$RUNNER_TEMP/native-artifacts"', step["run"])
        self.assertNotIn("if", step)
        self.assertFalse(step.get("continue-on-error", False))
        for event in ("pull_request", "merge_group"):
            workflow = copy.deepcopy(self.workflow)
            gate = next(step for step in workflow["jobs"]["gate"]["steps"] if step.get("name") == "Check results")
            gate["run"] = gate["run"].replace("${{ needs.daemon-lifecycle.result }}", "success")
            self.assertIn(f"{event}: daemon-lifecycle=skipped", gate_errors(workflow))

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
