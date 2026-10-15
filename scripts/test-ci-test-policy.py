#!/usr/bin/env python3
"""Required CI Rust test steps must match the local wrapper's caller policy.

Reads real YAML (PyYAML, already installed in release-scripts). The command
scan covers current bare and rustup-run test commands, not echoed plans or
command-substitution discovery lists. Current indirect routes have explicit
registration plus actual stub-child probes below, not a general shell parser.
Mutation tests ensure removed/disabled step policy and missing wiring fail.
"""

import importlib.util
import copy
import json
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import unittest

import yaml


ROOT = Path(__file__).resolve().parent.parent

_workflow_spec = importlib.util.spec_from_file_location('workflow_tests', ROOT / 'scripts/ci-workflow-tests.py')
_workflow_tests = importlib.util.module_from_spec(_workflow_spec)
_workflow_spec.loader.exec_module(_workflow_tests)

JOBS = ("check", "daemon-lifecycle", "coverage-e2e", "coverage-all", "coverage-changed")
# These required steps launch through Python or a Bash array; scanning shell
# command prefixes cannot discover their runtime children. Execute their real
# workflow bodies with a stub rustup in test_indirect_workflow_child_environments.
INDIRECT_TEST_STEPS = {
    ("daemon-lifecycle", "Tunnel deadline and TLS controls (macOS runtime)"),
    ("daemon-lifecycle", "Retained workspace CoW fixtures (macOS runtime)"),
}
COMMAND = re.compile(
    r"^(?:[A-Z_]+=\S+\s+)*(?:(?:rustup\s+run\s+\S+\s+)?cargo\s+(?:test\b|nextest\s+run\b|llvm-cov\b[^\n]*\bnextest\b)"
    r"|(?:python3 (?:-[IB] )*scripts/ci-consumer-process\.py -- )?python3 (?:-[IB] )*scripts/(?:ci-native-artifacts\.py libtest|ci-linux-artifacts\.py (?:test|coverage|callback)|test-daemon-lifecycle\.py --archives)\b"
    r"|\./scripts/(?:coverage-e2e|coverage-all|changed-tests)\.sh\b)"
)


def test_steps(workflow):
    for name in JOBS:
        job = workflow["jobs"][name]
        for step in job["steps"]:
            run = str(step.get("run", ""))
            if (name, step.get("name")) in INDIRECT_TEST_STEPS or any(COMMAND.search(line.strip()) and "--dry-run" not in line for line in run.splitlines()):
                yield name, job, step


def coverage_budget_violations(workflow):
    """Keep the combined instrumented lane finite, with post-action headroom."""
    job = workflow["jobs"]["coverage-changed"]
    limits = {
        "Run changed tests under llvm-cov": 20,
        "Run callback fixture composition under llvm-cov": 8,
    }
    errors = []
    total = job.get("timeout-minutes")
    if type(total) is not int or total != sum(limits.values()) + 2:
        errors.append("coverage-changed must reserve 2m outside its bounded test phases")
    for name, limit in limits.items():
        steps = [step for step in job["steps"] if step.get("name") == name]
        if len(steps) != 1 or type(steps[0].get("timeout-minutes")) is not int or steps[0]["timeout-minutes"] != limit:
            errors.append(f"coverage-changed: {name} must retain its {limit}m phase bound")
    combined = workflow["jobs"]["coverage-all"]
    steps = [step for step in combined["steps"] if step.get("name") == "Run combined coverage"]
    if type(combined.get("timeout-minutes")) is not int or combined["timeout-minutes"] != 30:
        errors.append("coverage-all must reserve 2m outside its 28m combined phase")
    if len(steps) != 1 or type(steps[0].get("timeout-minutes")) is not int or steps[0]["timeout-minutes"] != 28:
        errors.append("coverage-all must retain its 28m compile/test/report bound")
    return errors


def profile_pool_violations(workflow):
    errors = []
    producer = workflow['jobs']['check']
    proofs = [s for s in producer['steps'] if 'scripts/test-coverage-profile-pool.py' in s.get('run', '')]
    if len(proofs) != 1:
        errors.append('check: require exactly one profile pooling proof')
    else:
        proof = proofs[0]
        body = proof['run']
        if (proof.get('if') is not None or proof.get('continue-on-error')
                or 'LLVM_PROFILE_FILE_NAME="intent-%8m.profraw"' not in body
                or 'ci-linux-artifacts.py produce' not in body
                or body.index('test-coverage-profile-pool.py') > body.index('ci-linux-artifacts.py produce')):
            errors.append('check: require fresh profile proof before producing instrumented archives')
    for job_id in ('coverage-all', 'coverage-changed'):
        job = workflow['jobs'][job_id]
        if (job.get('env', {}).get('LLVM_PROFILE_FILE_NAME') != 'intent-%8m.profraw'
                or 'LLVM_PROFILE_FILE' in job.get('env', {})):
            errors.append(f'{job_id}: retain the bounded profile pool')
        for step in job['steps']:
            if ('test-coverage-profile-pool.py' in step.get('run', '')
                    or any(key in step.get('env', {}) for key in ('LLVM_PROFILE_FILE_NAME', 'LLVM_PROFILE_FILE'))):
                errors.append(f'{job_id}: no consumer compiler proof or profile override')
    return errors


def violations(workflow, policy):
    errors = coverage_budget_violations(workflow) + profile_pool_violations(workflow)
    found = set()
    for name, job, step in test_steps(workflow):
        found.add(name)
        env = {**workflow.get("env", {}), **job.get("env", {}), **step.get("env", {})}
        command_policies = [str(env.get("INTENTD_ASSERT_BOUND_CALLER", ""))]
        # A step-wide unset previously disabled every retained CoW fixture.
        if re.search(r"^\s*unset\b[^\n]*\bINTENTD_ASSERT_BOUND_CALLER\b", str(step.get("run", "")), re.M):
            command_policies.append("")
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
    if not any(step.get("run") == "python3 -I -B scripts/ci-linux-artifacts.py test -- cargo test --workspace --test '*_lint'" for step in source_lints):
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


# Independently audited from ci.yml, not generated by test_steps/COMMAND. Keep
# this inventory explicit so scanner omissions cannot also erase mutations.
REQUIRED_TEST_STEPS = {
    ("check", "Linux lifecycle runtime proof"): 1,
    ("check", "Real import and public session contract"): 1,
    ("check", "Source lints"): 1,
    ("check", "GitLab auth, invitations and checkout over real sockets with explicit test transports"): 1,
    ("check", "Coverage-skipped tests (STAB-40/43)"): 2,
    ("check", "Installer guard tests (install_ps1_owner)"): 1,
    # None: indirect Python/Bash-array routes are executed with a stub below.
    ("daemon-lifecycle", "Tunnel deadline and TLS controls (macOS runtime)"): None,
    ("daemon-lifecycle", "Retained workspace CoW fixtures (macOS runtime)"): None,
    ("daemon-lifecycle", "Native lifecycle runtime proof"): 1,
    ("daemon-lifecycle", "Headless workspace setup (Windows runtime)"): 1,
    ("daemon-lifecycle", "Codex diagnostic process ownership (Windows runtime)"): 1,
    ("daemon-lifecycle", "Codex focused diagnostics (macOS runtime)"): 2,
    ("coverage-e2e", "Run e2e coverage"): 1,
    ("coverage-all", "Run combined coverage"): 1,
    ("coverage-changed", "Run changed tests under llvm-cov"): 1,
    ("coverage-changed", "Run callback fixture composition under llvm-cov"): 1,
}


class CiPolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = _workflow_tests.load_ci(ROOT)

    def test_required_ci_matches_wrapper(self):
        result = subprocess.run(
            ["bash", str(ROOT / "scripts/with-test-policy.sh"), "sh", "-c", 'printf "%s" "$INTENTD_ASSERT_BOUND_CALLER"'],
            env={**os.environ, "INTENTD_ASSERT_BOUND_CALLER": "0"}, capture_output=True, text=True, timeout=5,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "1")
        self.assertEqual(violations(self.workflow, result.stdout), [])

    def test_each_required_test_step_rejects_missing_or_disabled_policy(self):
        for name, step_name in REQUIRED_TEST_STEPS:
            for value in (None, "0", "false"):
                with self.subTest(job=name, step=step_name, value=value):
                    workflow = copy.deepcopy(self.workflow)
                    mutated = next(s for s in workflow["jobs"][name]["steps"] if s.get("name") == step_name)
                    env = mutated.setdefault("env", {})
                    if value is None:
                        env.pop("INTENTD_ASSERT_BOUND_CALLER", None)
                    else:
                        env["INTENTD_ASSERT_BOUND_CALLER"] = value
                    self.assertIn(
                        f"{name}: {step_name} must arm INTENTD_ASSERT_BOUND_CALLER=1",
                        violations(workflow, "1"),
                    )

    def test_discovery_matches_independent_required_step_inventory(self):
        discovered = {}
        for name, _, step in test_steps(self.workflow):
            discovered[(name, step["name"])] = sum(
                bool(COMMAND.search(line.strip())) and "--dry-run" not in line
                for line in step["run"].splitlines()
            ) or None
        self.assertEqual(discovered, REQUIRED_TEST_STEPS)

    def test_indirect_route_cannot_unset_policy(self):
        for step_name in ("Tunnel deadline and TLS controls (macOS runtime)",
                          "Retained workspace CoW fixtures (macOS runtime)"):
            with self.subTest(step=step_name):
                workflow = copy.deepcopy(self.workflow)
                step = next(s for s in workflow['jobs']['daemon-lifecycle']['steps'] if s.get('name') == step_name)
                step['run'] = 'unset INTENTD_ASSERT_BOUND_CALLER\n' + step['run']
                self.assertIn(f"daemon-lifecycle: {step_name} must arm INTENTD_ASSERT_BOUND_CALLER=1", violations(workflow, "1"))

    def test_callback_scope_changes_select_validation_without_rust_changes(self):
        job = self.workflow["jobs"]["coverage-changed"]
        step = next(s for s in job["steps"] if s.get("name") == "Select changed tests (dry run)")
        paths = ("scripts/callback-build-scope.py", "scripts/changed-tests.sh",
                 "scripts/test-callback-build-scope.py", "scripts/unrelated.txt")
        for path in paths:
            with self.subTest(path=path), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                repo = root / "repo"
                (repo / "scripts").mkdir(parents=True)
                for name in paths:
                    (repo / name).write_text((ROOT / name).read_text() if (ROOT / name).exists() else "unrelated")
                (repo / "scripts/changed-tests.sh").chmod(0o755)
                env = {**os.environ, **{k: str(v) for k, v in job["env"].items()},
                       "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1",
                       "GIT_AUTHOR_NAME": "test", "GIT_AUTHOR_EMAIL": "test@example.invalid",
                       "GIT_COMMITTER_NAME": "test", "GIT_COMMITTER_EMAIL": "test@example.invalid",
                       "RUNNER_TEMP": str(root), "GITHUB_OUTPUT": str(root / "output"),
                       "GITHUB_STEP_SUMMARY": str(root / "summary")}
                for args in (["init", "-q"], ["add", "."], ["commit", "-q", "-m", "fixture"]):
                    subprocess.run(["git", *args], cwd=repo, env=env, check=True)
                env["BASE_SHA"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, env=env, text=True).strip()
                with (repo / path).open("a") as file:
                    file.write("\n# changed\n")
                subprocess.run(["git", "add", "."], cwd=repo, env=env, check=True)
                subprocess.run(["git", "commit", "-q", "-m", "change"], cwd=repo, env=env, check=True)
                result = subprocess.run(["bash", "-c", step["run"]], cwd=repo, env=env,
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 0, result.stderr)
                expected = "false" if path.endswith("unrelated.txt") else "true"
                output = (root / "output").read_text()
                self.assertIn("callback_fixture_changed=" + expected, output)
                self.assertIn("has_selection=" + expected, output)
                self.assertEqual(json.loads((root / "coverage-changed-plan.json").read_text())["plans"], [])

    def test_profile_pool_proof_stays_in_producer_and_consumers_keep_budget(self):
        job = self.workflow['jobs']['check']
        proof = [s for s in job['steps'] if 'scripts/test-coverage-profile-pool.py' in s.get('run', '')]
        self.assertEqual(len(proof), 1)
        self.assertIn('LLVM_PROFILE_FILE_NAME="intent-%8m.profraw"', proof[0]['run'])
        self.assertIn('ci-linux-artifacts.py produce', proof[0]['run'])
        self.assertNotIn('if', proof[0])
        self.assertNotIn('continue-on-error', proof[0])
        for name in ('coverage-all', 'coverage-e2e', 'coverage-changed'):
            consumer = self.workflow['jobs'][name]
            self.assertEqual(consumer['timeout-minutes'], 30)
            self.assertFalse(any('test-coverage-profile-pool.py' in s.get('run', '') for s in consumer['steps']))
        self.assertEqual(self.workflow['jobs']['coverage-all']['env']['LLVM_PROFILE_FILE_NAME'], 'intent-%8m.profraw')

    def test_coverage_budget_rejects_lost_bounds_and_cleanup_margin(self):
        self.assertEqual(coverage_budget_violations(self.workflow), [])
        for name in (None, "Run changed tests under llvm-cov",
                     "Run callback fixture composition under llvm-cov"):
            for limit in (None, 0, True, 60):
                with self.subTest(phase=name, limit=limit):
                    workflow = copy.deepcopy(self.workflow)
                    job = workflow["jobs"]["coverage-changed"]
                    target = job if name is None else next(s for s in job["steps"] if s.get("name") == name)
                    if limit is None:
                        target.pop("timeout-minutes")
                    else:
                        target["timeout-minutes"] = limit
                    self.assertTrue(coverage_budget_violations(workflow))
                    self.assertTrue(violations(workflow, "1"))

    def test_combined_coverage_budget_rejects_lost_bounds_and_report_margin(self):
        self.assertEqual(coverage_budget_violations(self.workflow), [])
        for phase in (False, True):
            for limit in (None, 0, True, 45, 60):
                with self.subTest(phase=phase, limit=limit):
                    workflow = copy.deepcopy(self.workflow)
                    job = workflow["jobs"]["coverage-all"]
                    target = next(s for s in job["steps"] if s.get("name") == "Run combined coverage") if phase else job
                    if limit is None:
                        target.pop("timeout-minutes")
                    else:
                        target["timeout-minutes"] = limit
                    self.assertTrue(coverage_budget_violations(workflow))
                    self.assertTrue(violations(workflow, "1"))

    def test_profile_pool_mutations_fail_closed(self):
        self.assertEqual(profile_pool_violations(self.workflow), [])
        for name in ('coverage-all', 'coverage-changed'):
            for mutation in ('missing', 'per-process', 'direct', 'step-override'):
                with self.subTest(job=name, mutation=mutation):
                    workflow = copy.deepcopy(self.workflow)
                    job = workflow['jobs'][name]
                    if mutation == 'missing':
                        job['env'].pop('LLVM_PROFILE_FILE_NAME')
                    elif mutation == 'per-process':
                        job['env']['LLVM_PROFILE_FILE_NAME'] = 'intent-%p-%32m.profraw'
                    elif mutation == 'direct':
                        job['env']['LLVM_PROFILE_FILE'] = 'intent-%p.profraw'
                    else:
                        job['steps'][-1]['env'] = {'LLVM_PROFILE_FILE_NAME': 'intent-%p.profraw'}
                    self.assertTrue(profile_pool_violations(workflow))
        for mutation in ('missing', 'skipped', 'ignored', 'late'):
            with self.subTest(producer=mutation):
                workflow = copy.deepcopy(self.workflow)
                steps = workflow['jobs']['check']['steps']
                proof = next(s for s in steps if 'scripts/test-coverage-profile-pool.py' in s.get('run', ''))
                if mutation == 'missing':
                    steps.remove(proof)
                elif mutation == 'skipped':
                    proof['if'] = False
                elif mutation == 'ignored':
                    proof['continue-on-error'] = True
                else:
                    proof['run'] = 'python3 scripts/ci-linux-artifacts.py produce\n' + proof['run']
                self.assertTrue(profile_pool_violations(workflow))

    def test_indirect_workflow_child_environments(self):
        for step_name, expected_tests in (
            ("Retained workspace CoW fixtures (macOS runtime)", 6),
        ):
            with self.subTest(step=step_name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / "scripts").mkdir()
                stub = root / "scripts/ci-native-artifacts.py"
                stub.write_text(
                    f"#!{sys.executable} -S\n"
                    "import json,os,sys\n"
                    "args=sys.argv[1:]\n"
                    "with open(os.environ['POLICY_CHILD_LOG'],'a') as log:\n"
                    " log.write(json.dumps([args,os.environ.get('INTENTD_ASSERT_BOUND_CALLER')])+'\\n')\n"
                    "if '--no-run' in args: sys.exit(0)\n"
                    "test=args[args.index('--')+1]\n"
                    "if '--list' in args:\n"
                    " print(test+': test'); print('1 test, 0 benchmarks'); sys.exit(0)\n"
                    "print('running 1 test')\n"
                    "print('test result: ok. 1 passed; 0 failed; 0 ignored;')\n"
                    "print('CoW fixture clone and independent contents verified')\n"
                    "print('CoW fixture proof scratch cleaned')\n"
                    "print('CoW fixture assertions passed: '+test.split('::')[-1])\n"
                )
                stub.chmod(0o755)
                step = next(s for s in self.workflow['jobs']['daemon-lifecycle']['steps'] if s.get('name') == step_name)
                env = {**os.environ, 'INTENTD_ASSERT_BOUND_CALLER': '0', **step.get('env', {})}
                env = {k: re.sub(r'\$\{\{.*?\}\}', 'fixture', str(v)) for k, v in env.items()}
                env.update(PATH=str(root)+os.pathsep+os.environ['PATH'], RUNNER_TEMP=str(root), POLICY_CHILD_LOG=str(root/'children.jsonl'))
                run = step['run'].replace('${{ matrix.target }}', 'fixture')
                result = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', run], cwd=root, env=env, capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
                calls = [json.loads(line) for line in (root/'children.jsonl').read_text().splitlines()]
                runtime = [(args, policy) for args, policy in calls if '--nocapture' in args]
                self.assertEqual(len(runtime), expected_tests)
                for args, policy in runtime:
                    self.assertEqual(policy, '1', args)
                    self.assertIn('--exact', args)
                    self.assertIn('--test-threads=1', args)
                self.assertEqual(len([args for args, _ in calls if '--list' in args]), expected_tests)

    def test_rustup_runtime_commands_are_discovered(self):
        for command in (
            'rustup run "$INTENTD_DIAGNOSTIC_TOOLCHAIN" cargo test -p intent-services --lib',
            'INTENTD_ASSERT_BOUND_CALLER=0 rustup run stable cargo test -p intentd',
        ):
            with self.subTest(command=command):
                workflow = copy.deepcopy(self.workflow)
                workflow["jobs"]["daemon-lifecycle"]["steps"].append({
                    "name": "New platform tests", "run": command,
                })
                self.assertIn("daemon-lifecycle: New platform tests must arm INTENTD_ASSERT_BOUND_CALLER=1", violations(workflow, "1"))

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


class TunnelArtifactTests(unittest.TestCase):
    """Execute the real tunnel workflow with Cargo artifacts and native children."""

    TESTS = {
        "tunnel::tests::connect_pending_deadline_emits_terminal_open_err",
        "tunnel::tests::connect_reset_preserves_error_before_deadline",
        "tunnel::tests::connect_success_before_deadline_relays",
        "tunnel_open_echo_eof_close_lifecycle",
        "tunnel_open_err_for_closed_port_keeps_connection_alive",
        "tunnel_close_tears_down_and_frees_stream_id",
        "tunnel_connections_counted_in_health_and_stopped",
        "tunnel_unauthenticated_upgrade_rejected",
    }

    @classmethod
    def setUpClass(cls):
        workflow = _workflow_tests.load_ci(ROOT)
        cls.step = next(s for s in workflow["jobs"]["daemon-lifecycle"]["steps"]
                        if s.get("name") == "Tunnel deadline and TLS controls (macOS runtime)")

    def probe(self, mode="valid", *, inherited_libraries=False, timeout_probe=False):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root / "fixture binary"
            binary.write_text(f"#!{sys.executable} -S\n" + r'''
import json, os, signal, subprocess, sys
args = sys.argv[1:]
mode = os.environ["TUNNEL_MODE"]
with open(os.environ["TUNNEL_LOG"], "a") as log:
    log.write(json.dumps({"kind": "native", "args": args, "cwd": os.getcwd(),
                         "policy": os.environ.get("INTENTD_ASSERT_BOUND_CALLER"),
                         "libraries": os.environ.get("DYLD_FALLBACK_LIBRARY_PATH")}) + "\n")
test = args[0]
phase = "list" if "--list" in args else "run"
if mode == "hang-" + phase:
    child = subprocess.Popen([sys.executable, "-S", "-c", "import signal; signal.pause()"])
    with open(os.environ["TUNNEL_PIDS"], "w") as f: json.dump([os.getpid(), child.pid], f)
    signal.pause()
if mode == "fail-" + phase: sys.exit(24)
if phase == "list":
    if mode != "missing-test": print(test + ": test")
    print("2 tests, 0 benchmarks" if mode == "extra-test" else "1 test, 0 benchmarks")
else:
    print("running 0 tests" if mode == "zero-tests" else "running 1 test")
    print("test result: ok. 0 passed; 0 failed; 1 ignored;" if mode == "ignored-test"
          else "test result: ok. 1 passed; 0 failed; 0 ignored;")
''')
            binary.chmod(0o755)
            (root / 'scripts').mkdir()
            for name in ('ci-native-artifacts.py', 'ci-test-artifacts.py', 'ci-build-identity.py'):
                (root / 'scripts' / name).write_text((ROOT / 'scripts' / name).read_text())
            artifact_root = root / 'owned artifacts' / 'run-42-3-native'
            artifact_root.mkdir(parents=True)
            prepared = {}
            for variant, package, binary_id in (('transport', 'intent-transport', 'intent-transport'),
                                                 ('daemon', 'intentd', 'intentd::e2e_wss_tunnel')):
                package_dir = root / 'crates' / package
                package_dir.mkdir(parents=True)
                target = root / 'target with spaces'
                target.mkdir(exist_ok=True)
                executable = target / package
                executable.write_bytes(binary.read_bytes())
                executable.chmod(0o755)
                tests = {binary_id: {'path': package, 'cwd': 'crates/' + package}}
                if mode == 'missing-artifact': tests = {}
                if mode == 'missing-file': executable.unlink()
                if mode == 'wrong-package': tests[binary_id]['cwd'] = 'crates/absent'
                if mode == 'wrong-target': tests[binary_id]['path'] = 'absent'
                prepared[variant] = {'source': str(root), 'target': str(target), 'producer_group': 'fixture',
                    'runtime': {'tests': tests, 'companions': {}, 'library_dirs': ['lib', 'deps', 'build/native lib']}}
            (artifact_root / 'prepared.json').write_text(json.dumps(prepared))
            env = {**os.environ, **self.step["env"], "INTENTD_TUNNEL_TOOLCHAIN": "pinned-channel",
                   "INTENTD_TUNNEL_TARGET": "aarch64-apple-darwin", "TUNNEL_ROOT": tmp,
                   "RUNNER_TEMP": str(root), "NATIVE_ARTIFACT_ROOT": str(artifact_root), "TUNNEL_MODE": mode, "TUNNEL_LOG": str(root / "calls.jsonl"),
                   "TUNNEL_PIDS": str(root / "pids.json"), "PATH": tmp + os.pathsep + os.environ["PATH"]}
            env.pop("DYLD_FALLBACK_LIBRARY_PATH", None)
            if inherited_libraries:
                env["DYLD_FALLBACK_LIBRARY_PATH"] = str(root / "existing lib")
            body = self.step["run"]
            if timeout_probe:
                self.assertIn("communicate(timeout=30)", body)
                body = body.replace("communicate(timeout=30)", "communicate(timeout=0.5)")
            states = []
            try:
                result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", body],
                                        cwd=root, env=env, capture_output=True, text=True, timeout=10)
            finally:
                # A broken timeout wrapper must not leak the regression's children.
                if (root / "pids.json").exists():
                    pids = json.loads((root / "pids.json").read_text())
                    states = [subprocess.run(["ps", "-o", "stat=", "-p", str(pid)],
                              capture_output=True, text=True).stdout.strip() for pid in pids]
                    try:
                        os.killpg(pids[0], signal.SIGKILL)
                    except ProcessLookupError:
                        pass
            calls = [json.loads(line) for line in (root / "calls.jsonl").read_text().splitlines()] if (root / "calls.jsonl").exists() else []
            if mode.startswith("hang-"):
                self.assertTrue((root / "pids.json").exists(), result.stdout + result.stderr)
                for state in states:
                    self.assertTrue(not state or state.startswith("Z"), f"timed out process still alive: {state}")
            return result, calls, tmp

    def test_built_artifacts_are_reused_without_cargo_inside_runtime_budget(self):
        for inherited in (False, True):
            with self.subTest(inherited_libraries=inherited):
                result, calls, tmp = self.probe(inherited_libraries=inherited)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                builds = [c for c in calls if c["kind"] == "rustup" and "cargo" in c["args"]]
                self.assertEqual(builds, [], 'consumer must never invoke rustup or Cargo')
                native = [c for c in calls if c["kind"] == "native"]
                self.assertEqual(len(native), 16)
                self.assertEqual({c["args"][0] for c in native if "--list" in c["args"]}, self.TESTS)
                self.assertEqual({c["args"][0] for c in native if "--nocapture" in c["args"]}, self.TESTS)
                for call in native:
                    test = call["args"][0]
                    package = "intent-transport" if test.startswith("tunnel::") else "intentd"
                    self.assertEqual(call["cwd"], str(Path(tmp) / "crates" / package))
                    self.assertEqual(call["policy"], "1")
                    self.assertIn("--exact", call["args"])
                    libraries = call["libraries"].split(os.pathsep)
                    self.assertIn(str(Path(tmp) / "target with spaces/deps"), libraries)
                    self.assertIn(str(Path(tmp) / "target with spaces/lib"), libraries)
                    self.assertIn(str(Path(tmp) / "target with spaces/build/native lib"), libraries)
                    self.assertNotIn("/outside/target", libraries)
                    if inherited: self.assertIn(str(Path(tmp) / "existing lib"), libraries)
                    if "--list" not in call["args"]:
                        self.assertIn("--nocapture", call["args"])
                        self.assertIn("--test-threads=1", call["args"])

    def test_artifact_or_test_failure_cannot_become_success(self):
        for mode in ("missing-artifact", "missing-file", "wrong-package", "wrong-target",
                     "fail-list", "missing-test", "extra-test", "fail-run", "zero-tests", "ignored-test"):
            with self.subTest(mode=mode):
                result, calls, _ = self.probe(mode)
                self.assertNotEqual(result.returncode, 0)
                native = [c for c in calls if c["kind"] == "native"]
                if mode in ("missing-artifact", "missing-file", "wrong-package", "wrong-target"):
                    self.assertEqual(native, [])
                else:
                    self.assertTrue(native, "must actually exercise native failure")

    def test_discovery_and_runtime_hangs_kill_process_group_and_fail(self):
        for mode in ("hang-list", "hang-run"):
            with self.subTest(mode=mode):
                result, _, _ = self.probe(mode, timeout_probe=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("TimeoutExpired", result.stderr)
        self.assertEqual(self.step["timeout-minutes"], 20)


class CallbackDiscoveryTests(unittest.TestCase):
    """Execute the actual workflow shell; stub only Cargo and selection inputs."""

    @classmethod
    def setUpClass(cls):
        cls.workflow = _workflow_tests.load_ci(ROOT)
        cls.job = cls.workflow["jobs"]["coverage-changed"]
        cls.step = next(s for s in cls.job["steps"] if s.get("name") == "Run callback fixture composition under llvm-cov")

    def test_workflow_uses_compile_free_callback_consumer(self):
        self.assertEqual(self.step["run"], "python3 -I -B scripts/ci-consumer-process.py -- python3 -I -B scripts/ci-linux-artifacts.py callback")
        # Inventory, runtime policy and failure propagation are exercised against
        # the actual archive consumer in test-ci-linux-artifacts.py.

    def test_callback_inventory_is_exact(self):
        self.assertEqual(self.job["env"]["CALLBACK_FIXTURE_TESTS"].split(), [
            "callback_registration::tests::actual_adapter_sdk_and_rust_mcp_deliver_distinct_callback_preserving_old_result",
            "callback_registration::tests::actual_adapter_replacement_rejects_old_query_and_disabled_never_registers",
            "agent_manager::repository_origin::callback_delivery::tests::live_context::confirmed_context_actual_created_3_2_reaches_original_guidance_boundary",
            "agent_manager::repository_origin::callback_delivery::tests::live_context::confirmed_context_saved_legacy_and_disabled_outbound_messages_remain_ordinary",
            "agent_manager::repository_origin::callback_delivery::tests::live_context::confirmed_context_load_and_failed_load_new_keep_original_producers_and_capture",
            "agent_manager::repository_origin::callback_delivery::tests::live_context::confirmed_context_successful_zero_pending_absent_and_failed_never_conflate",
        ])

    def test_callback_inventory_names_exist_in_source(self):
        sources = "\n".join((ROOT / path).read_text() for path in (
            "crates/intent-acp/src/callback_registration/tests.rs",
            "crates/intent-services/src/agent_manager/repository_origin/callback_delivery/tests.rs",
        ))
        for name in self.job["env"]["CALLBACK_FIXTURE_TESTS"].split():
            with self.subTest(test=name):
                self.assertIsNotNone(re.search(r"\bfn\s+" + re.escape(name.rsplit("::", 1)[-1]) + r"\s*\(", sources), name)

    def test_callback_selection_routing(self):
        select = next(s for s in self.job["steps"] if s.get("id") == "select")
        self.assertEqual(self.step["if"], "steps.select.outputs.callback_fixture_changed == 'true'")
        for fixture, changed in ((False, False), (True, False), (False, True), (True, True)):
            with self.subTest(fixture=fixture, changed=changed), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / "scripts").mkdir()
                script = root / "scripts/changed-tests.sh"
                script.write_text("#!/bin/sh\n" + ("echo 'cargo llvm-cov --no-report nextest -p other'\n" if changed else "echo 'No changed tests'\n"))
                script.chmod(0o755)
                git = root / "git"
                git.write_text("#!/bin/sh\nexit " + ("1" if fixture else "0") + "\n")
                git.chmod(0o755)
                env = {**os.environ, **{k: str(v) for k, v in self.job["env"].items()},
                       "PATH": str(root) + os.pathsep + os.environ["PATH"], "RUNNER_TEMP": tmp,
                       "GITHUB_OUTPUT": str(root / "output"), "GITHUB_STEP_SUMMARY": str(root / "summary"), "BASE_SHA": "base"}
                result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", select["run"]],
                                        env=env, cwd=root, capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual((root / "output").read_text().splitlines(),
                                 ["callback_fixture_changed=" + str(fixture).lower(), "has_selection=" + str(fixture or changed).lower()])


if __name__ == "__main__":
    unittest.main()
