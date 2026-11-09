#!/usr/bin/env python3
"""Required CI Rust test steps must match the local wrapper's caller policy.

Reads real YAML (PyYAML, already installed in release-scripts). The command
scan covers current bare and rustup-run test commands, not echoed plans or
command-substitution discovery lists. Current indirect routes have explicit
registration plus actual stub-child probes below, not a general shell parser.
Mutation tests ensure removed/disabled step policy and missing wiring fail.

Every test a ci.yml step names (cargo test filters, `NAME: test` listing
proofs, the tunnel step's Python groups) must exist in the source of the
target it selects. Queue- and push-only steps otherwise fail late: intentd#3205
deleted tests a macOS queue step named, and merge-group run 42657324709
selected 0 tests.
"""

import ast
import copy
import fnmatch
import functools
import json
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import tomllib
import unittest
from unittest import mock

import yaml


ROOT = Path(__file__).resolve().parent.parent
JOBS = ("check", "build", "windows", "coverage-e2e", "coverage-all", "coverage-changed")
# These required steps launch through Python or a Bash array; scanning shell
# command prefixes cannot discover their runtime children. Execute their real
# workflow bodies with a stub rustup in test_indirect_workflow_child_environments.
INDIRECT_TEST_STEPS = {
    ("build", "Tunnel deadline and TLS controls (macOS runtime)"),
    ("build", "Retained capability gate fixture (macOS runtime)"),
}
# ci-killed-rustc-retry.sh is a transparent prefix: it runs the same cargo
# command (once more only when the runner killed rustc).
COMMAND = re.compile(
    r"^(?:[A-Z_]+=\S+\s+)*(?:\./scripts/ci-killed-rustc-retry\.sh\s+)?(?:(?:rustup\s+run\s+\S+\s+)?cargo\s+(?:test\b|nextest\s+run\b|llvm-cov\b[^\n]*\bnextest\b)"
    r"|\./scripts/(?:coverage-e2e|coverage-all|changed-tests)\.sh\b)"
)


def test_steps(workflow):
    for name in JOBS:
        job = workflow["jobs"][name]
        for step in job["steps"]:
            run = str(step.get("run", ""))
            if (name, step.get("name")) in INDIRECT_TEST_STEPS or any(COMMAND.search(line.strip()) and "--dry-run" not in line for line in run.splitlines()):
                yield name, job, step


# Source approximation of `cargo test -- --list`: every fn reachable through
# the module tree, named by its module path. Methods and helpers are included,
# so a check can only pass loosely, never reject a test that exists.
# The leading lookahead rejects most positions before trying each branch (5x).
RUST_TOKEN = re.compile(r"""(?=[/"'#bcrmf{}])(?:
    (?P<line>//[^\n]*)
  | (?P<block>/\*)
  | (?P<raw>\b[bc]?r(?P<hashes>\#*)")
  | (?P<str>"[^"\\]*(?:\\.[^"\\]*)*")
  | (?P<char>'(?:[^'\\\n]|\\(?:x[0-9a-fA-F]{2}|u\{[0-9a-fA-F]{1,6}\}|.))')
  | (?P<lifetime>'[A-Za-z_]\w*)
  | \#\[\s*path\s*=\s*"(?P<path>[^"]*)"\s*\]
  | \bmod\s+(?:r\#)?(?P<mod>\w+)\s*(?P<modend>[;{])
  | \bfn\s+(?:r\#)?(?P<fn>\w+)
  | (?P<open>\{)
  | (?P<close>\})
)""", re.X | re.S)

BLOCK_COMMENT = re.compile(r"/\*|\*/")


def rust_items(text):
    """Yield (kind, value) for module, fn, path-attribute and brace tokens."""
    pos = 0
    while match := RUST_TOKEN.search(text, pos):
        pos = match.end()
        kind = match.lastgroup
        if kind == "block":
            depth = 1
            while depth and (inner := BLOCK_COMMENT.search(text, pos)):
                depth += 1 if inner.group() == "/*" else -1
                pos = inner.end()
            if depth:
                return
        elif kind == "raw":
            end = text.find('"' + match.group("hashes"), pos)
            if end < 0:
                return
            pos = end + 1 + len(match.group("hashes"))
        elif kind == "modend":
            yield ("mod" + match.group("modend"), match.group("mod"))
        elif kind in ("path", "fn"):
            yield kind, match.group(kind)
        elif kind in ("open", "close"):
            yield kind, None


@functools.lru_cache(maxsize=None)
def target_fns(root_file):
    """All fn paths of the crate target rooted at root_file, as libtest names them."""
    found = set()
    walked = set()

    def walk(file, prefix, child_dir):
        if file in walked or not file.is_file():
            return
        walked.add(file)
        stack, path_attr = [], None
        for kind, value in rust_items(file.read_text(errors="replace")):
            if kind == "path":
                path_attr = value
                continue
            if kind == "mod;":
                # #[path] is relative to the file's directory outside inline
                # modules and to the inline module's directory inside one; a
                # file loaded through it owns its directory like a mod.rs.
                if path_attr is not None:
                    target = (child_dir if any(stack) else file.parent) / path_attr
                    walk(target, prefix + [value], target.parent)
                else:
                    flat = child_dir / (value + ".rs")
                    walk(flat if flat.is_file() else child_dir / value / "mod.rs", prefix + [value], child_dir / value)
            elif kind == "mod{":
                stack.append((prefix, child_dir))
                prefix, child_dir = prefix + [value], child_dir / value
            elif kind == "open":
                stack.append(None)
            elif kind == "close" and stack:
                if (saved := stack.pop()) is not None:
                    prefix, child_dir = saved
            elif kind == "fn":
                found.add("::".join(prefix + [value]))
            path_attr = None

    root_file = Path(root_file)
    walk(root_file, [], root_file.parent)
    return frozenset(found)


@functools.lru_cache(maxsize=None)
def crate_targets(root):
    """Map package name to the roots of its lib, bin and test targets."""
    crates = {}
    for manifest in sorted(Path(root).glob("crates/*/Cargo.toml")):
        data = tomllib.loads(manifest.read_text())
        base = manifest.parent
        lib = base / data.get("lib", {}).get("path", "src/lib.rs")
        bins, tests = {}, {}
        if data["package"].get("autobins", True):
            if (base / "src/main.rs").is_file():
                bins[data["package"]["name"]] = base / "src/main.rs"
            bins.update({p.stem: p for p in (base / "src/bin").glob("*.rs")})
            bins.update({p.parent.name: p for p in (base / "src/bin").glob("*/main.rs")})
        for binary in data.get("bin", []):
            bins[binary["name"]] = base / binary.get("path", f"src/bin/{binary['name']}.rs")
        if data["package"].get("autotests", True):
            tests.update({p.stem: p for p in (base / "tests").glob("*.rs")})
            tests.update({p.parent.name: p for p in (base / "tests").glob("*/main.rs")})
        for test in data.get("test", []):
            tests[test["name"]] = base / test.get("path", f"tests/{test['name']}.rs")
        crates[data["package"]["name"]] = {"lib": lib if lib.is_file() else None, "bins": bins, "tests": tests}
    return crates


CARGO_VALUE_FLAGS = {"-p", "--package", "--test", "--target", "--features", "-F", "--bin", "--example",
                     "--bench", "-j", "--jobs", "--manifest-path", "--profile", "--target-dir",
                     "--message-format", "--color", "--config", "-Z", "--exclude"}
LIBTEST_VALUE_FLAGS = {"--test-threads", "--skip", "--format", "--color", "--logfile", "-Z", "--shuffle-seed"}


def cargo_test_selection(words):
    """Parse the words after `cargo test` into the targets and names it selects."""
    selection = {"packages": [], "lib": False, "bins": [], "tests": [], "all": set(),
                 "workspace": False, "filters": [], "exact": False, "listed": []}
    libtest, index = False, 0
    while index < len(words):
        word = words[index]
        index += 1
        if word == "--" and not libtest:
            libtest = True
            continue
        flag, equals, value = word.partition("=")
        if flag in (LIBTEST_VALUE_FLAGS if libtest else CARGO_VALUE_FLAGS):
            if not equals:
                value = words[index] if index < len(words) else ""
                index += 1
            if not libtest and flag in ("-p", "--package"):
                selection["packages"].append(value)
            elif not libtest and flag in ("--test", "--bin"):
                selection[flag[2:] + "s"].append(value)
        elif word.startswith("-"):
            if not libtest and word == "--lib":
                selection["lib"] = True
            elif not libtest and word in ("--bins", "--tests", "--all-targets"):
                selection["all"].add(word)
            elif not libtest and word in ("--workspace", "--all"):
                selection["workspace"] = True
            elif libtest and word == "--exact":
                selection["exact"] = True
        else:
            selection["filters"].append(word)
    return selection


def command_words(text, raw):
    """Shell words of one command, ending at an operator, redirect or comment."""
    lexer = shlex.shlex(text, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    try:
        words = list(lexer)
    except ValueError as error:
        raise ValueError(f"cannot parse cargo test line {raw!r}: {error}") from None
    end = next((i for i, word in enumerate(words) if set(word) <= set("();<>|&")), len(words))
    if end < len(words) and words[end][0] in "<>" and end and words[end - 1].isdigit():
        end -= 1
    return words[:end]


def shell_lines(run):
    """Logical shell lines: continuations and multi-line `name=( ... )` arrays joined."""
    lines = []
    for raw in re.sub(r"\$\{\{.*?\}\}", "GHA_EXPR", run).replace("\\\n", " ").splitlines():
        line = raw.strip()
        if lines and lines[-1].count("(") > lines[-1].count(")") and re.match(r"\w+=\(", lines[-1]):
            lines[-1] += "\n" + line
        else:
            lines.append(line)
    return lines


def shell_selections(run):
    """Cargo test selections in a shell body, after simple variable expansion.

    Arrays and `for VAR in ...; do` loops are unrolled. `NAME: test` greps of
    a listing attach to the preceding cargo command.
    """
    selections = []

    def expand(line, scalars, arrays):
        for name, words in arrays.items():
            line = line.replace(f'"${{{name}[@]}}"', shlex.join(words))
        return re.sub(r"\$\{(\w+)\}|\$(\w+)",
                      lambda m: scalars.get(m.group(1) or m.group(2), m.group(0)), line)

    def execute(lines, scalars, arrays):
        index = 0
        while index < len(lines):
            raw = lines[index]
            index += 1
            line = expand(raw, scalars, arrays)
            if line.startswith("#"):
                continue
            loop = re.fullmatch(r"for (\w+) in (.*?);\s*do\s+(.*?);?\s*done", line)
            if loop:
                for value in shlex.split(loop.group(2)):
                    execute(re.split(r";\s*", loop.group(3)), {**scalars, loop.group(1): value}, arrays)
                continue
            loop = re.fullmatch(r"for (\w+) in (.*?)(?:;\s*do)?", line)
            if loop:
                if re.search(r"\bdone\b", loop.group(2)):
                    raise ValueError(f"cannot unroll loop {raw!r}")
                if not line.endswith("do") and index < len(lines) and lines[index] == "do":
                    index += 1
                depth, body = 1, []
                while index < len(lines) and depth:
                    inner = lines[index]
                    index += 1
                    depth += bool(re.search(r"(?:^|;\s*)do$", inner)) - bool(re.match(r"done\b", inner))
                    if depth:
                        body.append(inner)
                for value in shlex.split(loop.group(2)):
                    execute(body, {**scalars, loop.group(1): value}, arrays)
                continue
            assign = re.fullmatch(r"(\w+)=(.*)", line, re.S)
            if assign and not assign.group(2).startswith("$("):
                name, value = assign.groups()
                try:
                    if value.startswith("(") and value.endswith(")"):
                        arrays = {**arrays, name: shlex.split(value[1:-1], comments=True)}
                        continue
                    words = shlex.split(value)
                except ValueError:
                    words = []
                if len(words) == 1:
                    scalars = {**scalars, name: words[0]}
                    continue
            cargo = re.search(r"\bcargo\s+test\b", line)
            if cargo:
                selections.append(cargo_test_selection(command_words(line[cargo.end():], raw)))
                continue
            for flags, _, name in re.findall(r"\bgrep\s+((?:-[A-Za-z]+\s+)+)(['\"])(.*?): test\2", line):
                if "F" not in flags:
                    continue
                if not selections:
                    raise ValueError(f"listing proof {name!r} follows no cargo test command")
                selections[-1]["listed"].append((name, "x" in flags))

    execute(shell_lines(run), {}, {})
    return selections


def python_selections(run):
    """Selections from a Python heredoc's literal `[(cargo args, [exact names])]` groups."""
    selections = []
    for _, body in re.findall(r"\bpython3?\b[^\n]*<<\s*'(\w+)'\n(.*?)\n\1[ \t]*$", run, re.S | re.M):
        for node in ast.walk(ast.parse(body)):
            if not isinstance(node, ast.Assign):
                continue
            try:
                value = ast.literal_eval(node.value)
            except ValueError:
                continue
            if (isinstance(value, list) and value
                    and all(isinstance(g, tuple) and len(g) == 2 and "-p" in g[0] for g in value)):
                selections += [cargo_test_selection([*args, "--", "--exact", *names]) for args, names in value]
    return selections


def step_selections(run):
    return [s for s in python_selections(run) + shell_selections(run)
            if s["filters"] or s["listed"] or s["tests"] or s["bins"]]


def selection_violations(selection, crates, root):
    errors, targets = [], []
    packages = sorted(crates) if selection["workspace"] else selection["packages"]
    for package in packages:
        crate = crates.get(package)
        if crate is None:
            errors.append(f"names no package {package}")
            continue
        every = not (selection["lib"] or selection["bins"] or selection["tests"] or selection["all"])
        every_bin = every or selection["all"] & {"--bins", "--tests", "--all-targets"}
        every_test = every or selection["all"] & {"--tests", "--all-targets"}
        if selection["lib"] or every or every_test:
            if crate["lib"] is not None:
                targets.append(crate["lib"])
            elif selection["lib"]:
                errors.append(f"names no lib target in {package}")
        if every_bin:
            targets += crate["bins"].values()
        if every_test:
            targets += crate["tests"].values()
        for kind, label in (("bins", "bin"), ("tests", "test")):
            for pattern in selection[kind]:
                matched = fnmatch.filter(crate[kind], pattern)
                if not matched and not selection["workspace"]:
                    errors.append(f"names no {label} target {pattern} in {package}")
                targets += [crate[kind][name] for name in matched]
    if not targets:
        return errors or ["selects no test target"]
    scope = " / ".join(sorted(str(t.relative_to(root)) for t in set(targets)))
    paths = set().union(*(target_fns(t) for t in targets))
    for name in selection["filters"]:
        if not (name in paths if selection["exact"] else any(name in path for path in paths)):
            errors.append(f"{name!r} names no test in {scope}")
    if errors:
        return errors
    selected = [p for p in paths if not selection["filters"] or any(
        (name == p if selection["exact"] else name in p) for name in selection["filters"])]
    for name, full in selection["listed"]:
        if not any(path == name if full else path.endswith(name) for path in selected):
            errors.append(f"listing proof {name!r} names no selected test in {scope}")
    return errors


def named_test_violations(workflow, root=ROOT):
    """Tests ci.yml names must exist in the targets their steps select."""
    errors = []
    crates = crate_targets(Path(root))
    for job_id, job in workflow["jobs"].items():
        for step in job.get("steps", []):
            label = f"{job_id}: {step.get('name')}"
            try:
                selections = step_selections(str(step.get("run", "")))
            except (SyntaxError, ValueError) as error:
                errors.append(f"{label}: {error}")
                continue
            for selection in selections:
                errors += [f"{label}: {e}" for e in selection_violations(selection, crates, Path(root))]
    return list(dict.fromkeys(errors))


def coverage_budget_violations(workflow):
    """Keep the combined instrumented lane finite, with post-action headroom."""
    job = workflow["jobs"]["coverage-changed"]
    limits = {
        "Run changed tests under llvm-cov": 50,
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
    if type(combined.get("timeout-minutes")) is not int or combined["timeout-minutes"] != 55:
        errors.append("coverage-all must reserve 2m outside its 53m combined phase")
    if len(steps) != 1 or type(steps[0].get("timeout-minutes")) is not int or steps[0]["timeout-minutes"] != 53:
        errors.append("coverage-all must retain its 53m compile/test/report bound")
    return errors


def profile_pool_violations(workflow):
    errors = []
    for job_id, first_test, condition in (
        ("coverage-all", "Run combined coverage", None),
        ("coverage-changed", "Run changed tests under llvm-cov", "steps.select.outputs.has_selection == 'true'"),
    ):
        job = workflow["jobs"][job_id]
        steps = job["steps"]
        proofs = [s for s in steps if s.get("name") == "Verify bounded coverage profile pooling"]
        if (job.get("env", {}).get("LLVM_PROFILE_FILE_NAME") != "intent-%8m.profraw"
                or "LLVM_PROFILE_FILE" in job.get("env", {})):
            errors.append(f"{job_id}: retain the bounded profile pool")
        if len(proofs) != 1:
            errors.append(f"{job_id}: require exactly one profile pooling proof")
            continue
        proof = proofs[0]
        if (proof.get("run") != "python3 -I -B scripts/test-coverage-profile-pool.py"
                or proof.get("if") != condition or proof.get("continue-on-error")
                or steps.index(proof) >= next(i for i, s in enumerate(steps) if s.get("name") == first_test)):
            errors.append(f"{job_id}: require the profile proof before instrumented tests")
        for step in steps:
            if any(key in step.get("env", {}) for key in ("LLVM_PROFILE_FILE_NAME", "LLVM_PROFILE_FILE")):
                errors.append(f"{job_id}: do not override the job profile pool in a step")
    return errors


THREAD_STEP = "Size nextest threads from runner CPUs"
THREAD_RUN = (
    "cpus=$(nproc)\n"
    "threads=$(( cpus < 16 ? cpus : 16 ))\n"
    'echo "NEXTEST_TEST_THREADS=$threads" >> "$GITHUB_ENV"\n'
    'echo "NEXTEST_TEST_THREADS=$threads (nproc=$cpus, cap 16)"\n'
)
COVERAGE_JOBS = ("coverage-e2e", "coverage-all", "coverage-changed")
HARDCODED_THREADS = re.compile(r"(?:NEXTEST_TEST_THREADS=|--test-threads[= ])\d")


def thread_sizing_violations(workflow):
    """Coverage jobs size nextest from the runner's CPUs (min(nproc, 16)).

    A fixed 16 oversubscribed the 8-CPU cpuset-pinned intent-cache heavy slot
    2x and timed out latency-bound tests.
    """
    errors = []
    if "NEXTEST_TEST_THREADS" in workflow.get("env", {}):
        errors.append("workflow env must not fix NEXTEST_TEST_THREADS")
    for job_id in COVERAGE_JOBS:
        job = workflow["jobs"][job_id]
        steps = job["steps"]
        if "NEXTEST_TEST_THREADS" in job.get("env", {}):
            errors.append(f"{job_id}: do not hardcode NEXTEST_TEST_THREADS; size it from nproc")
        sizing = [s for s in steps if s.get("name") == THREAD_STEP]
        if (len(sizing) != 1 or steps.index(sizing[0]) != 0 or sizing[0].get("run") != THREAD_RUN
                or sizing[0].get("shell") != "bash" or "if" in sizing[0] or sizing[0].get("continue-on-error")):
            errors.append(f"{job_id}: the first step must size NEXTEST_TEST_THREADS from nproc")
        for step in steps:
            if step.get("name") == THREAD_STEP:
                continue
            if "NEXTEST_TEST_THREADS" in step.get("env", {}) or HARDCODED_THREADS.search(str(step.get("run", ""))):
                errors.append(f"{job_id}: {step.get('name')} must not override the sized nextest thread count")
    return errors


def violations(workflow, policy):
    errors = (coverage_budget_violations(workflow) + profile_pool_violations(workflow)
              + thread_sizing_violations(workflow) + named_test_violations(workflow))
    found = set()
    for name, job, step in test_steps(workflow):
        found.add(name)
        env = {**workflow.get("env", {}), **job.get("env", {}), **step.get("env", {})}
        command_policies = [str(env.get("INTENTD_ASSERT_BOUND_CALLER", ""))]
        # A step-wide unset previously disabled every retained fixture.
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


# Independently audited from ci.yml, not generated by test_steps/COMMAND. Keep
# this inventory explicit so scanner omissions cannot also erase mutations.
REQUIRED_TEST_STEPS = {
    ("check", "Real import and public session contract"): 1,
    ("check", "Real Claude adapter utility-session contract"): 1,
    ("check", "Source lints"): 1,
    ("check", "GitLab auth, invitations and checkout over real sockets with explicit test transports"): 2,
    ("check", "No provider CLIs on the runner"): 1,
    ("check", "Coverage-skipped tests (STAB-40/43)"): 2,
    ("check", "Installer guard tests (install_ps1_owner)"): 1,
    # None: indirect Python/Bash-array routes are executed with a stub below.
    ("build", "Tunnel deadline and TLS controls (macOS runtime)"): None,
    ("build", "Retained capability gate fixture (macOS runtime)"): None,
    # Post-merge only (push); see the `windows` job.
    ("windows", "Headless workspace setup (Windows runtime)"): 2,
    ("windows", "Codex diagnostic process ownership (Windows runtime)"): 1,
    ("build", "Codex focused diagnostics (macOS runtime)"): 2,
    ("coverage-e2e", "Run e2e coverage"): 1,
    ("coverage-all", "Run combined coverage"): 1,
    ("coverage-changed", "Run changed tests under llvm-cov"): 1,
    ("coverage-changed", "Run callback fixture composition under llvm-cov"): 1,
}


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
                          "Retained capability gate fixture (macOS runtime)"):
            with self.subTest(step=step_name):
                workflow = copy.deepcopy(self.workflow)
                owner = "build"
                step = next(s for s in workflow['jobs'][owner]['steps'] if s.get('name') == step_name)
                step['run'] = 'unset INTENTD_ASSERT_BOUND_CALLER\n' + step['run']
                self.assertIn(f"{owner}: {step_name} must arm INTENTD_ASSERT_BOUND_CALLER=1", violations(workflow, "1"))

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

    def test_coverage_budget_rejects_lost_bounds_and_cleanup_margin(self):
        self.assertEqual(coverage_budget_violations(self.workflow), [])
        for name in (None, "Run changed tests under llvm-cov",
                     "Run callback fixture composition under llvm-cov"):
            for limit in (None, 0, True, 30 if name is None else 60):
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
            for limit in (None, 0, True, 30, 60):
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

    def test_coverage_threads_are_sized_from_runner_cpus(self):
        self.assertEqual(thread_sizing_violations(self.workflow), [])
        for job_id in COVERAGE_JOBS:
            steps = self.workflow["jobs"][job_id]["steps"]
            first_test = min(steps.index(step) for name, _, step in test_steps(self.workflow) if name == job_id)
            self.assertEqual(steps[0]["name"], THREAD_STEP)
            self.assertLess(0, first_test)
        for mutation in ("hardcoded-job", "missing", "late", "step-override", "fixed-run", "skipped", "skipped-bool", "cli-flag"):
            for job_id in COVERAGE_JOBS:
                with self.subTest(job=job_id, mutation=mutation):
                    workflow = copy.deepcopy(self.workflow)
                    job = workflow["jobs"][job_id]
                    steps = job["steps"]
                    sizing = steps[0]
                    test_step = next(step for name, _, step in test_steps(workflow) if name == job_id)
                    if mutation == "hardcoded-job":
                        job.setdefault("env", {})["NEXTEST_TEST_THREADS"] = 16
                    elif mutation == "missing":
                        steps.remove(sizing)
                    elif mutation == "late":
                        steps.remove(sizing)
                        steps.insert(steps.index(test_step) + 1, sizing)
                    elif mutation == "step-override":
                        test_step.setdefault("env", {})["NEXTEST_TEST_THREADS"] = "16"
                    elif mutation == "fixed-run":
                        sizing["run"] = 'echo "NEXTEST_TEST_THREADS=16" >> "$GITHUB_ENV"\n'
                    elif mutation == "skipped":
                        sizing["if"] = "false"
                    elif mutation == "skipped-bool":
                        sizing["if"] = False
                    else:
                        test_step["run"] += "\ncargo nextest run --test-threads 16\n"
                    self.assertTrue(thread_sizing_violations(workflow))
                    self.assertTrue(violations(workflow, "1"))

    def test_hardcoded_threads_in_previous_workflow_are_rejected(self):
        workflow = copy.deepcopy(self.workflow)
        for job_id in COVERAGE_JOBS:
            job = workflow["jobs"][job_id]
            job["steps"] = [s for s in job["steps"] if s.get("name") != THREAD_STEP]
            job["env"]["NEXTEST_TEST_THREADS"] = 16
        errors = thread_sizing_violations(workflow)
        for job_id in COVERAGE_JOBS:
            self.assertIn(f"{job_id}: do not hardcode NEXTEST_TEST_THREADS; size it from nproc", errors)
            self.assertIn(f"{job_id}: the first step must size NEXTEST_TEST_THREADS from nproc", errors)

    def test_thread_sizing_step_caps_nproc_at_sixteen(self):
        for cpus, expected in (("1", "1"), ("8", "8"), ("16", "16"), ("64", "16")):
            with self.subTest(cpus=cpus), tempfile.TemporaryDirectory() as tmp:
                stub = Path(tmp) / "nproc"
                stub.write_text(f"#!/bin/sh\necho {cpus}\n")
                stub.chmod(0o755)
                github_env = Path(tmp) / "github_env"
                result = subprocess.run(
                    ["bash", "-e", "-c", THREAD_RUN],
                    env={**os.environ, "PATH": f"{tmp}:{os.environ['PATH']}", "GITHUB_ENV": str(github_env)},
                    capture_output=True, text=True, timeout=5,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(github_env.read_text(), f"NEXTEST_TEST_THREADS={expected}\n")
                self.assertIn(f"NEXTEST_TEST_THREADS={expected} (nproc={cpus}, cap 16)", result.stdout)

    def test_full_coverage_budget_includes_report_generation(self):
        # 53m phase plus 2m: room for an 8-CPU heavy slot (~44 min) under the
        # merge queue's 60-minute check response timeout.
        self.assertEqual(self.workflow["jobs"]["coverage-all"]["timeout-minutes"], 55)

    def test_coverage_profile_pool_rejects_unbounded_or_unverified_collection(self):
        self.assertEqual(profile_pool_violations(self.workflow), [])
        for job_id in ("coverage-all", "coverage-changed"):
            for mutation in ("missing-env", "per-process", "direct-profile", "missing-proof", "skipped-proof",
                             "ignored-proof", "late-proof", "step-override"):
                with self.subTest(job=job_id, mutation=mutation):
                    workflow = copy.deepcopy(self.workflow)
                    job = workflow["jobs"][job_id]
                    steps = job["steps"]
                    proof = next(s for s in steps if s.get("name") == "Verify bounded coverage profile pooling")
                    if mutation == "missing-env":
                        job["env"].pop("LLVM_PROFILE_FILE_NAME")
                    elif mutation == "per-process":
                        job["env"]["LLVM_PROFILE_FILE_NAME"] = "intent-%p-%32m.profraw"
                    elif mutation == "direct-profile":
                        job["env"]["LLVM_PROFILE_FILE"] = "intent-%p.profraw"
                    elif mutation == "missing-proof":
                        steps.remove(proof)
                    elif mutation == "skipped-proof":
                        proof["if"] = False
                    elif mutation == "ignored-proof":
                        proof["continue-on-error"] = True
                    elif mutation == "late-proof":
                        steps.remove(proof)
                        steps.append(proof)
                    else:
                        proof["env"] = {"LLVM_PROFILE_FILE_NAME": "intent-%p.profraw"}
                    self.assertTrue(profile_pool_violations(workflow))
                    self.assertTrue(violations(workflow, "1"))

    def test_changed_coverage_uses_the_same_verified_profile_pool(self):
        job = self.workflow["jobs"]["coverage-changed"]
        self.assertEqual(job["env"].get("LLVM_PROFILE_FILE_NAME"),
                         self.workflow["jobs"]["coverage-all"]["env"]["LLVM_PROFILE_FILE_NAME"])
        steps = job["steps"]
        proof = next(s for s in steps if s.get("name") == "Verify bounded coverage profile pooling")
        self.assertEqual(proof["run"], "python3 -I -B scripts/test-coverage-profile-pool.py")
        self.assertEqual(proof["if"], "steps.select.outputs.has_selection == 'true'")
        self.assertFalse(proof.get("continue-on-error"))
        toolchain = next(s for s in steps if s.get("uses", "").startswith("dtolnay/rust-toolchain@"))
        execute = next(s for s in steps if s.get("name") == "Run changed tests under llvm-cov")
        self.assertLess(steps.index(toolchain), steps.index(proof))
        self.assertLess(steps.index(proof), steps.index(execute))

    def test_changed_coverage_storage_evidence_is_retained_on_failure(self):
        job = self.workflow["jobs"]["coverage-changed"]
        steps = job["steps"]
        init = next(s for s in steps if s.get("name") == "Initialize coverage storage evidence")
        proof = next(s for s in steps if s.get("name") == "Verify bounded coverage profile pooling")
        execute = next(s for s in steps if s.get("name") == "Run changed tests under llvm-cov")
        upload = next(s for s in steps if s.get("name") == "Upload changed coverage evidence")
        self.assertEqual(init["if"], "steps.select.outputs.has_selection == 'true'")
        self.assertLess(steps.index(proof), steps.index(init))
        self.assertLess(steps.index(init), steps.index(execute))
        self.assertEqual(upload["if"], "always()")
        self.assertIn("coverage-storage", upload["with"]["path"])
        self.assertIn("coverage-changed-plan.json", upload["with"]["path"])
        self.assertFalse(execute.get("continue-on-error"))
        self.assertIn("python3 -I -B scripts/test-coverage-storage.py",
                      "\n".join(s.get("run", "") for j in self.workflow["jobs"].values()
                                for s in j.get("steps", [])))

    def test_callback_inventory_shares_coverage_environment_and_fails_closed(self):
        job = self.workflow["jobs"]["coverage-changed"]
        step = next(s for s in job["steps"] if s.get("name") == "Run callback fixture composition under llvm-cov")
        self.assertEqual(coverage_budget_violations(self.workflow), [])

        self.assertEqual(job["timeout-minutes"], 60)
        names = job["env"]["CALLBACK_FIXTURE_TESTS"].split()
        self.assertEqual(len(names), 6)
        for mode in ("valid", "missing", "extra", "duplicate", "env-failure", "metadata-failure", "list-failure", "run-failure"):
            for override in (False, True):
                with self.subTest(mode=mode, override=override), tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    stub = root / "cargo"
                    stub.write_text(
                        f"#!{sys.executable} -S\n" + r'''
import json, os, shlex, sys
args = sys.argv[1:]
mode = os.environ["INVENTORY_TEST_MODE"]
with open(os.environ["INVENTORY_TEST_LOG"], "a") as log:
    log.write(json.dumps({"args": args, "target": os.environ.get("CARGO_TARGET_DIR"),
                         "cov_target": os.environ.get("CARGO_LLVM_COV_TARGET_DIR"),
                         "instrumented": os.environ.get("INVENTORY_TEST_INSTRUMENTED"),
                         "caller": os.environ.get("INTENTD_ASSERT_BOUND_CALLER")}) + "\n")
if args[0] == "metadata":
    if mode == "metadata-failure": sys.exit(41)
    print(json.dumps({"target_directory": os.environ["INVENTORY_TEST_TARGET"]}))
elif args[:2] == ["llvm-cov", "show-env"]:
    if mode == "env-failure": sys.exit(42)
    print("export INVENTORY_TEST_INSTRUMENTED=1")
    print("export CARGO_LLVM_COV_TARGET_DIR=" + shlex.quote(os.environ["CARGO_TARGET_DIR"]))
elif args[:2] == ["nextest", "list"]:
    if mode == "list-failure": sys.exit(43)
    names = os.environ["CALLBACK_FIXTURE_TESTS"].split()
    if mode == "missing": names.pop()
    if mode == "extra": names.append("unexpected::test")
    cases = {name: {"filter-match": {"status": "matches"}} for name in names}
    suites = {"first": {"testcases": cases}}
    if mode == "duplicate": suites["second"] = {"testcases": {names[0]: cases[names[0]]}}
    print(json.dumps({"rust-suites": suites}))
elif args[:3] == ["llvm-cov", "--no-report", "nextest"]:
    if mode == "run-failure": sys.exit(44)
else:
    sys.exit("unexpected cargo call: " + repr(args))
'''
                    )
                    stub.chmod(0o755)
                    env = {**os.environ, **{k: str(v) for k, v in job["env"].items()}, **step["env"],
                           "PATH": str(root) + os.pathsep + os.environ["PATH"], "RUNNER_TEMP": str(root),
                           "CARGO_TARGET_DIR": str(root / "slot with spaces"),
                           "INVENTORY_TEST_TARGET": str(root / "slot with spaces"),
                           "INVENTORY_TEST_MODE": mode, "INVENTORY_TEST_LOG": str(root / "calls.jsonl")}
                    env.pop("CARGO_LLVM_COV_TARGET_DIR", None)
                    env.pop("INVENTORY_TEST_INSTRUMENTED", None)
                    if override:
                        env["CARGO_LLVM_COV_TARGET_DIR"] = str(root / "explicit coverage")
                    original_scope = ["-p", "intent-core", "-p", "intent-services",
                                      "-p", "intent-transport", "--lib", "--bins", "--tests"]
                    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
                    (root / "coverage-changed-plan.json").write_text(json.dumps({
                        "version": 1, "head": head, "mergeBase": head,
                        "plans": [original_scope, ["-p", "intent-store"]],
                    }))
                    result = subprocess.run(["bash", "-c", step["run"]], env=env, cwd=ROOT,
                                            capture_output=True, text=True, timeout=10)
                    calls = [json.loads(line) for line in (root / "calls.jsonl").read_text().splitlines()]
                    executions = [c for c in calls if c["args"][:3] == ["llvm-cov", "--no-report", "nextest"]]
                    self.assertEqual(result.returncode == 0, mode == "valid" or (mode == "metadata-failure" and override), result.stderr)
                    if mode not in ("valid", "run-failure") and not (mode == "metadata-failure" and override):
                        self.assertEqual(executions, [])
                        continue
                    self.assertEqual(len(executions), 1)
                    inventory = next(c for c in calls if c["args"][:2] == ["nextest", "list"])
                    expected_target = env.get("CARGO_LLVM_COV_TARGET_DIR", env["CARGO_TARGET_DIR"] + "/llvm-cov-target")
                    self.assertEqual(inventory["target"], env["CARGO_TARGET_DIR"])
                    self.assertEqual(inventory["args"][inventory["args"].index("--target-dir") + 1], expected_target)
                    self.assertEqual(inventory["cov_target"], expected_target)
                    self.assertEqual(inventory["instrumented"], "1")
                    execution = executions[0]
                    self.assertIsNone(execution["instrumented"])
                    self.assertEqual(execution["target"], env["CARGO_TARGET_DIR"])
                    self.assertEqual(execution["cov_target"], expected_target)
                    self.assertEqual(inventory["caller"], "1")
                    self.assertEqual(execution["caller"], "1")
                    inventory_filter = inventory["args"].index("-E")
                    execution_filter = execution["args"].index("-E")
                    expected_scope = original_scope + ["-p", "intent-acp"]
                    self.assertEqual(inventory["args"][2:inventory_filter], expected_scope + ["--target-dir", expected_target])
                    self.assertEqual(execution["args"][3:execution_filter], expected_scope)
                    self.assertEqual(inventory["args"][inventory_filter + 1], execution["args"][execution_filter + 1])
                    self.assertEqual(execution["args"][execution_filter + 2:], ["--no-fail-fast", "--retries", "0", "--show-progress", "none", "--status-level", "pass", "--final-status-level", "fail"])

    def test_indirect_workflow_child_environments(self):
        for step_name, expected_tests in (
            ("Retained capability gate fixture (macOS runtime)", 1),
        ):
            with self.subTest(step=step_name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                stub = root / "rustup"
                stub.write_text(
                    f"#!{sys.executable} -S\n"
                    "import json,os,sys\n"
                    "args=sys.argv[1:]\n"
                    "with open(os.environ['POLICY_CHILD_LOG'],'a') as log:\n"
                    " log.write(json.dumps([args,os.environ.get('INTENTD_ASSERT_BOUND_CALLER')])+'\\n')\n"
                    "if '--no-run' in args: sys.exit(0)\n"
                    "test=args[args.index('--')-1]\n"
                    "if '--list' in args:\n"
                    " print(test+': test'); print('1 test, 0 benchmarks'); sys.exit(0)\n"
                    "print('running 1 test')\n"
                    "print('test result: ok. 1 passed; 0 failed; 0 ignored;')\n"
                )
                stub.chmod(0o755)
                step = next(s for s in self.workflow['jobs']['build']['steps'] if s.get('name') == step_name)
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
                workflow["jobs"]["build"]["steps"].append({
                    "name": "New platform tests", "run": command,
                })
                self.assertIn("build: New platform tests must arm INTENTD_ASSERT_BOUND_CALLER=1", violations(workflow, "1"))

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


# Independently audited from ci.yml: every step that names tests, and how many
# names (filters, listing proofs, test targets) the check resolves in it.
NAMED_TEST_STEPS = {
    ("check", "Real import and public session contract"): 1,
    ("check", "Real Claude adapter utility-session contract"): 1,
    ("check", "Source lints"): 1,
    ("check", "GitLab auth, invitations and checkout over real sockets with explicit test transports"): 5,
    ("check", "No provider CLIs on the runner"): 1,
    ("check", "Coverage-skipped tests (STAB-40/43)"): 3,
    ("check", "Installer guard tests (install_ps1_owner)"): 1,
    ("build", "Tunnel deadline and TLS controls (macOS runtime)"): 9,
    ("build", "Codex focused diagnostics (macOS runtime)"): 8,
    ("build", "Retained capability gate fixture (macOS runtime)"): 3,
    ("windows", "Headless workspace setup (Windows runtime)"): 3,
    ("windows", "Codex diagnostic process ownership (Windows runtime)"): 3,
}

# The macOS queue step as intentd#3205 first queued it: the PR deleted five of
# the tests it names, and merge-group run 42657324709 selected 0 tests.
INCIDENT_3205_DELETED = (
    "tests::worktree_provisioning::create_provisions_cow_checkout_when_isolation_enabled",
    "tests::worktree_provisioning::duplicate_provisions_cow_checkout_when_isolation_enabled",
    "tests::worktree_provisioning::create_cleans_up_empty_ws_dir_when_cow_provisioning_fails",
    "tests::worktree_provisioning::duplicate_cleans_up_empty_ws_dir_when_cow_provisioning_fails",
    "tests::clone_orchestration::progress_id_cow_create_streams_cow_copy_milestone",
)
INCIDENT_3205_RUN = """\
set -euo pipefail
unset INTENTD_TEST_KEEP_TMP
tests=(
""" + "".join(f"  {name}\n" for name in INCIDENT_3205_DELETED) + """\
  capability::tests::unbound_caller_is_forbidden_on_every_gate
)
cargo_test=(rustup run "$INTENTD_COW_TOOLCHAIN" cargo test --locked -p intent-services --lib --target ${{ matrix.target }})
status=0
for test in "${tests[@]}"; do
  listed=$("${cargo_test[@]}" "$test" -- --exact --list)
  printf '%s\\n' "$listed"
  grep -Fx "$test: test" <<< "$listed" > /dev/null
  grep -Fx '1 test, 0 benchmarks' <<< "$listed" > /dev/null
  scratch=$(mktemp -d "$RUNNER_TEMP/cow-${test##*::}.XXXXXX")
  if ! TMPDIR="$scratch" "${cargo_test[@]}" "$test" -- --exact --nocapture --test-threads=1 2>&1 | tee "$RUNNER_TEMP/cow.log"; then
    status=1
  fi
done
exit "$status"
"""

FIXTURE_LIB = r"""// mod commented_out;
/* outer /* nested */ mod hidden; */
mod flat;
mod dir;
#[cfg(test)]
#[path = "elsewhere/custom.rs"]
mod renamed;
pub(crate) mod inline {
    const OPEN: char = '{';
    const QUOTE: char = '\'';
    const TEXT: &str = "} mod fake { fn not_a_test() {} \" }";
    const RAW: &str = r#"" } fn raw_fake() {"#;
    fn lifetime<'a>(value: &'a str) -> &'a str { value }
    #[path = "inline_path.rs"]
    mod via_path;
    mod nested_file;
    #[test]
    fn inline_test() {}
}
fn after_inline() {}
"""
FIXTURE_FILES = {
    "Cargo.toml": '[package]\nname = "demo"\nautotests = false\n\n[[test]]\nname = "suite"\n',
    "src/lib.rs": FIXTURE_LIB,
    "src/flat.rs": 'mod child;\nmod m {\n    #[path = "x.rs"]\n    mod y;\n}\n#[test]\nfn flat_test() {}\n',
    "src/flat/child.rs": "fn child_test() {}\n",
    "src/flat/m/x.rs": "fn y_test() {}\n",
    "src/dir/mod.rs": "mod sub;\nfn dir_test() {}\n",
    "src/dir/sub.rs": "fn sub_test() {}\n",
    "src/elsewhere/custom.rs": "mod beside;\nfn renamed_test() {}\n",
    "src/elsewhere/beside.rs": "fn beside_test() {}\n",
    "src/inline/inline_path.rs": "fn via_path_test() {}\n",
    "src/inline/nested_file.rs": "fn nested_test() {}\n",
    "tests/suite.rs": "mod case;\n",
    "tests/case.rs": "#[tokio::test]\nasync fn suite_test() {}\n",
}
FIXTURE_QUEUE_RUN = """\
test=inline::inline_test
listed=$(cargo test -p demo --lib "$test" -- --exact --list)
grep -Fx "$test: test" <<< "$listed" > /dev/null
cargo test -p demo --test suite case:: -- --nocapture
"""
# A crate with only binary targets, as intentd is (its tests live in main.rs).
FIXTURE_BIN_FILES = {
    "Cargo.toml": '[package]\nname = "daemon"\n',
    "src/main.rs": "mod cli;\nfn main() {}\n",
    "src/cli.rs": "#[test]\nfn bin_test() {}\n",
    "src/bin/tool.rs": "fn main() {}\n#[test]\nfn tool_test() {}\n",
}
# (run, expected violations after the "queue: Shapes: " prefix). Valid shell
# around a test must not be read as a name; shapes naming a missing test fail.
FIXTURE_SHELL_SHAPES = (
    ("cargo test -p daemon cli::bin_test\ncargo test -p daemon --bin tool tool_test", []),
    ("cargo test -p daemon --bin tool cli::bin_test", ["'cli::bin_test' names no test in crates/daemon/src/bin/tool.rs"]),
    ("cargo test -p daemon --bins cli::", []),
    ("cargo test -p daemon --bin gone cli::", ["names no bin target gone in daemon"]),
    ("cat > check.sh <<'SCRIPT'\nif [ -n \"$x\" ]; then echo; fi\nSCRIPT\ncargo test -p demo --lib inline::", []),
    ("for t in inline::inline_test inline::gone; do cargo test -p demo --lib \"$t\" -- --exact; done\n"
     "cargo test -p demo --lib dir::", ["'inline::gone' names no test in crates/demo/src/lib.rs"]),
    ("listed=$(cargo test -p demo --lib inline:: -- --list)\ngrep -qFx 'inline::gone: test' <<< \"$listed\"",
     ["listing proof 'inline::gone' names no selected test in crates/demo/src/lib.rs"]),
    ("listed=$(cargo test -p demo --lib inline:: -- --list)\ngrep -xF 'inline::inline_test: test' <<< \"$listed\"", []),
    ("cargo test -p demo --lib inline::inline_test >/dev/null 2>&1 # quiet\n"
     "cargo test -p demo --lib dir:: 2>/dev/null\n"
     "out=$(cargo test -p demo --lib flat:: -- --list) && echo \"$out\"", []),
)


class NamedTestTests(unittest.TestCase):
    """Tests ci.yml names must exist, so queue/push-only steps cannot fail late."""

    @classmethod
    def setUpClass(cls):
        cls.workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())

    def step(self, workflow, job, name):
        return next(s for s in workflow["jobs"][job]["steps"] if s.get("name") == name)

    def test_every_named_test_exists(self):
        self.assertEqual(named_test_violations(self.workflow), [])

    def test_discovery_matches_independent_named_step_inventory(self):
        discovered = {}
        for job_id, job in self.workflow["jobs"].items():
            for step in job.get("steps", []):
                selections = step_selections(str(step.get("run", "")))
                if selections:
                    discovered[(job_id, step["name"])] = sum(
                        len(s["filters"]) + len(s["listed"]) + len(s["tests"]) + len(s["bins"]) for s in selections)
        self.assertEqual(discovered, NAMED_TEST_STEPS)

    def test_queue_step_naming_deleted_tests_fails(self):
        workflow = copy.deepcopy(self.workflow)
        label = "build: Retained capability gate fixture (macOS runtime)"
        self.step(workflow, "build", label.split(": ", 1)[1])["run"] = INCIDENT_3205_RUN
        expected = [f"{label}: {name!r} names no test in crates/intent-services/src/lib.rs"
                    for name in INCIDENT_3205_DELETED]
        self.assertEqual(named_test_violations(workflow), expected)
        self.assertTrue(set(expected).issubset(violations(workflow, "1")))

    def test_removing_any_named_test_fails_its_step(self):
        real = target_fns
        for job, step_name in NAMED_TEST_STEPS:
            for selection in step_selections(self.step(self.workflow, job, step_name)["run"]):
                names = [(n, "exact" if selection["exact"] else "substring") for n in selection["filters"]]
                names += [(n, "exact" if full else "suffix") for n, full in selection["listed"]]
                for name, mode in names:
                    with self.subTest(job=job, step=step_name, name=name):
                        def kept(path, name=name, mode=mode):
                            return not (path == name if mode == "exact" else
                                        path.endswith(name) if mode == "suffix" else name in path)
                        lister = lambda root: frozenset(filter(kept, real(root)))
                        with mock.patch.dict(globals(), target_fns=lister):
                            errors = named_test_violations(self.workflow)
                        self.assertTrue(any(e.startswith(f"{job}: {step_name}: ") and repr(name) in e
                                            for e in errors), errors)

    def test_removing_a_named_test_target_fails_its_step(self):
        real = crate_targets(ROOT)
        for job, step_name in NAMED_TEST_STEPS:
            for selection in step_selections(self.step(self.workflow, job, step_name)["run"]):
                for package in selection["packages"]:
                    for kind, target in [("bins", t) for t in selection["bins"]] + [("tests", t) for t in selection["tests"]]:
                        with self.subTest(job=job, step=step_name, target=target):
                            crates = copy.deepcopy(real)
                            del crates[package][kind][target]
                            with mock.patch.dict(globals(), crate_targets=lambda root: crates):
                                errors = named_test_violations(self.workflow)
                            self.assertIn(f"{job}: {step_name}: names no {kind[:-1]} target {target} in {package}", errors)

    def test_source_lister_follows_module_rules_and_sees_deleted_functions(self):
        with tempfile.TemporaryDirectory() as tmp:
            crate = Path(tmp) / "crates/demo"
            for name, content in FIXTURE_FILES.items():
                (crate / name).parent.mkdir(parents=True, exist_ok=True)
                (crate / name).write_text(content)
            self.assertEqual(target_fns(crate / "src/lib.rs"), {
                "flat::flat_test", "flat::child::child_test", "flat::m::y::y_test",
                "dir::dir_test", "dir::sub::sub_test",
                "renamed::renamed_test", "renamed::beside::beside_test",
                "inline::lifetime", "inline::via_path::via_path_test",
                "inline::nested_file::nested_test", "inline::inline_test", "after_inline",
            })
            workflow = {"jobs": {"queue": {"if": "github.event_name == 'merge_group'",
                                           "steps": [{"name": "Queue-only proof", "run": FIXTURE_QUEUE_RUN}]}}}
            self.assertEqual(named_test_violations(workflow, tmp), [])
            prefix = "queue: Queue-only proof: "
            for path, old, new, error in (
                ("src/lib.rs", "fn inline_test() {}", "", "'inline::inline_test' names no test in crates/demo/src/lib.rs"),
                ("tests/case.rs", "suite_test", "", "'case::' names no test in crates/demo/tests/suite.rs"),
                ("Cargo.toml", 'name = "suite"', 'name = "other"', "names no test target suite in demo"),
                ("Cargo.toml", 'name = "demo"', 'name = "renamed"', "names no package demo"),
            ):
                with self.subTest(error=error):
                    original = (crate / path).read_text()
                    (crate / path).write_text(original.replace(old, new))
                    target_fns.cache_clear()
                    crate_targets.cache_clear()
                    try:
                        self.assertIn(prefix + error, named_test_violations(workflow, tmp))
                    finally:
                        (crate / path).write_text(original)
                        target_fns.cache_clear()
                        crate_targets.cache_clear()

    def test_shell_shapes_and_binary_targets_resolve_like_cargo(self):
        with tempfile.TemporaryDirectory() as tmp:
            for crate, files in (("demo", FIXTURE_FILES), ("daemon", FIXTURE_BIN_FILES)):
                for name, content in files.items():
                    path = Path(tmp) / "crates" / crate / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(content)
            for run, expected in FIXTURE_SHELL_SHAPES:
                with self.subTest(run=run):
                    workflow = {"jobs": {"queue": {"steps": [{"name": "Shapes", "run": run}]}}}
                    self.assertEqual(named_test_violations(workflow, tmp), ["queue: Shapes: " + e for e in expected])


class TunnelArtifactTests(unittest.TestCase):
    """Execute the real tunnel workflow with Cargo artifacts and native children."""

    TESTS = {
        "tunnel::tests::connect_pending_deadline_emits_terminal_open_err",
        "tunnel::tests::connect_reset_preserves_error_before_deadline",
        "tunnel::tests::connect_success_before_deadline_relays",
        "e2e_wss_tunnel::tunnel_open_echo_eof_close_lifecycle",
        "e2e_wss_tunnel::tunnel_open_err_for_closed_port_keeps_connection_alive",
        "e2e_wss_tunnel::tunnel_close_tears_down_and_frees_stream_id",
        "e2e_wss_tunnel::tunnel_connections_counted_in_health_and_stopped",
        "e2e_wss_tunnel::tunnel_unauthenticated_upgrade_rejected",
    }

    @classmethod
    def setUpClass(cls):
        workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
        cls.step = next(s for s in workflow["jobs"]["build"]["steps"]
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
            stub = root / "rustup"
            stub.write_text(f"#!{sys.executable} -S\n" + r'''
import json, os, pathlib, shutil, sys
args = sys.argv[1:]
root = pathlib.Path(os.environ["TUNNEL_ROOT"])
mode = os.environ["TUNNEL_MODE"]
with open(os.environ["TUNNEL_LOG"], "a") as log:
    log.write(json.dumps({"kind": "rustup", "args": args,
                         "policy": os.environ.get("INTENTD_ASSERT_BOUND_CALLER")}) + "\n")
assert args[:2] == ["run", "pinned-channel"], args
if args[2:] == ["rustc", "--print", "target-libdir", "--target", "aarch64-apple-darwin"]:
    print(root / "toolchain lib"); sys.exit(0)
assert args[2:5] == ["cargo", "test", "--locked"], args
assert args[args.index("--target") + 1] == "aarch64-apple-darwin", args
if "--no-run" not in args:
    sys.exit("unexpected recompilation in bounded discovery/runtime")
if mode == "build-failure": sys.exit(25)
package = args[args.index("-p") + 1]
kind = "lib" if "--lib" in args else "test"
name = package.replace("-", "_") if kind == "lib" else args[args.index("--test") + 1]
package_dir = root / "crates" / package
package_dir.mkdir(parents=True, exist_ok=True)
(package_dir / "Cargo.toml").touch()
executable = root / "target with spaces" / "aarch64-apple-darwin" / "debug" / "deps" / name
executable.parent.mkdir(parents=True, exist_ok=True)
shutil.copy2(root / "fixture binary", executable)
artifact = {"reason": "compiler-artifact", "manifest_path": str(package_dir / "Cargo.toml"),
            "target": {"kind": [kind], "name": name}, "profile": {"test": True},
            "executable": str(executable)}
print(json.dumps({"reason": "compiler-artifact", "profile": {"test": False}, "executable": "/wrong"}))
print(json.dumps({"reason": "build-script-executed", "linked_paths": [
    "native=" + str(executable.parent.parent / "build/native lib"), "/outside/target"]}))
if mode == "wrong-target": artifact["target"]["name"] = "other_test"
if mode == "wrong-package": artifact["manifest_path"] = str(root / "other/Cargo.toml")
if mode == "non-test": artifact["profile"]["test"] = False
if mode == "missing-file": executable.unlink()
if mode == "missing-executable": artifact["executable"] = None
if mode != "missing-artifact": print(json.dumps(artifact))
if mode == "ambiguous-artifact": print(json.dumps(artifact))
print(json.dumps({"reason": "build-finished", "success": True}))
''')
            stub.chmod(0o755)
            env = {**os.environ, **self.step["env"], "INTENTD_TUNNEL_TOOLCHAIN": "pinned-channel",
                   "INTENTD_TUNNEL_TARGET": "aarch64-apple-darwin", "TUNNEL_ROOT": tmp,
                   "TUNNEL_MODE": mode, "TUNNEL_LOG": str(root / "calls.jsonl"),
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
            calls = [json.loads(line) for line in (root / "calls.jsonl").read_text().splitlines()]
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
                self.assertEqual(len(builds), 2)
                for call in builds:
                    self.assertIn("--no-run", call["args"])
                    self.assertIn("--message-format=json-render-diagnostics", call["args"])
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
                    self.assertIn(str(Path(tmp) / "target with spaces/aarch64-apple-darwin/debug/deps"), libraries)
                    self.assertIn(str(Path(tmp) / "toolchain lib"), libraries)
                    self.assertIn(str(Path(tmp) / "target with spaces/aarch64-apple-darwin/debug/build/native lib"), libraries)
                    self.assertNotIn("/outside/target", libraries)
                    self.assertIn(str(Path(tmp) / "existing lib") if inherited else "/usr/lib", libraries)
                    if "--list" not in call["args"]:
                        self.assertIn("--nocapture", call["args"])
                        self.assertIn("--test-threads=1", call["args"])

    def test_artifact_or_test_failure_cannot_become_success(self):
        for mode in ("build-failure", "missing-artifact", "missing-executable", "missing-file",
                     "wrong-package", "wrong-target", "non-test", "ambiguous-artifact",
                     "fail-list", "missing-test", "extra-test", "fail-run", "zero-tests", "ignored-test"):
            with self.subTest(mode=mode):
                result, calls, _ = self.probe(mode)
                self.assertNotEqual(result.returncode, 0)
                native = [c for c in calls if c["kind"] == "native"]
                if mode in ("build-failure", "missing-artifact", "missing-executable", "missing-file",
                            "wrong-package", "wrong-target", "non-test", "ambiguous-artifact"):
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
        cls.workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
        cls.job = cls.workflow["jobs"]["coverage-changed"]
        cls.step = next(s for s in cls.job["steps"] if s.get("name") == "Run callback fixture composition under llvm-cov")

    def probe(self, *, target=None, coverage_target=None, listing="exact", failure=None, style="wrapper", plans=None, storage=False):
        with tempfile.TemporaryDirectory(prefix="callback-discovery-") as tmp:
            root = Path(tmp)
            stub = root / "cargo"
            stub.write_text(f"#!{sys.executable} -S\n" + r'''import json, os, shlex, sys
args = sys.argv[1:]
keys = ["RUSTFLAGS", "RUSTC_WRAPPER", "CARGO_TARGET_DIR", "CARGO_LLVM_COV_TARGET_DIR",
        "CARGO_LLVM_COV_SHOW_ENV", "LLVM_PROFILE_FILE", "INTENTD_ASSERT_BOUND_CALLER",
        "INTENTD_TEST_TIMEOUT_MULTIPLIER", "__CARGO_LLVM_COV_RUSTC_WRAPPER"]
with open(os.environ["CHILD_LOG"], "a") as log:
    log.write(json.dumps({"args": args, "env": {k: os.environ.get(k) for k in keys}}) + "\n")
if args[0] == "metadata":
    print(json.dumps({"target_directory": os.environ.get("CARGO_TARGET_DIR", os.environ["RUNNER_TEMP"] + "/target")}))
elif args[:2] == ["llvm-cov", "show-env"]:
    if os.environ["FAILURE"] == "show-env": sys.exit(9)
    target = os.environ.get("CARGO_LLVM_COV_TARGET_DIR", os.environ.get("CARGO_TARGET_DIR", os.environ["RUNNER_TEMP"] + "/target"))
    exports = {"CARGO_LLVM_COV_TARGET_DIR": os.environ.get("CARGO_TARGET_DIR", os.environ["RUNNER_TEMP"] + "/target"), "CARGO_LLVM_COV_SHOW_ENV": "1", "LLVM_PROFILE_FILE": target + "/test.profraw"}
    if os.environ["STYLE"] == "wrapper":
        exports.update(RUSTC_WRAPPER="coverage-wrapper", __CARGO_LLVM_COV_RUSTC_WRAPPER="1")
    else:
        exports["RUSTFLAGS"] = os.environ.get("RUSTFLAGS", "") + " -C instrument-coverage --cfg=coverage"
    for key, value in exports.items(): print("export " + key + "=" + shlex.quote(value))
elif args[:2] == ["nextest", "list"]:
    if os.environ["FAILURE"] == "list": sys.exit(10)
    names = os.environ["CALLBACK_FIXTURE_TESTS"].split()
    mode = os.environ["LISTING"]
    if mode == "missing": names.pop()
    if mode == "extra": names.append("unexpected_test")
    if mode == "wrong": names[-1] = "wrong_test"
    if mode == "empty": names = []
    cases = {n: {"filter-match": {"status": "matches"}} for n in names}
    cases["unselected_test"] = {"filter-match": {"status": "mismatch"}}
    suites = {"unit": {"testcases": cases}}
    if mode == "duplicate": suites["duplicate"] = {"testcases": {names[0]: cases[names[0]]}}
    print(json.dumps({"rust-suites": suites}))
elif args[:3] == ["llvm-cov", "--no-report", "nextest"]:
    sys.exit(11 if os.environ["FAILURE"] == "run" else 0)
else:
    sys.exit("unexpected cargo invocation: " + str(args))
''')
            stub.chmod(0o755)
            env = {k: v for k, v in os.environ.items() if not k.startswith(("CARGO_", "RUST", "LLVM_", "__CARGO_"))}
            env.update({k: str(v) for k, v in self.job["env"].items()})
            env.update({k: str(v) for k, v in self.step["env"].items()})
            env.update(PATH=str(root) + os.pathsep + os.environ["PATH"], RUNNER_TEMP=tmp,
                       CHILD_LOG=str(root / "calls"), LISTING=listing, FAILURE=failure or "", STYLE=style,
                       RUSTFLAGS="--cfg=existing", RUSTC_WRAPPER="existing-wrapper")
            if target: env["CARGO_TARGET_DIR"] = str(root / target)
            if coverage_target: env["CARGO_LLVM_COV_TARGET_DIR"] = str(root / coverage_target)
            if storage:
                evidence = root / "evidence"
                evidence.mkdir()
                (evidence / "paths.json").write_text(json.dumps({name: str(root) for name in
                    ("workspace", "target", "profiles", "temporary", "home")}))
                env["COVERAGE_STORAGE_DIR"] = str(evidence)
            head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
            (root / "coverage-changed-plan.json").write_text(json.dumps({
                "version": 1, "head": head, "mergeBase": head,
                "plans": [] if plans is None else plans,
            }))
            result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", self.step["run"]],
                                    env=env, cwd=ROOT, capture_output=True, text=True, timeout=10)
            calls = [json.loads(line) for line in (root / "calls").read_text().splitlines()]
            expected_target = str(root / (coverage_target or ((target or "target") + "/llvm-cov-target")))
            if storage:
                self.storage_rows = [json.loads(line) for line in
                                     (evidence / "resources.jsonl").read_text().splitlines()]
            return result, calls, expected_target

    def test_storage_evidence_preserves_callback_discovery_and_execution_status(self):
        for failure, code in ((None, 0), ("show-env", 9), ("list", 10), ("run", 11)):
            with self.subTest(failure=failure):
                result, _, _ = self.probe(failure=failure, storage=True)
                self.assertEqual(result.returncode, code, result.stderr)
                self.assertEqual([(row["stage"], row["event"]) for row in self.storage_rows],
                                 [("callback", "start"), ("callback", "exit")])
                self.assertEqual(self.storage_rows[-1]["exit_code"], code)

    def test_instrumented_discovery_reuses_execution_build_without_environment_leak(self):
        for target, override in ((None, None), ("slot target", None), ("slot target", "coverage override")):
            for style in ("wrapper", "flags"):
                with self.subTest(target=target, override=override, style=style):
                    result, calls, target_dir = self.probe(target=target, coverage_target=override, style=style)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    listing = next(c for c in calls if c["args"][:2] == ["nextest", "list"])
                    run = next(c for c in calls if c["args"][:3] == ["llvm-cov", "--no-report", "nextest"])
                    self.assertEqual(listing["env"]["CARGO_LLVM_COV_SHOW_ENV"], "1")
                    self.assertEqual(listing["args"][listing["args"].index("--target-dir") + 1], target_dir)
                    self.assertEqual(run["env"]["CARGO_LLVM_COV_TARGET_DIR"], target_dir)
                    self.assertEqual(run["env"]["RUSTFLAGS"], "--cfg=existing")
                    self.assertEqual(run["env"]["RUSTC_WRAPPER"], "existing-wrapper")
                    for key in ("CARGO_LLVM_COV_SHOW_ENV", "LLVM_PROFILE_FILE", "__CARGO_LLVM_COV_RUSTC_WRAPPER"):
                        self.assertIsNone(run["env"][key], key)
                    if style == "wrapper":
                        self.assertEqual(listing["env"]["RUSTC_WRAPPER"], "coverage-wrapper")
                    else:
                        self.assertEqual(listing["env"]["RUSTFLAGS"].count("instrument-coverage"), 1)
                    for call in (listing, run):
                        args = call["args"]
                        self.assertEqual([args[i + 1] for i, arg in enumerate(args) if arg == "-p"], ["intent-acp", "intent-services"])
                        self.assertIn("--lib", args)
                        for flag in ("--release", "--cargo-profile", "--profile", "--features", "--all-features", "--no-default-features", "--target"):
                            self.assertNotIn(flag, args)
                        self.assertEqual(call["env"]["INTENTD_ASSERT_BOUND_CALLER"], "1")
                        self.assertEqual(call["env"]["INTENTD_TEST_TIMEOUT_MULTIPLIER"], "3")
                    self.assertEqual(listing["args"][listing["args"].index("-E") + 1], run["args"][run["args"].index("-E") + 1])
                    self.assertEqual(run["args"][run["args"].index("--retries") + 1], "0")
                    self.assertIn("--no-fail-fast", run["args"])

    def test_shared_scope_keeps_packages_targets_and_coverage_directory(self):
        scope = ["-p", "intent-core", "-p", "intent-services", "-p", "intent-transport",
                 "--lib", "--bins", "--tests"]
        result, calls, target = self.probe(plans=[scope], coverage_target="coverage override")
        self.assertEqual(result.returncode, 0, result.stderr)
        listing = next(c for c in calls if c["args"][:2] == ["nextest", "list"])
        run = next(c for c in calls if c["args"][:3] == ["llvm-cov", "--no-report", "nextest"])
        expected = scope + ["-p", "intent-acp"]
        self.assertEqual(listing["args"][2:listing["args"].index("-E")],
                         expected + ["--target-dir", target])
        self.assertEqual(run["args"][3:run["args"].index("-E")], expected)
        self.assertEqual(run["env"]["CARGO_LLVM_COV_TARGET_DIR"], target)
        self.assertIsNone(run["env"]["CARGO_LLVM_COV_SHOW_ENV"])

    def test_exact_selection_and_discovery_failures_prevent_execution(self):
        for mode in ("missing", "extra", "wrong", "empty", "duplicate"):
            with self.subTest(mode=mode):
                result, calls, _ = self.probe(listing=mode)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(any(c["args"][:3] == ["llvm-cov", "--no-report", "nextest"] for c in calls))
        for failure, code in (("show-env", 9), ("list", 10), ("run", 11)):
            with self.subTest(failure=failure):
                result, calls, _ = self.probe(failure=failure)
                self.assertEqual(result.returncode, code, result.stderr)
                if failure != "run":
                    self.assertFalse(any(c["args"][:3] == ["llvm-cov", "--no-report", "nextest"] for c in calls))

    def test_callback_inventory_is_exact(self):
        self.assertEqual(self.job["env"]["CALLBACK_FIXTURE_TESTS"].split(), [
            "callback_registration::tests::actual_adapter_sdk_and_rust_mcp_deliver_distinct_callback_preserving_old_result",
            "callback_registration::tests::actual_adapter_replacement_rejects_old_query_and_disabled_never_registers",
            "agent_manager::repository_origin::callback_delivery::tests::live_context::confirmed_context_actual_created_current_reaches_original_guidance_boundary",
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
