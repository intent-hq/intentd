#!/usr/bin/env python3
"""Exercise the real runner probe, native event routing and CI Gate offline.

The small expression interpreter supports only the boolean/context subset used
by these job and native-step conditions; unknown syntax fails closed. This is
not an Actions runner: matrix expressions and implicit success are modeled,
while the actual probe JS and gate shell execute against API/result fixtures.
"""

import ast
import copy
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
RESULTS = ("success", "skipped", "failure", "cancelled")
ALWAYS_REQUIRED = {"route"}
RUNTIME = {"deb-packaging", "release-scripts", "pi-session-paths"}
# Windows lint (native check + target-gated Clippy) runs in the merge queue,
# so a Windows-only compile or lint error cannot reach main (intentd#2574's
# unix-only test helper broke main's push gate that way). The slower Windows
# runtime proofs and release build stay post-merge: they never run on
# pull_request or merge_group, and the push CI Gate (which auto-cut-alpha
# reads on the main tip before cutting) requires all of them. No Windows
# runner is ever requested on pull_request.
WINDOWS_QUEUE_JOBS = {"windows-lint"}
WINDOWS_JOBS = {"windows", "install-ps1"}
# PRs lint the Windows GNU target on Linux (no Windows runner), so most
# Windows-only lint errors fail the PR instead of the queue (intentd#2672).
WINDOWS_PR_JOBS = {"windows-cross-lint"}
REQUIRED = {
    "pull_request": ALWAYS_REQUIRED | WINDOWS_PR_JOBS | {"pr-static", "monorepo-consumer-checks", "check", "coverage-changed", "release-scripts"},
    "merge_group": ALWAYS_REQUIRED | RUNTIME | WINDOWS_QUEUE_JOBS | {"cache-smoke", "daemon-lifecycle", "check", "build", "macos-services-runtime", "coverage-e2e", "coverage-all", "coverage-changed", "monorepo-consumer-checks"},
    "push": ALWAYS_REQUIRED | RUNTIME | WINDOWS_JOBS | {"build"},
}
PR_METADATA = {"pr-title", "docs-pr-scope", "conflict-markers"}
# Release-metadata PRs (release-fast-path reports fast_path=true) skip these
# slow jobs; the queue still requires them (intentd#2493).
FAST_PATH_SKIPS = {"check", "windows-cross-lint", "coverage-changed"}
# Main pushes skip the Windows post-merge job and the paid macOS build when
# release-fast-path reports release_push=true (a tree auto-cut-alpha just
# required green plus release metadata; see
# test_release_push_skips_macos_release_build) or `supersede` reports
# superseded=true (main's live tip is already strictly ahead of this commit,
# and only the tip's proof is read). Every other push job still runs.
RELEASE_PUSH_SKIPS = {"windows", "build"}
SUPERSEDED_SKIPS = {"windows", "build"}
# Advisory gate dependencies: any result is accepted (the gate reads only
# their outputs), so a failed classifier or scan degrades to full CI.
ADVISORY = {"release-fast-path", "supersede"}
# Queue-only watcher outside CI Gate's `needs`: it cancels a doomed run early
# and never decides the gate (scripts/test-merge-queue-fail-fast.py).
QUEUE_WATCHERS = {"fail-fast"}
RELEASE_BRANCH = "release-plz-2026-10-19T04-14-25Z"
# macOS proofs that run once in the merge queue.
NATIVE_STEPS = (
    "Check (native production configuration)",
    "Clippy (target-gated code)",
    "Tunnel deadline and TLS controls (macOS runtime)",
    "Codex focused diagnostics (macOS runtime)",
    "Retained workspace CoW fixtures (macOS runtime)",
)
# The Windows lint, in order, in the queue-only `windows-lint` job.
WINDOWS_LINT_STEPS = {
    "Check (native production configuration)": "cargo check --workspace --target ${{ matrix.target }}",
    "Clippy (target-gated code)": "cargo clippy --workspace --all-targets --target ${{ matrix.target }} -- -D warnings",
}
# Every Windows runtime proof, in order, in the push-only `windows` job.
WINDOWS_STEPS = {
    "Headless workspace setup (Windows runtime)": None,
    "Codex diagnostic process ownership (Windows runtime)": None,
    "Native lifecycle runtime proof (Windows)": "python3 -I -B scripts/test-daemon-lifecycle.py",
    "Build (release)": "cargo build --workspace --release --target ${{ matrix.target }}",
}
WINDOWS_TARGET = "x86_64-pc-windows-msvc"
MACOS_SERVICE_STEPS = {
    "Codex focused diagnostics (macOS runtime)",
    "Retained workspace CoW fixtures (macOS runtime)",
}


def native_step_job(name):
    return "macos-services-runtime" if name in MACOS_SERVICE_STEPS else "build"


GITLAB_STEP = "GitLab auth, invitations and checkout over real sockets with explicit test transports"
# e2e_wss_gitlab_auth is a standalone target; the invitation and checkout
# files are modules of suite targets, selected by their module filter.
GITLAB_COMMAND = (
    "cargo test --locked -p intentd --features repository-test-fixtures "
    "--test e2e_wss_gitlab_auth -- --test-threads=2\n"
    "cargo test --locked -p intentd --features repository-test-fixtures "
    "--test suite_wss_g_r --test suite_wss_repo -- --test-threads=2 "
    "e2e_wss_invite_join_gitlab:: e2e_repository_resource_read::\n"
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
    expression = re.sub(r"startsWith\(github.head_ref, '([\w.-]+)'\)",
                        lambda m: repr(str(context.get("github.head_ref", "")).startswith(m[1])), expression)
    expression = re.sub(r"\b(?:github|needs|runner|env)\.[\w.-]+", lambda m: repr(context.get(m[0], "")), expression)
    expression = expression.replace("&&", " and ").replace("||", " or ")
    expression = re.sub(r"!(?!=)", " not ", expression).strip()
    expression = re.sub(r"\btrue\b|\bfalse\b", lambda m: m[0].title(), expression)
    tree = ast.parse(expression, mode="eval")
    allowed = (ast.Expression, ast.BoolOp, ast.UnaryOp, ast.Compare, ast.Constant,
               ast.And, ast.Or, ast.Not, ast.Eq, ast.NotEq)
    if any(not isinstance(node, allowed) for node in ast.walk(tree)):
        raise ValueError(f"unsupported Actions condition: {value}")
    return (has_status or success) and bool(eval(compile(tree, "<condition>", "eval"), {"__builtins__": {}}))


def template_value(value, values):
    """Evaluate a `${{ }}` template (the concurrency group / cancel flag).

    Supports the condition subset plus `format('literal', context)`; an
    unknown context key raises, so a new input cannot be silently ignored.
    A lone expression keeps its type; mixed text concatenates strings.
    """
    def evaluate(expression):
        # Title-case bare true/false only, never inside quoted literals.
        expression = re.sub(r"'[^']*'|\btrue\b|\bfalse\b",
                            lambda m: m[0] if m[0].startswith("'") else m[0].title(), expression)
        expression = re.sub(r"format\('([^']*)', ([\w.]+)\)",
                            lambda m: repr(m[1].replace("{0}", str(values[m[2]]))), expression)
        expression = re.sub(r"\b(?:github|steps)\.[\w.-]+", lambda m: repr(values[m[0]]), expression)
        expression = expression.replace("&&", " and ").replace("||", " or ")
        expression = re.sub(r"!(?!=)", " not ", expression).strip()
        tree = ast.parse(expression, mode="eval")
        allowed = (ast.Expression, ast.BoolOp, ast.UnaryOp, ast.Compare, ast.Constant,
                   ast.And, ast.Or, ast.Not, ast.Eq, ast.NotEq)
        if any(not isinstance(node, allowed) for node in ast.walk(tree)):
            raise ValueError(f"unsupported Actions expression: {value}")
        return eval(compile(tree, "<template>", "eval"), {"__builtins__": {}})
    if isinstance(value, bool):
        return value
    parts = re.split(r"\$\{\{(.*?)\}\}", str(value))
    if len(parts) == 3 and not parts[0] and not parts[2]:
        return evaluate(parts[1])
    return "".join(part if i % 2 == 0 else str(evaluate(part)) for i, part in enumerate(parts))


def run_concurrency(workflow, event, *, sha, run_id, attempt="1"):
    """(group, cancel-in-progress) the workflow-level concurrency gives one run."""
    ref = {"push": "refs/heads/main", "pull_request": "refs/pull/7/merge",
           "merge_group": f"refs/heads/gh-readonly-queue/main/pr-7-{sha}"}[event]
    values = {"github.workflow": "CI", "github.ref": ref, "github.event_name": event,
              "github.sha": sha, "github.run_id": run_id, "github.run_attempt": attempt}
    block = workflow.get("concurrency") or {}
    return template_value(block.get("group", ""), values), template_value(block.get("cancel-in-progress", False), values)


def concurrency_errors(workflow):
    """Concurrency never cancels or serializes main-push runs.

    GitHub admits runs of one concurrency group in no guaranteed order, so a
    shared cancelling group could let an older commit's late run cancel the
    tip's, and a shared queueing group makes the tip wait behind an older run.
    Each push run gets a group of its own; the `supersede` job retires older
    runs by commit ancestry instead.
    """
    errors = []
    older, older_cancel = run_concurrency(workflow, "push", sha="a" * 40, run_id="10")
    tip, tip_cancel = run_concurrency(workflow, "push", sha="b" * 40, run_id="11")
    rerun, _ = run_concurrency(workflow, "push", sha="a" * 40, run_id="10", attempt="2")
    if older == tip:
        errors.append("push: runs must not share a concurrency group (admission order is not push order)")
    if older_cancel is not False or tip_cancel is not False:
        errors.append("push: concurrency must never cancel a push run")
    if rerun == tip:
        errors.append("push: a re-run must not share the tip's group")
    for event in ("pull_request", "merge_group"):
        group, _ = run_concurrency(workflow, event, sha="c" * 40, run_id="12")
        if group in (older, tip):
            errors.append(f"{event}: must not share a main push group")
    if run_concurrency(workflow, "pull_request", sha="c" * 40, run_id="12")[1] is not True:
        errors.append("pull_request: superseded PR runs must stay cancelled")
    if run_concurrency(workflow, "merge_group", sha="c" * 40, run_id="12")[1] is not False:
        errors.append("merge_group: queue runs must never be cancelled by concurrency")
    for name, job in workflow["jobs"].items():
        if "concurrency" in job:
            errors.append(f"{name}: job-level concurrency could cancel the tip's run")
    return errors


def supersede_job_errors(workflow):
    """The ancestry scan: push-to-main only, hosted, independent, least privilege."""
    errors = []
    job = workflow["jobs"].get("supersede")
    if not job:
        return ["supersede: missing push job"]
    for event in REQUIRED:
        for ref in ("refs/heads/main", "refs/heads/other"):
            values = {**context(event), "github.ref": ref}
            if condition(job.get("if", True), values) != (event == "push" and ref == "refs/heads/main"):
                errors.append(f"supersede: wrong selection for {event}/{ref}")
    if job.get("needs"):
        errors.append("supersede: must start at once, independent of route")
    if job.get("runs-on") != "ubuntu-24.04":
        errors.append("supersede: must run on the free hosted runner")
    if job.get("permissions") != {"actions": "write", "contents": "read"}:
        errors.append("supersede: permissions must be exactly actions: write, contents: read")
    scan = next((step for step in job.get("steps", []) if step.get("id") == "scan"), {})
    if "scripts/supersede-push-runs.py" not in str(scan.get("run", "")) or scan.get("continue-on-error"):
        errors.append("supersede: scan step must run scripts/supersede-push-runs.py")
    if job.get("outputs", {}).get("superseded") != "${{ steps.scan.outputs.superseded }}":
        errors.append("supersede: must export the scan's superseded output")
    for name in SUPERSEDED_SKIPS:
        if "supersede" not in (workflow["jobs"][name].get("needs") or []):
            errors.append(f"{name}: must read the supersede scan")
    tests = workflow["jobs"]["release-scripts"]["steps"]
    if not any(re.search(r"^python3 -I -B scripts/test-supersede-push-runs\.py$", str(step.get("run", "")), re.M)
               and not step.get("if") and not step.get("continue-on-error") for step in tests):
        errors.append("supersede: its offline suite must run in release-scripts")
    return errors


def context(event, *, fork=False, fast_path="", platform="macOS", head_ref="feature", release_push="", superseded=""):
    return {
        "github.event_name": event,
        "github.head_ref": head_ref if event == "pull_request" else "",
        "github.repository": "intent-hq/intentd",
        "github.event.pull_request.head.repo.full_name": "outside/intentd" if fork else "intent-hq/intentd",
        "needs.release-fast-path.outputs.fast_path": fast_path,
        "needs.release-fast-path.outputs.release_push": release_push,
        "needs.supersede.outputs.superseded": superseded,
        "github.ref": "refs/heads/main" if event == "push" else "",
        "runner.os": platform,
    }


def scheduled(workflow, event, *, fork=False, fast_path="", route="success", cancelled=False, head_ref="feature",
              release_push="", classifier=None, superseded="", scan=None):
    results = {}
    pending = dict(workflow["jobs"])
    while pending:
        # Dependency order, not file order (check needs the later
        # release-fast-path job); a cycle or unknown dependency fails loudly.
        ready = [name for name, job in pending.items()
                 if all(n in results for n in ([job["needs"]] if isinstance(job.get("needs"), str) else job.get("needs", [])))]
        if not ready:
            raise ValueError(f"unresolvable job dependencies: {sorted(pending)}")
        name = ready[0]
        job = pending.pop(name)
        needs = job.get("needs", [])
        if isinstance(needs, str):
            needs = [needs]
        values = context(event, fork=fork, fast_path=fast_path, head_ref=head_ref, release_push=release_push,
                         superseded=superseded)
        values.update({f"needs.{key}.result": value for key, value in results.items()})
        runs = condition(job.get("if", True), values,
                         success=all(results[n] == "success" for n in needs), cancelled=cancelled)
        outcome = {"route": route, "release-fast-path": classifier or "success", "supersede": scan or "success"}.get(name, "success")
        results[name] = outcome if runs else "skipped"
    return results


def gate_passes(workflow, event, results, fast_path="", release_push="", superseded=""):
    body = next(s["run"] for s in workflow["jobs"]["gate"]["steps"] if s.get("name") == "Check results")
    values = {"github.event_name": event, "needs.release-fast-path.outputs.fast_path": fast_path,
              "needs.release-fast-path.outputs.release_push": release_push,
              "needs.supersede.outputs.superseded": superseded,
              **{f"needs.{k}.result": v for k, v in results.items()}}
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
    expected = set.union(*REQUIRED.values(), PR_METADATA, ADVISORY) | {"route"}
    if needs != expected:
        errors.append("gate dependencies changed")
    for event, required in REQUIRED.items():
        baseline = {name: "success" if name in required or (event == "pull_request" and name in PR_METADATA)
                    else "skipped" for name in expected}
        if not gate_passes(workflow, event, baseline):
            errors.append(f"{event}: intended skips rejected")
        for name in expected:
            for result in RESULTS:
                accepted = name in ADVISORY or result == "success" or (result == "skipped" and name not in required and not (event == "pull_request" and name in PR_METADATA))
                if gate_passes(workflow, event, {**baseline, name: result}) != accepted:
                    errors.append(f"{event}: {name}={result}")
    errors.extend(fast_path_gate_errors(workflow, expected))
    errors.extend(release_push_gate_errors(workflow, expected))
    return errors


def queue_fail_fast_errors(workflow):
    """The fail-fast watcher cancels on any failed merge_group job, which is
    safe only while every such job's failure already fails CI Gate."""
    errors = []
    jobs = scheduled(workflow, "merge_group")
    ran = {name for name, result in jobs.items() if result == "success"} - {"gate"}
    needs = set(workflow["jobs"]["gate"]["needs"])
    if ran - needs != QUEUE_WATCHERS:
        errors.append(f"merge_group jobs outside CI Gate's needs must be exactly the watchers: {sorted(ran - needs)}")
    baseline = {name: jobs.get(name, "skipped") for name in needs}
    if not gate_passes(workflow, "merge_group", baseline):
        errors.append("merge_group: baseline rejected")
    for name in sorted(ran & needs):
        if workflow["jobs"][name].get("continue-on-error"):
            errors.append(f"{name}: a continue-on-error job's failure would cancel a passing run")
        if gate_passes(workflow, "merge_group", {**baseline, name: "failure"}):
            errors.append(f"{name}: CI Gate accepts its failure, so the watcher would eject a passing entry")
    return errors


def release_push_gate_errors(workflow, expected):
    """The push gate accepts a skipped Windows job only on a reported skip.

    A skip is reported when release-fast-path succeeded with release_push=true
    or supersede succeeded with superseded=true. Anything else fails closed: a
    failed or cancelled Windows job, a skip without the reporting job's
    success, and a skip of any other push job.
    """
    errors = []
    required = REQUIRED["push"]
    # The skip flags take all three values; cancelled is covered by the
    # per-job results below and by gate_errors' generic table.
    flags, outcomes = ("", "false", "true"), ("success", "skipped", "failure")
    for release_push in flags:
        for classifier in outcomes:
            for superseded in flags:
                for scan in outcomes:
                    release = release_push == "true" and classifier == "success"
                    stale = superseded == "true" and scan == "success"
                    skips = (RELEASE_PUSH_SKIPS if release else set()) | (SUPERSEDED_SKIPS if stale else set())
                    baseline = {name: "success" if name in required else "skipped" for name in expected}
                    baseline["release-fast-path"] = classifier
                    baseline["supersede"] = scan
                    for name in skips:
                        baseline[name] = "skipped"
                    label = (f"push/release_push={release_push or '-'}/release-fast-path={classifier}"
                             f"/superseded={superseded or '-'}/supersede={scan}")
                    flags_in = {"release_push": release_push, "superseded": superseded}
                    if not gate_passes(workflow, "push", baseline, **flags_in):
                        errors.append(f"{label}: baseline")
                    for name in required:
                        for result in ("skipped", "failure", "cancelled"):
                            accepted = result == "skipped" and name in skips
                            if gate_passes(workflow, "push", {**baseline, name: result}, **flags_in) != accepted:
                                errors.append(f"{label}: {name}={result}")
    return errors


def fast_path_gate_errors(workflow, expected):
    """The PR gate accepts skipped slow jobs only on a reported fast path."""
    errors = []
    required = REQUIRED["pull_request"] | PR_METADATA
    for fast_path in ("", "false", "true"):
        for classifier in RESULTS:
            reported = fast_path == "true" and classifier == "success"
            baseline = {name: "success" if name in required else "skipped" for name in expected}
            baseline["release-fast-path"] = classifier
            for name in FAST_PATH_SKIPS:
                baseline[name] = "skipped" if reported else "success"
            label = f"pull_request/fast_path={fast_path or '-'}/release-fast-path={classifier}"
            if not gate_passes(workflow, "pull_request", baseline, fast_path):
                errors.append(f"{label}: baseline")
            for name in FAST_PATH_SKIPS:
                for result in RESULTS:
                    accepted = result == "success" or (result == "skipped" and reported)
                    if gate_passes(workflow, "pull_request", {**baseline, name: result}, fast_path) != accepted:
                        errors.append(f"{label}: {name}={result}")
    return errors


def routing_errors(workflow):
    errors = []
    for event in REQUIRED:
        if event == "pull_request":
            cases = [("feature", "", "")] + [(RELEASE_BRANCH, f, "") for f in ("", "false", "true")]
        elif event == "push":
            cases = [("", "", r, x) for r in ("", "false", "true") for x in ("", "false", "true")]
        else:
            cases = [("", "", "", "")]
        for head_ref, fast_path, release_push, superseded in [c if len(c) == 4 else (*c, "") for c in cases]:
            jobs = scheduled(workflow, event, fast_path=fast_path, head_ref=head_ref, release_push=release_push,
                             superseded=superseded)
            expected = REQUIRED[event] | {"route", "gate"}
            if event == "pull_request":
                expected |= PR_METADATA
                if head_ref == RELEASE_BRANCH:
                    expected |= {"release-fast-path"}
                if fast_path == "true":
                    expected -= FAST_PATH_SKIPS
            if event == "merge_group":
                expected |= QUEUE_WATCHERS
            if event == "push":
                # Every main push classifies its diff against the previous tip
                # and scans for superseded runs.
                expected |= {"release-fast-path", "supersede"}
                if release_push == "true":
                    expected -= RELEASE_PUSH_SKIPS
                if superseded == "true":
                    expected -= SUPERSEDED_SKIPS
            actual = {name for name, result in jobs.items() if result == "success"}
            if actual != expected:
                errors.append(f"{event}/{head_ref}/{fast_path}/{release_push}/{superseded}: wrong jobs {actual ^ expected}")
    # A failed or cancelled classifier or scan reports nothing: Windows still runs.
    for result in ("failure", "cancelled"):
        if scheduled(workflow, "push", classifier=result)["windows"] != "success":
            errors.append(f"push: Windows skipped after release-fast-path={result}")
        for job in SUPERSEDED_SKIPS:
            if scheduled(workflow, "push", scan=result)[job] != "success":
                errors.append(f"push: {job} skipped after supersede={result}")
    if any(value != "skipped" for value in scheduled(workflow, "pull_request", fork=True).values()):
        errors.append("fork PR allocated runners")
    for event in REQUIRED:
        for route in ("failure", "skipped", "cancelled"):
            jobs = scheduled(workflow, event, route=route)
            if event != "push" and gate_passes(workflow, event, jobs):
                errors.append(f"{event}: gate accepted route={route}")
            # Native routing deliberately falls back to hosted runners on a
            # failed/empty probe. Linux required checks still fail the gate.
            if event != "pull_request" and jobs["build"] != "success":
                errors.append(f"{event}: missing native fallback for route={route}")
            if event == "push" and jobs["windows"] != "success":
                errors.append(f"{event}: missing Windows fallback for route={route}")
            if event == "merge_group" and jobs["windows-lint"] != "success":
                errors.append(f"{event}: missing Windows lint fallback for route={route}")
    return errors


PI_OS_MATRIX = re.compile(
    r"\$\{\{ fromJSON\(github\.event_name == '(\w+)' && '(\[[^']*\])' \|\| '(\[[^']*\])'\) \}\}")


def matrix_values(value, event):
    """Evaluate a matrix dimension: a literal list or the event-selected fromJSON form."""
    if isinstance(value, list):
        return value
    match = PI_OS_MATRIX.fullmatch(str(value))
    if not match:
        raise ValueError(f"unsupported matrix expression: {value}")
    return json.loads(match[2] if event == match[1] else match[3])


def windows_legs(workflow, event):
    """Jobs (and matrix legs) that would request a Windows runner on `event`."""
    legs = set()
    jobs = scheduled(workflow, event)
    for name, job in workflow["jobs"].items():
        if jobs[name] != "success":
            continue
        matrix = job.get("strategy", {}).get("matrix", {})
        candidates = [str(job.get("runs-on", ""))]
        for row in matrix.get("include", []):
            candidates.extend(str(value) for value in row.values())
        for key, value in matrix.items():
            if key not in ("include", "exclude"):
                candidates.extend(str(item) for item in matrix_values(value, event))
        if any("windows" in candidate.lower() for candidate in candidates):
            legs.add(name)
    return legs


def windows_errors(workflow):
    errors = []
    if windows_legs(workflow, "pull_request"):
        errors.append(f"pull_request: Windows runner requested by {sorted(windows_legs(workflow, 'pull_request'))}")
    if windows_legs(workflow, "merge_group") != WINDOWS_QUEUE_JOBS:
        errors.append(f"merge_group: Windows legs changed {sorted(windows_legs(workflow, 'merge_group'))}")
    if windows_legs(workflow, "push") != WINDOWS_JOBS | {"pi-session-paths"}:
        errors.append(f"push: Windows legs changed {sorted(windows_legs(workflow, 'push'))}")
    pi = workflow["jobs"]["pi-session-paths"]["strategy"]["matrix"]["os"]
    for event in REQUIRED:
        expected = ["ubuntu-24.04", "windows-2025"] if event == "push" else ["ubuntu-24.04"]
        if matrix_values(pi, event) != expected:
            errors.append(f"pi-session-paths: wrong platforms on {event}")
    errors += windows_job_errors(workflow, "windows-lint", "merge_group", WINDOWS_LINT_STEPS)
    errors += windows_job_errors(workflow, "windows", "push", WINDOWS_STEPS)
    # Moved, not duplicated: main's tree is one the queue just linted. Match
    # the cargo subcommand too, so a renamed copy is still caught.
    for step in workflow["jobs"].get("windows", {}).get("steps", []):
        run = str(step.get("run", ""))
        if step.get("name") in WINDOWS_LINT_STEPS or re.search(r"\bcargo\s+(?:\+\S+\s+)?(?:check|clippy)\b", run):
            errors.append("windows: lint steps belong to the queue-only windows-lint job")
            break
    return errors


def windows_job_errors(workflow, name, event, proofs):
    """One routed Windows job: required, runs only on `event`, every proof in order."""
    errors = []
    job = workflow["jobs"].get(name)
    if not job:
        return [f"{name}: missing {event} job"]
    if job.get("continue-on-error") or job.get("strategy", {}).get("fail-fast") is True:
        errors.append(f"{name}: job must stay required")
    rows = job["strategy"]["matrix"]["include"]
    if [row.get("target") for row in rows] != [WINDOWS_TARGET] or job["runs-on"] != "${{ matrix.os }}":
        errors.append(f"{name}: single Windows target routed through matrix.os")
    for route, cancelled in (("success", False), ("failure", False), ("skipped", False), ("success", True)):
        for candidate in REQUIRED:
            runs = condition(job["if"], {**context(candidate), "needs.route.result": route},
                             success=route == "success", cancelled=cancelled)
            if runs != (candidate == event and not cancelled):
                errors.append(f"{name}: wrong selection for {candidate}/route={route}/cancelled={cancelled}")
    steps = job["steps"]
    found = [step.get("name") for step in steps if step.get("name") in proofs]
    if found != list(proofs):
        errors.append(f"{name}: proofs missing or reordered {found}")
    for proof, command in proofs.items():
        step = next((s for s in steps if s.get("name") == proof), None)
        if not step or not step.get("run") or step.get("continue-on-error") or "if" in step:
            errors.append(f"{proof}: missing required Windows proof in {name}")
        elif command is not None and step["run"] != command:
            errors.append(f"{proof}: Windows command changed in {name}")
    return errors


def native_errors(workflow):
    errors = []
    build = workflow["jobs"]["build"]
    steps = {step.get("name"): step for step in build["steps"]}
    if build.get("continue-on-error") or build["strategy"]["fail-fast"]:
        errors.append("native matrix must be required and independent")
    targets = {entry["target"] for entry in build["strategy"]["matrix"]["include"]}
    if targets != {"aarch64-apple-darwin"}:
        errors.append("build must carry only the macOS target (Windows is post-merge)")
    for name in NATIVE_STEPS:
        owner = workflow["jobs"][native_step_job(name)]
        step = next((s for s in owner["steps"] if s.get("name") == name), None)
        if not step or owner.get("continue-on-error") or step.get("continue-on-error") or not step.get("run"):
            errors.append(f"{name}: missing required proof")
            continue
        for event in ("merge_group", "push"):
            if condition(step.get("if", True), context(event, platform="macOS")) != (event == "merge_group"):
                errors.append(f"{name}: wrong selection for {event}/macOS")
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
        # The warm check only helps a runner that keeps its target dir, so it
        # runs on self-hosted runners alone; the release build runs on any.
        elif any(condition(step.get("if", True), {**context(event), "matrix.os": ["self-hosted", "macOS", "ARM64"]})
                 != (event == "push") for event in REQUIRED):
            errors.append(f"{name}: must be push-only")
        elif condition(step.get("if", True), {**context("push"), "matrix.os": ["warp-macos-26-arm64-6x"]}) != (
                name == "Build (release)"):
            errors.append(f"{name}: wrong selection for an ephemeral push runner")
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


NODE_TOOLS = r"(?:node|npx|npm|pnpm|corepack)"
NODE_COMMAND = re.compile(r"(?m)(?:^|[;&|(]|\$\(|\b(?:then|do|exec|time)\s)\s*(?:\w+=\S*\s+)*" + NODE_TOOLS + r"(?=\s+[^=\s]|\s*$)")
NODE_PYTHON = re.compile(r"""(?:\[\s*|shutil\.which\(\s*)["']""" + NODE_TOOLS + r"""["']""")
SCRIPT_EXECUTION = re.compile(
    r"(?m)(?:^|[;&|(]\s*|\$\(\s*|(?:python3?|bash|sh)(?:\s+-X\s+\S+|\s+-\S+)*\s+|\./)(scripts/[\w./-]+\.(?:py|sh))\b")


def without_comments(text):
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def executes_node(path):
    if not path.is_file():
        return False
    text = without_comments(path.read_text())
    if path.suffix == ".py":
        return bool(NODE_PYTHON.search(text))
    return bool(NODE_COMMAND.search(text))


def node_without_setup(workflow):
    """Jobs whose steps execute Node before an unconditional actions/setup-node step.

    GitHub-hosted images preinstall Node, but the self-hosted runner images do not,
    so routing a job there fails with `node` not found unless it sets Node up.
    A conditional setup-node step covers only later steps with the identical
    `if:` expression (a Windows-only setup does not cover Linux). Detected:
    node/npx/npm/pnpm/corepack at a shell command position (comment lines
    ignored), and directly executed scripts/*.py that spawn or look up those
    tools or scripts/*.sh that run them. Not detected: Node spawned by compiled
    tests (cargo/nextest), by scripts called from other scripts, or by inline
    heredoc programs; those jobs must keep their setup-node step by review.
    """
    errors = []
    for job_id, job in workflow["jobs"].items():
        ready = set()
        for step in job.get("steps", []):
            if "actions/setup-node@" in step.get("uses", ""):
                ready.add(step.get("if"))
            command = without_comments(step.get("run", ""))
            if not command or None in ready or step.get("if", False) in ready:
                continue
            if NODE_COMMAND.search(command) or any(
                    executes_node(ROOT / reference) for reference in SCRIPT_EXECUTION.findall(command)):
                errors.append(f"{job_id}: {step.get('name') or command.splitlines()[0]}")
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
    listWorkflowRunsForRepo: 'runs', listJobsForWorkflowRun: 'jobs',
  } },
  async paginate(endpoint, args) {
    if (endpoint === 'runs') {
      if ((fixture.queueErrors || []).includes(args.repo)) throw new Error('queue unavailable');
      return args.repo === 'intentd' && args.status === 'queued' ? [{id: 1}] : [];
    }
    if (endpoint === 'jobs') return fixture.jobs || [];
    calls.push({ endpoint, args });
    if (fixture[endpoint + 'Error']) throw new Error(fixture[endpoint + 'Error']);
    return fixture[endpoint] ?? [];
  },
};
const core = { info: message => logs.push(message), setOutput: (name, value) => { outputs[name] = value; } };
const context = { repo: { owner: 'intent-hq', repo: 'intentd' }, runId: 99, eventName: fixture.eventName || 'push', payload: fixture.payload || {} };
process.env.THRESHOLD = '2';
process.env.XL_THRESHOLD = fixture.xlThreshold || '';
// The fleet's configured bigmem limit; pass '' for the unset default (0).
process.env.BIGMEM_THRESHOLD = fixture.bigmemThreshold ?? '2';
process.env.LINUX_SIZE = '16';
process.env.WINDOWS_RUNNER_DISABLED = fixture.windowsDisabled || '';
process.env.MACOS_RUNNER_DISABLED = fixture.macosDisabled || '';
process.env.LINUX_RUNNER_DISABLED = fixture.linuxDisabled || '';
process.env.SLOT_JOBS = fixture.slotJobs || '';
// Most cases check the GitHub fallback; the WarpBuild cases turn WarpBuild on.
const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
new AsyncFunction('github', 'core', 'context', script)(github, core, context)
  .then(() => process.stdout.write(JSON.stringify({ outputs, calls, logs })))
  .catch(error => { console.error(error); process.exitCode = 1; });
"""
    result = subprocess.run(["node", "-e", harness],
                            input=json.dumps({"script": step["with"]["script"],
                                              "fixture": {"slotJobs": step["env"].get("SLOT_JOBS", ""), **fixture}}),
                            env={k: v for k, v in os.environ.items() if k != "NODE_OPTIONS"},
                            capture_output=True, text=True, timeout=10, check=True)
    return json.loads(result.stdout)


def run_heavy(workflow, decide, event):
    """Execute the route job's actual merge-queue override step offline."""
    job = workflow["jobs"]["route"]
    step = next(s for s in job["steps"] if s.get("id") == "heavy")
    env = {"PATH": os.environ["PATH"], "EVENT": event, **{k: str(v) for k, v in job.get("env", {}).items()}}
    for key, expression in step["env"].items():
        match = re.fullmatch(r"\$\{\{ steps\.decide\.outputs\.(\w+) \}\}", expression)
        if match:
            env[key] = decide.get(match[1], "")
        elif expression != "${{ github.event_name }}":
            raise ValueError(f"unsupported heavy step input: {expression}")
    with tempfile.TemporaryDirectory() as directory:
        env["GITHUB_OUTPUT"] = str(Path(directory) / "output")
        logs = subprocess.run(["bash", "-eo", "pipefail", "-c", step["run"]], env=env,
                              capture_output=True, text=True, timeout=10, check=True).stdout
        outputs = dict(line.split("=", 1) for line in Path(env["GITHUB_OUTPUT"]).read_text().splitlines())
    return outputs, logs


def route_outputs(workflow, outputs, event="push"):
    result = {}
    heavy = None
    for key, expression in workflow["jobs"]["route"]["outputs"].items():
        match = re.fullmatch(r"\$\{\{ steps\.(decide|heavy)\.outputs\.(\w+) \}\}", expression)
        if not match:
            raise ValueError(f"unsupported job output: {expression}")
        if match[1] == "heavy":
            heavy = heavy or run_heavy(workflow, outputs, event)[0]
        result[key] = (heavy if match[1] == "heavy" else outputs).get(match[2], "")
    return result


def matrix_labels(value, outputs):
    """Evaluate only the actual matrix's output-or-JSON-fallback expression."""
    if isinstance(value, list):
        return value
    slot = re.fullmatch(r"\$\{\{ fromJSON\(needs\.route\.outputs\.slots \|\| '\{\}'\)\['([\w-]+)'\] \|\| fromJSON\('([^']+)'\) \}\}", value)
    if slot:
        return json.loads(outputs.get("slots", "") or "{}").get(slot[1]) or json.loads(slot[2])
    match = re.fullmatch(r"\$\{\{ fromJSON\(needs\.route\.outputs\.(\w+) \|\| '([^']+)'\) \}\}", value)
    if not match:
        raise ValueError(f"unsupported runner expression: {value}")
    return json.loads(outputs.get(match[1], "") or match[2])


def runner(platform="macOS", arch="ARM64", *, status="online", busy=False, extra=()):
    return {"status": status, "busy": busy,
            "labels": [{"name": name} for name in ("self-hosted", platform, arch, *extra)]}


# intent-cache slots register with GitHub's default labels plus `intent-cache`;
# routed Linux jobs ask for exactly these labels.
CACHE = ["self-hosted", "linux", "x64", "intent-cache"]
SMALL = ["self-hosted", "linux", "x64", "intent-cache-small"]
XL = ["self-hosted", "linux", "x64", "intent-cache-xl"]
# Heavy slots with 30 GB or more also carry `intent-cache-bigmem`; gh-linux-16x
# jobs overflow from xl only to these.
BIG = ["self-hosted", "linux", "x64", "intent-cache", "intent-cache-bigmem"]


def slot(**kwargs):
    return runner("Linux", "X64", extra=("intent-cache", "intent-cache-bigmem"), **kwargs)


def small_heavy_slot(**kwargs):
    # A 12 GB heavy slot (intent-ci3): 8-core jobs only.
    return runner("Linux", "X64", extra=("intent-cache",), **kwargs)


def xl_slot(**kwargs):
    return runner("Linux", "X64", extra=("intent-cache-xl",), **kwargs)


def small_slot(**kwargs):
    return runner("Linux", "X64", extra=("intent-cache-small",), **kwargs)


class RunnerAvailabilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())

    def assert_mac_route(self, fixture, expected):
        result = run_route(self.workflow, **fixture)
        outputs = route_outputs(self.workflow, result["outputs"])
        self.assertEqual(json.loads(outputs.get("macos_labels", "null")), expected)
        self.assertEqual(outputs.get("macos_burst"), str(expected == ["warp-macos-26-arm64-6x"]).lower())
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
                self.assert_mac_route({"org": fleet}, ["warp-macos-26-arm64-6x"])
        for fleet in ([runner()], [runner(platform="MACos", arch="arm64")],
                      [{**runner(), "labels": [{"name": n} for n in ("SELF-HOSTED", "macos", "arm64")]}],
                      [runner(busy=True), runner(status="offline"), runner(), runner(arch="X64")]):
            with self.subTest(fleet=fleet):
                self.assert_mac_route({"org": fleet}, ["self-hosted", "macOS", "ARM64"])

    def test_org_lookup_and_repo_fallback_are_shared_and_fail_to_hosted(self):
        cases = [({}, ["warp-macos-26-arm64-6x"], ["org"]),
                 ({"orgError": "403 missing permission", "repo": [runner()]},
                  ["self-hosted", "macOS", "ARM64"], ["org", "repo"]),
                 ({"orgError": "403", "repo": [runner(busy=True)]}, ["warp-macos-26-arm64-6x"], ["org", "repo"]),
                 ({"orgError": "403", "repo": []}, ["warp-macos-26-arm64-6x"], ["org", "repo"]),
                 ({"orgError": "403", "repoError": "403 missing permission"}, ["warp-macos-26-arm64-6x"], ["org", "repo"]),
                 ({"orgError": "network timeout", "repoError": "503 unavailable"}, ["warp-macos-26-arm64-6x"], ["org", "repo"]),
                 ({"org": {"unexpected": "response"}}, ["warp-macos-26-arm64-6x"], ["org"]),
                 ({"org": [], "repo": [runner()]}, ["warp-macos-26-arm64-6x"], ["org"])]
        for fixture, expected, endpoints in cases:
            with self.subTest(fixture=fixture):
                result = self.assert_mac_route(fixture, expected)
                self.assertEqual([call["endpoint"] for call in result["calls"]], endpoints)
                for call in result["calls"]:
                    self.assertEqual(call["args"], {"org": "intent-hq", "per_page": 100} if call["endpoint"] == "org"
                                     else {"owner": "intent-hq", "repo": "intentd", "per_page": 100})

    def test_linux_keeps_queue_semantics_and_idle_windows_stays_self_hosted(self):
        fleet = [slot(busy=True)] * 2 + [runner("Windows", "X64"), runner()]
        for depth, queue_errors, linux_burst in ((1, [], False), (2, [], True),
                                               (0, ["intentd"], True), (0, ["cloudlands-fe", "intent"], True)):
            with self.subTest(depth=depth, queue_errors=queue_errors):
                jobs = [{"status": "queued" if i % 2 else "in_progress", "labels": CACHE, "created_at": "2099-01-01T00:00:00Z"}
                        for i in range(depth)]
                jobs += [{"status": "in_progress", "labels": ["gh-linux-16x"]}] * 20
                # Jobs still asking for the generic self-hosted Linux fleet never occupy a slot.
                jobs += [{"status": "in_progress", "labels": ["self-hosted", "Linux", "X64"]}] * 5
                result = self.assert_mac_route({"org": fleet, "jobs": jobs, "queueErrors": queue_errors},
                                              ["warp-macos-26-arm64-6x"] if queue_errors else ["self-hosted", "macOS", "ARM64"])
                outputs = result["outputs"]
                # Both slots are busy, so 16-core work has no idle bigmem runner and bursts;
                # 8-core work keeps the heavy pool's queue semantics.
                self.assertEqual(json.loads(outputs["linux_labels"]), ["warp-ubuntu-2404-x64-16x"])
                self.assertEqual(json.loads(outputs["check_labels"]), ["warp-ubuntu-2404-x64-8x"] if linux_burst else CACHE)
                self.assertEqual(outputs["linux_burst"], "true")
                self.assertEqual(json.loads(outputs["windows_labels"]), ["warp-windows-2022-x64-16x"] if queue_errors else ["self-hosted", "Windows", "X64"])
                self.assertEqual(outputs["windows_burst"], str(bool(queue_errors)).lower())
        for fixture in ({}, {"orgError": "403", "repoError": "403"},
                        {"org": [slot(status="offline"), runner("Windows", "X64", status="offline")]},
                        {"org": [runner("Linux", "X64"), runner("Linux", "X64")]}):
            with self.subTest(fixture=fixture):
                outputs = run_route(self.workflow, **fixture)["outputs"]
                self.assertEqual(json.loads(outputs["linux_labels"]), ["warp-ubuntu-2404-x64-16x"])
                self.assertEqual(json.loads(outputs["check_labels"]), ["warp-ubuntu-2404-x64-8x"])
                self.assertEqual(json.loads(outputs["windows_labels"]), ["warp-windows-2022-x64-16x"])
                self.assertEqual(outputs["linux_burst"], "true")
                self.assertEqual(outputs["windows_burst"], "true")

    def test_merge_queue_heavy_jobs_read_their_own_slots(self):
        # The queue's heavy 16-CPU Rust jobs take only the slots their own
        # merge_group entries win; the shared linux_labels mapping bursts there.
        # Here the one bigmem slot goes to coverage-all, so check bursts too
        # (test_merge_queue_check_takes_a_slot_after_the_coverage_suites covers
        # check on a slot). Light jobs, PRs and pushes keep the probe's choice.
        same_repo = {"pull_request": {"head": {"repo": {"full_name": "intent-hq/intentd"}}}}
        for idle in (True, False):
            fleet = [slot(), small_slot(), runner("Windows", "X64"), runner()] if idle else []
            for event in ("pull_request", "push", "merge_group"):
                with self.subTest(idle=idle, event=event):
                    result = run_route(self.workflow, org=fleet, eventName=event,
                                       payload=same_repo if event == "pull_request" else {})
                    outputs = route_outputs(self.workflow, result["outputs"], event)
                    hosted = event == "merge_group" or not idle
                    slots = json.loads(outputs["slots"])
                    # The one free heavy slot goes to `coverage-changed` on a PR (it ranks
                    # above check) and to coverage-all in the queue. check bursts to its 16x WarpBuild runner.
                    self.assertEqual(slots["check"], ["warp-ubuntu-2404-x64-16x"])
                    self.assertEqual(slots["coverage-changed"], BIG if event == "pull_request" and idle else ["warp-ubuntu-2404-x64-16x"])
                    # Free standard labels never take a self-hosted slot, even an idle one.
                    self.assertEqual(slots["release-scripts"], ["ubuntu-24.04"])
                    # Unlisted heavy jobs only get a slot that is still free after the listed ones.
                    unlisted = hosted or event == "pull_request"
                    self.assertEqual(json.loads(outputs["linux_labels"]),
                                     ["warp-ubuntu-2404-x64-16x"] if unlisted else BIG)
                    self.assertEqual(json.loads(outputs["check_labels"]),
                                     ["warp-ubuntu-2404-x64-8x"] if unlisted else CACHE)
                    self.assertEqual(outputs["linux_burst"], str(unlisted).lower())
                    self.assertEqual(json.loads(outputs["runners"])["ubuntu-24.04"], ["ubuntu-24.04"])
                    if idle:
                        self.assertEqual(json.loads(outputs["windows_labels"]), ["self-hosted", "Windows", "X64"])
                        self.assertEqual(json.loads(outputs["macos_labels"]), ["self-hosted", "macOS", "ARM64"])
                    logged = "merge queue: the shared linux_labels mapping takes the burst runner" in run_heavy(self.workflow, result["outputs"], event)[1]
                    self.assertEqual(logged, event == "merge_group")
        # The queue-only coverage suites read their own merge_group slots (the
        # slot hosts raise the inotify limit to 32768).
        # Their fallback, if the route job failed, is the listed size's WarpBuild runner.
        for job_id, label in {"check": "warp-ubuntu-2404-x64-16x", "coverage-changed": "warp-ubuntu-2404-x64-16x",
                              "coverage-e2e": "warp-ubuntu-2404-x64-16x", "coverage-all": "warp-ubuntu-2404-x64-16x"}.items():
            with self.subTest(job=job_id):
                self.assertEqual(self.workflow["jobs"][job_id]["runs-on"],
                                 "${{ fromJSON(needs.route.outputs.slots || '{}')['%s'] || fromJSON('[\"%s\"]') }}" % (job_id, label))
        lifecycle = self.workflow["jobs"]["daemon-lifecycle"]["strategy"]["matrix"]["include"]
        linux = next(row for row in lifecycle if row["platform"] == "linux")
        # The Linux leg reads its own slot: hosted unless the probe granted one.
        self.assertEqual(matrix_labels(linux["os"], {"slots": '{"daemon-lifecycle-linux":["gh-linux-16x"]}'}), ["gh-linux-16x"])
        self.assertEqual(matrix_labels(linux["os"], {"slots": json.dumps({"daemon-lifecycle-linux": CACHE})}), CACHE)
        self.assertNotIn("linux_labels", linux["os"])
        for job_id in ("release-scripts", "deb-packaging"):
            with self.subTest(job=job_id):
                self.assertIn("needs.route.outputs.slots", self.workflow["jobs"][job_id]["runs-on"])

    def test_free_label_jobs_stay_on_github_with_idle_small_slots(self):
        # Standard GitHub runners are free for this public repository, so the fast
        # jobs and both Linux pi-session-paths legs keep their GitHub label.
        result = run_route(self.workflow, org=[small_slot()] * 3, eventName="merge_group")
        slots = json.loads(result["outputs"]["slots"])
        self.assertNotIn(SMALL, slots.values())
        for key in ("release-scripts", "deb-packaging", "pi-session-paths-22", "pi-session-paths-24"):
            self.assertEqual(slots[key], ["ubuntu-24.04"])
        self.assertEqual(json.loads(result["outputs"]["runners"])["ubuntu-24.04"], ["ubuntu-24.04"])
        runs_on = self.workflow["jobs"]["pi-session-paths"]["runs-on"]
        self.assertIn("format('pi-session-paths-{0}', matrix.node)", runs_on)
        self.assertTrue(runs_on.startswith("${{ matrix.os == 'ubuntu-24.04' && fromJSON(needs.route.outputs.slots"))

    def test_bursts_go_to_warpbuild(self):
        same_repo = {"pull_request": {"head": {"repo": {"full_name": "intent-hq/intentd"}}}}
        warp = {"linux_labels": ["warp-ubuntu-2404-x64-16x"], "check_labels": ["warp-ubuntu-2404-x64-8x"],
                "windows_labels": ["warp-windows-2022-x64-16x"], "macos_labels": ["warp-macos-26-arm64-6x"]}
        for event in ("pull_request", "push", "merge_group"):
            with self.subTest(event=event):
                result = run_route(self.workflow, org=[], eventName=event, warpbuildDisabled="",
                                   payload=same_repo if event == "pull_request" else {})
                outputs = route_outputs(self.workflow, result["outputs"], event)
                for key, labels in warp.items():
                    self.assertEqual(json.loads(outputs[key]), labels, key)
                slots = json.loads(outputs["slots"])
                self.assertEqual(slots["check"], ["warp-ubuntu-2404-x64-16x"])
                self.assertEqual(slots["coverage-changed"], ["warp-ubuntu-2404-x64-16x"])
                self.assertEqual(slots["release-scripts"], ["ubuntu-24.04"])
                cloud = json.loads(outputs["cloud"])
                self.assertEqual(cloud["gh-linux-16x"], ["warp-ubuntu-2404-x64-16x"])
                self.assertEqual(cloud["ubuntu-24.04"], ["ubuntu-24.04"])
        # In the merge queue the shared linux_labels mapping takes WarpBuild, not an idle slot.
        idle = [slot(), slot(), runner("Windows", "X64"), runner()]
        result = run_route(self.workflow, org=idle, eventName="merge_group", warpbuildDisabled="")
        outputs, logs = run_heavy(self.workflow, result["outputs"], "merge_group")
        self.assertEqual(json.loads(outputs["linux_labels"]), ["warp-ubuntu-2404-x64-16x"])
        self.assertEqual(json.loads(outputs["check_labels"]), ["warp-ubuntu-2404-x64-8x"])
        self.assertEqual(outputs["linux_burst"], "true")
        self.assertIn("merge queue: the shared linux_labels mapping takes the burst runner", logs)
        # Idle native runners still win over WarpBuild.
        outputs = route_outputs(self.workflow, result["outputs"], "merge_group")
        self.assertEqual(json.loads(outputs["windows_labels"]), ["self-hosted", "Windows", "X64"])
        self.assertEqual(json.loads(outputs["macos_labels"]), ["self-hosted", "macOS", "ARM64"])
        # An empty or unreadable cloud map falls back to the same WarpBuild labels.
        for cloud in ("", "not json", "{}"):
            with self.subTest(cloud=cloud):
                outputs, _ = run_heavy(self.workflow, {**result["outputs"], "cloud": cloud}, "merge_group")
                self.assertEqual(json.loads(outputs["linux_labels"]), ["warp-ubuntu-2404-x64-16x"])
                self.assertEqual(json.loads(outputs["check_labels"]), ["warp-ubuntu-2404-x64-8x"])

    def test_release_linux_legs_take_the_burst_runner(self):
        for name, job, key in (("release-cache.yml", "daemon", "matrix.runner"),
                               ("release-sitter.yml", "build", "matrix.os"),
                               ("v-release.yml", "build-local-artifacts", "matrix.runner")):
            with self.subTest(workflow=name):
                runs_on = yaml.safe_load((ROOT / ".github/workflows" / name).read_text())["jobs"][job]["runs-on"]
                self.assertIn("(fromJSON(needs.runner-route.outputs.cloud || '{}')[%s] || %s)" % (key, key), runs_on)

    def test_pr_fits_heavy_rust_jobs_to_free_slots(self):
        same_repo = {"pull_request": {"head": {"repo": {"full_name": "intent-hq/intentd"}}}}
        busy = {"status": "in_progress", "labels": CACHE, "created_at": "2099-01-01T00:00:00Z"}
        # coverage-changed ranks above check, so a single free slot goes to it.
        for free, expected in ((2, ["check", "coverage-changed"]), (1, ["coverage-changed"]), (0, [])):
            with self.subTest(free=free):
                result = run_route(self.workflow, org=[slot(), slot()], eventName="pull_request",
                                   payload=same_repo, jobs=[busy] * (2 - free))
                slots = json.loads(result["outputs"]["slots"])
                self.assertEqual(sorted(k for k, v in slots.items() if v == BIG), expected)
        result = run_route(self.workflow, org=[slot(), slot()], eventName="pull_request",
                           payload=same_repo, linuxDisabled="true")
        # The kill switch sends every listed job to its hosted label.
        slots = json.loads(result["outputs"]["slots"])
        self.assertEqual(slots["check"], ["warp-ubuntu-2404-x64-16x"])
        self.assertNotIn(CACHE, slots.values())
        self.assertNotIn(SMALL, slots.values())

    def test_pr_check_takes_an_xl_slot_only_after_coverage_changed(self):
        # Two xl slots (intent-ci1 and intent-ci2) and two heavy slots online.
        # coverage-changed (15.5 min on xl, ~33 on heavy) always picks first;
        # check takes what is left: xl, then a bigmem heavy slot, then a 16x burst.
        same_repo = {"pull_request": {"head": {"repo": {"full_name": "intent-hq/intentd"}}}}
        fleet = [xl_slot(), xl_slot(), slot(), slot()]
        def busy(labels):
            return {"status": "in_progress", "labels": labels, "created_at": "2099-01-01T00:00:00Z"}
        for xl_free, heavy_free, coverage, check in (
                (2, 2, XL, XL),
                (1, 2, XL, BIG),
                (0, 2, BIG, BIG),
                # Both xl slots busy, one heavy slot free: coverage-changed takes
                # the bigmem slot and check bursts.
                (0, 1, BIG, ["warp-ubuntu-2404-x64-16x"]),
                (0, 0, ["warp-ubuntu-2404-x64-16x"], ["warp-ubuntu-2404-x64-16x"])):
            with self.subTest(xl_free=xl_free, heavy_free=heavy_free):
                jobs = [busy(XL)] * (2 - xl_free) + [busy(CACHE)] * (2 - heavy_free)
                result = run_route(self.workflow, org=fleet, eventName="pull_request", payload=same_repo,
                                   jobs=jobs, xlThreshold="2")
                slots = json.loads(result["outputs"]["slots"])
                self.assertEqual((slots["coverage-changed"], slots["check"]), (coverage, check))
        # In the merge queue check also takes a free slot (see the next test).
        result = run_route(self.workflow, org=fleet, eventName="merge_group", xlThreshold="2")
        self.assertEqual(json.loads(result["outputs"]["slots"])["check"], BIG)

    def test_merge_queue_check_takes_a_slot_after_the_coverage_suites(self):
        # The queue builds one entry at a time and coverage-all on xl (~20 min)
        # sets its pace. check takes a free xl or bigmem slot after coverage-all
        # and coverage-e2e (it slows down least on 8 CPUs), else bursts at 16x.
        # coverage-all takes only an xl slot (on an 8-CPU bigmem slot it took
        # p50 27 / p90 39 min and held up the queue), else bursts at 16x.
        # coverage-changed stays on WarpBuild in the queue.
        fleet = [xl_slot(), xl_slot(), slot(), slot()]
        warp16 = ["warp-ubuntu-2404-x64-16x"]
        def busy(labels):
            return {"status": "in_progress", "labels": labels, "created_at": "2099-01-01T00:00:00Z"}
        for xl_free, big_free, expected in (
                (2, 2, {"coverage-all": XL, "coverage-e2e": XL, "check": BIG, "daemon-lifecycle-linux": BIG}),
                (1, 2, {"coverage-all": XL, "coverage-e2e": BIG, "check": BIG, "daemon-lifecycle-linux": warp16}),
                (0, 2, {"coverage-all": warp16, "coverage-e2e": BIG, "check": BIG, "daemon-lifecycle-linux": warp16}),
                (2, 0, {"coverage-all": XL, "coverage-e2e": XL, "check": warp16, "daemon-lifecycle-linux": warp16}),
                (0, 0, {"coverage-all": warp16, "coverage-e2e": warp16, "check": warp16, "daemon-lifecycle-linux": warp16})):
            with self.subTest(xl_free=xl_free, big_free=big_free):
                jobs = [busy(XL)] * (2 - xl_free) + [busy(BIG)] * (2 - big_free)
                result = run_route(self.workflow, org=fleet, eventName="merge_group", jobs=jobs, xlThreshold="2")
                slots = json.loads(result["outputs"]["slots"])
                self.assertEqual({key: slots[key] for key in expected}, expected)
                self.assertEqual(slots["coverage-changed"], warp16)
        # A queue run with only 12 GB heavy slots free never puts check there.
        result = run_route(self.workflow, org=[small_heavy_slot(), small_heavy_slot()], eventName="merge_group")
        self.assertEqual(json.loads(result["outputs"]["slots"])["check"], warp16)
        # The kill switch sends it back to WarpBuild with every other listed job.
        result = run_route(self.workflow, org=fleet, eventName="merge_group", xlThreshold="2", linuxDisabled="true")
        self.assertEqual(json.loads(result["outputs"]["slots"])["check"], warp16)

    def test_16_core_jobs_never_overflow_to_small_heavy_slots(self):
        # intent-ci3's 12 GB heavy slots OOM-killed check and coverage-changed
        # (one rustc passed 12 GB) when both xl slots were busy.
        same_repo = {"pull_request": {"head": {"repo": {"full_name": "intent-hq/intentd"}}}}
        busy_xl = {"status": "in_progress", "labels": XL, "created_at": "2099-01-01T00:00:00Z"}
        fleet = [xl_slot(), xl_slot()] + [small_heavy_slot() for _ in range(4)]
        for event in ("pull_request", "merge_group"):
            with self.subTest(event=event):
                result = run_route(self.workflow, org=fleet, eventName=event, payload=same_repo if event == "pull_request" else {},
                                   jobs=[busy_xl] * 2, xlThreshold="2")
                slots = json.loads(result["outputs"]["slots"])
                for key in ("check", "coverage-changed", "coverage-all", "coverage-e2e", "daemon-lifecycle-linux"):
                    self.assertEqual(slots[key], ["warp-ubuntu-2404-x64-16x"], key)
                self.assertEqual(json.loads(result["outputs"]["linux_labels"]), ["warp-ubuntu-2404-x64-16x"])
                # 8-core work still uses the small heavy slots.
                self.assertEqual(json.loads(result["outputs"]["check_labels"]), CACHE)
        # With CI_LINUX_BIGMEM_SLOTS unset, 16-core overflow bursts even with bigmem slots online.
        result = run_route(self.workflow, org=[xl_slot(), slot(), slot()], eventName="pull_request", payload=same_repo,
                           jobs=[busy_xl], xlThreshold="1", bigmemThreshold="")
        slots = json.loads(result["outputs"]["slots"])
        self.assertEqual((slots["coverage-changed"], slots["check"]), (["warp-ubuntu-2404-x64-16x"],) * 2)
        step = next(s for s in self.workflow["jobs"]["route"]["steps"] if s.get("id") == "decide")
        self.assertEqual(step["env"].get("BIGMEM_THRESHOLD"), "${{ env.LINUX_BIGMEM_BURST_THRESHOLD }}")
        self.assertEqual(self.workflow["env"]["LINUX_BIGMEM_BURST_THRESHOLD"], "${{ vars.CI_LINUX_BIGMEM_SLOTS || '0' }}")

    def test_windows_bursts_without_explicit_idle_capacity(self):
        missing_busy = runner("Windows", "X64")
        missing_busy.pop("busy")
        for fleet in ([runner("Windows", "X64", busy=True)], [missing_busy],
                      [runner("Windows", "X64", busy=None)],
                      [runner("Windows", "X64", busy="false")],
                      [runner("Windows", "X64", busy=0)],
                      [runner("Windows", "X64", status="offline")]):
            with self.subTest(fleet=fleet):
                outputs = run_route(self.workflow, org=fleet)["outputs"]
                self.assertEqual(json.loads(outputs["windows_labels"]), ["warp-windows-2022-x64-16x"])
                self.assertEqual(outputs["windows_burst"], "true")
        outputs = run_route(self.workflow, org=[runner("Windows", "X64", busy=True),
                                               runner("Windows", "X64")])["outputs"]
        self.assertEqual(json.loads(outputs["windows_labels"]), ["self-hosted", "Windows", "X64"])

    def test_windows_maintenance_override_preserves_other_runner_preferences(self):
        step = next(s for s in self.workflow["jobs"]["route"]["steps"] if s.get("id") == "decide")
        self.assertEqual(step["env"].get("WINDOWS_RUNNER_DISABLED"), "${{ vars.CI_WINDOWS_RUNNER_DISABLED }}")
        fleet = [slot(), runner("Windows", "X64"), runner()]
        for disabled in ("", "false", "true"):
            with self.subTest(disabled=disabled):
                outputs = run_route(self.workflow, org=fleet, windowsDisabled=disabled)["outputs"]
                self.assertEqual(json.loads(outputs["windows_labels"]),
                                 ["warp-windows-2022-x64-16x"] if disabled == "true" else ["self-hosted", "Windows", "X64"])
                self.assertEqual(outputs["windows_burst"], str(disabled == "true").lower())
                self.assertEqual(json.loads(outputs["linux_labels"]), BIG)
                self.assertEqual(json.loads(outputs["macos_labels"]), ["self-hosted", "macOS", "ARM64"])

    def test_linux_maintenance_override_preserves_native_runner_preferences(self):
        fleet = [slot(), runner("Windows", "X64"), runner()]
        for disabled in ("", "false", "true"):
            with self.subTest(disabled=disabled):
                outputs = run_route(self.workflow, org=fleet, linuxDisabled=disabled)["outputs"]
                self.assertEqual(json.loads(outputs["linux_labels"]),
                                 ["warp-ubuntu-2404-x64-16x"] if disabled == "true" else BIG)
                self.assertEqual(json.loads(outputs["check_labels"]),
                                 ["warp-ubuntu-2404-x64-8x"] if disabled == "true" else CACHE)
                self.assertEqual(outputs["linux_burst"], str(disabled == "true").lower())
                self.assertEqual(json.loads(outputs["macos_labels"]), ["self-hosted", "macOS", "ARM64"])
                self.assertEqual(json.loads(outputs["windows_labels"]), ["self-hosted", "Windows", "X64"])
        step = next(s for s in self.workflow["jobs"]["route"]["steps"] if s.get("id") == "decide")
        self.assertEqual(step["env"].get("LINUX_RUNNER_DISABLED"), "${{ vars.CI_LINUX_RUNNER_DISABLED }}")

    def test_macos_maintenance_override_preserves_other_runner_preferences(self):
        fleet = [slot(), runner("Windows", "X64"), runner()]
        for disabled in ("", "false", "true"):
            with self.subTest(disabled=disabled):
                outputs = run_route(self.workflow, org=fleet, macosDisabled=disabled)["outputs"]
                self.assertEqual(json.loads(outputs["macos_labels"]),
                                 ["warp-macos-26-arm64-6x"] if disabled == "true" else ["self-hosted", "macOS", "ARM64"])
                self.assertEqual(outputs["macos_burst"], str(disabled == "true").lower())
                self.assertEqual(json.loads(outputs["linux_labels"]), BIG)
                self.assertEqual(json.loads(outputs["windows_labels"]), ["self-hosted", "Windows", "X64"])
        step = next(s for s in self.workflow["jobs"]["route"]["steps"] if s.get("id") == "decide")
        self.assertEqual(step["env"].get("MACOS_RUNNER_DISABLED"), "${{ vars.CI_MACOS_RUNNER_DISABLED }}")

    def test_empty_outputs_use_hosted_macos_in_both_matrices(self):
        for job_id in ("build", "windows", "daemon-lifecycle", "macos-services-runtime"):
            for row in self.workflow["jobs"][job_id]["strategy"]["matrix"]["include"]:
                expected = ["warp-macos-26-arm64-6x"] if row.get("target") == "aarch64-apple-darwin" or row.get("platform") == "macos" else (
                    ["warp-ubuntu-2404-x64-16x"] if row.get("platform") == "linux" else ["warp-windows-2022-x64-16x"])
                for outputs in ({}, {"macos_labels": "", "windows_labels": "", "linux_labels": ""}):
                    with self.subTest(job=job_id, row=row, outputs=outputs):
                        self.assertEqual(matrix_labels(row["os"], outputs), expected)
        mac = self.workflow["jobs"]["build"]["strategy"]["matrix"]["include"][0]
        self.assertEqual(mac["name"], "build (aarch64-apple-darwin)")
        self.assertEqual(self.workflow["jobs"]["daemon-lifecycle"]["name"], "Daemon lifecycle (${{ matrix.platform }})")

    def test_hosted_macos_skips_persistent_cleanup_and_uses_cache(self):
        steps = {s.get("name"): s for s in self.workflow["jobs"]["build"]["steps"]}
        for labels in (["warp-macos-26-arm64-6x"], ["self-hosted", "macOS", "ARM64"]):
            for fast_path in ("", "true"):
                values = {**context("merge_group", fast_path=fast_path), "matrix.os": labels}
                for name in ("Ensure macOS CI disk headroom", "Point CARGO_TARGET_DIR outside the work dir (self-hosted)"):
                    self.assertEqual(condition(steps[name]["if"], values), fast_path != "true" and "self-hosted" in labels)
                self.assertIn("Configure the self-hosted compiler cache", steps)


class LifecycleCacheTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.job = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]["daemon-lifecycle"]
        cls.steps = cls.job["steps"]

    def step(self, name):
        matches = [s for s in self.steps if s.get("name") == name]
        self.assertEqual(len(matches), 1, name)
        return matches[0]

    def test_windows_shell_is_ready_before_cache_setup(self):
        jobs = yaml.safe_load((ROOT / '.github/workflows/ci.yml').read_text())['jobs']
        for job in ('windows', 'windows-lint'):
            with self.subTest(job=job):
                steps = jobs[job]['steps']
                setup = next(i for i, step in enumerate(steps) if step.get('name') == 'Configure the self-hosted compiler cache')
                git_bash = next(i for i, step in enumerate(steps) if step.get('name') == 'Put Git Bash first on PATH (Windows)')
                self.assertLess(git_bash, setup)
                self.assertNotIn('if', steps[git_bash])
                self.assertTrue(all(not step.get('run') for step in steps[:git_bash]))
        for name in ('build', 'daemon-lifecycle'):
            self.assertFalse(any(step.get('name') == 'Put Git Bash first on PATH (Windows)' for step in jobs[name]['steps']), name)

    def test_pi_windows_uses_git_bash_before_any_shell_steps(self):
        job = yaml.safe_load((ROOT / '.github/workflows/ci.yml').read_text())['jobs']['pi-session-paths']
        steps = job['steps']
        setup = next((s for s in steps if s.get('name') == 'Put Git Bash first on PATH (Windows)'), None)
        self.assertIsNotNone(setup)
        self.assertEqual(setup['shell'], 'cmd')
        self.assertIn('Git\\bin', setup['run'])
        self.assertIn('GITHUB_PATH', setup['run'])
        self.assertTrue(condition(setup['if'], context('push', platform='Windows')))
        self.assertFalse(condition(setup['if'], context('push', platform='Linux')))
        for step in steps[:steps.index(setup)]:
            self.assertNotIn('run', step)

    def test_installer_bootstraps_shell_and_nuget_without_prompting(self):
        job = yaml.safe_load((ROOT / '.github/workflows/ci.yml').read_text())['jobs']['install-ps1']
        steps = job['steps']
        setup = next((s for s in steps if s.get('name') == 'Prepare PowerShell 7'), None)
        self.assertIsNotNone(setup)
        self.assertEqual(setup['shell'], 'powershell')
        self.assertIn('GITHUB_PATH', setup['run'])
        self.assertIn('Get-FileHash', setup['run'])
        pester = next(s for s in steps if s.get('name', '').startswith('Pester ('))
        self.assertLess(steps.index(setup), steps.index(pester))
        self.assertIn('Install-PackageProvider -Name NuGet', pester['run'])
        self.assertLess(pester['run'].index('Install-PackageProvider'), pester['run'].index('Install-Module'))
        provider = next(line for line in pester['run'].splitlines() if 'Install-PackageProvider' in line)
        self.assertIn('-Force', provider)
        self.assertIn('-Scope CurrentUser', provider)

    def test_managed_cache_covers_all_routed_runners(self):
        cache = self.step("Configure the self-hosted compiler cache")
        self.assertEqual(cache["run"], "python3 scripts/setup-sccache.py")
        self.assertNotIn("if", cache)

    def test_persistent_paths_are_platform_scoped_and_precede_compilation(self):
        names = {"Point lifecycle target outside checkout (macOS)": {"macOS"},
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

    def test_linux_jobs_assume_a_fresh_runner(self):
        # intent-cache slots are fresh containers: no shared /raid volume, no
        # per-slot target dirs or markers, and nothing tinybox-specific.
        text = (ROOT / ".github/workflows/ci.yml").read_text()
        for forbidden in ("/raid", "ci-target-slot.sh", "INSTALL_ACTION_HOME", "ci-target/intentd/$RUNNER_NAME"):
            self.assertNotIn(forbidden, text)
        for path in [*(ROOT / ".github").rglob("*.yml"), *(ROOT / ".github").rglob("*.yaml")]:
            self.assertNotRegex(path.read_text(), r"(?i)tinybox", str(path))
        self.assertFalse((ROOT / "scripts/ci-target-slot.sh").exists())

    def test_macos_and_windows_keep_native_persistent_paths(self):
        step = self.step("Point lifecycle target outside checkout (macOS)")
        self.assertEqual(step["shell"], "bash")
        jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
        # windows-lint shares the push job's dir so the single-slot runner stays warm.
        targets = {job: next(s for s in jobs[job]["steps"] if s.get("name") == "Point CARGO_TARGET_DIR outside the work dir (self-hosted)")
                   for job in ("windows", "windows-lint")}
        for platform, body, profile, expected in (
                ("macos", step["run"].replace("${{ matrix.platform }}", "macos"), "",
                 os.environ["HOME"] + "/ci-target/intentd/daemon-lifecycle-macos"),
                *((job, target["run"].replace("${{ matrix.target }}", WINDOWS_TARGET), "C:\\Users\\CI User",
                   "C:\\Users\\CI User/ci-target/intentd/build-" + WINDOWS_TARGET) for job, target in targets.items())):
            with self.subTest(platform=platform), tempfile.TemporaryDirectory() as tmp:
                envfile = Path(tmp) / "env"
                result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", body], cwd=ROOT,
                                        env={**os.environ, "USERPROFILE": profile, "GITHUB_ENV": str(envfile)},
                                        capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(envfile.read_text().strip(), "CARGO_TARGET_DIR=" + expected)

class NativeRoutingTests(unittest.TestCase):
    def test_consumer_workflow_allows_nested_runner_probe_reads(self):
        caller = self.workflow["jobs"]["monorepo-consumer-checks"]
        self.assertEqual(caller["permissions"].get("contents"), "read")
        self.assertEqual(caller["permissions"].get("actions"), "read")

    def test_consumer_caller_tracks_monorepo_main(self):
        # A SHA pin also pins the protocol docs the checks read, so docs merged
        # first (the documented fix order) stay invisible until a hand bump
        # (intentd#2476, #2481). @main resolves the monorepo tip per run.
        caller = self.workflow["jobs"]["monorepo-consumer-checks"]
        self.assertEqual(caller["uses"], "intent-hq/intent/.github/workflows/consumer-checks.yml@main")

    def test_consumer_caller_passes_the_self_hosted_cache_token(self):
        caller = self.workflow["jobs"]["monorepo-consumer-checks"]
        self.assertEqual(caller["secrets"], {
            "CI_QUEUE_READ_TOKEN": "${{ secrets.CI_QUEUE_READ_TOKEN }}",
            "INTENT_SCCACHE_TOKEN": "${{ secrets.INTENT_SCCACHE_TOKEN }}",
        })

    @classmethod
    def setUpClass(cls):
        cls.workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())

    def test_every_queue_job_failure_already_fails_the_gate(self):
        self.assertEqual(queue_fail_fast_errors(self.workflow), [])
        workflow = copy.deepcopy(self.workflow)
        body = next(st for st in workflow["jobs"]["gate"]["steps"] if st.get("name") == "Check results")
        # A gate that stops reading a queue job would accept its failure.
        merge_group = body["run"].index("merge_group)")
        line = 'require deb-packaging "${{ needs.deb-packaging.result }}"\n'
        at = body["run"].index(line, merge_group)
        body["run"] = body["run"][:at] + body["run"][at + len(line):]
        self.assertTrue(any(e.startswith("deb-packaging:") for e in queue_fail_fast_errors(workflow)))

    def test_job_selection_and_route_failures(self):
        self.assertEqual(routing_errors(self.workflow), [])

    def test_concurrency_never_cancels_or_serializes_main_pushes(self):
        self.assertEqual(concurrency_errors(self.workflow), [])

    def test_concurrency_mutations_are_caught(self):
        cases = {
            "shared-cancelling-group": lambda c: c.update({
                "group": "${{ github.workflow }}-${{ github.ref }}",
                "cancel-in-progress": "${{ github.event_name == 'pull_request' || github.event_name == 'push' }}"}),
            "shared-queueing-group": lambda c: c.__setitem__("group", "${{ github.workflow }}-${{ github.ref }}"),
            "per-commit-cancelling": lambda c: c.__setitem__("cancel-in-progress", "${{ github.event_name != 'merge_group' }}"),
            "queue-cancelled": lambda c: c.__setitem__("cancel-in-progress", True),
        }
        for label, mutate in cases.items():
            with self.subTest(mutation=label):
                workflow = copy.deepcopy(self.workflow)
                mutate(workflow["concurrency"])
                self.assertTrue(concurrency_errors(workflow))
        workflow = copy.deepcopy(self.workflow)
        workflow["jobs"]["windows"]["concurrency"] = {"group": "windows", "cancel-in-progress": True}
        self.assertIn("windows: job-level concurrency could cancel the tip's run", concurrency_errors(workflow))

    def test_supersede_job_contract(self):
        self.assertEqual(supersede_job_errors(self.workflow), [])
        cases = {
            "pr": lambda j: j.__setitem__("if", "github.event_name != 'merge_group'"),
            "any-branch": lambda j: j.__setitem__("if", "github.event_name == 'push'"),
            "routed": lambda j: j.__setitem__("needs", ["route"]),
            "broad-token": lambda j: j["permissions"].__setitem__("contents", "write"),
            "optional": lambda j: next(s for s in j["steps"] if s.get("id") == "scan").__setitem__("continue-on-error", True),
        }
        for label, mutate in cases.items():
            with self.subTest(mutation=label):
                workflow = copy.deepcopy(self.workflow)
                mutate(workflow["jobs"]["supersede"])
                self.assertTrue(supersede_job_errors(workflow))

    def test_release_push_skips_macos_release_build(self):
        # The push macOS release build is the only mandatory per-tip macOS
        # workspace release build (release-profile code such as
        # cfg(not(debug_assertions)) is never compiled in the queue). auto-cut
        # cuts only after the tip's push gate, macOS release build included,
        # is green, and the release-metadata push that follows changes only
        # versions on that tree, so it skips the paid rebuild (the tag's
        # release workflow builds every target anyway).
        self.assertEqual(scheduled(self.workflow, "push", release_push="true")["build"], "skipped")
        self.assertEqual(scheduled(self.workflow, "push", release_push="false")["build"], "success")
        steps = {step.get("name"): step for step in self.workflow["jobs"]["build"]["steps"]}
        self.assertTrue(condition(steps["Build (release)"]["if"], context("push")))

    def test_push_warms_the_check_cache_only_on_self_hosted_runners(self):
        # An ephemeral VM discards its target dir, so the warm check would
        # only spend paid macOS minutes there.
        step = next(s for s in self.workflow["jobs"]["build"]["steps"] if s.get("name") == "Warm merge queue check cache")
        self.assertIn("contains(matrix.os, 'self-hosted')", step["if"])
        self.assertNotIn("merge_group", step["if"])

    def test_release_push_classifier_compares_against_the_previous_tip(self):
        job = self.workflow["jobs"]["release-fast-path"]
        self.assertTrue(condition(job["if"], context("push")))
        decide = next(step for step in job["steps"] if step.get("id") == "decide")
        # A push classifies only a single commit on the previous tip, so a
        # force push or multi-commit push cannot borrow a merge-base diff.
        fetch = next(step for step in job["steps"] if "git fetch" in str(step.get("run", "")))
        self.assertIn("--exact-parent", decide["run"])
        self.assertIn("--depth=2", fetch["run"])
        for event, base in (("push", "github.event.before"), ("pull_request", "github.event.pull_request.base.sha")):
            values = {"github.event_name": event, "github.event.before": "before",
                      "github.event.pull_request.base.sha": "pr-base"}
            self.assertEqual(template_value(decide["env"]["BASE_SHA"], values), values[base], event)
        # Each event reads only its own output, so a release push can never
        # trip the PR fast-path guards on the `build` job's steps.
        for event, own, other in (("push", "release_push", "fast_path"), ("pull_request", "fast_path", "release_push")):
            values = {"github.event_name": event, "steps.decide.outputs.fast_path": "true"}
            self.assertEqual(template_value(job["outputs"][own], values), "true", own)
            self.assertEqual(template_value(job["outputs"][other], values), "", other)

    def test_gate_result_truth_tables(self):
        self.assertEqual(gate_errors(self.workflow), [])

    def test_native_proofs_and_push_release_builds(self):
        self.assertEqual(native_errors(self.workflow), [])

    def test_windows_lints_in_queue_and_runs_runtime_proofs_after_merge(self):
        self.assertEqual(windows_errors(self.workflow), [])
        self.assertEqual(windows_legs(self.workflow, "pull_request"), set())
        self.assertEqual(windows_legs(self.workflow, "merge_group"), {"windows-lint"})
        self.assertEqual(windows_legs(self.workflow, "push"), {"windows", "install-ps1", "pi-session-paths"})

    def test_mutations_cannot_move_windows_jobs_between_events(self):
        cases = {
            "lint-pr": lambda w: w["jobs"]["windows-lint"].__setitem__("if", "github.event_name != 'push'"),
            "lint-push": lambda w: w["jobs"]["windows-lint"].__setitem__("if", "github.event_name != 'pull_request'"),
            "lint-dropped": lambda w: w["jobs"]["windows-lint"].__setitem__("if", "github.event_name == 'push'"),
            "lint-duplicated": lambda w: w["jobs"]["windows"]["steps"].insert(
                0, copy.deepcopy(next(s for s in w["jobs"]["windows-lint"]["steps"] if s.get("name") == "Clippy (target-gated code)"))),
            "lint-renamed": lambda w: w["jobs"]["windows"]["steps"].insert(
                0, {"name": "Clippy (Windows)", "run": "cargo clippy --workspace --all-targets -- -D warnings"}),
            "check-renamed": lambda w: w["jobs"]["windows"]["steps"].insert(
                0, {"name": "Typecheck", "run": "rustup run stable cargo check --workspace"}),
            "job-queue": lambda w: w["jobs"]["windows"].__setitem__("if", "github.event_name == 'merge_group' || github.event_name == 'push'"),
            "job-pr": lambda w: w["jobs"]["windows"].__setitem__("if", "github.event_name != 'merge_group'"),
            "installer-queue": lambda w: w["jobs"]["install-ps1"].__setitem__("if", "github.event_name == 'merge_group' || github.event_name == 'push'"),
            "pi-static": lambda w: w["jobs"]["pi-session-paths"]["strategy"]["matrix"].__setitem__("os", ["ubuntu-24.04", "windows-2025"]),
            "build-row": lambda w: w["jobs"]["build"]["strategy"]["matrix"]["include"].append(
                {"target": WINDOWS_TARGET, "os": "${{ fromJSON(needs.route.outputs.windows_labels || '[\"gh-windows-16x\"]') }}"}),
            "lifecycle-row": lambda w: w["jobs"]["daemon-lifecycle"]["strategy"]["matrix"]["include"].append(
                {"platform": "windows", "os": "${{ fromJSON(needs.route.outputs.windows_labels || '[\"gh-windows-16x\"]') }}"}),
        }
        for label, mutate in cases.items():
            with self.subTest(mutation=label):
                workflow = copy.deepcopy(self.workflow)
                mutate(workflow)
                self.assertTrue(windows_errors(workflow) + native_errors(workflow))

    def test_mutations_cannot_drop_windows_proofs(self):
        proofs = [("windows-lint", name) for name in WINDOWS_LINT_STEPS] + [("windows", name) for name in WINDOWS_STEPS]
        for job, name in proofs:
            for mutation in ("remove", "skip", "queue-only", "optional", "empty"):
                with self.subTest(job=job, step=name, mutation=mutation):
                    workflow = copy.deepcopy(self.workflow)
                    steps = workflow["jobs"][job]["steps"]
                    step = next(s for s in steps if s.get("name") == name)
                    if mutation == "remove":
                        steps.remove(step)
                    elif mutation == "skip":
                        step["if"] = False
                    elif mutation == "queue-only":
                        step["if"] = "github.event_name == 'merge_group'"
                    elif mutation == "optional":
                        step["continue-on-error"] = True
                    else:
                        step["run"] = ""
                    self.assertTrue(any(name in error for error in windows_errors(workflow)))
        for job in ("windows", "windows-lint"):
            workflow = copy.deepcopy(self.workflow)
            workflow["jobs"][job]["continue-on-error"] = True
            self.assertIn(f"{job}: job must stay required", windows_errors(workflow))

    def test_gates_require_windows(self):
        for event, job in [("push", job) for job in WINDOWS_JOBS] + [("merge_group", job) for job in WINDOWS_QUEUE_JOBS]:
            with self.subTest(event=event, job=job):
                workflow = copy.deepcopy(self.workflow)
                step = next(s for s in workflow["jobs"]["gate"]["steps"] if s.get("name") == "Check results")
                step["run"] = step["run"].replace("${{ needs.%s.result }}" % job, "success")
                self.assertIn(f"{event}: {job}=skipped", gate_errors(workflow))
                workflow["jobs"]["gate"]["needs"].remove(job)
                self.assertIn("gate dependencies changed", gate_errors(workflow))

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
                    removed = {"feature": "--features repository-test-fixtures", "target": " e2e_repository_resource_read::", "threads": "--test-threads=2"}[mutation]
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
        self.assertFalse(condition(self.workflow["jobs"]["windows"]["if"], context("push"), cancelled=True))

    def test_mutations_cannot_disable_native_queue_steps(self):
        for name in NATIVE_STEPS:
            for mutation in ("remove", "skip", "pr-only", "optional", "empty"):
                with self.subTest(step=name, mutation=mutation):
                    workflow = copy.deepcopy(self.workflow)
                    steps = workflow["jobs"][native_step_job(name)]["steps"]
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

    def test_services_runtime_has_independent_required_budget(self):
        job = self.workflow["jobs"]["macos-services-runtime"]
        self.assertEqual(job["timeout-minutes"], 60)
        self.assertNotIn("build", job["needs"])
        self.assertFalse(job.get("continue-on-error", False))
        self.assertEqual(job["strategy"]["matrix"]["include"][0]["target"], "aarch64-apple-darwin")
        for name in MACOS_SERVICE_STEPS:
            self.assertFalse(any(s.get("name") == name for s in self.workflow["jobs"]["build"]["steps"]))
        directory = next(s["run"] for s in job["steps"] if s.get("name", "").startswith("Point CARGO_TARGET_DIR"))
        self.assertIn("/macos-services-runtime-", directory)
        for mutation in ("skip", "ignore", "dependency"):
            workflow = copy.deepcopy(self.workflow)
            if mutation == "skip":
                workflow["jobs"]["macos-services-runtime"]["if"] = False
                self.assertTrue(routing_errors(workflow))
            elif mutation == "ignore":
                step = next(s for s in workflow["jobs"]["gate"]["steps"] if s.get("name") == "Check results")
                step["run"] = step["run"].replace("${{ needs.macos-services-runtime.result }}", "success")
                self.assertIn("merge_group: macos-services-runtime=skipped", gate_errors(workflow))
                self.assertIn("merge_group: macos-services-runtime=failure", gate_errors(workflow))
            else:
                workflow["jobs"]["gate"]["needs"].remove("macos-services-runtime")
                self.assertIn("gate dependencies changed", gate_errors(workflow))

    def test_mutation_cannot_accept_skipped_queue_build(self):
        workflow = copy.deepcopy(self.workflow)
        step = next(s for s in workflow["jobs"]["gate"]["steps"] if s.get("name") == "Check results")
        step["run"] = step["run"].replace('${{ needs.build.result }}', "success")
        self.assertIn("merge_group: build=skipped", gate_errors(workflow))
        self.assertIn("push: build=skipped", gate_errors(workflow))

    def test_release_fast_path_skips_only_pr_slow_jobs(self):
        for fast_path in ("", "false", "true"):
            for event in REQUIRED:
                jobs = scheduled(self.workflow, event, fast_path=fast_path if event == "pull_request" else "", head_ref=RELEASE_BRANCH)
                for name in FAST_PATH_SKIPS:
                    pr_only = event == "merge_group" and name in WINDOWS_PR_JOBS
                    expected = "skipped" if (event == "pull_request" and fast_path == "true") or event == "push" or pr_only else "success"
                    self.assertEqual(jobs[name], expected, f"{event}/{fast_path}: {name}")
        # A failed or skipped route still skips the slow jobs, and CI Gate
        # rejects that skip on a PR without a reported fast path.
        for route in ("failure", "skipped"):
            jobs = scheduled(self.workflow, "pull_request", route=route)
            self.assertTrue(all(jobs[name] == "skipped" for name in FAST_PATH_SKIPS))
            self.assertFalse(gate_passes(self.workflow, "pull_request", jobs))

    def test_classifier_runs_only_on_release_plz_branches(self):
        # Ordinary PRs skip the classifier at once, so the slow jobs that
        # need it start right after route, and a failed classifier on a
        # release branch still leaves the gate passing with full CI.
        jobs = scheduled(self.workflow, "pull_request", head_ref="feature/release-plz-like")
        self.assertEqual(jobs["release-fast-path"], "skipped")
        self.assertTrue(all(jobs[name] == "success" for name in FAST_PATH_SKIPS))
        self.assertTrue(gate_passes(self.workflow, "pull_request", jobs))
        jobs = scheduled(self.workflow, "pull_request", head_ref=RELEASE_BRANCH)
        self.assertTrue(gate_passes(self.workflow, "pull_request", {**jobs, "release-fast-path": "failure"}))
        self.assertFalse(gate_passes(self.workflow, "pull_request",
                                     {**jobs, "release-fast-path": "failure", "check": "skipped"}, "true"))

    def test_mutations_cannot_widen_the_release_fast_path(self):
        mutations = {
            "unconditional": ('[ "${{ needs.release-fast-path.result }}" = success ] && [ "${{ needs.release-fast-path.outputs.fast_path }}" = true ]', "true"),
            "ignore-classifier-result": ('[ "${{ needs.release-fast-path.result }}" = success ] && ', ""),
            "ignore-output": (' && [ "${{ needs.release-fast-path.outputs.fast_path }}" = true ]', ""),
            "optional-check": ('"$slow" check', "optional check"),
            "optional-coverage": ('"$slow" coverage-changed', "optional coverage-changed"),
            "optional-windows-cross-lint": ('"$slow" windows-cross-lint', "optional windows-cross-lint"),
        }
        for label, (old, new) in mutations.items():
            with self.subTest(mutation=label):
                workflow = copy.deepcopy(self.workflow)
                step = next(s for s in workflow["jobs"]["gate"]["steps"] if s.get("name") == "Check results")
                self.assertIn(old, step["run"])
                step["run"] = step["run"].replace(old, new, 1)
                self.assertTrue(gate_errors(workflow))
        for job in FAST_PATH_SKIPS:
            widen = [" && needs.release-fast-path.outputs.fast_path != 'true'"]
            if job in REQUIRED["merge_group"]:
                widen.append("github.event_name == 'merge_group' || ")
            for clause in widen:
                with self.subTest(job=job, clause=clause):
                    workflow = copy.deepcopy(self.workflow)
                    self.assertIn(clause, workflow["jobs"][job]["if"])
                    workflow["jobs"][job]["if"] = workflow["jobs"][job]["if"].replace(clause, "")
                    self.assertTrue(routing_errors(workflow))

    def test_windows_cross_lint_runs_on_prs_only_on_a_cloud_linux_runner(self):
        # The PR-time pre-filter for the queue's native windows-lint: Clippy
        # for the Windows GNU target, denying warnings, on a Linux runner
        # that can apt-install MinGW (route's `cloud` map never returns a
        # self-hosted slot). The queue and main pushes skip it.
        job = self.workflow["jobs"]["windows-cross-lint"]
        commands = [str(step.get("run", "")) for step in job["steps"]]
        # The native job's production check, then Clippy, in that order.
        self.assertEqual([run for run in commands if "cargo " in run], [
            "cargo check --workspace --target x86_64-pc-windows-gnu",
            "cargo clippy --workspace --all-targets --target x86_64-pc-windows-gnu -- -D warnings"])
        self.assertTrue(any("gcc-mingw-w64-x86-64" in run for run in commands))
        toolchain = next(step for step in job["steps"] if str(step.get("uses", "")).startswith("dtolnay/rust-toolchain@"))
        self.assertEqual(toolchain["with"]["targets"], "x86_64-pc-windows-gnu")
        self.assertIn("clippy", toolchain["with"]["components"])
        self.assertIn("needs.route.outputs.cloud", job["runs-on"])
        self.assertIn("['gh-linux-8x']", job["runs-on"])
        self.assertNotIn("windows", job["runs-on"].lower())
        self.assertNotIn("strategy", job)
        self.assertFalse(job.get("continue-on-error"))
        for event in REQUIRED:
            expected = "success" if event == "pull_request" else "skipped"
            self.assertEqual(scheduled(self.workflow, event)["windows-cross-lint"], expected, event)
        self.assertEqual(scheduled(self.workflow, "pull_request", fork=True)["windows-cross-lint"], "skipped")

    def test_mutation_cannot_drop_gate_dependency(self):
        workflow = copy.deepcopy(self.workflow)
        workflow["jobs"]["gate"]["needs"].remove("build")
        self.assertIn("gate dependencies changed", gate_errors(workflow))

    def test_auxiliary_native_workflows_cannot_run_on_prs(self):
        for name in ("windows-setup.yml", "windows-prune.yml", "cache-smoke.yml"):
            doc = yaml.safe_load((ROOT / ".github/workflows" / name).read_text())
            self.assertNotIn("pull_request", doc.get("on", doc.get(True)), name)
        cache = yaml.safe_load((ROOT / ".github/workflows/cache-smoke.yml").read_text())
        self.assertIn("workflow_call", cache.get("on", cache.get(True)))
        self.assertEqual(self.workflow["jobs"]["cache-smoke"]["uses"], "./.github/workflows/cache-smoke.yml")
        release = yaml.safe_load((ROOT / ".github/workflows/v-release.yml").read_text())
        self.assertIn("github.event_name != 'pull_request'", release["jobs"]["build-local-artifacts"]["if"])

    def test_pr_static_does_not_compile_or_install_application_dependencies(self):
        job = self.workflow["jobs"]["pr-static"]
        self.assertIn("ubuntu-24.04", job["runs-on"])
        commands = "\n".join(step.get("run", "") for step in job["steps"])
        self.assertIn("cargo fmt --all -- --check", commands)
        self.assertIn("python3 -I -B scripts/test-ci-native-routing.py", commands)
        self.assertNotRegex(commands, r"cargo (build|check|clippy|test|nextest|llvm-cov)|pnpm install|npm install")

    def changed_test_base_step(self):
        return next(s for s in self.workflow["jobs"]["coverage-changed"]["steps"]
                    if s.get("name") == "Resolve changed-test base from the event")

    def test_jobs_that_execute_node_install_it_first(self):
        self.assertEqual(node_without_setup(self.workflow), [])

        def without_setup(job_id, *, conditional=None):
            workflow = copy.deepcopy(self.workflow)
            steps = workflow["jobs"][job_id]["steps"]
            for step in steps:
                if "actions/setup-node@" in step.get("uses", ""):
                    if conditional is None:
                        steps.remove(step)
                    else:
                        step["if"] = conditional
                    break
            return [error for error in node_without_setup(workflow) if error.startswith(job_id + ": ")]

        self.assertTrue(without_setup("pr-static"))
        self.assertTrue(without_setup("release-scripts"))
        self.assertTrue(without_setup("pr-static", conditional="runner.os == 'Windows'"))
        # Lookup via shutil.which in a directly executed script, not comment text.
        for job_id in ("coverage-all", "coverage-changed"):
            workflow = copy.deepcopy(self.workflow)
            for step in workflow["jobs"][job_id]["steps"]:
                if step.get("run"):
                    step["run"] = without_comments(step["run"])
            workflow["jobs"][job_id]["steps"] = [step for step in workflow["jobs"][job_id]["steps"]
                if "actions/setup-node@" not in step.get("uses", "")]
            self.assertTrue([e for e in node_without_setup(workflow) if e.startswith(job_id + ": ")], job_id)

        def flagged(command):
            return node_without_setup({"jobs": {"probe": {"steps": [{"run": command}]}}})

        for command in ("node -e 1", "npm ci", "FOO=1 npx tsc", "make && pnpm test", "out=$(node -v)",
                        "scripts/test-ci-native-routing.py", "python3 -I -B scripts/test-ci-native-routing.py"):
            self.assertTrue(flagged(command), command)
        for command in ("# npm is not needed here", "for node in a b; do echo $node; done",
                        "node=1", "echo checking node version", "git diff -- scripts/test-ci-native-routing.py"):
            self.assertFalse(flagged(command), command)

    def test_queue_coverage_base_is_bound_to_the_actual_checkout(self):
        step = self.changed_test_base_step()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stub = root / "git"
            stub.write_text("#!/bin/sh\ncase \"$1\" in rev-parse) echo \"$TEST_HEAD\" ;; fetch) exit \"$FETCH_STATUS\" ;; *) exit 99 ;; esac\n")
            stub.chmod(0o755)
            for base, head, actual, fetch_status, accepted in (
                ("a"*40, "b"*40, "b"*40, "0", True),
                ("", "b"*40, "b"*40, "0", False),
                ("a"*40, "b"*40, "c"*40, "0", False),
                ("a"*40, "b"*40, "b"*40, "19", False),
            ):
                output = root / "env"
                output.unlink(missing_ok=True)
                result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", step["run"]],
                    env={**os.environ, "PATH": tmp + os.pathsep + os.environ["PATH"],
                         "GITHUB_EVENT_NAME": "merge_group",
                         "QUEUE_BASE_SHA": base, "QUEUE_HEAD_SHA": head, "TEST_HEAD": actual,
                         "FETCH_STATUS": fetch_status, "GITHUB_ENV": str(output)}, capture_output=True)
                self.assertEqual(result.returncode == 0, accepted, result.stderr)
                self.assertEqual(output.read_text() if output.exists() else "", "BASE_SHA=" + base + "\n" if accepted else "")

    def test_pr_coverage_base_is_the_merge_commit_base_parent(self):
        step = self.changed_test_base_step()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            origin, work = root / "origin.git", root / "work"
            env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t",
                   "GIT_COMMITTER_EMAIL": "t@t", "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}
            git = lambda *args: subprocess.run(["git", "-C", str(work), *args], env=env, check=True,
                                               capture_output=True, text=True).stdout.strip()
            subprocess.run(["git", "init", "-q", "--bare", "-b", "main", str(origin)], env=env, check=True)
            subprocess.run(["git", "init", "-q", "-b", "main", str(work)], env=env, check=True)
            git("remote", "add", "origin", str(origin))
            git("commit", "-q", "--allow-empty", "-m", "root")
            git("checkout", "-q", "-b", "feature")
            git("commit", "-q", "--allow-empty", "-m", "feature work")
            git("checkout", "-q", "main")
            git("commit", "-q", "--allow-empty", "-m", "main advances")
            synced_main = git("rev-parse", "HEAD")
            git("checkout", "-q", "feature")
            # The PR branch tip is itself a merge commit ("merge main into feature").
            git("merge", "-q", "--no-ff", "-m", "merge main into feature", "main")
            pr_head = git("rev-parse", "HEAD")
            git("checkout", "-q", "main")
            git("commit", "-q", "--allow-empty", "-m", "main advances again")
            base_tip = git("rev-parse", "HEAD")
            git("push", "-q", "origin", "main")
            git("merge", "-q", "--no-ff", "-m", "synthetic PR merge", pr_head)
            synthetic = git("rev-parse", "HEAD")

            def resolve(checkout, *, head=pr_head, base_ref="main"):
                git("checkout", "-q", "--detach", checkout)
                output = root / "env"
                output.unlink(missing_ok=True)
                result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", step["run"]], cwd=work,
                    env={**env, "GITHUB_EVENT_NAME": "pull_request", "PR_BASE_REF": base_ref, "PR_HEAD_SHA": head,
                         "QUEUE_BASE_SHA": "", "QUEUE_HEAD_SHA": "", "GITHUB_ENV": str(output)},
                    capture_output=True, text=True)
                return result, output.read_text() if output.exists() else ""

            for name, checkout, kwargs, expected, message in (
                ("synthetic merge", synthetic, {}, base_tip, None),
                ("PR head that is itself a merge", pr_head, {}, synced_main, None),
                ("unrelated checkout", synthetic, {"head": base_tip}, None, "unexpected checkout"),
                ("empty PR head", synthetic, {"head": ""}, None, "PR head unknown"),
                ("empty base ref", pr_head, {"base_ref": ""}, None, "PR_BASE_REF is empty"),
                ("missing base branch", pr_head, {"base_ref": "missing-branch"}, None, None),
            ):
                with self.subTest(name):
                    result, written = resolve(checkout, **kwargs)
                    if expected is None:
                        self.assertNotEqual(result.returncode, 0, result.stdout)
                        self.assertEqual(written, "")
                        if message:
                            self.assertIn(message, result.stdout)
                    else:
                        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                        self.assertEqual(written, f"BASE_SHA={expected}\n")

    def test_lifecycle_matrix_is_required_and_runs_real_proof(self):
        job = self.workflow["jobs"]["daemon-lifecycle"]
        # Windows runs the same proof post-merge (WINDOWS_STEPS in the `windows` job).
        self.assertEqual({row["platform"] for row in job["strategy"]["matrix"]["include"]}, {"linux", "macos"})
        self.assertFalse(job["strategy"]["fail-fast"])
        step = next(step for step in job["steps"] if step.get("name") == "Native lifecycle lint and runtime proof")
        self.assertIn("python3 -I -B scripts/test-daemon-lifecycle.py", step["run"])
        self.assertIn("cargo clippy --locked -p intentd-sitter -p intentd --all-targets -- -D warnings", step["run"])
        self.assertNotIn("if", step)
        self.assertFalse(step.get("continue-on-error", False))
        for event in ("merge_group",):
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
