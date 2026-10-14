#!/usr/bin/env python3
"""Validate classifier output and run the compiled-docs Cargo selection."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys


FULL = {"tier": "full", "packages": [], "tests": []}
NAME = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_-]*\Z")


def validate(scope):
    if not isinstance(scope, dict) or set(scope) != {"tier", "packages", "tests"}:
        raise ValueError("invalid docs scope fields")
    if scope["tier"] not in ("docs", "compiled", "full"):
        raise ValueError("unknown docs tier")
    if not isinstance(scope["packages"], list) or not isinstance(scope["tests"], list):
        raise ValueError("invalid Cargo selection")
    for package in scope["packages"]:
        if not isinstance(package, str) or not NAME.fullmatch(package):
            raise ValueError("invalid package name")
    for test in scope["tests"]:
        if not isinstance(test, dict) or set(test) != {"package", "target"}:
            raise ValueError("invalid test selection")
        if any(not isinstance(value, str) or not NAME.fullmatch(value) for value in test.values()):
            raise ValueError("invalid test name")
    selected = bool(scope["packages"] or scope["tests"])
    if selected != (scope["tier"] == "compiled"):
        raise ValueError("docs tier does not match Cargo selection")
    return scope


def classify(base, head):
    try:
        subprocess.run(["git", "fetch", "--no-tags", "--depth=1", "origin", base, head],
                       check=True, timeout=60)
        result = subprocess.run(["bash", "scripts/docs-ci-scope.sh", base, head],
                                check=True, capture_output=True, text=True, timeout=120)
        sys.stderr.write(result.stderr)
        return validate(json.loads(result.stdout))
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"Docs classification unavailable; running full CI: {error}", file=sys.stderr)
        return FULL.copy()


def commands(scope):
    validate(scope)
    if scope["tier"] != "compiled":
        raise ValueError("focused checks require the compiled tier")
    selected = [arg for name in sorted(set(scope["packages"])) for arg in ("-p", name)]
    result = []
    if selected:
        result.extend([
            ["cargo", "check", "--locked", *selected],
            ["cargo", "clippy", "--locked", *selected, "--all-targets", "--", "-D", "warnings"],
            ["cargo", "test", "--locked", *selected],
        ])
    for test in scope["tests"]:
        if test["package"] in scope["packages"]:
            continue
        target = ["-p", test["package"], "--test", test["target"]]
        result.extend([
            ["cargo", "check", "--locked", *target],
            ["cargo", "clippy", "--locked", *target, "--", "-D", "warnings"],
            ["cargo", "test", "--locked", *target],
        ])
    return result


def main():
    if len(sys.argv) == 4 and sys.argv[1] == "classify":
        scope = classify(*sys.argv[2:])
        encoded = json.dumps(scope, separators=(",", ":"))
        print(encoded)
        with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
            output.write(f"tier={scope['tier']}\nscope={encoded}\n")
        if os.environ.get("GITHUB_STEP_SUMMARY"):
            with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as summary:
                summary.write(f"### Docs CI selection: {scope['tier']}\n\n```json\n{encoded}\n```\n")
    elif sys.argv[1:] == ["compiled"]:
        scope = validate(json.loads(os.environ["DOCS_SCOPE"]))
        env = {**os.environ, "INTENTD_ASSERT_BOUND_CALLER": "1"}
        for command in commands(scope):
            print("+ " + " ".join(command), flush=True)
            subprocess.run(command, check=True, env=env)
    else:
        raise SystemExit("Usage: docs-ci.py classify BASE HEAD | compiled")


if __name__ == "__main__":
    main()
