#!/usr/bin/env python3
"""Build this tree's sitter, then require actual native lifecycle execution."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys


def require_passed(output, minimum=1):
    results = re.findall(r"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored", output)
    if not results or sum(int(passed) for passed, _, _ in results) < minimum:
        raise RuntimeError("lifecycle proof selected no passing tests")
    if any(int(failed) or int(ignored) for _, failed, ignored in results):
        raise RuntimeError("lifecycle proof failed or skipped tests")


def run(args, env=None):
    print("+ " + " ".join(args), flush=True)
    result = subprocess.run(args, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print(result.stdout, flush=True)
    result.check_returncode()
    return result.stdout


def main():
    env = dict(os.environ, CARGO_TERM_PROGRESS_WHEN="never")
    artifacts = run(["cargo", "build", "--locked", "-p", "intentd-sitter", "--message-format=json"], env)
    sitter = None
    for line in artifacts.splitlines():
        if line.startswith("{"):
            item = json.loads(line)
            if item.get("reason") == "compiler-artifact" and item.get("target", {}).get("name") == "intentd-sitter" and item.get("executable"):
                sitter = Path(item["executable"]).resolve()
    if sitter is None or not sitter.is_file():
        raise RuntimeError("current build did not produce the sitter executable")
    env["INTENTD_TEST_SITTER_BIN"] = str(sitter)
    require_passed(run(["cargo", "test", "--locked", "-p", "intentd-sitter", "--lib"], env))
    if os.name != "nt":
        require_passed(run(["cargo", "test", "--locked", "-p", "intentd-sitter", "--lib", "--", "--exact", "startup::tests::dead_supervisor_cleanup_closes_descendant_pipe"], env))
        require_passed(run(["cargo", "test", "--locked", "-p", "intentd-sitter", "--test", "supervisor_e2e", "background_start"], env))
    else:
        require_passed(run(["cargo", "test", "--locked", "-p", "intentd-sitter", "--lib", "--", "--exact", "windows::tests::restart_events_reject_foreign_identity_and_require_explicit_completion"], env))
        require_passed(run(["cargo", "test", "--locked", "-p", "intentd", "--test", "e2e_detached_lifecycle", "windows_"], env))
    require_passed(run(["cargo", "test", "--locked", "-p", "intentd", "--test", "e2e_detached_lifecycle", "--", "--ignored", "--exact", "detached_sitter_lifecycle_over_wss", "--nocapture"], env))


if __name__ == "__main__":
    main()
