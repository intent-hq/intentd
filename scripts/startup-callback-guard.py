#!/usr/bin/env python3
"""Temporary single-test coverage supervisor; never changes test policy."""

import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

GIB = 1024**3


def free_bytes(path):
    return shutil.disk_usage(path).free


def preflight(path):
    free = free_bytes(path)
    print(json.dumps({"time": time.time(), "free_bytes": free}), flush=True)
    if free < 50 * GIB:
        raise RuntimeError("Native diagnostic needs at least 50 GiB free; no cleanup attempted")


def stop_group(child):
    # A runner-owned session only; also stop descendants after their leader exits.
    try:
        os.killpg(child.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        child.wait(timeout=10)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    child.wait(timeout=10)


def supervise(command, disk, evidence, seconds, interval=5):
    evidence.mkdir(parents=True, exist_ok=True)
    preflight(disk)
    started = time.monotonic()
    reason = "command-exit"
    code = 1
    with (evidence / "native-validation.log").open("w") as log, \
            (evidence / "resources.jsonl").open("w") as resources:
        child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                 start_new_session=True)
        try:
            while child.poll() is None:
                free = free_bytes(disk)
                elapsed = time.monotonic() - started
                sample = json.dumps({"elapsed_seconds": elapsed, "free_bytes": free})
                print(sample, flush=True)
                resources.write(sample + "\n")
                resources.flush()
                if free < 10 * GIB:
                    reason, code = "disk-below-10-GiB", 125
                    break
                if elapsed >= seconds:
                    reason, code = "runtime-limit", 124
                    break
                time.sleep(min(interval, max(0, seconds - elapsed)))
            else:
                code = child.returncode
        except BaseException:
            reason, code = "supervisor-interrupted", 143
            raise
        finally:
            stop_group(child)
            (evidence / "supervisor-result.json").write_text(json.dumps({
                "command": command, "exit_code": code, "reason": reason,
                "elapsed_seconds": time.monotonic() - started,
            }, indent=2) + "\n")
    return code


def verify_selection(evidence):
    expected = (evidence / "expected-test.txt").read_text().strip()
    suites = json.loads((evidence / "listing.json").read_text())["rust-suites"]
    selected = [name for suite in suites.values() for name, case in suite["testcases"].items()
                if case["filter-match"]["status"] == "matches"]
    if not expected or selected != [expected]:
        raise RuntimeError(f"Expected exactly the selected diagnostic test, got {selected}")
    print(json.dumps({"selected": selected}))


def verify_result(evidence):
    verify_selection(evidence)
    expected = (evidence / "expected-test.txt").read_text().strip()
    root = ET.parse(evidence / "junit.xml").getroot()
    cases = root.findall(".//testcase")
    if len(cases) != 1 or cases[0].get("name") != expected:
        raise RuntimeError("Expected one matching JUnit test result")
    for tag in ("failure", "error", "skipped", "rerunFailure", "rerunError", "flakyFailure", "flakyError"):
        if list(root.iter(tag)):
            raise RuntimeError(f"Diagnostic result contains {tag}")
    if (evidence / "test-exit-code.txt").read_text().strip() != "0":
        raise RuntimeError("Diagnostic process did not succeed")
    stages = verify_stages(cases[0].findtext("system-err", ""))
    (evidence / "verified-result.json").write_text(json.dumps({
        "test": expected, "fresh_passes": 1, "retries": 0, "exit_code": 0,
        "admission": stages,
    }, indent=2) + "\n")


def verify_stages(stderr):
    prefix = "budget-admission-trace "
    traces = [json.loads(line[len(prefix):]) for line in stderr.splitlines()
              if line.startswith(prefix)]
    if len(traces) != 2 or [t.get("iterations") for t in traces] != [63, 64]:
        raise RuntimeError("Missing or duplicate admission stage evidence for both quota cases")
    if any(t.get("subcase") != "mixed-alias-budget" or t.get("returned") is not True
           or t.get("unwinding") is not False for t in traces):
        raise RuntimeError("Admission stage evidence does not describe completed quota cases")
    stages = traces[0].get("stages", [])
    if (len(stages) != 4 or [s.get("stage") for s in stages] != [
            "retain-start", "retain-complete", "original-start", "original-complete"]
            or traces[1].get("stages") != []):
        raise RuntimeError("Admission stage evidence is incomplete or contradicts the quota boundary")
    path = stages[0].get("path")
    if path not in ("admit", "admit_optional") or any(
            s.get("path") != path or s.get("records") != 64 for s in stages):
        raise RuntimeError("Admission stage evidence has inconsistent path or record count")
    times = [s.get("elapsed_ms") for s in stages]
    if any(type(t) is not int or t < 0 for t in times) or times != sorted(times):
        raise RuntimeError("Admission stage evidence has invalid or decreasing times")
    return {"path": path, "records": 64, "stages": stages,
            "fixture_validation_ms": times[1] - times[0],
            "between_validation_and_admission_ms": times[2] - times[1],
            "original_admission_ms": times[3] - times[2]}


def main():
    mode, *args = sys.argv[1:]
    if mode == "preflight":
        preflight(Path(args[0]))
        return 0
    if mode == "run":
        signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
        return supervise(args[3:], Path(args[0]), Path(args[1]), int(args[2]))
    if mode == "verify-selection":
        verify_selection(Path(args[0]))
        return 0
    if mode == "verify-result":
        verify_result(Path(args[0]))
        return 0
    raise ValueError(mode)


if __name__ == "__main__":
    sys.exit(main())
