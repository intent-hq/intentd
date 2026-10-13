#!/usr/bin/env python3
"""Temporary PR 2415 diagnostic supervisor; never changes test policy."""

import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time

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
    with (evidence / "native-triage.log").open("w") as log, \
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


def selection(targets):
    if len(targets) != 13 or len(set(map(tuple, targets))) != 13:
        raise ValueError("Expected the 13 distinct pre-storage failures")
    return " | ".join(f"(binary_id(={binary}) & test(={name}))" for binary, name in targets)


def verify_listing(evidence, targets):
    listings = list((evidence / "gate-runs").rglob("listing-1.json"))
    if len(listings) != 1:
        raise RuntimeError(f"Expected one focused listing, got {len(listings)}")
    data = json.loads(listings[0].read_text())
    selected = {
        (binary, name)
        for binary, suite in data["rust-suites"].items()
        for name, case in suite["testcases"].items()
        if case["filter-match"]["status"] == "matches" and not case["ignored"]
    }
    expected = set(map(tuple, targets))
    if selected != expected:
        raise RuntimeError(f"Selection mismatch: missing={expected - selected}, extra={selected - expected}")
    print("Confirmed exact selection of all 13 unresolved tests; full suite not run.")


def main():
    mode, *args = sys.argv[1:]
    if mode == "preflight":
        preflight(Path(args[0]))
        return 0
    if mode == "run":
        signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
        return supervise(args[3:], Path(args[0]), Path(args[1]), int(args[2]))
    targets = json.loads(Path(args[0]).read_text())
    if mode == "plan":
        import shlex
        print(shlex.join(["--workspace", "-E", selection(targets)]))
    elif mode == "verify":
        verify_listing(Path(args[1]), targets)
    else:
        raise ValueError(mode)
    return 0


if __name__ == "__main__":
    sys.exit(main())
