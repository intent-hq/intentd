#!/usr/bin/env python3
"""Bounded, passive storage evidence for job-owned coverage commands.

No cleanup or low-space termination. Child status remains authoritative even
when diagnostic storage is unavailable. Directory totals explicitly report
incomplete traversal; file names and environment dumps are never recorded.
"""
import argparse
import json
import os
from pathlib import Path
import shlex
import stat
import subprocess
import sys
import time


def directory_usage(path, *, max_entries=20000, seconds=1.0):
    result = {"files": 0, "bytes": 0, "allocated_bytes": 0,
              "profiles": 0, "profile_bytes": 0, "complete": True}
    pending = [Path(path)]
    visited = 0
    deadline = time.monotonic() + seconds
    while pending:
        directory = pending.pop()
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    visited += 1
                    if visited > max_entries or time.monotonic() >= deadline:
                        result["complete"] = False
                        result["limit"] = "entries-or-time"
                        return result
                    info = entry.stat(follow_symlinks=False)
                    if stat.S_ISDIR(info.st_mode):
                        pending.append(Path(entry.path))
                    elif stat.S_ISREG(info.st_mode):
                        result["files"] += 1
                        result["bytes"] += info.st_size
                        result["allocated_bytes"] += info.st_blocks * 512
                        if entry.name.endswith(".profraw"):
                            result["profiles"] += 1
                            result["profile_bytes"] += info.st_size
        except OSError as error:
            result["complete"] = False
            result["errno"] = error.errno
    return result


def filesystem_usage(path):
    probe = Path(path)
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    info = os.statvfs(probe)
    return {"capacity_bytes": info.f_blocks * info.f_frsize,
            "available_bytes": info.f_bavail * info.f_frsize,
            "free_bytes": info.f_bfree * info.f_frsize,
            "inodes": info.f_files, "free_inodes": info.f_ffree}


def record(output, stage, event, **extra):
    """Best effort only: a telemetry error must not replace a test result."""
    try:
        config = json.loads((output / "paths.json").read_text())
        filesystems = {}
        for name, path in config.items():
            try:
                filesystems[name] = filesystem_usage(path)
            except OSError as error:
                filesystems[name] = {"errno": error.errno}
        target = directory_usage(config["target"])
        profiles = (target if config["profiles"] == config["target"]
                    else directory_usage(config["profiles"]))
        value = {"stage": stage, "event": event, "time_unix": time.time(),
                 "filesystems": filesystems,
                 "target": target, "profile_directory": profiles, **extra}
        # One append per record; observers are sequential within each stage.
        with (output / "resources.jsonl").open("a") as stream:
            stream.write(json.dumps(value) + "\n")
    except (OSError, ValueError, KeyError) as error:
        print(f"coverage storage evidence unavailable: {type(error).__name__}", file=sys.stderr)


def initialize(output):
    output.mkdir(parents=True, exist_ok=True)
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"], timeout=30))
    target = Path(os.environ.get("CARGO_LLVM_COV_TARGET_DIR",
                                 str(Path(metadata["target_directory"]) / "llvm-cov-target"))).resolve()
    coverage_env = subprocess.check_output(
        ["cargo", "llvm-cov", "show-env", "--sh"], text=True, timeout=30,
        env={**os.environ, "CARGO_TARGET_DIR": str(target),
             "CARGO_LLVM_COV_TARGET_DIR": str(target)})
    # Parse just the reported profile path; never execute or save the exports.
    profile = None
    for line in coverage_env.splitlines():
        fields = shlex.split(line)
        if len(fields) == 2 and fields[0] == "export" and fields[1].startswith("LLVM_PROFILE_FILE="):
            profile = fields[1].split("=", 1)[1].removesuffix(";")
    if not profile:
        raise ValueError("llvm-cov did not report its profile path")
    paths = {"workspace": str(Path.cwd().resolve()), "target": str(target),
             "profiles": str(Path(profile).resolve().parent),
             "temporary": str(Path(os.environ["RUNNER_TEMP"]).resolve()),
             "home": str(Path.home().resolve())}
    (output / "paths.json").write_text(json.dumps(paths, indent=2) + "\n")
    record(output, "job", "start")


def run(output, stage, command, interval=30):
    record(output, stage, "start")
    started = time.monotonic()
    try:
        # Same process group and inherited stdio as the original command.
        # CI cancellation owns termination; this observer never sends signals.
        child = subprocess.Popen(command)
    except OSError as error:
        record(output, stage, "launch-error", errno=error.errno, exit_code=127)
        return 127
    while True:
        try:
            code = child.wait(timeout=interval)
            break
        except subprocess.TimeoutExpired:
            record(output, stage, "periodic")
    code = code if code >= 0 else 128 - code
    record(output, stage, "exit", exit_code=code,
           elapsed_seconds=round(time.monotonic() - started, 3))
    return code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("init", "run", "snapshot"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--stage", default="job")
    parser.add_argument("--event", default="snapshot")
    parser.add_argument("--exit-code", type=int)
    args, command = parser.parse_known_args()
    if args.mode == "run":
        if not command or command[0] != "--" or len(command) < 2:
            parser.error("run requires -- COMMAND")
        return run(args.output, args.stage, command[1:])
    if command:
        parser.error("unexpected command")
    if args.mode == "init":
        initialize(args.output)
    else:
        record(args.output, args.stage, args.event, exit_code=args.exit_code)
    return 0


if __name__ == "__main__":
    sys.exit(main())
