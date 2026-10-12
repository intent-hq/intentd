#!/usr/bin/env python3
"""Keep historical note-index lockfile inputs valid during release planning.

v0.10.43 embeds ../../../Cargo.lock from src/note_page_index.rs. That path fails
both in target/package and in Cargo's temporary registry. In release-plz's
throwaway baseline worktree, redirect only that include to the same baseline
lock via an absolute environment path, then restore the original source bytes.
Do not skip Cargo verification or use this adapter in the publication job. It
becomes a no-op once the selected baseline contains the OUT_DIR-based fix.
"""
import fcntl
import os
from pathlib import Path
import signal
import subprocess
import sys

BROKEN = b'include_str!("../../../Cargo.lock")'
COMPATIBLE = b'include_str!(env!("INTENT_STORE_BASELINE_LOCK"))'


def run_cargo(cargo, args, env):
    child = subprocess.Popen([cargo, *args], env=env)
    previous = {}
    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, lambda sig, _frame: child.send_signal(sig))
        code = child.wait()
        return code if code >= 0 else 128 - code
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def main():
    args = sys.argv[1:]
    cargo = os.environ["RELEASE_PLZ_REAL_CARGO"]
    source = Path("crates/intent-store/src/note_page_index.rs")
    env = dict(os.environ)
    if args != ["package", "--allow-dirty", "--workspace"] or not source.is_file():
        return run_cargo(cargo, args, env)
    # Serialize adapters for the same worktree; different baseline worktrees
    # have independent source files and never share compatibility inputs.
    with source.open("r+b") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        original = handle.read()
        if BROKEN not in original:
            return run_cargo(cargo, args, env)
        if original.count(BROKEN) != 1:
            raise ValueError("unexpected historical lockfile includes; reassess compatibility")
        env["INTENT_STORE_BASELINE_LOCK"] = str(Path("Cargo.lock").resolve(strict=True))
        try:
            handle.seek(0)
            handle.write(original.replace(BROKEN, COMPATIBLE))
            handle.truncate()
            handle.flush()
            return run_cargo(cargo, args, env)
        finally:
            handle.seek(0)
            handle.write(original)
            handle.truncate()


if __name__ == "__main__":
    sys.exit(main())
