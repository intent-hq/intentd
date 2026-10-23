#!/usr/bin/env python3
"""Download the locked crates once, retrying transient crates.io failures.

Callers then run cargo-dist with CARGO_NET_OFFLINE=true, so its internal
`cargo metadata` needs no network. Used by the Release `host` job (v0.11.31
publication failed twice on crates.io HTTP2/TLS errors in run 39880269962,
intent-hq/intent#7639) and by scripts/test-optional-intel-dist.py (runs
39394522385 and 39355882408). scripts/test-optional-intel-dist-network.py
covers the retry handling offline.

Run: python3 -I -B scripts/fetch-locked-crates.py
"""
from pathlib import Path
import os
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
FETCH = ['cargo', 'fetch', '--locked']
# Worst case (deadline plus backoff) is under 8 minutes, inside the 15-minute
# release-scripts job alongside its other steps.
FETCH_ATTEMPTS = 4
FETCH_ATTEMPT_TIMEOUT = 150
FETCH_DEADLINE = 420


def fetch_dependencies(root, env, attempts=FETCH_ATTEMPTS, sleep=time.sleep,
                       attempt_timeout=FETCH_ATTEMPT_TIMEOUT, deadline=FETCH_DEADLINE, clock=time.monotonic):
    """Download the locked crates, retrying transient network failures with backoff.

    A hung attempt is killed after attempt_timeout and counts as a failure; no
    attempt starts, or runs, past the overall deadline. HTTP/2 multiplexing is
    off: it caused the crates.io "HTTP2 framing layer" error.
    """
    env = {**env, 'CARGO_NET_RETRY': '3', 'CARGO_HTTP_MULTIPLEXING': 'false'}
    env.pop('CARGO_NET_OFFLINE', None)
    end = clock() + deadline
    tried = 0
    for attempt in range(1, attempts + 1):
        remaining = end - clock()
        if remaining <= 0:
            break
        tried = attempt
        try:
            if subprocess.run(FETCH, cwd=root, env=env, timeout=min(attempt_timeout, remaining)).returncode == 0:
                return
        except subprocess.TimeoutExpired:
            print(f'cargo fetch attempt {attempt} timed out', flush=True)
        if attempt < attempts:
            sleep(5 * 2 ** (attempt - 1))
    raise SystemExit(f'FAIL: `{" ".join(FETCH)}` failed after {tried} attempts ({deadline}s limit); the network '
                     '(crates.io) is unavailable. Re-run the job.')


def offline_env(env):
    return {**env, 'CARGO_NET_OFFLINE': 'true'}


if __name__ == '__main__':
    fetch_dependencies(ROOT, {**os.environ, 'CARGO_TERM_PROGRESS_WHEN': 'never'})
    print('locked crates fetched; later cargo calls can run with CARGO_NET_OFFLINE=true', flush=True)
