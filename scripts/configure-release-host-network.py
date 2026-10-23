#!/usr/bin/env python3
"""Reapply the Release workflow's crates.io resilience after `dist generate`.

`dist build` and `dist host` run `cargo metadata`; on a fresh runner that
downloads every crate, and a crates.io HTTP2/TLS blip failed v0.11.31
publication twice in `dist host` (run 39880269962, intent-hq/intent#7639).
The global `dist build` in build-global-artifacts runs just before it, after
every platform build has passed, and is exposed to the same blip. For both
steps, fetch the locked crates first with retries, then run dist with
CARGO_NET_OFFLINE=true. scripts/test-release-host-network.py checks the result.
"""
from pathlib import Path
import re

FETCH_STEP_NAME = 'Fetch locked crates (retrying)'
FETCH_STEP = (
    f'      - name: {FETCH_STEP_NAME}\n'
    '        run: python3 -I -B scripts/fetch-locked-crates.py\n'
)
OFFLINE_ENV = (
    '        env:\n'
    '          CARGO_NET_OFFLINE: "true"\n'
)
HOST_STEP = '      - id: host\n        shell: bash\n'
HOST_CONFIGURED = (
    '      # LOCAL CUSTOMIZATION: fetch crates with retries so `dist host` can run\n'
    '      # its cargo metadata offline. After `dist generate`, run\n'
    '      # python3 scripts/configure-release-host-network.py (source of this block).\n'
    + FETCH_STEP + HOST_STEP + OFFLINE_ENV
)
GLOBAL_JOB = 'build-global-artifacts'
GLOBAL_STEP = '      - id: cargo-dist\n        shell: bash\n'
GLOBAL_CONFIGURED = (
    '      # LOCAL CUSTOMIZATION: fetch crates with retries so the global `dist build`\n'
    '      # can run its cargo metadata offline. After `dist generate`, run\n'
    '      # python3 scripts/configure-release-host-network.py (source of this block).\n'
    + FETCH_STEP + GLOBAL_STEP + OFFLINE_ENV
)
HOST_JOB = 'host'
JOBS = re.compile(r'^jobs:\n', re.MULTILINE)
TOP_LEVEL_KEY = re.compile(r'^[^\s#]', re.MULTILINE)
JOB_HEADER = re.compile(r'^  [A-Za-z0-9_-]+:\n', re.MULTILINE)


def _job_span(text, job):
    """Return the [start, end) span of `job` within the top-level `jobs:` mapping."""
    jobs = list(JOBS.finditer(text))
    if len(jobs) != 1:
        raise ValueError('cargo-dist template changed; expected exactly one top-level `jobs:`')
    body = jobs[0].end()
    after = TOP_LEVEL_KEY.search(text, body)
    jobs_end = len(text) if after is None else after.start()
    header = f'  {job}:\n'
    headers = [m for m in JOB_HEADER.finditer(text, body, jobs_end)]
    starts = [i for i, m in enumerate(headers) if m.group() == header]
    if len(starts) != 1:
        raise ValueError(f'cargo-dist template changed; expected exactly one `{job}` job')
    i = starts[0]
    end = headers[i + 1].start() if i + 1 < len(headers) else jobs_end
    return headers[i].start(), end


def _configure_job(text, job_name, step, configured, what):
    """Replace the one `step` header inside `job_name` with `configured`, failing loudly on drift."""
    start, end = _job_span(text, job_name)
    job = text[start:end]
    if job.count(step) != 1:
        raise ValueError(f'cargo-dist template changed; expected exactly one {what} bash step')
    if configured in job:
        return text
    step_start = job.index(step) + len(step)
    following = job.find('\n      - ', step_start)
    body = job[step_start:] if following < 0 else job[step_start:following]
    if '\n        env:' in '\n' + body:
        raise ValueError(f'cargo-dist template changed; the {what} step already has env: '
                         '— merge CARGO_NET_OFFLINE by hand')
    return text[:start] + job.replace(step, configured, 1) + text[end:]


def configure_host(text):
    return _configure_job(text, HOST_JOB, HOST_STEP, HOST_CONFIGURED, '`- id: host`')


def configure_global(text):
    return _configure_job(text, GLOBAL_JOB, GLOBAL_STEP, GLOBAL_CONFIGURED, f'{GLOBAL_JOB} `- id: cargo-dist`')


def configure(text):
    return configure_host(configure_global(text))


if __name__ == '__main__':
    path = Path(__file__).resolve().parents[1] / '.github/workflows/v-release.yml'
    path.write_text(configure(path.read_text()))
