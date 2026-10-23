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
JOB_HEADER = re.compile(r'^  [A-Za-z0-9_-]+:\n', re.MULTILINE)


def _configure_step(text, step, configured, what):
    """Replace the one `step` header in text with `configured`; text is a job or the whole workflow."""
    if configured in text:
        return text
    if text.count(step) != 1:
        raise ValueError(f'cargo-dist template changed; expected exactly one {what} bash step')
    start = text.index(step) + len(step)
    following = text.find('\n      - ', start)
    body = text[start:] if following < 0 else text[start:following]
    if '\n        env:' in '\n' + body:
        raise ValueError(f'cargo-dist template changed; the {what} step already has env: '
                         '— merge CARGO_NET_OFFLINE by hand')
    return text.replace(step, configured, 1)


def _job_span(text, job):
    header = f'  {job}:\n'
    starts = [m.start() for m in JOB_HEADER.finditer(text) if m.group() == header]
    if len(starts) != 1:
        raise ValueError(f'cargo-dist template changed; expected exactly one `{job}` job')
    start = starts[0]
    following = JOB_HEADER.search(text, start + len(header))
    return start, len(text) if following is None else following.start()


def configure_host(text):
    return _configure_step(text, HOST_STEP, HOST_CONFIGURED, '`- id: host`')


def configure_global(text):
    start, end = _job_span(text, GLOBAL_JOB)
    job = _configure_step(text[start:end], GLOBAL_STEP, GLOBAL_CONFIGURED, f'{GLOBAL_JOB} `- id: cargo-dist`')
    return text[:start] + job + text[end:]


def configure(text):
    return configure_host(configure_global(text))


if __name__ == '__main__':
    path = Path(__file__).resolve().parents[1] / '.github/workflows/v-release.yml'
    path.write_text(configure(path.read_text()))
