#!/usr/bin/env python3
"""Reapply the Release host job's crates.io resilience after `dist generate`.

`dist host` runs `cargo metadata`; on a fresh runner that downloads every
crate, and a crates.io HTTP2/TLS blip failed v0.11.31 publication twice (run
39880269962, intent-hq/intent#7639). Fetch the locked crates first with
retries, then run `dist host` with CARGO_NET_OFFLINE=true.
scripts/test-release-host-network.py checks the result.
"""
from pathlib import Path

FETCH_STEP_NAME = 'Fetch locked crates (retrying)'
HOST_STEP = '      - id: host\n        shell: bash\n'
CONFIGURED = (
    '      # LOCAL CUSTOMIZATION: fetch crates with retries so `dist host` can run\n'
    '      # its cargo metadata offline. After `dist generate`, run\n'
    '      # python3 scripts/configure-release-host-network.py (source of this block).\n'
    f'      - name: {FETCH_STEP_NAME}\n'
    '        run: python3 -I -B scripts/fetch-locked-crates.py\n'
    '      - id: host\n'
    '        shell: bash\n'
    '        env:\n'
    '          CARGO_NET_OFFLINE: "true"\n'
)


def configure(text):
    if CONFIGURED in text:
        return text
    if text.count(HOST_STEP) != 1:
        raise ValueError('cargo-dist template changed; expected exactly one `- id: host` bash step')
    return text.replace(HOST_STEP, CONFIGURED, 1)


if __name__ == '__main__':
    path = Path(__file__).resolve().parents[1] / '.github/workflows/v-release.yml'
    path.write_text(configure(path.read_text()))
