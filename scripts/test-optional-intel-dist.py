#!/usr/bin/env python3
"""Exercise the pinned cargo-dist with fake binaries and real release metadata.

Requires the version of dist pinned by dist-workspace.toml. No GitHub writes or
native compilation; all generated artifacts live in a disposable local clone.

Crates are fetched once up front, with retries (scripts/fetch-locked-crates.py),
and every dist call then runs with CARGO_NET_OFFLINE=true so a crates.io blip
cannot fail a dist step (runs 39394522385 and 39355882408). scripts/test-optional-intel-dist-network.py
covers that handling offline.
"""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[1]
INTEL = 'x86_64-apple-darwin'
_spec = importlib.util.spec_from_file_location('fetch_locked_crates', ROOT / 'scripts/fetch-locked-crates.py')
_fetch = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_fetch)
# Shared with the Release host job; re-exported for test-optional-intel-dist-network.py.
FETCH = _fetch.FETCH
FETCH_ATTEMPTS = _fetch.FETCH_ATTEMPTS
FETCH_ATTEMPT_TIMEOUT = _fetch.FETCH_ATTEMPT_TIMEOUT
FETCH_DEADLINE = _fetch.FETCH_DEADLINE
fetch_dependencies = _fetch.fetch_dependencies
offline_env = _fetch.offline_env


def run_dist(root, env, *args):
    return json.loads(subprocess.check_output(['dist', *args, '--output-format=json'], cwd=root, env=env))


def main():
    config = tomllib.loads((ROOT / 'dist-workspace.toml').read_text())['dist']
    version = subprocess.check_output(['dist', '--version'], text=True).strip()
    assert version == 'cargo-dist ' + config['cargo-dist-version'], version
    with tempfile.TemporaryDirectory(prefix='intentd-optional-intel-') as temp:
        root = Path(temp) / 'repo'
        subprocess.run(['git', 'clone', '--quiet', '--shared', str(ROOT), str(root)], check=True)
        shutil.copy(ROOT / 'dist-workspace.toml', root / 'dist-workspace.toml')
        shutil.copy(ROOT / 'scripts/optional-intel-release.py', root / 'scripts/optional-intel-release.py')
        env = {**os.environ, 'CARGO_TERM_PROGRESS_WHEN': 'never'}
        fetch_dependencies(root, env)
        env = offline_env(env)

        def dist(*args):
            return run_dist(root, env, *args)

        artifacts = root / 'target/distrib'
        manifest = dist('build', '--artifacts=lies')
        (artifacts / 'local-dist-manifest.json').write_text(json.dumps(manifest))
        policy = ['python3', '-I', '-B', 'scripts/optional-intel-release.py', 'targets', str(artifacts)]
        flags = subprocess.check_output(policy, cwd=root, text=True).split()
        assert '--target=' + INTEL in flags
        included = dist('build', '--artifacts=global', *flags)
        assert any(INTEL in name for name in included['artifacts'])
        (artifacts / 'global-dist-manifest.json').write_text(json.dumps(included))
        # Simulate an optional failed upload retaining only its archive: its
        # archive, stale manifest records, checksums and download row must go.
        (artifacts / f'intentd-{INTEL}.tar.xz.sha256').unlink()
        flags = subprocess.check_output(policy, cwd=root, text=True).split()
        assert '--target=' + INTEL not in flags
        # Late host/final cleanup must sanitize the already-built aggregate.
        assert INTEL not in (artifacts / 'sha256.sum').read_text()
        # Inspect the existing global outputs: rebuilding here would hide late
        # cleanup defects in the actual host/final publication path.
        omitted = json.loads((artifacts / 'global-dist-manifest.json').read_text())
        assert not any(INTEL in name for name in omitted['artifacts'])
        assert INTEL not in omitted['announcement_github_body']
        assert INTEL not in (artifacts / 'sha256.sum').read_text()
        assert not [p for p in artifacts.glob(f'*{INTEL}*') if p.is_file()]
        for target in config['targets']:
            if target != INTEL:
                assert any(target in name for name in omitted['artifacts']), target
        # The final host manifest is built from the same filtered local/global
        # inputs; use manifest mode to inspect it without any hosting writes.
        final = dist('manifest', '--artifacts=all', *flags)
        assert INTEL not in final['announcement_github_body']
        assert not any(INTEL in name for name in final['artifacts'])
        # Negative control: a missing required checksum must reject publication,
        # even when all other required artifacts are healthy and Intel is absent.
        required_checksum = artifacts / 'intentd-aarch64-apple-darwin.tar.xz.sha256'
        required_checksum.unlink()
        rejected = subprocess.run(policy, cwd=root, capture_output=True, text=True)
        assert rejected.returncode != 0
        assert 'missing or invalid required archive/checksum: aarch64-apple-darwin' in rejected.stderr
        assert not rejected.stdout.strip()
        # Reapply the customization to real generated YAML, not only an
        # already-customized fixture. Existing unrelated customizations are
        # outside this script's ownership.
        subprocess.run(['dist', 'generate'], cwd=root, env=env, check=True)
        shutil.copy(ROOT / 'scripts/configure-optional-intel-release.py', root / 'scripts/configure-optional-intel-release.py')
        customize = ['python3', '-I', '-B', 'scripts/configure-optional-intel-release.py']
        subprocess.run(customize, cwd=root, check=True)
        generated = (root / '.github/workflows/v-release.yml').read_text()
        subprocess.run(customize, cwd=root, check=True)
        assert generated == (root / '.github/workflows/v-release.yml').read_text()
        shutil.copy(ROOT / 'scripts/configure-release-host-network.py',
                    root / 'scripts/configure-release-host-network.py')
        host_network = ['python3', '-I', '-B', 'scripts/configure-release-host-network.py']
        subprocess.run(host_network, cwd=root, check=True)
        fetched = (root / '.github/workflows/v-release.yml').read_text()
        subprocess.run(host_network, cwd=root, check=True)
        assert fetched == (root / '.github/workflows/v-release.yml').read_text()
        assert 'CARGO_NET_OFFLINE: "true"' in fetched
        shutil.copy(ROOT / 'scripts/configure-runner-routing.py', root / 'scripts/configure-runner-routing.py')
        route_command = ['python3', '-I', '-B', 'scripts/configure-runner-routing.py']
        subprocess.run(route_command, cwd=root, check=True)
        routed = (root / '.github/workflows/v-release.yml').read_text()
        subprocess.run(route_command, cwd=root, check=True)
        assert routed == (root / '.github/workflows/v-release.yml').read_text()
        assert "needs.runner-route.outputs.runners" in routed
        print('PASS: pinned cargo-dist includes successful Intel and removes partial Intel assets, checksums and release links')


if __name__ == '__main__':
    main()
