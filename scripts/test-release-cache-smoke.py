#!/usr/bin/env python3
"""Offline compiler-cache smoke test; requires cargo, rustc and sccache on PATH.

Uses an isolated cache/server and no downloaded dependencies. This proves cache
reuse, profile/flag separation and eviction fallback, NOT release speedups.
"""
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time


def main():
    sccache = shutil.which('sccache')
    if not sccache:
        raise SystemExit('sccache is required')
    with tempfile.TemporaryDirectory(prefix='release-cache-smoke-') as temp:
        root = Path(temp)
        (root / 'src').mkdir()
        (root / 'src/lib.rs').write_text('pub fn answer() -> u64 { (1..=6).product() }\n')
        (root / 'Cargo.toml').write_text('''[package]
name = "release_cache_smoke"
version = "0.1.0"
edition = "2021"
[profile.dist]
inherits = "release"
lto = "thin"
''')
        with socket.socket() as port:
            port.bind(('127.0.0.1', 0))
            server_port = str(port.getsockname()[1])
        env = {**os.environ, 'RUSTC_WRAPPER': sccache, 'CARGO_INCREMENTAL': '0',
               'SCCACHE_DIR': str(root / 'cache'), 'SCCACHE_CACHE_SIZE': '256M',
               'SCCACHE_SERVER_PORT': server_port, 'CARGO_TARGET_DIR': str(root / 'target')}
        # Never use an inherited remote backend or compiler flags for this test.
        for key in list(env):
            if key.startswith(('SCCACHE_BUCKET', 'SCCACHE_GHA_', 'SCCACHE_REDIS', 'SCCACHE_MEMCACHED', 'SCCACHE_AZURE', 'SCCACHE_GCS', 'RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS')):
                env.pop(key)
        env['SCCACHE_GHA_ENABLED'] = 'false'

        def run(*args):
            return subprocess.check_output(args, cwd=root, env=env, text=True, stderr=subprocess.STDOUT)

        def hits():
            return sum(json.loads(run(sccache, '--show-stats', '--stats-format=json'))['stats']['cache_hits']['counts'].values())

        def build(profile):
            shutil.rmtree(root / 'target', ignore_errors=True)
            start = time.monotonic()
            run('cargo', 'build', '--offline', '--profile', profile)
            return round(time.monotonic() - start, 3)

        results = {}
        try:
            run(sccache, '--start-server')
            for profile in ('dist', 'release'):
                before = hits()
                cold = build(profile)
                after_cold = hits()
                assert after_cold == before, f'{profile} incorrectly reused another profile'
                warm = build(profile)
                assert hits() > after_cold, f'{profile} produced no compiler cache hit'
                results[profile] = {'cold_seconds': cold, 'warm_seconds': warm}
            env['RUSTFLAGS'] = '-C opt-level=1'
            before = hits()
            build('release')
            assert hits() == before, 'changed flags incorrectly hit cached compilation'
            run(sccache, '--stop-server')
            results['cache_bytes'] = sum(p.stat().st_size for p in (root / 'cache').rglob('*') if p.is_file())
            shutil.rmtree(root / 'cache')
            run(sccache, '--start-server')
            build('release')
            assert hits() == 0, 'evicted cache should rebuild cold'
            print(json.dumps(results, indent=2))
        finally:
            subprocess.run([sccache, '--stop-server'], cwd=root, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == '__main__':
    main()
