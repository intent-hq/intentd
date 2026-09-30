#!/usr/bin/env python3
"""Offline compiler-cache smoke test; requires PyYAML, cargo, rustc and sccache.

Uses an isolated cache/server and no downloaded dependencies. This proves cache
reuse, profile/flag separation and eviction fallback, NOT release speedups.
"""
import json
import os
from pathlib import Path
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import time

import yaml


def pressure_regression(sccache):
    """Replay a graph larger than the cache using the action's consumer policy.

    Three deterministic, incompressible Rust libraries exceed the scaled-down
    cache. A writable consumer evicts the restored last library before reaching
    it, even though every compiler key is identical. No network is needed.
    """
    action = yaml.safe_load((Path(__file__).resolve().parents[1] /
                             '.github/actions/release-rust-cache/action.yml').read_text())
    with tempfile.TemporaryDirectory(prefix='release-cache-pressure-') as temp:
        root = Path(temp)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = str(sock.getsockname()[1])
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(('SCCACHE_', 'RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS',
                                    'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER'))}
        env.update(CACHE_FLAVOR='daemon-dist', CACHE_TARGET='x86_64-unknown-linux-musl',
                   CACHE_RUNNER='gh-linux-16x', CACHE_GENERATION='', TOOLCHAIN='test',
                   BUILD_INPUTS='test', RUNNER_TEMP=temp,
                   GITHUB_OUTPUT=str(root / 'output'), GITHUB_ENV=str(root / 'environment'))
        def settings(generation):
            (root / 'environment').write_text('')
            subprocess.run(['bash', '-eu', '-c', action['runs']['steps'][0]['run']],
                           env={**env, 'CACHE_GENERATION': generation}, check=True)
            return dict(line.split('=', 1) for line in (root / 'environment').read_text().splitlines())

        consumer = settings('')
        producer = settings('100-1')
        env.update(consumer)
        consumer_mode = consumer.get('SCCACHE_LOCAL_RW_MODE', 'READ_WRITE')
        env.update(SCCACHE_SERVER_PORT=port, SCCACHE_CONF=str(root / 'config'),
                   SCCACHE_CACHE_SIZE='1M')
        (root / 'config').write_text('')
        rng = random.Random(0)
        for i in range(3):
            data = ','.join(str(rng.getrandbits(64)) for _ in range(32768))
            (root / f'entry{i}.rs').write_text(f'pub static DATA: [u64; 32768] = [{data}];')

        def run(*args):
            return subprocess.check_output(args, cwd=root, env=env, text=True,
                                           stderr=subprocess.PIPE)

        def build(mode):
            env['SCCACHE_LOCAL_RW_MODE'] = mode
            run(sccache, '--start-server')
            for i in range(3):
                run(sccache, shutil.which('rustc'), '--crate-name', f'entry{i}',
                    '--crate-type', 'rlib', '--emit=link', '--out-dir', str(root),
                    str(root / f'entry{i}.rs'))
            stats = json.loads(run(sccache, '--show-stats', '--stats-format=json'))['stats']
            run(sccache, '--stop-server')
            return stats

        cache = Path(env['SCCACHE_DIR'])
        try:
            cold = build('READ_WRITE')
            assert cold['cache_writes'] == 3 and cold['cache_write_errors'] == 0, cold
            seed = {p.relative_to(cache): p.read_bytes() for p in cache.rglob('*') if p.is_file()}
            assert seed, 'cold build must retain a real compiler entry'
            warm = build(consumer_mode)
            assert warm['cache_hits']['counts'].get('Rust', 0) > 0, (
                'daemon consumer evicted the restored entries before they could be reused', warm)
            assert {p.relative_to(cache): p.read_bytes() for p in cache.rglob('*') if p.is_file()} == seed
            # Cold/missing archives still compile successfully in consumer mode.
            shutil.rmtree(cache)
            empty = build(consumer_mode)
            assert empty['cache_misses']['counts'].get('Rust') == 3, empty
            # Producers get bounded working room, but upload the original cap.
            # Scale both production limits by the same factor for this fixture.
            for name, data in seed.items():
                (cache / name).parent.mkdir(parents=True, exist_ok=True)
                (cache / name).write_bytes(data)
            size = producer['SCCACHE_CACHE_SIZE']
            env['SCCACHE_CACHE_SIZE'] = str(int(size[:-1]) * {'M': 2**20, 'G': 2**30}[size[-1]] // 256)
            refresh = build(producer.get('SCCACHE_LOCAL_RW_MODE', 'READ_WRITE'))
            assert refresh['cache_hits']['counts'].get('Rust', 0) > 0, refresh
            assert refresh['cache_writes'] == 2 and refresh['cache_write_errors'] == 0, refresh
            # A changed compiler input must still refresh; never freeze producers.
            with (root / 'entry2.rs').open('a') as source:
                source.write('\npub fn generation() -> u8 { 2 }\n')
            changed = build(producer.get('SCCACHE_LOCAL_RW_MODE', 'READ_WRITE'))
            assert changed['cache_writes'] == 1 and changed['cache_write_errors'] == 0, changed
            controls = Path(__file__).with_name('release-cache-controls.py')
            bound = json.loads(run(sys.executable, '-B', str(controls), 'bound', str(cache),
                                   '--limit', str(2**20)))
            assert bound['after_bytes'] <= 2**20, bound
            env['SCCACHE_CACHE_SIZE'] = '1M'
            refreshed = build(consumer_mode)
            assert refreshed['cache_hits']['counts'].get('Rust', 0) > 0, refreshed
            return {'warm_rust_hits': warm['cache_hits']['counts']['Rust'],
                    'warm_rust_misses': warm['cache_misses']['counts']['Rust'],
                    'producer_rust_hits': refresh['cache_hits']['counts']['Rust']}
        finally:
            subprocess.run([sccache, '--stop-server'], env=env,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main():
    sccache = shutil.which('sccache')
    if not sccache:
        raise SystemExit('sccache is required')
    print(json.dumps({'pressure_regression': pressure_regression(sccache)}), flush=True)
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
