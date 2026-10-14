#!/usr/bin/env python3
"""Depot managed compiler-cache and release warming contracts."""
import os
from pathlib import Path
import subprocess
import tempfile
import tomllib
import unittest
import yaml

ROOT = Path(__file__).resolve().parents[1]
ACTION = './.github/actions/release-rust-cache'


def workflow(name):
    return yaml.safe_load((ROOT / '.github/workflows' / name).read_text())


class ManagedCache(unittest.TestCase):
    def test_requires_managed_endpoint_without_exposing_credentials(self):
        action = yaml.safe_load((ROOT / '.github/actions/release-rust-cache/action.yml').read_text())
        setup = next(s['run'] for s in action['runs']['steps'] if s.get('name') == 'Require Depot managed cache')
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / 'env'
            env = {k: v for k, v in os.environ.items() if not k.startswith('SCCACHE_')}
            env['GITHUB_ENV'] = str(output)
            missing = subprocess.run(['bash', '-eu', '-c', setup], env=env, capture_output=True, text=True)
            self.assertNotEqual(missing.returncode, 0)
            self.assertFalse(output.exists())
            env.update(SCCACHE_WEBDAV_ENDPOINT='https://cache.depot.dev', SCCACHE_WEBDAV_TOKEN='secret-fixture')
            result = subprocess.run(['bash', '-eu', '-c', setup], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertNotIn('secret-fixture', result.stdout + result.stderr + output.read_text())
            values = dict(line.split('=', 1) for line in output.read_text().splitlines())
            self.assertEqual(values['RUSTC_WRAPPER'], 'sccache')
            self.assertEqual(values['CARGO_INCREMENTAL'], '0')
            self.assertEqual(values['SCCACHE_GHA_ENABLED'], 'false')
            self.assertNotIn('SCCACHE_DIR', values)
            self.assertNotIn('SCCACHE_LOCAL_RW_MODE', values)

    def test_no_archive_backend_or_local_cache_limits(self):
        text = (ROOT / '.github/actions/release-rust-cache/action.yml').read_text()
        for obsolete in ['actions/cache', 'SCCACHE_DIR', 'SCCACHE_CACHE_SIZE', 'SCCACHE_LOCAL_RW_MODE']:
            self.assertNotIn(obsolete, text)
        for name in ['release-cache.yml', 'release-plz.yml']:
            text = (ROOT / '.github/workflows' / name).read_text()
            self.assertNotIn('runner.temp }}/sccache', text)
            self.assertNotIn('release-cache-controls.py', text)

    def test_warming_is_build_only_and_reproducible(self):
        subprocess.run(['python3', '-I', '-B', 'scripts/configure-release-cache-producer.py', '--check'], cwd=ROOT, check=True)
        warm = workflow('release-cache.yml')
        self.assertEqual(warm['permissions'], {'contents': 'read'})
        self.assertNotIn('retention', warm['jobs'])
        for job in warm['jobs'].values():
            self.assertIn("github.ref == 'refs/heads/main'", job['if'])
            for step in job['steps']:
                self.assertNotIn('actions/cache/save', step.get('uses', ''))
        text = (ROOT / '.github/workflows/release-cache.yml').read_text()
        for forbidden in ['gh release', 'dist host', 'dist publish', 'git push', 'repository_dispatch']:
            self.assertNotIn(forbidden, text)

    def test_release_and_warming_use_identical_setup_and_targets(self):
        source = yaml.safe_load((ROOT / '.github/dist-build-setup.yml').read_text())
        daemon = workflow('v-release.yml')['jobs']['build-local-artifacts']['steps']
        start = next(i for i, s in enumerate(daemon) if s.get('name') == source[0]['name'])
        self.assertEqual(daemon[start:start + len(source)], source)
        sitter = workflow('release-sitter.yml')['jobs']['build']
        warm = workflow('release-cache.yml')['jobs']
        self.assertEqual(sitter['strategy'], warm['sitter']['strategy'])
        targets = tomllib.loads((ROOT / 'dist-workspace.toml').read_text())['dist']['targets']
        self.assertEqual({row['target'] for row in sitter['strategy']['matrix']['include']}, set(targets))
        for steps in [source, sitter['steps'], warm['daemon']['steps'], warm['sitter']['steps']]:
            configured = [i for i, s in enumerate(steps) if s.get('uses') == ACTION]
            self.assertEqual(len(configured), 1)
            provision = next(i for i, s in enumerate(steps) if s.get('name', '').startswith('Provision Rust'))
            self.assertLess(provision, configured[0])
            for i, step in enumerate(steps):
                if step.get('name') in ['Build artifacts', 'Build (cargo)', 'Build (cargo-zigbuild)', 'Build cache inputs']:
                    self.assertLess(configured[0], i)
                    self.assertNotIn('cache', step.get('if', ''))


if __name__ == '__main__':
    unittest.main()
