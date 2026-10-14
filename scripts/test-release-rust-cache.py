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
CACHE_RUN = 'python3 scripts/setup-depot-sccache.py'


def workflow(name):
    return yaml.safe_load((ROOT / '.github/workflows' / name).read_text())


class ManagedCache(unittest.TestCase):
    def test_no_archive_backend_or_local_cache_limits(self):
        text = (ROOT / 'scripts/setup-depot-sccache.py').read_text()
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
            configured = [i for i, s in enumerate(steps) if s.get('run') == CACHE_RUN]
            self.assertEqual(len(configured), 1)
            provision = next(i for i, s in enumerate(steps) if s.get('name', '').startswith('Provision Rust'))
            self.assertLess(provision, configured[0])
            for i, step in enumerate(steps):
                if step.get('name') in ['Build artifacts', 'Build (cargo)', 'Build (cargo-zigbuild)', 'Build cache inputs']:
                    self.assertLess(configured[0], i)
                    self.assertNotIn('cache', step.get('if', ''))


if __name__ == '__main__':
    unittest.main()
