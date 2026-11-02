#!/usr/bin/env python3
"""Self-hosted compiler-cache and release warming contracts."""
from copy import deepcopy
from pathlib import Path
import os
import subprocess
import tempfile
import tomllib
import unittest
import yaml

ROOT = Path(__file__).resolve().parents[1]
CACHE_RUN = 'python3 scripts/setup-sccache.py'


def workflow(name):
    return yaml.safe_load((ROOT / '.github/workflows' / name).read_text())


class ManagedCache(unittest.TestCase):
    def test_managed_smoke_skips_when_cache_is_switched_off(self):
        # No cache endpoint; keep the loader path so a setup-python interpreter still starts.
        env = {'PATH': '/usr/bin:/bin', **{k: v for k, v in os.environ.items() if k == 'LD_LIBRARY_PATH'}}
        for phase in ('populate', 'verify'):
            result = subprocess.run(['python3', '-I', '-B', 'scripts/test-release-cache-smoke.py', '--managed', '--phase', phase],
                                    cwd=ROOT, env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('skipping the managed cache smoke test', result.stdout)

    def test_no_archive_backend_or_local_cache_limits(self):
        text = (ROOT / 'scripts/setup-sccache.py').read_text()
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
        self.assertEqual(warm['jobs']['runner-route']['uses'], './.github/workflows/runner-route.yml')
        for name, job in warm['jobs'].items():
            if name == 'runner-route':
                continue
            self.assertIn("github.ref == 'refs/heads/main'", job['if'])
            for step in job['steps']:
                self.assertNotIn('actions/cache/save', step.get('uses', ''))
        text = (ROOT / '.github/workflows/release-cache.yml').read_text()
        for forbidden in ['gh release', 'dist host', 'dist publish', 'git push', 'repository_dispatch']:
            self.assertNotIn(forbidden, text)

    def test_warming_skips_release_metadata_pushes(self):
        # release-plz version bumps touch Cargo.toml/Cargo.lock on every
        # release, but the tree was already warmed and the tag's release build
        # compiles the new versions itself; dispatch always warms.
        warm = workflow('release-cache.yml')
        plan = warm['jobs']['plan']
        classify = next(step for step in plan['steps'] if step.get('id') == 'release')
        self.assertEqual(classify['if'], "github.event_name == 'push'")
        self.assertEqual(classify['env'], {'BASE_SHA': '${{ github.event.before }}'})
        self.assertIn('./scripts/release-pr-fast-path.sh --exact-parent "$BASE_SHA" HEAD', classify['run'])
        self.assertIn('|| out="fast_path=false"', classify['run'])
        self.assertEqual(plan['outputs']['release_push'], '${{ steps.release.outputs.release_push }}')
        for name in ('daemon', 'sitter'):
            job = warm['jobs'][name]
            self.assertIn('plan', job['needs'])
            self.assertTrue(job['if'].endswith("&& needs.plan.outputs.release_push != 'true'"), name)
        # The classifier's output line maps onto release_push; a miss or an
        # unparseable result keeps warming.
        body = classify['run'].replace('git fetch --no-tags --depth=2 origin "$GITHUB_SHA" || true\n', '')
        for script, expected in (('echo fast_path=true', 'true'), ('echo fast_path=false', 'false'),
                                 ('exit 3', 'false'), ('echo nonsense', 'false')):
            with self.subTest(script=script), tempfile.TemporaryDirectory() as tmp:
                tmp = Path(tmp)
                (tmp / 'scripts').mkdir()
                fake = tmp / 'scripts' / 'release-pr-fast-path.sh'
                fake.write_text('#!/usr/bin/env bash\n' + script + '\n')
                fake.chmod(0o755)
                out = tmp / 'out'
                subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', body], cwd=tmp, check=True,
                               env={'PATH': os.environ['PATH'], 'GITHUB_OUTPUT': str(out), 'BASE_SHA': 'a' * 40,
                                    **{k: v for k, v in os.environ.items() if k == 'LD_LIBRARY_PATH'}})
                self.assertEqual(out.read_text(), f'release_push={expected}\n')

    def test_release_and_warming_use_identical_setup_and_targets(self):
        source = yaml.safe_load((ROOT / '.github/dist-build-setup.yml').read_text())
        daemon = workflow('v-release.yml')['jobs']['build-local-artifacts']['steps']
        start = next(i for i, s in enumerate(daemon) if s.get('name') == source[0]['name'])
        self.assertEqual(daemon[start:start + len(source)], source)
        sitter = workflow('release-sitter.yml')['jobs']['build']
        warm = workflow('release-cache.yml')['jobs']
        expected = deepcopy(sitter['strategy'])
        expected['matrix']['include'] = [row for row in expected['matrix']['include']
                                        if row['target'] != 'x86_64-apple-darwin']
        self.assertEqual(expected, warm['sitter']['strategy'])
        self.assertIn('optional-intel-release.py matrix cache-plan.json --event cache --ref refs/heads/main',
                      warm['plan']['steps'][-1]['run'])
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
