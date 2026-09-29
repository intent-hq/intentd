#!/usr/bin/env python3
"""Offline release cache controls and producer contracts (requires PyYAML)."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[1]


def module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'scripts' / f'{name}.py')
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


class CacheControls(unittest.TestCase):
    def test_bound_keeps_complete_entries_and_never_follows_symlinks(self):
        cache = module('release-cache-controls')
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / 'cache'
            root.mkdir()
            outside = Path(temp) / 'outside'
            outside.write_bytes(b'untouched')
            (root / 'old').write_bytes(b'a' * 7)
            (root / 'new').write_bytes(b'b' * 5)
            os.utime(root / 'old', (1, 1))
            os.utime(root / 'new', (2, 2))
            (root / 'link').symlink_to(outside)
            result = cache.bound(root, 6)
            self.assertEqual(result, {'before_bytes': 12, 'after_bytes': 5, 'removed_entries': 1})
            self.assertEqual((root / 'new').read_bytes(), b'b' * 5)
            self.assertFalse((root / 'link').exists())
            self.assertEqual(outside.read_bytes(), b'untouched')

    def test_empty_and_oversized_entry(self):
        cache = module('release-cache-controls')
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.assertEqual(cache.bound(root, 1)['after_bytes'], 0)
            (root / 'huge').write_bytes(b'123')
            self.assertEqual(cache.bound(root, 1)['after_bytes'], 0)

    def test_retention_only_deletes_older_owned_main_generations(self):
        cache = module('release-cache-controls')
        slot = 'intentd-release-v1-daemon-dist-aarch64-apple-darwin-macos-14--'
        rows = [
            {'id': 1, 'key': slot + 'old', 'ref': 'refs/heads/main', 'created_at': '2026-01-01'},
            {'id': 2, 'key': slot + 'new', 'ref': 'refs/heads/main', 'created_at': '2026-02-01'},
            {'id': 3, 'key': slot + 'branch', 'ref': 'refs/heads/feature', 'created_at': '2026-03-01'},
            {'id': 4, 'key': 'v0-rust-release-plz-pr-x', 'ref': 'refs/heads/main', 'created_at': '2026-01-01'},
            {'id': 5, 'key': 'intentd-release-v1-unknown--old', 'ref': 'refs/heads/main', 'created_at': '2026-01-01'},
        ]
        self.assertEqual(cache.obsolete(rows), [1])


class ProducerContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = yaml.safe_load((ROOT / '.github/workflows/release-cache.yml').read_text())

    def test_main_only_events_and_permissions(self):
        w = self.workflow
        events = w['on'] if 'on' in w else w[True]
        self.assertEqual(set(events), {'push', 'workflow_dispatch'})
        self.assertEqual(events['push']['branches'], ['main'])
        self.assertNotIn('crates/**', events['push']['paths'])
        self.assertEqual(w['permissions'], {'contents': 'read'})
        guard = module('configure-release-cache-producer').MAIN_ONLY
        for event, ref, expected in [('push', 'refs/heads/main', True), ('workflow_dispatch', 'refs/heads/main', True), ('pull_request', 'refs/heads/main', False), ('push', 'refs/tags/v1.0.0', False), ('workflow_dispatch', 'refs/heads/topic', False)]:
            expression = guard.replace('github.event_name', repr(event)).replace('github.ref', repr(ref)).replace('&&', 'and').replace('||', 'or')
            self.assertEqual(eval(expression, {'__builtins__': {}}), expected)
        for name, job in w['jobs'].items():
            self.assertIn(guard, job['if'])
            self.assertEqual(job.get('permissions', {}), {'actions': 'write', 'contents': 'read'} if name == 'retention' else {})
        text = (ROOT / '.github/workflows/release-cache.yml').read_text()
        for forbidden in ('gh release', 'dist host', 'dist publish', 'git push', 'repository_dispatch', 'secrets.GH_RELEASES_TOKEN'):
            self.assertNotIn(forbidden, text)

    def test_matrix_and_build_inputs_follow_release_sources(self):
        import tomllib
        config = tomllib.loads((ROOT / 'dist-workspace.toml').read_text())['dist']
        targets = set(config['targets'])
        sitter = yaml.safe_load((ROOT / '.github/workflows/release-sitter.yml').read_text())['jobs']['build']
        self.assertEqual({r['target'] for r in sitter['strategy']['matrix']['include']}, targets)
        self.assertEqual(self.workflow['jobs']['sitter']['strategy'], sitter['strategy'])
        self.assertIn('artifacts_matrix', self.workflow['jobs']['plan']['steps'][-1]['run'])
        self.assertEqual(self.workflow['jobs']['daemon']['strategy']['matrix'], '${{ fromJson(needs.plan.outputs.matrix) }}')
        for job_name in ('daemon', 'sitter'):
            steps = self.workflow['jobs'][job_name]['steps']
            restore = next(i for i, s in enumerate(steps) if s.get('uses') == './.github/actions/release-rust-cache')
            provision = next(i for i, s in enumerate(steps) if s.get('name', '').startswith('Provision Rust'))
            self.assertLess(provision, restore)
            build = next(i for i, s in enumerate(steps) if s.get('name') == 'Build cache inputs')
            trim = next(i for i, s in enumerate(steps) if s.get('name') == 'Bound cache and record measurements')
            save = next(i for i, s in enumerate(steps) if s.get('uses', '').startswith('actions/cache/save@'))
            self.assertLess(restore, build)
            self.assertLess(build, trim)
            self.assertLess(trim, save)
            self.assertIn('--limit 268435456', steps[trim]['run'])

    def test_generation_is_reproducible(self):
        subprocess.run(['python3', '-B', 'scripts/configure-release-cache-producer.py', '--check'], cwd=ROOT, check=True)

    def test_executed_keys_separate_slots_and_preserve_dependency_fallback(self):
        action = yaml.safe_load((ROOT / '.github/actions/release-rust-cache/action.yml').read_text())
        command = action['runs']['steps'][0]['run']
        controls = module('release-cache-controls')
        keys = set()
        with tempfile.TemporaryDirectory() as temp:
            def key(flavor, target, runner, toolchain='rust1', inputs='deps1'):
                output = Path(temp) / 'output'
                environment = Path(temp) / 'environment'
                output.write_text('')
                environment.write_text('')
                env = {**os.environ, 'CACHE_FLAVOR': flavor, 'CACHE_TARGET': target,
                       'CACHE_RUNNER': runner, 'TOOLCHAIN': toolchain, 'BUILD_INPUTS': inputs,
                       'RUNNER_TEMP': temp, 'GITHUB_OUTPUT': str(output), 'GITHUB_ENV': str(environment)}
                subprocess.run(['bash', '-e', '-c', command], env=env, check=True)
                return dict(line.split('=', 1) for line in output.read_text().splitlines())
            for index, flavor in enumerate(('daemon-dist', 'sitter-release')):
                for target, runners in controls.TARGET_RUNNERS.items():
                    result = key(flavor, target, runners[index])
                    keys.add(result['key'])
                    changed_deps = key(flavor, target, runners[index], inputs='deps2')
                    self.assertNotEqual(result['key'], changed_deps['key'])
                    self.assertEqual(result['prefix'], changed_deps['prefix'])
                    changed_rust = key(flavor, target, runners[index], toolchain='rust2')
                    self.assertNotEqual(result['prefix'], changed_rust['prefix'])
                    self.assertEqual(result['directory'], str(Path(temp) / 'intentd-release-sccache'))
            self.assertEqual(len(keys), 10)
            with self.assertRaises(subprocess.CalledProcessError):
                key('unknown-profile', 'x86_64-apple-darwin', 'macos-14')

    def test_restore_contract_and_profile_separation(self):
        action = yaml.safe_load((ROOT / '.github/actions/release-rust-cache/action.yml').read_text())
        text = json.dumps(action)
        self.assertNotIn('actions/cache/save', text)
        self.assertNotIn('target/', text)
        self.assertIn('inputs.flavor', text)
        self.assertIn('inputs.target', text)
        self.assertIn('inputs.runner', text)
        self.assertIn('rust-toolchain.toml', text)
        self.assertIn('SCCACHE_CACHE_SIZE=256M', text)
        self.assertIn('RUSTC_WRAPPER=sccache', text)
        flavors = [next(s for s in self.workflow['jobs'][j]['steps'] if s.get('uses') == './.github/actions/release-rust-cache')['with']['flavor'] for j in ('daemon', 'sitter')]
        self.assertEqual(flavors, ['daemon-dist', 'sitter-release'])


if __name__ == '__main__':
    unittest.main()
