#!/usr/bin/env python3
"""Offline release cache controls and producer contracts (requires PyYAML)."""
import importlib.util
import hashlib
import re
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

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
        # A refresh failure adds no completed archive to the listing. Retention
        # keeps the sole usable main generation instead of deleting to refresh.
        self.assertEqual(cache.obsolete([rows[0], *rows[2:]]), [])
        fresh = {**rows[1], 'id': 6, 'key': slot + 'new-inputs-101-2', 'created_at': '2026-04-01'}
        self.assertEqual(cache.obsolete([*rows, fresh]), [1, 2])


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
            build = next(i for i, s in enumerate(steps) if s.get('name') == ('Build cache inputs' if job_name == 'daemon' else 'Build (cargo)'))
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

    def test_non_manifest_inputs_refresh_generation_identity(self):
        action = yaml.safe_load((ROOT / '.github/actions/release-rust-cache/action.yml').read_text())
        patterns = re.findall(r"'([^']+)'", action['runs']['steps'][0]['env']['BUILD_INPUTS'])
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            files = ('Cargo.lock', 'Cargo.toml', 'crates/example/build.rs',
                     '.github/dist-build-setup.yml', '.github/actions/release-rust-cache/action.yml',
                     '.github/workflows/release-cache.yml', '.github/workflows/release-sitter.yml',
                     'scripts/release-cache-controls.py', 'scripts/configure-release-cache-producer.py',
                     'crates/example/native.c', 'crates/example/native.h')
            for name in files:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('original')
            def digest():
                selected = sorted({path for pattern in patterns for path in root.glob(pattern) if path.is_file()})
                return hashlib.sha256(b''.join(path.read_bytes() for path in selected)).hexdigest()
            for name in files:
                with self.subTest(input=name):
                    before = digest()
                    (root / name).write_text('changed')
                    self.assertNotEqual(before, digest())
                    (root / name).write_text('original')

    def test_manual_refresh_generations_remain_discoverable(self):
        action = yaml.safe_load((ROOT / '.github/actions/release-rust-cache/action.yml').read_text())
        command = action['runs']['steps'][0]['run']
        with tempfile.TemporaryDirectory() as temp:
            def key(generation):
                output = Path(temp) / 'output'
                output.write_text('')
                env = {**os.environ, 'CACHE_FLAVOR': 'daemon-dist', 'CACHE_TARGET': 'x86_64-unknown-linux-musl',
                       'CACHE_RUNNER': 'gh-linux-16x', 'TOOLCHAIN': 'rust1', 'BUILD_INPUTS': 'same-inputs',
                       'CACHE_GENERATION': generation, 'RUNNER_TEMP': temp,
                       'GITHUB_OUTPUT': str(output), 'GITHUB_ENV': str(Path(temp) / 'env')}
                subprocess.run(['bash', '-eu', '-c', command], env=env, check=True)
                return dict(line.split('=', 1) for line in output.read_text().splitlines())
            old, fresh, retried, consumer = (key(g) for g in ('100-1', '101-1', '101-2', ''))
            self.assertEqual(len({r['key'] for r in (old, fresh, retried, consumer)}), 4)
            self.assertEqual(consumer['prefix'], fresh['prefix'])
            self.assertTrue(fresh['key'].startswith(consumer['input-prefix']))
            # Read-only lookups must never exactly match an older saved key,
            # which would take precedence over the newest prefix generation.
            self.assertFalse(any(consumer['key'] == r['key'] for r in (old, fresh, retried)))
            restore_config = action['runs']['steps'][-1]['with']
            resolve = lambda expression: consumer[re.fullmatch(r'\$\{\{ steps.key.outputs.([\w-]+) \}\}', expression).group(1)]
            lookup_order = [resolve(restore_config['key']),
                            *(resolve(line) for line in restore_config['restore-keys'].splitlines())]
            legacy = consumer['input-prefix'].removesuffix('-')

            def restored(archives):
                # Model GitHub lookup order with fixtures oldest-to-newest:
                # exact match, then newest prefix match for each restore key.
                for lookup in lookup_order:
                    if lookup in archives:
                        return lookup
                    matches = [archive for archive in archives if archive.startswith(lookup)]
                    if matches:
                        return matches[-1]
                return None

            self.assertEqual(restored([legacy, fresh['key']]), fresh['key'])
            self.assertEqual(restored([legacy]), legacy)  # refresh save failed
            self.assertIsNone(restored([]))  # ordinary eviction/cold miss
            for job in ('daemon', 'sitter'):
                restore = next(s for s in self.workflow['jobs'][job]['steps'] if s.get('uses') == './.github/actions/release-rust-cache')
                self.assertEqual(restore['with']['generation'], '${{ github.run_id }}-${{ github.run_attempt }}')

    def test_sitter_build_mutations_propagate_to_generated_producer(self):
        generator = module('configure-release-cache-producer')
        original = yaml.safe_load
        before = generator.generate()
        for change in ('command', 'zig-command', 'condition', 'step-env', 'job-env'):
            def edited_load(value):
                doc = original(value)
                if isinstance(doc, dict) and 'build' in doc.get('jobs', {}):
                    job = doc['jobs']['build']
                    step = next(s for s in job['steps'] if s.get('name') == ('Build (cargo-zigbuild)' if change == 'zig-command' else 'Build (cargo)'))
                    if change in ('command', 'zig-command'):
                        step['run'] += ' --features regression_flag'
                    elif change == 'condition':
                        step['if'] = '${{ matrix.zigbuild }}'
                    elif change == 'step-env':
                        step['env'] = {'RUSTFLAGS': '-C opt-level=1'}
                    else:
                        job['env']['RUSTFLAGS'] = '-C opt-level=1'
                return doc
            with self.subTest(change=change), patch.object(generator.yaml, 'safe_load', edited_load):
                changed = generator.generate()
                self.assertTrue(before != changed, "sitter build change did not affect generation")
                output = original(changed)['jobs']['sitter']
                self.assertFalse(any(s.get('name', '').startswith('Package') for s in output['steps']))
                expected = edited_load((ROOT / '.github/workflows/release-sitter.yml').read_text())['jobs']['build']
                self.assertEqual(output['env'], expected['env'])
                for source in (s for s in expected['steps'] if s.get('name', '').startswith('Build (')):
                    emitted = next(s for s in output['steps'] if s.get('name') == source['name'])
                    for field in ('if', 'env', 'shell'):
                        self.assertEqual(emitted.get(field), source.get(field))
                    self.assertIn(source['run'], emitted['run'])

    def test_producer_suppresses_post_stop_zero_statistics(self):
        action = yaml.safe_load((ROOT / '.github/actions/release-rust-cache/action.yml').read_text())
        install = next(s for s in action['runs']['steps'] if s.get('uses', '').startswith('mozilla-actions/'))
        self.assertEqual(install['with']['disable_annotations'], '${{ inputs.disable-annotations }}')
        for job in ('daemon', 'sitter'):
            restore = next(s for s in self.workflow['jobs'][job]['steps'] if s.get('uses') == './.github/actions/release-rust-cache')
            self.assertEqual(restore['with']['disable-annotations'], 'true')

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
