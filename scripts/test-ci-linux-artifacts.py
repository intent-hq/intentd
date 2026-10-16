#!/usr/bin/env python3
"""Linux demand, gate mutations and archive-only callback execution contracts."""
import copy
import hashlib
import io
import tarfile
import importlib.util
import json
import os
import re
from pathlib import Path
import subprocess
import shutil
import tempfile
import unittest
from unittest import mock

import yaml


def load(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


linux = load('ci-linux-artifacts')
routing = load('test-ci-native-routing')
ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = yaml.safe_load((ROOT / '.github/workflows/ci.yml').read_text())


class LinuxTests(unittest.TestCase):
    def test_producer_rejects_capacity_before_download_or_build(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with mock.patch.dict(os.environ, GITHUB_REPOSITORY='intent-hq/intentd'), \
                 mock.patch.object(linux, 'select', return_value=(self.plan([]), False)), \
                 mock.patch.object(linux.native, 'platform_facts', return_value={'platform': {'target': 'host'}, 'native_tools': {}}), \
                 mock.patch.object(linux, 'recipes', return_value={}), \
                 mock.patch.object(linux.storage, 'producer_capacity', side_effect=ValueError('insufficient producer storage')) as capacity, \
                 mock.patch.object(linux.registry, 'GitHub') as api, \
                 mock.patch.object(linux.registry, 'context', return_value={}), \
                 mock.patch.object(linux.registry, 'publication', return_value={}), \
                 mock.patch.object(linux.registry, 'restore') as restore, \
                 mock.patch.object(linux.archives, 'produce', return_value=[]) as build, \
                 mock.patch.object(linux, 'compiler_gates'):
                with self.assertRaisesRegex(ValueError, 'producer storage'):
                    linux.produce(ROOT, root, 'merge_group', None)
                capacity.assert_called_once()
                api.assert_not_called(); restore.assert_not_called(); build.assert_not_called()

    def test_full_and_changed_scopes_use_bounded_profile_pool_in_fresh_extractions(self):
        for scope, name in (('coverage-all', 'coverage-all'), ('changed', 'changed-0')):
            with self.subTest(scope=scope), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / 'requests.json').write_text(json.dumps({name: {}}))
                item = {'target': str(root / 'restored' / name / 'target'), 'extract_seconds': 0}
                with mock.patch.object(linux, 'prepare', return_value={name: item}), \
                     mock.patch.object(linux.coverage, 'report_environment', return_value={}), \
                     mock.patch.object(linux, 'compiler_exclusion', return_value=None), \
                     mock.patch.object(linux.archives, 'run_nextest', return_value=100) as run:
                    with self.assertRaises(subprocess.CalledProcessError):
                        linux.run_coverage(ROOT, root, scope)
                    pattern = run.call_args.kwargs['env']['LLVM_PROFILE_FILE']
                    self.assertEqual(pattern, str(root / 'restored' / name / 'intent-%8m.profraw'))

    def test_real_source_lint_root_uses_relocated_checkout_not_producer_decoy(self):
        with tempfile.TemporaryDirectory(prefix='intent-lint-relocation-') as tmp:
            root = Path(tmp)
            producer = root / 'producer/crates/intentd-test-support'
            consumer = root / 'consumer/crates/intent-core'
            producer.mkdir(parents=True)
            consumer.mkdir(parents=True)
            source = root / 'main.rs'
            module = ROOT / 'crates/intentd-test-support/src'
            source.write_text('#![allow(dead_code)]\n' +
                '#[path=' + json.dumps(str(module / 'source_lint.rs')) + '] mod source_lint;\n' +
                '#[path=' + json.dumps(str(module / 'runtime_paths.rs')) + '] mod runtime_paths;\n' +
                'fn main() { assert_eq!(source_lint::workspace_root(), std::path::PathBuf::from(std::env::var("EXPECTED_WORKSPACE").unwrap())); }\n')
            executable = root / 'lint-proof'
            subprocess.run(['rustc', '--edition=2021', str(source), '-o', str(executable)],
                           env=dict(os.environ, CARGO_MANIFEST_DIR=str(producer)), check=True, capture_output=True)
            env = dict(os.environ, CARGO_MANIFEST_DIR=str(consumer), EXPECTED_WORKSPACE=str(root / 'consumer'))
            # An existing producer is a decoy, not an acceptable fallback.
            for exists in (True, False):
                if not exists:
                    shutil.rmtree(root / 'producer')
                result = subprocess.run([str(executable)], env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_note_index_inputs_choose_runtime_checkout_before_collecting(self):
        # Compile the real_inputs call site and the actual runtime-path helper.
        # A filesystem-reading collector isolates this boundary; the Rust
        # note_index_profile suite separately verifies the real fingerprint.
        text = (ROOT / 'crates/intent-store/src/tests/note_index_profile.rs').read_text()
        function = text.split('fn real_inputs() -> deps::Inputs {', 1)[1].split('\n}', 1)[0]
        with tempfile.TemporaryDirectory(prefix='intent-index-relocation-') as tmp:
            root = Path(tmp)
            producer = root / 'producer/crates/intent-store'
            consumer = root / 'consumer/crates/intent-store'
            for directory, marker in ((producer, 'producer-decoy'), (consumer, 'consumer-lock')):
                directory.mkdir(parents=True)
                (directory.parent.parent / 'Cargo.lock').write_text(marker)
                (directory / 'Cargo.toml').write_text('[package]\nname="intent-store"\nversion="0.0.0"\n')
            source = root / 'main.rs'
            source.write_text('#![allow(dead_code, unused_imports)]\nextern crate self as intentd_test_support;\n' +
                '#[path=' + json.dumps(str(ROOT / 'crates/intentd-test-support/src/runtime_paths.rs')) + '] pub mod runtime_paths;\n' +
                'use std::path::Path;\nmod deps { pub type Inputs = String; pub fn collect(path: &std::path::Path) -> Result<Inputs, String> {\n' +
                'assert!(path.join("Cargo.toml").is_file());\n' +
                'let lock = path.ancestors().map(|p| p.join("Cargo.lock")).find(|p| p.is_file()).ok_or("missing lock")?;\n' +
                'std::fs::read_to_string(lock).map_err(|e| e.to_string()) } }\n' +
                'fn real_inputs() -> deps::Inputs {' + function + '\n}\n' +
                'fn main() { assert_eq!(real_inputs(), std::env::var("EXPECTED_LOCK").unwrap()); }\n')
            binary = root / 'inputs-proof'
            subprocess.run(['rustc', '--edition=2021', str(source), '-o', str(binary)],
                           env=dict(os.environ, CARGO_MANIFEST_DIR=str(producer)), check=True, capture_output=True)
            env = dict(os.environ, CARGO_MANIFEST_DIR=str(consumer), EXPECTED_LOCK='consumer-lock')
            for exists in (True, False):
                with self.subTest(producer_exists=exists):
                    if not exists:
                        shutil.rmtree(root / 'producer')
                    result = subprocess.run([str(binary)], env=env, capture_output=True, text=True)
                    self.assertEqual(result.returncode, 0, result.stderr)
            invalid = subprocess.run([str(binary)], env={**env, 'CARGO_MANIFEST_DIR': str(root / 'missing')},
                                     capture_output=True, text=True)
            self.assertNotEqual(invalid.returncode, 0)
            producer.mkdir(parents=True)
            (producer / 'Cargo.toml').write_text('[package]\nname="intent-store"\nversion="0.0.0"\n')
            (producer.parent.parent / 'Cargo.lock').write_text('normal-cargo')
            fallback_env = dict(os.environ, EXPECTED_LOCK='normal-cargo')
            fallback_env.pop('CARGO_MANIFEST_DIR', None)
            fallback = subprocess.run([str(binary)], env=fallback_env, capture_output=True, text=True)
            self.assertEqual(fallback.returncode, 0, fallback.stderr)

    def test_startup_cli_commands_use_relocated_binary_not_producer_decoy(self):
        text = (ROOT / 'crates/intentd/tests/wss_integration/startup.rs').read_text()
        expressions = re.findall(r'(?:std|tokio)::process::Command::new\((.*?)\)\s*(?:;|\.args?\()', text, re.S)
        expressions = [expression for expression in expressions if 'CARGO_BIN_EXE_intentd' in expression]
        self.assertEqual(len(expressions), 3, 'all active startup CLI launch sites must be covered')
        with tempfile.TemporaryDirectory(prefix='intent-startup-relocation-') as tmp:
            root = Path(tmp)
            producer, consumer = root / 'producer', root / 'consumer'
            for binary, value in ((producer, 'decoy'), (consumer, 'consumer')):
                binary.write_text('#!/bin/sh\nprintf ' + value)
                binary.chmod(0o755)
            source = root / 'main.rs'
            source.write_text('#![allow(dead_code)]\nextern crate self as intentd_test_support;\n' +
                '#[path=' + json.dumps(str(ROOT / 'crates/intentd-test-support/src/runtime_paths.rs')) + '] pub mod runtime_paths;\n' +
                'fn main() {\n' + ''.join('let result = std::process::Command::new(' + expression +
                ').output().unwrap(); assert_eq!(String::from_utf8(result.stdout).unwrap(), std::env::var("EXPECTED_OUTPUT").unwrap());\n'
                for expression in expressions) + '}\n')
            proof = root / 'proof'
            subprocess.run(['rustc', '--edition=2021', str(source), '-o', str(proof)],
                           env=dict(os.environ, CARGO_BIN_EXE_intentd=str(producer)), check=True, capture_output=True)
            env = dict(os.environ, NEXTEST_BIN_EXE_intentd=str(consumer), EXPECTED_OUTPUT='consumer')
            for exists in (True, False):
                with self.subTest(producer_exists=exists):
                    if not exists:
                        producer.unlink()
                    run = subprocess.run([str(proof)], env=env, capture_output=True, text=True)
                    self.assertEqual(run.returncode, 0, run.stderr)
            invalid = subprocess.run([str(proof)], env={**env, 'NEXTEST_BIN_EXE_intentd': str(root / 'missing')},
                                     capture_output=True, text=True)
            self.assertNotEqual(invalid.returncode, 0)

    def plan(self, args):
        return {'version': 1, 'head': 'a'*40, 'mergeBase': 'b'*40, 'plans': args}

    def test_narrow_changed_selection_never_builds_full_coverage(self):
        selected = ['-p', 'intent-core', '--test', 'clock']
        variants = linux.recipes('x86_64-unknown-linux-gnu', 'pull_request', self.plan([selected]))
        instrumented = {name: recipe for name, (recipe, mode) in variants.items() if mode}
        self.assertEqual(list(instrumented), ['changed-0'])
        command = instrumented['changed-0']['command']
        for part in selected:
            self.assertIn(part, command)
        self.assertNotIn('--workspace', command)
        self.assertNotIn('--target', command)
        self.assertFalse(any(mode for recipe, mode in linux.recipes('host', 'pull_request', self.plan([])).values()))

    def test_full_graphs_and_callback_scope_remain_distinct(self):
        plan = self.plan([['-p', 'intent-core', '-p', 'intent-services', '--lib', '--bins', '--tests']])
        variants = linux.recipes('host', 'pull_request', plan, callback_changed=True, full_proof=True)
        self.assertIn('--workspace', variants['coverage-all'][0]['command'])
        self.assertNotIn('--workspace', variants['coverage-e2e'][0]['command'])
        self.assertIn('intentd', variants['coverage-e2e'][0]['command'])
        self.assertIn('check-services', variants)
        self.assertNotIn('check-gitlab', variants)
        self.assertIn('check-gitlab', linux.recipes('host', 'merge_group', self.plan([])))
        self.assertIn('changed-0', variants)
        for part in plan['plans'][0] + ['-p', 'intent-acp']:
            self.assertIn(part, variants['callback'][0]['command'])
        for malformed in ([['--workspace']], [['-p', 'intent-core', '--features', 'other']], [['--lib']]):
            with self.assertRaises(ValueError):
                linux.recipes('host', 'pull_request', self.plan(malformed))

    def test_pipeline_paths_request_full_proof_but_ordinary_changes_do_not(self):
        for path in linux.FULL_PROOF_PATHS:
            self.assertTrue((ROOT / path).is_file(), path)
            self.assertTrue(linux.full_proof([path]))
        for path in ('crates/intent-core/src/config.rs', 'docs/example.md', 'scripts/unrelated.py'):
            self.assertFalse(linux.full_proof([path]))
        self.assertTrue(linux.full_proof(None))

    def test_queue_requires_all_reports_and_changed_execution_but_pr_is_static(self):
        baseline = {name: 'success' for name in WORKFLOW['jobs']['gate']['needs']}
        self.assertTrue(routing.gate_passes(WORKFLOW, 'merge_group', baseline))
        for name in ('coverage-e2e', 'coverage-all', 'coverage-changed'):
            for result in ('skipped', 'failure', 'cancelled', ''):
                self.assertFalse(routing.gate_passes(WORKFLOW, 'merge_group', {**baseline, name: result}), (name, result))
        baseline.update({name: 'skipped' for name in ('coverage-e2e', 'coverage-all', 'coverage-changed', 'check', 'daemon-lifecycle')})
        self.assertTrue(routing.gate_passes(WORKFLOW, 'pull_request', baseline))

    def test_queue_producer_and_consumer_resolve_identical_event_base(self):
        jobs = WORKFLOW['jobs']
        bodies = []
        for name in ('check', 'coverage-changed'):
            job = jobs[name]
            self.assertEqual(job['env']['QUEUE_BASE_SHA'], '${{ github.event.merge_group.base_sha }}')
            self.assertEqual(job['env']['QUEUE_HEAD_SHA'], '${{ github.event.merge_group.head_sha }}')
            bodies.append(next(s['run'] for s in job['steps'] if s.get('name') == 'Resolve queue base from the event'))
        self.assertEqual(*bodies)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan = self.plan([['-p', 'intent-core', '--lib', '--bins', '--tests']])
            def selection(command, **kwargs):
                self.assertEqual(command[command.index('--base') + 1], 'b'*40)
                (root / 'changed-plan.json').write_text(json.dumps(plan))
            with mock.patch.object(linux.archives, 'source_commit', return_value='a'*40), \
                 mock.patch.object(linux.subprocess, 'run', side_effect=selection) as run, \
                 mock.patch.object(linux.archives, 'output', return_value='scripts/changed-tests.sh'):
                selected, changed = linux.select(ROOT, root, 'merge_group', 'b'*40)
                run.assert_called_once()
                self.assertEqual(selected, plan)
                self.assertTrue(changed)
                with self.assertRaisesRegex(ValueError, 'connected base'):
                    linux.select(ROOT, root, 'merge_group', None)
            recipes = linux.recipes('host', 'merge_group', selected, callback_changed=changed)
            self.assertTrue({'coverage-all', 'coverage-e2e', 'changed-0', 'callback', 'check-gitlab'} <= recipes.keys())
            for part in ('-p', 'intent-acp', 'intent-services'):
                self.assertIn(part, recipes['callback'][0]['command'])

    def test_one_linux_producer_and_no_coverage_build_commands(self):
        check = WORKFLOW['jobs']['check']
        producers = [s for s in check['steps'] if 'ci-linux-artifacts.py produce' in s.get('run', '')]
        self.assertEqual(len(producers), 1)
        self.assertEqual(check['concurrency']['group'], 'native-producer-v2-${{ github.repository_id }}-${{ github.sha }}-linux')
        names = [s.get('name') for s in check['steps']]
        publish = names.index('Publish Linux variants before checks and tests')
        self.assertLess(publish, names.index('Linux lifecycle runtime proof'))
        self.assertLess(publish, names.index('Clippy'))
        for job in ('coverage-e2e', 'coverage-all', 'coverage-changed'):
            self.assertIn('check', WORKFLOW['jobs'][job]['needs'])
            for step in WORKFLOW['jobs'][job]['steps']:
                run = step.get('run', '')
                self.assertNotIn('cargo llvm-cov --no-report nextest', run.replace('printf', 'echo') if step.get('name') != 'Select changed tests (dry run)' else '')
                self.assertNotIn('cargo nextest list', run)
                self.assertNotIn('Swatinem/rust-cache', step.get('uses', ''))

    def test_compile_fail_gate_runs_fresh_after_every_production_including_hits(self):
        request = {'recipe': {'coverage': {}}, 'identity': {'source': {'commit': 'a' * 40}}}
        binary = 'intent-store::store_pool_api'
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            item = {'runtime': {'tests': {binary: {}}}}
            with mock.patch.object(linux, 'select', return_value=(self.plan([]), False)), \
                 mock.patch.object(linux.native, 'platform_facts', return_value={'platform': {'target': 'host'}, 'native_tools': {}}), \
                 mock.patch.object(linux.native, 'config_digest', return_value='config'), \
                 mock.patch.object(linux, 'recipes', return_value={'coverage-all': ({}, True)}), \
                 mock.patch.object(linux.archives, 'capture_request', return_value=request), \
                 mock.patch.object(linux.storage, 'producer_capacity'), \
                 mock.patch.object(linux.archives, 'produce', side_effect=[[{'reused': False}], [{'reused': True}], [{'reused': True}]]) as produce, \
                 mock.patch.object(linux.archives, 'source_commit', return_value='a' * 40), \
                 mock.patch.object(linux.archives.identity, 'build_key', return_value='key'), \
                 mock.patch.object(linux.archives, 'verify_bundle', return_value=item), \
                 mock.patch.object(linux, 'prepare_streamed', return_value=item), \
                 mock.patch.object(linux.coverage, 'capture', return_value={}), \
                 mock.patch.object(linux.coverage, 'build_environment', return_value={}) as environment, \
                 mock.patch.object(linux, 'compiler_inventory', return_value=linux.COMPILER_TESTS), \
                 mock.patch.object(linux.archives, 'run_libtest', side_effect=[0, 0, 41]) as run:
                for expected in ({'reused': False}, {'reused': True}):
                    self.assertEqual(linux.produce(ROOT, root, 'merge_group', None, reuse=False), [expected])
                    with self.assertRaisesRegex(ValueError, 'authenticated producer prerequisite'):
                        linux.compiler_exclusion(root, 'coverage-all', request, item)
                with self.assertRaises(subprocess.CalledProcessError):
                    linux.produce(ROOT, root, 'merge_group', None, reuse=False)
                self.assertEqual(produce.call_count, 3)
                self.assertEqual(run.call_count, 3)
                self.assertFalse(environment.call_args.kwargs['remap'])
                self.assertEqual(run.call_args.args[1:], (binary, []))
                self.assertIn('compiler-gate-', run.call_args.kwargs['env']['LLVM_PROFILE_FILE'])
                with self.assertRaises(ValueError):
                    linux.compiler_exclusion(root, 'coverage-all', request, item)

    def test_compile_fail_exclusion_requires_exact_successful_current_source_and_variant(self):
        request = {'identity': {'source': {'commit': 'a' * 40}}}
        binary = 'intent-store::store_pool_api'
        item = {'runtime': {'tests': {binary: {}}}}
        receipt = {'source': 'a' * 40, 'gates': {'changed-0': {'key': 'key', 'binary': binary, 'tests': linux.COMPILER_TESTS, 'exit_code': 0}}}
        mutations = [lambda r: r.update(source='b' * 40), lambda r: r['gates'].clear(),
                     lambda r: r['gates']['changed-0'].update(key='other'),
                     lambda r: r['gates']['changed-0'].update(binary='other'),
                     lambda r: r['gates']['changed-0'].update(exit_code=1),
                     lambda r: r['gates']['changed-0'].update(tests=[])]
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(linux.archives.identity, 'build_key', return_value='key'):
            root = Path(tmp)
            for mutate in mutations:
                invalid = copy.deepcopy(receipt)
                mutate(invalid)
                (root / 'compiler-gates.json').write_text(json.dumps(invalid))
                with self.assertRaises(ValueError):
                    linux.compiler_exclusion(root, 'changed-0', request, item)
            with mock.patch.object(linux.archives, 'source_commit', return_value='a' * 40), \
                 mock.patch.object(linux.archives, 'verify_bundle', return_value={'runtime': {'tests': {'intent-core::clock': {}}}}), \
                 mock.patch.object(linux.archives, 'run_libtest') as run:
                linux.compiler_gates({'changed-0': request, 'callback': request}, ROOT, root, root)
                run.assert_not_called()
                self.assertEqual(json.loads((root / 'compiler-gates.json').read_text())['gates'], {})
                self.assertIsNone(linux.compiler_exclusion(root, 'changed-0', request, {'runtime': {'tests': {}}}))

    def test_failed_runtime_keeps_timing_and_truthful_compiler_receipt(self):
        for compiler_attempt in (False, True):
            with self.subTest(compiler_attempt=compiler_attempt), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / 'requests.json').write_text(json.dumps({'changed-0': {}}))
                item = {'target': str(root / 'restored/changed-0/target'), 'extract_seconds': 0.3}
                if compiler_attempt:
                    (root / 'compiler-calls.log').write_text('["cargo", "build"]\n')
                with mock.patch.object(linux, 'prepare', return_value={'changed-0': item}), \
                     mock.patch.object(linux.coverage, 'report_environment', return_value={}), \
                     mock.patch.object(linux, 'compiler_exclusion', return_value=None), \
                     mock.patch.object(linux.archives, 'run_nextest', return_value=100):
                    with self.assertRaises(ValueError if compiler_attempt else subprocess.CalledProcessError):
                        linux.run_coverage(ROOT, root, 'changed')
                    receipt = json.loads((root / 'receipt-changed.json').read_text())
                    self.assertEqual(receipt['compiler_calls'], int(compiler_attempt))
                    self.assertEqual(receipt['variants']['changed-0']['test_exit_code'], 100)
                    self.assertGreaterEqual(receipt['variants']['changed-0']['test_seconds'], 0)

    def test_compiler_gate_accounts_for_exact_test_inventory(self):
        item = {'source': str(ROOT), 'target': '/tmp/archive-target',
                'runtime': {'tests': {'intent-store::store_pool_api': {'cwd': 'crates/intent-store', 'path': 'gate'}}}}
        for listing in ('pool_escape_apis_are_rejected: test\n', '', 'new_test: test\n',
                        'pool_escape_apis_are_rejected: test\nnew_test: test\n'):
            with mock.patch.object(linux.archives, 'output', return_value=listing), \
                 mock.patch.object(linux.archives, 'runtime_env', side_effect=lambda item, env: env):
                if listing == 'pool_escape_apis_are_rejected: test\n':
                    self.assertEqual(linux.compiler_inventory(item, 'intent-store::store_pool_api', {}), linux.COMPILER_TESTS)
                else:
                    with self.assertRaises(ValueError):
                        linux.compiler_inventory(item, 'intent-store::store_pool_api', {})

    def test_narrow_compiler_only_demand_is_accounted_for_without_empty_runtime_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'requests.json').write_text(json.dumps({'changed-0': {}}))
            item = {'target': str(root / 'restored/changed-0/target'), 'extract_seconds': 0,
                    'runtime': {'tests': {'intent-store::store_pool_api': {}}}}
            with mock.patch.object(linux, 'prepare', return_value={'changed-0': item}), \
                 mock.patch.object(linux.coverage, 'report_environment', return_value={}), \
                 mock.patch.object(linux, 'compiler_exclusion', return_value='not binary(=store_pool_api)'), \
                 mock.patch.object(linux.archives, 'run_nextest') as run:
                linux.run_coverage(ROOT, root, 'changed')
                run.assert_not_called()
                receipt = json.loads((root / 'receipt-changed.json').read_text())['variants']['changed-0']
                self.assertTrue(receipt['compiler_only_selection'])
                self.assertEqual(receipt['producer_test_inventory'], linux.COMPILER_TESTS)

    def test_disk_budget_covers_measured_full_and_changed_profile_growth(self):
        # c68 hosted receipts measured st_blocks high-water including fresh
        # profiles/reports. Keep at least ten percent above those observations.
        cases = [([8730647500], [51230514375], 123968008192),
                 ([220328059, 10581410, 210633949, 8157503283, 266215686],
                  [1171306148, 45542078, 1260479575, 48142893085, 1590533052], 123224186880)]
        for archives, expansions, measured in cases:
            with self.subTest(measured=measured):
                budget = linux.consumer_disk_budget(archives, expansions)
                self.assertGreaterEqual(budget, measured * 11 // 10)

    def test_callback_is_prepared_before_changed_tests_generate_runtime_files(self):
        names = WORKFLOW['jobs']['coverage-changed']['env']['CALLBACK_FIXTURE_TESTS'].split()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / 'source'
            source.mkdir()
            (root / 'requests.json').write_text(json.dumps({'changed-0': {'identity': {}}, 'callback': {'identity': {'source': {'commit': 'head'}, 'build': {'inputs': {}}}}}))
            prepared_names = []
            def extract(bundle, request, checkout, destination, **kwargs):
                self.assertFalse((source / 'default.profraw').exists(), 'preparation must precede runtime outputs')
                prepared_names.append(destination.name)
                return {'source': str(source), 'target': str(destination / 'target'), 'extract_seconds': 0.1, 'expansion_validation_seconds': 0.2, 'runtime': {}}
            def execute(item, args, **kwargs):
                self.assertEqual(prepared_names, ['changed-0', 'callback'])
                (source / 'default.profraw').write_bytes(b'fresh runtime output')
                return 0
            with mock.patch.dict(os.environ, {'CALLBACK_FIXTURE_TESTS': '\n'.join(names)}), \
                 mock.patch.object(linux.archives.identity, 'build_key', return_value='key'), \
                 mock.patch.object(linux, 'prepare_streamed', side_effect=extract) as prepare, \
                 mock.patch.object(linux.native, 'consumer_environment', return_value={}), \
                 mock.patch.object(linux.coverage, 'report_environment', return_value={}), \
                 mock.patch.object(linux, 'compiler_exclusion', return_value=None), \
                 mock.patch.object(linux.archives, 'runtime_env', side_effect=lambda item, env: env), \
                 mock.patch.object(linux.archives, 'output', side_effect=lambda cmd, *args: 'head' if cmd[:2] == ['git', 'rev-parse'] else '' if cmd[0] == 'git' else json.dumps({'rust-suites': {'one': {'testcases': {n: {'filter-match': {'status': 'matches'}} for n in names}}}})), \
                 mock.patch.object(linux.archives, 'run_nextest', side_effect=execute) as run:
                linux.run_coverage(source, root, 'changed')
                self.assertEqual(linux.run_callback(source, root), 0)
                self.assertEqual(prepare.call_count, 2)
                self.assertEqual(run.call_count, 2)
                self.assertEqual(json.loads((root / 'receipt-callback.json').read_text())['selected_tests'], names)

    def test_callback_only_demand_prepares_without_running_changed_tests(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'requests.json').write_text(json.dumps({'callback': {}}))
            with mock.patch.object(linux, 'prepare', return_value={'callback': {}}) as prepare, \
                 mock.patch.object(linux.archives, 'run_nextest') as run:
                linux.run_coverage(ROOT, root, 'changed')
                self.assertEqual(prepare.call_args.args[2], ['callback'])
                run.assert_not_called()
                self.assertEqual(json.loads((root / 'receipt-changed.json').read_text())['variants'], {})

    def test_callback_inventory_and_failure_prevent_execution(self):
        names = WORKFLOW['jobs']['coverage-changed']['env']['CALLBACK_FIXTURE_TESTS'].split()
        for mode in ('valid', 'missing', 'extra', 'duplicate', 'empty', 'list-failure', 'run-failure', 'missing-prepared', 'missing-callback', 'wrong-source', 'wrong-key', 'changed-head', 'changed-tracked', 'stale-target', 'changed-input', 'swallowed-compiler'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / 'requests.json').write_text(json.dumps({'callback': {'identity': {'source': {'commit': 'head'}, 'build': {'inputs': {}}}}}))
                item = {'source': str(ROOT), 'target': str(root / 'restored/callback/target'), 'runtime': {}, 'request_key': 'key'}
                if mode == 'stale-target': item['target'] = str(root / 'previous-run/target')
                if mode == 'wrong-source': item['source'] = str(root / 'elsewhere')
                if mode == 'wrong-key': item['request_key'] = 'wrong'
                if mode != 'missing-prepared':
                    (root / 'prepared.json').write_text(json.dumps({} if mode == 'missing-callback' else {'callback': item}))
                selected = list(names)
                if mode == 'missing': selected.pop()
                if mode == 'extra': selected.append('wrong')
                if mode == 'empty': selected.clear()
                cases = {n: {'filter-match': {'status': 'matches'}} for n in selected}
                suites = {'one': {'testcases': cases}}
                if mode == 'duplicate': suites['two'] = {'testcases': {names[0]: cases[names[0]]}}
                if mode == 'changed-input':
                    request = json.loads((root / 'requests.json').read_text())
                    request['callback']['identity']['build']['inputs'] = {'Cargo.lock': '0' * 64}
                    (root / 'requests.json').write_text(json.dumps(request))
                real_environment = linux.native.consumer_environment
                def execute(item, args, *, env):
                    if mode == 'swallowed-compiler':
                        attempted = subprocess.run(['rustc', '--version'], env=real_environment(root), capture_output=True)
                        self.assertEqual(attempted.returncode, 97)
                    return 44 if mode == 'run-failure' else 0
                def output(command, *args):
                    if command[:2] == ['git', 'rev-parse']:
                        return 'other-head' if mode == 'changed-head' else 'head'
                    if command[:2] == ['git', 'diff']:
                        return 'src/lib.rs' if mode == 'changed-tracked' else ''
                    if mode == 'list-failure':
                        raise subprocess.CalledProcessError(43, command)
                    return json.dumps({'rust-suites': suites})
                with mock.patch.dict(os.environ, {'CALLBACK_FIXTURE_TESTS': '\n'.join(names), 'INTENTD_ASSERT_BOUND_CALLER': '0'}), \
                     mock.patch.object(linux.archives.identity, 'build_key', return_value='key'), \
                     mock.patch.object(linux.native, 'consumer_environment', return_value={}), \
                     mock.patch.object(linux.archives, 'runtime_env', side_effect=lambda item, env: env), \
                     mock.patch.object(linux.archives, 'output', side_effect=output) as listing, \
                     mock.patch.object(linux.archives, 'run_nextest', side_effect=execute) as run:
                    if mode in ('valid', 'run-failure'):
                        self.assertEqual(linux.run_callback(ROOT, root), 44 if mode == 'run-failure' else 0)
                        args = run.call_args.args[1]
                        self.assertEqual(args[args.index('--retries') + 1], '0')
                        self.assertIn('--no-fail-fast', args)
                        env = run.call_args.kwargs['env']
                        self.assertEqual(env['INTENTD_ASSERT_BOUND_CALLER'], '1')
                        self.assertEqual(env['INTENTD_TEST_TIMEOUT_MULTIPLIER'], '3')
                        self.assertIn('/restored/callback/', env['LLVM_PROFILE_FILE'])
                        self.assertEqual(listing.call_args.args[0][-3], args[1])
                    else:
                        with self.assertRaises((ValueError, KeyError, FileNotFoundError, subprocess.CalledProcessError)):
                            linux.run_callback(ROOT, root)
                        if mode == 'swallowed-compiler':
                            run.assert_called_once()
                            self.assertEqual(json.loads((root / 'receipt-callback.json').read_text())['compiler_calls'], 1)
                        else:
                            run.assert_not_called()


class ProducerPrerequisiteTests(unittest.TestCase):
    def test_only_successful_unsuperseded_job_can_authorize_retained_gate(self):
        expected = {'source': 's', 'head_sha': 'h'}
        marker = {'run_id': 42, 'attempt': 1, 'context': expected}
        job = dict(id=100, name='check', run_id=42, run_attempt=1, head_sha='h',
                   status='completed', conclusion='success', steps=[dict(
                       name='Produce Linux variants sequentially in one shared target', status='completed', conclusion='success')])
        api = mock.Mock()
        api.pages.side_effect = lambda path, field: [job] if '/attempts/1/' in path else []
        valid = linux.producer_prerequisite(api, marker, 3)
        self.assertEqual(valid['job_id'], 100)
        self.assertEqual(valid['producer_attempt'], 1)
        self.assertEqual(valid['consumer_attempt'], 3)
        cases = [[], [dict(job, conclusion='failure')], [dict(job, conclusion='cancelled')],
                 [dict(job, status='in_progress')], [dict(job, run_attempt=2)],
                 [dict(job, run_id=99)], [dict(job, head_sha='other')], [dict(job, steps=[])],
                 [job, job]]
        for rows in cases:
            with self.subTest(rows=rows):
                api.pages.side_effect = lambda path, field: rows if '/attempts/1/' in path else []
                with self.assertRaisesRegex(ValueError, 'producer prerequisite'):
                    linux.producer_prerequisite(api, marker, 3)
        for conclusion in ('success', 'failure', 'cancelled'):
            with self.subTest(new_producer=conclusion):
                newer = dict(job, id=200, run_attempt=2, conclusion=conclusion)
                api.pages.side_effect = lambda path, field: [job] if '/attempts/1/' in path else [newer] if '/attempts/2/' in path else []
                with self.assertRaisesRegex(ValueError, 'superseded'):
                    linux.producer_prerequisite(api, marker, 3)


class StreamExtractionTests(unittest.TestCase):
    def fixture(self, root, members):
        path = root / 'archive.tar.gz'
        with tarfile.open(path, 'w:gz') as tar:
            for name, data, kind in members:
                member = tarfile.TarInfo(name); member.type = kind; member.mode = 0o755
                member.size = len(data) if kind == tarfile.REGTYPE else 0
                member.linkname = '/tmp/outside'
                tar.addfile(member, io.BytesIO(data) if member.isfile() else None)
        return {'archive': path.name, 'files': {'target/binary': {'size': 3, 'sha256': hashlib.sha256(b'abc').hexdigest()}}}

    def test_stream_checks_written_bytes_and_preserves_executable_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = self.fixture(root, [('target/binary', b'abc', tarfile.REGTYPE)])
            linux.extract_stream(root, manifest, root / 'out', False)
            self.assertEqual((root / 'out/target/binary').read_bytes(), b'abc')
            self.assertEqual((root / 'out/target/binary').stat().st_mode & 0o777, 0o755)
            with self.assertRaises(FileExistsError):
                linux.extract_stream(root, manifest, root / 'out', False)

    def test_directories_are_limited_to_unique_manifest_ancestors(self):
        for directories in ([('outside-inventory', b'', tarfile.DIRTYPE)],
                            [('target', b'', tarfile.DIRTYPE)] * 2):
            with self.subTest(directories=directories), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest = self.fixture(root, directories + [('target/binary', b'abc', tarfile.REGTYPE)])
                with self.assertRaisesRegex(ValueError, 'directory'):
                    linux.extract_stream(root, manifest, root / 'out', False)
                self.assertFalse((root / 'out/outside-inventory').exists())
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = self.fixture(root, [('target', b'', tarfile.DIRTYPE), ('target/binary', b'abc', tarfile.REGTYPE)])
            linux.extract_stream(root, manifest, root / 'out', False)
            self.assertEqual((root / 'out/target/binary').read_bytes(), b'abc')

    def test_stream_rejects_tamper_oversize_missing_links_duplicates_and_escape(self):
        cases = [[('target/binary', b'abd', tarfile.REGTYPE)],
                 [('target/binary', b'abcd', tarfile.REGTYPE)], [],
                 [('target/binary', b'', tarfile.SYMTYPE)], [('target/binary', b'', tarfile.LNKTYPE)],
                 [('target/binary', b'abc', tarfile.REGTYPE)] * 2,
                 [('../escape', b'abc', tarfile.REGTYPE)],
                 [('target/unexpected', b'abc', tarfile.REGTYPE)]]
        for members in cases:
            with self.subTest(members=members), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp); manifest = self.fixture(root, members)
                with self.assertRaises(ValueError):
                    linux.extract_stream(root, manifest, root / 'out', False)
                self.assertFalse((root / 'escape').exists())


if __name__ == '__main__':
    unittest.main()
