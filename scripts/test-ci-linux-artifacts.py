#!/usr/bin/env python3
"""Linux demand, gate mutations and archive-only callback execution contracts."""
import copy
import importlib.util
import json
import os
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

    def test_gate_requires_both_reports_on_pipeline_pr_and_merge_group(self):
        baseline = {name: 'success' for name in WORKFLOW['jobs']['gate']['needs']}
        for event in ('pull_request', 'merge_group'):
            self.assertTrue(routing.gate_passes(WORKFLOW, event, baseline, full_coverage='true'))
            for name in ('coverage-e2e', 'coverage-all'):
                for result in ('skipped', 'failure', 'cancelled', ''):
                    self.assertFalse(routing.gate_passes(WORKFLOW, event, {**baseline, name: result}, full_coverage='true'), (event, name, result))
        for demand in ('', 'unknown'):
            self.assertFalse(routing.gate_passes(WORKFLOW, 'pull_request', baseline, full_coverage=demand))
        baseline.update({'coverage-e2e': 'skipped', 'coverage-all': 'skipped'})
        self.assertTrue(routing.gate_passes(WORKFLOW, 'pull_request', baseline, full_coverage='false'))

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
                 mock.patch.object(linux.archives, 'produce', side_effect=[[{'reused': False}], [{'reused': True}], [{'reused': True}]]) as produce, \
                 mock.patch.object(linux.archives, 'source_commit', return_value='a' * 40), \
                 mock.patch.object(linux.archives.identity, 'build_key', return_value='key'), \
                 mock.patch.object(linux.archives, 'verify_bundle', return_value=item), \
                 mock.patch.object(linux.archives, 'prepare', return_value=item), \
                 mock.patch.object(linux.coverage, 'build_environment', return_value={}) as environment, \
                 mock.patch.object(linux, 'compiler_inventory', return_value=linux.COMPILER_TESTS), \
                 mock.patch.object(linux.archives, 'run_libtest', side_effect=[0, 0, 41]) as run:
                for expected in ({'reused': False}, {'reused': True}):
                    self.assertEqual(linux.produce(ROOT, root, 'merge_group', None, reuse=False), [expected])
                    self.assertEqual(linux.compiler_exclusion(root, 'coverage-all', request, item), 'not binary(=store_pool_api)')
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

    def test_callback_inventory_and_failure_prevent_execution(self):
        names = WORKFLOW['jobs']['coverage-changed']['env']['CALLBACK_FIXTURE_TESTS'].split()
        for mode in ('valid', 'missing', 'extra', 'duplicate', 'empty', 'list-failure', 'run-failure'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / 'requests.json').write_text(json.dumps({'callback': {}}))
                item = {'source': str(ROOT), 'target': str(root / 'restored/callback/target'), 'runtime': {}}
                selected = list(names)
                if mode == 'missing': selected.pop()
                if mode == 'extra': selected.append('wrong')
                if mode == 'empty': selected.clear()
                cases = {n: {'filter-match': {'status': 'matches'}} for n in selected}
                suites = {'one': {'testcases': cases}}
                if mode == 'duplicate': suites['two'] = {'testcases': {names[0]: cases[names[0]]}}
                with mock.patch.dict(os.environ, {'CALLBACK_FIXTURE_TESTS': '\n'.join(names), 'INTENTD_ASSERT_BOUND_CALLER': '0'}), \
                     mock.patch.object(linux, 'prepare', return_value={'callback': item}), \
                     mock.patch.object(linux.native, 'consumer_environment', return_value={}), \
                     mock.patch.object(linux.archives, 'runtime_env', side_effect=lambda item, env: env), \
                     mock.patch.object(linux.archives, 'output', side_effect=subprocess.CalledProcessError(43, []) if mode == 'list-failure' else None, return_value=json.dumps({'rust-suites': suites})) as listing, \
                     mock.patch.object(linux.archives, 'run_nextest', return_value=44 if mode == 'run-failure' else 0) as run:
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
                        with self.assertRaises((ValueError, subprocess.CalledProcessError)):
                            linux.run_callback(ROOT, root)
                        run.assert_not_called()


if __name__ == '__main__':
    unittest.main()
