#!/usr/bin/env python3
"""Native archives must cover exact event demands and forbid consumer builds."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]

def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'scripts' / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

archives = load('ci-test-artifacts')

class NativeDemandTests(unittest.TestCase):
    def test_macos_full_demand_contains_the_doctor_proof(self):
        recipes = archives.native_recipes('macos', 'merge_group', 'aarch64-apple-darwin')
        self.assertIn('e2e_doctor_codex', recipes['daemon']['command'])

    def test_pr_does_not_build_unneeded_native_variants(self):
        for platform in ('linux', 'macos', 'windows'):
            recipes = archives.native_recipes(platform, 'pull_request', 'fixture')
            self.assertEqual(set(recipes), {'daemon', 'sitter', 'sitter-production'})
            self.assertNotIn('e2e_doctor_codex', recipes['daemon']['command'])
            self.assertNotIn('--workspace', recipes['daemon']['command'])

    def test_production_sitter_never_inherits_test_graph(self):
        recipe = archives.native_recipes('windows', 'merge_group', 'fixture')['sitter-production']
        self.assertEqual(recipe['command'][:2], ['cargo', 'build'])
        self.assertNotIn('--all-targets', recipe['command'])
        self.assertNotIn('--features', recipe['command'])

class ConsumerTests(unittest.TestCase):
    def setUp(self):
        self.native = load('ci-native-artifacts')

    def test_scope_is_full_only_for_merge_group_or_native_ci_changes(self):
        for path in ('.github/workflows/ci.yml', 'scripts/ci-test-artifacts.py',
                     'scripts/ci-native-artifacts.py', 'scripts/test-daemon-lifecycle.py',
                     '.github/actions/native/action.yml', '.cargo/config.toml', 'Cargo.lock',
                     'crates/intentd/Cargo.toml', 'crates/intentd/tests/common/mod.rs',
                     'crates/intentd-test-support/src/runtime_paths.rs'):
            self.assertEqual(self.native.native_scope('pull_request', [path]), 'merge_group')
        self.assertEqual(self.native.native_scope('pull_request', ['crates/intentd/src/main.rs']), 'pull_request')
        self.assertEqual(self.native.native_scope('merge_group', []), 'merge_group')
        self.assertEqual(self.native.native_scope('pull_request', None), 'merge_group')
        with self.assertRaises(ValueError):
            self.native.native_scope('push', [])

    def test_existing_cow_proof_and_scratch_sources_request_full_native_pr_validation(self):
        for path in ('crates/intent-services/src/tests.rs',
                     'crates/intent-services/src/test_support/scratch.rs'):
            self.assertTrue((ROOT / path).is_file(), path)
            self.assertEqual(self.native.native_scope('pull_request', [path]), 'merge_group', path)

    def test_producer_passes_all_exact_variants_once_before_lints(self):
        facts = {'platform': {'target': 'native-target'}, 'native_tools': {'cc': 'cc', 'linker': 'linker'}}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / 'source'; source.mkdir()
            calls = []
            with patch.dict(os.environ), patch.object(self.native, 'platform_facts', return_value=facts), \
                 patch.object(self.native.archives, 'capture_request', side_effect=lambda source, platform, recipe, **kw: recipe), \
                 patch.object(self.native.archives, 'produce', side_effect=lambda requests, source, target, published: calls.append(('produce', requests, target))), \
                 patch.object(self.native.subprocess, 'run', side_effect=lambda command, **kw: calls.append(('lint', command))):
                self.native.produce(source, root / 'artifacts', 'macos', 'merge_group')
            self.assertEqual([call[0] for call in calls], ['produce', 'lint', 'lint'])
            self.assertEqual(calls[0][1], list(archives.native_recipes('macos', 'merge_group', 'native-target').values()))
            for call in calls[1:]:
                self.assertEqual(call[1][call[1].index('--target-dir') + 1], str(calls[0][2]))
            self.assertNotIn('--all-targets', calls[1][1])
            self.assertIn('--all-targets', calls[2][1])

    def test_workflow_publishes_before_consumers_and_retains_failures(self):
        import yaml
        job = yaml.safe_load((ROOT / '.github/workflows/ci.yml').read_text())['jobs']['daemon-lifecycle']
        steps = job['steps']
        names = [step.get('name', '') for step in steps]
        producer = names.index('Produce native archives and lint in one shared target')
        upload = names.index('Publish native variants')
        restore = names.index('Restore native executables with compiler tripwires')
        lifecycle = names.index('Native lifecycle runtime proof')
        self.assertLess(producer, upload)
        self.assertLess(upload, restore)
        self.assertLess(restore, lifecycle)
        self.assertEqual(steps[upload]['if'], 'always()')
        for step in steps[restore:]:
            self.assertNotRegex(step.get('run', ''), r'cargo (?:build|test|check|clippy|nextest archive)')
        self.assertEqual(sum('ci-native-artifacts.py produce' in step.get('run', '') for step in steps), 1)

    def test_archive_regressions_run_after_pinned_tooling_in_ci(self):
        import yaml
        steps = yaml.safe_load((ROOT / '.github/workflows/ci.yml').read_text())['jobs']['release-scripts']['steps']
        compiler = next(i for i, step in enumerate(steps) if step.get('name') == 'Install release cache regression compiler')
        nextest = next(i for i, step in enumerate(steps) if step.get('with', {}).get('tool') == 'cargo-nextest@0.9.143')
        for name in ('test-ci-build-identity.py', 'test-ci-test-artifacts.py'):
            index = next(i for i, step in enumerate(steps) if name in step.get('run', ''))
            self.assertGreater(index, compiler)
            self.assertGreater(index, nextest)
            self.assertNotIn('if', steps[index])
            self.assertNotIn('continue-on-error', steps[index])

    def test_windows_tools_resolve_from_msvc_not_git_bash(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            compiler = root / 'MSVC/cl.exe'
            compiler.parent.mkdir()
            compiler.touch(); compiler.chmod(0o755)
            shadow = root / 'Git/usr/bin'; shadow.mkdir(parents=True)
            for name in ('link', 'link.exe', 'dumpbin.exe'):
                tool = shadow / name
                tool.write_text('#!/bin/sh\nexit 91\n'); tool.chmod(0o755)
            linker = compiler.with_name('link.exe'); linker.touch()
            dumpbin = compiler.with_name('dumpbin.exe'); dumpbin.touch()
            with patch.dict(os.environ, {'PATH': str(shadow) + os.pathsep + str(compiler.parent)}):
                self.assertEqual(self.native.msvc_tool('link'), str(linker))
                self.assertEqual(self.native.msvc_tool('dumpbin'), str(dumpbin))
                linker.unlink()
                with self.assertRaisesRegex(RuntimeError, 'MSVC'):
                    self.native.msvc_tool('link')

    def test_msvc_help_nonzero_exit_requires_valid_tool_banner_and_usage(self):
        banners = {'link': 'Incremental Linker', 'dumpbin': 'COFF/PE Dumper'}
        for name, banner in banners.items():
            help_text = f'Microsoft (R) {banner} Version 14.44.35229.0\n usage: {name.upper()} [options] [files]\n'
            with patch.object(self.native, 'msvc_tool', return_value=f'/MSVC/{name}.exe'), \
                 patch.object(self.native.subprocess, 'run') as run:
                run.return_value = subprocess.CompletedProcess([], 1100, help_text)
                self.assertEqual(self.native.msvc_help(name), help_text.strip())
                self.assertEqual(run.call_args.args[0], [f'/MSVC/{name}.exe', '/?'])
                for invalid in ('link: missing operand', help_text + 'LINK : fatal error LNK1104: cannot open file'):
                    run.return_value = subprocess.CompletedProcess([], 1, invalid)
                    with self.assertRaises(RuntimeError):
                        self.native.msvc_help(name)

    def test_dependency_inspection_uses_the_provisioned_msvc_tool(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'System32').mkdir()
            (root / 'System32/KERNEL32.dll').touch()
            binary = root / 'fixture.exe'; binary.touch()
            with patch.dict(os.environ, {'SystemRoot': str(root), 'DUMPBIN': str(root / 'MSVC/dumpbin.exe')}), \
                 patch.object(archives.sys, 'platform', 'win32'), \
                 patch.object(archives, 'output', return_value='    KERNEL32.dll\n') as output:
                archives.stage_binary_libraries(binary, root)
                self.assertEqual(output.call_args.args[0][0], str(root / 'MSVC/dumpbin.exe'))
                self.assertEqual(archives.build_environment()['DUMPBIN'], str(root / 'MSVC/dumpbin.exe'))

    def test_windows_tripwires_are_executables_not_cmd_fallbacks(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.dict(os.environ), patch.object(self.native.sys, 'platform', 'win32'):
                with self.assertRaisesRegex(RuntimeError, 'producer.*tripwire'):
                    self.native.consumer_environment(root)
                (root / 'compiler-tripwire.exe').write_bytes(b'fixture PE executable')
                env = self.native.consumer_environment(root)
            for command in ('cargo', 'rustc', 'rustup'):
                self.assertEqual((root / 'no-compiler' / (command + '.exe')).read_bytes(), b'fixture PE executable')
            self.assertTrue(env['RUSTC'].endswith('rustc.exe'))

    def test_native_compiler_environment_is_bound_without_credentials(self):
        with patch.dict(os.environ, {'VCPKG_ROOT': '/native/vcpkg', 'CL': '/O2', 'CARGO_REGISTRIES_PRIVATE_TOKEN': 'secret'}):
            env = archives.build_environment()
            self.assertEqual(env['VCPKG_ROOT'], '/native/vcpkg')
            self.assertEqual(env['CL'], '/O2')
            self.assertNotIn('CARGO_REGISTRIES_PRIVATE_TOKEN', env)

    def test_effective_config_includes_home_and_ancestors_without_serializing_contents(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / 'parent/repo'
            source.mkdir(parents=True)
            cargo_home = root / 'cargo-home'
            cargo_home.mkdir()
            (root / 'parent/.cargo').mkdir()
            (root / 'parent/.cargo/config.toml').write_text('[build]\nrustflags=["--cfg=parent"]\n')
            (cargo_home / 'config.toml').write_text('[build]\njobs=2\n')
            with patch.dict(os.environ, {'CARGO_HOME': str(cargo_home)}):
                first = self.native.config_digest(source)
                (cargo_home / 'config.toml').write_text('[build]\njobs=3\n')
                self.assertNotEqual(first, self.native.config_digest(source))
                (root / 'parent/.cargo/config').write_text('[build]\njobs=4\n')
                self.assertNotEqual(first, self.native.config_digest(source))

    def test_consumer_libtest_uses_package_cwd_companions_libraries_and_no_compiler(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / 'source'; target = root / 'target'
            (source / 'crates/fixture').mkdir(parents=True); target.mkdir()
            binary = target / 'test-fixture'
            binary.write_text(f'#!{sys.executable}\n' + '''import os, pathlib
assert pathlib.Path.cwd().name == 'fixture'
assert os.environ['INTENTD_ASSERT_BOUND_CALLER'] == '1'
assert pathlib.Path(os.environ['NEXTEST_BIN_EXE_fixture']).is_file()
print('test result: ok. 1 passed; 0 failed; 0 ignored;')
''')
            binary.chmod(0o755)
            prepared = {'source': str(source), 'target': str(target), 'producer_group': 'fixture',
                        'runtime': {'tests': {'fixture': {'path': binary.name, 'cwd': 'crates/fixture'}},
                                    'companions': {'fixture': binary.name}, 'library_dirs': ['.']}}
            # The actual consumer helper is used; the stub is a prebuilt executable.
            with patch.dict(os.environ, {'INTENTD_ASSERT_BOUND_CALLER': '1'}):
                env = self.native.consumer_environment(root)
                self.assertEqual(self.native.archives.run_libtest(prepared, 'fixture', ['--nocapture'], env=env), 0)
                for command in ('cargo', 'rustc', 'rustup'):
                    result = subprocess.run([command, '--version'], env=env, capture_output=True)
                    self.assertNotEqual(result.returncode, 0)
                self.assertEqual(len((root / 'compiler-calls.log').read_text().splitlines()), 3)

if __name__ == '__main__':
    unittest.main()
