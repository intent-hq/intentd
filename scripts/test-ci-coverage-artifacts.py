#!/usr/bin/env python3
"""Real LLVM archive relocation, fresh profiles, compiler guards and floors."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from unittest import mock


def load(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


archives = load('ci-test-artifacts')
coverage = load('ci-coverage-artifacts')
native = load('ci-native-artifacts')


class CoverageTests(unittest.TestCase):
    def test_package_report_keeps_original_workspace_proc_macro_scope(self):
        with tempfile.TemporaryDirectory(prefix='intent-coverage-scope-') as tmp:
            root = Path(tmp)
            source = root / 'source'
            (source / 'library/src').mkdir(parents=True)
            (source / 'macros/src').mkdir(parents=True)
            (source / '.gitignore').write_text('lcov.info\ncoverage-summary.txt\n')
            (source / 'Cargo.toml').write_text('[workspace]\nresolver="2"\nmembers=["library","macros"]\n')
            (source / 'library/Cargo.toml').write_text('[package]\nname="scope-library"\nversion="0.1.0"\nedition="2021"\n[dependencies]\nscope-macros={path="../macros"}\n')
            (source / 'macros/Cargo.toml').write_text('[package]\nname="scope-macros"\nversion="0.1.0"\nedition="2021"\n[lib]\nproc-macro=true\n')
            (source / 'macros/src/lib.rs').write_text('use proc_macro::TokenStream;\n#[proc_macro]\npub fn answer(_: TokenStream) -> TokenStream { "42u8".parse().unwrap() }\n')
            (source / 'library/src/lib.rs').write_text('pub fn answer() -> u8 { scope_macros::answer!() }\n#[test] fn test_answer() { assert_eq!(answer(), 42); }\n')
            subprocess.run(['cargo', 'generate-lockfile', '--offline'], cwd=source, check=True, capture_output=True)
            for argv in (['init', '-q'], ['add', '.'], ['-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'fixture']):
                subprocess.run(['git', *argv], cwd=source, check=True, capture_output=True)
            env = dict(os.environ, CARGO_TARGET_DIR=str(root / 'baseline'))
            subprocess.run(['cargo', 'llvm-cov', '--no-report', '--remap-path-prefix', 'nextest', '-p', 'scope-library'], cwd=source, env=env, check=True)
            baseline = root / 'baseline.info'
            subprocess.run(['cargo', 'llvm-cov', 'report', '--remap-path-prefix', '--lcov', '--output-path', str(baseline)], cwd=source, env=env, check=True)
            facts = native.platform_facts('linux')
            recipe = dict(kind='test-archive', command=['cargo', 'nextest', 'archive', '--locked', '--offline', '-p', 'scope-library', '--target-dir', '{target}', '--cargo-message-format', 'json', '--archive-file', '{archive}'], companions=[])
            request = archives.capture_request(source, facts['platform'], recipe, config_sha256='0'*64, instrumented=True)
            record = archives.produce([request], source, root / 'archive-target', root / 'published')[0]
            bundle = root / 'published' / record['key']
            run = root / 'run'
            prepared = archives.prepare(bundle, request, source, run)
            runtime = dict(native.consumer_environment(root / 'guard'), LLVM_PROFILE_FILE=str(run / 'coverage-%p-%m.profraw'))
            self.assertEqual(archives.run_nextest(prepared, [], env=runtime), 0)
            self.assertEqual(coverage.report(bundle, prepared, request, 0)['exit_code'], 0)
            def source_lines(path):
                result, current = {}, None
                for line in path.read_text().splitlines():
                    if line.startswith('SF:'):
                        current = line[3:]
                        result[current] = set()
                    elif line.startswith('DA:'):
                        result[current].add(int(line[3:].split(',')[0]))
                return result
            self.assertEqual(source_lines(source / 'lcov.info'), source_lines(baseline))
            extra = bundle / 'report-objects.tar.gz'
            with tarfile.open(extra) as tar:
                self.assertTrue(tar.getmembers())
                self.assertTrue(all(member.name.endswith('.so') for member in tar.getmembers()))
            extra.write_bytes(extra.read_bytes() + b'tampered')
            with self.assertRaisesRegex(ValueError, 'report archive size/digest mismatch'):
                archives.verify_bundle(bundle, request)

    def test_real_relocated_archives_fresh_profiles_and_floors(self):
        with tempfile.TemporaryDirectory(prefix='intent-coverage-archive-') as tmp:
            root = Path(tmp)
            source = root / 'producer'
            (source / 'src').mkdir(parents=True)
            (source / '.gitignore').write_text('lcov.info\ncoverage-summary.txt\n')
            (source / 'Cargo.toml').write_text('[package]\nname="coverage-fixture"\nversion="0.1.0"\nedition="2021"\n[workspace]\n')
            (source / 'src/lib.rs').write_text('''pub fn answer(yes: bool) -> u8 {
    if yes {
        42
    } else {
        7
    }
}
pub fn uncalled() -> u8 {
    1
}
#[cfg(test)] mod tests {
    #[test] fn partial() { assert_eq!(super::answer(true), 42); }
    #[test] fn complete() { assert_eq!(super::answer(false), 7); assert_eq!(super::uncalled(), 1); }
    #[test] fn low() { assert_eq!(1, 1); }
}
''')
            subprocess.run(['cargo', 'generate-lockfile', '--offline'], cwd=source, check=True, capture_output=True)
            for argv in (['init', '-q'], ['add', '.'], ['-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'fixture']):
                subprocess.run(['git', *argv], cwd=source, check=True, capture_output=True)
            facts = native.platform_facts('linux')
            recipe = dict(kind='test-archive', command=['cargo', 'nextest', 'archive', '--locked', '--offline', '--lib', '--target', facts['platform']['target'], '--target-dir', '{target}', '--cargo-message-format', 'json', '--archive-file', '{archive}'], companions=[])
            requests = [archives.capture_request(source, facts['platform'], recipe, config_sha256='0'*64, instrumented=mode) for mode in (False, True)]
            self.assertNotEqual(archives.identity.build_key(requests[0]['identity']), archives.identity.build_key(requests[1]['identity']))
            self.assertEqual(archives.identity.producer_group_key(requests[0]['identity']), archives.identity.producer_group_key(requests[1]['identity']))
            records = archives.produce(requests, source, root / 'shared-target', root / 'published')
            self.assertEqual(len(records), 2)
            with mock.patch.object(archives.subprocess, 'Popen', side_effect=AssertionError('archive hit compiled')):
                self.assertTrue(all(r['reused'] for r in archives.produce(requests, source, root / 'shared-target', root / 'published')))
            consumer = root / 'relocated'
            shutil.copytree(source, consumer)
            shutil.rmtree(source)
            shutil.rmtree(root / 'shared-target')
            request = requests[1]
            bundle = root / 'published' / records[1]['key']
            # Independent executions cannot see each other's profraw. First
            # exercise every branch, then only one branch: 80% must fail again.
            for label, selection, floor, expected in [('all', 'all()', 80, 0), ('partial', 'test(partial)', 80, 1), ('e2e-floor', 'test(partial)', 40, 0), ('low40', 'test(low)', 40, 1), ('low80', 'test(low)', 80, 1)]:
                run = root / label
                prepared = archives.prepare(bundle, request, consumer, run, env=native.consumer_environment(root / 'guard'))
                env = dict(native.consumer_environment(root / 'guard'), LLVM_PROFILE_FILE=str(run / 'coverage-%p-%m.profraw'), INTENTD_ASSERT_BOUND_CALLER='1')
                self.assertEqual(archives.run_nextest(prepared, ['-E', selection, '--no-tests', 'fail'], env=env), 0)
                self.assertTrue(list(run.glob('*.profraw')))
                result = coverage.report(bundle, prepared, request, floor)
                self.assertEqual(result['exit_code'], expected)
                self.assertIn('TOTAL', (consumer / 'coverage-summary.txt').read_text())
                self.assertTrue((consumer / 'lcov.info').stat().st_size)
                self.assertFalse((run / 'compiler-calls.log').exists(), (run / 'compiler-calls.log').read_text() if (run / 'compiler-calls.log').exists() else '')
                probes = [json.loads(line) for line in (run / 'metadata-probes.jsonl').read_text().splitlines()]
                self.assertTrue(probes)
            # Both floors reject an actual low report; the isolated empty test
            # run writes zero profiles and must fail, never borrow prior data.
            run = root / 'empty'
            prepared = archives.prepare(bundle, request, consumer, run, env=native.consumer_environment(root / 'guard'))
            for floor in (40, 80):
                with self.assertRaises(subprocess.CalledProcessError):
                    coverage.report(bundle, prepared, request, floor)
            guard = coverage.report_environment(root)
            for argv in (['cargo', 'build'], ['cargo', 'test'], ['rustc', 'input.rs'], ['cc', 'input.c']):
                self.assertNotEqual(subprocess.run(argv, env=guard, capture_output=True).returncode, 0)
            self.assertEqual(len((root / 'compiler-calls.log').read_text().splitlines()), 4)


if __name__ == '__main__':
    unittest.main()
