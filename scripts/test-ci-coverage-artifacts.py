#!/usr/bin/env python3
"""Real LLVM archive relocation, fresh profiles, compiler guards and floors."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
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
