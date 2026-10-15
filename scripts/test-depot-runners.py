#!/usr/bin/env python3
"""Audit static, matrix and generated runner choices; maintenance is manual."""
from pathlib import Path
import re
import importlib.util
import tomllib
import unittest
import yaml

ROOT = Path(__file__).resolve().parents[1]
MAINTENANCE = {'m1mac-smoke.yml', 'provision-m1mac.yml', 'macos-prune.yml', 'tinybox-prune.yml', 'windows-prune.yml'}
# Cleanup and its read-access diagnostic must remain available independently of
# the native runner queues they inspect. Only these exact jobs use hosted Linux.
HOSTED_RETIREMENT_JOBS = {
    ('retire-obsolete-merge-runs.yml', 'cleanup'),
    ('retirement-credential-probe.yml', 'existing-read-credential'),
}
LABEL = re.compile(r'depot-(?:ubuntu-(?:22\.04|24\.04)(?:-arm)?(?:-(?:4|8|16|32|64))?|windows-(?:2022|2025)(?:-(?:4|8|16|32|64))?|macos-latest)\Z')


class DepotRunners(unittest.TestCase):
    def test_retirement_hosted_runner_exceptions_are_exact(self):
        for workflow, job in (
            ('retire-obsolete-merge-runs.yml', 'cleanup'),
            ('retirement-credential-probe.yml', 'existing-read-credential'),
        ):
            with self.subTest(workflow=workflow, job=job):
                self.assert_job_labels(workflow, job, {'runs-on': 'ubuntu-latest'})
                for label in ('ubuntu-24.04', 'windows-latest', 'depot-ubuntu-24.04',
                              ['ubuntu-latest'], '${{ matrix.os }}'):
                    with self.subTest(label=label), self.assertRaises(AssertionError):
                        self.assert_job_labels(workflow, job, {'runs-on': label})
                for other_workflow, other_job in (
                    ('ci.yml', job), (workflow, 'unexpected-job'),
                    (workflow + '.copy', job),
                ):
                    with self.subTest(other_workflow=other_workflow, other_job=other_job), self.assertRaises(AssertionError):
                        self.assert_job_labels(other_workflow, other_job, {'runs-on': 'ubuntu-latest'})
                # The exception covers only the job's static runner, not nested choices.
                with self.assertRaises(AssertionError):
                    self.assert_job_labels(workflow, job, {
                        'runs-on': 'ubuntu-latest',
                        'strategy': {'matrix': {'os': ['ubuntu-latest']}},
                    })

    def test_large_linux_exception_is_limited_to_shared_producer_and_heavy_consumers(self):
        doc = yaml.safe_load((ROOT / '.github/workflows/ci.yml').read_text())
        jobs = doc['jobs']
        route = next(s['with']['script'] for s in jobs['route']['steps'] if s.get('id') == 'decide')
        self.assertIn('const checkBurst = ["gh-linux-16x"];', route)
        self.assertIn('const burst = ["depot-ubuntu-24.04-16"];', route)
        self.assertEqual({name for name, job in jobs.items()
                          if job.get('runs-on') == '${{ fromJSON(needs.route.outputs.check_labels) }}'},
                         {'check', 'coverage-all', 'coverage-changed'})
        self.assertEqual(jobs['coverage-e2e']['runs-on'], '${{ fromJSON(needs.route.outputs.linux_labels) }}')
        self.assertEqual(route.count('gh-linux-16x'), 1)
        # The aggregate target/staging guard remains a prerequisite to building;
        # no archive producer is added to either large consumer.
        for name in ('coverage-all', 'coverage-changed'):
            self.assertEqual(jobs[name]['timeout-minutes'], 30)
            self.assertFalse(any('ci-linux-artifacts.py produce' in step.get('run', '')
                                 for step in jobs[name]['steps']))

    def assert_job_labels(self, workflow, job_id, job):
        def visit(value):
            if isinstance(value, dict):
                for key, item in value.items():
                    if key in {'runs-on', 'os', 'runner'}:
                        for label in item if isinstance(item, list) else [item]:
                            if not isinstance(label, str):
                                continue
                            if '${{' not in label:
                                self.assertRegex(label, LABEL)
                            else:
                                self.assertNotRegex(label, r'(?<![\w-])(?:ubuntu|windows|macos|gh-linux|gh-windows)-[\w.-]+')
                    visit(item)
            elif isinstance(value, list):
                for item in value:
                    visit(item)
        self.assertNotIsInstance(job.get('runs-on'), list)
        if (workflow, job_id) in HOSTED_RETIREMENT_JOBS:
            self.assertEqual(job.get('runs-on'), 'ubuntu-latest')
            visit({key: value for key, value in job.items() if key != 'runs-on'})
        else:
            visit(job)

    def test_static_and_matrix_labels(self):
        for path in (ROOT / '.github/workflows').glob('*.yml'):
            with self.subTest(workflow=path.name):
                doc = yaml.safe_load(path.read_text())
                if path.name in MAINTENANCE:
                    self.assertIn('workflow_dispatch', doc.get('on', doc.get(True)))
                    continue
                for job_id, job in doc['jobs'].items():
                    self.assert_job_labels(path.name, job_id, job)
        config = tomllib.loads((ROOT / 'dist-workspace.toml').read_text())['dist']
        for value in config['github-custom-runners'].values():
            self.assertRegex(value if isinstance(value, str) else value['runner'], LABEL)

    def test_windows_cleanup_requires_native_tests_and_excludes_pr_deletion(self):
        doc = yaml.safe_load((ROOT / '.github/workflows/windows-prune.yml').read_text())
        events = doc.get('on', doc.get(True))
        self.assertFalse(events['workflow_dispatch']['inputs']['apply']['default'])
        self.assertFalse(events['workflow_call']['inputs']['apply']['default'])
        self.assertFalse(doc['concurrency']['cancel-in-progress'])
        jobs = doc['jobs']
        self.assertEqual(jobs['prune']['if'], "github.event_name != 'pull_request'")
        self.assertEqual(jobs['prune']['needs'], 'safety-tests')
        self.assertEqual(jobs['prune']['runs-on'], ['self-hosted', 'Windows', 'X64'])
        self.assertEqual(jobs['safety-tests']['if'], "github.event_name != 'pull_request' || github.event.pull_request.head.repo.full_name == github.repository")
        self.assertTrue(any('--require-windows' in step.get('run', '') for step in jobs['safety-tests']['steps']))
        apply = [step for step in jobs['prune']['steps'] if '--apply' in step.get('run', '')]
        self.assertEqual(len(apply), 1)
        self.assertEqual(apply[0]['if'], "github.event_name == 'schedule' || inputs.apply")

    def test_windows_maintenance_build_is_opt_in_after_cleanup(self):
        doc = yaml.safe_load((ROOT / '.github/workflows/windows-prune.yml').read_text())
        events = doc.get('on', doc.get(True))
        for event in ('workflow_dispatch', 'workflow_call'):
            self.assertFalse(events[event]['inputs']['verify_build']['default'])
        steps = doc['jobs']['prune']['steps']
        cleanup = next(i for i, step in enumerate(steps) if '--apply' in step.get('run', ''))
        build = next(i for i, step in enumerate(steps) if 'cargo build --locked -p intentd' in step.get('run', ''))
        cache = next(i for i, step in enumerate(steps) if 'scripts/setup-depot-sccache.py' in step.get('run', ''))
        self.assertLess(cleanup, cache)
        self.assertLess(cache, build)
        for step in steps[cleanup+1:]:
            self.assertEqual(step['if'], 'inputs.apply && inputs.verify_build')
        dispatcher = yaml.safe_load((ROOT / '.github/workflows/macos-prune.yml').read_text())
        self.assertEqual(dispatcher['jobs']['windows']['with']['verify_build'], '${{ inputs.verify_build }}')
        self.assertEqual(dispatcher['jobs']['windows']['secrets'], 'inherit')

    def test_compiling_jobs_configure_managed_cache(self):
        doc = yaml.safe_load((ROOT / '.github/workflows/ci.yml').read_text())
        spec = importlib.util.spec_from_file_location('workflows', ROOT / 'scripts/ci-workflow-tests.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        doc = module.load_ci(ROOT)
        for name in ['check', 'build', 'daemon-lifecycle']:
            with self.subTest(job=name):
                steps = doc['jobs'][name]['steps']
                configure = next(i for i, step in enumerate(steps) if step.get('run') == 'python3 scripts/setup-depot-sccache.py')
                builds = [i for i, s in enumerate(steps) if re.search(r'\bcargo (?:check|clippy|build|test|nextest|llvm-cov)\b|ci-(?:linux|native)-artifacts\.py produce', s.get('run', ''))]
                self.assertTrue(builds)
                self.assertLess(configure, min(builds))
        for name in ['coverage-e2e', 'coverage-all', 'coverage-changed']:
            steps = doc['jobs'][name]['steps']
            self.assertFalse(any('setup-depot-sccache.py' in s.get('run', '') for s in steps))
            self.assertTrue(any('ci-linux-artifacts.py fetch' in s.get('run', '') for s in steps))
        for job in yaml.safe_load((ROOT / '.github/workflows/ci.yml').read_text())['jobs'].values():
            if job.get('uses') == './.github/workflows/native-artifacts.yml':
                self.assertEqual(job['secrets']['DEPOT_TOKEN'], '${{ secrets.DEPOT_TOKEN }}')


if __name__ == '__main__':
    unittest.main()
