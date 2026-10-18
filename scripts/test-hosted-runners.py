#!/usr/bin/env python3
"""Audit static, matrix and generated runner choices; maintenance is manual."""
from pathlib import Path
import unittest
import re
import yaml

ROOT = Path(__file__).resolve().parents[1]


class HostedRunners(unittest.TestCase):
    def test_workloads_use_self_hosted_first_policy(self):
        import subprocess
        subprocess.run(['python3', '-B', str(ROOT / 'scripts/test_runner_policy.py')], check=True)

    def test_no_depot_runner_labels(self):
        # Depot was retired; every fallback is a GitHub-hosted label.
        for path in [*(ROOT / '.github').rglob('*.yml'), *(ROOT / '.github').rglob('*.yaml'), ROOT / 'dist-workspace.toml']:
            self.assertEqual(re.findall(r'depot-(?:ubuntu|windows|macos)[\w.-]*', path.read_text()), [], str(path))

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
        cache = next(i for i, step in enumerate(steps) if 'scripts/setup-sccache.py' in step.get('run', ''))
        self.assertLess(cleanup, cache)
        self.assertLess(cache, build)
        for step in steps[cleanup+1:]:
            self.assertEqual(step['if'], 'inputs.apply && inputs.verify_build')
        dispatcher = yaml.safe_load((ROOT / '.github/workflows/macos-prune.yml').read_text())
        self.assertEqual(dispatcher['jobs']['windows']['with']['verify_build'], '${{ inputs.verify_build }}')
        self.assertEqual(dispatcher['jobs']['windows']['secrets'], 'inherit')

    def test_compiling_jobs_configure_managed_cache(self):
        doc = yaml.safe_load((ROOT / '.github/workflows/ci.yml').read_text())
        for name in ['check', 'build', 'macos-services-runtime', 'coverage-e2e', 'coverage-all', 'coverage-changed', 'daemon-lifecycle']:
            with self.subTest(job=name):
                steps = doc['jobs'][name]['steps']
                configure = next(i for i, step in enumerate(steps) if step.get('run') == 'python3 scripts/setup-sccache.py')
                builds = [i for i, s in enumerate(steps) if re.search(r'\bcargo (?:check|clippy|build|test|nextest|llvm-cov)\b', s.get('run', ''))]
                self.assertTrue(builds)
                self.assertLess(configure, min(builds))


if __name__ == '__main__':
    unittest.main()
