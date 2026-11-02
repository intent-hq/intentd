#!/usr/bin/env python3
"""Keep macOS cleanup reachable, scoped, and ahead of builds."""
from pathlib import Path
import unittest
import yaml

ROOT = Path(__file__).resolve().parents[1]


def workflow(name):
    return yaml.safe_load((ROOT / '.github/workflows' / name).read_text())


class WorkflowTests(unittest.TestCase):
    def test_build_preflight_precedes_toolchain_and_compilation(self):
        steps = workflow('ci.yml')['jobs']['build']['steps']
        index = next(i for i, s in enumerate(steps) if s.get('name') == 'Ensure macOS CI disk headroom')
        step = steps[index]
        self.assertEqual(step['run'], 'python3 -I -B scripts/macos-ci-cleanup.py --apply')
        self.assertIn("runner.os == 'macOS'", step['if'])
        self.assertIn("contains(matrix.os, 'self-hosted')", step['if'])
        self.assertIn("needs.release-fast-path.outputs.fast_path != 'true'", step['if'])
        self.assertLess(index, next(i for i, s in enumerate(steps) if s.get('name') == 'Resolve pinned Rust channel'))
        self.assertFalse(any('cargo ' in s.get('run', '') for s in steps[:index]))

    def test_macos_queue_proofs_share_the_build_preflight(self):
        # The macOS lifecycle proof and Services runtime steps run in `build`,
        # after its single preflight and in its target dir; no other ci.yml job
        # requests a macOS runner or prunes the m1mac target roots.
        jobs = workflow('ci.yml')['jobs']
        steps = jobs['build']['steps']
        preflight = next(i for i, s in enumerate(steps) if s.get('name') == 'Ensure macOS CI disk headroom')
        lifecycle = next(i for i, s in enumerate(steps) if s.get('name') == 'Native lifecycle runtime proof (macOS)')
        self.assertLess(preflight, lifecycle)
        self.assertEqual(steps[lifecycle]['env'], {'CARGO_BUILD_TARGET': '${{ matrix.target }}'})
        self.assertNotIn('macos-services-runtime', jobs)
        self.assertEqual([row['platform'] for row in jobs['daemon-lifecycle']['strategy']['matrix']['include']], ['linux'])
        for name, job in jobs.items():
            if name == 'build':
                continue
            with self.subTest(job=name):
                self.assertNotIn('macos_labels', str(job.get('runs-on', '')) + str(job.get('strategy', '')))
                self.assertFalse(any('scripts/macos-ci-cleanup.py --apply' in s.get('run', '') for s in job.get('steps', [])))

    def test_maintenance_is_serialized_and_manual_defaults_to_dry_run(self):
        data = workflow('macos-prune.yml')
        events = data.get('on', data.get(True))
        self.assertFalse(events['workflow_dispatch']['inputs']['apply']['default'])
        self.assertTrue(events['schedule'])
        self.assertEqual(data['permissions'], {'contents': 'read'})
        self.assertEqual(data['concurrency'], {'group': 'macos-ci-disk-maintenance', 'cancel-in-progress': False})
        job = data['jobs']['prune']
        self.assertEqual(job['runs-on'], ['self-hosted', 'macOS', 'ARM64', 'm1mac'])
        steps = job['steps']
        self.assertEqual(steps[1]['run'], 'python3 -I -B scripts/macos-ci-cleanup.py')
        self.assertEqual(steps[2]['if'], "github.event_name == 'schedule' || inputs.apply")
        self.assertEqual(steps[2]['run'], 'python3 -I -B scripts/macos-ci-cleanup.py --apply')

    def test_safety_tests_are_enforced_in_ci(self):
        steps = workflow('ci.yml')['jobs']['release-scripts']['steps']
        commands = '\n'.join(s.get('run', '') for s in steps)
        self.assertIn('python3 -I -B scripts/test-macos-ci-cleanup.py', commands)
        self.assertIn('python3 -I -B scripts/test-macos-ci-workflows.py', commands)


if __name__ == '__main__':
    unittest.main()
