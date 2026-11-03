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

    def test_preflight_is_opt_in_and_never_runs_in_the_queue_or_on_main(self):
        # Paid macOS minutes stay off ordinary PRs: only a dispatch or the
        # ci:macos label on a same-repo PR starts it, and it never runs on the
        # events ci.yml's `build` owns.
        data = workflow('macos-preflight.yml')
        events = data.get('on', data.get(True))
        self.assertEqual(sorted(events), ['pull_request', 'workflow_dispatch'])
        self.assertEqual(events['pull_request'], {'types': ['opened', 'synchronize', 'reopened', 'labeled']})
        ref = events['workflow_dispatch']['inputs']['ref']
        self.assertEqual((ref['required'], ref['default']), (False, ''))
        self.assertEqual(data['permissions'], {'contents': 'read'})
        jobs = data['jobs']
        self.assertEqual(sorted(jobs), ['preflight', 'runner-route'])
        guard = jobs['runner-route']['if']
        self.assertEqual(guard, (
            "github.event_name == 'workflow_dispatch' || "
            "(github.event.pull_request.head.repo.full_name == github.repository && "
            "((github.event.action == 'labeled' && github.event.label.name == 'ci:macos') || "
            "(github.event.action != 'labeled' && contains(github.event.pull_request.labels.*.name, 'ci:macos'))))"))
        self.assertEqual(jobs['preflight']['needs'], ['runner-route'])
        self.assertNotIn('if', jobs['preflight'])

    def test_preflight_check_name_is_distinct_from_required_contexts(self):
        # A same-named check would let a preflight run stand in for (or block
        # as) a ci.yml context on the PR head.
        ci = workflow('ci.yml')['jobs']
        name = workflow('macos-preflight.yml')['jobs']['preflight']['name']
        self.assertEqual(name, 'macOS preflight (aarch64-apple-darwin)')
        self.assertNotIn(name, {ci['gate']['name'], 'build (aarch64-apple-darwin)'})
        self.assertNotIn('macos-preflight', str(ci['gate']['needs']))

    def test_preflight_runs_the_queue_compile_and_clippy_commands(self):
        # The preflight is only useful while it runs what ejects queue entries.
        build = workflow('ci.yml')['jobs']['build']
        target = build['strategy']['matrix']['include'][0]['target']
        self.assertEqual(target, 'aarch64-apple-darwin')

        def by_name(steps):
            return {s['name']: s for s in steps if 'name' in s}

        queue = by_name(build['steps'])
        pre = by_name(workflow('macos-preflight.yml')['jobs']['preflight']['steps'])
        for name in ('Check (native production configuration)', 'Clippy (target-gated code)',
                     'Point CARGO_TARGET_DIR outside the work dir (self-hosted)', 'Ensure macOS CI disk headroom',
                     'Resolve pinned Rust channel', 'Configure the self-hosted compiler cache'):
            with self.subTest(step=name):
                self.assertEqual(pre[name]['run'], queue[name]['run'].replace('${{ matrix.target }}', target))
        self.assertEqual(pre['Configure the self-hosted compiler cache'].get('env'),
                         queue['Configure the self-hosted compiler cache'].get('env'))
        self.assertIn('-D warnings', pre['Clippy (target-gated code)']['run'])
        job_env = workflow('macos-preflight.yml')['jobs']['preflight']['env']
        for key, value in build['env'].items():
            self.assertEqual(job_env[key], value)

    def test_preflight_cleans_only_the_persistent_runner_before_compiling(self):
        job = workflow('macos-preflight.yml')['jobs']['preflight']
        self.assertEqual(job['runs-on'], "${{ fromJSON(needs.runner-route.outputs.runners || '{}')['macos-26-xlarge'] || fromJSON('[\"warp-macos-26-arm64-6x\"]') }}")
        # WarpBuild runners report runner.environment == 'self-hosted' too.
        self.assertEqual(job['env']['PERSISTENT'], "${{ contains(fromJSON(needs.runner-route.outputs.runners || '{}')['macos-26-xlarge'], 'self-hosted') }}")
        steps = job['steps']
        names = [s.get('name') for s in steps]
        cleanup = names.index('Ensure macOS CI disk headroom')
        for name in ('Ensure macOS CI disk headroom', 'Point CARGO_TARGET_DIR outside the work dir (self-hosted)'):
            self.assertEqual(steps[names.index(name)]['if'], "env.PERSISTENT == 'true'")
        self.assertLess(names.index('Point CARGO_TARGET_DIR outside the work dir (self-hosted)'), cleanup)
        self.assertLess(cleanup, names.index('Resolve pinned Rust channel'))
        self.assertFalse(any('cargo ' in s.get('run', '') for s in steps[:cleanup]))
        checkout = next(s for s in steps if str(s.get('uses', '')).startswith('actions/checkout@'))
        self.assertEqual(checkout['with'], {'ref': "${{ inputs.ref || '' }}", 'persist-credentials': False})

    def test_safety_tests_are_enforced_in_ci(self):
        steps = workflow('ci.yml')['jobs']['release-scripts']['steps']
        commands = '\n'.join(s.get('run', '') for s in steps)
        self.assertIn('python3 -I -B scripts/test-macos-ci-cleanup.py', commands)
        self.assertIn('python3 -I -B scripts/test-macos-ci-workflows.py', commands)


if __name__ == '__main__':
    unittest.main()
