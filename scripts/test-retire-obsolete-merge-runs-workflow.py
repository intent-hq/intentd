#!/usr/bin/env python3
"""Cleanup trust boundary and existing CI contract regressions (offline)."""
from pathlib import Path
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[1]


def workflow(name):
    return yaml.safe_load((ROOT / '.github/workflows' / name).read_text())


class WorkflowTests(unittest.TestCase):
    def test_cleanup_only_trusted_events_default_main_and_local_repo(self):
        doc = workflow('retire-obsolete-merge-runs.yml')
        self.assertEqual(set(doc[True]), {'schedule', 'workflow_dispatch'})
        self.assertEqual(doc[True]['schedule'], [{'cron': '7,22,37,52 * * * *'}])
        self.assertEqual(doc['permissions'], {})
        self.assertEqual(doc['concurrency'], {'group': 'retire-obsolete-merge-runs', 'cancel-in-progress': False})
        self.assertEqual(set(doc['jobs']), {'cleanup'})
        job = doc['jobs']['cleanup']
        self.assertEqual(job['if'], "github.repository == 'intent-hq/intentd' && github.ref == 'refs/heads/main'")
        self.assertEqual(job['runs-on'], 'ubuntu-latest')
        self.assertEqual(job['permissions'], {'actions': 'write', 'contents': 'read', 'pull-requests': 'read'})
        self.assertEqual(job['timeout-minutes'], 10)
        checkout = job['steps'][0]
        self.assertEqual(checkout['with'], {'ref': 'main', 'persist-credentials': False})
        run = next(s for s in job['steps'] if s.get('id') == 'sweep')
        self.assertEqual(run['env']['GH_TOKEN'], '${{ github.token }}')
        self.assertEqual(run['env']['RETIREMENT_READ_TOKEN'], '${{ secrets.CI_QUEUE_READ_TOKEN }}')
        self.assertIn('--workflow-credentials', run['run'])
        self.assertIn('--repository intent-hq/intentd', run['run'])
        self.assertIn('--state .retirement/cursor.json', run['run'])
        self.assertIn('python3 -I -B', run['run'])
        self.assertIn('tee retirement-report.json', run['run'])
        # Deployment remains preview-only until actual workflow credential proof.
        self.assertIn('--dry-run', run['run'])
        self.assertNotIn('--apply', run['run'])

    def test_cursor_persists_even_when_later_batch_fails(self):
        steps = workflow('retire-obsolete-merge-runs.yml')['jobs']['cleanup']['steps']
        restore = next(s for s in steps if s.get('id') == 'cursor')
        save = next(s for s in steps if s.get('uses', '').startswith('actions/cache/save@'))
        self.assertEqual(restore['with']['path'], '.retirement/cursor.json')
        self.assertEqual(save['with']['path'], restore['with']['path'])
        self.assertIn('github.run_id', save['with']['key'])
        self.assertIn('github.run_attempt', save['with']['key'])
        self.assertEqual(save['if'], "always() && steps.sweep.outcome != 'skipped' && hashFiles('.retirement/cursor.json') != ''")
        self.assertIn('dry-run', restore['with']['restore-keys'])
        upload = next(s for s in steps if s.get('uses', '').startswith('actions/upload-artifact@'))
        self.assertEqual(upload['if'], 'always()')
        self.assertEqual(upload['with']['path'], 'retirement-report.json')

    def test_pr_probe_cannot_mutate_and_attempts_actual_token(self):
        doc = workflow('retirement-credential-probe.yml')
        self.assertEqual(set(doc[True]), {'pull_request'})
        self.assertEqual(doc['permissions'], {})
        job = doc['jobs']['probe']
        self.assertEqual(job['permissions'], {'actions': 'read', 'contents': 'read', 'pull-requests': 'read'})
        self.assertEqual(job['runs-on'], 'ubuntu-latest')
        self.assertEqual(job['if'], "github.repository == 'intent-hq/intentd' && github.event.pull_request.head.repo.full_name == github.repository")
        steps = job['steps']
        run = next(s for s in steps if s.get('id') == 'probe')
        self.assertEqual(run['env']['GH_TOKEN'], '${{ github.token }}')
        self.assertIn('--probe-only', run['run'])
        self.assertNotIn('secrets.', str(job))
        self.assertNotIn('--apply', str(doc))
        self.assertNotIn('actions/cache', str(doc))

    def test_existing_read_credential_probe_uses_reviewed_immutable_code(self):
        job = workflow('retirement-credential-probe.yml')['jobs']['existing-read-credential']
        self.assertEqual(job['permissions'], {'contents': 'read'})
        self.assertEqual(job['runs-on'], 'ubuntu-latest')
        self.assertEqual(job['steps'][0]['with'], {
            'ref': '62a2a110b7e217151280736acb7fd19a1daf3c71', 'persist-credentials': False})
        step = next(s for s in job['steps'] if s.get('id') == 'probe')
        self.assertEqual(step['env']['GH_TOKEN'], '${{ secrets.CI_QUEUE_READ_TOKEN }}')
        self.assertIn("for name in ('protection', 'activity')", step['run'])
        self.assertNotIn('reconcile(', step['run'])
        self.assertIn('read-only probe rejected request', step['run'])
        self.assertIn("body.get('query') == policy.QUEUE_QUERY", step['run'])

    def test_gate_and_protected_events_stay_unchanged(self):
        ci = workflow('ci.yml')
        self.assertEqual(set(ci[True]), {'push', 'pull_request', 'merge_group'})
        self.assertEqual(ci['concurrency']['cancel-in-progress'], "${{ github.event_name == 'pull_request' }}")
        self.assertEqual(set(ci['jobs']['gate']['needs']), {
            'daemon-lifecycle', 'check', 'build', 'coverage-e2e', 'coverage-all',
            'coverage-changed', 'pr-title', 'conflict-markers', 'monorepo-consumer-checks',
            'deb-packaging', 'release-scripts', 'install-ps1', 'pi-session-paths'})
        commands = '\n'.join(s.get('run', '') for s in ci['jobs']['release-scripts']['steps'])
        for test in ('test-retire-obsolete-merge-runs.py', 'test-retire-obsolete-merge-runs-caller.py',
                     'test-retire-obsolete-merge-runs-workflow.py'):
            self.assertIn('python3 -I -B scripts/' + test, commands)
        for name, job in ci['jobs'].items():
            self.assertNotIn('retire-obsolete', str(job.get('needs', '')))


if __name__ == '__main__':
    unittest.main()
