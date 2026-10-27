#!/usr/bin/env python3
"""Offline tests for scripts/release-plz-rerun.py and its workflow wiring.

Run: python3 -I -B scripts/test-release-plz-rerun.py   (wiring tests need PyYAML)
"""
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('rerun', ROOT / 'scripts/release-plz-rerun.py')
rerun = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rerun)

HEAD = 'a' * 40
OLD = 'b' * 40
REPO = 'intent-hq/intentd'
LIST = (f'repos/{REPO}/actions/workflows/release-plz.yml/runs?branch=main&event=push&head_sha={HEAD}'
        '&per_page=100')


def run(number=10, attempt=1, status='completed', conclusion='failure', head=HEAD, run_id=None, **extra):
    return {'id': run_id or 1000 + number, 'run_number': number, 'run_attempt': attempt, 'status': status,
            'conclusion': conclusion, 'head_sha': head, 'head_branch': 'main', 'event': 'push',
            'path': '.github/workflows/release-plz.yml',
            'html_url': f'https://github.com/{REPO}/actions/runs/{run_id or 1000 + number}', **extra}


# Run 40430078531's planning failure, as GitHub serves the job log.
NETWORK_LOG = ('2026-10-27T01:03:38.7551171Z     2: cargo package failed: "...Caused by:\\n  download of '
               'config.json failed\\n\\nCaused by:\\n  curl failed\\n\\nCaused by:\\n  [16] Error in the '
               'HTTP2 framing layer\\n"\n')
COMPILE_LOG = '2026-10-27T01:03:38Z error[E0425]: cannot find value `x` in this scope\nError: failed to update packages\n'


def jobs(planning='failure', release='success', probe='success', marker='skipped'):
    return [{'name': 'runner-route / probe', 'conclusion': probe},
            {'name': 'Release-plz release', 'conclusion': release},
            {'id': 77, 'name': 'Release-plz PR', 'conclusion': planning},
            {'name': 'Release-plz no release needed: ${{ needs.release-plz-pr.outputs.baseline_tag }}@x',
             'conclusion': marker}]


class FakeApi:
    def __init__(self, runs, job_rows=None, head=HEAD, fail=None, after_first=None, log=NETWORK_LOG):
        self.runs, self.job_rows, self.head, self.fail = runs, job_rows, head, fail
        self.after_first, self.log = after_first, log
        self.calls = []

    def __call__(self, endpoint, method='GET', raw=False):
        self.calls.append((method, endpoint))
        if self.fail and self.fail in endpoint:
            raise rerun.ApiError(f'boom {endpoint}')
        if method == 'POST':
            return None
        if endpoint.endswith('/logs'):
            assert raw
            return self.log
        if endpoint.endswith('/git/ref/heads/main'):
            heads = [call for call in self.calls if call[1].endswith('/git/ref/heads/main')]
            if len(heads) > 1 and self.after_first:
                self.after_first(self)
            return {'object': {'sha': self.head}}
        if '/actions/workflows/release-plz.yml/runs' in endpoint:
            return {'total_count': len(self.runs), 'workflow_runs': self.runs}
        if '/jobs' in endpoint:
            return {'jobs': self.job_rows}
        raise AssertionError(endpoint)

    def posts(self):
        return [endpoint for method, endpoint in self.calls if method == 'POST']


class Decide(unittest.TestCase):
    def decide(self, latest, job_rows=None, log=NETWORK_LOG):
        return rerun.decide(HEAD, latest, job_rows, log)

    def test_no_run_yet(self):
        self.assertEqual(self.decide(None)[0], rerun.NONE)

    def test_in_progress_and_queued_runs_wait(self):
        for status in ('queued', 'in_progress', 'waiting'):
            with self.subTest(status):
                self.assertEqual(self.decide(run(status=status, conclusion=None))[0], rerun.NONE)

    def test_success_needs_nothing(self):
        self.assertEqual(self.decide(run(conclusion='success'))[0], rerun.NONE)

    def test_abnormal_conclusions_are_loud_and_never_rerun(self):
        # A cancelled or never-started run leaves no marker either: the
        # frontend stays blocked, so it must not pass silently.
        for conclusion in ('cancelled', 'startup_failure', 'action_required', 'stale', 'neutral', 'skipped'):
            with self.subTest(conclusion):
                action, message = self.decide(run(conclusion=conclusion), jobs(planning='cancelled'))
                self.assertEqual(action, rerun.LOUD)
                self.assertIn(conclusion, message)
                self.assertIn('gh run rerun 1010 --failed', message)

    def test_non_network_planning_failure_is_loud_at_once(self):
        for log in (COMPILE_LOG, '', None):
            with self.subTest(log=log):
                action, message = self.decide(run(attempt=1), jobs(), log)
                self.assertEqual(action, rerun.LOUD)
                self.assertIn('not a recognised network error', message)

    def test_network_signatures_are_recognised(self):
        for line in ('curl failed', '[16] Error in the HTTP2 framing layer', '[28] Timeout was reached',
                     'warning: spurious network error (3 tries remaining): [35] SSL connect error',
                     'failed to download from `https://static.crates.io/crates/x/1.0.0/download`',
                     'error sending request for url (https://api.github.com/)', 'Connection reset by peer',
                     '[56] Failure when receiving data from the peer', 'failed to get 200 response from'):
            with self.subTest(line):
                self.assertEqual(self.decide(run(), jobs(), f'Caused by:\n  {line}\n')[0], rerun.RERUN)

    def test_planning_only_failure_under_the_cap_reruns(self):
        for attempt in (1, 2):
            for conclusion in ('failure', 'timed_out'):
                with self.subTest(attempt=attempt, conclusion=conclusion):
                    action, message = self.decide(run(attempt=attempt, conclusion=conclusion), jobs())
                    self.assertEqual(action, rerun.RERUN)
                    self.assertIn(f'attempt {attempt} of 3', message)

    def test_planning_timeout_counts_as_a_planning_failure(self):
        # A hung network call times the job out without a curl error.
        self.assertEqual(self.decide(run(), jobs(planning='timed_out'), '')[0], rerun.RERUN)

    def test_failure_at_the_cap_is_loud(self):
        action, message = self.decide(run(attempt=3), jobs())
        self.assertEqual(action, rerun.LOUD)
        self.assertIn('after 3 attempts', message)
        self.assertIn('gh run rerun 1010 --failed', message)
        self.assertIn('intentd-skew', message)

    def test_release_job_failure_is_never_replayed(self):
        for job_rows in (jobs(release='failure', planning='skipped'), jobs(release='failure'),
                         jobs(release='cancelled', planning='skipped')):
            with self.subTest(job_rows=job_rows):
                action, message = self.decide(run(), job_rows)
                self.assertEqual(action, rerun.LOUD)
                self.assertIn('tag publication is never replayed', message)

    def test_other_failures_are_loud(self):
        cases = {
            'probe': jobs(probe='failure', release='skipped', planning='skipped'),
            'marker': jobs(planning='success', marker='failure'),
            'nothing failed': jobs(planning='success'),
            'release missing': [row for row in jobs() if row['name'] != 'Release-plz release'],
            'release skipped': jobs(release='skipped'),
        }
        for name, job_rows in cases.items():
            with self.subTest(name):
                self.assertEqual(self.decide(run(), job_rows)[0], rerun.LOUD)

    def test_unreadable_jobs_raise(self):
        for job_rows in (None, 'x', [{'conclusion': 'failure'}]):
            with self.subTest(job_rows=job_rows), self.assertRaises(rerun.ApiError):
                self.decide(run(), job_rows)


class GhApi(unittest.TestCase):
    def test_timeout_stderr_bytes_are_decoded(self):
        import subprocess
        from unittest import mock
        error = subprocess.TimeoutExpired(['gh'], 30, stderr=b'partial diagnostics')
        with mock.patch.object(rerun.subprocess, 'run', side_effect=error), \
                self.assertRaisesRegex(rerun.ApiError, r"failed: partial diagnostics$"):
            rerun.gh_api('repos/x')

    def test_raw_returns_text_and_allows_escape_sequences(self):
        from unittest import mock
        done = mock.Mock(stdout='\x1b[1mlog\n')
        with mock.patch.object(rerun.subprocess, 'run', return_value=done) as run_:
            self.assertEqual(rerun.gh_api('repos/x/logs', raw=True), '\x1b[1mlog\n')
        self.assertIn('--allow-escape-sequences', run_.call_args.args[0])


class LatestRun(unittest.TestCase):
    def test_newest_run_number_wins(self):
        self.assertEqual(rerun.latest_run([run(9), run(11), run(10)], HEAD)['run_number'], 11)

    def test_empty_listing(self):
        self.assertIsNone(rerun.latest_run([], HEAD))

    def test_untrusted_rows_raise(self):
        for bad in (run(head=OLD), run(event='workflow_dispatch'), run(head_branch='other'),
                    run(path='.github/workflows/ci.yml'), {**run(), 'run_attempt': None}):
            with self.subTest(bad=bad), self.assertRaises(rerun.ApiError):
                rerun.latest_run([run(), bad], HEAD)
        with self.assertRaises(rerun.ApiError):
            rerun.latest_run(None, HEAD)


class Main(unittest.TestCase):
    def main(self, api, *argv):
        lines = []
        with tempfile.TemporaryDirectory() as tmp:
            summary = Path(tmp) / 'summary.md'
            old = os.environ.get('GITHUB_STEP_SUMMARY')
            os.environ['GITHUB_STEP_SUMMARY'] = str(summary)
            try:
                code = rerun.main(list(argv), api=api, out=lines.append)
            finally:
                if old is None:
                    os.environ.pop('GITHUB_STEP_SUMMARY')
                else:
                    os.environ['GITHUB_STEP_SUMMARY'] = old
            return code, lines, summary.read_text() if summary.exists() else ''

    def test_rerun_posts_rerun_failed_jobs_for_the_assessed_attempt(self):
        api = FakeApi([run(attempt=1)], jobs())
        code, lines, summary = self.main(api)
        self.assertEqual(code, 0)
        self.assertEqual(api.posts(), [f'repos/{REPO}/actions/runs/1010/rerun-failed-jobs'])
        self.assertIn(f'repos/{REPO}/actions/runs/1010/attempts/1/jobs?per_page=100', [c[1] for c in api.calls])
        self.assertIn(LIST, [c[1] for c in api.calls])
        self.assertTrue(lines[-1].startswith('::warning::'))
        self.assertIn('re-running', summary)

    def test_reads_the_failed_planning_job_log(self):
        api = FakeApi([run(attempt=1)], jobs())
        self.main(api, '--dry-run')
        self.assertIn(f'repos/{REPO}/actions/jobs/77/logs', [c[1] for c in api.calls])

    def test_non_network_failure_fails_without_posting(self):
        api = FakeApi([run(attempt=1)], jobs(), log=COMPILE_LOG)
        code, lines, _ = self.main(api)
        self.assertEqual((code, api.posts()), (1, []))
        self.assertIn('not a recognised network error', lines[-1])

    def test_refused_rerun_request_names_the_run_and_manual_command(self):
        api = FakeApi([run(attempt=1)], jobs(), fail='/rerun-failed-jobs')
        code, lines, _ = self.main(api)
        self.assertEqual(code, 1)
        self.assertNotIn('Could not check', lines[-1])
        self.assertIn('refused the re-run', lines[-1])
        self.assertIn(f'https://github.com/{REPO}/actions/runs/1010', lines[-1])
        self.assertIn('gh run rerun 1010 --failed', lines[-1])

    def test_unreadable_log_is_loud(self):
        api = FakeApi([run(attempt=1)], jobs(), fail='/logs')
        code, lines, _ = self.main(api)
        self.assertEqual((code, api.posts()), (1, []))
        self.assertIn('Could not check', lines[-1])

    def test_dry_run_never_posts(self):
        api = FakeApi([run(attempt=1)], jobs())
        code, lines, _ = self.main(api, '--dry-run')
        self.assertEqual((code, api.posts()), (0, []))
        self.assertIn('dry run', lines[-1])

    def test_loud_decision_fails_without_posting(self):
        api = FakeApi([run(attempt=3)], jobs())
        code, lines, summary = self.main(api)
        self.assertEqual((code, api.posts()), (1, []))
        self.assertTrue(lines[-1].startswith('::error::'))
        self.assertIn('Action needed', summary)

    def test_success_and_missing_runs_skip_the_jobs_lookup(self):
        for runs in ([], [run(conclusion='success')], [run(status='in_progress', conclusion=None)]):
            with self.subTest(runs=runs):
                api = FakeApi(runs)
                self.assertEqual(self.main(api)[0], 0)
                self.assertFalse(any('/jobs' in endpoint for _, endpoint in api.calls))
                self.assertEqual(api.posts(), [])

    def test_api_errors_are_loud_and_never_post(self):
        for fail in ('/git/ref/heads/main', '/runs?', '/jobs?'):
            with self.subTest(fail=fail):
                api = FakeApi([run()], jobs(), fail=fail)
                code, lines, _ = self.main(api)
                self.assertEqual((code, api.posts()), (1, []))
                self.assertIn('Could not check intentd Release-plz runs', lines[-1])

    def test_unreadable_head_is_loud(self):
        api = FakeApi([run()], jobs(), head='not-a-sha')
        self.assertEqual(self.main(api)[0], 1)

    def test_change_during_assessment_skips_the_rerun(self):
        def moved_head(api):
            api.head = OLD
            api.runs = [run(head=OLD)]

        def new_attempt(api):
            api.runs = [run(attempt=2, status='queued', conclusion=None)]

        for change in (moved_head, new_attempt):
            with self.subTest(change.__name__):
                api = FakeApi([run()], jobs(), after_first=change)
                code, lines, _ = self.main(api)
                self.assertEqual((code, api.posts()), (0, []))
                self.assertIn('changed while assessing', lines[-1])


class Workflow(unittest.TestCase):
    def setUp(self):
        import yaml
        self.text = (ROOT / '.github/workflows/release-plz-rerun.yml').read_text()
        self.doc = yaml.safe_load(self.text)
        self.events = self.doc.get('on', self.doc.get(True))

    def test_triggers_on_release_plz_completion_with_a_schedule_backstop(self):
        self.assertEqual(self.events['workflow_run'],
                         {'workflows': ['Release-plz'], 'types': ['completed'], 'branches': ['main']})
        self.assertTrue(self.events['schedule'])
        self.assertIn('workflow_dispatch', self.events)
        import yaml
        release = yaml.safe_load((ROOT / '.github/workflows/release-plz.yml').read_text())
        self.assertEqual(release['name'], 'Release-plz')

    def test_least_privilege_and_single_flight(self):
        self.assertEqual(self.doc['permissions'], {})
        job = self.doc['jobs']['rerun']
        self.assertEqual(job['permissions'], {'actions': 'write', 'contents': 'read'})
        self.assertEqual(self.doc['concurrency'], {'group': 'release-plz-rerun', 'cancel-in-progress': False})
        self.assertNotIn('secrets.', self.text, 'only the scoped GITHUB_TOKEN is used')
        self.assertIs(job['steps'][0]['with']['persist-credentials'], False)

    def test_runs_the_policy_script(self):
        job = self.doc['jobs']['rerun']
        step = job['steps'][-1]
        self.assertEqual(step['run'], 'python3 -I -B scripts/release-plz-rerun.py')
        self.assertEqual(step['env'], {'GH_TOKEN': '${{ github.token }}'})
        self.assertIn("github.repository_owner == 'intent-hq'", job['if'])
        self.assertIn("github.event.workflow_run.conclusion != 'success'", job['if'])
        self.assertEqual(job['steps'][0]['with']['sparse-checkout'], 'scripts/release-plz-rerun.py')

    def test_policy_names_match_release_plz_jobs(self):
        import yaml
        release = yaml.safe_load((ROOT / '.github/workflows/release-plz.yml').read_text())['jobs']
        self.assertEqual(release['release-plz-pr']['name'], rerun.PLANNING_JOB)
        self.assertEqual(release['release-plz-release']['name'], rerun.RELEASE_JOB)
        self.assertEqual(rerun.WORKFLOW_PATH, '.github/workflows/release-plz.yml')


if __name__ == '__main__':
    unittest.main()
