#!/usr/bin/env python3
"""Offline tests for scripts/merge-queue-fail-fast.py (fake GitHub API and clock)."""
import importlib.util
import io
from pathlib import Path
import subprocess
import unittest
from contextlib import redirect_stderr, redirect_stdout
from urllib.parse import parse_qs, urlsplit

SPEC = importlib.util.spec_from_file_location('fail_fast', Path(__file__).with_name('merge-queue-fail-fast.py'))
fail_fast = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fail_fast)

REPO = 'intent-hq/intentd'
RUN = 4242
SELF = fail_fast.SELF_NAME
GATE = fail_fast.GATE_NAME


def job(name, status='completed', conclusion='success', completed_at='2026-10-27T01:00:00Z', job_id=None):
    return {'id': job_id or abs(hash(name)) % 10**9, 'name': name, 'status': status,
            'conclusion': conclusion if status == 'completed' else None,
            'completed_at': completed_at if status == 'completed' else None}


def running(name):
    return job(name, status='in_progress')


class FakeApi:
    """Serves one jobs snapshot per poll (the last repeats); records POSTs."""

    def __init__(self, snapshots, fail_reads=0, fail_posts=0, events=None):
        self.snapshots, self.fail_reads, self.fail_posts = list(snapshots), fail_reads, fail_posts
        self.reads, self.posts, self.paths = 0, [], []
        self.events = events if events is not None else []

    def __call__(self, method, path):
        self.paths.append((method, path))
        if method == 'POST':
            self.events.append('POST')
            if self.fail_posts:
                self.fail_posts -= 1
                raise subprocess.CalledProcessError(1, ['gh', 'api', path])
            self.posts.append(path)
            return {}
        if self.fail_reads:
            self.fail_reads -= 1
            raise subprocess.CalledProcessError(1, ['gh', 'api', path])
        jobs = self.snapshots[min(self.reads, len(self.snapshots) - 1)]
        page = int(parse_qs(urlsplit(path).query).get('page', ['1'])[0])
        chunk = jobs[(page - 1) * 100:page * 100]
        if page * 100 >= len(jobs):
            self.reads += 1  # every page of one poll comes from the same snapshot
        return {'total_count': len(jobs), 'jobs': chunk}


class Clock:
    def __init__(self):
        self.now, self.sleeps = 0.0, []

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def watch(api, clock=None, **kwargs):
    clock = clock or Clock()
    lines = []
    result = fail_fast.watch(api, REPO, RUN, 1, log=lines.append, clock=clock.time, sleep=clock.sleep, **kwargs)
    return result, lines, clock


class DecideTests(unittest.TestCase):
    def test_running_jobs_keep_polling(self):
        self.assertEqual(fail_fast.decide([job(SELF, 'in_progress'), running('check'), job('route')]),
                         ('continue', None))

    def test_gate_visible_means_everything_else_finished(self):
        for status in ('queued', 'in_progress', 'completed'):
            with self.subTest(status=status):
                self.assertEqual(fail_fast.decide([job('route'), job(GATE, status)]), ('done', None))

    def test_a_listed_but_blocked_gate_does_not_end_the_watch(self):
        for status in ('pending', 'waiting', 'requested'):
            with self.subTest(status=status):
                self.assertEqual(fail_fast.decide([running('check'), job(GATE, status)]), ('continue', None))
                action, _ = fail_fast.decide([job('check', conclusion='failure'), job(GATE, status)])
                self.assertEqual(action, 'cancel')

    def test_every_fatal_conclusion_cancels(self):
        for conclusion in ('failure', 'timed_out', 'cancelled', 'startup_failure'):
            with self.subTest(conclusion=conclusion):
                action, failed = fail_fast.decide([job('route'), job('check', conclusion=conclusion), running('build')])
                self.assertEqual(action, 'cancel')
                self.assertEqual(failed['name'], 'check')

    def test_benign_conclusions_do_not_cancel(self):
        for conclusion in ('success', 'skipped', 'neutral'):
            with self.subTest(conclusion=conclusion):
                self.assertEqual(fail_fast.decide([job('windows', conclusion=conclusion), running('check')]),
                                 ('continue', None))

    def test_own_job_and_gate_are_never_the_trigger(self):
        jobs = [job(SELF, conclusion='failure'), job(GATE, conclusion='failure')]
        self.assertEqual(fail_fast.decide(jobs), ('done', None))
        self.assertEqual(fail_fast.decide([job(SELF, conclusion='cancelled'), running('check')]), ('continue', None))

    def test_earliest_failure_is_reported(self):
        jobs = [job('coverage-all', conclusion='failure', completed_at='2026-10-27T01:20:00Z'),
                job('Daemon lifecycle (macos)', conclusion='failure', completed_at='2026-10-27T01:05:00Z'),
                running('build')]
        self.assertEqual(fail_fast.decide(jobs)[1]['name'], 'Daemon lifecycle (macos)')

    def test_gate_presence_wins_over_a_failure(self):
        # A failed job with the gate already queued still cancels nothing: the
        # gate is about to report, so cancelling would only race it.
        self.assertEqual(fail_fast.decide([job('check', conclusion='failure'), job(GATE, 'queued')]), ('done', None))


class WatchTests(unittest.TestCase):
    def test_passing_run_exits_without_cancelling(self):
        api = FakeApi([[running('route')], [job('route'), running('check')], [job('route'), job('check'), job(GATE, 'queued')]])
        result, _, clock = watch(api)
        self.assertEqual(result, 'done')
        self.assertEqual(api.posts, [])
        self.assertEqual(clock.sleeps, [fail_fast.POLL_SECONDS] * 2)

    def test_failure_cancels_once_and_names_the_job(self):
        api = FakeApi([[running('check')], [job('check', conclusion='failure'), running('build (aarch64-apple-darwin)')]])
        result, lines, _ = watch(api)
        self.assertEqual(result, 'cancelled')
        self.assertEqual(api.posts, [f'repos/{REPO}/actions/runs/{RUN}/cancel'])
        self.assertTrue(any(l.startswith('::error::') and "'check'" in l for l in lines), lines)

    def test_explanation_is_written_before_the_cancel_request(self):
        # Cancelling the run cancels this step too, so later output can be lost.
        events = []
        api = FakeApi([[job('check', conclusion='failure')]], events=events)
        fail_fast.watch(api, REPO, RUN, 1, log=lambda m: events.append(m.split('::')[1] if m.startswith('::') else 'log'),
                        clock=Clock().time, sleep=lambda _s: None)
        self.assertLess(events.index('error'), events.index('POST'), events)

    def test_reads_only_the_current_attempt(self):
        api, clock = FakeApi([[job(GATE, 'queued')]]), Clock()
        fail_fast.watch(api, REPO, RUN, 3, log=lambda _m: None, clock=clock.time, sleep=clock.sleep)
        self.assertTrue(all(f'/runs/{RUN}/attempts/3/jobs' in p for m, p in api.paths if m == 'GET'), api.paths)

    def test_pages_of_one_poll_come_from_one_snapshot(self):
        first = [running('check')] + [job(f'leg {i}') for i in range(150)]
        second = [job('check', conclusion='failure')] + [job(f'leg {i}') for i in range(150)]
        api = FakeApi([first, second])
        result, _, clock = watch(api)
        self.assertEqual(result, 'cancelled')
        self.assertEqual(clock.sleeps, [fail_fast.POLL_SECONDS])
        self.assertEqual(api.reads, 2)

    def test_malformed_jobs_page_counts_as_a_read_error(self):
        class Malformed(FakeApi):
            def __call__(self, method, path):
                if method == 'GET' and self.fail_reads:
                    self.fail_reads -= 1
                    return {'total_count': 1, 'jobs': ['not a job']}
                return super().__call__(method, path)
        api = Malformed([[job('check', conclusion='failure')]], fail_reads=2)
        self.assertEqual(watch(api)[0], 'cancelled')

    def test_paginates_beyond_one_hundred_jobs(self):
        jobs = [job(f'leg {i}') for i in range(150)] + [job('late failure', conclusion='failure')]
        api = FakeApi([jobs])
        result, _, _ = watch(api)
        self.assertEqual(result, 'cancelled')
        self.assertIn(('GET', f'repos/{REPO}/actions/runs/{RUN}/attempts/1/jobs?per_page=100&page=2'), api.paths)

    def test_dry_run_never_posts(self):
        api = FakeApi([[job('check', conclusion='failure')]])
        result, lines, _ = watch(api, dry_run=True)
        self.assertEqual(result, 'would-cancel')
        self.assertEqual(api.posts, [])
        self.assertTrue(any('would cancel' in l for l in lines))

    def test_once_reads_a_single_snapshot(self):
        api = FakeApi([[running('check')]])
        result, _, clock = watch(api, once=True)
        self.assertEqual(result, 'continue')
        self.assertEqual(api.reads, 1)
        self.assertEqual(clock.sleeps, [])

    def test_transient_read_errors_are_retried(self):
        self.assertGreaterEqual(fail_fast.MAX_READ_ERRORS, 3, 'one API blip must not disable the watcher')
        api = FakeApi([[job('check', conclusion='failure')]], fail_reads=fail_fast.MAX_READ_ERRORS - 1)
        result, _, _ = watch(api)
        self.assertEqual(result, 'cancelled')

    def test_persistent_read_errors_give_up_quietly(self):
        api = FakeApi([[job('check', conclusion='failure')]], fail_reads=fail_fast.MAX_READ_ERRORS)
        result, lines, _ = watch(api)
        self.assertEqual(result, 'gave-up')
        self.assertEqual(api.posts, [])
        self.assertTrue(any(l.startswith('::warning::') for l in lines))

    def test_cancel_post_is_retried_then_abandoned(self):
        api = FakeApi([[job('check', conclusion='failure')]], fail_posts=1)
        result, _, clock = watch(api)
        self.assertEqual(result, 'cancelled')
        self.assertEqual(clock.sleeps, [fail_fast.CANCEL_BACKOFF_SECONDS[0]], 'retries back off, not back to back')
        api = FakeApi([[job('check', conclusion='failure')]], fail_posts=fail_fast.MAX_POST_ATTEMPTS)
        result, lines, _ = watch(api)
        self.assertEqual(result, 'gave-up')
        self.assertTrue(any(l.startswith('::warning::') for l in lines))

    def test_deadline_exits_before_the_job_timeout(self):
        api = FakeApi([[running('coverage-all')]])
        result, _, clock = watch(api, deadline_seconds=300)
        self.assertEqual(result, 'deadline')
        self.assertLessEqual(clock.now, 300)


class MainTests(unittest.TestCase):
    def run_main(self, argv, api):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            clock = Clock()
            code = fail_fast.main(argv, api=api, clock=clock.time, sleep=clock.sleep)
        return code, out.getvalue() + err.getvalue()

    def test_always_exits_zero(self):
        base = ['--repo', REPO, '--run-id', str(RUN), '--attempt', '1']
        for api in (FakeApi([[job('check', conclusion='failure')]]),
                    FakeApi([[job(GATE, 'queued')]]),
                    FakeApi([[job('check', conclusion='failure')]], fail_reads=99)):
            with self.subTest(api=api.snapshots):
                self.assertEqual(self.run_main(base, api)[0], 0)

    def test_an_unexpected_bug_still_exits_zero_with_a_warning(self):
        def broken(_method, _path):
            raise RuntimeError('boom')
        code, output = self.run_main(['--repo', REPO, '--run-id', str(RUN), '--attempt', '1'], broken)
        self.assertEqual(code, 0)
        self.assertIn('::warning::', output)
        self.assertIn('outcome=error', output)

    def test_gh_failures_carry_stderr(self):
        import unittest.mock
        err = subprocess.CalledProcessError(1, ['gh'], stderr='HTTP 403: Resource not accessible by integration')
        with unittest.mock.patch.object(fail_fast.subprocess, 'run', side_effect=err):
            with self.assertRaisesRegex(subprocess.SubprocessError, 'HTTP 403'):
                fail_fast.gh_api('POST', 'repos/x/y/actions/runs/1/cancel')

    def test_dry_run_flag_reaches_watch(self):
        api = FakeApi([[job('check', conclusion='failure')]])
        code, output = self.run_main(['--repo', REPO, '--run-id', str(RUN), '--attempt', '1', '--once', '--dry-run'], api)
        self.assertEqual(code, 0)
        self.assertEqual(api.posts, [])
        self.assertIn("would cancel: 'check'", output)

    def test_summary_is_written_on_cancel(self):
        import os
        import tempfile
        with tempfile.NamedTemporaryFile('r', suffix='.md') as summary:
            old = os.environ.get('GITHUB_STEP_SUMMARY')
            os.environ['GITHUB_STEP_SUMMARY'] = summary.name
            try:
                self.run_main(['--repo', REPO, '--run-id', str(RUN), '--attempt', '1'],
                              FakeApi([[job('check', conclusion='failure')]]))
            finally:
                if old is None:
                    os.environ.pop('GITHUB_STEP_SUMMARY')
                else:
                    os.environ['GITHUB_STEP_SUMMARY'] = old
            text = summary.read()
        self.assertIn('`check`', text)

    def test_summary_lists_jobs_cut_short(self):
        import os
        import tempfile
        with tempfile.NamedTemporaryFile('r', suffix='.md') as summary:
            old = os.environ.get('GITHUB_STEP_SUMMARY')
            os.environ['GITHUB_STEP_SUMMARY'] = summary.name
            try:
                self.run_main(['--repo', REPO, '--run-id', str(RUN), '--attempt', '1'],
                              FakeApi([[job('check', conclusion='failure'), running('coverage-all'), running(SELF)]]))
            finally:
                if old is None:
                    os.environ.pop('GITHUB_STEP_SUMMARY')
                else:
                    os.environ['GITHUB_STEP_SUMMARY'] = old
            text = summary.read()
        self.assertIn('`coverage-all`', text)
        self.assertNotIn(f'`{SELF}`', text)


class WorkflowTests(unittest.TestCase):
    """Pin the ci.yml wiring the script's safety argument depends on."""

    @classmethod
    def setUpClass(cls):
        import yaml
        cls.jobs = yaml.safe_load((Path(__file__).resolve().parents[1] / '.github/workflows/ci.yml').read_text())['jobs']
        cls.job = cls.jobs['fail-fast']

    def test_runs_only_in_the_merge_queue_on_a_free_hosted_runner(self):
        self.assertEqual(self.job['if'], "github.event_name == 'merge_group'")
        self.assertEqual(self.job['runs-on'], 'ubuntu-24.04')
        self.assertNotIn('needs', self.job, 'must start at once, not behind route')

    def test_job_name_matches_the_script(self):
        self.assertEqual(self.job['name'], fail_fast.SELF_NAME)
        self.assertEqual(self.jobs['gate']['name'], fail_fast.GATE_NAME)

    def test_never_gates_and_gate_still_runs_after_a_cancel(self):
        self.assertNotIn('fail-fast', self.jobs['gate']['needs'])
        self.assertTrue(self.jobs['gate']['if'].lstrip('${ ').startswith('always()'), self.jobs['gate']['if'])

    def test_least_privilege(self):
        self.assertEqual(self.job['permissions'], {'actions': 'write', 'contents': 'read'})
        writers = sorted(name for name, j in self.jobs.items() if (j.get('permissions') or {}).get('actions') == 'write')
        self.assertEqual(writers, ['fail-fast', 'supersede'])

    def test_checks_out_only_its_script_and_runs_it(self):
        checkout, step = self.job['steps']
        self.assertTrue(checkout['uses'].startswith('actions/checkout@'))
        self.assertIs(checkout['with']['persist-credentials'], False)
        self.assertEqual(checkout['with']['sparse-checkout'], 'scripts/merge-queue-fail-fast.py')
        self.assertIs(checkout['with']['sparse-checkout-cone-mode'], False)
        self.assertEqual(step['env'], {'GH_TOKEN': '${{ github.token }}'})
        self.assertEqual(step['run'], 'python3 -I -B scripts/merge-queue-fail-fast.py --repo "$GITHUB_REPOSITORY" '
                                      '--run-id "$GITHUB_RUN_ID" --attempt "$GITHUB_RUN_ATTEMPT"')
        self.assertNotIn('--dry-run', step['run'])

    def test_script_deadline_ends_before_the_job_timeout(self):
        budget = self.job['timeout-minutes'] * 60
        self.assertLessEqual(fail_fast.DEADLINE_SECONDS + fail_fast.POLL_SECONDS + 4 * 60, budget)

    def test_release_scripts_runs_these_tests(self):
        commands = '\n'.join(s.get('run', '') for s in self.jobs['release-scripts']['steps'])
        self.assertIn('python3 -I -B scripts/test-merge-queue-fail-fast.py', commands)


if __name__ == '__main__':
    unittest.main()
