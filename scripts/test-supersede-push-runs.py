#!/usr/bin/env python3
"""Offline tests for scripts/supersede-push-runs.py (fake GitHub API)."""
import importlib.util
import io
from pathlib import Path
import subprocess
import unittest
from contextlib import redirect_stdout

SPEC = importlib.util.spec_from_file_location('supersede', Path(__file__).with_name('supersede-push-runs.py'))
supersede = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(supersede)

REPO = 'intent-hq/intentd'
A, B, C, D = ('a' * 40, 'b' * 40, 'c' * 40, 'd' * 40)
HISTORY = [A, B, C]  # linear main, oldest first


def run(run_id, sha, **extra):
    return {'id': run_id, 'head_sha': sha, 'event': 'push', 'head_branch': 'main',
            'path': '.github/workflows/ci.yml', **extra}


class FakeApi:
    def __init__(self, tip, runs, history=HISTORY, fail=()):
        self.tip, self.runs, self.history, self.fail = tip, runs, history, set(fail)
        self.cancelled = []

    def __call__(self, method, path):
        for marker in self.fail:
            if marker in path:
                raise subprocess.CalledProcessError(1, ['gh', 'api', path])
        if method == 'POST':
            self.cancelled.append(int(path.split('/')[-2]))
            return {}
        if path.endswith('git/ref/heads/main'):
            return {'object': {'sha': self.tip}}
        if '/compare/' in path:
            older, newer = path.rsplit('/', 1)[1].split('...')
            if older not in self.history or newer not in self.history:
                return {'status': 'diverged', 'ahead_by': 1, 'behind_by': 1}
            i, j = self.history.index(older), self.history.index(newer)
            if i == j:
                return {'status': 'identical', 'ahead_by': 0, 'behind_by': 0}
            return {'status': 'ahead', 'ahead_by': j - i, 'behind_by': 0} if j > i else \
                {'status': 'behind', 'ahead_by': 0, 'behind_by': i - j}
        if '/runs?' in path:
            status = path.split('status=')[1].split('&')[0]
            return {'workflow_runs': [r for r in self.runs if r.get('status', 'in_progress') == status]}
        raise AssertionError(path)


def scan(api, sha, run_id):
    return supersede.supersede(api, REPO, sha, run_id, log=lambda _m: None)


class SupersedeTests(unittest.TestCase):
    def test_tip_cancels_strictly_older_live_runs_and_is_not_superseded(self):
        api = FakeApi(C, [run(1, A), run(2, B, status='queued'), run(3, C)])
        self.assertFalse(scan(api, C, 3))
        self.assertEqual(sorted(api.cancelled), [1, 2])

    def test_late_older_run_never_cancels_the_tip_and_skips_its_windows(self):
        # Concurrency admission is unordered: B's run may start after C's.
        api = FakeApi(C, [run(3, C), run(2, B)])
        self.assertTrue(scan(api, B, 2))
        self.assertEqual(api.cancelled, [])

    def test_same_commit_and_own_run_are_never_cancelled(self):
        api = FakeApi(C, [run(3, C), run(4, C)])
        self.assertFalse(scan(api, C, 3))
        self.assertEqual(api.cancelled, [])

    def test_unrelated_runs_are_ignored(self):
        runs = [run(5, A, event='merge_group'), run(6, A, head_branch='feature'),
                run(7, A, path='.github/workflows/release-plz.yml'), run(8, D), run(9, 'not-a-sha')]
        api = FakeApi(C, runs)
        self.assertFalse(scan(api, C, 3))
        self.assertEqual(api.cancelled, [])

    def test_diverged_or_unknown_ancestry_is_never_superseded_or_cancelled(self):
        api = FakeApi(D, [run(1, A)], history=[A, C])
        self.assertFalse(scan(api, C, 3))
        api = FakeApi(C, [run(8, D)])
        self.assertFalse(scan(api, C, 3))
        self.assertEqual(api.cancelled, [])

    def test_api_failures_fail_open_to_running_windows(self):
        for marker in ('git/ref/heads/main', f'compare/{B}...{C}'):
            with self.subTest(marker=marker):
                api = FakeApi(C, [], fail={marker})
                self.assertFalse(scan(api, B, 2))
        api = FakeApi(C, [run(1, A), run(2, B)], fail={'/runs/1/cancel'})
        self.assertFalse(scan(api, C, 3))
        self.assertEqual(api.cancelled, [2])
        api = FakeApi(C, [run(1, A)], fail={'/runs?'})
        self.assertFalse(scan(api, C, 3))
        self.assertEqual(api.cancelled, [])

    def test_old_caller_after_reset_never_cancels_the_live_tip(self):
        # Main reset to B (or C's scan re-run later): only runs strictly
        # behind the validated live tip are retired, never the tip's.
        api = FakeApi(B, [run(1, A), run(2, B), run(3, C)])
        self.assertFalse(scan(api, C, 3))
        self.assertEqual(api.cancelled, [1])

    def test_unreadable_tip_cancels_nothing(self):
        api = FakeApi(C, [run(1, A), run(2, B)], fail={'git/ref/heads/main'})
        self.assertFalse(scan(api, C, 3))
        self.assertEqual(api.cancelled, [])

    def test_any_api_error_reports_not_superseded(self):
        # B is superseded by tip C, but any failed read or cancel must fall
        # back to running Windows on B.
        for marker in ('/runs?', f'compare/{A}...{C}', '/runs/1/cancel'):
            with self.subTest(marker=marker):
                api = FakeApi(C, [run(1, A)], fail={marker})
                self.assertFalse(scan(api, B, 2))
        self.assertTrue(scan(FakeApi(C, [run(1, A)]), B, 2))

    def test_main_prints_one_output_line(self):
        out = io.StringIO()
        with redirect_stdout(out):
            supersede.main(['--repo', REPO, '--sha', B, '--run-id', '2'], api=FakeApi(C, []))
        self.assertEqual(out.getvalue(), 'superseded=true\n')
        with self.assertRaises(ValueError):
            scan(FakeApi(C, []), 'HEAD', 2)


if __name__ == '__main__':
    unittest.main()
