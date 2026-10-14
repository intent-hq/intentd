#!/usr/bin/env python3
"""Permanent caller regressions; all transport and mutations are offline."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


T = load('fixtures', 'test-retire-obsolete-merge-runs.py')
C = load('caller', 'retire-obsolete-merge-runs-caller.py')
C.P = T.P
REPO = 'intent-hq/intentd'


class CallerTests(unittest.TestCase):
    def setUp(self):
        self.api = T.FakeGitHub(REPO)

    def sweep(self, **kwargs):
        return C.sweep(self.api, REPO, **kwargs)

    def test_probe_checks_protection_and_activity_when_no_runs(self):
        self.api.runs = {}
        result = self.sweep()
        self.assertTrue(result['probe']['protection']['ok'], result)
        self.assertTrue(result['probe']['activity']['ok'], result)
        self.assertEqual(result['probe']['protection']['rule_ids'], [1])
        self.assertTrue(any(path == '/graphql' for _, path, _ in self.api.calls))
        self.assertEqual(self.api.mutations, [])

    def test_probe_activity_still_runs_after_missing_bypass(self):
        del self.api.rules[1]['bypass_actors']
        result = self.sweep(apply=True)
        self.assertFalse(result['complete'])
        self.assertFalse(result['probe']['protection']['ok'])
        self.assertTrue(result['probe']['activity']['ok'])
        self.assertEqual(self.api.mutations, [])

    def test_unknown_activity_stops_empty_scan(self):
        self.api.runs = {}
        self.api.heads = [None]
        result = self.sweep(apply=True)
        self.assertTrue(result['probe']['protection']['ok'])
        self.assertFalse(result['probe']['activity']['ok'])
        self.assertFalse(result['complete'])
        self.assertEqual(self.api.mutations, [])

    def test_older_ineligible_batches_do_not_starve_later_run(self):
        self.api.runs = {i: self.api.make_run(i, T.H if i < 14 else T.S) for i in range(10, 15)}
        self.api.compares[(T.B, T.H)] = T.comparison(head=T.H, status='ahead')
        result = self.sweep(apply=True, max_batches=5, batch_size=1)
        self.assertTrue(result['complete'], result)
        self.assertEqual([d['id'] for b in result['batches'] for d in b['decisions']], list(range(10, 15)))
        self.assertEqual([m[1] for m in self.api.mutations], [self.api.root + '/actions/runs/14/cancel'])
        self.assertEqual(result['next_after_run_id'], 0)

    def test_bound_preserves_cursor_and_next_invocation_finishes(self):
        self.api.runs = {i: self.api.make_run(i) for i in range(10, 15)}
        first = self.sweep(max_batches=2, batch_size=1)
        self.assertFalse(first['complete'])
        self.assertEqual(first['next_after_run_id'], 11)
        second = self.sweep(after_run_id=first['next_after_run_id'], max_batches=3, batch_size=1)
        self.assertTrue(second['complete'])
        self.assertEqual([d['id'] for b in second['batches'] for d in b['decisions']], [12, 13, 14])
        self.assertEqual(second['next_after_run_id'], 0)

    def test_reset_finds_late_visible_older_id(self):
        first = self.sweep()
        self.api.runs[5] = self.api.make_run(5)
        second = self.sweep(after_run_id=first['next_after_run_id'])
        self.assertEqual([d['id'] for d in second['batches'][0]['decisions']], [5, 10])

    def test_reset_reconsiders_after_main_advances(self):
        self.api.compares[(T.B, T.S)] = T.comparison(status='ahead')
        first = self.sweep()
        self.assertEqual(first['batches'][0]['decisions'][0]['action'], 'skip')
        self.api.base = T.H
        self.api.compares[(T.H, T.S)] = T.comparison(base=T.H)
        second = self.sweep(after_run_id=first['next_after_run_id'])
        self.assertEqual(second['batches'][0]['decisions'][0]['action'], 'would-cancel')

    def test_incomplete_batch_is_not_retried_or_advanced(self):
        responses = [dict(complete=True, next_after_run_id=10, decisions=[]),
                     dict(complete=False, error='partial page', decisions=[])]
        with mock.patch.object(C.P, 'reconcile', side_effect=responses) as reconcile:
            result = self.sweep(max_batches=8)
        self.assertEqual(reconcile.call_count, 2)
        self.assertFalse(result['complete'])
        self.assertEqual(result['next_after_run_id'], 10)
        self.assertEqual(result['error'], 'partial page')

    def test_repeated_cursor_stops_without_busy_retry(self):
        with mock.patch.object(C.P, 'reconcile', return_value=dict(complete=True, next_after_run_id=10, decisions=[])) as reconcile:
            result = self.sweep(after_run_id=10)
        self.assertEqual(reconcile.call_count, 1)
        self.assertIn('cursor', result['error'])

    def test_total_api_budget_and_deadline_stop_before_mutation(self):
        for kwargs in ({'max_requests': 2}, {'seconds': 0}):
            with self.subTest(kwargs=kwargs):
                result = self.sweep(apply=True, **kwargs)
                self.assertFalse(result['complete'])
                self.assertEqual(self.api.mutations, [])
                self.assertLessEqual(len(self.api.calls), 2)
                self.api.calls.clear()

    def test_checkpoint_updates_each_completed_batch(self):
        self.api.runs = {i: self.api.make_run(i) for i in range(10, 13)}
        cursors = []
        self.sweep(max_batches=3, batch_size=1, checkpoint=cursors.append)
        self.assertEqual(cursors, [10, 11, 0])

    def test_checkpoint_round_trip_and_repository_binding(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'cursor.json'
            self.assertEqual(C.read_cursor(path, REPO, 'dry-run'), 0)
            C.write_cursor(path, REPO, 'dry-run', 42)
            self.assertEqual(C.read_cursor(path, REPO, 'dry-run'), 42)
            for repo, mode in ((REPO, 'apply'), ('intent-hq/cloudlands-fe', 'dry-run')):
                with self.assertRaises(ValueError):
                    C.read_cursor(path, repo, mode)
            path.write_text(json.dumps({'repository': REPO, 'mode': 'dry-run', 'after_run_id': True}))
            with self.assertRaises(ValueError):
                C.read_cursor(path, REPO, 'dry-run')

    def test_workflow_transport_selects_read_and_cancel_credentials(self):
        api = C.WorkflowApi(REPO, 'offline-read-token', 'offline-write-token')
        response = mock.Mock(stdout='HTTP/2.0 200 OK' + chr(10) * 2 + '{}', returncode=0)
        with mock.patch.object(C.subprocess, 'run', return_value=response) as run:
            api.request('GET', '/repos/' + REPO + '/rulesets/1')
            self.assertEqual(run.call_args.kwargs['env']['GH_TOKEN'], 'offline-read-token')
            api.request('POST', '/graphql', {'query': C.P.QUEUE_QUERY, 'variables': {}})
            self.assertEqual(run.call_args.kwargs['env']['GH_TOKEN'], 'offline-read-token')
            api.request('POST', '/repos/' + REPO + '/actions/runs/10/cancel')
            self.assertEqual(run.call_args.kwargs['env']['GH_TOKEN'], 'offline-write-token')
            self.assertNotIn('RETIREMENT_READ_TOKEN', run.call_args.kwargs['env'])
            self.assertNotIn('offline-write-token', str(run.call_args.args))

    def test_workflow_transport_refuses_other_mutations_or_missing_credentials(self):
        api = C.WorkflowApi(REPO, 'offline-read-token', 'offline-write-token')
        with mock.patch.object(C.subprocess, 'run') as run:
            for method, path, body in (
                ('POST', '/repos/other/repo/actions/runs/10/cancel', None),
                ('POST', '/repos/' + REPO + '/actions/runs/10/force-cancel', None),
                ('DELETE', '/repos/' + REPO + '/git/refs/heads/main', None),
                ('POST', '/graphql', {'query': 'mutation { nope }'}),
            ):
                with self.subTest(path=path), self.assertRaises(C.P.SafetyError):
                    api.request(method, path, body)
            run.assert_not_called()
        for read, write in (('', 'write'), ('read', '')):
            with self.assertRaises(C.P.SafetyError):
                C.WorkflowApi(REPO, read, write)

    def test_probe_transport_refuses_mutation(self):
        with self.assertRaises(T.P.SafetyError):
            C.ReadOnlyApi(self.api).request('POST', self.api.root + '/actions/runs/10/cancel')
        self.assertEqual(self.api.mutations, [])


if __name__ == '__main__':
    unittest.main()
