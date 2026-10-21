#!/usr/bin/env python3
"""Offline transport-level regressions; every cancellation uses a fake API."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest import mock
from urllib.parse import parse_qs, urlsplit

ISOLATION_SPEC = importlib.util.spec_from_file_location(
    'retirement_test_isolation', Path(__file__).with_name('retirement-test-isolation.py'))
ISOLATION = importlib.util.module_from_spec(ISOLATION_SPEC)
ISOLATION_SPEC.loader.exec_module(ISOLATION)
ISOLATION.install()


SCRIPT = Path(__file__).with_name('retire-obsolete-merge-runs.py')
SPEC = importlib.util.spec_from_file_location('retirement', SCRIPT)
P = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(P)
B, S, C, H = ('b' * 40, 'a' * 40, 'c' * 40, 'd' * 40)


def comparison(base=B, head=S, status='diverged'):
    shapes = {'behind': (0, 2, head), 'diverged': (1, 2, C),
              'ahead': (2, 0, base), 'identical': (0, 0, base)}
    ahead, behind, common = shapes[status]
    return dict(base_commit={'sha': base}, merge_base_commit={'sha': common},
                status=status, ahead_by=ahead, behind_by=behind, total_commits=ahead)


class FakeGitHub:
    def __init__(self, repo):
        self.repo = repo
        self.path = 'ci.yml' if repo.endswith('/intentd') else 'intent-pr.yml'
        self.root = '/repos/' + repo
        self.metadata = dict(id=77, full_name=repo, default_branch='main')
        self.workflow = dict(id=123, path='.github/workflows/' + self.path, state='active')
        self.runs = {10: self.make_run(10)}
        self.list_pages = None
        self.refs = []
        self.heads = []
        self.queue_pages = None
        self.rules = {
            1: dict(id=1, target='branch', source_type='Organization', source='intent-hq',
                    enforcement='active', bypass_actors=[],
                    conditions={'ref_name': {'include': ['~DEFAULT_BRANCH'], 'exclude': []}},
                    rules=[{'type': t} for t in ('deletion', 'non_fast_forward', 'required_linear_history')]),
            2: dict(id=2, target='branch', source_type='Repository', source=repo,
                    enforcement='active', bypass_actors=[{'actor_type': 'Integration', 'actor_id': 42, 'bypass_mode': 'always'}],
                    conditions={'ref_name': {'include': ['refs/heads/main'], 'exclude': []}},
                    rules=[{'type': 'merge_queue', 'parameters': {'merge_method': 'SQUASH'}}]),
        }
        self.effective = [dict(type=r['type'], ruleset_id=key,
                               ruleset_source_type=rule['source_type'], ruleset_source=rule['source'])
                          for key, rule in self.rules.items() for r in rule['rules']]
        self.base = B
        self.compares = {(B, S): comparison()}
        self.calls, self.mutations, self.counts = [], [], {}
        self.override = None
        self.before_cancel = None
        self.cancel_status = 202

    def make_run(self, number, sha=S):
        return dict(id=number, run_attempt=1, check_suite_id=100 + number,
                    workflow_id=123, path='.github/workflows/' + self.path,
                    event='merge_group', repository={'id': 77, 'full_name': self.repo},
                    head_repository={'id': 77, 'full_name': self.repo},
                    head_branch='gh-readonly-queue/main/pr-2400-' + C,
                    head_sha=sha, status='queued', conclusion=None)

    def queue_body(self, heads, more=False, cursor=None):
        return {'data': {'repository': {'databaseId': 77, 'nameWithOwner': self.repo,
            'mergeQueue': {'entries': {
                'nodes': [{'id': 'MQE_' + str(i), 'state': 'QUEUED' if sha is None else 'AWAITING_CHECKS',
                           'headCommit': None if sha is None else {'oid': sha},
                           'pullRequest': {'baseRefName': 'main'}} for i, sha in enumerate(heads)],
                'pageInfo': {'hasNextPage': more, 'endCursor': cursor}}}}}}

    def response(self, body, status=200, next_url=None):
        headers = {} if next_url is None else {'link': '<' + next_url + '>; rel="next"'}
        return P.Response(status, copy.deepcopy(body), headers)

    def request(self, method, path, body=None):
        self.calls.append((method, path, copy.deepcopy(body)))
        key = (method, path)
        self.counts[key] = self.counts.get(key, 0) + 1
        if self.override:
            result = self.override(method, path, body, self.counts[key])
            if result is not None:
                return result
        if method == 'POST' and path.endswith('/cancel'):
            number = int(path.split('/')[-2])
            if self.before_cancel:
                self.before_cancel(number)
            self.mutations.append((method, path, self.runs[number]['run_attempt'], self.runs[number]['head_sha']))
            if self.cancel_status == 202:
                self.runs[number].update(status='completed', conclusion='cancelled')
            return self.response(None, self.cancel_status)
        if path == '/graphql':
            assert method == 'POST' and body['query'].lstrip().startswith('query')
            cursor = body['variables'].get('cursor')
            index = 0 if cursor is None else int(cursor)
            if self.queue_pages:
                return self.response(self.queue_pages[index])
            return self.response(self.queue_body(self.heads))
        assert method == 'GET', (method, path)
        parsed = urlsplit(path)
        endpoint = parsed.path
        query = parse_qs(parsed.query)
        if endpoint == self.root:
            return self.response(self.metadata)
        if endpoint == self.root + '/actions/workflows/' + self.path:
            return self.response(self.workflow)
        if endpoint == self.root + '/actions/workflows/123/runs':
            status = query['status'][0]
            index = int(query.get('page', ['1'])[0]) - 1
            if self.list_pages is not None and status == 'queued':
                page = self.list_pages[index]
                next_url = None if index == len(self.list_pages) - 1 else (
                    'https://api.github.com' + endpoint + '?event=merge_group&status=queued&per_page=100&page=' + str(index+2))
                return self.response(page, next_url=next_url)
            rows = [r for r in self.runs.values() if r['status'] == status]
            return self.response({'total_count': len(rows), 'workflow_runs': rows})
        if endpoint.startswith(self.root + '/actions/runs/'):
            return self.response(self.runs[int(endpoint.split('/')[-1])])
        if endpoint == self.root + '/rules/branches/main':
            return self.response(self.effective)
        if endpoint == self.root + '/rulesets':
            return self.response([{k: v for k, v in r.items() if k in ('id', 'target', 'source', 'source_type', 'enforcement')}
                                  for r in self.rules.values()])
        if endpoint.startswith(self.root + '/rulesets/'):
            return self.response(self.rules[int(endpoint.split('/')[-1])])
        if endpoint == self.root + '/git/ref/heads/main':
            return self.response({'ref': 'refs/heads/main', 'object': {'type': 'commit', 'sha': self.base}})
        if endpoint == self.root + '/git/matching-refs/heads/gh-readonly-queue/':
            return self.response(self.refs)
        if endpoint.startswith(self.root + '/compare/'):
            pair = tuple(endpoint.split('/')[-1].split('...'))
            return self.response(self.compares[pair])
        raise AssertionError('Unexpected endpoint: ' + path)


class Scenarios:
    def setUp(self):
        self.api = FakeGitHub(self.REPO)

    def scan(self, apply=True, **kwargs):
        return P.reconcile(self.api, self.REPO, apply=apply, **kwargs)

    def expected(self, number=10, attempt=1, sha=S):
        return [('POST', self.api.root + f'/actions/runs/{number}/cancel', attempt, sha)]

    def assert_skips(self, **kwargs):
        result = self.scan(**kwargs)
        self.assertEqual(self.api.mutations, [], result)
        return result

    def test_diverged_and_behind_have_exact_positive_mutation(self):
        for status in ('diverged', 'behind'):
            with self.subTest(status=status):
                self.api = FakeGitHub(self.REPO)
                self.api.compares[(B, S)] = comparison(status=status)
                result = self.scan()
                self.assertEqual(self.api.mutations, self.expected(), result)
                decision = result['decisions'][0]
                self.assertEqual(decision['action'], 'cancel-requested')
                self.assertEqual(decision['base_sha'], B)
                self.assertEqual(decision['head_sha'], S)
                self.assertEqual(decision['rule_ids'], [1])

    def test_dry_run_proves_eligibility_without_mutation(self):
        result = self.assert_skips(apply=False)
        self.assertEqual(result['decisions'][0]['action'], 'would-cancel')

    def test_ahead_identical_and_current_speculative_descendant_are_protected(self):
        self.api.compares[(B, S)] = comparison(status='ahead')
        self.assert_skips()
        self.api.base = S
        self.api.compares[(S, S)] = comparison(S, S, 'identical')
        self.assert_skips()

    def test_same_sha_rerun_after_final_read_is_still_excluded(self):
        replacement = self.api.make_run(20, H)
        self.api.runs[20] = replacement
        self.api.compares[(B, H)] = comparison(B, H, 'ahead')
        def rerun(number):
            self.api.runs[number].update(status='completed', conclusion='failure')
            self.api.runs[number].update(run_attempt=2, status='queued', conclusion=None)
        self.api.before_cancel = rerun
        result = self.scan()
        self.assertEqual(self.api.mutations, self.expected(attempt=2), result)
        self.assertEqual(self.api.runs[20], replacement)

    def test_different_sha_replacement_and_nonmerge_runs_untouched(self):
        for number, event in enumerate(('push', 'pull_request', 'workflow_dispatch', 'schedule', 'release'), 20):
            self.api.runs[number] = dict(self.api.make_run(number), event=event)
        self.api.runs[30] = self.api.make_run(30, H)
        self.api.compares[(B, H)] = comparison(B, H, 'ahead')
        result = self.scan()
        self.assertEqual(self.api.mutations, self.expected(), result)

    def test_current_or_reused_ref_even_elsewhere_overrules_ancestry(self):
        for name in ('main/pr-2400-' + C, 'main/pr-2500-' + H, 'release/pr-2400-' + C):
            self.api.refs = [{'ref': 'refs/heads/gh-readonly-queue/' + name,
                              'object': {'type': 'commit', 'sha': S}}]
            self.assert_skips()

    def test_distinct_sha_reusing_the_ref_is_not_cancelled(self):
        self.api.refs = [{'ref': 'refs/heads/' + self.api.runs[10]['head_branch'],
                          'object': {'type': 'commit', 'sha': H}}]
        self.api.heads = [H]
        self.api.runs[20] = self.api.make_run(20, H)
        self.api.compares[(B, H)] = comparison(B, H, 'ahead')
        result = self.scan()
        self.assertEqual(self.api.mutations, self.expected(), result)

    def test_unknown_or_current_queue_head_and_later_page(self):
        self.api.heads = [S]
        self.assert_skips()
        for state in sorted(P.QUEUE_STATES - {'QUEUED'}):
            page = self.api.queue_body([None])
            page['data']['repository']['mergeQueue']['entries']['nodes'][0]['state'] = state
            self.api.queue_pages = [page]
            with self.subTest(state=state): self.assert_skips()
        self.api.queue_pages = [self.api.queue_body([], True, '1'), self.api.queue_body([S])]
        self.assert_skips()

    def test_unbuilt_queued_entry_without_head_does_not_block(self):
        # GitHub builds only maximumEntriesToBuild entries; later QUEUED entries report headCommit null.
        self.api.heads = [H, None]
        result = self.scan()
        self.assertEqual(self.api.mutations, self.expected(), result)

    def test_partial_graphql_and_incomplete_cursor_fail_closed(self):
        for change in ('error', 'cursor', 'unknown-state', 'wrong-repo', 'other-base'):
            page = self.api.queue_body([H])
            entries = page['data']['repository']['mergeQueue']['entries']
            if change == 'error': page['errors'] = [{'message': 'rate limit'}]
            if change == 'cursor': entries['pageInfo'] = {'hasNextPage': True, 'endCursor': None}
            if change == 'unknown-state': entries['nodes'][0]['state'] = 'NEW_STATE'
            if change == 'wrong-repo': page['data']['repository']['databaseId'] = 88
            if change == 'other-base': entries['nodes'][0]['pullRequest']['baseRefName'] = 'release'
            self.api.queue_pages = [page]
            self.assert_skips()

    def test_missing_null_nonempty_bypass_disabled_or_unapplied_proof(self):
        for variant in ('missing', 'null', 'nonempty', 'disabled', 'evaluate', 'wrong-id', 'wrong-target', 'excluded', 'no-deletion'):
            self.api = FakeGitHub(self.REPO)
            rule = self.api.rules[1]
            if variant == 'missing': del rule['bypass_actors']
            if variant == 'null': rule['bypass_actors'] = None
            if variant == 'nonempty': rule['bypass_actors'] = [{'actor_type': 'OrganizationAdmin', 'bypass_mode': 'always'}]
            if variant in ('disabled', 'evaluate'): rule['enforcement'] = variant
            if variant == 'wrong-id': rule['id'] = 9
            if variant == 'wrong-target': rule['conditions']['ref_name']['include'] = ['refs/heads/release']
            if variant == 'excluded': rule['conditions']['ref_name']['exclude'] = ['refs/heads/main']
            if variant == 'no-deletion': self.api.effective = [r for r in self.api.effective if r['type'] != 'deletion']
            with self.subTest(variant=variant): self.assert_skips()

    def test_new_queue_target_or_broad_pattern_is_unknown(self):
        for pattern in ('refs/heads/release', '~ALL', 'refs/heads/*'):
            self.api.rules[2]['conditions']['ref_name']['include'] = [pattern]
            self.assert_skips()

    def test_boolean_ruleset_ids_are_not_numeric_protection_proof(self):
        self.api.effective[0]['ruleset_id'] = True
        self.assert_skips()
        self.api = FakeGitHub(self.REPO)
        def corrupt(method, path, body, n):
            if path == self.api.root + '/rulesets/1':
                return self.api.response(dict(self.api.rules[1], id=True))
        self.api.override = corrupt
        self.assert_skips()

    def test_unknown_ruleset_source_cannot_supply_protection_proof(self):
        self.api.rules[1]['source_type'] = 'Unknown'
        for rule in self.api.effective:
            if rule['ruleset_id'] == 1: rule['ruleset_source_type'] = 'Unknown'
        self.assert_skips()

    def test_main_deletion_and_missing_ref_data_at_final_read_skip(self):
        for status, value in ((404, {'message': 'Not Found'}), (200, {'ref': 'refs/heads/main'})):
            self.api = FakeGitHub(self.REPO)
            self.api.override = lambda m,p,b,n,status=status,value=value: (
                self.api.response(value, status) if p == self.api.root + '/git/ref/heads/main' and n == 2 else None)
            self.assert_skips()

    def test_exact_numeric_identity_workflow_suite_attempt_and_target(self):
        for field, value in (('repository', {'id': 88, 'full_name': self.REPO}),
                             ('head_repository', {'id': 77, 'full_name': 'other/repo'}),
                             ('workflow_id', 999), ('check_suite_id', None), ('run_attempt', True),
                             ('head_sha', 'oops'), ('head_branch', 'gh-readonly-queue/release/pr-1-' + C),
                             ('path', '.github/workflows/release.yml')):
            self.api = FakeGitHub(self.REPO)
            self.api.runs[10][field] = value
            with self.subTest(field=field): self.assert_skips()

    def test_comparison_metadata_must_prove_literal_base_to_head(self):
        mutations = [('base_commit', {'sha': S}), ('merge_base_commit', {'sha': B}),
                     ('merge_base_commit', {'sha': 'invalid'}), ('ahead_by', 0), ('ahead_by', True),
                     ('behind_by', 0), ('behind_by', -1), ('behind_by', True),
                     ('total_commits', 99), ('total_commits', None), ('status', 'unknown')]
        for field, value in mutations:
            self.api.compares[(B, S)] = dict(comparison(), **{field: value})
            with self.subTest(field=field, value=value): self.assert_skips()
        self.api.compares[(B, S)] = dict(comparison(status='behind'), merge_base_commit={'sha': C})
        self.assert_skips()

    def test_commits_array_truncation_is_not_ancestry_evidence(self):
        self.api.compares[(B, S)].update(ahead_by=500, total_commits=500, commits=[])
        result = self.scan()
        self.assertEqual(self.api.mutations, self.expected(), result)

    def test_api_errors_at_every_read_produce_no_mutations(self):
        baseline = FakeGitHub(self.REPO)
        P.reconcile(baseline, self.REPO, apply=False)
        reads = list(dict.fromkeys((m, p) for m, p, _ in baseline.calls))
        for method, path in reads:
            for status in (401, 403, 404, 409, 422, 429, 500):
                self.api = FakeGitHub(self.REPO)
                self.api.override = lambda m, p, b, n, method=method, path=path, status=status: (
                    self.api.response({'message': 'API error'}, status) if (m, p) == (method, path) else None)
                with self.subTest(path=path, status=status): self.assert_skips()

    def test_final_revalidation_rejects_identity_protections_activity_and_rewind(self):
        for variant in ('attempt', 'suite', 'sha', 'completed', 'rules', 'default', 'refs', 'queue', 'rewind'):
            self.api = FakeGitHub(self.REPO)
            def change(method, path, body, n):
                if path == self.api.root + '/actions/runs/10' and n == 2:
                    if variant == 'attempt': self.api.runs[10]['run_attempt'] = 2
                    if variant == 'suite': self.api.runs[10]['check_suite_id'] = 999
                    if variant == 'sha': self.api.runs[10]['head_sha'] = H
                    if variant == 'completed': self.api.runs[10].update(status='completed', conclusion='success')
                if path == self.api.root + '/rulesets/1' and n == 2 and variant == 'rules':
                    del self.api.rules[1]['bypass_actors']
                if path == self.api.root and n >= 3 and variant == 'default':
                    self.api.metadata['default_branch'] = 'release'
                if path.startswith(self.api.root + '/git/matching-refs/') and n == 2 and variant == 'refs':
                    self.api.refs = [{'ref':'refs/heads/' + self.api.runs[10]['head_branch'], 'object': {'type':'commit', 'sha': S}}]
                if path == '/graphql' and n == 2 and variant == 'queue': self.api.heads = [S]
                if path == self.api.root + '/git/ref/heads/main' and n == 2 and variant == 'rewind':
                    self.api.base = C
                    self.api.compares[(B, C)] = comparison(B, C, 'behind')
            self.api.override = change
            with self.subTest(variant=variant): self.assert_skips()

    def test_forward_main_advance_retains_exclusion(self):
        def advance(method, path, body, n):
            if path == self.api.root + '/git/ref/heads/main' and n == 2:
                self.api.base = H
                self.api.compares[(B, H)] = comparison(B, H, 'ahead')
        self.api.override = advance
        result = self.scan()
        self.assertEqual(self.api.mutations, self.expected(), result)

    def test_late_discovery_multiple_pages_repeated_scans_and_bounds(self):
        original = self.api.runs.pop(10)
        self.assert_skips()
        self.api.runs[10] = original
        self.api.runs[20] = self.api.make_run(20, H)
        self.api.compares[(B, H)] = comparison(B, H, 'ahead')
        self.api.list_pages = [{'total_count': 2, 'workflow_runs': [original]},
                               {'total_count': 2, 'workflow_runs': [self.api.runs[20]]}]
        result = self.scan(max_candidates=1)
        self.assertEqual(self.api.mutations, self.expected(), result)
        self.api.list_pages = None
        self.scan()
        self.assertEqual(self.api.mutations, self.expected())

    def test_incomplete_duplicate_and_truncated_run_pages(self):
        for pages in ([{'total_count': 2, 'workflow_runs': [self.api.runs[10]]}],
                      [{'total_count': 1001, 'workflow_runs': []}],
                      [{'total_count': 2, 'workflow_runs': [self.api.runs[10]]}]*2):
            self.api.list_pages = pages
            self.assert_skips()
        self.api.list_pages = [{'total_count': 2, 'workflow_runs': [self.api.runs[10]]}]*2
        self.assert_skips(max_pages=1)

    def test_pagination_must_not_escape_repo_or_loop(self):
        for link in ('https://evil.invalid/runs', 'https://api.github.com/repos/other/repo/runs',
                     'https://api.github.com' + self.api.root + '/rules/branches/main?per_page=100'):
            self.api.override = lambda m,p,b,n,link=link: self.api.response(self.api.effective, next_url=link) if '/rules/branches/' in p else None
            self.assert_skips()

    def test_request_budget_is_fail_closed(self):
        self.assert_skips(max_requests=3)
        self.assertLessEqual(len(self.api.calls), 3)

    def test_bounded_continuation_does_not_starve_a_late_eligible_run(self):
        self.api.runs[10]['head_sha'] = H
        self.api.compares[(B, H)] = comparison(B, H, 'ahead')
        self.api.runs[20] = self.api.make_run(20)
        first = self.scan(max_candidates=1)
        self.assertEqual(self.api.mutations, [])
        self.assertEqual(first['next_after_run_id'], 10)
        second = self.scan(max_candidates=1, after_run_id=first['next_after_run_id'])
        self.assertEqual(self.api.mutations, self.expected(number=20), second)
        self.assertIsNone(second['next_after_run_id'])

    def test_rule_changes_even_if_still_enforced_stop_the_scan(self):
        def changed(method, path, body, n):
            if path == self.api.root + '/rulesets/2' and n == 2:
                self.api.rules[2]['rules'][0]['parameters']['merge_method'] = 'REBASE'
        self.api.override = changed
        self.assert_skips()

    def test_later_activity_page_and_refs_api_errors_skip(self):
        self.api.queue_pages = [self.api.queue_body([], True, '1'),
                               {'data': None, 'errors': [{'message': 'permission denied'}]}]
        self.assert_skips()
        self.api.queue_pages = [self.api.queue_body([], True, '1'),
                               self.api.queue_body([], True, '1')]
        self.assert_skips()

    def test_409_and_post_failure_never_force_cancel_or_retry(self):
        for status in (409, 403, 500):
            self.api = FakeGitHub(self.REPO)
            self.api.cancel_status = status
            result = self.scan()
            self.assertEqual(self.api.mutations, self.expected(), result)
            self.assertNotEqual(result['decisions'][0]['action'], 'cancel-requested')
            self.assertFalse(any('force-cancel' in path for _,path,_ in self.api.calls))

    def test_lost_cancel_response_is_logged_as_unknown_not_skipped(self):
        def lost(method, path, body, n):
            if method == 'POST' and path.endswith('/cancel'):
                self.api.mutations.append(('POST', path, 1, S))
                raise P.SafetyError('response lost after request')
        self.api.override = lost
        result = self.scan()
        self.assertEqual(self.api.mutations, self.expected())
        self.assertFalse(result['complete'])
        self.assertEqual(result['decisions'][0]['action'], 'cancel-outcome-unknown')

    def test_success_on_second_rules_and_ref_pages(self):
        def paginate(method, path, body, n):
            if path == self.api.root + '/rules/branches/main?per_page=100':
                return self.api.response([], next_url='https://api.github.com' + path + '&page=2')
            if path == self.api.root + '/rules/branches/main?per_page=100&page=2':
                return self.api.response(self.api.effective)
            if path == self.api.root + '/git/matching-refs/heads/gh-readonly-queue/':
                return self.api.response([], next_url='https://api.github.com' + path + '?page=2')
            if path == self.api.root + '/git/matching-refs/heads/gh-readonly-queue/?page=2':
                return self.api.response([])
        self.api.override = paginate
        result = self.scan()
        self.assertEqual(self.api.mutations, self.expected(), result)


class IntentdTests(Scenarios, unittest.TestCase):
    REPO = 'intent-hq/intentd'


class FrontendTests(Scenarios, unittest.TestCase):
    REPO = 'intent-hq/cloudlands-fe'


class TransportTests(unittest.TestCase):
    def test_gh_request_methods_and_exact_cancel_endpoint(self):
        endpoint = '/repos/intent-hq/intentd/actions/runs/10/cancel'
        process = subprocess.CompletedProcess([], 0, 'HTTP/2.0 202 Accepted\r\nContent-Length: 0\r\n\r\n', '')
        with mock.patch.object(P.subprocess, 'run', return_value=process) as child:
            response = P.GhApi().request('POST', endpoint)
        self.assertEqual(response.status, 202)
        argv = child.call_args.args[0]
        self.assertEqual(argv, ['gh', 'api', '--hostname', 'github.com', '--include', '--method', 'POST',
                               '-H', 'Accept: application/vnd.github+json',
                               '-H', 'X-GitHub-Api-Version: 2022-11-28', endpoint])
        self.assertNotIn('shell', child.call_args.kwargs)
        self.assertEqual(child.call_args.kwargs['timeout'], 30)

    def test_graphql_is_a_static_query_and_variables_use_stdin(self):
        body = {'query': P.QUEUE_QUERY, 'variables': {'owner': 'intent-hq', 'name': 'intentd', 'cursor': None}}
        process = subprocess.CompletedProcess([], 0, 'HTTP/2.0 200 OK\nContent-Type: application/json\n\n{}', '')
        with mock.patch.object(P.subprocess, 'run', return_value=process) as child:
            P.GhApi().request('POST', '/graphql', body)
        self.assertEqual(json.loads(child.call_args.kwargs['input']), body)
        self.assertEqual(child.call_args.args[0][-2:], ['--input', '-'])
        self.assertTrue(P.QUEUE_QUERY.startswith('query('))

    def test_409_is_preserved_and_network_errors_are_not_retried(self):
        process = subprocess.CompletedProcess([], 1, 'HTTP/2.0 409 Conflict\nContent-Type: application/json\n\n{}', 'gh: conflict')
        with mock.patch.object(P.subprocess, 'run', return_value=process) as child:
            self.assertEqual(P.GhApi().request('POST', '/repos/intent-hq/intentd/actions/runs/10/cancel').status, 409)
            self.assertEqual(child.call_count, 1)
        for error in (OSError('network'), subprocess.TimeoutExpired('gh', 30)):
            with mock.patch.object(P.subprocess, 'run', side_effect=error) as child:
                with self.assertRaises(P.SafetyError): P.GhApi().request('GET', '/repos/intent-hq/intentd')
                self.assertEqual(child.call_count, 1)

    def test_malformed_headers_or_json_fail_closed(self):
        for text in ('{}', 'HTTP/2.0 200 OK\n\nnot JSON',
                     'HTTP/2.0 200 OK\nLink: first\nLink: second\n\n[]',
                     'HTTP/2.0 301 Redirect\n\nHTTP/2.0 200 OK\n\n{}'):
            with self.assertRaises(ValueError): P.parse_response(text)

    def test_cli_defaults_to_dry_run_and_rejects_unlisted_repository(self):
        with mock.patch.object(sys, 'argv', [str(SCRIPT), '--repository', 'intent-hq/intentd']), \
                mock.patch.object(P, 'reconcile', return_value={'complete': True}) as reconcile, \
                mock.patch('builtins.print'):
            self.assertEqual(P.main(), 0)
            self.assertFalse(reconcile.call_args.kwargs['apply'])
        process = subprocess.run([sys.executable, '-I', '-B', str(SCRIPT), '--repository', 'other/repo', '--apply'],
                                 capture_output=True, text=True, timeout=5)
        self.assertEqual(process.returncode, 2)


if __name__ == '__main__':
    unittest.main()
