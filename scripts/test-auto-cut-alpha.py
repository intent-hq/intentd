#!/usr/bin/env python3
"""Execute the actual workflow shell with offline gh fixtures and real git."""
import json
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import sys
import textwrap
import unittest

ROOT = Path(__file__).resolve().parents[1]
Fixture = runpy.run_path(str(ROOT / 'scripts/test-release-pr-fast-path.py'))['Fixture']
MOCK = r'''
import json, os, pathlib, subprocess, sys
path = pathlib.Path(os.environ['MOCK_STATE'])
s = json.loads(path.read_text())
a = sys.argv[1:]
s['calls'].append({'args': a, 'token': os.environ.get('GH_TOKEN')})
path.write_text(json.dumps(s))
def emit(value):
    if '--jq' in a:
        subprocess.run(['jq', '-r', a[a.index('--jq') + 1]], input=json.dumps(value), text=True, check=True)
    else:
        print(json.dumps(value))
if a[:2] == ['pr', 'list']:
    emit([s['pr']])
elif a[:2] == ['pr', 'view']:
    fields = a[a.index('--json') + 1].split(',')
    emit({k: s['detail'][k] for k in fields})
elif a[:2] == ['api', 'graphql']:
    query = a[a.index('-f') + 1]
    if 'viewer' in query:
        print('release-bot')
    else:
        if s.get('thread_error'): sys.exit(1)
        print(s.get('unresolved', 0))
elif a[:2] == ['pr', 'merge']:
    if s.get('changed_head'):
        if '--match-head-commit' in a:
            print('head changed; refusing', file=sys.stderr)
            sys.exit(1)
    s['merged'] = '--admin' in a
    s['submitted'] = True
    path.write_text(json.dumps(s))
elif a[:1] == ['api'] and 'matching-refs' in ' '.join(a):
    print('v0.9.134')
elif a[:1] == ['api'] and '/commits/' in ' '.join(a):
    print(s.get('last_cut', '2000-01-01T00:00:00Z'))
elif a[:1] == ['api'] and '/compare/' in ' '.join(a):
    print(s.get('compare', 'ahead'))
else:
    raise SystemExit('unexpected gh call: ' + repr(a))
'''


class AutoCutTests(Fixture):
    def setUp(self):
        super().setUp()
        self.bump(sitter=True)
        self.head = self.commit()
        self.git('remote', 'add', 'origin', str(self.repo))
        self.bin = self.repo / 'bin'
        self.bin.mkdir()
        gh = self.bin / 'gh'
        gh.write_text(f'#!{sys.executable} -S\n' + MOCK)
        gh.chmod(0o755)
        sleep = self.bin / 'sleep'
        sleep.write_text('#!/bin/sh\necho "Unexpected polling" >&2\nexit 97\n')
        sleep.chmod(0o755)
        self.state = {
            'calls': [], 'pr': {'number': 42, 'title': 'chore: release', 'headRefName': 'release-plz-test', 'isDraft': False, 'labels': [], 'isCrossRepository': False, 'author': {'login': 'release-bot'}},
            'detail': {'isDraft': False, 'mergeable': 'MERGEABLE', 'mergeStateStatus': 'CLEAN', 'statusCheckRollup': [{'name': 'CI Gate', 'conclusion': 'SUCCESS'}], 'headRefOid': self.head, 'baseRefOid': self.base, 'baseRefName': 'main', 'reviewDecision': ''},
        }
        self.env = {**os.environ, 'PATH': f'{self.bin}:{os.environ["PATH"]}', 'MOCK_STATE': str(self.repo / 'state'), 'GH_TOKEN': 'read-token', 'RELEASE_PLZ_TOKEN': 'merge-token', 'GITHUB_REPOSITORY': 'intent-hq/intentd', 'EVENT_NAME': 'workflow_dispatch', 'DRY_RUN': 'false', 'PUSH_SHA': self.base, 'HEAD_COMMIT_MSG': 'fix: example'}
        self.env.pop('GITHUB_TOKEN', None)
        # Execute trusted helpers from the workflow checkout, never PR blobs.
        dest = self.repo / 'scripts'
        dest.mkdir()
        for source in (ROOT / 'scripts').glob('release-pr-fast-path.*'):
            shutil.copy(source, dest / source.name)

    def run_workflow(self, **env):
        self.env.update(env)
        state_path = self.repo / 'state'
        state_path.write_text(json.dumps(self.state))
        workflow = (ROOT / '.github/workflows/auto-cut-alpha.yml').read_text()
        step = workflow.split('      - name: Merge the Release PR when green\n', 1)[1]
        script = textwrap.dedent(step.split('        run: |\n', 1)[1])
        result = subprocess.run(['bash', '-euo', 'pipefail', '-c', script], cwd=self.repo, env=self.env, capture_output=True, text=True, timeout=10)
        self.state = json.loads(state_path.read_text())
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout

    def merge_calls(self):
        return [c for c in self.state['calls'] if c['args'][:2] == ['pr', 'merge']]

    def test_green_metadata_direct_merge_matches_tested_head(self):
        output = self.run_workflow()
        calls = self.merge_calls()
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]['token'], 'merge-token')
        self.assertEqual(calls[0]['args'], ['pr', 'merge', '42', '--repo', 'intent-hq/intentd', '--squash', '--match-head-commit', self.head, '--admin'])
        self.assertIn('Directly squash-merged', output)

    def test_missing_or_failed_gate_skips(self):
        for checks in ([], [{'name': 'CI Gate', 'conclusion': 'FAILURE'}], [{'name': 'CI Gate', 'status': 'IN_PROGRESS', 'conclusion': None}]):
            with self.subTest(checks=checks):
                self.state['calls'] = []
                self.state['detail']['statusCheckRollup'] = checks
                self.run_workflow()
                self.assertEqual(self.merge_calls(), [])

    def test_held_draft_conflicted_behind_unknown_or_changes_requested_skips(self):
        original = json.loads(json.dumps(self.state))
        for field, value in [('hold', True), ('isDraft', True), ('mergeable', 'CONFLICTING'), ('mergeable', 'UNKNOWN'), ('mergeStateStatus', 'BEHIND'), ('mergeStateStatus', 'BLOCKED'), ('reviewDecision', 'CHANGES_REQUESTED'), ('reviewDecision', 'REVIEW_REQUIRED'), ('baseRefName', 'other')]:
            with self.subTest(field=field, value=value):
                self.state = json.loads(json.dumps(original))
                if field == 'hold':
                    self.state['pr']['labels'] = [{'name': 'hold-release'}]
                else:
                    self.state['detail'][field] = value
                self.run_workflow()
                self.assertEqual(self.merge_calls(), [])

    def test_untrusted_or_fork_pr_skips(self):
        for key, value in [('author', {'login': 'outsider'}), ('isCrossRepository', True), ('headRefName', 'ordinary')]:
            with self.subTest(key=key):
                old = self.state['pr'][key]
                self.state['calls'] = []
                self.state['pr'][key] = value
                self.run_workflow()
                self.assertEqual(self.merge_calls(), [])
                self.state['pr'][key] = old

    def test_unresolved_human_threads_or_unreadable_threads_skip(self):
        for fixture in ({'unresolved': 1}, {'thread_error': True, 'unresolved': 0}):
            self.state.update(fixture)
            self.state['calls'] = []
            self.run_workflow()
            self.assertEqual(self.merge_calls(), [])

    def test_changed_head_cannot_merge(self):
        self.state['changed_head'] = True
        output = self.run_workflow()
        self.assertFalse(self.state.get('submitted', False))
        self.assertNotIn('Directly squash-merged', output)

    def test_dry_run_describes_direct_merge_without_mutation(self):
        output = self.run_workflow(DRY_RUN='true')
        self.assertEqual(self.merge_calls(), [])
        self.assertIn('direct', output.lower())

    def test_metadata_mismatch_uses_queue_without_claiming_merge(self):
        self.write('crates/intentd/src/main.rs', 'fn main() { panic!(); }\n')
        self.state['detail']['headRefOid'] = self.commit()
        output = self.run_workflow()
        call = self.merge_calls()[0]['args']
        self.assertNotIn('--admin', call)
        self.assertIn('--match-head-commit', call)
        self.assertIn('queue', output.lower())
        self.assertNotIn('Squash-merged', output)

    def test_pr_helper_is_never_executed(self):
        path = 'scripts/release-pr-fast-path.sh'
        trusted = (self.repo / path).read_text()
        self.write(path, '#!/bin/sh\ntouch unsafe-pr-code-ran\necho fast_path=true\n')
        self.state['detail']['headRefOid'] = self.commit()
        self.write(path, trusted)
        self.run_workflow()
        self.assertFalse((self.repo / 'unsafe-pr-code-ran').exists())
        self.assertNotIn('--admin', self.merge_calls()[0]['args'])

    def test_missing_objects_fall_back_to_queue(self):
        self.state['detail']['headRefOid'] = 'f' * 40
        output = self.run_workflow()
        self.assertNotIn('--admin', self.merge_calls()[0]['args'])
        self.assertIn('queue', output.lower())

    def test_classifier_error_falls_back_to_queue(self):
        self.write('scripts/release-pr-fast-path.sh', '#!/bin/sh\nexit 2\n')
        self.run_workflow()
        self.assertNotIn('--admin', self.merge_calls()[0]['args'])

    def test_main_not_contained_in_tested_head_skips(self):
        self.git('checkout', '-q', self.base)
        self.write('unreleased', 'main advanced')
        self.state['detail']['baseRefOid'] = self.commit()
        self.run_workflow()
        self.assertEqual(self.merge_calls(), [])

    def test_push_freshness_queries_the_tested_sha(self):
        self.run_workflow(EVENT_NAME='push')
        comparisons = [c['args'] for c in self.state['calls'] if '/compare/' in ' '.join(c['args'])]
        self.assertEqual(comparisons[0][1], f'repos/intent-hq/intentd/compare/{self.base}...{self.head}')

    def test_recent_release_throttles(self):
        import datetime
        self.state['last_cut'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self.run_workflow(EVENT_NAME='schedule')
        self.assertEqual(self.merge_calls(), [])

    def test_release_push_does_not_retrigger(self):
        self.run_workflow(EVENT_NAME='push', HEAD_COMMIT_MSG='chore: release v0.9.135')
        self.assertEqual(self.state['calls'], [])


if __name__ == '__main__':
    unittest.main()
