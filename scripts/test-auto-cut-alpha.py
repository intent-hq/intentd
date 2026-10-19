#!/usr/bin/env python3
"""Execute the actual workflow shell with offline gh fixtures and real git."""
import json
import os
import re
from pathlib import Path
import runpy
import shutil
import subprocess
import sys
import textwrap
import unittest

ROOT = Path(__file__).resolve().parents[1]
OUTCOMES = 'noop|throttle|hold|defer|skip|refuse|dry-run|merge|queue|error'
DECISION = re.compile(r'^auto-cut-decision: (?P<outcome>%s) (?P<reason>[a-z0-9]+(?:-[a-z0-9]+)*) \S.*$' % OUTCOMES)
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
elif a[:1] == ['api'] and '/actions/workflows/ci.yml/runs' in ' '.join(a):
    if s.get('main_runs_error'): sys.exit(1)
    emit({'total_count': len(s['main_runs']), 'workflow_runs': s['main_runs']})
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
        self.forbid_polling()
        self.state = {
            'calls': [], 'pr': {'number': 42, 'title': 'chore: release', 'headRefName': 'release-plz-test', 'isDraft': False, 'labels': [], 'isCrossRepository': False, 'author': {'login': 'release-bot'}},
            'detail': {'isDraft': False, 'mergeable': 'MERGEABLE', 'mergeStateStatus': 'CLEAN', 'statusCheckRollup': [{'name': 'CI Gate', 'conclusion': 'SUCCESS'}], 'headRefOid': self.head, 'baseRefOid': self.base, 'baseRefName': 'main', 'reviewDecision': ''},
            # The push-event ci.yml run on the Release PR's base (the main
            # tip): carries the post-merge Windows validation.
            'main_runs': [self.main_run(1, 'completed', 'success')],
        }
        self.initial_state = json.loads(json.dumps(self.state))
        self.env = {**os.environ, 'PATH': f'{self.bin}:{os.environ["PATH"]}', 'MOCK_STATE': str(self.repo / 'state'), 'GH_TOKEN': 'read-token', 'RELEASE_PLZ_TOKEN': 'merge-token', 'GITHUB_REPOSITORY': 'intent-hq/intentd', 'EVENT_NAME': 'workflow_dispatch', 'DRY_RUN': 'false', 'PUSH_SHA': self.base, 'HEAD_COMMIT_MSG': 'fix: example'}
        self.env.pop('GITHUB_TOKEN', None)
        # Execute trusted helpers from the workflow checkout, never PR blobs.
        dest = self.repo / 'scripts'
        dest.mkdir()
        for source in (ROOT / 'scripts').glob('release-pr-fast-path.*'):
            shutil.copy(source, dest / source.name)

    @staticmethod
    def merge_step():
        workflow = (ROOT / '.github/workflows/auto-cut-alpha.yml').read_text()
        step = workflow.split('      - name: Merge the Release PR when green\n', 1)[1]
        return textwrap.dedent(step.split('        run: |\n', 1)[1])

    def run_workflow(self, expect_status=0, **env):
        self.env.update(env)
        state_path = self.repo / 'state'
        state_path.write_text(json.dumps(self.state))
        summary = self.repo / 'step-summary'
        summary.write_text('')
        self.env['GITHUB_STEP_SUMMARY'] = str(summary)
        # Hang guard only; the longest fixture makes ~40 mocked polls.
        result = subprocess.run(['bash', '-euo', 'pipefail', '-c', self.merge_step()], cwd=self.repo, env=self.env, capture_output=True, text=True, timeout=60)
        self.state = json.loads(state_path.read_text())
        self.assertEqual(result.returncode, expect_status, result.stdout + result.stderr)
        # Decision contract (read by monorepo `make release-status`): every
        # run, on every exit path, prints exactly one decision line and
        # mirrors it to the step summary.
        lines = [line for line in result.stdout.splitlines() if line.startswith('auto-cut-decision:')]
        self.assertEqual(len(lines), 1, result.stdout)
        self.assertRegex(lines[0], DECISION)
        self.assertEqual(summary.read_text().splitlines(), lines)
        self.last_decision = tuple(DECISION.match(lines[0]).group('outcome', 'reason'))
        return result.stdout

    def forbid_polling(self):
        sleep = self.bin / 'sleep'
        sleep.write_text('#!/bin/sh\necho "Unexpected polling" >&2\nexit 97\n')
        sleep.chmod(0o755)

    def reset(self):
        # Fresh gh state, real clock and no-polling sleep for the next
        # subtest, reusing the fixture repository.
        self.state = json.loads(json.dumps(self.initial_state))
        (self.bin / 'date').unlink(missing_ok=True)
        self.forbid_polling()

    def finish_ci_after_polls(self, polls=1):
        # After `polls` sleeps, the Release PR's CI Gate turns green.
        self.update_after_polls(polls, {'mergeStateStatus': 'CLEAN', 'statusCheckRollup': [{'name': 'CI Gate', 'conclusion': 'SUCCESS'}]})

    def update_after_polls(self, polls, detail, seconds_per_poll=30, stages=None):
        # Each "sleep" records a poll and advances the fake clock; after
        # `polls` of them the Release PR detail takes the given values
        # (`stages` maps several poll counts to successive updates).
        stages = {str(k): v for k, v in (stages or {polls: detail}).items()}
        sleep = self.bin / 'sleep'
        sleep.write_text(f"""#!{sys.executable} -S
import json, os, pathlib
path = pathlib.Path(os.environ['MOCK_STATE'])
s = json.loads(path.read_text())
s['polls'] = s.get('polls', 0) + 1
update = json.loads({json.dumps(json.dumps(stages))}).get(str(s['polls']))
if update:
    s['detail'].update(update)
path.write_text(json.dumps(s))
""")
        sleep.chmod(0o755)
        # `date +%s` returns real start time plus seconds_per_poll per poll,
        # so wait budgets are exercised without real waiting; every other
        # date call goes to the real binary.
        real = shutil.which('date')
        date = self.bin / 'date'
        date.write_text(f"""#!{sys.executable} -S
import json, os, pathlib, sys
if sys.argv[1:] == ['+%s']:
    s = json.loads(pathlib.Path(os.environ['MOCK_STATE']).read_text())
    print(int(os.environ['FAKE_EPOCH']) + s.get('polls', 0) * {seconds_per_poll})
else:
    os.execv({real!r}, [{real!r}, *sys.argv[1:]])
""")
        date.chmod(0o755)
        import time
        self.env['FAKE_EPOCH'] = str(int(time.time()))

    def tearDown(self):
        (self.bin / 'date').unlink(missing_ok=True)
        super().tearDown()

    @staticmethod
    def main_run(run_id, status, conclusion):
        return {'id': run_id, 'status': status, 'conclusion': conclusion,
                'html_url': f'https://github.com/intent-hq/intentd/actions/runs/{run_id}'}

    def finish_main_ci_after_polls(self, polls=1):
        # Each "sleep" records a poll; after `polls` of them main's
        # post-merge CI run turns green.
        sleep = self.bin / 'sleep'
        sleep.write_text(f"""#!{sys.executable} -S
import json, os, pathlib
path = pathlib.Path(os.environ['MOCK_STATE'])
s = json.loads(path.read_text())
s['polls'] = s.get('polls', 0) + 1
if s['polls'] >= {polls}:
    for run in s['main_runs']:
        run['status'], run['conclusion'] = 'completed', 'success'
path.write_text(json.dumps(s))
""")
        sleep.chmod(0o755)

    def main_ci_calls(self):
        return [c['args'] for c in self.state['calls'] if '/actions/workflows/ci.yml/runs' in ' '.join(c['args'])]

    def pending_ci(self):
        self.state['detail']['mergeStateStatus'] = 'BLOCKED'
        self.state['detail']['statusCheckRollup'] = [{'name': 'CI Gate', 'status': 'IN_PROGRESS', 'conclusion': None}]

    def merge_calls(self):
        return [c for c in self.state['calls'] if c['args'][:2] == ['pr', 'merge']]

    def test_green_metadata_direct_merge_matches_tested_head(self):
        output = self.run_workflow()
        calls = self.merge_calls()
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]['token'], 'merge-token')
        self.assertEqual(calls[0]['args'], ['pr', 'merge', '42', '--repo', 'intent-hq/intentd', '--squash', '--match-head-commit', self.head, '--admin'])
        self.assertIn('Directly squash-merged', output)

    def test_green_main_post_merge_ci_on_the_release_base_allows_the_cut(self):
        output = self.run_workflow()
        calls = self.main_ci_calls()
        self.assertEqual(len(calls), 1)
        self.assertIn(f'repos/intent-hq/intentd/actions/workflows/ci.yml/runs?head_sha={self.base}&event=push&per_page=100', calls[0])
        self.assertEqual(len(self.merge_calls()), 1)
        self.assertIn("main's post-merge CI passed", output)

    def test_red_main_post_merge_ci_blocks_the_cut_without_waiting(self):
        # Windows validation runs only after merge; a red run on main must
        # hold the alpha until a fix or re-run turns it green.
        for event in ('push', 'schedule', 'workflow_dispatch'):
            for conclusion in ('failure', 'cancelled', 'timed_out', 'startup_failure', 'action_required', 'neutral', 'skipped', 'stale'):
                with self.subTest(event=event, conclusion=conclusion):
                    self.reset()
                    self.state['main_runs'] = [self.main_run(7, 'completed', conclusion)]
                    output = self.run_workflow(EVENT_NAME=event)
                    self.assertEqual(self.merge_calls(), [])
                    self.assertIn(f"main's post-merge CI concluded {conclusion}", output)
                    self.assertIn('actions/runs/7', output)

    def test_latest_main_run_decides(self):
        for runs, merged in (([self.main_run(1, 'completed', 'failure'), self.main_run(2, 'completed', 'success')], True),
                             ([self.main_run(3, 'completed', 'failure'), self.main_run(2, 'completed', 'success')], False)):
            with self.subTest(runs=runs):
                self.reset()
                self.state['main_runs'] = runs
                self.run_workflow(EVENT_NAME='schedule')
                self.assertEqual(len(self.merge_calls()), int(merged))

    def test_pending_main_post_merge_ci_waits_then_cuts(self):
        for event in ('push', 'schedule', 'workflow_dispatch'):
            for status in ('queued', 'in_progress', 'pending', 'waiting', 'requested'):
                with self.subTest(event=event, status=status):
                    self.reset()
                    self.state['main_runs'] = [self.main_run(5, status, None)]
                    self.finish_main_ci_after_polls(2)
                    output = self.run_workflow(EVENT_NAME=event)
                    self.assertEqual(self.state.get('polls'), 2)
                    self.assertEqual(len(self.merge_calls()), 1)
                    self.assertIn(f"main's post-merge CI is still {status} on {self.base} (https://github.com/intent-hq/intentd/actions/runs/5)", output)

    def test_pending_or_missing_main_ci_wait_is_bounded(self):
        for runs in ([self.main_run(5, 'in_progress', None)], []):
            with self.subTest(runs=runs):
                self.reset()
                self.state['main_runs'] = runs
                output = self.run_workflow(EVENT_NAME='schedule', AUTO_CUT_CI_WAIT_MINUTES='0')
                self.assertEqual(self.merge_calls(), [])
                self.assertIn('budget exhausted', output)

    def test_missing_main_ci_gets_only_the_short_grace(self):
        self.state['main_runs'] = []
        self.finish_main_ci_after_polls(1)
        sleep = self.bin / 'sleep'
        sleep.write_text(sleep.read_text().replace("for run in s['main_runs']:", "s['main_runs'] = [{'id': 9, 'html_url': 'u'}]\n    for run in s['main_runs']:"))
        output = self.run_workflow(EVENT_NAME='schedule', AUTO_CUT_CI_WAIT_MINUTES='25')
        self.assertEqual(self.state.get('polls'), 1)
        self.assertEqual(len(self.merge_calls()), 1)
        self.assertIn("main's post-merge CI has not started", output)

    def test_unreadable_main_ci_never_merges(self):
        self.state['main_runs_error'] = True
        output = self.run_workflow(EVENT_NAME='schedule')
        self.assertEqual(self.merge_calls(), [])
        self.assertIn('post-merge CI lookup failed', output)

    def test_dry_run_still_reports_a_red_main(self):
        self.state['main_runs'] = [self.main_run(4, 'completed', 'failure')]
        output = self.run_workflow(DRY_RUN='true')
        self.assertNotIn('DRY RUN: would', output)
        self.assertIn("main's post-merge CI concluded failure", output)

    def test_missing_or_failed_gate_skips(self):
        # A zero CI wait budget keeps the check-once behavior (the sleep stub
        # fails the run if anything polls).
        for checks in ([], [{'name': 'CI Gate', 'conclusion': 'FAILURE'}], [{'name': 'CI Gate', 'status': 'IN_PROGRESS', 'conclusion': None}]):
            with self.subTest(checks=checks):
                self.state['calls'] = []
                self.state['detail']['statusCheckRollup'] = checks
                self.run_workflow(AUTO_CUT_CI_WAIT_MINUTES='0')
                self.assertEqual(self.merge_calls(), [])

    def test_failed_gate_never_polls_even_with_ci_budget(self):
        # A failed required check is what makes GitHub report BLOCKED, so the
        # realistic shape is BLOCKED + FAILURE; push runs must not spend
        # their refresh budget on it either.
        for event in ('push', 'schedule', 'workflow_dispatch'):
            for merge_state in ('BLOCKED', 'CLEAN'):
                with self.subTest(event=event, merge_state=merge_state):
                    self.reset()
                    self.state['detail']['mergeStateStatus'] = merge_state
                    self.state['detail']['statusCheckRollup'] = [{'name': 'CI Gate', 'conclusion': 'FAILURE'}]
                    output = self.run_workflow(EVENT_NAME=event)
                    self.assertEqual(self.merge_calls(), [])
                    self.assertIn('CI Gate concluded unsuccessfully', output)

    def test_missing_gate_waits_only_a_short_grace(self):
        # CI that never starts must not hold every cron run for the full
        # budget (it gets a short grace instead). A zero budget proves no
        # wait; the green-after-one-poll case proves the grace still waits
        # for checks that are merely late.
        self.state['detail']['mergeStateStatus'] = 'BLOCKED'
        self.state['detail']['statusCheckRollup'] = []
        self.run_workflow(EVENT_NAME='schedule', AUTO_CUT_CI_WAIT_MINUTES='0')
        self.assertEqual(self.merge_calls(), [])
        self.reset()
        self.state['detail']['mergeStateStatus'] = 'BLOCKED'
        self.state['detail']['statusCheckRollup'] = []
        self.finish_ci_after_polls(1)
        self.run_workflow(EVENT_NAME='schedule', AUTO_CUT_CI_WAIT_MINUTES='25')
        self.assertEqual(self.state.get('polls'), 1)
        self.assertEqual(len(self.merge_calls()), 1)

    def test_unresolved_human_thread_exits_before_any_ci_wait(self):
        self.pending_ci()
        self.state['unresolved'] = 1
        output = self.run_workflow(EVENT_NAME='schedule')
        self.assertEqual(self.merge_calls(), [])
        self.assertIn('unresolved human review thread', output)

    def test_invalid_ci_wait_budget_fails_loudly(self):
        for value in ('40m', '31'):
            with self.subTest(value=value):
                output = self.run_workflow(expect_status=1, EVENT_NAME='schedule', AUTO_CUT_CI_WAIT_MINUTES=value)
                self.assertIn('AUTO_CUT_CI_WAIT_MINUTES must be whole minutes', output)
                self.assertEqual(self.last_decision, ('error', 'invalid-ci-wait'))

    def test_scheduled_and_dispatched_runs_wait_for_pending_release_ci(self):
        # Release-PR CI runs the ~13 min Linux checks; a cron run that checked
        # once kept finding it pending (intentd v0.11.8 stalled ~7 h).
        for event in ('schedule', 'workflow_dispatch'):
            with self.subTest(event=event):
                self.reset()
                self.pending_ci()
                self.finish_ci_after_polls(2)
                output = self.run_workflow(EVENT_NAME=event)
                self.assertEqual(self.state.get('polls'), 2)
                self.assertEqual(len(self.merge_calls()), 1)
                self.assertIn('waiting for CI Gate', output)

    def test_pending_ci_wait_is_bounded(self):
        self.pending_ci()
        self.run_workflow(EVENT_NAME='schedule', AUTO_CUT_CI_WAIT_MINUTES='0')
        self.assertEqual(self.merge_calls(), [])

    def test_review_or_base_blockers_never_wait_for_ci(self):
        for field, value in (('reviewDecision', 'REVIEW_REQUIRED'), ('reviewDecision', 'CHANGES_REQUESTED'), ('baseRefName', 'other')):
            with self.subTest(field=field, value=value):
                self.reset()
                self.pending_ci()
                self.state['detail'][field] = value
                self.run_workflow(EVENT_NAME='schedule')
                self.assertEqual(self.merge_calls(), [])

    def test_held_draft_conflicted_behind_or_changes_requested_skips(self):
        original = json.loads(json.dumps(self.state))
        for field, value in [('hold', True), ('isDraft', True), ('mergeable', 'CONFLICTING'), ('mergeStateStatus', 'BEHIND'), ('mergeStateStatus', 'BLOCKED'), ('reviewDecision', 'CHANGES_REQUESTED'), ('reviewDecision', 'REVIEW_REQUIRED'), ('baseRefName', 'other')]:
            with self.subTest(field=field, value=value):
                self.state = json.loads(json.dumps(original))
                if field == 'hold':
                    self.state['pr']['labels'] = [{'name': 'hold-release'}]
                else:
                    self.state['detail'][field] = value
                self.run_workflow()
                self.assertEqual(self.merge_calls(), [])

    def test_unknown_mergeability_is_re_read_on_every_event(self):
        # intentd#2493: scheduled runs skipped on the first UNKNOWN read even
        # though GitHub recomputes mergeability within a minute.
        for event in ('schedule', 'workflow_dispatch', 'push'):
            with self.subTest(event=event):
                self.reset()
                self.state['detail']['mergeable'] = 'UNKNOWN'
                self.update_after_polls(1, {'mergeable': 'MERGEABLE'})
                output = self.run_workflow(EVENT_NAME=event)
                self.assertEqual(self.state.get('polls'), 1)
                self.assertEqual(len(self.merge_calls()), 1)
                self.assertIn('mergeable=UNKNOWN) — re-reading in 30s.', output)

    def test_persistent_unknown_mergeability_skips_after_bounded_reads(self):
        self.state['detail']['mergeable'] = 'UNKNOWN'
        self.update_after_polls(99, {})
        output = self.run_workflow(EVENT_NAME='schedule')
        self.assertEqual(self.state.get('polls'), 4)
        self.assertEqual(self.merge_calls(), [])
        self.assertIn('mergeable=UNKNOWN); skipping.', output)

    def test_unknown_after_push_budget_still_gets_bounded_reads(self):
        # The 07:39 shape from intentd#2493: a push run spends its 15-min
        # refresh budget waiting for CI; CI then goes green while GitHub
        # recomputes mergeability. That UNKNOWN must be re-read, not skipped.
        self.pending_ci()
        green = {'mergeStateStatus': 'CLEAN', 'mergeable': 'UNKNOWN',
                 'statusCheckRollup': [{'name': 'CI Gate', 'conclusion': 'SUCCESS'}]}
        self.update_after_polls(None, None, seconds_per_poll=8 * 60,
                                stages={2: green, 3: {'mergeable': 'MERGEABLE'}})
        output = self.run_workflow(EVENT_NAME='push')
        self.assertEqual(self.state.get('polls'), 3)
        self.assertEqual(len(self.merge_calls()), 1)
        self.assertIn('mergeable=UNKNOWN) — re-reading in 30s.', output)

    def test_missing_gate_beside_running_ci_gets_the_full_ci_budget(self):
        # CI Gate needs every CI job, so it only appears once CI finishes.
        # Three 400-second polls (20 min) outlast the 5-min missing grace;
        # with CI visibly running they fit the 25-min CI budget, and the
        # gate turns green on the third.
        running = [{'name': 'check', 'workflowName': 'CI', 'status': 'IN_PROGRESS', 'conclusion': None},
                   {'name': 'route', 'workflowName': 'CI', 'status': 'COMPLETED', 'conclusion': 'SUCCESS'}]
        self.state['detail']['mergeStateStatus'] = 'BLOCKED'
        self.state['detail']['statusCheckRollup'] = running
        self.update_after_polls(3, {'mergeStateStatus': 'CLEAN', 'statusCheckRollup': [{'name': 'CI Gate', 'workflowName': 'CI', 'status': 'COMPLETED', 'conclusion': 'SUCCESS'}]}, seconds_per_poll=400)
        output = self.run_workflow(EVENT_NAME='schedule', AUTO_CUT_CI_WAIT_MINUTES='25')
        self.assertEqual(self.state.get('polls'), 3)
        self.assertEqual(len(self.merge_calls()), 1)
        self.assertIn('but CI is still running', output)

    def test_each_unknown_episode_gets_its_own_grace(self):
        # An UNKNOWN right after the refresh must not use up the grace for
        # the UNKNOWN that follows CI turning green after the push budget.
        self.state['detail']['mergeable'] = 'UNKNOWN'
        pending = {'mergeable': 'MERGEABLE', 'mergeStateStatus': 'BLOCKED',
                   'statusCheckRollup': [{'name': 'check', 'workflowName': 'CI', 'status': 'IN_PROGRESS', 'conclusion': None}]}
        green = {'mergeable': 'UNKNOWN', 'mergeStateStatus': 'CLEAN',
                 'statusCheckRollup': [{'name': 'CI Gate', 'workflowName': 'CI', 'status': 'COMPLETED', 'conclusion': 'SUCCESS'}]}
        self.update_after_polls(None, None, stages={4: pending, 40: green, 42: {'mergeable': 'MERGEABLE'}})
        self.run_workflow(EVENT_NAME='push')
        self.assertEqual(self.state.get('polls'), 42)
        self.assertEqual(len(self.merge_calls()), 1)

    def test_missing_gate_beside_a_failed_ci_job_keeps_the_short_grace(self):
        # A failed CI job dooms the gate; do not hold the run for 25 min.
        doomed = [{'name': 'check', 'workflowName': 'CI', 'status': 'COMPLETED', 'conclusion': 'FAILURE'},
                  {'name': 'coverage-changed', 'workflowName': 'CI', 'status': 'IN_PROGRESS', 'conclusion': None}]
        self.state['detail']['mergeStateStatus'] = 'BLOCKED'
        self.state['detail']['statusCheckRollup'] = doomed
        self.update_after_polls(99, {}, seconds_per_poll=400)
        output = self.run_workflow(EVENT_NAME='schedule', AUTO_CUT_CI_WAIT_MINUTES='25')
        self.assertEqual(self.state.get('polls'), 1)
        self.assertEqual(self.merge_calls(), [])
        self.assertNotIn('but CI is still running', output)

    def test_missing_gate_without_ci_activity_keeps_the_short_grace(self):
        # Only finished CI jobs or other workflows: CI is not running, so the
        # 5-min grace applies — one 400-second poll exhausts it.
        idle = [{'name': 'route', 'workflowName': 'CI', 'status': 'COMPLETED', 'conclusion': 'SUCCESS'},
                {'name': 'probe', 'workflowName': 'Release', 'status': 'IN_PROGRESS', 'conclusion': None}]
        self.state['detail']['mergeStateStatus'] = 'BLOCKED'
        self.state['detail']['statusCheckRollup'] = idle
        self.update_after_polls(99, {}, seconds_per_poll=400)
        output = self.run_workflow(EVENT_NAME='schedule', AUTO_CUT_CI_WAIT_MINUTES='25')
        self.assertEqual(self.state.get('polls'), 1)
        self.assertEqual(self.merge_calls(), [])
        self.assertIn('CI Gate has not reported on the current head yet; CI wait budget exhausted', output)

    def test_running_ci_wait_is_still_bounded(self):
        running = [{'name': 'check', 'workflowName': 'CI', 'status': 'QUEUED', 'conclusion': None}]
        self.state['detail']['mergeStateStatus'] = 'BLOCKED'
        self.state['detail']['statusCheckRollup'] = running
        self.update_after_polls(99, {}, seconds_per_poll=400)
        output = self.run_workflow(EVENT_NAME='schedule', AUTO_CUT_CI_WAIT_MINUTES='25')
        self.assertEqual(self.state.get('polls'), 4)
        self.assertEqual(self.merge_calls(), [])
        self.assertIn('CI wait budget exhausted', output)

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

    def test_every_exit_path_reports_its_decision(self):
        # One fixture per exit path of the merge step; run_workflow asserts
        # the single well-formed decision line, this pins outcome + reason.
        import datetime
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        def detail(**kw):
            return lambda: self.state['detail'].update(kw)
        def pr(**kw):
            return lambda: self.state['pr'].update(kw)
        def pending():
            self.pending_ci()
        def write_main_change():
            self.write('crates/intentd/src/main.rs', 'fn main() { panic!(); }\n')
            self.state['detail']['headRefOid'] = self.commit()
        def main_base_moved():
            self.git('checkout', '-q', self.base)
            self.write('unreleased', 'main advanced')
            self.state['detail']['baseRefOid'] = self.commit()
        def no_release_pr():
            self.state['pr']['headRefName'] = 'ordinary'
        def two_release_prs():
            mock = (self.bin / 'gh').read_text().replace("emit([s['pr']])", "emit([s['pr'], {**s['pr'], 'number': 43}])")
            (self.bin / 'gh').write_text(mock)
        cases = [
            ('release-commit', {'EVENT_NAME': 'push', 'HEAD_COMMIT_MSG': 'chore: release v0.9.135'}, None, ('noop', 'release-commit')),
            ('throttled', {'EVENT_NAME': 'schedule'}, lambda: self.state.update(last_cut=now), ('throttle', 'recent-cut')),
            ('no release PR', {}, no_release_pr, ('noop', 'no-release-pr')),
            ('two release PRs', {}, two_release_prs, ('refuse', 'multiple-release-prs')),
            ('untrusted author', {}, pr(author={'login': 'outsider'}), ('refuse', 'untrusted-author')),
            ('hold-release', {}, pr(labels=[{'name': 'hold-release'}]), ('hold', 'hold-release')),
            ('draft', {}, detail(isDraft=True), ('skip', 'draft')),
            ('behind', {}, detail(mergeStateStatus='BEHIND'), ('defer', 'pr-behind')),
            ('unknown mergeability', {'EVENT_NAME': 'schedule'}, lambda: (detail(mergeable='UNKNOWN')(), self.update_after_polls(99, {})), ('defer', 'mergeable-unknown')),
            ('conflict', {}, detail(mergeable='CONFLICTING'), ('skip', 'not-mergeable')),
            ('review required', {}, detail(reviewDecision='REVIEW_REQUIRED'), ('defer', 'review-or-base')),
            ('human thread', {}, lambda: self.state.update(unresolved=1), ('skip', 'review-threads')),
            ('thread read error', {}, lambda: self.state.update(thread_error=True), ('defer', 'api-error')),
            ('failed gate', {}, detail(statusCheckRollup=[{'name': 'CI Gate', 'conclusion': 'FAILURE'}]), ('skip', 'ci-gate-failed')),
            ('missing gate', {'AUTO_CUT_CI_WAIT_MINUTES': '0'}, detail(mergeStateStatus='BLOCKED', statusCheckRollup=[]), ('defer', 'ci-gate-missing')),
            ('pending gate', {'AUTO_CUT_CI_WAIT_MINUTES': '0'}, pending, ('defer', 'ci-gate-pending')),
            ('green but blocked', {}, detail(mergeStateStatus='BLOCKED'), ('defer', 'review-or-base')),
            ('unreadable head', {}, detail(headRefOid='nope'), ('defer', 'api-error')),
            ('stale push head', {'EVENT_NAME': 'push', 'AUTO_CUT_CI_WAIT_MINUTES': '0'}, lambda: (self.state.update(compare='behind'), self.update_after_polls(99, {}, seconds_per_poll=16 * 60)), ('defer', 'pr-stale')),
            ('main CI lookup error', {}, lambda: self.state.update(main_runs_error=True), ('defer', 'api-error')),
            ('main CI missing', {'AUTO_CUT_CI_WAIT_MINUTES': '0'}, lambda: self.state.update(main_runs=[]), ('defer', 'main-ci-pending')),
            ('main CI running', {'AUTO_CUT_CI_WAIT_MINUTES': '0'}, lambda: self.state.update(main_runs=[self.main_run(5, 'in_progress', None)]), ('defer', 'main-ci-pending')),
            ('main CI red', {}, lambda: self.state.update(main_runs=[self.main_run(7, 'completed', 'failure')]), ('skip', 'main-ci-failed')),
            ('base not contained', {}, main_base_moved, ('defer', 'pr-stale')),
            ('dry run direct', {'DRY_RUN': 'true'}, None, ('dry-run', 'direct-merge')),
            ('dry run queue', {'DRY_RUN': 'true'}, write_main_change, ('dry-run', 'merge-queue')),
            ('direct merge', {}, None, ('merge', 'direct-merge')),
            ('queued merge', {}, write_main_change, ('queue', 'merge-queue')),
            ('merge failed', {}, lambda: self.state.update(changed_head=True), ('error', 'merge-failed')),
        ]
        mock = (self.bin / 'gh').read_text()
        for name, env, setup, expected in cases:
            with self.subTest(name):
                self.tearDown()
                self.setUp()
                (self.bin / 'gh').write_text(mock)
                if setup:
                    setup()
                self.run_workflow(**env)
                self.assertEqual(self.last_decision, expected)

    def test_unplanned_failure_still_reports_a_decision(self):
        # A `set -e` failure outside every guard (here: the clock read that
        # arms the CI wait budget) must still leave one decision line, via
        # the EXIT trap, and keep the failing status.
        date = self.bin / 'date'
        date.write_text('#!/bin/sh\nexit 5\n')
        date.chmod(0o755)
        output = self.run_workflow(expect_status=5, EVENT_NAME='workflow_dispatch')
        self.assertEqual(self.last_decision, ('error', 'unexpected-exit'))
        self.assertIn('status 5', output)

    def test_merge_step_never_exits_around_decide(self):
        # Every exit goes through decide(); a new bare `exit` would bypass
        # the decision contract on a path no fixture covers yet.
        code = [line.split('#', 1)[0] for line in self.merge_step().splitlines()]
        exits = [line.strip() for line in code if re.search(r'(?<![-\w])exit\b', line)]
        self.assertEqual(exits, ['exit "${4:-0}"'])

    def test_missing_release_token_reports_a_decision(self):
        workflow = (ROOT / '.github/workflows/auto-cut-alpha.yml').read_text()
        step = workflow.split('      - name: Check release token\n', 1)[1].split('      - uses:', 1)[0]
        script = textwrap.dedent(step.split('        run: |\n', 1)[1])
        out, summary = self.repo / 'out', self.repo / 'summary'
        result = subprocess.run(['bash', '-e', '-c', script], env={**self.env, 'RELEASE_PLZ_TOKEN': '', 'GITHUB_OUTPUT': str(out), 'GITHUB_STEP_SUMMARY': str(summary)},
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        lines = [line for line in result.stdout.splitlines() if line.startswith('auto-cut-decision:')]
        self.assertEqual(len(lines), 1)
        self.assertRegex(lines[0], DECISION)
        self.assertTrue(lines[0].startswith('auto-cut-decision: skip no-release-token '))
        self.assertEqual(summary.read_text().splitlines(), lines)

if __name__ == '__main__':
    unittest.main()
