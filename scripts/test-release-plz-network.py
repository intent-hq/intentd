#!/usr/bin/env python3
"""Release-plz planning crates.io contract: survive a network blip in place.

`release-plz release-pr` runs `cargo package`, which re-resolves every crate
against the crates.io index, so it cannot run offline like the dist steps
(scripts/test-release-host-network.py). Run 40430078531 failed there on
"curl failed: [16] Error in the HTTP2 framing layer" while downloading the
sparse index config.json; with no "Release-plz no release needed" marker the
frontend alpha cut deferred for six hours. The planning job therefore fetches
the locked crates with retries, turns off HTTP/2 multiplexing, raises cargo's
own retries, and runs release-pr a second time when the first attempt fails.
scripts/release-plz-rerun.py re-runs the job when both attempts fail.
Requires PyYAML.

Run: python3 -B scripts/test-release-plz-network.py
"""
from pathlib import Path
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / '.github/workflows/release-plz.yml'
FETCH_RUN = 'python3 -I -B scripts/fetch-locked-crates.py'
ACTION = 'release-plz/action@'
NETWORK_ENV = {'CARGO_HTTP_MULTIPLEXING': 'false', 'CARGO_NET_RETRY': '10'}


def planning_steps(text=None):
    doc = yaml.safe_load(WORKFLOW.read_text() if text is None else text)
    return doc['jobs']['release-plz-pr']['steps']


def by_id(steps, step_id):
    matches = [i for i, step in enumerate(steps) if step.get('id') == step_id]
    if len(matches) != 1:
        raise AssertionError(f'expected exactly one step with id {step_id!r}, found {len(matches)}')
    return matches[0]


class ReleasePlzNetwork(unittest.TestCase):
    def setUp(self):
        self.steps = planning_steps()

    def test_locked_crates_are_fetched_with_retries_before_planning(self):
        fetches = [i for i, step in enumerate(self.steps) if step.get('run') == FETCH_RUN]
        self.assertEqual(len(fetches), 1, 'one retrying crate fetch in the planning job')
        self.assertLess(fetches[0], by_id(self.steps, 'release_pr'))
        toolchain = next(i for i, step in enumerate(self.steps)
                         if 'dtolnay/rust-toolchain' in step.get('uses', ''))
        self.assertLess(toolchain, fetches[0], 'the pinned toolchain is installed before fetching')

    def test_every_planning_attempt_uses_the_resilient_cargo_network(self):
        attempts = [step for step in self.steps if step.get('uses', '').startswith(ACTION)]
        self.assertEqual([step.get('id') for step in attempts], ['release_pr', 'release_pr_retry'])
        for step in attempts:
            env = step.get('env') or {}
            for key, value in NETWORK_ENV.items():
                self.assertEqual(str(env.get(key)), value, f'{step["id"]}: {key}')
            self.assertEqual(env.get('GITHUB_TOKEN'), '${{ secrets.RELEASE_PLZ_TOKEN }}')

    def test_retry_repeats_the_first_attempt_only_after_it_fails(self):
        first = self.steps[by_id(self.steps, 'release_pr')]
        retry_index = by_id(self.steps, 'release_pr_retry')
        retry = self.steps[retry_index]
        self.assertIs(first.get('continue-on-error'), True)
        self.assertNotIn('continue-on-error', retry, 'a failed retry fails the job')
        self.assertEqual(retry['uses'], first['uses'])
        self.assertEqual(retry['with'], first['with'])
        self.assertEqual(retry.get('if'), "${{ steps.release_pr.outcome == 'failure' }}")
        backoff = self.steps[retry_index - 1]
        self.assertEqual(backoff.get('if'), retry['if'])
        self.assertIn('sleep', backoff['run'])
        cleanup = next(i for i, step in enumerate(self.steps) if 'release-plz-planning.toml' in step.get('run', '')
                       and step['run'].startswith('rm '))
        self.assertGreater(cleanup, retry_index, 'the planning config outlives the retry')

    def test_no_op_marker_reads_whichever_attempt_succeeded(self):
        step = self.steps[by_id(self.steps, 'no_release')]
        succeeded = "(steps.release_pr.outcome == 'success' || steps.release_pr_retry.outcome == 'success')"
        self.assertEqual(step['if'], '${{ success() && ' + succeeded + ' }}')
        self.assertEqual(step['env']['RELEASE_PLZ_OUTCOME'],
                         "${{ steps.release_pr.outcome == 'success' && 'success' "
                         "|| steps.release_pr_retry.outcome }}")
        self.assertEqual(step['env']['RELEASE_PLZ_PRS'],
                         "${{ steps.release_pr.outcome == 'success' && steps.release_pr.outputs.prs "
                         "|| steps.release_pr_retry.outputs.prs }}")

    def test_only_the_first_planning_attempt_may_continue_on_error(self):
        doc = yaml.safe_load(WORKFLOW.read_text())
        allowed = []
        for name, job in doc['jobs'].items():
            self.assertNotIn('continue-on-error', job, f'{name}: job-level continue-on-error')
            for step in job.get('steps', []):
                if 'continue-on-error' in step:
                    allowed.append((name, step.get('id')))
        self.assertEqual(allowed, [('release-plz-pr', 'release_pr')])

    def test_tag_publication_job_stays_online_and_unretried(self):
        doc = yaml.safe_load(WORKFLOW.read_text())
        release = yaml.safe_dump(doc['jobs']['release-plz-release'])
        self.assertNotIn(FETCH_RUN, release)
        self.assertNotIn('CARGO_NET_OFFLINE', WORKFLOW.read_text())

    def test_fetch_script_documents_this_caller(self):
        self.assertIn('release-plz.yml', (ROOT / 'scripts/fetch-locked-crates.py').read_text())


if __name__ == '__main__':
    unittest.main()
