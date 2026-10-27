#!/usr/bin/env python3
"""Cancel a merge-queue CI run as soon as one of its jobs fails.

Runs as ci.yml's `fail-fast` job on merge_group only. The queue builds one
entry at a time and requires only `CI Gate`, which waits for every job and
rejects any failure, so the first failed job already dooms the entry. Before
this watcher a doomed entry held the queue (and paid macOS/Linux runners)
until the slowest job finished: a median 16 min after the first failure
across Oct 20-27, 2026 (41 failed runs).

It polls this attempt's jobs and, on the first fatal conclusion of any job
other than itself and the gate, cancels the run. `CI Gate` (`if: always()`)
still runs on the cancelled run and fails, so the queue ejects the entry with
`failed_checks` exactly as before, only sooner.

Stop conditions: the gate job is visible (every other job finished; jobs
blocked on `needs` are absent from the jobs API, so "all visible jobs done"
is not evidence), the run was cancelled, or the deadline passed. The watcher
is advisory: every outcome exits 0, and it is not in the gate's `needs`, so
it can neither fail nor delay the required check.

https://docs.github.com/en/rest/actions/workflow-jobs#list-jobs-for-a-workflow-run-attempt
https://docs.github.com/en/rest/actions/workflow-runs#cancel-a-workflow-run
"""
import argparse
import json
import os
import subprocess
import sys
import time

SELF_NAME = 'Merge queue fail-fast'
GATE_NAME = 'CI Gate'
FATAL = ('failure', 'timed_out', 'cancelled', 'startup_failure')
POLL_SECONDS = 45
MAX_READ_ERRORS = 5
MAX_POST_ATTEMPTS = 3
# The job's timeout-minutes is 75; finish well before it so the watcher never
# shows a timed-out job of its own.
DEADLINE_SECONDS = 70 * 60
ERRORS = (subprocess.SubprocessError, OSError, ValueError, KeyError, TypeError, AttributeError)


def gh_api(method, path):
    """The sole network boundary: `gh api` with the job's token."""
    out = subprocess.run(['gh', 'api', '--method', method, path], check=True,
                         capture_output=True, text=True, timeout=60).stdout
    return json.loads(out) if out.strip() else {}


def list_jobs(api, repo, run_id, attempt):
    """Every job of this run attempt, across pages."""
    jobs, page = [], 1
    while True:
        data = api('GET', f'repos/{repo}/actions/runs/{run_id}/attempts/{attempt}/jobs?per_page=100&page={page}')
        chunk = data['jobs']
        jobs.extend(chunk)
        if not chunk or len(jobs) >= int(data['total_count']):
            return jobs
        page += 1


def decide(jobs):
    """Return ('done'|'cancel'|'continue', failed_job_or_None)."""
    if any(j.get('name') == GATE_NAME for j in jobs):
        # The gate exists only once every other job has finished; it reports
        # any failure itself, so cancelling now would only race it.
        return 'done', None
    failed = [j for j in jobs if j.get('name') != SELF_NAME and j.get('status') == 'completed'
              and j.get('conclusion') in FATAL]
    if failed:
        return 'cancel', min(failed, key=lambda j: j.get('completed_at') or '')
    return 'continue', None


def cancel(api, repo, run_id, log):
    for attempt in range(1, MAX_POST_ATTEMPTS + 1):
        try:
            api('POST', f'repos/{repo}/actions/runs/{run_id}/cancel')
            return True
        except ERRORS as err:
            log(f'cancel attempt {attempt} failed: {err}')
    return False


def summarize(text):
    path = os.environ.get('GITHUB_STEP_SUMMARY')
    if path:
        with open(path, 'a', encoding='utf-8') as fh:
            fh.write(text + '\n')


def watch(api, repo, run_id, attempt, *, log=print, clock=time.monotonic, sleep=time.sleep,
          dry_run=False, once=False, deadline_seconds=DEADLINE_SECONDS):
    """Poll until done, cancelled, given up or past the deadline; return the outcome."""
    start, errors = clock(), 0
    while True:
        try:
            jobs = list_jobs(api, repo, run_id, attempt)
            errors = 0
        except ERRORS as err:
            errors += 1
            log(f'reading jobs failed ({errors}/{MAX_READ_ERRORS}): {err}')
            if errors >= MAX_READ_ERRORS:
                log('::warning::fail-fast watcher stopped: the jobs API kept failing; the run continues normally')
                return 'gave-up'
            jobs = None
        if jobs is not None:
            action, failed = decide(jobs)
            if action == 'done':
                log('every other job finished; nothing to cancel')
                return 'done'
            if action == 'cancel':
                name, conclusion = failed.get('name'), failed.get('conclusion')
                if dry_run:
                    log(f'would cancel: {name!r} concluded {conclusion}')
                    return 'would-cancel'
                if not cancel(api, repo, run_id, log):
                    log(f'::warning::{name!r} {conclusion}, but cancelling the run failed; it continues to CI Gate')
                    return 'gave-up'
                log(f'::error::Cancelled this merge-queue run early: {name!r} concluded {conclusion}, '
                    f'so CI Gate cannot pass. See that job for the real failure.')
                summarize(f'### Merge queue fail-fast\n\nCancelled the run because `{name}` concluded '
                          f'`{conclusion}`. CI Gate fails on the cancelled run and the queue ejects the entry.')
                return 'cancelled'
            if once:
                log('no failed job yet')
                return 'continue'
        if clock() - start + POLL_SECONDS > deadline_seconds:
            log('deadline reached before the gate started; exiting without cancelling')
            return 'deadline'
        sleep(POLL_SECONDS)


def main(argv=None, api=gh_api, clock=time.monotonic, sleep=time.sleep):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--repo', required=True)
    parser.add_argument('--run-id', required=True, type=int)
    parser.add_argument('--attempt', required=True, type=int)
    parser.add_argument('--dry-run', action='store_true', help='decide and print, never cancel')
    parser.add_argument('--once', action='store_true', help='evaluate one snapshot and exit')
    args = parser.parse_args(argv)
    outcome = watch(api, args.repo, args.run_id, args.attempt, log=lambda m: print(m, flush=True),
                    clock=clock, sleep=sleep, dry_run=args.dry_run, once=args.once)
    print(f'outcome={outcome}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
