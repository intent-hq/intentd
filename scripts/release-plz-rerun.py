#!/usr/bin/env python3
"""Re-run a failed Release-plz planning run on main's head, or fail loudly.

The frontend's alpha cut exempts intentd test/ci-only commits only when the
newest Release-plz push run on intentd main's head succeeded with its "no
release needed" marker (cloudlands-fe scripts/intentd-release-readiness.mjs).
A failed planning run on that head is never retried by anything else until
the next intentd push, so every ten-minute cut defers silently: run
40430078531 (crates.io "HTTP2 framing layer" in release-pr) held the alpha
from about 01:00 to 07:00 UTC until someone ran `gh run rerun --failed`.

This does that rerun, bounded and only when it is safe:

- nothing to do: no run yet, still running, or succeeded;
- rerun: the run failed, the tag-publishing "Release-plz release" job
  succeeded, "Release-plz PR" is the only failed job, its log shows a network
  error (or the job timed out), and the run has had fewer than MAX_ATTEMPTS
  attempts. rerun-failed-jobs re-runs just the planning job and its marker
  job, as the manual recovery did;
- fail loudly (exit 1, ::error plus a step summary): attempts used up, a
  planning failure that is not a recognised network error (a real failure
  would only repeat), any other job failed (tag publication is never replayed
  automatically), the run ended cancelled/startup_failure/..., or the GitHub
  API could not be read or refused the rerun. While main's head stays
  blocked, each check fails again: the frontend is still deferring.

Only main's current head matters: an older run is superseded by the newer
push's own run. release-plz-rerun.yml calls this on Release-plz completion
and on a schedule. scripts/test-release-plz-rerun.py covers it offline.

Run: GH_TOKEN=... python3 -I -B scripts/release-plz-rerun.py [--dry-run]
"""
import argparse
import json
import os
import re
import subprocess
import sys

REPO = 'intent-hq/intentd'
WORKFLOW = 'release-plz.yml'
WORKFLOW_PATH = '.github/workflows/release-plz.yml'
PLANNING_JOB = 'Release-plz PR'
RELEASE_JOB = 'Release-plz release'
# The original attempt plus two automatic reruns; the planning job already
# retries release-pr once inside each attempt.
MAX_ATTEMPTS = 3
FAILED = {'failure', 'timed_out', 'cancelled', 'startup_failure'}
# cargo/curl/reqwest wording for transient network failures. Matching is a
# gate for automatic reruns only; anything else is reported for a human.
NETWORK_ERROR = re.compile(
    r'curl failed|Error in the HTTP2 framing layer|spurious network error|failed to download from'
    r'|Timeout was reached|SSL connect error|Connection reset by peer|Failure when receiving data from the peer'
    r'|Could not resolve host|error sending request for url|failed to get 200 response from'
    r'|operation timed out|connection closed before message completed|Couldn\'t connect to server',
    re.IGNORECASE)
SHA = re.compile(r'[0-9a-f]{40}')
IMPACT = ('Until this run succeeds (or intentd main gets a new push), the cloudlands-fe alpha cut defers '
          'with "intentd-skew" whenever intentd main is ahead of the pinned sidecar.')

NONE, RERUN, LOUD = 'none', 'rerun', 'loud'


class ApiError(Exception):
    pass


def gh_api(endpoint, method='GET', raw=False):
    # Job logs carry ANSI colour codes, which gh refuses to print by default.
    command = ['gh', 'api', '--method', method, *(['--allow-escape-sequences'] if raw else []), endpoint]
    try:
        out = subprocess.run(command, check=True, capture_output=True, text=True, timeout=30).stdout
    except (OSError, subprocess.SubprocessError) as error:
        # TimeoutExpired carries bytes even with text=True.
        stderr = getattr(error, 'stderr', None) or ''
        if isinstance(stderr, bytes):
            stderr = stderr.decode('utf-8', 'replace')
        raise ApiError(f'gh api {method} {endpoint} failed: {stderr.strip() or error}') from error
    if raw:
        return out
    if method != 'GET':
        return None
    try:
        return json.loads(out)
    except ValueError as error:
        raise ApiError(f'gh api {endpoint} returned invalid JSON') from error


def run_url(run):
    return run.get('html_url') or f'https://github.com/{REPO}/actions/runs/{run.get("id")}'


def latest_run(runs, head):
    """The newest trusted Release-plz main push run for `head`, or None."""
    if not isinstance(runs, list):
        raise ApiError('unreadable workflow run listing')
    for run in runs:
        if not (isinstance(run, dict) and isinstance(run.get('id'), int) and isinstance(run.get('run_number'), int)
                and isinstance(run.get('run_attempt'), int) and run.get('head_sha') == head
                and run.get('path') == WORKFLOW_PATH and run.get('event') == 'push'
                and run.get('head_branch') == 'main'):
            raise ApiError('unexpected run in the Release-plz listing')
    return max(runs, key=lambda run: run['run_number'], default=None)


def manual(run):
    return f'`gh run rerun {run["id"]} --failed --repo {REPO}`'


def planning_job(jobs):
    """The one failed "Release-plz PR" job when it is the run's only failure, else None."""
    if not isinstance(jobs, list) or not all(isinstance(job, dict) and isinstance(job.get('name'), str)
                                             for job in jobs):
        raise ApiError('unreadable job listing')
    failed = [job for job in jobs if job.get('conclusion') in FAILED]
    release = [job for job in jobs if job['name'] == RELEASE_JOB]
    if (len(failed) == 1 and failed[0]['name'] == PLANNING_JOB and isinstance(failed[0].get('id'), int)
            and len(release) == 1 and release[0].get('conclusion') == 'success'):
        return failed[0]
    return None


def decide(head, run, jobs, log=None, max_attempts=MAX_ATTEMPTS):
    """Return (action, message) for the newest Release-plz run on main's head.

    `log` is the failed planning job's log (needed only for a rerun decision).
    """
    short = head[:9]
    if run is None:
        return NONE, f'No Release-plz run for intentd main head {short} yet.'
    url = run_url(run)
    if run.get('status') != 'completed':
        return NONE, f'Release-plz run {url} on {short} is {run.get("status")}; nothing to do yet.'
    conclusion = run.get('conclusion')
    if conclusion == 'success':
        return NONE, f'Release-plz run {url} on {short} succeeded.'
    attempt = run['run_attempt']
    if conclusion not in {'failure', 'timed_out'}:
        # Cancelled or never started: no marker either, but a cancel may be
        # deliberate and a job may have stopped mid-publication.
        return LOUD, (f'Release-plz run {url} on intentd main head {short} concluded {conclusion} (attempt '
                      f'{attempt}); it is not re-run automatically. Check the run, then re-run it by hand '
                      f'({manual(run)}) once it is safe. {IMPACT}')
    job = planning_job(jobs)
    if job is None:
        names = ', '.join(sorted({j['name'] for j in jobs if j.get('conclusion') in FAILED})) or 'none reported'
        return LOUD, (f'Release-plz run {url} on intentd main head {short} failed (attempt {attempt}; failed jobs: '
                      f'{names}). Only a lone "{PLANNING_JOB}" failure after a successful "{RELEASE_JOB}" is '
                      'retried automatically, so tag publication is never replayed: inspect the run and re-run '
                      f'it by hand ({manual(run)}) once it is safe. {IMPACT}')
    if job.get('conclusion') != 'timed_out' and not NETWORK_ERROR.search(log or ''):
        return LOUD, (f'Release-plz run {url} on intentd main head {short} failed in "{PLANNING_JOB}" (attempt '
                      f'{attempt}), and its log shows the failure is not a recognised network error, so a re-run would '
                      f'only repeat it. Fix the cause, or re-run it by hand ({manual(run)}) if it was transient. {IMPACT}')
    if attempt >= max_attempts:
        return LOUD, (f'Release-plz run {url} on intentd main head {short} still fails after {attempt} attempts in '
                      f'"{PLANNING_JOB}". Read its log: re-run it by hand ({manual(run)}) if the failure is a '
                      f'network error, otherwise fix the cause. {IMPACT}')
    return RERUN, (f'Release-plz run {url} on intentd main head {short} failed in "{PLANNING_JOB}" '
                   f'(attempt {attempt} of {max_attempts}); re-running its failed jobs.')


def assess(api=gh_api):
    ref = api(f'repos/{REPO}/git/ref/heads/main')
    head = (ref or {}).get('object', {}).get('sha') if isinstance(ref, dict) else None
    if not isinstance(head, str) or not SHA.fullmatch(head):
        raise ApiError('unreadable intentd main head')
    listing = api(f'repos/{REPO}/actions/workflows/{WORKFLOW}/runs?branch=main&event=push&head_sha={head}'
                  '&per_page=100')
    runs = listing.get('workflow_runs') if isinstance(listing, dict) else None
    run = latest_run(runs, head)
    jobs = log = None
    if run is not None and run.get('status') == 'completed' and run.get('conclusion') in {'failure', 'timed_out'}:
        data = api(f'repos/{REPO}/actions/runs/{run["id"]}/attempts/{run["run_attempt"]}/jobs?per_page=100')
        jobs = data.get('jobs') if isinstance(data, dict) else None
        job = planning_job(jobs)
        if job is not None and job.get('conclusion') != 'timed_out':
            log = api(f'repos/{REPO}/actions/jobs/{job["id"]}/logs', raw=True)
    return head, run, jobs, log


def identity(run):
    keys = ('id', 'run_attempt', 'status', 'conclusion')
    return None if run is None else tuple(run.get(key) for key in keys)


def report(action, message, out=print):
    level = {NONE: 'notice', RERUN: 'warning', LOUD: 'error'}[action]
    out(f'::{level}::{message}')
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        try:
            with open(summary, 'a', encoding='utf-8') as handle:
                handle.write(f'{"**Action needed:** " if action == LOUD else ""}{message}\n')
        except OSError:
            out('::warning::Could not write the step summary.')


def main(argv=None, api=gh_api, out=print):
    parser = argparse.ArgumentParser(description=__doc__.split('\n', 1)[0])
    parser.add_argument('--dry-run', action='store_true', help='report the decision without re-running')
    args = parser.parse_args(argv)
    try:
        head, run, jobs, log = assess(api)
        action, message = decide(head, run, jobs, log)
        if action == RERUN and not args.dry_run:
            # Bound the race with a concurrent push or manual rerun: act only
            # if main and the run are exactly as assessed.
            again_head, again = assess(api)[:2]
            if again_head != head or identity(again) != identity(run):
                report(NONE, 'intentd main or its Release-plz run changed while assessing; the next check '
                       'decides again.', out)
                return 0
            try:
                api(f'repos/{REPO}/actions/runs/{run["id"]}/rerun-failed-jobs', method='POST')
            except ApiError as error:
                report(LOUD, f'GitHub refused the re-run of Release-plz run {run_url(run)} (attempt '
                       f'{run["run_attempt"]}) on intentd main head {head[:9]}: {error}. Re-run it by hand '
                       f'({manual(run)}). {IMPACT}', out)
                return 1
    except ApiError as error:
        report(LOUD, f'Could not check intentd Release-plz runs ({error}); re-run this workflow, and check that '
               f'the newest Release-plz run on intentd main succeeded. {IMPACT}', out)
        return 1
    if args.dry_run and action == RERUN:
        message += ' (dry run: not re-running)'
    report(action, message, out)
    return 1 if action == LOUD else 0


if __name__ == '__main__':
    sys.exit(main())
