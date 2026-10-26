#!/usr/bin/env python3
"""Retire main-push CI runs that a newer main commit supersedes.

Runs as ci.yml's `supersede` job on every push to main. For this run's commit
S it:

1. reads and validates main's live tip T first (nothing is cancelled when it
   is unreadable),
2. cancels every other live push-event ci.yml run on main whose commit is a
   strict ancestor of T (compare run...T is `ahead` with behind_by 0), and
3. prints `superseded=true` when T is strictly ahead of S and every read and
   cancel succeeded, so this run skips its own Windows post-merge job;
   otherwise `superseded=false`.

Only ancestry against the validated live tip decides, never run order or the
caller's commit: GitHub admits concurrency-group runs in no guaranteed order,
and a re-run of an old scan (or a main reset) must not cancel the tip's run.
T is never a strict ancestor of itself, so its run is never cancelled here; an
older run admitted after the tip's finds main ahead of it and skips its
Windows job instead. Every unreadable or unexpected answer reports
`superseded=false`: the cost of an error is one extra Windows run, never a
missing proof on the tip. The cancel request is best-effort (202 means
accepted, not stopped). Residual race, as in retire-obsolete-merge-runs.py:
the tip can only move between the read and a cancel by a push (whose run
then supersedes the cancelled one) or by an administrative reset the ruleset
forbids.

https://docs.github.com/en/rest/commits/commits#compare-two-commits
https://docs.github.com/en/rest/actions/workflow-runs#cancel-a-workflow-run
https://docs.github.com/en/actions/writing-workflows/choosing-what-your-workflow-does/control-the-concurrency-of-workflows-and-jobs
"""
import argparse
import json
import re
import subprocess
import sys

LIVE = ('queued', 'in_progress', 'waiting', 'pending', 'requested')
SHA = re.compile(r'[0-9a-f]{40}')
WORKFLOW = '.github/workflows/ci.yml'


def gh_api(method, path):
    """The sole network boundary: `gh api` with the job's token."""
    out = subprocess.run(['gh', 'api', '--method', method, path], check=True,
                         capture_output=True, text=True, timeout=60).stdout
    return json.loads(out) if out.strip() else {}


def strictly_behind(api, repo, older, newer):
    """True only when `older` is a strict ancestor of `newer`."""
    if older == newer:
        return False
    cmp = api('GET', f'repos/{repo}/compare/{older}...{newer}')
    return cmp.get('status') == 'ahead' and cmp.get('behind_by') == 0 and cmp.get('ahead_by', 0) > 0


def live_runs(api, repo):
    for status in LIVE:
        page = api('GET', f'repos/{repo}/actions/workflows/ci.yml/runs?event=push&branch=main&status={status}&per_page=100')
        yield from page.get('workflow_runs', [])


def supersede(api, repo, sha, run_id, log=print):
    """Cancel live main-push runs strictly behind main's live tip.

    Returns True only when every read and cancel succeeded and `sha` is
    strictly behind the live tip. The tip is read first and is the only
    reference for cancelling, so a caller on an older or reset-away commit
    can never cancel the tip's run; an unreadable tip cancels nothing.
    """
    if not SHA.fullmatch(sha):
        raise ValueError(f'not a commit sha: {sha!r}')
    errors = (subprocess.SubprocessError, OSError, ValueError, KeyError, TypeError, AttributeError)
    try:
        tip = api('GET', f'repos/{repo}/git/ref/heads/main')['object']['sha']
        if not SHA.fullmatch(str(tip)):
            raise ValueError(f'unreadable main tip: {tip!r}')
    except errors as err:
        log(f'::warning::cancelling nothing and treating {sha[:12]} as the tip: {err}')
        return False
    clean = True
    try:
        for run in live_runs(api, repo):
            other = str(run.get('head_sha', ''))
            if (run.get('id') == run_id or run.get('event') != 'push' or run.get('head_branch') != 'main'
                    or run.get('path') != WORKFLOW or not SHA.fullmatch(other)):
                continue
            try:
                if strictly_behind(api, repo, other, tip):
                    api('POST', f'repos/{repo}/actions/runs/{run["id"]}/cancel')
                    log(f'cancelled superseded push run {run["id"]} on {other[:12]}')
            except errors as err:
                clean = False
                log(f'::warning::left push run {run.get("id")} running: {err}')
    except errors as err:
        clean = False
        log(f'::warning::could not list live push runs: {err}')
    try:
        superseded = strictly_behind(api, repo, sha, tip)
    except errors as err:
        log(f'::warning::treating {sha[:12]} as the tip: {err}')
        return False
    if superseded and not clean:
        log(f'::warning::running Windows on superseded {sha[:12]}: the scan hit an API error')
    return superseded and clean


def main(argv=None, api=gh_api):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--repo', required=True)
    parser.add_argument('--sha', required=True)
    parser.add_argument('--run-id', required=True, type=int)
    args = parser.parse_args(argv)
    superseded = supersede(api, args.repo, args.sha, args.run_id, log=lambda m: print(m, file=sys.stderr))
    print(f'superseded={"true" if superseded else "false"}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
