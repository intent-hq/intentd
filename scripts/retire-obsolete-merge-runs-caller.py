#!/usr/bin/env python3
"""Bounded periodic sweep around the independently reviewed retirement policy.

A completed sweep resets its cursor: older late-visible runs and candidates
excluded only by later main advancement must be reconsidered. Partial sweeps
checkpoint after each complete batch. API uncertainty stops this invocation;
only the next scheduled invocation retries. Cursors are progress, not authority.
"""
import argparse
import importlib.util
import json
import os
import re
import subprocess
from pathlib import Path
import sys
import time

SPEC = importlib.util.spec_from_file_location('retirement', Path(__file__).with_name('retire-obsolete-merge-runs.py'))
P = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(P)


class WorkflowApi:
    """Read evidence with the proven read credential; cancel with the job token.

    Credentials go only in the child process environment, never argv/logs.
    No fallback: losing either credential must be visible, not silently skipped.
    """
    def __init__(self, repository, read_token, write_token):
        P.require(repository in P.WORKFLOWS, 'repository not allowlisted')
        P.require(bool(read_token) and bool(write_token), 'workflow credentials unavailable')
        self.root = '/repos/' + repository
        self.read_token, self.write_token = read_token, write_token

    def request(self, method, path, body=None):
        cancel = (method == 'POST' and body is None and re.fullmatch(
            re.escape(self.root) + r'/actions/runs/[1-9][0-9]*/cancel', path))
        read = ((method == 'GET' and (path == self.root or path.startswith(self.root + '/')))
                or (method == 'POST' and path == '/graphql' and isinstance(body, dict)
                    and body.get('query') == P.QUEUE_QUERY))
        P.require(cancel or read, 'workflow transport refuses non-local or unexpected request')
        env = {key: value for key, value in os.environ.items()
               if key not in ('RETIREMENT_READ_TOKEN', 'GH_TOKEN', 'GITHUB_TOKEN')}
        env['GH_TOKEN'] = self.write_token if cancel else self.read_token
        command = ['gh', 'api', '--hostname', 'github.com', '--include', '--method', method,
                   '-H', 'Accept: application/vnd.github+json',
                   '-H', 'X-GitHub-Api-Version: 2022-11-28', path]
        if body is not None:
            command += ['--input', '-']
        try:
            result = subprocess.run(command, input=None if body is None else json.dumps(body),
                                    env=env, text=True, capture_output=True, timeout=30)
            response = P.parse_response(result.stdout)
        except (OSError, subprocess.TimeoutExpired, ValueError) as error:
            raise P.SafetyError('GitHub request failed; no retry') from error
        P.require(result.returncode == 0 or response.status >= 400, 'gh failed without HTTP error')
        return response


class BudgetApi:
    def __init__(self, api, requests, seconds):
        self.api, self.remaining = api, requests
        self.deadline = time.monotonic() + seconds

    def request(self, method, path, body=None):
        P.require(self.remaining > 0, 'total request budget exhausted')
        P.require(time.monotonic() < self.deadline, 'sweep deadline reached')
        self.remaining -= 1
        return self.api.request(method, path, body)


class ReadOnlyApi:
    def __init__(self, api):
        self.api = api

    def request(self, method, path, body=None):
        P.require(method == 'GET' or (method == 'POST' and path == '/graphql'
                  and isinstance(body, dict) and body.get('query') == P.QUEUE_QUERY),
                  'credential probe refuses mutations')
        return self.api.request(method, path, body)


def probe(api, repository):
    """Always attempt BOTH proofs, even on empty discovery or a failed proof.

    None cannot equal a valid SHA: activity() must validate all refs/queue
    entries rather than short-circuit on a particular protected candidate.
    """
    result = {}
    for name in ('protection', 'activity'):
        try:
            client = P.Client(ReadOnlyApi(api), repository, 10, 100)
            ctx = P.context(client)
            if name == 'protection':
                proof = P.protection(client, ctx)
                result[name] = {'ok': True, 'rule_ids': proof['rule_ids'], 'bypass_verified': True}
            else:
                P.activity(client, ctx, None)
                result[name] = {'ok': True, 'refs_and_queue_complete': True}
        except (P.SafetyError, P.Skip, KeyError, TypeError, AttributeError) as error:
            result[name] = {'ok': False, 'error': str(error)}
    result['ok'] = all(value['ok'] for value in result.values())
    return result


def sweep(api, repository, *, apply=False, after_run_id=0, max_batches=5,
          batch_size=10, max_requests=500, seconds=300, checkpoint=lambda cursor: None):
    P.require(repository in P.WORKFLOWS, 'repository not allowlisted')
    P.require(P.integer(after_run_id), 'invalid cursor')
    P.require(all(P.integer(v, 1) for v in (max_batches, batch_size, max_requests)), 'invalid bounds')
    bounded = BudgetApi(api, max_requests, seconds)
    result = {'repository': repository, 'mode': 'apply' if apply else 'dry-run',
              'complete': False, 'next_after_run_id': after_run_id, 'batches': []}
    result['probe'] = probe(bounded, repository)
    if not result['probe']['ok']:
        result['error'] = 'credential/activity proof failed; no reconciliation attempted'
        return result
    for _ in range(max_batches):
        batch = P.reconcile(bounded, repository, apply=apply, after_run_id=after_run_id,
                            max_candidates=batch_size)
        result['batches'].append(batch)
        if batch.get('complete') is not True:
            result['error'] = batch.get('error', 'incomplete batch')
            break
        cursor = batch.get('next_after_run_id')
        if cursor is not None and not (P.integer(cursor, 1) and cursor > after_run_id):
            result['error'] = 'invalid or non-advancing batch cursor'
            break
        after_run_id = 0 if cursor is None else cursor
        checkpoint(after_run_id)
        result['next_after_run_id'] = after_run_id
        if cursor is None:
            result['complete'] = True
            break
    result['requests_used'] = max_requests - bounded.remaining
    return result


def read_cursor(path, repository, mode):
    if not path.exists():
        return 0
    P.require(path.stat().st_size <= 1024, 'oversized cursor')
    data = json.loads(path.read_text())
    P.require(data.get('repository') == repository and data.get('mode') == mode
              and data.get('version') == 1 and P.integer(data.get('after_run_id')),
              'invalid cursor identity or format')
    return data['after_run_id']


def write_cursor(path, repository, mode, cursor):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps({'version': 1, 'repository': repository,
                                     'mode': mode, 'after_run_id': cursor}) + '\n')
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', required=True, choices=sorted(P.WORKFLOWS))
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--apply', action='store_true')
    modes.add_argument('--dry-run', action='store_true')
    modes.add_argument('--probe-only', action='store_true')
    parser.add_argument('--state', type=Path)
    parser.add_argument('--workflow-credentials', action='store_true',
                        help='Use RETIREMENT_READ_TOKEN for evidence and GH_TOKEN only for cancellation')
    args = parser.parse_args()
    mode = 'apply' if args.apply else 'dry-run'
    try:
        api = (WorkflowApi(args.repository, os.environ.get('RETIREMENT_READ_TOKEN'), os.environ.get('GH_TOKEN'))
               if args.workflow_credentials else P.GhApi())
        if args.probe_only:
            result = probe(BudgetApi(api, 200, 180), args.repository)
            success = result['ok']
        else:
            cursor = read_cursor(args.state, args.repository, mode) if args.state else 0
            def checkpoint(value):
                if args.state:
                    write_cursor(args.state, args.repository, mode, value)
            result = sweep(api, args.repository, apply=args.apply,
                           after_run_id=cursor, checkpoint=checkpoint)
            # A clean work bound is resumable success, not uncertain API evidence.
            success = 'error' not in result
    except (ValueError, OSError, TypeError, AttributeError) as error:
        result, success = {'error': str(error), 'complete': False}, False
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if success else 2


if __name__ == '__main__':
    sys.exit(main())
