#!/usr/bin/env python3
"""Retire only ancestry-excluded main merge-queue runs; dry-run by default.

For protected main B and immutable run head S, compare B...S must prove B is
not an ancestor of S. With enforced no-bypass deletion/non-fast-forward rules,
future main tips retain B and valid future queue candidates contain B. Thus S
cannot become a current candidate, even when this run ID is rerun on S after
our final read. Observed current refs/queue heads always override that proof.

This is cleanup after base advancement, not immediate ejection cleanup. An
unchanged base, incomplete API evidence or unreadable bypass metadata skips.
The operating assumption is that the verified protections remain enforced
through the action; this does not withstand administrative rule changes that
occur entirely between reads. No age threshold or event payload is authority.

Sources and proof reviewed 2026-10-14:
https://docs.github.com/en/rest/commits/commits#compare-two-commits
https://docs.github.com/en/rest/repos/rules#get-rules-for-a-branch
https://docs.github.com/en/rest/repos/rules#get-a-repository-ruleset
https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/managing-a-merge-queue#how-merge-queues-work
https://docs.github.com/en/actions/reference/workflows-and-actions/variables#default-environment-variables
https://docs.github.com/en/rest/actions/workflow-runs#cancel-a-workflow-run

Production integration must prove actual workflow-token visibility of bypass
metadata in dry-run before enabling apply; local gh visibility is insufficient.
The injectable transport is the sole network/cancellation boundary. 202 means
accepted, not stopped. No retries, force-cancel, or replacement-run targeting.
"""
import argparse
import json
import re
import subprocess
import sys
from urllib.parse import parse_qs, urlsplit

WORKFLOWS = {'intent-hq/intentd': '.github/workflows/ci.yml',
             'intent-hq/cloudlands-fe': '.github/workflows/intent-pr.yml'}
LIVE = ('queued', 'in_progress', 'waiting', 'pending', 'requested')
QUEUE_STATES = {'QUEUED', 'AWAITING_CHECKS', 'MERGEABLE', 'UNMERGEABLE', 'LOCKED'}
REQUIRED_RULES = {'deletion', 'non_fast_forward', 'required_linear_history'}
QUEUE_REF = re.compile(r'gh-readonly-queue/main/pr-[1-9][0-9]*-[0-9a-f]{40}')
SHA = re.compile(r'[0-9a-f]{40}')
QUEUE_QUERY = '''query($owner:String!,$name:String!,$cursor:String){
  repository(owner:$owner,name:$name){databaseId nameWithOwner
    mergeQueue(branch:"main"){entries(first:100,after:$cursor){
      nodes{id state headCommit{oid} pullRequest{baseRefName}}
      pageInfo{hasNextPage endCursor}
    }}
  }
}'''


class SafetyError(ValueError):
    """Incomplete or inconsistent evidence: stop the scan without retrying."""


class Skip(ValueError):
    """Known ineligible run: other independent runs can still be considered."""


class Response:
    def __init__(self, status, data, headers=None):
        self.status, self.data, self.headers = status, data, headers or {}


def require(condition, message):
    if not condition:
        raise SafetyError(message)


def integer(value, minimum=0):
    return type(value) is int and value >= minimum


def sha(value):
    return isinstance(value, str) and SHA.fullmatch(value) is not None


def parse_response(text):
    """Parse gh --include output, rejecting ambiguous/multiple HTTP responses."""
    header, separator, body = text.replace('\r\n', '\n').partition('\n\n')
    lines = header.splitlines()
    require(separator and lines, 'missing HTTP response headers')
    match = re.fullmatch(r'HTTP/[0-9.]+ ([0-9]{3})(?: .*|)', lines[0])
    require(match is not None, 'invalid HTTP status')
    headers = {}
    for line in lines[1:]:
        key, sep, value = line.partition(':')
        require(sep and key.lower() not in headers, 'invalid or duplicate HTTP header')
        headers[key.lower()] = value.strip()
    return Response(int(match[1]), json.loads(body) if body.strip() else None, headers)


class GhApi:
    def request(self, method, path, body=None):
        command = ['gh', 'api', '--hostname', 'github.com', '--include', '--method', method,
                   '-H', 'Accept: application/vnd.github+json',
                   '-H', 'X-GitHub-Api-Version: 2022-11-28', path]
        if body is not None:
            command += ['--input', '-']
        try:
            result = subprocess.run(command, input=None if body is None else json.dumps(body),
                                    text=True, capture_output=True, timeout=30)
            response = parse_response(result.stdout)
        except (OSError, subprocess.TimeoutExpired, ValueError) as error:
            raise SafetyError('GitHub request failed: ' + str(error)) from error
        require(result.returncode == 0 or response.status >= 400, 'gh failed without an HTTP error')
        return response


class Client:
    def __init__(self, api, repository, max_pages, max_requests):
        self.api, self.repository = api, repository
        self.root = '/repos/' + repository
        self.max_pages, self.remaining = max_pages, max_requests

    def request(self, method, path, body=None):
        require(self.remaining > 0, 'request budget exhausted')
        require(path == '/graphql' or path.startswith(self.root + '/') or path == self.root,
                'request outside local repository')
        self.remaining -= 1
        response = self.api.request(method, path, body)
        require(isinstance(response, Response) and integer(response.status, 100), 'invalid API response')
        return response

    def get(self, path):
        response = self.request('GET', path)
        require(response.status == 200, f'GET {path}: HTTP {response.status}')
        return response.data

    def pages(self, path):
        """Follow Link headers with same endpoint/filters and a hard page bound."""
        original = urlsplit(path)
        filters = parse_qs(original.query)
        seen, pages = set(), []
        while path is not None:
            require(len(pages) < self.max_pages and path not in seen, 'incomplete or looping pagination')
            seen.add(path)
            response = self.request('GET', path)
            require(response.status == 200, f'GET {path}: HTTP {response.status}')
            pages.append(response.data)
            links = response.headers.get('link', '')
            next_paths = []
            if links:
                for item in links.split(','):
                    match = re.fullmatch(r'\s*<([^>]+)>;\s*rel="([a-z ]+)"\s*', item)
                    require(match is not None, 'malformed pagination Link')
                    if 'next' not in match[2].split():
                        continue
                    target = urlsplit(match[1])
                    require(target.scheme == 'https' and target.netloc == 'api.github.com'
                            and target.path == original.path and not target.fragment,
                            'pagination escaped endpoint')
                    query = parse_qs(target.query)
                    page = query.pop('page', [])
                    require(len(page) == 1 and page[0].isdigit() and int(page[0]) > 1
                            and query == filters, 'pagination changed filters')
                    next_paths.append(target.path + '?' + target.query)
            require(len(next_paths) <= 1, 'ambiguous next page')
            path = next_paths[0] if next_paths else None
        return pages

    def rows(self, path):
        rows = []
        for page in self.pages(path):
            require(isinstance(page, list), 'expected paginated array')
            rows.extend(page)
        return rows


def context(client):
    repo = client.get(client.root)
    require(repo['full_name'] == client.repository and integer(repo['id'], 1)
            and repo['default_branch'] == 'main', 'unknown repository/default branch')
    path = WORKFLOWS[client.repository]
    workflow = client.get(client.root + '/actions/workflows/' + path.rsplit('/', 1)[1])
    require(workflow['path'] == path and integer(workflow['id'], 1)
            and workflow['state'] == 'active', 'unknown workflow identity')
    return {'id': repo['id'], 'full_name': repo['full_name'], 'default_branch': 'main',
            'workflow_id': workflow['id'], 'path': path}


def discover(client, ctx):
    discovered = {}
    for status in LIVE:
        path = client.root + f'/actions/workflows/{ctx["workflow_id"]}/runs?event=merge_group&status={status}&per_page=100'
        total, runs = None, []
        for page in client.pages(path):
            count = page['total_count']
            require(integer(count) and count <= 1000, 'filtered run search truncated')
            require(total is None or total == count, 'run count changed during pagination')
            total = count
            require(isinstance(page['workflow_runs'], list) and len(page['workflow_runs']) <= 100, 'invalid run page')
            runs.extend(page['workflow_runs'])
        require(len(runs) == total, 'incomplete run pages')
        for run in runs:
            require(integer(run['id'], 1) and run['id'] not in discovered, 'duplicate or invalid run ID')
            discovered[run['id']] = run
        require(len(discovered) <= 1000, 'run discovery bound exceeded')
    return [discovered[key] for key in sorted(discovered)]


def run_identity(run, ctx):
    require(all(integer(run[key], 1) for key in ('id', 'run_attempt', 'check_suite_id', 'workflow_id')),
            'invalid run/attempt/suite identity')
    require(sha(run['head_sha']), 'invalid original run SHA')
    for key in ('repository', 'head_repository'):
        require(run[key]['id'] == ctx['id'] and type(run[key]['id']) is int
                and run[key]['full_name'] == ctx['full_name'], 'foreign run repository')
    if (run['event'] != 'merge_group' or run['workflow_id'] != ctx['workflow_id']
            or run['path'] != ctx['path'] or not isinstance(run['head_branch'], str)
            or not QUEUE_REF.fullmatch(run['head_branch'])):
        raise Skip('out-of-scope workflow/event/queue target')
    return {key: run[key] for key in ('id', 'run_attempt', 'check_suite_id', 'workflow_id',
                                     'path', 'event', 'head_branch', 'head_sha')}


def live_run(run):
    if run['status'] == 'completed':
        raise Skip('completed')
    require(run['status'] in LIVE and run['conclusion'] is None, 'unknown run state')


def protection(client, ctx):
    repo = client.get(client.root)
    require(integer(repo['id'], 1) and all(repo[key] == ctx[key] for key in ('id', 'full_name', 'default_branch')),
            'repository/default changed')
    effective = client.rows(client.root + '/rules/branches/main?per_page=100')
    inventory = client.rows(client.root + '/rulesets?includes_parents=true&per_page=100')
    require(effective and inventory, 'missing effective rules')
    require(all(integer(rule['ruleset_id'], 1) for rule in effective), 'invalid effective ruleset IDs')
    details, ids, witnesses, queues = [], set(), [], set()
    for summary in inventory:
        require(integer(summary['id'], 1) and summary['id'] not in ids, 'invalid ruleset inventory')
        ids.add(summary['id'])
        require(summary['enforcement'] in ('active', 'evaluate', 'disabled')
                and summary['target'] in ('branch', 'tag', 'push'), 'unknown ruleset kind')
        if summary['enforcement'] != 'active' or summary['target'] != 'branch':
            continue
        rule = client.get(client.root + '/rulesets/' + str(summary['id']))
        require(integer(rule['id'], 1) and all(rule[key] == summary[key] for key in ('id', 'target', 'source', 'source_type', 'enforcement')),
                'ruleset identity/enforcement changed')
        require((rule['source_type'], rule['source']) in
                {('Repository', client.repository), ('Organization', client.repository.split('/')[0])},
                'unknown ruleset source')
        require(isinstance(rule.get('bypass_actors'), list), 'bypass metadata unreadable; actual cleanup credential needs ruleset visibility')
        require(isinstance(rule['rules'], list), 'invalid rules')
        kinds = {r['type'] for r in rule['rules']}
        conditions = rule['conditions']['ref_name']
        main_only = (isinstance(conditions['include'], list) and bool(conditions['include'])
                     and set(conditions['include']) <= {'~DEFAULT_BRANCH', 'refs/heads/main'}
                     and conditions['exclude'] == [])
        applicable = {r['type'] for r in effective if r['ruleset_id'] == rule['id']
                      and r['ruleset_source_type'] == rule['source_type'] and r['ruleset_source'] == rule['source']}
        if 'merge_queue' in kinds:
            require(main_only and 'merge_queue' in applicable, 'unknown queue target/configuration')
            queues.add(rule['id'])
        if REQUIRED_RULES <= kinds and REQUIRED_RULES <= applicable and main_only and rule['bypass_actors'] == []:
            witnesses.append(rule['id'])
        details.append({key: rule[key] for key in ('id', 'target', 'source', 'source_type', 'enforcement',
                                                 'conditions', 'rules', 'bypass_actors')})
    require(witnesses and queues, 'no effective unbypassed main protection/queue proof')
    require(all(r['ruleset_id'] in ids for r in effective), 'effective rules missing from inventory')
    fingerprint = json.dumps({'effective': effective, 'details': details}, sort_keys=True)
    return {'rule_ids': sorted(witnesses), 'fingerprint': fingerprint}


def main_tip(client):
    ref = client.get(client.root + '/git/ref/heads/main')
    require(ref['ref'] == 'refs/heads/main' and ref['object']['type'] == 'commit'
            and sha(ref['object']['sha']), 'unknown main tip')
    return ref['object']['sha']


def compare(client, base, head):
    require(sha(base) and sha(head), 'invalid comparison SHAs')
    result = client.get(client.root + '/compare/' + base + '...' + head)
    require(result['base_commit']['sha'] == base and sha(result['merge_base_commit']['sha']), 'wrong comparison identity')
    a, b, total = (result[k] for k in ('ahead_by', 'behind_by', 'total_commits'))
    require(all(integer(v) for v in (a, b, total)), 'invalid comparison counts')
    common, status = result['merge_base_commit']['sha'], result['status']
    valid = ((status == 'behind' and b > 0 and a == total == 0 and common == head and base != head)
             or (status == 'diverged' and b > 0 and a > 0 and total == a and common not in (base, head) and base != head)
             or (status == 'ahead' and a > 0 and b == 0 and total == a and common == base and base != head)
             or (status == 'identical' and a == b == total == 0 and common == base == head))
    require(valid, 'inconsistent comparison metadata')
    return {'status': status, 'ahead_by': a, 'behind_by': b, 'total_commits': total, 'merge_base_sha': common}


def activity(client, ctx, candidate):
    refs = client.rows(client.root + '/git/matching-refs/heads/gh-readonly-queue/')
    require(len(refs) <= 1000, 'queue ref bound exceeded')
    names = set()
    for ref in refs:
        name, obj = ref['ref'], ref['object']
        require(isinstance(name, str) and name.startswith('refs/heads/gh-readonly-queue/'), 'invalid queue ref')
        require(name not in names and obj['type'] == 'commit' and sha(obj['sha']), 'invalid/duplicate queue ref')
        names.add(name)
        if obj['sha'] == candidate:
            raise Skip('current queue ref protects SHA')
        require(name.startswith('refs/heads/gh-readonly-queue/main/'), 'unknown queue target')
    owner, name = client.repository.split('/')
    cursor, cursors, node_ids = None, set(), set()
    for _ in range(client.max_pages):
        response = client.request('POST', '/graphql', {'query': QUEUE_QUERY,
            'variables': {'owner': owner, 'name': name, 'cursor': cursor}})
        require(response.status == 200 and not response.data.get('errors'), 'GraphQL activity unavailable/partial')
        repo = response.data['data']['repository']
        require(integer(repo['databaseId'], 1) and repo['databaseId'] == ctx['id']
                and repo['nameWithOwner'] == client.repository, 'foreign queue response')
        entries = repo['mergeQueue']['entries']
        require(isinstance(entries['nodes'], list) and len(entries['nodes']) <= 100, 'invalid queue nodes')
        for node in entries['nodes']:
            require(isinstance(node['id'], str) and node['id'] not in node_ids, 'invalid/duplicate queue entry')
            node_ids.add(node['id'])
            require(node['state'] in QUEUE_STATES and node['pullRequest']['baseRefName'] == 'main', 'unknown queue state/target')
            if node['headCommit'] is None and node['state'] == 'QUEUED':
                continue  # not yet built: no merge-group commit exists, so no run SHA to protect
            require(node['headCommit'] is not None and sha(node['headCommit']['oid']), 'unknown candidate activity')
            if node['headCommit']['oid'] == candidate:
                raise Skip('current queue head protects SHA')
        page = entries['pageInfo']
        require(type(page['hasNextPage']) is bool, 'invalid queue pagination')
        if not page['hasNextPage']:
            return
        cursor = page['endCursor']
        require(isinstance(cursor, str) and cursor and cursor not in cursors, 'lost/repeated queue cursor')
        cursors.add(cursor)
    raise SafetyError('incomplete queue pagination')


def reconcile(api, repository, *, apply=False, max_pages=10, max_candidates=20, max_requests=1000, after_run_id=0):
    result = {'mode': 'apply' if apply else 'dry-run', 'complete': False, 'decisions': []}
    decision = None
    try:
        require(repository in WORKFLOWS, 'repository not allowlisted')
        require(all(integer(v, 1) for v in (max_pages, max_candidates, max_requests)), 'invalid reconciliation bounds')
        require(integer(after_run_id), 'invalid reconciliation cursor')
        client = Client(api, repository, max_pages, max_requests)
        ctx = context(client)
        runs = [run for run in discover(client, ctx) if run['id'] > after_run_id]
        result['deferred'] = max(0, len(runs) - max_candidates)
        result['next_after_run_id'] = None
        for run in runs[:max_candidates]:
            decision = {'repository': repository, 'repository_id': ctx['id'], 'event': run.get('event'),
                        'id': run['id'], 'run_attempt': run.get('run_attempt'),
                        'check_suite_id': run.get('check_suite_id'), 'workflow_id': run.get('workflow_id'),
                        'path': run.get('path'), 'head_sha': run.get('head_sha'),
                        'head_ref': 'refs/heads/' + str(run.get('head_branch', '')), 'action': 'skip'}
            result['decisions'].append(decision)
            try:
                identity = run_identity(run, ctx)
                live_run(run)
                run_path = client.root + '/actions/runs/' + str(run['id'])
                fresh = client.get(run_path)
                if run_identity(fresh, ctx) != identity:
                    raise Skip('run identity/attempt changed')
                live_run(fresh)
                proof = protection(client, ctx)
                base = main_tip(client)
                evidence = compare(client, base, identity['head_sha'])
                decision.update(base_sha=base, comparison=evidence, rule_ids=proof['rule_ids'], bypass_verified=True)
                if evidence['status'] not in ('behind', 'diverged'):
                    raise Skip('main is contained in candidate; no durable exclusion')
                activity(client, ctx, identity['head_sha'])
                # Fresh protection/main/run/activity reads. The irreversible ancestry
                # witness, not apparent atomicity of these reads, closes the rerun race.
                require(context(client) == ctx, 'repository/workflow changed')
                latest_proof = protection(client, ctx)
                require(latest_proof == proof, 'effective protections changed')
                latest = main_tip(client)
                if latest != base:
                    advancement = compare(client, base, latest)
                    require(advancement['status'] == 'ahead', 'main rewound or has unknown ancestry')
                decision['current_base_sha'] = latest
                fresh = client.get(run_path)
                if run_identity(fresh, ctx) != identity:
                    raise Skip('run identity/attempt changed')
                live_run(fresh)
                activity(client, ctx, identity['head_sha'])
                decision['reason'] = 'protected main contains history absent from this run SHA; no observed active SHA'
                if not apply:
                    decision['action'] = 'would-cancel'
                    continue
                # Only this exact immutable run ID, never the workflow/ref/replacement.
                require(client.remaining > 0, 'request budget exhausted before cancellation')
                decision['action'] = 'cancel-outcome-unknown'
                response = client.request('POST', run_path + '/cancel')
                decision['http_status'] = response.status
                if response.status == 202:
                    decision['action'] = 'cancel-requested'
                elif response.status == 409:
                    decision.update(action='cancel-conflict', reason='run no longer cancellable; no retry')
                else:
                    decision['action'] = 'cancel-error'
                    raise SafetyError(f'cancel HTTP {response.status}; outcome may be unknown; no retry')
            except Skip as error:
                decision['reason'] = str(error)
        if result['deferred']:
            result['next_after_run_id'] = runs[max_candidates - 1]['id']
        result['complete'] = True
    except (SafetyError, KeyError, TypeError, AttributeError) as error:
        result['error'] = str(error)
        if decision is not None:
            decision['reason'] = str(error)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--repository', required=True, choices=sorted(WORKFLOWS))
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--apply', action='store_true', help='Request cancellation after all safety checks')
    mode.add_argument('--dry-run', action='store_true', help='Assess only (default)')
    parser.add_argument('--max-candidates', type=int, default=20)
    parser.add_argument('--after-run-id', type=int, default=0,
                        help='Resume a bounded sweep using next_after_run_id; reset to 0 for the next sweep')
    args = parser.parse_args()
    result = reconcile(GhApi(), args.repository, apply=args.apply, max_candidates=args.max_candidates,
                       after_run_id=args.after_run_id)
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0 if result['complete'] else 2


if __name__ == '__main__':
    sys.exit(main())
