#!/usr/bin/env python3
"""Read-only, cross-workflow native artifact registry.

The reusable workflow holds the repository/source/platform concurrency lock
BEFORE calling restore; that deliberately broader boundary cannot drift with
runner facts or demand sets. Lookup here is the post-lock recheck, never an
optimistic pre-lock result. Only exact validated variants become local hits.
GitHub's immutable finalized artifact, trusted run metadata, publication marker,
and inner archive checksums are all required; a run's test result is irrelevant.

No token/inaccessible trust context is a failure, not a silent cold build. Fork,
pull_request_target, dispatch, and arbitrary workflow producers are excluded.
An expired/deleted artifact is a miss. Network/permission/download corruption
fails the job explicitly. Cancellation retains only fully uploaded artifacts;
queue:max overflow (>100 pending) and runner loss may cancel a request and
require a rerun. These are disposable archives, not permanent exactly-once work.
"""
import datetime
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time
import urllib.parse
import zipfile

_spec = importlib.util.spec_from_file_location('archives', Path(__file__).with_name('ci-test-artifacts.py'))
archives = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(archives)

WORKFLOWS = {'.github/workflows/ci.yml', '.github/workflows/native-reuse-proof.yml'}


class Unavailable(RuntimeError):
    pass


class Deleted(Unavailable):
    pass


class GitHub:
    def __init__(self, repository):
        if repository != 'intent-hq/intentd':
            raise ValueError('reuse repository is not trusted')
        self.repository = repository

    def api(self, suffix, destination=None, *, timeout=120):
        command = ['gh', 'api', f'repos/{self.repository}/{suffix}']
        if destination is None:
            result = subprocess.run(command, capture_output=True, timeout=120)
        else:
            with Path(destination).open('wb') as stream:
                result = subprocess.run(command, stdout=stream, stderr=subprocess.PIPE, timeout=timeout)
        if result.returncode:
            diagnostic = result.stderr.decode(errors='replace')
            if re.search(r'HTTP (404|410)\b', diagnostic):
                raise Deleted('artifact or provenance was deleted/unavailable (HTTP 404/410)')
            raise Unavailable('GitHub artifact lookup/download failed: ' + diagnostic)
        return json.loads(result.stdout) if destination is None else None

    def pages(self, suffix, field):
        page = 1
        while True:
            separator = '&' if '?' in suffix else '?'
            result = self.api(f'{suffix}{separator}per_page=100&page={page}')
            yield from result[field]
            if len(result[field]) < 100:
                return
            page += 1

    def candidates(self, name, head_sha):
        # Enumerate all workflow names for this event head, then exact source
        # namespace/attempt names. Each attempt is immutable: a cancelled retry
        # must never delete the previous attempt's completed publication.
        runs = self.pages('actions/runs?' + urllib.parse.urlencode({'head_sha': head_sha}), 'workflow_runs')
        for run in runs:
            if run.get('path') not in WORKFLOWS:
                continue
            for artifact in self.pages(f'actions/runs/{run["id"]}/artifacts', 'artifacts'):
                if re.fullmatch(re.escape(name) + r'-[1-9][0-9]*', artifact['name']):
                    yield artifact

    def run(self, run_id):
        return self.api(f'actions/runs/{int(run_id)}')

    def download(self, artifact, destination):
        # Metadata stays bounded at two minutes; a full coverage publication
        # can exceed 25 GB. Budget transfer at 32 MiB/s plus setup overhead,
        # capped at 30 minutes so a stalled endpoint still fails the job.
        size = artifact['size_in_bytes']
        if type(size) is not int or size <= 0:
            raise ValueError('invalid GitHub artifact size')
        timeout = min(1800, 120 + (size + 32 * 1024**2 - 1) // (32 * 1024**2))
        try:
            self.api(f'actions/artifacts/{int(artifact["id"])}/zip', destination, timeout=timeout)
        except subprocess.TimeoutExpired as error:
            Path(destination).unlink(missing_ok=True)
            raise Unavailable(f'GitHub artifact download timed out after {timeout}s ({size} bytes)') from error


def artifact_name(platform, source):
    if platform not in ('linux', 'macos', 'windows') or not re.fullmatch('[0-9a-f]{40}', source):
        raise ValueError('invalid native artifact namespace')
    return f'native-v2-{platform}-{source}'


def trusted_run(run, expected):
    """The actual source commit is bound separately, never replaced by head_sha."""
    if run.get('repository', {}).get('full_name') != expected['repository'] or run.get('head_repository', {}).get('full_name') != expected['repository']:
        return False
    if run.get('path') not in WORKFLOWS or run.get('event') != expected['event'] or run.get('head_sha') != expected['head_sha']:
        return False
    if run['event'] == 'pull_request':
        return any(pr.get('number') == expected['pull_request'] and
                   pr.get('head', {}).get('sha') == expected['head_sha'] and
                   pr.get('base', {}).get('sha') == expected['base_sha'] and
                   pr.get('head', {}).get('repo', {}).get('id') == run['repository']['id']
                   for pr in run.get('pull_requests', []))
    return run['event'] == 'merge_group' and run['head_sha'] == expected['source']


def context(source, api):
    actual = archives.source_commit(source)
    if actual != os.environ.get('GITHUB_SHA'):
        raise ValueError('checkout must equal actual GitHub source/merge SHA')
    event = json.loads(Path(os.environ['GITHUB_EVENT_PATH']).read_text())
    expected = {'repository': os.environ['GITHUB_REPOSITORY'], 'source': actual,
                'event': os.environ['GITHUB_EVENT_NAME'], 'head_sha': actual,
                'pull_request': None, 'base_sha': None}
    if expected['event'] == 'pull_request':
        pr = event['pull_request']
        if pr['head']['repo']['full_name'] != expected['repository']:
            raise ValueError('fork artifact context is not trusted')
        expected.update(head_sha=pr['head']['sha'], base_sha=pr['base']['sha'], pull_request=event['number'])
        # A checkout head and GitHub's PR head are not interchangeable. Git can
        # read the commit header even in a depth-one checkout.
        parents = [line[7:] for line in archives.output(['git', 'cat-file', '-p', actual], source).splitlines() if line.startswith('parent ')]
        if parents != [expected['base_sha'], expected['head_sha']]:
            raise ValueError('PR source is not the expected base/head merge commit')
    run = api.run(int(os.environ['GITHUB_RUN_ID']))
    if not trusted_run(run, expected):
        raise ValueError('current workflow is outside the permitted producer trust context')
    return expected


def publication(expected):
    return {'schema': 1, 'context': expected, 'run_id': int(os.environ['GITHUB_RUN_ID']),
            'attempt': int(os.environ['GITHUB_RUN_ATTEMPT']), 'workflow': os.environ['GITHUB_WORKFLOW_REF'].split('@')[0].split('/', 2)[2]}


def extract_download(archive, destination):
    """Reject paths/links/duplicates before extracting any untrusted members."""
    with zipfile.ZipFile(archive) as zipped:
        seen = set()
        for entry in zipped.infolist():
            archives.safe_join(destination, entry.filename.rstrip('/'))
            if entry.filename in seen or (entry.external_attr >> 16) & 0o170000 == 0o120000:
                raise ValueError('duplicate or symlink artifact member')
            seen.add(entry.filename)
        zipped.extractall(destination)


def restore(requests, root, api, expected, platform):
    """Run under the workflow lock; successful downloads only seed exact hits."""
    root = Path(root)
    published = root / 'published'
    published.mkdir(parents=True, exist_ok=True)
    wanted = {archives.identity.build_key(r['identity']): r for r in requests.values()}
    if any(r['identity']['source']['commit'] != expected['source'] for r in wanted.values()):
        raise ValueError('requested source differs from trusted context')
    report = {'lookup': 'after-lock', 'context': expected, 'hits': {}, 'misses': {}, 'rejected': []}
    started = time.monotonic()
    try:
        for candidate in api.candidates(artifact_name(platform, expected['source']), expected['head_sha']):
            if not wanted.keys() - report['hits'].keys():
                break
            origin = {'artifact_id': candidate['id'], 'run_id': candidate['workflow_run']['id']}
            expiry = datetime.datetime.fromisoformat(candidate['expires_at'].replace('Z', '+00:00'))
            if candidate['expired'] or expiry <= datetime.datetime.now(datetime.timezone.utc):
                report['rejected'].append({**origin, 'reason': 'expired; rebuild missing variants'})
                continue
            try:
                run = api.run(origin['run_id'])
            except Deleted:
                report['rejected'].append({**origin, 'reason': 'producer provenance unavailable; not trusted'})
                continue
            if not trusted_run(run, expected):
                report['rejected'].append({**origin, 'reason': 'untrusted producer context'})
                continue
            with tempfile.TemporaryDirectory(prefix='download-', dir=root) as temporary:
                stage = Path(temporary)
                try:
                    api.download(candidate, stage / 'download.zip')
                except Deleted:
                    report['rejected'].append({**origin, 'reason': 'deleted/expired during download; rebuild missing variants'})
                    continue
                digest = candidate.get('digest')
                if not digest or digest != 'sha256:' + archives.file_sha(stage / 'download.zip'):
                    raise ValueError('download digest mismatch or missing GitHub artifact digest')
                extract_download(stage / 'download.zip', stage / 'content')
                content = stage / 'content'
                marker = archives.identity.read_request(content / 'publication.json')
                if (marker.get('schema') != 1 or marker.get('context') != expected or marker.get('run_id') != origin['run_id']
                        or marker.get('workflow') != run['path'] or type(marker.get('attempt')) is not int
                        or not 1 <= marker['attempt'] <= run['run_attempt']):
                    raise ValueError('publication provenance mismatch')
                for key, request in wanted.items():
                    if key in report['hits']:
                        continue
                    bundle = content / 'published' / key
                    if not bundle.exists():
                        continue
                    archives.verify_bundle(bundle, request)
                    destination = published / key
                    if destination.exists():
                        archives.verify_bundle(destination, request)
                    else:
                        shutil.copytree(bundle, destination)
                    report['hits'][key] = {**origin, 'workflow': run['path'], 'producer_conclusion': run.get('conclusion'), 'attempt': marker['attempt']}
        report['misses'] = {key: 'no retained compatible finalized artifact; build required' for key in wanted if key not in report['hits']}
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        report['lookup_download_seconds'] = time.monotonic() - started
        (root / 'reuse.json').write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(report), flush=True)
    return report
