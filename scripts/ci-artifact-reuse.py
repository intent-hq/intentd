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

    def list_artifacts(self, run_id):
        return list(self.pages(f'actions/runs/{int(run_id)}/artifacts', 'artifacts'))

    def run(self, run_id):
        return self.api(f'actions/runs/{int(run_id)}')

    def download(self, artifact, destination):
        # Metadata stays bounded at two minutes; a full coverage publication
        # can exceed 25 GB. Budget transfer at 16 MiB/s plus setup overhead (measured22MB/s),
        # capped at 30 minutes so a stalled endpoint still fails the job.
        size = artifact['size_in_bytes']
        if type(size) is not int or size <= 0:
            raise ValueError('invalid GitHub artifact size')
        timeout = min(1800, 120 + (size + 16 * 1024**2 - 1) // (16 * 1024**2))
        try:
            self.api(f'actions/artifacts/{int(artifact["id"])}/zip', destination, timeout=timeout)
        except subprocess.TimeoutExpired as error:
            Path(destination).unlink(missing_ok=True)
            raise Unavailable(f'GitHub artifact download timed out after {timeout}s ({size} bytes)') from error


def artifact_name(platform, source):
    if platform not in ('linux', 'macos', 'windows') or not re.fullmatch('[0-9a-f]{40}', source):
        raise ValueError('invalid native artifact namespace')
    return f'native-v3-index-{platform}-{source}'


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
        if len(parents) != 2 or parents[1] != expected['head_sha'] or not all(
                re.fullmatch('[0-9a-f]{40}', value) for value in [*parents, expected['base_sha']]):
            raise ValueError('PR source is not the expected base/head merge commit')
        if parents[0] != expected['base_sha']:
            # GitHub can generate GITHUB_SHA against newer main than the base
            # recorded in its synchronize event/run metadata. Keep that event
            # base binding and independently authenticate the immutable tested
            # merge and a forward-only base advance; never substitute a live ref.
            commit = api.api('git/commits/' + actual)
            if commit.get('sha') != actual or [p.get('sha') for p in commit.get('parents', [])] != parents:
                raise ValueError('PR source is not the authenticated base/head merge commit')
            comparison = api.api('compare/' + expected['base_sha'] + '...' + parents[0])
            if comparison.get('status') != 'ahead' or comparison.get('merge_base_commit', {}).get('sha') != expected['base_sha']:
                raise ValueError('PR merge base is not an authenticated advance of the event base')
            expected['actual_base_sha'] = parents[0]
    run = api.run(int(os.environ['GITHUB_RUN_ID']))
    if not trusted_run(run, expected):
        raise ValueError('current workflow is outside the permitted producer trust context')
    return expected


def publication(expected):
    return {'schema': 1, 'context': expected, 'run_id': int(os.environ['GITHUB_RUN_ID']),
            'attempt': int(os.environ['GITHUB_RUN_ATTEMPT']), 'workflow': os.environ['GITHUB_WORKFLOW_REF'].split('@')[0].split('/', 2)[2]}


def extract_download(archive, destination, *, max_bytes):
    """Reject paths/links/duplicates before extracting any untrusted members."""
    with zipfile.ZipFile(archive) as zipped:
        seen = set()
        if sum(entry.file_size for entry in zipped.infolist()) > max_bytes:
            raise ValueError("artifact ZIP expanded size exceeds catalog budget")
        for entry in zipped.infolist():
            archives.safe_join(destination, entry.filename.rstrip('/'))
            if entry.filename in seen or (entry.external_attr >> 16) & 0o170000 == 0o120000:
                raise ValueError('duplicate or symlink artifact member')
            seen.add(entry.filename)
        zipped.extractall(destination)


def variant_name(platform, source, key, attempt):
    if not re.fullmatch(r'intentd-build-v1-[0-9a-f]{64}', key):
        raise ValueError('invalid variant key')
    return artifact_name(platform, source).replace('-index-', '-variant-') + '-' + key.removeprefix('intentd-build-v1-') + '-' + str(attempt)


def publication_plan(root, platform):
    """Index first: finalized variants survive an interrupted later upload."""
    root = Path(root).resolve()
    marker = archives.identity.read_request(root / 'publication.json')
    requests = archives.identity.read_request(root / 'requests.json')
    index = root / 'publication-index'
    index.mkdir(exist_ok=True)
    for name in ('publication.json', 'requests.json', 'reuse.json', 'compiler-gates.json', 'producer.json', 'compiler-tripwire.exe'):
        if (root / name).is_file():
            shutil.copyfile(root / name, index / name)
    variants, uploads = {}, []
    for request in requests.values():
        archives.validate_request(request)
        key = archives.identity.build_key(request['identity'])
        if key in variants:
            continue
        bundle = root / 'published' / key
        if not bundle.is_dir():
            continue  # A failed build still publishes earlier completed variants.
        manifest = archives.verify_bundle(bundle, request)
        name = variant_name(platform, marker['context']['source'], key, marker['attempt'])
        variants[key] = {'name': name, 'manifest_sha256': archives.file_sha(bundle / 'manifest.json'),
                         'archive_bytes': manifest['archive_bytes'] + manifest.get('report_archive', {}).get('bytes', 0),
                         'expanded_bytes': sum(item['size'] for item in manifest['files'].values())}
        files = [root / 'publication.json', *sorted(p for p in bundle.iterdir() if p.is_file())]
        uploads.append({'name': name, 'root': str(root), 'files': list(map(str, files))})
    (index / 'catalog.json').write_text(json.dumps({'schema': 3, 'variants': variants}, indent=2) + '\n')
    plan = [{'name': artifact_name(platform, marker['context']['source']) + '-' + str(marker['attempt']),
             'root': str(index), 'files': list(map(str, sorted(index.iterdir())))}, *uploads]
    (root / 'publication-plan.json').write_text(json.dumps(plan, indent=2) + '\n')
    return plan


def live(artifact):
    expiry = datetime.datetime.fromisoformat(artifact['expires_at'].replace('Z', '+00:00'))
    return not artifact['expired'] and expiry > datetime.datetime.now(datetime.timezone.utc)


def download_verified(api, artifact, destination, *, max_bytes=32 * 1024**2):
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    zipped = destination / 'download.zip'
    started = time.monotonic()
    api.download(artifact, zipped)
    downloaded = time.monotonic()
    if artifact.get('digest') != 'sha256:' + archives.file_sha(zipped):
        raise ValueError('download digest mismatch or missing GitHub artifact digest')
    checked = time.monotonic()
    extract_download(zipped, destination / 'content', max_bytes=max_bytes)
    zipped.unlink()
    return {'artifact_id': artifact['id'], 'bytes': artifact['size_in_bytes'],
            'download_seconds': downloaded-started, 'digest_seconds': checked-downloaded,
            'zip_extract_seconds': time.monotonic()-checked}


def validate_marker(content, candidate, run, expected, platform):
    marker = archives.identity.read_request(content / 'publication.json')
    if (marker.get('schema') != 1 or marker.get('context') != expected or marker.get('run_id') != run['id']
            or marker.get('workflow') != run['path'] or type(marker.get('attempt')) is not int
            or not 1 <= marker['attempt'] <= run['run_attempt']
            or candidate['name'] != artifact_name(platform, expected['source']) + '-' + str(marker['attempt'])):
        raise ValueError('publication provenance mismatch')
    catalog = archives.identity.read_request(content / 'catalog.json')
    if catalog.get('schema') != 3 or not isinstance(catalog.get('variants'), dict):
        raise ValueError('unsupported selective publication catalog')
    for key, entry in catalog['variants'].items():
        if (entry.get('name') != variant_name(platform, expected['source'], key, marker['attempt'])
                or not re.fullmatch('[0-9a-f]{64}', entry.get('manifest_sha256', ''))
                or any(type(entry.get(field)) is not int or entry[field] < 0 for field in ('archive_bytes', 'expanded_bytes'))):
            raise ValueError('invalid selective variant catalog entry')
    return marker, catalog['variants']


def restore_variants(requests, root, api, expected, platform, candidate, run, content, *, require=False):
    marker, catalog = validate_marker(content, candidate, run, expected, platform)
    available = api.list_artifacts(run['id'])
    hits, transfers = {}, []
    for key, request in requests.items():
        entry = catalog.get(key)
        matches = [item for item in available if entry and item['name'] == entry['name'] and live(item)]
        if not matches:
            if require:
                raise ValueError('required variant not finalized; no consumer build fallback: ' + key)
            continue
        if len(matches) != 1 or matches[0]['workflow_run']['id'] != run['id']:
            raise ValueError('ambiguous or cross-run variant publication')
        with tempfile.TemporaryDirectory(prefix='variant-download-', dir=root) as tmp:
            stage = Path(tmp)
            try:
                transfer = download_verified(api, matches[0], stage, max_bytes=entry['archive_bytes'] + 32 * 1024**2)
            except Deleted:
                if require:
                    raise
                continue
            bundle = stage / 'content/published' / key
            if (archives.identity.read_request(stage / 'content/publication.json') != marker
                    or archives.file_sha(bundle / 'manifest.json') != entry['manifest_sha256']):
                raise ValueError('variant provenance or manifest digest mismatch')
            manifest = archives.verify_bundle(bundle, request)
            if (entry['archive_bytes'] != manifest['archive_bytes'] + manifest.get('report_archive', {}).get('bytes', 0)
                    or entry['expanded_bytes'] != sum(item['size'] for item in manifest['files'].values())):
                raise ValueError('catalog disk budget differs from verified manifest')
            destination = root / 'published' / key
            destination.parent.mkdir(exist_ok=True)
            if destination.exists():
                archives.verify_bundle(destination, request)
            else:
                shutil.move(str(bundle), destination)  # Same filesystem: no third archive copy.
            hits[key] = {'artifact_id': matches[0]['id'], 'run_id': run['id'], 'workflow': run['path'],
                         'producer_conclusion': run.get('conclusion'), 'attempt': marker['attempt']}
            transfers.append(transfer)
    return hits, transfers


def restore(requests, root, api, expected, platform):
    """Post-lock index lookup, then only missing exact variant downloads."""
    root = Path(root)
    (root / 'published').mkdir(parents=True, exist_ok=True)
    wanted = {archives.identity.build_key(r['identity']): r for r in requests.values()}
    if any(r['identity']['source']['commit'] != expected['source'] for r in wanted.values()):
        raise ValueError('requested source differs from trusted context')
    report = {'lookup': 'after-lock', 'context': expected, 'hits': {}, 'misses': {}, 'rejected': [], 'transfers': []}
    started = time.monotonic()
    try:
        for candidate in api.candidates(artifact_name(platform, expected['source']), expected['head_sha']):
            missing = {key: request for key, request in wanted.items() if key not in report['hits']}
            if not missing:
                break
            origin = {'artifact_id': candidate['id'], 'run_id': candidate['workflow_run']['id']}
            if not live(candidate):
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
            if candidate['size_in_bytes'] > 32 * 1024**2:
                raise ValueError('publication index exceeds metadata size budget')
            with tempfile.TemporaryDirectory(prefix='index-download-', dir=root) as temporary:
                stage = Path(temporary)
                try:
                    report['transfers'].append(download_verified(api, candidate, stage))
                except Deleted:
                    report['rejected'].append({**origin, 'reason': 'deleted during download; rebuild missing variants'})
                    continue
                hits, transfers = restore_variants(missing, root, api, expected, platform, candidate, run, stage / 'content')
                report['hits'].update(hits)
                report['transfers'].extend(transfers)
        report['misses'] = {key: 'no retained compatible finalized artifact; build required' for key in wanted if key not in report['hits']}
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        report['lookup_download_seconds'] = time.monotonic() - started
        (root / 'reuse.json').write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(report), flush=True)
    return report


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--publication-plan', type=Path, required=True)
    parser.add_argument('--platform', choices=('linux', 'macos', 'windows'), required=True)
    args = parser.parse_args()
    publication_plan(args.publication_plan, args.platform)
