#!/usr/bin/env python3
"""Storage, trust and coordinated-miss regressions for native workflow artifacts."""
import copy
import concurrent.futures
import importlib.util
import json
import os
import shutil
import subprocess
import tempfile
import threading
import zipfile
from unittest.mock import patch
import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'scripts' / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class CoordinationRegression(unittest.TestCase):
    def test_native_producer_queues_overlapping_requests_without_cancellation(self):
        import yaml
        workflow = yaml.safe_load((ROOT / '.github/workflows/native-artifacts.yml').read_text())
        lock = workflow['jobs']['native']['concurrency']
        self.assertEqual(lock['queue'], 'max')
        self.assertIs(lock['cancel-in-progress'], False)
        for unstable in ('workflow', 'run_id', 'run_attempt', 'runner.', 'event_name', 'scope', 'variants'):
            self.assertNotIn(unstable, lock['group'])
        self.assertIn('github.sha', lock['group'])
        self.assertIn('inputs.platform', lock['group'])

    def test_callers_do_not_hold_the_producer_lock_or_cancel_pending_consumers(self):
        import yaml
        for name in ('ci.yml', 'native-reuse-proof.yml'):
            workflow = yaml.safe_load((ROOT / '.github/workflows' / name).read_text())
            lock = workflow['concurrency']
            self.assertEqual(lock['queue'], 'max')
            self.assertIs(lock['cancel-in-progress'], False)
            self.assertNotIn('native-producer-', lock['group'])
            for job in workflow['jobs'].values():
                if job.get('uses') == './.github/workflows/native-artifacts.yml':
                    self.assertNotIn('concurrency', job)
                    self.assertEqual(job.get('permissions', workflow['permissions'])['actions'], 'read')

    def test_publication_precedes_required_lints_and_all_runtime_proofs(self):
        import yaml
        job = yaml.safe_load((ROOT / '.github/workflows/native-artifacts.yml').read_text())['jobs']['native']
        self.assertEqual(job['timeout-minutes'], 60)
        steps = job['steps']
        names = [step.get('name', '') for step in steps]
        self.assertLess(names.index('Produce native archives in one shared target'), names.index('Publish native variants'))
        self.assertLess(names.index('Publish native variants'), names.index('Production check and Clippy in shared target'))
        self.assertLess(names.index('Production check and Clippy in shared target'), names.index('Restore native executables with compiler tripwires'))
        self.assertEqual(steps[names.index('Publish native variants')]['if'], 'always()')
        upload = steps[names.index('Publish native variants')]['with']
        self.assertEqual(steps[names.index('Publish native variants')]['uses'], './.github/actions/publish-variants')
        self.assertIn('inputs.platform', upload['platform'])

    def test_api_discovery_searches_all_workflows_and_attempts_for_event_head(self):
        api = reuse.GitHub('intent-hq/intentd')
        name = reuse.artifact_name('linux', 'c'*40)
        calls = []
        def response(path):
            calls.append(path)
            if path.startswith('actions/runs?'):
                return {'workflow_runs': [{'id': 1, 'path': '.github/workflows/ci.yml'},
                                         {'id': 2, 'path': '.github/workflows/native-reuse-proof.yml'},
                                         {'id': 3, 'path': '.github/workflows/untrusted.yml'}]}
            run_id = int(path.split('/')[2])
            return {'artifacts': [{'id': run_id, 'name': name + '-1'}, {'id': run_id + 10, 'name': name + '-2'},
                                  {'id': 99, 'name': name + '-0'}, {'id': 98, 'name': name + '-bad'}]}
        with patch.object(api, 'api', side_effect=response):
            self.assertEqual([a['id'] for a in api.candidates(name, 'a'*40)], [1, 11, 2, 12])
        self.assertIn('head_sha=' + 'a'*40, calls[0])
        self.assertFalse(any('/3/artifacts' in call for call in calls))

    def test_large_publication_download_has_a_bounded_size_aware_budget(self):
        api = reuse.GitHub('intent-hq/intentd')
        observed = []
        def transfer(command, **kwargs):
            observed.append(kwargs['timeout'])
            if kwargs['timeout'] < 180:
                raise subprocess.TimeoutExpired(command, kwargs['timeout'])
            kwargs['stdout'].write(b'verified separately')
            return subprocess.CompletedProcess(command, 0, stderr=b'')
        with tempfile.TemporaryDirectory() as directory, patch.object(reuse.subprocess, 'run', side_effect=transfer):
            path = Path(directory) / 'publication.zip'
            api.download({'id': 42, 'size_in_bytes': 26_336_283_909}, path)
            self.assertEqual(path.read_bytes(), b'verified separately')
        self.assertGreaterEqual(observed[0], 180)
        self.assertLessEqual(observed[0], 1800)

    def test_download_timeout_removes_partial_archive_and_fails_explicitly(self):
        api = reuse.GitHub('intent-hq/intentd')
        def timeout(command, **kwargs):
            kwargs['stdout'].write(b'partial')
            raise subprocess.TimeoutExpired(command, kwargs['timeout'])
        with tempfile.TemporaryDirectory() as directory, patch.object(reuse.subprocess, 'run', side_effect=timeout):
            path = Path(directory) / 'publication.zip'
            with self.assertRaisesRegex(reuse.Unavailable, 'timed out'):
                api.download({'id': 42, 'size_in_bytes': 26_336_283_909}, path)
            self.assertFalse(path.exists())

    def test_metadata_keeps_short_budget_and_download_budget_is_capped(self):
        api = reuse.GitHub('intent-hq/intentd')
        def response(command, **kwargs):
            return subprocess.CompletedProcess(command, 0, stdout=b'{}', stderr=b'')
        with tempfile.TemporaryDirectory() as directory, patch.object(reuse.subprocess, 'run', side_effect=response) as run:
            self.assertEqual(api.api('actions/runs/1'), {})
            self.assertEqual(run.call_args.kwargs['timeout'], 120)
            api.download({'id': 42, 'size_in_bytes': 10**15}, Path(directory) / 'large.zip')
            self.assertEqual(run.call_args.kwargs['timeout'], 1800)

    def test_actual_pr_merge_source_is_distinct_from_event_head(self):
        expected = {'repository': 'intent-hq/intentd', 'source': 'c'*40, 'head_sha': 'a'*40,
                    'base_sha': 'b'*40, 'pull_request': 42, 'event': 'pull_request'}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = Store(root, expected)
            bundle = root / 'bundle'; bundle.mkdir()
            store.publish(bundle, 1)
            event = root / 'event.json'
            event.write_text(json.dumps({'number': 42, 'pull_request': {'head': {'sha': 'a'*40, 'repo': {'full_name': 'intent-hq/intentd'}}, 'base': {'sha': 'b'*40}}}))
            env = dict(GITHUB_EVENT_PATH=str(event), GITHUB_SHA='c'*40, GITHUB_EVENT_NAME='pull_request', GITHUB_REPOSITORY='intent-hq/intentd', GITHUB_RUN_ID='1')
            with patch.dict(os.environ, env), patch.object(archives, 'source_commit', return_value='c'*40), \
                 patch.object(archives, 'output', return_value='parent ' + 'b'*40 + '\nparent ' + 'a'*40):
                self.assertEqual(reuse.context(root, store), expected)
            with patch.dict(os.environ, env), patch.object(archives, 'source_commit', return_value='a'*40):
                with self.assertRaisesRegex(ValueError, 'source/merge SHA'):
                    reuse.context(root, store)
            with patch.dict(os.environ, env), patch.object(archives, 'source_commit', return_value='c'*40), \
                 patch.object(archives, 'output', return_value='parent ' + 'd'*40 + '\nparent ' + 'a'*40):
                with self.assertRaisesRegex(ValueError, 'base/head merge'):
                    reuse.context(root, store)



reuse = load('ci-artifact-reuse')
archives = reuse.archives


class Store:
    """Finalized Actions API model; uploads become visible atomically.

    The test scheduler below supplies the Actions job concurrency boundary.
    Compilation, staging, archive validation, downloads and consumers are real.
    """
    repository = 'intent-hq/intentd'

    def __init__(self, root, expected):
        self.root, self.expected = root, expected
        self.artifacts, self.runs = [], {}
        self.download_error = None
        self.downloads = []

    def candidates(self, name, head_sha):
        return [a for a in reversed(self.artifacts) if a['name'] == name + '-1']

    def run(self, run_id):
        return self.runs[run_id]

    def download(self, artifact, destination):
        if self.download_error:
            raise self.download_error
        self.downloads.append(artifact['id'])
        shutil.copyfile(self.root / (str(artifact['id']) + '.zip'), destination)

    def list_artifacts(self, run_id):
        return [a for a in self.artifacts if a['workflow_run']['id'] == run_id]

    def publish(self, root, run_id, *, workflow='.github/workflows/ci.yml', conclusion='success'):
        expected = self.expected
        self.runs[run_id] = {'id': run_id, 'run_attempt': 1, 'repository': {'full_name': self.repository, 'id': 42},
                            'head_repository': {'full_name': self.repository}, 'path': workflow,
                            'head_sha': expected['head_sha'], 'event': expected['event'], 'conclusion': conclusion,
                            'pull_requests': [{'number': expected['pull_request'],
                                               'head': {'sha': expected['head_sha'], 'repo': {'id': 42}},
                                               'base': {'sha': expected['base_sha']}}]}
        marker = {'schema': 1, 'context': expected, 'run_id': run_id, 'attempt': 1, 'workflow': workflow}
        (root / 'publication.json').write_text(json.dumps(marker))
        requests = {}
        for path in (root / 'published').glob('*/manifest.json'):
            manifest = json.loads(path.read_text())
            requests[path.parent.name] = {'identity': manifest['identity'], 'recipe': manifest['recipe']}
        if not (root / 'requests.json').exists():
            (root / 'requests.json').write_text(json.dumps(requests))
        plan = reuse.publication_plan(root, 'linux')
        index = None
        for number, upload in enumerate(plan):
            artifact_id = run_id if number == 0 else run_id*1000 + number
            path = self.root / (str(artifact_id) + '.zip')
            with zipfile.ZipFile(path, 'w') as archive:
                for name in upload['files']:
                    archive.write(name, Path(name).relative_to(upload['root']))
            artifact = {'id': artifact_id, 'name': upload['name'],
                        'expired': False, 'expires_at': '2099-01-01T00:00:00Z', 'workflow_run': {'id': run_id},
                        'digest': 'sha256:' + archives.file_sha(path), 'size_in_bytes': path.stat().st_size}
            self.artifacts.append(artifact)
            if index is None:
                index = artifact
        return index


def make_fixture(root):
    source = root / 'source'; source.mkdir()
    (source / 'src').mkdir()
    (source / 'Cargo.toml').write_text('[package]\nname="reuse-fixture"\nversion="0.1.0"\nedition="2021"\n[workspace]\n[features]\na=[]\nb=[]\nc=[]\n')
    (source / 'src/lib.rs').write_text('''#[test] fn runtime_proof() {
        let path = std::env::var("REUSE_TEST_LOG").unwrap();
        use std::io::Write;
        writeln!(std::fs::OpenOptions::new().create(true).append(true).open(path).unwrap(), "ran").unwrap();
        assert!(std::env::var("REUSE_FAIL").is_err());
    }
''')
    subprocess.run(['cargo', 'generate-lockfile', '--offline'], cwd=source, check=True, capture_output=True)
    for command in (['init', '-q'], ['add', '.'], ['-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'fixture']):
        subprocess.run(['git', *command], cwd=source, check=True, capture_output=True)
    host = next(line[6:] for line in archives.output(['rustc', '-vV']).splitlines() if line.startswith('host: '))
    platform = dict(host=host, target=host, os='linux', arch='x86_64', abi='fixture', sdk='none', image='fixture')
    requests = {}
    for variant in ('a', 'b', 'c'):
        recipe = {'kind': 'test-archive', 'companions': [], 'command': ['cargo', 'nextest', 'archive', '--locked', '--offline',
                  '--lib', '--features', variant, '--target-dir', '{target}', '--archive-file', '{archive}', '--cargo-message-format', 'json']}
        requests[variant] = archives.capture_request(source, platform, recipe, config_sha256='0'*64)
    source_sha = archives.source_commit(source)
    expected = {'repository': 'intent-hq/intentd', 'source': source_sha, 'head_sha': 'a'*40,
                'base_sha': 'b'*40, 'pull_request': 42, 'event': 'pull_request'}
    return source, requests, expected


class FunctionalReuse(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temporary.name)
        cls.source, cls.requests, cls.expected = make_fixture(cls.root)
        cls.built = cls.root / 'built'; cls.built.mkdir()
        archives.produce(list(cls.requests.values()), cls.source, cls.root / 'target', cls.built / 'published')

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def setUp(self):
        self.case = tempfile.TemporaryDirectory(dir=self.root)
        self.addCleanup(self.case.cleanup)
        self.directory = Path(self.case.name)
        self.store = Store(self.directory, self.expected)
        self.store.publish(self.built, 1)

    def restore(self, requests=None):
        return reuse.restore(requests or self.requests, self.directory / 'restore', self.store, self.expected, 'linux')

    def test_real_consumer_fetches_only_its_scope_and_checks_actual_disk(self):
        linux = load('ci-linux-artifacts')
        selected_root = self.directory / 'scoped'; shutil.copytree(self.built, selected_root)
        names = {'coverage-e2e': self.requests['a'], 'coverage-all': self.requests['b'],
                 'changed-0': self.requests['c'], 'callback': self.requests['a']}
        (selected_root / 'requests.json').write_text(json.dumps(names))
        self.store.artifacts.clear(); self.store.publish(selected_root, 1)
        env = dict(GITHUB_REPOSITORY='intent-hq/intentd', GITHUB_RUN_ID='1', GITHUB_RUN_ATTEMPT='1',
                   GITHUB_WORKFLOW_REF='intent-hq/intentd/.github/workflows/ci.yml@refs/pull/42/merge')
        with patch.dict(os.environ, env), patch.object(linux.registry, 'GitHub', return_value=self.store), \
             patch.object(linux.registry, 'context', return_value=self.expected):
            for scope, count in [('coverage-e2e', 1), ('coverage-all', 1), ('changed', 2)]:
                root = self.directory / scope
                self.store.downloads.clear()
                linux.fetch(self.source, root, scope)
                receipt = json.loads((root / 'download.json').read_text())
                self.assertTrue(receipt['complete'])
                self.assertEqual(len(receipt['variant_artifacts']), count)
                self.assertEqual(len(self.store.downloads), count + 1)
                self.assertEqual(len(list((root / 'published').iterdir())), count)
                self.assertGreater(receipt['required_free_bytes'], 8 * 1024**3)
                self.assertEqual(len(json.loads((root / 'requests.json').read_text())), count)
            from types import SimpleNamespace
            self.store.downloads.clear()
            root = self.directory / 'out-of-space'
            with patch.object(linux.shutil, 'disk_usage', return_value=SimpleNamespace(free=0)):
                with self.assertRaisesRegex(ValueError, 'free bytes'):
                    linux.fetch(self.source, root, 'coverage-all')
            self.assertEqual(len(self.store.downloads), 1)  # never download an 8GB bundle before failing
            self.assertFalse(json.loads((root / 'download.json').read_text())['complete'])
            self.store.artifacts = self.store.artifacts[:1]  # only index finalized
            with self.assertRaisesRegex(ValueError, 'no consumer build fallback'):
                linux.fetch(self.source, self.directory / 'incomplete', 'coverage-all')

    def test_narrow_restore_never_downloads_unrequested_variants(self):
        report = self.restore({'a': self.requests['a']})
        self.assertEqual(len(report['hits']), 1)
        self.assertEqual(len(self.store.downloads), 2)  # small index + exact bundle
        self.assertEqual(self.store.downloads[0], 1)
        key = archives.identity.build_key(self.requests['a']['identity'])
        self.assertEqual(self.store.downloads[1], report['hits'][key]['artifact_id'])
        self.assertEqual(len(report['transfers']), 2)

    def test_partial_upload_keeps_finalized_variants_and_rebuilds_only_missing(self):
        self.store.artifacts.pop()  # index already visible; last upload interrupted
        report = self.restore()
        self.assertEqual(len(report['hits']), 2)
        self.assertEqual(len(report['misses']), 1)
        self.assertEqual(len(self.store.downloads), 3)

    def test_legacy_platform_zip_is_not_a_selective_index(self):
        self.store.artifacts[0]['name'] = self.store.artifacts[0]['name'].replace('native-v3-index-', 'native-v2-')
        self.assertEqual(len(self.restore()['misses']), 3)
        self.assertEqual(self.store.downloads, [])

    def test_variant_from_another_run_cannot_satisfy_index(self):
        self.store.artifacts[1]['workflow_run']['id'] = 999
        report = self.restore()
        self.assertEqual(len(report['hits']), 2)
        self.assertEqual(len(report['misses']), 1)

    def test_catalog_digest_binds_the_exact_inner_manifest(self):
        candidate = self.store.artifacts[0]
        path = self.directory / '1.zip'
        with zipfile.ZipFile(path) as source:
            entries = {name: source.read(name) for name in source.namelist()}
        catalog = json.loads(entries['catalog.json'])
        next(iter(catalog['variants'].values()))['manifest_sha256'] = '0'*64
        entries['catalog.json'] = json.dumps(catalog).encode()
        with zipfile.ZipFile(path, 'w') as archive:
            for name, value in entries.items():
                archive.writestr(name, value)
        candidate['digest'] = 'sha256:' + archives.file_sha(path)
        with self.assertRaisesRegex(ValueError, 'manifest digest'):
            self.restore()

    def test_reuses_across_workflow_names_and_failed_or_cancelled_runs(self):
        for conclusion in ('success', 'failure', 'cancelled', None):
            with self.subTest(conclusion=conclusion):
                self.store.runs[1].update(path='.github/workflows/native-reuse-proof.yml', conclusion=conclusion)
                self.store.artifacts.clear()
                self.store.publish(self.built, 1, workflow='.github/workflows/native-reuse-proof.yml', conclusion=conclusion)
                report = self.restore()
                self.assertEqual(len(report['hits']), 3)
                self.assertFalse(report['misses'])
                self.assertTrue(all(v['producer_conclusion'] == conclusion for v in report['hits'].values()))

    def test_exact_variant_identity_preserves_every_build_dimension(self):
        original = self.requests['a']
        for field, value in [('source.commit', 'c'*40), ('tools.rustc', 'other rustc'), ('platform.sdk', 'other sdk'),
                             ('platform.abi', 'other ABI'), ('environment.RUSTFLAGS', '--cfg=other'),
                             ('build.cargo_config_sha256', 'e'*64), ('build.resolved_features_sha256', 'f'*64)]:
            with self.subTest(field=field):
                request = copy.deepcopy(original)
                section, name = field.split('.')
                request['identity'][section][name] = value
                if field == 'source.commit':
                    request['identity']['source']['embedded_commit'] = value
                    with self.assertRaisesRegex(ValueError, 'source differs'):
                        self.restore({'a': request})
                else:
                    report = self.restore({'a': request})
                    self.assertFalse(report['hits'])
                    self.assertEqual(len(report['misses']), 1)
        # A available is not a superset of B, despite sharing the Cargo target.
        key = archives.identity.build_key(self.requests['b']['identity'])
        only_a = self.directory / 'only-a'; only_a.mkdir()
        shutil.copytree(self.built / 'published' / archives.identity.build_key(original['identity']), only_a / 'published' / archives.identity.build_key(original['identity']))
        self.store.artifacts.clear(); self.store.publish(only_a, 2)
        self.assertIn(key, self.restore({'b': self.requests['b']})['misses'])

    def test_expired_deleted_untrusted_and_unpublished_variants_are_misses(self):
        candidate = self.store.artifacts[0]
        candidate['expired'] = True
        self.assertEqual(len(self.restore()['misses']), 3)
        candidate['expired'] = False
        self.store.download_error = reuse.Deleted('410')
        self.assertEqual(len(self.restore()['misses']), 3)
        self.store.download_error = None
        self.store.runs[1]['head_repository']['full_name'] = 'outside/intentd'
        self.assertEqual(len(self.restore()['misses']), 3)
        self.store.runs[1]['head_repository']['full_name'] = 'intent-hq/intentd'
        self.store.runs[1]['path'] = '.github/workflows/other.yml'
        self.assertEqual(len(self.restore()['misses']), 3)
        self.store.artifacts.clear()  # cancelled before final upload
        self.assertEqual(len(self.restore()['misses']), 3)

    def test_corruption_and_transient_download_fail_instead_of_compiling(self):
        self.store.download_error = reuse.Unavailable('network failed')
        with self.assertRaisesRegex(reuse.Unavailable, 'network'):
            self.restore()
        self.store.download_error = None
        path = self.directory / '1.zip'
        path.write_bytes(b'corrupt')
        with self.assertRaisesRegex(ValueError, 'digest'):
            self.restore()
        self.assertIn('error', json.loads((self.directory / 'restore/reuse.json').read_text()))

    def test_manifest_provenance_and_inner_digest_are_required(self):
        candidate = self.store.artifacts[0]
        path = self.directory / '1.zip'
        for kind in ('marker', 'archive'):
            self.store.artifacts.clear(); self.store.publish(self.built, 1)
            candidate = self.store.artifacts[0 if kind == 'marker' else 1]
            path = self.directory / (str(candidate['id']) + '.zip')
            with zipfile.ZipFile(path) as source:
                entries = {name: source.read(name) for name in source.namelist()}
            if kind == 'marker':
                marker = json.loads(entries['publication.json']); marker['context']['source'] = 'd'*40
                entries['publication.json'] = json.dumps(marker).encode()
            else:
                key = next(name for name in entries if name.endswith('tests.tar.zst'))
                entries[key] = b'bad inner archive'
            with zipfile.ZipFile(path, 'w') as archive:
                for name, value in entries.items():
                    archive.writestr(name, value)
            candidate['digest'] = 'sha256:' + archives.file_sha(path)
            with self.assertRaises(ValueError):
                self.restore()

    def test_completed_variant_survives_later_compile_failure_and_can_be_reused(self):
        root = self.directory / 'partial'; root.mkdir()
        invalid = copy.deepcopy(self.requests['b'])
        invalid['recipe']['command'].append('--not-a-nextest-option')
        invalid['identity']['build']['commands'] = [invalid['recipe']['command']]
        invalid['identity']['build']['recipe_sha256'] = archives.recipe_digest(invalid['recipe'])
        with self.assertRaises(subprocess.CalledProcessError):
            archives.produce([self.requests['a'], invalid], self.source, self.directory / 'partial-target', root / 'published')
        self.store.artifacts.clear()
        self.store.publish(root, 2, conclusion='failure')
        report = self.restore()
        self.assertEqual(len(report['hits']), 1)
        self.assertEqual(len(report['misses']), 2)
        self.assertIn(archives.identity.build_key(self.requests['a']['identity']), report['hits'])

    def concurrent_requests(self, selections):
        self.store.artifacts.clear()
        barrier, lock = threading.Barrier(len(selections)), threading.Lock()
        def job(index, names):
            root = self.directory / ('job-' + str(index)); root.mkdir()
            requests = {name: self.requests[name] for name in names}
            barrier.wait(timeout=30)  # Both callers observe a cold initial registry.
            # Actions owns this lock in production. The actual YAML lock contract
            # is checked separately; no result from outside it is used here.
            with lock:
                report = reuse.restore(requests, root, self.store, self.expected, 'linux')
                records = archives.produce(list(requests.values()), self.source, self.directory / 'shared-target', root / 'published')
                self.store.publish(root, index + 10, workflow='.github/workflows/ci.yml' if index == 0 else '.github/workflows/native-reuse-proof.yml', conclusion='failure')
                for name, request in requests.items():
                    key = archives.identity.build_key(request['identity'])
                    env = dict(os.environ, RUSTC='/compiler-forbidden', REUSE_TEST_LOG=str(self.directory / 'test-runs'), REUSE_FAIL='1')
                    prepared = archives.prepare(root / 'published' / key, request, self.source, root / 'restore' / name, env=env)
                    # Successful build publication survives failing runtime tests;
                    # every consumer must still execute and fail its own proof.
                    self.assertNotEqual(archives.run_libtest(prepared, 'reuse-fixture', [], env=env), 0)
                return report, records
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(selections)) as pool:
            futures = [pool.submit(job, index, names) for index, names in enumerate(selections)]
            results = [future.result(timeout=90) for future in futures]
        built = [record['key'] for _, records in results for record in records if not record['reused']]
        expected = {archives.identity.build_key(self.requests[name]['identity']) for names in selections for name in names}
        self.assertEqual(len(built), len(expected))
        self.assertEqual(set(built), expected)
        self.assertEqual(len((self.directory / 'test-runs').read_text().splitlines()), sum(map(len, selections)))
        self.assertEqual(sum(len(report['hits']) for report, _ in results), sum(map(len, selections)) - len(expected))

    def test_simultaneous_identical_callers_build_once_and_rerun_failed_tests(self):
        self.concurrent_requests([['a', 'b'], ['a', 'b'], ['a', 'b']])

    def test_simultaneous_overlapping_sets_build_common_variant_once(self):
        self.concurrent_requests([['a', 'b'], ['b', 'c']])


if __name__ == '__main__':
    unittest.main()
