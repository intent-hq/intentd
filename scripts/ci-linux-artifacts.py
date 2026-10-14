#!/usr/bin/env python3
"""One Linux producer for ordinary tests and distinct LLVM coverage graphs.

The caller holds the source/platform lock used by native-artifacts.yml. Build
all requested variants sequentially, stage each immediately, and publish before
tests/lints. Only executable inputs are reusable; every consumer starts fresh.
"""
import argparse
import fnmatch
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import shutil
import sys
import tempfile
import tarfile
import time


def load(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


native = load('ci-native-artifacts')
archives = native.archives
registry = load('ci-artifact-reuse')
coverage = load('ci-coverage-artifacts')
callback = load('callback-build-scope')
storage = load('ci-artifact-storage')

CALLBACK_PATHS = ('scripts/prepare-acp-callback-fixture.py', 'scripts/test-prepare-acp-callback-fixture.py',
                  'scripts/changed-tests.sh', 'scripts/callback-build-scope.py',
                  'scripts/test-callback-build-scope.py', 'crates/intent-acp/tests/fixtures/claude-callback-adapter.json',
                  'crates/intent-acp/tests/fixtures/claude-callback-canonical.patch',
                  'crates/intent-acp/tests/fixtures/claude-callback-files.json', '.github/workflows/ci.yml')
FULL_PROOF_PATHS = ('scripts/ci-consumer-process.py', 'scripts/test-ci-consumer-process.py', '.github/workflows/ci.yml', 'scripts/ci-linux-artifacts.py',
                    'scripts/ci-coverage-artifacts.py', 'scripts/ci-test-artifacts.py',
                    'scripts/test-ci-linux-artifacts.py', 'scripts/test-ci-coverage-artifacts.py',
                    'scripts/ci-artifact-reuse.py', 'scripts/ci-publish-variants.cjs',
                    '.github/actions/publish-variants/action.yml', 'scripts/ci-artifact-storage.py',
                    '.github/actions/publish-variants/package.json', '.github/actions/publish-variants/package-lock.json')
COMPILER_TESTS = ['pool_escape_apis_are_rejected']


def compiler_inventory(item, binary, env):
    test = item['runtime']['tests'][binary]
    cwd = archives.safe_join(item['source'], test['cwd'])
    executable = archives.safe_join(item['target'], test['path'])
    listing = archives.output([str(executable), '--list', '--format', 'terse'], cwd,
        archives.runtime_env(item, dict(env, CARGO_MANIFEST_DIR=str(cwd))))
    names = [line.removesuffix(': test') for line in listing.splitlines() if line.endswith(': test')]
    if names != COMPILER_TESTS:
        raise ValueError('compiler gate inventory changed: ' + repr(names))
    return names


def full_proof(changed):
    # An unreadable diff must not silently turn full pipeline validation off.
    return changed is None or bool(set(changed) & set(FULL_PROOF_PATHS))


def recipes(target, event, plan, *, callback_changed=False, full_proof=False):
    if event not in ('pull_request', 'merge_group'):
        raise ValueError('unsupported Linux event')
    result = {name: (recipe, False) for name, recipe in archives.native_recipes('linux', event, target).items()}
    def add(name, args, instrumented=False):
        # Keep implicit host targeting for LLVM: an explicit --target stops
        # instrumenting host procedural macros and narrows the old report.
        result[name] = ({'kind': 'test-archive', 'command': ['cargo', 'nextest', 'archive', '--locked', *([] if instrumented else ['--target', target]),
                           '--target-dir', '{target}', *args, '--cargo-message-format', 'json', '--archive-file', '{archive}'], 'companions': []}, instrumented)
    # Retain package feature graphs. A standalone service library differs from
    # the workspace and daemon graphs even when it names the same test.
    add('check-services', ['-p', 'intent-services', '--lib'])
    add('check-lints', ['--workspace', '--test', '*_lint'])
    add('check-core', ['-p', 'intent-core', '--lib'])
    add('check-daemon', ['-p', 'intentd', '--test', 'wss_integration'])
    add('check-installer', ['-p', 'intentd-sitter', '--test', 'install_ps1_owner'])
    if event == 'merge_group':
        add('check-gitlab', ['-p', 'intentd', '--features', 'repository-test-fixtures', '--test', 'e2e_wss_gitlab_auth', '--test', 'e2e_wss_invite_join_gitlab', '--test', 'e2e_repository_resource_read'])
    if event == 'merge_group' or full_proof:
        add('coverage-e2e', ['-p', 'intentd'], True)
        add('coverage-all', ['--workspace'], True)
    if event == 'pull_request':
        scope = callback.scope(plan, plan['head'])  # validates every structured argv
        for index, args in enumerate(plan['plans']):
            add('changed-' + str(index), args, True)
        if callback_changed:
            add('callback', scope, True)
    return result


def select(source, root, event, base):
    head = archives.source_commit(source)
    plan = {'version': 1, 'head': head, 'mergeBase': head, 'plans': []}
    changed = False
    if event == 'pull_request':
        if not base:
            raise ValueError('PR changed selection requires a connected base')
        subprocess.run(['bash', 'scripts/changed-tests.sh', '--instrumented', '--dry-run', '--base', base,
                        '--plan-json', str(root / 'changed-plan.json')], cwd=source, check=True)
        plan = json.loads((root / 'changed-plan.json').read_text())
        changed = bool(archives.output(['git', 'diff', '--name-only', base, 'HEAD', '--', *CALLBACK_PATHS], source))
    return plan, changed


def produce(source, root, event, base, *, reuse=True):
    source, root = Path(source).resolve(), Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    plan, changed = select(source, root, event, base)
    try:
        paths = archives.output(['git', 'diff', '--name-only', base, 'HEAD'], source).splitlines() if event == 'pull_request' else []
    except subprocess.CalledProcessError:
        paths = None
    proof = event == 'pull_request' and full_proof(paths)
    if os.environ.get('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a') as stream:
            stream.write('full_coverage=' + str(proof).lower() + '\n')
    facts = native.platform_facts('linux')
    demand = recipes(facts['platform']['target'], event, plan, callback_changed=changed, full_proof=proof)
    requests = {name: archives.capture_request(source, facts['platform'], recipe,
                 config_sha256=native.config_digest(source), native_tools=facts['native_tools'], instrumented=instrumented, compressed=instrumented)
                for name, (recipe, instrumented) in demand.items()}
    (root / 'requests.json').write_text(json.dumps(requests, indent=2) + '\n')
    if reuse:
        api = registry.GitHub(os.environ['GITHUB_REPOSITORY'])
        expected = registry.context(source, api)
        (root / 'publication.json').write_text(json.dumps(registry.publication(expected), indent=2) + '\n')
        registry.restore(requests, root, api, expected, 'linux')
    target = Path(os.environ.get('CARGO_TARGET_DIR', root / 'cargo-target')).resolve()
    (root / 'storage-target.json').write_text(json.dumps({'target': str(target)}))
    initial_state = 'populated' if target.exists() and any(p.is_file() for p in target.rglob('*.rlib')) else 'empty'
    started = time.monotonic()
    records = archives.produce(list(requests.values()), source, target, root / 'published')
    elapsed = time.monotonic() - started
    (root / 'producer.json').write_text(json.dumps({
        'target_initial_state': initial_state,
        'github_cache_exact_match': os.environ.get('LINUX_CACHE_EXACT_MATCH'),
        'compiler_cache_wrapper': os.environ.get('RUSTC_WRAPPER'),
        'archive_builds': sum(not record['reused'] for record in records),
        'archive_hits': sum(record['reused'] for record in records),
        'build_archive_stage_seconds': elapsed,
        'variants': records,
    }, indent=2) + '\n')
    compiler_gates(requests, source, target, root)
    return records


def compiler_gates(requests, source, target, root):
    """Run intentional compile-fail tests freshly, even on archive hits.

    Use each exact archived test executable, never approximate its features by
    recompiling with a narrower package scope. trybuild's child compilation is
    a compiler gate; its result/profraw are not reusable runtime artifacts.
    """
    receipt = {'source': archives.source_commit(source), 'gates': {}}
    try:
        for name, request in requests.items():
            if not (name == 'coverage-all' or name.startswith('changed-')):
                continue
            key = archives.identity.build_key(request['identity'])
            bundle = root / 'published' / key
            manifest = archives.verify_bundle(bundle, request)
            binary = 'intent-store::store_pool_api'
            if binary not in manifest['runtime']['tests']:
                continue
            with tempfile.TemporaryDirectory(prefix='compiler-gate-', dir=root) as tmp:
                item = prepare_streamed(bundle, request, source, Path(tmp) / 'archive')
                diagnostic_target = coverage.target_directory(target, coverage.capture(source, remap=False))
                env = coverage.build_environment(request['recipe']['coverage'], source, diagnostic_target, remap=False)
                env.update(INTENTD_ASSERT_BOUND_CALLER='1', INTENTD_TEST_TIMEOUT_MULTIPLIER='3',
                           CARGO_TARGET_DIR=str(diagnostic_target),
                           LLVM_PROFILE_FILE=str(Path(tmp) / 'compiler-gate-%p-%m.profraw'))
                started = time.monotonic()
                tests = compiler_inventory(item, binary, env)
                result = archives.run_libtest(item, binary, [], env=env)
                receipt['gates'][name] = {'key': key, 'binary': binary, 'tests': tests, 'exit_code': result, 'diagnostic_paths': 'original',
                                           'seconds': time.monotonic() - started}
                if result:
                    raise subprocess.CalledProcessError(result, ['fresh-compiler-gate', name, binary])
    finally:
        (root / 'compiler-gates.json').write_text(json.dumps(receipt, indent=2) + '\n')


def producer_prerequisite(api, marker, consumer_attempt):
    """Authenticate a successful prerequisite, never an artifact's result claim."""
    run_id, producer_attempt = marker['run_id'], marker['attempt']
    if not 1 <= producer_attempt <= consumer_attempt:
        raise ValueError('invalid producer prerequisite attempt lineage')
    original = None
    for attempt in range(producer_attempt, consumer_attempt + 1):
        jobs = [job for job in api.pages(f'actions/runs/{run_id}/attempts/{attempt}/jobs', 'jobs')
                if job.get('name') == 'check']
        if attempt == producer_attempt:
            if len(jobs) != 1:
                raise ValueError('missing or ambiguous producer prerequisite job')
            original = jobs[0]
            steps = [step for step in original.get('steps', [])
                     if step.get('name') == 'Produce Linux variants sequentially in one shared target']
            if (original.get('run_id') != run_id or original.get('run_attempt') != producer_attempt
                    or original.get('head_sha') != marker['context']['head_sha']
                    or original.get('status') != 'completed' or original.get('conclusion') != 'success'
                    or type(original.get('id')) is not int or len(steps) != 1
                    or steps[0].get('status') != 'completed' or steps[0].get('conclusion') != 'success'):
                raise ValueError('producer prerequisite was not an exact successful job and build step')
        elif any(job != original for job in jobs):
            # A new check (even one failing before publication) supersedes the
            # retained prerequisite. Only an absent or identical retained job fits.
            raise ValueError('producer prerequisite was superseded by a later attempt')
    return {'context': marker['context'], 'run_id': run_id, 'job_id': original['id'],
            'producer_attempt': producer_attempt, 'consumer_attempt': consumer_attempt,
            'job_name': 'check', 'conclusion': 'success', 'producer_step': 'success'}


def compiler_exclusion(root, name, request, item):
    binary = 'intent-store::store_pool_api'
    if binary not in item['runtime']['tests']:
        return None
    receipt = archives.identity.read_request(root / 'compiler-gates.json')
    gate = receipt['gates'].get(name)
    if receipt['source'] != request['identity']['source']['commit'] or not gate or gate['key'] != archives.identity.build_key(request['identity']) or gate['binary'] != binary or gate['exit_code'] != 0 or gate.get('tests') != COMPILER_TESTS:
        raise ValueError('compile-fail test has no successful fresh producer gate')
    path = root / 'producer-prerequisite.json'
    if not path.is_file():
        raise ValueError('compile-fail exclusion requires an authenticated producer prerequisite')
    prerequisite = archives.identity.read_request(path)
    publication = archives.identity.read_request(root / 'publication.json')
    if (prerequisite.get('context') != publication['context']
            or prerequisite['context']['source'] != receipt['source']
            or prerequisite.get('run_id') != publication['run_id']
            or prerequisite.get('run_id') != int(os.environ['GITHUB_RUN_ID'])
            or prerequisite.get('producer_attempt') != publication['attempt']
            or prerequisite.get('consumer_attempt') != int(os.environ['GITHUB_RUN_ATTEMPT'])
            or prerequisite.get('job_name') != 'check' or prerequisite.get('conclusion') != 'success'
            or prerequisite.get('producer_step') != 'success' or type(prerequisite.get('job_id')) is not int
            or prerequisite.get('gate_keys', {}).get(name) != gate['key']):
        raise ValueError('compile-fail exclusion producer prerequisite binding differs')
    return 'not binary(=store_pool_api)'


def consumer_disk_budget(archive_sizes, expansion_sizes):
    # Hosted full/changed runs reached 124 GB from roughly 51 GB of
    # extracted objects: profiles/reports need far more than twenty percent.
    # Reserve another 150% (plus the in-flight ZIP) above fresh extractions.
    # This is conservative measured headroom, not a bound on arbitrary tests.
    expanded = sum(expansion_sizes)
    return sum(archive_sizes) + expanded + max(archive_sizes) + max(8 * 1024**3, 3 * expanded // 2)


def fetch(source, root, scope):
    """Validate this run's small catalog, then download only this consumer's keys."""
    if scope not in ('coverage-e2e', 'coverage-all', 'changed'):
        raise ValueError('consumer fetch requires an explicit scope')
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    api = registry.GitHub(os.environ['GITHUB_REPOSITORY'])
    expected = registry.context(source, api)
    run_id = int(os.environ['GITHUB_RUN_ID'])
    run = api.run(run_id)
    prefix = registry.artifact_name('linux', expected['source']) + '-'
    attempt = int(os.environ['GITHUB_RUN_ATTEMPT'])
    initial_root = root
    receipt = {'scope': scope, 'transfers': [], 'complete': False}
    started = time.monotonic()
    try:
        available = api.list_artifacts(run_id)
        candidates = [a for a in available if a['name'] == prefix + str(attempt) and registry.live(a)]
        if not candidates:
            candidates = [a for a in available if a['name'].startswith(prefix)
                          and a['name'][len(prefix):].isdigit()
                          and 1 <= int(a['name'][len(prefix):]) < attempt and registry.live(a)]
            candidates.sort(key=lambda a: int(a['name'][len(prefix):]), reverse=True)
        if not candidates or len({a['name'] for a in candidates}) != len(candidates):
            raise ValueError('required Linux producer index missing or ambiguous; no consumer build fallback')
        with tempfile.TemporaryDirectory(prefix='verified-index-', dir=root) as tmp:
            for candidate in candidates:
                if candidate['size_in_bytes'] > 32 * 1024**2:
                    raise ValueError('publication index exceeds metadata size budget')
                stage = Path(tmp) / str(candidate['id'])
                receipt['transfers'].append(registry.download_verified(api, candidate, stage))
                content = stage / 'content'
                marker, catalog = registry.validate_marker(content, candidate, run, expected, 'linux')
                if marker != dict(registry.publication(expected), attempt=marker['attempt']):
                    raise ValueError('Linux consumer requires its current run provenance')
                requests = archives.identity.read_request(content / 'requests.json')
                wanted_names = {n: r for n, r in requests.items()
                                if n == scope or (scope == 'changed' and (n.startswith('changed-') or n == 'callback'))}
                keys = {archives.identity.build_key(r['identity']) for r in wanted_names.values()}
                complete = keys and all(key in catalog and any(a['name'] == catalog[key]['name'] and registry.live(a)
                                                              for a in available) for key in keys)
                if marker['attempt'] == attempt or complete:
                    break  # An existing current producer is authoritative, even if partial.
            else:
                raise ValueError('required earlier producer variants not finalized; no consumer build fallback')
            receipt.update(producer_attempt=marker['attempt'], consumer_attempt=attempt)
            requests = archives.identity.read_request(content / 'requests.json')
            for request in requests.values():
                archives.validate_request(request)
                if request['identity']['source']['commit'] != expected['source'] or request['identity']['platform']['os'] != 'linux':
                    raise ValueError('Linux publication source/platform mismatch')
            selected = {name: request for name, request in requests.items()
                        if name == scope or (scope == 'changed' and (name.startswith('changed-') or name == 'callback'))}
            if not selected:
                raise ValueError('required consumer scope absent from publication')
            wanted = {archives.identity.build_key(r['identity']): r for r in selected.values()}
            if wanted.keys() - catalog.keys():
                raise ValueError('required consumer variants absent from publication')
            prerequisite = None
            gates_path = content / 'compiler-gates.json'
            if gates_path.is_file():
                gate_names = set(archives.identity.read_request(gates_path)['gates']) & selected.keys()
                if gate_names:
                    prerequisite = producer_prerequisite(api, marker, attempt)
                    prerequisite['gate_keys'] = {name: archives.identity.build_key(selected[name]['identity']) for name in gate_names}
                    receipt['producer_prerequisite'] = prerequisite
            entries = [catalog[key] for key in wanted]
            # Compressed bundles + all selected fresh extractions + one in-flight
            # ZIP, plus fresh profiles and report headroom. Never use another
            # filesystem's free-space number to approve this root.
            budget = consumer_disk_budget(
                [entry['archive_bytes'] for entry in entries],
                [catalog[archives.identity.build_key(r['identity'])]['expanded_bytes'] for r in selected.values()])
            free = shutil.disk_usage(root).free
            receipt.update(selected_names=list(selected), required_free_bytes=budget, free_bytes_before=free)
            if free < budget and (root / 'storage.json').is_file():
                root = storage.reselect(root, budget)
                free = shutil.disk_usage(root).free
                receipt.update(initial_root=str(initial_root), selected_root=str(root), free_bytes_before=free)
            if free < budget:
                raise ValueError(f'selected coverage artifacts need {budget} free bytes at {root}; only {free} available')
            hits, transfers = registry.restore_variants(wanted, root, api, expected, 'linux', candidate, run, content, require=True)
            receipt['transfers'].extend(transfers)
            for filename in ('publication.json', 'compiler-gates.json', 'producer.json'):
                if (content / filename).is_file():
                    shutil.copyfile(content / filename, root / filename)
            if prerequisite is not None:
                (root / 'producer-prerequisite.json').write_text(json.dumps(prerequisite, indent=2) + '\n')
            (root / 'requests.json').write_text(json.dumps(selected, indent=2) + '\n')
            receipt.update(complete=True, variant_artifacts=hits, free_bytes_after=shutil.disk_usage(root).free)
    finally:
        receipt['fetch_seconds'] = time.monotonic() - started
        receipt['bytes'] = sum(item['bytes'] for item in receipt['transfers'])
        receipt['download_seconds'] = sum(item['download_seconds'] for item in receipt['transfers'])
        (root / 'download.json').write_text(json.dumps(receipt, indent=2) + '\n')
        if root != initial_root:
            storage.cleanup(initial_root)


def extract_stream(bundle, manifest, destination, zstd_archive):
    """One bounded pass: authenticate every byte while writing a fresh target.

    No symlinks, special files, overwrites or unlisted expansion are permitted.
    Preserve nextest metadata verbatim; run_nextest supplies both remap paths.
    """
    destination.mkdir(parents=True, exist_ok=False)
    seen, directories = set(), set()
    allowed_directories = {parent.as_posix() for name in manifest['files'] for parent in Path(name).parents}
    def extract(tar, report=False):
        for member in tar:
            name = member.name.rstrip('/')
            path = archives.safe_join(destination, name)
            if member.isdir() and not report:
                if name not in allowed_directories or name in directories or name in manifest['files']:
                    raise ValueError('unlisted, duplicate or colliding archive directory')
                directories.add(name)
                path.mkdir(parents=True, exist_ok=True)
                continue
            if not member.isfile() or name in seen:
                raise ValueError('unsupported or duplicate expanded archive member')
            expected = manifest['files'].get(name)
            if expected is None or member.size != expected['size'] or member.size < 0:
                raise ValueError('expanded archive member exceeds manifest inventory')
            if report and (not name.startswith('target/') or path.suffix != '.so'):
                raise ValueError('invalid coverage report object')
            seen.add(name)
            path.parent.mkdir(parents=True, exist_ok=True)
            digest = hashlib.sha256()
            with tar.extractfile(member) as incoming, path.open('xb') as output:
                remaining = member.size
                while remaining:
                    chunk = incoming.read(min(1024 * 1024, remaining))
                    if not chunk:
                        raise ValueError('truncated expanded archive member')
                    output.write(chunk); digest.update(chunk); remaining -= len(chunk)
            if digest.hexdigest() != expected['sha256']:
                raise ValueError('extracted file digest mismatch')
            mode = member.mode & 0o777
            if report:
                # Match the existing Python data-filtered report-object extraction.
                mode = (mode & 0o755) | 0o600
                if not member.mode & 0o100:
                    mode &= ~0o111
            path.chmod(mode)
    archive = bundle / manifest['archive']
    if zstd_archive:
        with subprocess.Popen(['zstd', '-dq', '-M2048MB', '-c', str(archive)], stdout=subprocess.PIPE) as process:
            try:
                with tarfile.open(fileobj=process.stdout, mode='r|', bufsize=1024 * 1024) as tar:
                    extract(tar)
                # Consume the authenticated stream through its checksum/footer.
                while process.stdout.read(1024 * 1024):
                    pass
                if process.wait():
                    raise ValueError('archive decompression failed')
            finally:
                if process.poll() is None:
                    process.kill()
                process.wait()
    else:
        with tarfile.open(archive, 'r:gz') as tar:
            extract(tar)
    if 'report_archive' in manifest:
        with tarfile.open(bundle / manifest['report_archive']['name'], 'r:gz') as tar:
            extract(tar, report=True)
    if seen != manifest['files'].keys():
        raise ValueError('expanded archive inventory is incomplete')


def prepare_streamed(bundle, request, source, destination, *, env=None):
    bundle, source, destination = (Path(p).resolve() for p in (bundle, source, destination))
    started = time.monotonic()
    manifest = archives.verify_bundle(bundle, request)
    archives.check_source(source, manifest['identity'])
    if archives.output([*archives.nextest_command(), '--version'], source, env) != manifest['identity']['tools']['nextest']:
        raise ValueError('consumer nextest version differs from identity')
    validation_seconds = time.monotonic() - started
    started = time.monotonic()
    is_nextest = request['recipe']['kind'] == 'test-archive'
    extract_stream(bundle, manifest, destination, is_nextest)
    runtime = archives.nextest_inventory(destination, source) if is_nextest else manifest['runtime']
    if runtime != manifest['runtime']:
        raise ValueError('archive runtime inventory mismatch')
    if set(request['recipe']['companions']) - runtime['companions'].keys():
        raise ValueError('archive missing required companions')
    return {'source': str(source), 'target': str(destination / 'target'),
            'report_metadata': manifest['files'].get('target/nextest/binaries-metadata.json'),
            'producer_group': archives.identity.producer_group_key(manifest['identity']),
            'runtime': runtime, 'extract_seconds': time.monotonic() - started,
            'expansion_validation_seconds': validation_seconds}


def prepare(source, root, names):
    requests = json.loads((root / 'requests.json').read_text())
    prepared = {}
    env = native.consumer_environment(root)
    for name in names:
        request = requests[name]
        key = archives.identity.build_key(request['identity'])
        # No overwrite: a previous run's profraw or binaries must never leak in.
        prepared[name] = prepare_streamed(root / 'published' / key, request, source, root / 'restored' / name, env=env)
        prepared[name]['request_key'] = key
        progress = {n: {k: item[k] for k in ('request_key', 'extract_seconds', 'expansion_validation_seconds')}
                    for n, item in prepared.items()}
        pending = root / 'preparation-progress.tmp'
        pending.write_text(json.dumps(progress, indent=2) + '\n')
        pending.replace(root / 'preparation-progress.json')
    (root / 'prepared.json').write_text(json.dumps(prepared, indent=2) + '\n')
    return prepared


def run_coverage(source, root, scope):
    requests = json.loads((root / 'requests.json').read_text())
    names = [scope] if scope != 'changed' else [name for name in requests if name.startswith('changed-')]
    # Validate every requested archive against the clean checkout before tests
    # can create runtime files. Callback execution remains a separate step.
    prepare_names = names + (['callback'] if scope == 'changed' and 'callback' in requests else [])
    prepared = prepare(source, root, prepare_names)
    download = json.loads((root / 'download.json').read_text()) if (root / 'download.json').is_file() else {}
    receipt = {'scope': scope, 'compiler_calls': 0, 'variants': {},
               'producer_attempt': download.get('producer_attempt'), 'consumer_attempt': download.get('consumer_attempt'),
               'producer_prerequisite': download.get('producer_prerequisite')}
    try:
        for name in names:
            item = prepared[name]
            run = Path(item['target']).parent
            # Source-scanning tests legitimately query Cargo metadata. The same
            # strict report guards permit those read-only probes, never builds.
            env = dict(coverage.report_environment(root), LLVM_PROFILE_FILE=str(run / 'intentd-%p-%32m.profraw'),
                       INTENTD_ASSERT_BOUND_CALLER='1', INTENTD_TEST_TIMEOUT_MULTIPLIER='3')
            args = [] if scope == 'coverage-all' else ['-E', 'kind(test) and not binary(intentd) and not binary(auggie_context_e2e)' if scope == 'coverage-e2e' else 'not binary(auggie_context_e2e)']
            exclusion = compiler_exclusion(root, name, requests[name], item)
            if exclusion:
                args = ['-E', '(' + args[1] + ') and ' + exclusion] if args else ['-E', exclusion]
            started = time.monotonic()
            compiler_only = bool(exclusion) and len(item['runtime']['tests']) == 1
            result = 0 if compiler_only else archives.run_nextest(item, args, env=env)
            timing = {'test_seconds': time.monotonic() - started, 'extract_seconds': item['extract_seconds'], 'test_exit_code': result, 'producer_compiler_gate': bool(exclusion)}
            timing['validation_seconds'] = item.get('expansion_validation_seconds', 0)
            timing['producer_test_inventory'] = COMPILER_TESTS if exclusion else []
            timing['compiler_only_selection'] = compiler_only
            receipt['variants'][name] = timing
            if result:
                raise subprocess.CalledProcessError(result, ['archive-tests', name])
            if scope != 'changed':
                request = requests[name]
                bundle = root / 'published' / archives.identity.build_key(request['identity'])
                timing.update(coverage.report(bundle, item, request, 40 if scope == 'coverage-e2e' else 80))
                if timing['exit_code']:
                    raise subprocess.CalledProcessError(timing['exit_code'], ['coverage-report', name])
    finally:
        logs = [root / 'compiler-calls.log', *(root / 'restored').glob('*/compiler-calls.log')]
        calls = [line for log in logs if log.exists() for line in log.read_text().splitlines() if line]
        receipt['compiler_calls'] = len(calls)
        (root / ('receipt-' + scope + '.json')).write_text(json.dumps(receipt, indent=2) + '\n')
        if calls:
            raise ValueError('runtime consumer attempted compilation: ' + '\n'.join(calls))


def run_test(root, command):
    """Accept the old test selection as data, with no Cargo build fallback.

    Keeping the literal source-lint glob at the workflow boundary makes new
    lint targets discoverable to the existing repository lint as well.
    """
    scopes = {
        ('--workspace', '--test', '*_lint'): 'check-lints',
        ('-p', 'intent-services', '--lib'): 'check-services',
        ('-p', 'intent-core', '--lib'): 'check-core',
        ('-p', 'intentd', '--test', 'wss_integration'): 'check-daemon',
        ('-p', 'intentd-sitter', '--test', 'install_ps1_owner'): 'check-installer',
        ('--locked', '-p', 'intentd', '--features', 'repository-test-fixtures', '--test', 'e2e_wss_gitlab_auth', '--test', 'e2e_wss_invite_join_gitlab', '--test', 'e2e_repository_resource_read'): 'check-gitlab',
    }
    if command[:2] != ['cargo', 'test']:
        raise ValueError('expected an explicit cargo test selection')
    args = command[2:]
    for scope, name in scopes.items():
        if args[:len(scope)] != list(scope):
            continue
        test_args = args[len(scope):]
        if test_args[:1] == ['--']:
            test_args = test_args[1:]
        item = json.loads((root / 'prepared.json').read_text())[name]
        env = coverage.report_environment(root)
        env['INTENTD_ASSERT_BOUND_CALLER'] = '1'
        if not item['runtime']['tests']:
            raise ValueError('required test archive contains no test binaries')
        for binary_id in item['runtime']['tests']:
            if name == 'check-lints' and not fnmatch.fnmatchcase(binary_id.split('::')[-1], '*_lint'):
                raise ValueError('source-lint archive contains an unexpected target')
            code = archives.run_libtest(item, binary_id, test_args, env=env)
            if code:
                return code
        return 0
    raise ValueError('unsupported test selection; consumer compilation is forbidden')


def run_callback(source, root):
    requests = json.loads((root / 'requests.json').read_text())
    if 'callback' not in requests:
        raise ValueError('required callback archive missing')
    prepared = json.loads((root / 'prepared.json').read_text())
    item = prepared['callback']
    if Path(item['source']).resolve() != Path(source).resolve():
        raise ValueError('callback prepared source differs from current checkout')
    if Path(item['target']).resolve() != (root / 'restored/callback/target').resolve():
        raise ValueError('callback prepared target differs from this run')
    request = requests['callback']
    if item.get('request_key') != archives.identity.build_key(request['identity']):
        raise ValueError('callback prepared request differs from current demand')
    # Runtime-created untracked files do not invalidate an already validated
    # extraction. A different commit or any tracked source change still does.
    if archives.output(['git', 'rev-parse', 'HEAD'], source) != request['identity']['source']['commit']:
        raise ValueError('callback source commit changed after preparation')
    changed_paths = archives.output(['git', 'diff', '--name-only', 'HEAD', '--'], source)
    if changed_paths:
        raise ValueError('callback tracked source changed after preparation: ' + changed_paths)
    for name, digest in request['identity']['build']['inputs'].items():
        path = archives.safe_join(source, name)
        if (archives.file_sha(path) if path.is_file() else archives.sha(b'')) != digest:
            raise ValueError('callback source input changed after preparation: ' + name)
    names = os.environ['CALLBACK_FIXTURE_TESTS'].split()
    if len(names) != 6 or len(set(names)) != 6:
        raise ValueError('expected exactly six callback fixtures')
    filterset = '|'.join('test(=' + name + ')' for name in names)
    run = Path(item['target']).parent
    env = archives.runtime_env(item, dict(native.consumer_environment(root),
        LLVM_PROFILE_FILE=str(run / 'intentd-%p-%32m.profraw'), INTENTD_ASSERT_BOUND_CALLER='1', INTENTD_TEST_TIMEOUT_MULTIPLIER='3'))
    target = Path(item['target'])
    command = [*archives.nextest_command(), 'list', '--cargo-metadata', str(target / 'nextest/cargo-metadata.json'),
               '--binaries-metadata', str(target / 'nextest/binaries-metadata.json'), '--workspace-remap', str(source),
               '--target-dir-remap', str(target), '-E', filterset, '--message-format', 'json']
    receipt = {'test_exit_code': None, 'selected_tests': [],
               'extract_seconds': item.get('extract_seconds', 0),
               'expansion_validation_seconds': item.get('expansion_validation_seconds', 0)}
    started = time.monotonic()
    try:
        listing = json.loads(archives.output(command, source, env))
        selected = [name for suite in listing['rust-suites'].values() for name, case in suite['testcases'].items()
                    if case['filter-match']['status'] == 'matches']
        receipt['selected_tests'] = selected
        if len(selected) != len(names) or set(selected) != set(names):
            raise ValueError('callback fixture inventory differs: ' + repr(selected))
        started = time.monotonic()
        result = archives.run_nextest(item, ['-E', filterset, '--no-fail-fast', '--retries', '0', '--status-level', 'pass', '--final-status-level', 'fail'], env=env)
        receipt['test_exit_code'] = result
        return result
    finally:
        logs = [root / 'compiler-calls.log', *(root / 'restored').glob('*/compiler-calls.log')]
        calls = [line for log in logs if log.exists() for line in log.read_text().splitlines() if line]
        receipt.update(test_seconds=time.monotonic() - started, compiler_calls=len(calls))
        (root / 'receipt-callback.json').write_text(json.dumps(receipt, indent=2) + '\n')
        if calls:
            raise ValueError('callback consumer attempted compilation: ' + '\n'.join(calls))



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('produce', 'fetch', 'prepare-check', 'coverage', 'test', 'callback'))
    parser.add_argument('--root', type=Path, default=Path(os.environ.get('NATIVE_ARTIFACT_ROOT', str(Path(os.environ.get('RUNNER_TEMP', '.')) / 'native-artifacts'))))
    parser.add_argument('--source', type=Path, default=Path.cwd())
    parser.add_argument('--event', choices=('pull_request', 'merge_group'), default=os.environ.get('GITHUB_EVENT_NAME'))
    parser.add_argument('--base')
    parser.add_argument('--scope', choices=('coverage-e2e', 'coverage-all', 'changed'))
    args, test_args = parser.parse_known_args()
    if args.command != 'test' and test_args:
        parser.error('unexpected arguments')
    if args.command == 'produce':
        produce(args.source, args.root, args.event, args.base)
    elif args.command == 'fetch':
        fetch(args.source, args.root, args.scope)
    elif args.command == 'prepare-check':
        requests = json.loads((args.root / 'requests.json').read_text())
        prepare(args.source, args.root, [name for name, request in requests.items() if request['identity']['build']['instrumentation']['mode'] == 'none'])
    elif args.command == 'coverage':
        run_coverage(args.source, args.root, args.scope)
    elif args.command == 'test':
        return run_test(args.root, test_args[1:] if test_args[:1] == ['--'] else test_args)
    else:
        return run_callback(args.source, args.root)


if __name__ == '__main__':
    sys.exit(main())
