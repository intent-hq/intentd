#!/usr/bin/env python3
"""Cleanup trust boundary and existing CI contract regressions (offline)."""
from pathlib import Path
import importlib.util
import unittest
import os
import json
import subprocess
import tempfile

ISOLATION_SPEC = importlib.util.spec_from_file_location(
    'retirement_test_isolation', Path(__file__).with_name('retirement-test-isolation.py'))
ISOLATION = importlib.util.module_from_spec(ISOLATION_SPEC)
ISOLATION_SPEC.loader.exec_module(ISOLATION)
ISOLATION.install()


import yaml

ROOT = Path(__file__).resolve().parents[1]


def workflow(name):
    return yaml.safe_load((ROOT / '.github/workflows' / name).read_text())


class WorkflowTests(unittest.TestCase):
    def test_cleanup_only_trusted_events_default_main_and_local_repo(self):
        doc = workflow('retire-obsolete-merge-runs.yml')
        self.assertEqual(set(doc[True]), {'schedule', 'workflow_dispatch'})
        self.assertEqual(doc[True]['schedule'], [{'cron': '7,22,37,52 * * * *'}])
        self.assertEqual(doc['permissions'], {})
        self.assertEqual(doc['concurrency'], {'group': 'retire-obsolete-merge-runs', 'cancel-in-progress': False})
        self.assertEqual(set(doc['jobs']), {'cleanup'})
        job = doc['jobs']['cleanup']
        self.assertEqual(job['if'], "github.repository == 'intent-hq/intentd' && github.ref == 'refs/heads/main'")
        self.assertEqual(job['runs-on'], 'ubuntu-latest')
        self.assertEqual(job['permissions'], {'actions': 'write', 'contents': 'read', 'pull-requests': 'read'})
        self.assertEqual(job['timeout-minutes'], 10)
        checkout = job['steps'][0]
        self.assertEqual(checkout['with'], {'ref': 'main', 'persist-credentials': False})
        run = next(s for s in job['steps'] if s.get('id') == 'sweep')
        self.assertEqual(run['env']['GH_TOKEN'], '${{ github.token }}')
        self.assertEqual(run['env']['RETIREMENT_READ_TOKEN'], '${{ secrets.CI_QUEUE_READ_TOKEN }}')
        self.assertIn('--workflow-credentials', run['run'])
        self.assertIn('--repository intent-hq/intentd', run['run'])
        self.assertIn('--state .retirement/cursor.json', run['run'])
        self.assertIn('python3 -I -B', run['run'])
        self.assertIn('tee retirement-report.json', run['run'])
        self.assertEqual(doc[True]['workflow_dispatch']['inputs']['mode'], {
            'description': 'Preview by default; apply requests exact eligible-run cancellation',
            'type': 'choice', 'options': ['dry-run', 'apply'], 'default': 'dry-run'})
        self.assertEqual(job['env']['RETIREMENT_MODE'],
                         "${{ github.event_name == 'schedule' && 'apply' || inputs.mode || 'dry-run' }}")
        self.assertIn('--"$RETIREMENT_MODE"', run['run'])
        commands = '\n'.join(s.get('run', '') for s in job['steps'][:job['steps'].index(run)])
        self.assertIn('scripts/test-retire-obsolete-merge-runs.py', commands)
        self.assertIn('scripts/test-retire-obsolete-merge-runs-caller.py', commands)

    def test_cursor_persists_even_when_later_batch_fails(self):
        steps = workflow('retire-obsolete-merge-runs.yml')['jobs']['cleanup']['steps']
        restore = next(s for s in steps if s.get('id') == 'cursor')
        save = next(s for s in steps if s.get('uses', '').startswith('actions/cache/save@'))
        self.assertEqual(restore['with']['path'], '.retirement/cursor.json')
        self.assertEqual(save['with']['path'], restore['with']['path'])
        self.assertIn('github.run_id', save['with']['key'])
        self.assertIn('github.run_attempt', save['with']['key'])
        self.assertEqual(save['if'], "always() && steps.sweep.outcome != 'skipped' && hashFiles('.retirement/cursor.json') != ''")
        self.assertIn('env.RETIREMENT_MODE', restore['with']['restore-keys'])
        upload = next(s for s in steps if s.get('uses', '').startswith('actions/upload-artifact@'))
        self.assertEqual(upload['if'], 'always()')
        self.assertEqual(upload['with']['path'], 'retirement-report.json')

    def test_existing_read_credential_probe_uses_reviewed_immutable_code(self):
        doc = workflow('retirement-credential-probe.yml')
        self.assertEqual(set(doc[True]), {'pull_request'})
        self.assertEqual(doc['permissions'], {})
        self.assertEqual(set(doc['jobs']), {'existing-read-credential'})
        job = doc['jobs']['existing-read-credential']
        self.assertEqual(job['if'], "github.repository == 'intent-hq/intentd' && github.event.pull_request.head.repo.full_name == github.repository")
        self.assertEqual(job['permissions'], {'contents': 'read'})
        self.assertEqual(job['runs-on'], 'ubuntu-latest')
        self.assertEqual(job['steps'][0]['with'], {
            'ref': '62a2a110b7e217151280736acb7fd19a1daf3c71', 'persist-credentials': False})
        step = next(s for s in job['steps'] if s.get('id') == 'probe')
        self.assertEqual(step['env']['GH_TOKEN'], '${{ secrets.CI_QUEUE_READ_TOKEN }}')
        self.assertIn("for name in ('protection', 'activity', 'reads')", step['run'])
        self.assertNotIn('reconcile(', step['run'])
        self.assertIn('read-only probe rejected request', step['run'])
        self.assertIn("body.get('query') == policy.QUEUE_QUERY", step['run'])

    def test_fixed_probe_executes_all_reads_and_distinguishes_unknown_activity(self):
        job = workflow('retirement-credential-probe.yml')['jobs']['existing-read-credential']
        command = next(s['run'] for s in job['steps'] if s.get('id') == 'probe')
        for scenario in ('known', 'null-head', 'missing-bypass', 'queue-denied', 'compare-denied',
                         'two-pages', 'later-denied', 'later-errors', 'later-bad-page', 'later-loop',
                         'no-live', 'foreign-exact', 'wrong-exact-id', 'refs-incomplete'):
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / 'scripts').mkdir()
                (root / 'scripts/retire-obsolete-merge-runs.py').write_text(
                    (ROOT / 'scripts/retire-obsolete-merge-runs.py').read_text())
                driver = root / 'gh'
                driver.write_text("#!/usr/bin/env -S python3 -I -S -B\n" + r"""
import importlib.util, json, os, sys
from pathlib import Path
import importlib.util
fixture_path = FIXTURE_PATH
spec = importlib.util.spec_from_file_location('fixture', fixture_path)
t = importlib.util.module_from_spec(spec)
spec.loader.exec_module(t)
api = t.FakeGitHub('intent-hq/intentd')
api.compares[(t.B, t.B)] = t.comparison(base=t.B, head=t.B, status='identical')
scenario = os.environ['PROBE_CASE']
if scenario == 'null-head': api.heads = [None]
if scenario == 'missing-bypass': del api.rules[1]['bypass_actors']
if scenario == 'no-live':
    api.runs = {38388564446: api.make_run(38388564446)}
    api.runs[38388564446]['status'] = 'completed'
    api.runs[38388564446]['conclusion'] = 'success'
if scenario == 'two-pages' or scenario.startswith('later-'):
    first = api.queue_body([None], more=True, cursor='1')
    second = api.queue_body([t.H])
    second['data']['repository']['mergeQueue']['entries']['nodes'][0]['id'] = 'MQE_1'
    if scenario == 'later-errors': second['errors'] = [{'message': 'partial'}]
    if scenario == 'later-bad-page':
        second['data']['repository']['mergeQueue']['entries']['pageInfo']['hasNextPage'] = 'false'
    if scenario == 'later-loop':
        second['data']['repository']['mergeQueue']['entries']['pageInfo'] = {'hasNextPage': True, 'endCursor': '1'}
    api.queue_pages = [first, second]
method = sys.argv[sys.argv.index('--method') + 1]
path = next(a for a in sys.argv[1:] if a.startswith('/'))
body = json.load(sys.stdin) if '--input' in sys.argv else None
with open('requests.jsonl', 'a') as log: log.write(json.dumps([method, path]) + '\n')
if (scenario == 'queue-denied' and path == '/graphql') or (scenario == 'compare-denied' and '/compare/' in path) or (scenario == 'later-denied' and path == '/graphql' and body['variables'].get('cursor') == '1'):
    response = api.response({'message': 'forbidden'}, status=403)
else:
    response = api.request(method, path, body)
if '/actions/runs/' in path and scenario == 'foreign-exact':
    response.data['repository']['full_name'] = 'other/repo'
if '/actions/runs/' in path and scenario == 'wrong-exact-id':
    response.data['id'] = 999
if '/git/matching-refs/' in path and scenario == 'refs-incomplete':
    response.headers = {'link': 'invalid-link'}
headers = ''.join(k + ': ' + v + '\n' for k, v in response.headers.items())
print('HTTP/2.0 ' + str(response.status) + ' response\n' + headers + '\n' + json.dumps(response.data))
""".replace('FIXTURE_PATH', repr(str(ROOT / 'scripts/test-retire-obsolete-merge-runs.py'))))
                driver.chmod(0o755)
                result = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', command],
                                        cwd=root, env={**os.environ, 'PATH': str(root) + ':' + os.environ['PATH'],
                                                       'PROBE_CASE': scenario, 'GH_TOKEN': 'offline-read-token'},
                                        capture_output=True, text=True, timeout=15)
                report = json.loads((root / 'existing-read-credential-report.json').read_text())
                good = scenario in ('known', 'null-head', 'two-pages', 'no-live')
                self.assertEqual(result.returncode == 0, good, (report, result.stderr))
                self.assertEqual(report['access_proven'], good)
                requests = [json.loads(line) for line in (root / 'requests.jsonl').read_text().splitlines()]
                self.assertTrue(all(method == 'GET' or (method == 'POST' and path == '/graphql')
                                    for method, path in requests))
                self.assertTrue(any('/compare/' in path for _, path in requests))
                if good:
                    self.assertTrue(report['checks']['reads']['exact_run_read_exercised'])
                if scenario in ('null-head', 'two-pages'):
                    self.assertEqual(report['checks']['activity']['eligibility'], 'unknown')
                if scenario == 'two-pages':
                    pages = [r for r in report['requests'] if r['path'] == '/graphql']
                    self.assertEqual([r['page_cursor'] for r in pages], [None, '1'])
                if scenario == 'no-live':
                    self.assertEqual(report['checks']['reads']['sampled_run_id'], 38388564446)

    def test_actual_shell_selects_only_valid_modes_and_preserves_failure(self):
        job = workflow('retire-obsolete-merge-runs.yml')['jobs']['cleanup']
        command = next(s['run'] for s in job['steps'] if s.get('id') == 'sweep')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'scripts').mkdir()
            stub = root / 'scripts/retire-obsolete-merge-runs-caller.py'
            stub.write_text("import json,sys; print(json.dumps(sys.argv[1:]))")
            for mode in ('dry-run', 'apply', 'bogus'):
                result = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', command],
                                        cwd=root, env={**os.environ, 'RETIREMENT_MODE': mode},
                                        capture_output=True, text=True, timeout=5)
                if mode == 'bogus':
                    self.assertNotEqual(result.returncode, 0)
                else:
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn('--' + mode, result.stdout)
            stub.write_text('raise SystemExit(2)')
            result = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', command],
                                    cwd=root, env={**os.environ, 'RETIREMENT_MODE': 'apply'},
                                    capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 2)

    def test_gate_and_protected_events_stay_unchanged(self):
        ci = workflow('ci.yml')
        self.assertEqual(set(ci[True]), {'push', 'pull_request', 'merge_group'})
        self.assertEqual(ci['concurrency']['cancel-in-progress'], "${{ github.event_name == 'pull_request' }}")
        self.assertEqual(set(ci['jobs']['gate']['needs']), {
            'route', 'pr-static', 'cache-smoke',
            'daemon-lifecycle', 'check', 'build', 'macos-services-runtime', 'coverage-e2e', 'coverage-all',
            'coverage-changed', 'pr-title', 'conflict-markers', 'monorepo-consumer-checks',
            'deb-packaging', 'release-scripts', 'install-ps1', 'pi-session-paths'})
        commands = '\n'.join(s.get('run', '') for s in ci['jobs']['release-scripts']['steps'])
        self.assertIn('python3 -B scripts/test-hosted-runners.py', commands)
        for test in ('test-retire-obsolete-merge-runs.py', 'test-retire-obsolete-merge-runs-caller.py',
                     'test-retire-obsolete-merge-runs-workflow.py'):
            self.assertIn('python3 -I -B scripts/' + test, commands)
        for name, job in ci['jobs'].items():
            self.assertNotIn('retire-obsolete', str(job.get('needs', '')))


if __name__ == '__main__':
    unittest.main()
