#!/usr/bin/env python3
"""Release crates.io contract: fetch crates with retries, then run dist offline.

`dist build` and `dist host` run `cargo metadata`, which on a fresh runner
downloads the whole dependency graph. A crates.io HTTP2/TLS blip in `dist host`
failed v0.11.31 publication twice (run 39880269962, intent-hq/intent#7639)
after every build had passed; the global `dist build` that runs just before it
is exposed to the same blip. scripts/configure-release-host-network.py applies
the fix to both steps; this keeps it in place across `dist generate`.
Requires PyYAML.

Run: python3 -B scripts/test-release-host-network.py
"""
import importlib.util
from pathlib import Path
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / '.github/workflows/v-release.yml'
spec = importlib.util.spec_from_file_location('configure', ROOT / 'scripts/configure-release-host-network.py')
configure = importlib.util.module_from_spec(spec)
spec.loader.exec_module(configure)

FETCH_RUN = 'python3 -I -B scripts/fetch-locked-crates.py'

# The dist jobs as cargo-dist generates them (before any local customization).
# build-local-artifacts also has a `- id: cargo-dist` step, which must stay online.
LOCAL = '''  build-local-artifacts:
    steps:
      - name: Build artifacts
        run: |
          dist build --print=linkage --output-format=json > dist-manifest.json
      - id: cargo-dist
        name: Post-build
        shell: bash
        run: |
          echo "paths<<EOF" >> "$GITHUB_OUTPUT"
'''
GLOBAL = '''  build-global-artifacts:
    needs:
      [plan, build-local-artifacts]
    steps:
      - name: Fetch local artifacts
        uses: actions/download-artifact@v8
      - id: cargo-dist
        shell: bash
        run: |
          dist build --output-format=json "--artifacts=global" > dist-manifest.json
          echo "dist ran successfully"
      - name: "Upload artifacts"
        uses: actions/upload-artifact@v7
'''
HOST = '''  # Determines if we should publish/announce
  host:
    steps:
      - name: Fetch artifacts
        uses: actions/download-artifact@v8
      - id: host
        shell: bash
        run: |
          dist host --steps=upload --steps=release --output-format=json > dist-manifest.json
'''
GENERATED = 'jobs:\n' + LOCAL + GLOBAL + HOST

# Every step allowed to set CARGO_NET_OFFLINE, as (job, step id).
OFFLINE_STEPS = {('host', 'host'), ('build-global-artifacts', 'cargo-dist')}


def jobs(text):
    return yaml.safe_load(text)['jobs']


def labels(steps):
    return [step.get('name') or step.get('id') for step in steps]


def step_index(steps, step_id):
    return next(i for i, step in enumerate(steps) if step.get('id') == step_id)


class ReleaseNetwork(unittest.TestCase):
    def assert_fetch_then_offline(self, steps, step_id, command):
        offline = step_index(steps, step_id)
        fetch = labels(steps).index(configure.FETCH_STEP_NAME)
        self.assertEqual(fetch + 1, offline, f'crates are fetched immediately before {step_id}')
        self.assertEqual(steps[fetch]['run'], FETCH_RUN)
        self.assertEqual(steps[offline].get('env', {}).get('CARGO_NET_OFFLINE'), 'true')
        self.assertIn(command, steps[offline]['run'])

    def test_committed_workflow_fetches_before_offline_publication(self):
        self.assert_fetch_then_offline(jobs(WORKFLOW.read_text())['host']['steps'], 'host', 'dist host')

    def test_committed_workflow_fetches_before_offline_global_build(self):
        steps = jobs(WORKFLOW.read_text())['build-global-artifacts']['steps']
        self.assert_fetch_then_offline(steps, 'cargo-dist', 'dist build')
        self.assertIn('--artifacts=global', steps[step_index(steps, 'cargo-dist')]['run'])

    def test_configure_applies_to_generated_template_and_is_idempotent(self):
        once = configure.configure(GENERATED)
        self.assertEqual(configure.configure(once), once)
        configured = jobs(once)
        self.assertEqual(labels(configured['host']['steps']),
                         ['Fetch artifacts', configure.FETCH_STEP_NAME, 'host'])
        self.assertEqual(configured['host']['steps'][2]['env'], {'CARGO_NET_OFFLINE': 'true'})
        self.assertEqual(labels(configured['build-global-artifacts']['steps']),
                         ['Fetch local artifacts', configure.FETCH_STEP_NAME, 'cargo-dist', 'Upload artifacts'])
        self.assertEqual(configured['build-global-artifacts']['steps'][2]['env'], {'CARGO_NET_OFFLINE': 'true'})
        self.assertEqual(configured['build-local-artifacts'], jobs(GENERATED)['build-local-artifacts'],
                         'the local build Post-build step is untouched')

    def test_committed_workflow_is_already_configured(self):
        text = WORKFLOW.read_text()
        self.assertEqual(configure.configure(text), text)

    def test_unexpected_host_template_is_rejected(self):
        with self.assertRaises(ValueError):
            configure.configure(GENERATED.replace('      - id: host\n', '      - id: publish\n'))
        with self.assertRaises(ValueError):
            configure.configure(GENERATED + HOST.split('steps:\n', 1)[1])

    def test_unexpected_global_template_is_rejected(self):
        missing_job = 'jobs:\n' + LOCAL + HOST
        missing_step = 'jobs:\n' + LOCAL + GLOBAL.replace('      - id: cargo-dist\n', '      - id: build\n') + HOST
        duplicate_step = 'jobs:\n' + LOCAL + GLOBAL + GLOBAL.split('steps:\n', 1)[1] + HOST
        for name, template in [('missing job', missing_job), ('missing step', missing_step),
                               ('duplicate step', duplicate_step)]:
            with self.subTest(name), self.assertRaises(ValueError):
                configure.configure(template)

    def test_existing_step_env_is_rejected_not_duplicated(self):
        env = '        env:\n          FOO: bar\n'
        run = '        shell: bash\n        run: |\n'
        global_end = '      - name: "Upload artifacts"\n'
        cases = {
            'host env first': HOST.replace(run, '        shell: bash\n' + env + '        run: |\n'),
            'host env last': HOST + env,
            'global env first': GLOBAL.replace(run, '        shell: bash\n' + env + '        run: |\n'),
            'global env last': GLOBAL.replace(global_end, env + global_end),
        }
        for name, edited in cases.items():
            job = HOST if name.startswith('host') else GLOBAL
            self.assertNotEqual(edited, job, name)
            with self.subTest(name), self.assertRaises(ValueError):
                configure.configure(GENERATED.replace(job, edited))

    def test_only_the_configured_dist_steps_go_offline(self):
        doc = yaml.safe_load(WORKFLOW.read_text())
        self.assertNotIn('CARGO_NET_OFFLINE', doc.get('env') or {}, 'workflow-level env')
        offline = set()
        for name, job in doc['jobs'].items():
            self.assertNotIn('CARGO_NET_OFFLINE', job.get('env') or {}, f'{name}: job-level env')
            for step in job.get('steps', []):
                if 'CARGO_NET_OFFLINE' in (step.get('env') or {}):
                    offline.add((name, step.get('id')))
        self.assertEqual(offline, OFFLINE_STEPS)


if __name__ == '__main__':
    unittest.main()
