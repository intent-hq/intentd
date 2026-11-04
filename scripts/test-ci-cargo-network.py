#!/usr/bin/env python3
"""CI crates.io contract: PR and merge-queue Cargo jobs survive a network blip.

PR jobs failed while downloading crates with "curl failed: [16] Error in the
HTTP2 framing layer" (derive_more in job 126375150115; serde_with in a `check`
run on intentd#3021), each forcing a full re-run with no code at fault. As in
release-plz.yml (scripts/test-release-plz-network.py), ci.yml turns off HTTP/2
multiplexing and raises cargo's own retries for every job, and the heavy PR
jobs download the locked crates with backoff before their first build.
Requires PyYAML.

Run: python3 -B scripts/test-ci-cargo-network.py
"""
from pathlib import Path
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / '.github/workflows/ci.yml'
RELEASE_PLZ = ROOT / '.github/workflows/release-plz.yml'
FETCH_RUN = 'python3 -I -B scripts/fetch-locked-crates.py'
NETWORK_ENV = {'CARGO_HTTP_MULTIPLEXING': 'false', 'CARGO_NET_RETRY': '10'}
# PR-reachable jobs that build the workspace; each fetches before its first build.
FETCH_JOBS = ('check', 'windows-cross-lint', 'macos-cross-lint', 'coverage-changed')
TOOLCHAIN = 'dtolnay/rust-toolchain@'


def load(path=WORKFLOW):
    return yaml.safe_load(path.read_text())


def runs_cargo(step):
    return 'cargo ' in (step.get('run') or '')


class CiCargoNetwork(unittest.TestCase):
    def setUp(self):
        self.doc = load()

    def test_workflow_env_disables_multiplexing_and_raises_retries(self):
        env = self.doc.get('env') or {}
        for key, value in NETWORK_ENV.items():
            self.assertEqual(env.get(key), value, f'workflow env {key} (quoted string)')

    def test_settings_match_release_plz(self):
        release = load(RELEASE_PLZ)
        step = next(s for s in release['jobs']['release-plz-pr']['steps'] if s.get('id') == 'release_pr')
        for key in NETWORK_ENV:
            self.assertEqual(str(step['env'][key]), self.doc['env'][key], key)

    def test_no_job_or_step_overrides_the_network_settings(self):
        for name, job in self.doc['jobs'].items():
            scopes = [(name, job.get('env') or {})]
            scopes += [(f'{name}: {s.get("name") or s.get("uses")}', s.get('env') or {}) for s in job.get('steps', [])]
            for where, env in scopes:
                for key in (*NETWORK_ENV, 'CARGO_NET_OFFLINE'):
                    self.assertNotIn(key, env, f'{where} overrides {key}')

    def test_heavy_pr_jobs_fetch_locked_crates_before_the_first_build(self):
        for name in FETCH_JOBS:
            with self.subTest(job=name):
                steps = self.doc['jobs'][name]['steps']
                fetches = [i for i, s in enumerate(steps) if s.get('run') == FETCH_RUN]
                self.assertEqual(len(fetches), 1, f'{name}: one retrying crate fetch')
                fetch = fetches[0]
                toolchain = next(i for i, s in enumerate(steps) if s.get('uses', '').startswith(TOOLCHAIN))
                self.assertLess(toolchain, fetch, f'{name}: the pinned toolchain is installed before fetching')
                # Steps before the toolchain only plan (coverage-changed prints its cargo commands).
                first_cargo = next(i for i, s in enumerate(steps) if i > toolchain and i != fetch and runs_cargo(s))
                self.assertLess(fetch, first_cargo, f'{name}: fetch precedes {steps[first_cargo].get("name")!r}')
                self.assertEqual(steps[fetch].get('if'), steps[toolchain].get('if'),
                                 f'{name}: fetch runs exactly when the toolchain is installed')
                self.assertNotIn('continue-on-error', steps[fetch], f'{name}: a failed fetch fails the job')

    def test_fetch_script_documents_this_caller(self):
        text = (ROOT / 'scripts/fetch-locked-crates.py').read_text()
        self.assertIn('ci.yml', text)
        for name in FETCH_JOBS:
            self.assertIn(f'`{name}`', text)

    def test_this_suite_runs_in_release_scripts(self):
        steps = self.doc['jobs']['release-scripts']['steps']
        self.assertIn('python3 -B scripts/test-ci-cargo-network.py', yaml.safe_dump(steps, width=10_000))


if __name__ == '__main__':
    unittest.main()
