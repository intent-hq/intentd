#!/usr/bin/env python3
"""Release `host` job contract: fetch crates with retries, then publish offline.

`dist host` runs `cargo metadata`, which on a fresh runner downloads the whole
dependency graph. A crates.io HTTP2/TLS blip there failed v0.11.31 publication
twice (run 39880269962, intent-hq/intent#7639) after every build had passed.
scripts/configure-release-host-network.py applies the fix; this keeps it in
place across `dist generate`. Requires PyYAML.

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

# The host step as cargo-dist generates it (before any local customization).
GENERATED = '''jobs:
  host:
    steps:
      - name: Fetch artifacts
        uses: actions/download-artifact@v8
      - id: host
        shell: bash
        run: |
          dist host --steps=upload --steps=release --output-format=json > dist-manifest.json
'''


def host_steps(text):
    return yaml.safe_load(text)['jobs']['host']['steps']


class HostNetwork(unittest.TestCase):
    def test_committed_workflow_fetches_before_offline_publication(self):
        steps = host_steps(WORKFLOW.read_text())
        names = [step.get('name') or step.get('id') for step in steps]
        fetch = names.index(configure.FETCH_STEP_NAME)
        host = next(i for i, step in enumerate(steps) if step.get('id') == 'host')
        self.assertEqual(fetch + 1, host, 'crates are fetched immediately before dist host')
        self.assertEqual(steps[fetch]['run'], 'python3 -I -B scripts/fetch-locked-crates.py')
        self.assertEqual(steps[host].get('env', {}).get('CARGO_NET_OFFLINE'), 'true')
        self.assertIn('dist host', steps[host]['run'])

    def test_configure_applies_to_generated_template_and_is_idempotent(self):
        once = configure.configure(GENERATED)
        self.assertEqual(configure.configure(once), once)
        steps = host_steps(once)
        self.assertEqual([s.get('name') or s.get('id') for s in steps],
                         ['Fetch artifacts', configure.FETCH_STEP_NAME, 'host'])
        self.assertEqual(steps[2]['env'], {'CARGO_NET_OFFLINE': 'true'})

    def test_committed_workflow_is_already_configured(self):
        text = WORKFLOW.read_text()
        self.assertEqual(configure.configure(text), text)

    def test_unexpected_template_is_rejected(self):
        with self.assertRaises(ValueError):
            configure.configure(GENERATED.replace('      - id: host\n', '      - id: publish\n'))
        with self.assertRaises(ValueError):
            configure.configure(GENERATED + GENERATED.split('steps:\n', 1)[1])

    def test_existing_host_step_env_is_rejected_not_duplicated(self):
        for env_first in (True, False):
            run = '        run: |\n          dist host\n'
            env = '        env:\n          FOO: bar\n'
            body = env + run if env_first else run + env
            template = GENERATED.split('      - id: host\n')[0] + '      - id: host\n        shell: bash\n' + body
            with self.assertRaises(ValueError):
                configure.configure(template)

    def test_only_the_host_publication_step_goes_offline(self):
        doc = yaml.safe_load(WORKFLOW.read_text())
        self.assertNotIn('CARGO_NET_OFFLINE', doc.get('env') or {}, 'workflow-level env')
        for name, job in doc['jobs'].items():
            self.assertNotIn('CARGO_NET_OFFLINE', job.get('env') or {}, f'{name}: job-level env')
            for step in job.get('steps', []):
                if name == 'host' and step.get('id') == 'host':
                    continue
                self.assertNotIn('CARGO_NET_OFFLINE', step.get('env') or {}, f'{name}: {step}')


if __name__ == '__main__':
    unittest.main()
