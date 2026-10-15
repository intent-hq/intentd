#!/usr/bin/env python3
"""Reject workload cloud pins while allowing only named infrastructure exceptions."""
from pathlib import Path
import re
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[1]
# The probe must work when the fleet is offline; Depot smoke tests test Depot itself.
EXCEPTIONS = {('runner-route.yml', 'probe'), ('depot-cache-smoke.yml', 'populate'), ('depot-cache-smoke.yml', 'verify'), ('manual-signed-build.yml', 'smoke-macos')}
# Control jobs must remain independent of the queues they inspect or retire.
HOSTED_CONTROL = {('retire-obsolete-merge-runs.yml', 'cleanup'), ('retirement-credential-probe.yml', 'existing-read-credential')}
MAINTENANCE = {('m1mac-smoke.yml', 'smoke'), ('provision-m1mac.yml', 'provision'), ('provision-m1mac.yml', 'verify'), ('macos-prune.yml', 'prune'), ('tinybox-prune.yml', 'prune'), ('windows-prune.yml', 'prune')}


def violations(text, filename):
    errors = []
    for match in re.finditer(r'^  ([\w-]+):\s*\n(.*?)(?=^  [\w-]+:\s*$|\Z)', text.split('\njobs:\n', 1)[-1], re.M | re.S):
        job, body = match.groups()
        runner = re.search(r'^    runs-on: (.+)$', body, re.M)
        if not runner:
            continue
        if (filename, job) in HOSTED_CONTROL and runner[1] == 'ubuntu-latest' and 'matrix:' not in body:
            continue
        runner = runner[1].strip('"\'')
        if (filename, job) in EXCEPTIONS or (job == 'route' and 'id: decide' in body and 'listSelfHostedRunnersForOrg' in body):
            continue
        if runner.startswith('[self-hosted,') and (filename, job) in MAINTENANCE:
            continue
        # This reusable producer is placed by its routed callers, not a cloud
        # pin. No other job may accept arbitrary runner input this way.
        if (filename, job) == ('native-artifacts.yml', 'native') and runner == '${{ fromJSON(inputs.runner_labels) }}':
            continue
        routed = re.fullmatch(r"\$\{\{ fromJSON\(needs\.(route|runner-route)\.outputs\.(runners|linux_labels|check_labels).*\}\}", runner)
        if not routed and not (runner == '${{ matrix.os }}' and 'needs.route.outputs.' in body):
            errors.append(job)
    return errors


class RunnerPolicy(unittest.TestCase):
    def test_all_workloads_are_routed(self):
        for path in (ROOT / '.github/workflows').glob('*.yml'):
            with self.subTest(workflow=path.name):
                self.assertEqual(violations(path.read_text(), path.name), [])

    def test_policy_rejects_new_cloud_pins(self):
        for label in ['depot-ubuntu-24.04', 'ubuntu-latest', '${{ matrix.runner }}', '${{ needs.unrelated.outputs.runner }}']:
            self.assertEqual(violations(f'jobs:\n  new-work:\n    runs-on: {label}\n', 'new.yml'), ['new-work'])

    def test_control_exceptions_are_exact(self):
        for filename, job in HOSTED_CONTROL:
            for label in ['depot-ubuntu-24.04', '[ubuntu-latest]', '${{ matrix.os }}']:
                self.assertEqual(violations(f'jobs:\n  {job}:\n    runs-on: {label}\n', filename), [job])
            self.assertEqual(violations(f'jobs:\n  unexpected:\n    runs-on: ubuntu-latest\n', filename), ['unexpected'])
            self.assertEqual(violations(f'jobs:\n  {job}:\n    runs-on: ubuntu-latest\n', 'copy.yml'), [job])

    def test_routing_behavior(self):
        subprocess.run(['node', '--test', str(ROOT / 'scripts/runner-policy.test.cjs')], check=True)


if __name__ == '__main__':
    unittest.main()
