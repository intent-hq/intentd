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
# The intent-cache Linux slots are unprivileged containers (no sudo, no Docker
# socket). These jobs need sudo on Linux, so their Linux legs keep the hosted
# label while macOS and Windows legs stay routed.
HOSTED_LINUX = {
    ('release-cache.yml', 'daemon'): 'apt configuration and retrying apt-get shim',
    ('release-sitter.yml', 'build'): 'installs qemu-user-static for the aarch64 smoke test',
    ('v-release.yml', 'build-local-artifacts'): 'cargo-dist apt configuration and cross-build packages',
}
# Merge-queue-only heavy jobs that ci.yml's route `heavy` step always sends to Depot.
DEPOT_ONLY = {('ci.yml', 'coverage-all'): 'raises fs.inotify.max_user_instances with sudo sysctl'}
MAINTENANCE = {('m1mac-smoke.yml', 'smoke'), ('provision-m1mac.yml', 'provision'), ('provision-m1mac.yml', 'verify'), ('macos-prune.yml', 'prune'), ('windows-prune.yml', 'prune')}


def hosted_linux(key):
    return ("${{ (contains(%s, 'ubuntu') || contains(%s, 'linux')) && %s || "
            "fromJSON(needs.runner-route.outputs.runners || '{}')[%s] || %s }}") % ((key,) * 5)


def jobs(text):
    body = text.split('\njobs:\n', 1)[-1]
    return dict(m.groups() for m in re.finditer(r'^  ([\w-]+):\s*\n(.*?)(?=^  [\w-]+:\s*$|\Z)', body, re.M | re.S))


def privileged_steps(body):
    """Steps that call Docker, or sudo outside an `if !` / `command -v ... ||` probe.

    A probe only helps when the intent-cache image already has what it checks
    for (it ships libssl-dev and pkg-config); otherwise list the job above.
    """
    found = []
    code = '\n'.join(line for line in body.splitlines() if not line.lstrip().startswith('#'))
    for step in re.split(r'^      - |^    - ', code, flags=re.M)[1:]:
        guarded = re.search(r'^\s+if ! |command -v [\w-]+ >/dev/null \|\|', step, re.M)
        if re.search(r'\bdocker\b', step) or (re.search(r'\bsudo\b', step) and not guarded):
            found.append(step.splitlines()[0])
    return found


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
        routed = re.fullmatch(r"\$\{\{ fromJSON\(needs\.(route|runner-route)\.outputs\.(runners|linux_labels|check_labels).*\}\}", runner)
        if (filename, job) in HOSTED_LINUX:
            key = 'matrix.os' if 'matrix.os' in runner else 'matrix.runner'
            routed = runner == hosted_linux(key)
        if not routed and not (runner == '${{ matrix.os }}' and 'needs.route.outputs.' in body):
            errors.append(job)
    return errors


class RunnerPolicy(unittest.TestCase):
    def test_all_workloads_are_routed(self):
        for path in (ROOT / '.github/workflows').glob('*.yml'):
            with self.subTest(workflow=path.name):
                self.assertEqual(violations(path.read_text(), path.name), [])

    def test_routed_linux_jobs_run_unprivileged(self):
        # A routed Linux job may land on an intent-cache slot, which has no sudo or Docker.
        skip = set(HOSTED_LINUX) | set(DEPOT_ONLY) | MAINTENANCE | EXCEPTIONS | HOSTED_CONTROL
        for path in (ROOT / '.github/workflows').glob('*.yml'):
            for job, body in jobs(path.read_text()).items():
                if (path.name, job) in skip or not re.search(r'needs\.(route|runner-route)\.outputs', body):
                    continue
                with self.subTest(workflow=path.name, job=job):
                    self.assertEqual(privileged_steps(body), [])

    def test_privileged_exceptions_are_still_needed(self):
        for (name, job), reason in {**HOSTED_LINUX, **DEPOT_ONLY}.items():
            with self.subTest(workflow=name, job=job, reason=reason):
                body = jobs((ROOT / '.github/workflows' / name).read_text())[job]
                self.assertRegex(body, r'\bsudo\b', 'drop the exception once the job is unprivileged')
        self.assertEqual(violations("jobs:\n  build-local-artifacts:\n    runs-on: ${{ fromJSON(needs.runner-route.outputs.runners || '{}')[matrix.runner] || matrix.runner }}\n", 'v-release.yml'), ['build-local-artifacts'])
        heavy = (ROOT / '.github/workflows/ci.yml').read_text().split('      - id: heavy\n', 1)[1].split('\n  pr-static:', 1)[0]
        self.assertIn('if [[ "$EVENT" == merge_group ]]', heavy)

    def test_privileged_step_detection(self):
        self.assertEqual(privileged_steps('      - run: |\n          if ! pkg-config --exists openssl; then\n            sudo apt-get install -y libssl-dev\n          fi\n'), [])
        self.assertEqual(len(privileged_steps('      - run: sudo apt-get install -y bison\n')), 1)
        self.assertEqual(len(privileged_steps('    - run: sudo tee /etc/apt/x\n')), 1)
        self.assertEqual(len(privileged_steps('      - run: sudo -n docker run --rm ubuntu\n')), 1)

    def test_no_tinybox_routing(self):
        for path in [*(ROOT / '.github').rglob('*.yml'), *(ROOT / '.github').rglob('*.yaml')]:
            self.assertNotRegex(path.read_text(), r'(?i)tinybox', str(path))
        for name in ('.github/workflows/runner-route.yml', '.github/workflows/ci.yml'):
            self.assertIn("linux: ['self-hosted', 'linux', 'x64', 'intent-cache'],", (ROOT / name).read_text())

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
