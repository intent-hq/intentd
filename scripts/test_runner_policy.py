#!/usr/bin/env python3
"""Reject paid cloud pins while allowing free standard labels and named infrastructure exceptions."""
from pathlib import Path
import json
import re
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[1]
# Standard GitHub-hosted runners are free for public repositories, so jobs may pin
# them directly; the route never sends them to a self-hosted slot or WarpBuild.
FREE = {'ubuntu-22.04', 'ubuntu-24.04', 'ubuntu-latest', 'ubuntu-24.04-arm', 'windows-2022', 'windows-2025'}
# The probe must work when the fleet is offline; the cache smoke test needs a fresh hosted machine.
EXCEPTIONS = {('runner-route.yml', 'probe'), ('cache-smoke.yml', 'populate'), ('cache-smoke.yml', 'verify'), ('manual-signed-build.yml', 'smoke-macos')}
# Control jobs must remain independent of the queues they inspect or retire.
HOSTED_CONTROL = {('retire-obsolete-merge-runs.yml', 'cleanup'), ('retirement-credential-probe.yml', 'existing-read-credential')}
# The intent-cache Linux slots are unprivileged containers (no sudo, no Docker
# socket). These jobs need sudo on Linux, so their Linux legs take the burst
# runner (`cloud`: WarpBuild or GitHub) while macOS and Windows legs stay routed.
HOSTED_LINUX = {
    ('release-cache.yml', 'daemon'): 'apt configuration and retrying apt-get shim',
    ('release-sitter.yml', 'build'): 'installs qemu-user-static for the aarch64 smoke test',
    ('v-release.yml', 'build-local-artifacts'): 'cargo-dist apt configuration and cross-build packages',
}
# Merge-queue-only heavy jobs that ci.yml's route `heavy` step always sends to their burst runner.
HEAVY_HOSTED_ONLY = {('ci.yml', 'coverage-all'): 'raises fs.inotify.max_user_instances with sudo sysctl'}
MAINTENANCE = {('m1mac-smoke.yml', 'smoke'), ('provision-m1mac.yml', 'provision'), ('provision-m1mac.yml', 'verify'), ('macos-prune.yml', 'prune'), ('windows-prune.yml', 'prune')}


def hosted_linux(key):
    return ("${{ (contains(%s, 'ubuntu') || contains(%s, 'linux')) && (fromJSON(needs.runner-route.outputs.cloud || '{}')[%s] || %s) || "
            "fromJSON(needs.runner-route.outputs.runners || '{}')[%s] || %s }}") % ((key,) * 6)


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
        if runner in FREE or (filename, job) in EXCEPTIONS or (job == 'route' and 'id: decide' in body and 'listSelfHostedRunnersForOrg' in body):
            continue
        if runner.startswith('[self-hosted,') and (filename, job) in MAINTENANCE:
            continue
        routed = re.fullmatch(r"\$\{\{ fromJSON\(needs\.(route|runner-route)\.outputs\.(runners|slots|cloud|linux_labels|check_labels).*\}\}", runner)
        # Matrix legs that may take a pool read their own slot first.
        routed = routed or re.fullmatch(r"\$\{\{ [^|]+ && fromJSON\(needs\.(route|runner-route)\.outputs\.slots \|\| '\{\}'\)\[format\('[\w-]+-\{0\}', matrix\.\w+\)\] \|\| fromJSON\(needs\.(route|runner-route)\.outputs\.runners \|\| '\{\}'\)\[matrix\.(\w+)\] \|\| matrix\.\3 \}\}", runner)
        if (filename, job) in HOSTED_LINUX:
            key = 'matrix.os' if 'matrix.os' in runner else 'matrix.runner'
            routed = runner == hosted_linux(key)
        if not routed and not (runner == '${{ matrix.os }}' and 'needs.route.outputs.' in body):
            errors.append(job)
    return errors


# Free labels may still be listed; the route always gives them their GitHub label.
POOLED = {'gh-linux-8x', 'gh-linux-16x', 'ubuntu-22.04', 'ubuntu-24.04'}
SLOT_READ = re.compile(r"outputs\.slots \|\| '\{\}'\)\[(?:'([\w-]+)'|format\('([\w-]+)-\{0\}', matrix\.\w+\))\] \|\| (?:fromJSON\('\[\"([\w.-]+)\"\]'\)|fromJSON\(needs\.[\w-]+\.outputs\.runners \|\| '\{\}'\)\[matrix\.(\w+)\])")


def slot_list(text):
    """The workflow's slot_jobs / SLOT_JOBS list, or None when it lists no jobs."""
    match = re.search(r'^( +)(?:slot_jobs|SLOT_JOBS): >-\n((?:\1 +[^\n]*\n)+)', text, re.M)
    return json.loads(match[2]) if match else None


def slot_violations(text):
    """Listed jobs and the jobs reading outputs.slots must match one to one."""
    listed = slot_list(text) or []
    errors, used = [], set()
    by_key = {entry['key']: entry for entry in listed}
    if len(by_key) != len(listed):
        errors.append('duplicate slot key')
    for entry in listed:
        if entry.get('label') not in POOLED:
            errors.append(f"{entry['key']}: {entry.get('label')} is not an intent-cache pool label")
    for job, body in jobs(text).items():
        runner = re.search(r'^    runs-on: (.+)$', body, re.M)
        # A matrix row may read its own slot in its `os` value (runs-on: matrix.os).
        rows = re.findall(r'^ +(?:- )?os: (\$\{\{ .*outputs\.slots.*)$', body, re.M) if runner and runner[1] == '${{ matrix.os }}' else []
        for row in rows:
            read = SLOT_READ.search(row)
            entry = by_key.get(read and read[1])
            if not entry:
                errors.append(f'{job}: reads an unlisted slot')
                continue
            used.add(entry['key'])
            if entry['label'] != read[3]:
                errors.append(f"{job}: fallback {read[3]} differs from listed {entry['label']}")
            only = re.search(r"^    if: github\.event_name == '(\w+)'$", body, re.M)
            if only and entry.get('events') != [only[1]]:
                errors.append(f'{job}: runs only on {only[1]} but lists events {entry.get("events")}')
        if not runner or 'outputs.slots' not in runner[1]:
            continue
        read = SLOT_READ.search(runner[1])
        key = read and (read[1] or read[2])
        entry = by_key.get(key)
        if not entry:
            errors.append(f'{job}: reads an unlisted slot')
            continue
        used.add(key)
        if read[3] and entry['label'] != read[3]:
            errors.append(f"{job}: fallback {read[3]} differs from listed {entry['label']}")
        if read[4] and entry['label'] not in body:
            errors.append(f"{job}: no matrix leg asks for the listed {entry['label']}")
        if bool(read[2]) != bool(entry.get('shards') or entry.get('legs')):
            errors.append(f'{job}: matrix legs and the slot list disagree')
        only = re.search(r"^    if: github\.event_name == '(\w+)'$", body, re.M)
        if only and entry.get('events') != [only[1]]:
            errors.append(f'{job}: runs only on {only[1]} but lists events {entry.get("events")}')
    errors += [f'{key}: listed but no job reads it' for key in by_key.keys() - used]
    return errors


POOL_OF = {'gh-linux-8x': 'heavy', 'gh-linux-16x': 'heavy'}
# Merge-queue-only heavy jobs that ci.yml's route `heavy` step always sends to their burst runner.
QUEUE_HOSTED = {('ci.yml', 'coverage-e2e'), ('ci.yml', 'coverage-all')}

def pool_reads(body):
    """(pool, several legs?) for a job reading a pool-level mapping, else None."""
    runner = re.search(r'^    runs-on: (.+)$', body, re.M)
    runner = runner[1] if runner else ''
    legs = '    strategy:' in body
    fixed = re.search(r"outputs\.runners \|\| '\{\}'\)\['([\w.-]+)'\]", runner)
    if fixed:
        return (POOL_OF[fixed[1]], legs) if fixed[1] in POOL_OF else None
    if re.search(r'outputs\.(linux_labels|check_labels)', runner):
        return ('heavy', legs)
    by_leg = re.search(r"outputs\.runners \|\| '\{\}'\)\[matrix\.(\w+)\]", runner)
    if by_leg and 'outputs.slots' not in runner and not re.search(r"contains\(matrix\.\w+, 'linux'\)\) && \(fromJSON\(needs\.runner-route\.outputs\.cloud", runner):
        labels = [label for label in POOL_OF if label in body]
        return (POOL_OF[labels[0]], True) if labels else None
    return None


def parallel_pool_violations(text, filename):
    """Pooled jobs that can run together must take per-job slots, not the shared mapping."""
    found = jobs(text)
    needs = {}
    for job, body in found.items():
        # `needs: a`, `needs: [a, b]` (possibly on the next line), or a `- a` list.
        match = re.search(r'^    needs:\s*(?:\[([^\]]*)\]|(\S+)$|((?:\n      - [\w-]+)+))', body, re.M)
        names = (match[1] or match[2] or match[3]) if match else ''
        needs[job] = {n.strip(' -\n') for n in re.split(r',|\n', names) if n.strip(' -\n')}
    def after(job, other, seen=()):
        return other in needs[job] or any(after(n, other, seen + (job,)) for n in needs[job] if n in needs and n not in seen)
    readers = {}
    for job, body in found.items():
        read = pool_reads(body)
        if read and (filename, job) not in QUEUE_HOSTED:
            readers.setdefault(read[0], []).append((job, read[1]))
    errors = []
    for pool, members in readers.items():
        for i, (job, legs) in enumerate(members):
            if legs:
                errors.append(f'{job}: several matrix legs share the {pool} pool mapping')
            for other, _ in members[i + 1:]:
                if not after(job, other) and not after(other, job):
                    errors.append(f'{job} and {other} can run together on the {pool} pool mapping')
    return errors


class RunnerPolicy(unittest.TestCase):
    def test_parallel_pooled_jobs_take_per_job_slots(self):
        for path in (ROOT / '.github/workflows').glob('*.yml'):
            with self.subTest(workflow=path.name):
                self.assertEqual(parallel_pool_violations(path.read_text(), path.name), [])

    def test_parallel_pool_detection(self):
        read = """    needs: [{}]\n    runs-on: ${{{{ fromJSON(needs.route.outputs.runners || '{{}}')['gh-linux-16x'] || fromJSON('["gh-linux-16x"]') }}}}\n"""
        two = 'jobs:\n  a:\n' + read.format('route') + '  b:\n' + read.format('route')
        self.assertEqual(len(parallel_pool_violations(two, 'x.yml')), 1)
        self.assertEqual(parallel_pool_violations('jobs:\n  a:\n' + read.format('route') + '  b:\n' + read.format('route, a'), 'x.yml'), [])
        legs = 'jobs:\n  a:\n    strategy:\n      matrix:\n        os: [gh-linux-8x]\n        node: [22, 24]\n    runs-on: ${{ fromJSON(needs.route.outputs.runners || \'{}\')[matrix.os] || matrix.os }}\n'
        self.assertEqual(parallel_pool_violations(legs, 'x.yml'), ['a: several matrix legs share the heavy pool mapping'])

    def test_slot_lists_match_their_jobs(self):
        for path in (ROOT / '.github/workflows').glob('*.yml'):
            with self.subTest(workflow=path.name):
                self.assertEqual(slot_violations(path.read_text()), [])

    def test_slot_list_checks(self):
        job = "  a:\n    if: github.event_name == 'merge_group'\n    runs-on: ${{ fromJSON(needs.route.outputs.slots || '{}')['a'] || fromJSON('[\"ubuntu-24.04\"]') }}\n"
        listing = '    with:\n      slot_jobs: >-\n        [{{"key":"a","label":"{}"{}}}]\njobs:\n'
        self.assertEqual(slot_violations(listing.format('ubuntu-24.04', ',"events":["merge_group"]') + job), [])
        self.assertEqual(len(slot_violations(listing.format('ubuntu-24.04', '') + job)), 1)
        self.assertEqual(len(slot_violations(listing.format('gh-linux-8x', ',"events":["merge_group"]') + job)), 1)
        self.assertEqual(len(slot_violations(listing.format('windows-2025', ',"events":["merge_group"]') + job)), 2)
        self.assertEqual(slot_violations('jobs:\n' + job), ['a: reads an unlisted slot'])
        row = "  b:\n    strategy:\n      matrix:\n        include:\n          - os: ${{ fromJSON(needs.route.outputs.slots || '{}')['b-linux'] || fromJSON('[\"gh-linux-16x\"]') }}\n    runs-on: ${{ matrix.os }}\n"
        rows = '    with:\n      slot_jobs: >-\n        [{{"key":"b-linux","label":"{}"}}]\njobs:\n'
        self.assertEqual(slot_violations(rows.format('gh-linux-16x') + row), [])
        self.assertEqual(slot_violations(rows.format('gh-linux-8x') + row), ['b: fallback gh-linux-16x differs from listed gh-linux-8x'])
        self.assertEqual(slot_violations('jobs:\n' + row), ['b: reads an unlisted slot'])
        # A queue-only matrix row must list only that event, or it holds a slot on other events.
        queued = row.replace('    strategy:', "    if: github.event_name == 'merge_group'\n    strategy:", 1)
        events = '    with:\n      slot_jobs: >-\n        [{{"key":"b-linux","label":"gh-linux-16x"{}}}]\njobs:\n'
        self.assertEqual(slot_violations(events.format(',"events":["merge_group"]') + queued), [])
        self.assertEqual(slot_violations(events.format('') + queued), ['b: runs only on merge_group but lists events None'])

    def test_all_workloads_are_routed(self):
        for path in (ROOT / '.github/workflows').glob('*.yml'):
            with self.subTest(workflow=path.name):
                self.assertEqual(violations(path.read_text(), path.name), [])

    def test_queue_heavy_jobs_take_their_own_slots(self):
        # The Linux daemon-lifecycle leg runs only in the merge queue; it is
        # listed for merge_group so a free xl or heavy slot can take it, instead
        # of the `heavy` step's always-hosted mapping. The coverage jobs stay
        # hosted until the slot hosts raise their shared inotify limit.
        ci = (ROOT / '.github/workflows/ci.yml').read_text()
        listed = {entry['key']: entry for entry in slot_list(ci)}
        self.assertNotIn('coverage-e2e', listed)
        self.assertEqual(listed['daemon-lifecycle-linux'], {'key': 'daemon-lifecycle-linux', 'label': 'gh-linux-16x', 'rust': True, 'minutes': 4, 'events': ['merge_group']})
        body = jobs(ci)
        self.assertIn('outputs.linux_labels', body['coverage-e2e'])
        self.assertIn("os: ${{ fromJSON(needs.route.outputs.slots || '{}')['daemon-lifecycle-linux'] || fromJSON('[\"gh-linux-16x\"]') }}", body['daemon-lifecycle'])
        self.assertIn('outputs.linux_labels', body['coverage-all'])

    def test_routed_linux_jobs_run_unprivileged(self):
        # A routed Linux job may land on an intent-cache slot, which has no sudo or Docker.
        skip = set(HOSTED_LINUX) | set(HEAVY_HOSTED_ONLY) | MAINTENANCE | EXCEPTIONS | HOSTED_CONTROL
        for path in (ROOT / '.github/workflows').glob('*.yml'):
            for job, body in jobs(path.read_text()).items():
                if (path.name, job) in skip or not re.search(r'needs\.(route|runner-route)\.outputs', body):
                    continue
                with self.subTest(workflow=path.name, job=job):
                    self.assertEqual(privileged_steps(body), [])

    def test_privileged_exceptions_are_still_needed(self):
        for (name, job), reason in {**HOSTED_LINUX, **HEAVY_HOSTED_ONLY}.items():
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

    def test_pool_limits_and_xl_fleet(self):
        # Pool sizes are org/repo variables, so adding hosts needs no workflow change;
        # the defaults keep today's routing (no xl slots).
        route = (ROOT / '.github/workflows/runner-route.yml').read_text()
        for env, var, default in [('THRESHOLD', 'CI_LINUX_HEAVY_SLOTS', 2), ('XL_THRESHOLD', 'CI_LINUX_XL_SLOTS', 0)]:
            self.assertIn(f"          {env}: ${{{{ vars.{var} || '{default}' }}}}\n", route)
        self.assertIn("xl: ['self-hosted', 'linux', 'x64', 'intent-cache-xl'],", route)
        # The Linux kill switch covers xl, and only 16-core jobs may take an xl slot.
        self.assertIn("xl: process.env.LINUX_RUNNER_DISABLED === 'true',", route)
        self.assertIn("xl: ['gh-linux-16x'],", route)
        self.assertLess(route.index("xl: ['gh-linux-16x'],"), route.index("linux: ['gh-linux-8x', 'gh-linux-16x'],"))
        ci = (ROOT / '.github/workflows/ci.yml').read_text()
        self.assertIn("  LINUX_BURST_THRESHOLD: ${{ vars.CI_LINUX_HEAVY_SLOTS || '2' }}\n", ci)
        self.assertIn("  LINUX_XL_BURST_THRESHOLD: ${{ vars.CI_LINUX_XL_SLOTS || '0' }}\n", ci)
        self.assertNotIn('SMALL', ci)
        self.assertIn('          WARPBUILD_DISABLED: ${{ vars.CI_WARPBUILD_DISABLED }}\n', ci)
        self.assertIn('          XL_THRESHOLD: ${{ env.LINUX_XL_BURST_THRESHOLD }}\n', ci)

    def test_policy_rejects_new_paid_pins(self):
        for label in ['gh-linux-8x', 'gh-linux-16x', 'gh-windows-16x', 'macos-26-xlarge', 'macos-15-xlarge',
                      '${{ matrix.runner }}', '${{ needs.unrelated.outputs.runner }}']:
            self.assertEqual(violations(f'jobs:\n  new-work:\n    runs-on: {label}\n', 'new.yml'), ['new-work'])
        # Free standard labels and the cloud mapping are allowed.
        for label in sorted(FREE) + ["${{ fromJSON(needs.runner-route.outputs.cloud || '{}')['gh-linux-8x'] || fromJSON('[\"gh-linux-8x\"]') }}"]:
            self.assertEqual(violations(f'jobs:\n  new-work:\n    runs-on: {label}\n', 'new.yml'), [])

    def test_route_knows_the_free_labels(self):
        route = (ROOT / '.github/workflows/runner-route.yml').read_text()
        listed = re.search(r"const free = \[([^\]]*)\];", route)[1]
        self.assertEqual({label.strip(" '") for label in listed.split(',')}, FREE)

    def test_control_exceptions_are_exact(self):
        for filename, job in HOSTED_CONTROL:
            # A control job stays on one free GitHub label, never a matrix or a list.
            for label in ['[ubuntu-latest]', '${{ matrix.os }}']:
                self.assertEqual(violations(f'jobs:\n  {job}:\n    runs-on: {label}\n', filename), [job])
            self.assertEqual(violations(f'jobs:\n  {job}:\n    runs-on: ubuntu-latest\n', filename), [])

    def test_routing_behavior(self):
        subprocess.run(['node', '--test', str(ROOT / 'scripts/runner-policy.test.cjs')], check=True)


if __name__ == '__main__':
    unittest.main()
