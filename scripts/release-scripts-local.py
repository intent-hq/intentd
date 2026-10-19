#!/usr/bin/env python3
"""Run the test commands of ci.yml's release-scripts job locally.

The command list is read from the job itself (every `run:` line that invokes a
`scripts/test-*` file, leading VAR=... assignments dropped), so this cannot
drift from CI. Setup lines (pip install, installers, rustup, setup-sccache.py)
are not run. Suites that need CI-only tooling are skipped with
"SKIP <suite>: needs <X>":

- a `taiki-e/install-action` step makes its tool@version a requirement for every
  later step, as in CI (release-plz);
- a step that runs the cargo-dist installer needs `dist` at the
  dist-workspace.toml cargo-dist-version;
- HOST_RULES below name the suites that depend on the host itself. A rule whose
  suite is no longer in the job is a setup error, so the table cannot go stale.

Exit 0 when nothing failed (skips allowed), 1 when a suite failed, 2 on a setup
error. `--list` prints the plan without running anything.

Run: make test-release-scripts   (or python3 scripts/release-scripts-local.py)
"""
import argparse
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]
TEST_SCRIPT = re.compile(r'(?:^|[\s/])(?:\./)?scripts/(test-[\w.-]+)')
ASSIGNMENTS = re.compile(r'^(?:[A-Za-z_]\w*=(?:"[^"]*"|\'[^\']*\'|\S*)\s+)+')
VERSION = re.compile(r'(\d+(?:\.\d+)+)')


class SetupError(Exception):
    pass


def tool_version(command):
    """First dotted version printed by `command --version`, or None if absent."""
    if not shutil.which(command[0]):
        return None
    try:
        out = subprocess.run([*command, '--version'], capture_output=True, text=True, timeout=30).stdout
    except (OSError, subprocess.TimeoutExpired):
        return None
    match = VERSION.search(out)
    return match.group(1) if match else ''


def as_tuple(version):
    return tuple(int(part) for part in version.split('.'))


def exact_tool(name, version, command=None):
    """A requirement that `name` is on PATH, at `version` when one is pinned."""
    def check():
        found = tool_version(command or [name])
        if found is None:
            return 'not on PATH'
        if version and found != version:
            return f'found {found}'
        return None
    return (f'{name} {version}' if version else name), check


def sccache_rule(root):
    text = (root / 'scripts/setup-sccache.py').read_text()
    match = re.search(r'^VERSION = "([^"]+)"', text, re.M)
    if not match:
        raise SetupError('scripts/setup-sccache.py has no VERSION = "..." line')
    pinned = match.group(1)

    def check():
        binary = os.environ.get('SCCACHE_BIN') or 'sccache'
        found = tool_version([binary])
        if found is None:
            return f'{binary} not found'
        if not found or as_tuple(found) < as_tuple(pinned):
            return f'found {found or "unknown version"}'
        return None
    return f'sccache >= {pinned}', check


def preload_rule(root):
    preload = Path(os.environ.get('RELEASE_SCRIPTS_LOCAL_PRELOAD_FILE', '/etc/ld.so.preload'))

    def check():
        if os.environ.get('LD_PRELOAD'):
            return 'LD_PRELOAD is set'
        try:
            if preload.read_text().strip():
                return f'{preload} is not empty'
        except OSError:
            pass
        return None
    return 'a host without a global library preload, which writes files into TMPDIR', check


# Suites that depend on the host itself rather than on a tool CI installs.
HOST_RULES = {
    'test-release-cache-smoke.py': sccache_rule,
    'test-changed-tests-stdin.py': preload_rule,
}


def load_job(workflow, job):
    try:
        import yaml
    except ImportError:
        raise SetupError('PyYAML is required (CI pins it): python3 -m pip install PyYAML==6.0.2')
    doc = yaml.safe_load(workflow.read_text())
    steps = ((doc or {}).get('jobs') or {}).get(job, {}).get('steps')
    if not steps:
        raise SetupError(f'{workflow}: job {job!r} has no steps')
    return steps


def plan(root, workflow, job):
    """[(suite, command, [(need, check)])] in CI order."""
    entries = []
    carried = []
    for step in load_job(workflow, job):
        uses = step.get('uses', '')
        if uses.startswith('taiki-e/install-action@'):
            for tool in str((step.get('with') or {}).get('tool', '')).split(','):
                name, _, version = tool.strip().partition('@')
                if name:
                    carried.append(exact_tool(name, version))
            continue
        run = step.get('run')
        if not run:
            continue
        needs = list(carried)
        if 'cargo-dist-installer' in run:
            pinned = tomllib.loads((root / 'dist-workspace.toml').read_text())['dist']['cargo-dist-version']
            needs.append(exact_tool('dist', pinned))
        for line in run.replace('\\\n', ' ').splitlines():
            line = line.strip()
            if line.startswith('#'):
                continue
            match = TEST_SCRIPT.search(line)
            if not match:
                continue
            entries.append((match.group(1), ASSIGNMENTS.sub('', line), needs))
    if not entries:
        raise SetupError(f'{workflow}: job {job!r} runs no scripts/test-* commands')
    suites = {suite for suite, _, _ in entries}
    stale = sorted(set(HOST_RULES) - suites)
    if stale:
        raise SetupError(f'HOST_RULES names suites no longer in job {job!r}: {", ".join(stale)}; '
                         'remove them from scripts/release-scripts-local.py')
    return [(suite, command, needs + ([HOST_RULES[suite](root)] if suite in HOST_RULES else []))
            for suite, command, needs in entries]


def missing(needs):
    for need, check in needs:
        found = check()
        if found:
            return f'needs {need} ({found})'
    return None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--list', action='store_true', help='print the plan without running anything')
    parser.add_argument('--root', type=Path, default=ROOT, help=argparse.SUPPRESS)
    parser.add_argument('--job', default='release-scripts', help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    root = args.root.resolve()
    workflow = root / '.github/workflows/ci.yml'
    try:
        entries = plan(root, workflow, args.job)
    except SetupError as error:
        print(f'release-scripts-local: {error}', file=sys.stderr)
        return 2

    results = []
    for index, (suite, command, needs) in enumerate(entries, 1):
        reason = missing(needs)
        if reason:
            print(f'SKIP {suite}: {reason}', flush=True)
            results.append(('skip', suite, reason))
            continue
        if args.list:
            print(f'RUN  {suite}: {command}')
            continue
        print(f'==> [{index}/{len(entries)}] {command}', flush=True)
        status = subprocess.run(['bash', '-eo', 'pipefail', '-c', command], cwd=root).returncode
        results.append(('pass' if status == 0 else 'fail', suite, f'exit {status}'))
        print(f'{"PASS" if status == 0 else "FAIL"} {suite} (exit {status})', flush=True)
    if args.list:
        return 0

    failed = [r for r in results if r[0] == 'fail']
    skipped = [r for r in results if r[0] == 'skip']
    print(f'\nrelease-scripts: {len(results) - len(failed) - len(skipped)} passed, '
          f'{len(skipped)} skipped, {len(failed)} failed')
    for _, suite, reason in skipped:
        print(f'  skipped: {suite}: {reason}')
    for _, suite, reason in failed:
        print(f'  FAILED:  {suite} ({reason})')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
