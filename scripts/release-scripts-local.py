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
  suite is no longer in the job is a setup error, so the table cannot go stale;
- a step whose `if:`, `env:` or shell needs CI context is skipped.

Each command runs as CI runs it: the step's shell flags (`shell: bash` adds
pipefail), its working-directory, and its literal job/step env. A test step that
mixes test lines with shell the runner cannot reproduce line by line (loops,
conditionals, cd, ...) is a setup error rather than a silent divergence.

Exit 0 when nothing failed (skips allowed, but a skipped suite is unverified), 1
when a suite failed, 2 on a setup error. `--list` prints the plan without
running any suite.

Run from packages/intentd: make test-release-scripts
(monorepo root: make -C packages/intentd test-release-scripts)
"""
import argparse
import functools
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
TEST_SCRIPT = re.compile(r'(?:^|[\s/])(?:\./)?scripts/(test-[\w.-]+)')
ASSIGNMENT = re.compile(r'^([A-Za-z_]\w*)=("[^"]*"|\'[^\']*\'|\S*)\s+')
VARIABLE = re.compile(r'\$\{?([A-Za-z_]\w*)')
TEST_COMMAND = re.compile(r'^(?:python3(?:\s+-[A-Za-z]+)*\s+)?(?:\.{1,2}/)*scripts/test-[\w.-]+(?:\s+[^;&|<>`$()]*)?$')
# Non-test lines a test step may carry: CI-only setup the runner leaves out.
SETUP_LINE = re.compile(r'^(?:[A-Za-z_]\w*=|python3 -m pip install\s|curl\s|rustup\s|'
                        r'python3 scripts/setup-|echo\s.*>>\s*"?\$GITHUB_(?:ENV|PATH)"?$)')
VERSION = re.compile(r'(\d+(?:\.\d+)+)')


class SetupError(Exception):
    pass


@functools.cache
def tool_version(command):
    """First dotted version printed by `command --version`, '' when it prints none,
    or None when the command is absent or does not answer."""
    if not shutil.which(command):
        return None
    try:
        out = subprocess.run([command, '--version'], capture_output=True, text=True, timeout=30).stdout
    except (OSError, subprocess.TimeoutExpired):
        return None
    match = VERSION.search(out)
    return match.group(1) if match else ''


def as_tuple(version):
    return tuple(int(part) for part in version.split('.'))


def exact_tool(name, version):
    """A requirement that `name` is on PATH, at `version` when one is pinned."""
    def check():
        found = tool_version(name)
        if found is None:
            return 'not on PATH'
        if version and found != version:
            return f'found {found or "unknown version"}'
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
        found = tool_version(binary)
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
    try:
        doc = yaml.safe_load(workflow.read_text())
    except (OSError, yaml.YAMLError) as error:
        raise SetupError(f'cannot read {workflow}: {error}')
    found = ((doc or {}).get('jobs') or {}).get(job) or {}
    if not found.get('steps'):
        raise SetupError(f'{workflow}: job {job!r} has no steps')
    return found, found['steps']


def ci_context(reason):
    return 'CI context', lambda: reason


def dist_pin(root):
    try:
        import tomllib
        return tomllib.loads((root / 'dist-workspace.toml').read_text())['dist']['cargo-dist-version']
    except (OSError, KeyError, TypeError, ValueError) as error:
        raise SetupError(f'cannot read [dist].cargo-dist-version from dist-workspace.toml: {error!r}')


def env_needs(env, where):
    """Literal env values to apply, plus a CI-context need for any expression."""
    literal, needs = {}, []
    for key, value in (env or {}).items():
        if '${{' in str(value):
            needs.append(ci_context(f'{where} env {key} is a CI expression'))
        else:
            literal[str(key)] = str(value)
    return literal, needs


def test_command(line, local, name):
    """The command to run for a test line, CI-only VAR=$local prefixes dropped."""
    command = line
    kept = []
    while match := ASSIGNMENT.match(command):
        if not set(VARIABLE.findall(match.group(2))) & local:
            kept.append(match.group(0))
        command = command[match.end():]
    if not TEST_COMMAND.match(command):
        raise SetupError(f'step {name!r}: cannot run {line!r} on its own; '
                         'teach scripts/release-scripts-local.py this shape')
    return ''.join(kept) + command


def plan(root, workflow, job):
    """[{suite, command, needs, shell, cwd, env}] in CI order."""
    doc_job, steps = load_job(workflow, job)
    job_env, job_needs = env_needs(doc_job.get('env'), 'job')
    job_shell = ((doc_job.get('defaults') or {}).get('run') or {}).get('shell')
    entries = []
    carried = []
    for step in steps:
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
        name = step.get('name') or run.splitlines()[0]
        lines = [line.strip() for line in run.replace('\\\n', ' ').splitlines()]
        lines = [line for line in lines if line and not line.startswith('#')]
        if not any('scripts/test-' in line for line in lines):
            continue
        step_env, needs = env_needs(step.get('env'), 'step')
        needs = job_needs + needs + carried
        if 'if' in step:
            needs.append(ci_context(f'step runs only if: {step["if"]}'))
        shell = step.get('shell', job_shell)
        if shell not in (None, 'bash'):
            needs.append(ci_context(f'step runs under shell {shell!r}'))
        if 'cargo-dist-installer' in run:
            needs.append(exact_tool('dist', dist_pin(root)))
        cwd = root / str(step.get('working-directory', '.'))
        local = set()
        for line in lines:
            if 'scripts/test-' not in line:
                if not SETUP_LINE.match(line):
                    raise SetupError(f'step {name!r}: {line!r} is shell the runner cannot reproduce '
                                     'line by line; teach scripts/release-scripts-local.py this shape')
                if assigned := re.match(r'^([A-Za-z_]\w*)=', line):
                    local.add(assigned.group(1))
                continue
            match = TEST_SCRIPT.search(line)
            line_needs = list(needs)
            if '${{' in line and match:
                line_needs.append(ci_context('the command uses a CI expression'))
                command = line
            else:
                command = test_command(line, local, name)
            entries.append({'suite': match.group(1), 'command': command,
                            'needs': line_needs, 'shell': shell, 'cwd': cwd,
                            'env': {**job_env, **step_env}})
    if not entries:
        raise SetupError(f'{workflow}: job {job!r} runs no scripts/test-* commands')
    suites = {entry['suite'] for entry in entries}
    stale = sorted(set(HOST_RULES) - suites)
    if stale:
        raise SetupError(f'HOST_RULES names suites no longer in job {job!r}: {", ".join(stale)}; '
                         'remove them from scripts/release-scripts-local.py')
    for entry in entries:
        if entry['suite'] in HOST_RULES:
            entry['needs'] = entry['needs'] + [HOST_RULES[entry['suite']](root)]
    return entries


def missing(needs):
    for need, check in needs:
        found = check()
        if found:
            return f'needs {need} ({found})'
    return None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--list', action='store_true', help='print the plan without running any suite')
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
    for index, entry in enumerate(entries, 1):
        suite, command = entry['suite'], entry['command']
        reason = missing(entry['needs'])
        if reason:
            print(f'SKIP {suite}: {reason}', flush=True)
            results.append(('skip', suite, reason))
            continue
        if args.list:
            print(f'RUN  {suite}: {command}')
            continue
        print(f'==> [{index}/{len(entries)}] {command}', flush=True)
        # GitHub's default `bash -e {0}`; `shell: bash` is `bash --noprofile --norc -eo pipefail {0}`.
        shell = ['bash', '--noprofile', '--norc', '-eo', 'pipefail'] if entry['shell'] == 'bash' else ['bash', '-e']
        status = subprocess.run([*shell, '-c', command], cwd=entry['cwd'],
                                env={**os.environ, **entry['env']}).returncode
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
    if skipped:
        print('  Skipped suites were not checked; CI still runs them.')
    for _, suite, reason in failed:
        print(f'  FAILED:  {suite} ({reason})')
    return 1 if failed else 0


if __name__ == '__main__':
    if sys.version_info < (3, 11):
        print('release-scripts-local: Python 3.11+ is required (CI uses 3.11)', file=sys.stderr)
        sys.exit(2)
    sys.exit(main())
