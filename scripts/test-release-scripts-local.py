#!/usr/bin/env python3
"""Offline tests for scripts/release-scripts-local.py.

Each case builds a fixture repo (ci.yml, stub test scripts, pins) and a bin dir
holding only bash, python3 and stub tools, then runs the real runner against it.

Run: python3 -I -B scripts/test-release-scripts-local.py   (needs PyYAML)
"""
import os
from pathlib import Path
import shutil
import site
import subprocess
import sys
import tempfile
import textwrap
import unittest
import unittest.mock

LOADER_PATH = ('LD_LIBRARY_PATH', 'DYLD_LIBRARY_PATH', 'DYLD_FALLBACK_LIBRARY_PATH')
# Where the caller's PyYAML may live besides the system site.
IMPORT_PATH = ('PYTHONPATH',)


def child_env(bin_dir, home, preload, **extra_env):
    """Environment for a child runner: stub-only PATH, fixture HOME.

    The loader path is forwarded so a toolcache python3 still starts
    (intentd#2490). The fixture HOME would move Python's user site, so the
    caller's user base is pinned and PYTHONPATH forwarded: a PyYAML installed
    with `pip install --user` (or reached through PYTHONPATH) must stay
    importable for the runner. `extra_env` overrides any of these.
    """
    environ = {key: os.environ[key] for key in LOADER_PATH + IMPORT_PATH if key in os.environ}
    environ.update(PATH=str(bin_dir), HOME=str(home), RELEASE_SCRIPTS_LOCAL_PRELOAD_FILE=str(preload))
    user_base = os.environ.get('PYTHONUSERBASE') or site.getuserbase()
    if user_base:
        environ['PYTHONUSERBASE'] = user_base
    environ.update(extra_env)
    return environ
RUNNER = Path(__file__).resolve().with_name('release-scripts-local.py')

CI = '''\
jobs:
  other:
    steps:
      - run: ./scripts/test-not-this-job.sh
  release-scripts:
    steps:
      - uses: actions/checkout@v7
      - name: Pass and fail
        run: |
          python3 -m pip install PyYAML==6.0.2
          ./scripts/test-pass.sh
          # ./scripts/test-commented.sh
          ./scripts/test-fail.sh
      - name: Setup only
        run: python3 scripts/setup-sccache.py
      - name: Prefixed
        shell: bash
        run: |
          dir="$RUNNER_TEMP/cargo-dist"
          curl -LsSf "https://example.invalid/cargo-dist-installer.sh" | sh
          PATH="$dir:$PATH" ./scripts/test-dist.sh
      - name: Host rules
        run: |
          ./scripts/test-release-cache-smoke.py
          ./scripts/test-changed-tests-stdin.py
      - uses: taiki-e/install-action@v2
        with:
          tool: release-plz@1.2.3
      - name: After install
        run: ./scripts/test-plz.sh
'''


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = Path(tempfile.mkdtemp(prefix='release-scripts-local-'))
        self.addCleanup(shutil.rmtree, self.temp)
        self.root = self.temp / 'repo'
        (self.root / '.github/workflows').mkdir(parents=True)
        (self.root / 'scripts').mkdir()
        self.bin = self.temp / 'bin'
        self.bin.mkdir()
        for tool in ('bash', 'python3'):
            (self.bin / tool).symlink_to(shutil.which(tool) or sys.executable)
        self.preload = self.temp / 'preload'
        self.preload.write_text('')
        self.write('dist-workspace.toml', '[dist]\ncargo-dist-version = "0.32.0"\n')
        self.write('scripts/setup-sccache.py', 'VERSION = "0.17.0"\n')
        self.write('.github/workflows/ci.yml', CI)
        for name, status in [('test-pass.sh', 0), ('test-fail.sh', 3), ('test-dist.sh', 0),
                             ('test-plz.sh', 0), ('test-release-cache-smoke.py', 0),
                             ('test-changed-tests-stdin.py', 0), ('test-commented.sh', 9)]:
            self.script(f'scripts/{name}', f'echo ran {name}\nexit {status}')
        self.tool('dist', 'cargo-dist 0.32.0')
        self.tool('sccache', 'sccache 0.17.0')
        self.tool('release-plz', 'release-plz 1.2.3')

    def write(self, path, text):
        (self.root / path).write_text(text)

    def script(self, path, body):
        target = self.root / path
        target.write_text('#!/usr/bin/env bash\n' + body + '\n')
        target.chmod(0o755)

    def tool(self, name, version):
        target = self.bin / name
        target.write_text(f'#!/usr/bin/env bash\necho "{version}"\n')
        target.chmod(0o755)

    RUNNER_FLAGS = ('-B',)  # No -I/-s: they drop the user site the runner may need for PyYAML.

    def run_runner(self, *args, **extra_env):
        return subprocess.run([sys.executable, *self.RUNNER_FLAGS, str(RUNNER), '--root', str(self.root), *args],
                              env=child_env(self.bin, self.temp, self.preload, **extra_env),
                              capture_output=True, text=True, timeout=60)

    USER_SITE_PROBE = ('import site, sys; print(bool(site.ENABLE_USER_SITE), '
                       'site.getusersitepackages() in sys.path, site.getusersitepackages())')

    def probe_user_site(self, flags, env):
        probe = subprocess.run([sys.executable, *flags, '-c', self.USER_SITE_PROBE],
                               env=env, capture_output=True, text=True, timeout=60)
        self.assertEqual(probe.returncode, 0, probe.stderr)
        enabled, on_path, path = probe.stdout.split(maxsplit=2)
        return enabled, on_path, path.strip()

    def test_child_runner_sees_the_same_user_site_as_a_plain_run(self):
        # Reference: this interpreter run plainly in the caller's environment, as
        # `make test-release-scripts` runs the runner. A virtual environment
        # disables the user site there too, so only a difference is a failure.
        plain_env = dict(os.environ)
        plain_env.pop('PYTHONNOUSERSITE', None)
        expected = self.probe_user_site((), plain_env)
        actual = self.probe_user_site(self.RUNNER_FLAGS, child_env(self.bin, self.temp, self.preload))
        self.assertEqual(actual, expected, 'the fixture env changes where the runner finds user-site packages')

    def test_child_env_forwards_import_path_and_accepts_overrides(self):
        with unittest.mock.patch.dict(os.environ, {'PYTHONPATH': '/fixture/py', 'LD_LIBRARY_PATH': '/fixture/lib'}):
            environ = child_env(self.bin, self.temp, self.preload, PYTHONUSERBASE='/override', HOME='/h')
        self.assertEqual(environ['PYTHONPATH'], '/fixture/py')
        self.assertEqual(environ['LD_LIBRARY_PATH'], '/fixture/lib')
        self.assertEqual(environ['PYTHONUSERBASE'], '/override')
        self.assertEqual(environ['HOME'], '/h')

    def test_runs_every_test_line_in_ci_order_and_fails_on_a_real_failure(self):
        result = self.run_runner()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        ran = [line.split()[1] for line in result.stdout.splitlines() if line.startswith('ran ')]
        self.assertEqual(ran, ['test-pass.sh', 'test-fail.sh', 'test-dist.sh', 'test-release-cache-smoke.py',
                               'test-changed-tests-stdin.py', 'test-plz.sh'])
        self.assertIn('FAILED:  test-fail.sh (exit 3)', result.stdout)
        self.assertIn('5 passed, 0 skipped, 1 failed', result.stdout)

    def test_list_drops_setup_lines_comments_and_assignments(self):
        result = self.run_runner('--list')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('RUN  test-dist.sh: ./scripts/test-dist.sh\n', result.stdout)
        for absent in ('pip install', 'setup-sccache', 'curl', 'test-commented', 'test-not-this-job', 'ran '):
            self.assertNotIn(absent, result.stdout)

    def test_a_new_test_line_is_picked_up_without_runner_changes(self):
        self.script('scripts/test-new.sh', 'echo ran test-new.sh')
        self.write('.github/workflows/ci.yml', CI.replace('./scripts/test-fail.sh', './scripts/test-new.sh'))
        result = self.run_runner()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('ran test-new.sh', result.stdout)

    def test_install_action_tool_gates_only_later_steps(self):
        (self.bin / 'release-plz').unlink()
        result = self.run_runner('--list')
        self.assertIn('SKIP test-plz.sh: needs release-plz 1.2.3 (not on PATH)', result.stdout)
        self.assertIn('RUN  test-pass.sh', result.stdout)
        self.tool('release-plz', 'release-plz 1.2.4')
        result = self.run_runner('--list')
        self.assertIn('SKIP test-plz.sh: needs release-plz 1.2.3 (found 1.2.4)', result.stdout)

    def test_cargo_dist_installer_step_needs_the_pinned_dist(self):
        self.tool('dist', 'cargo-dist 0.31.0')
        result = self.run_runner('--list')
        self.assertIn('SKIP test-dist.sh: needs dist 0.32.0 (found 0.31.0)', result.stdout)

    def test_host_rules_skip_old_sccache_and_a_global_preload(self):
        self.tool('sccache', 'sccache 0.7.7')
        self.preload.write_text('/run/some/launcher.preload.so\n')
        result = self.run_runner()
        self.assertEqual(result.returncode, 1)
        self.assertIn('SKIP test-release-cache-smoke.py: needs sccache >= 0.17.0 (found 0.7.7)', result.stdout)
        self.assertIn('SKIP test-changed-tests-stdin.py: needs a host without a global library preload', result.stdout)
        self.assertNotIn('ran test-release-cache-smoke.py', result.stdout)
        self.assertIn('2 skipped', result.stdout)

    def test_sccache_bin_overrides_path_and_newer_sccache_runs(self):
        newer = self.temp / 'sccache-new'
        newer.write_text('#!/usr/bin/env bash\necho "sccache 0.18.1"\n')
        newer.chmod(0o755)
        self.tool('sccache', 'sccache 0.7.7')
        result = self.run_runner('--list', SCCACHE_BIN=str(newer))
        self.assertIn('RUN  test-release-cache-smoke.py', result.stdout)

    def test_ld_preload_skips_the_stdin_suite(self):
        result = self.run_runner('--list', LD_PRELOAD='/nonexistent.so')
        self.assertIn('SKIP test-changed-tests-stdin.py: needs a host without a global library preload, '
                      'which writes files into TMPDIR (LD_PRELOAD is set)', result.stdout)

    def test_a_host_rule_for_a_removed_suite_is_a_setup_error(self):
        self.write('.github/workflows/ci.yml', CI.replace('./scripts/test-changed-tests-stdin.py', ''))
        result = self.run_runner('--list')
        self.assertEqual(result.returncode, 2)
        self.assertIn('HOST_RULES names suites no longer in job', result.stderr)
        self.assertIn('test-changed-tests-stdin.py', result.stderr)

    def ci_with(self, step):
        self.write('.github/workflows/ci.yml', CI + textwrap.indent(textwrap.dedent(step), '      '))

    def test_literal_inline_env_is_kept_and_step_context_applied(self):
        (self.root / 'sub').mkdir()
        self.script('scripts/test-env.sh', 'echo "ran test-env.sh FOO=$FOO BAR=$BAR PWD=${PWD##*/}"')
        self.ci_with('''\
            - name: Env
              working-directory: sub
              env:
                BAR: two
              run: FOO=one ../scripts/test-env.sh
            ''')
        result = self.run_runner()
        self.assertIn('ran test-env.sh FOO=one BAR=two PWD=sub', result.stdout)

    def test_steps_needing_ci_context_are_skipped(self):
        for step, reason in [
            ("- if: runner.os == 'macOS'\n  run: ./scripts/test-pass.sh\n", "step runs only if: runner.os == 'macOS'"),
            ('- env:\n    TOKEN: ${{ secrets.X }}\n  run: ./scripts/test-pass.sh\n', 'step env TOKEN is a CI expression'),
            ('- shell: pwsh\n  run: ./scripts/test-pass.sh\n', "step runs under shell 'pwsh'"),
            ('- run: ./scripts/test-pass.sh ${{ matrix.os }}\n', 'the command uses a CI expression'),
        ]:
            with self.subTest(reason=reason):
                self.ci_with(step)
                result = self.run_runner('--list')
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(f'SKIP test-pass.sh: needs CI context ({reason})', result.stdout)

    def test_shell_the_runner_cannot_reproduce_is_a_setup_error(self):
        for run in ['for s in pass; do ./scripts/test-$s.sh; done', 'cd sub\n    ./scripts/test-pass.sh',
                    'false | ./scripts/test-pass.sh', 'chmod +x scripts/test-pass.sh']:
            with self.subTest(run=run):
                self.ci_with(f'- run: |\n    {run}\n')
                result = self.run_runner('--list')
                self.assertEqual(result.returncode, 2, result.stdout)
                self.assertIn('teach scripts/release-scripts-local.py this shape', result.stderr)

    def test_unreadable_dist_pin_is_a_setup_error(self):
        (self.root / 'dist-workspace.toml').unlink()
        result = self.run_runner('--list')
        self.assertEqual(result.returncode, 2)
        self.assertIn('cannot read [dist].cargo-dist-version', result.stderr)

    def test_a_tool_without_a_version_reads_unknown_version(self):
        self.tool('release-plz', 'release-plz nightly')
        result = self.run_runner('--list')
        self.assertIn('SKIP test-plz.sh: needs release-plz 1.2.3 (found unknown version)', result.stdout)

    def test_missing_job_or_no_test_commands_is_a_setup_error(self):
        result = self.run_runner('--job', 'nope')
        self.assertEqual(result.returncode, 2)
        self.assertIn("job 'nope' has no steps", result.stderr)
        self.write('.github/workflows/ci.yml', textwrap.dedent('''\
            jobs:
              release-scripts:
                steps:
                  - run: echo hello
            '''))
        result = self.run_runner()
        self.assertEqual(result.returncode, 2)
        self.assertIn('runs no scripts/test-* commands', result.stderr)


if __name__ == '__main__':
    unittest.main()
