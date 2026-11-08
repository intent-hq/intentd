#!/usr/bin/env python3
"""Bounded retries on every CI download (intent-hq/intent#8613 follow-up).

A single GitHub 504 on the sccache download ejected merge-queue PRs; the same
single-shot pattern sat in the cargo-dist installer, the rustup bootstrap,
the callback-fixture source download, the npm manifest fetch and install.sh's
wget fallback. This suite covers the shared Python helper
(scripts/download_retry.py), each caller, scripts/install-dist.sh and
install.sh's wget_download, and scans the workflows and scripts so a new
single-shot download fails here instead of in the merge queue.
Offline; requires PyYAML.

Run: python3 -I -B scripts/test-download-retry.py
"""
import contextlib
import http.client
import importlib.util
import io
import os
from pathlib import Path
import re
import shutil
import ssl
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import urllib.error

import yaml

ROOT = Path(__file__).resolve().parents[1]


def load(name, file):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'scripts' / file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


retry = load('download_retry', 'download_retry.py')
fixture = load('prepare_fixture', 'prepare-acp-callback-fixture.py')
metadata = load('provider_cli_metadata', 'provider_cli_metadata.py')


def http_error(code):
    return urllib.error.HTTPError('https://github.com/x', code, 'error', {}, None)


def quiet():
    return contextlib.redirect_stderr(io.StringIO())


class Helper(unittest.TestCase):
    def run_retrying(self, failures, delays=(1, 2, 3)):
        calls, sleeps, cleanups = [], [], []

        def operation():
            calls.append(1)
            if failures:
                raise failures.pop(0)
            return 'ok'
        with quiet():
            try:
                result = retry.retrying(operation, 'test', delays=delays, sleep=sleeps.append,
                                        cleanup=lambda: cleanups.append(1))
                return len(calls), sleeps, len(cleanups), result
            except Exception as error:
                return len(calls), sleeps, len(cleanups), error

    def test_transient_failures_are_retried(self):
        failures = [http_error(504), urllib.error.URLError('reset'), TimeoutError(), ConnectionResetError(),
                    ssl.SSLError('bad record mac'), http.client.IncompleteRead(b'')]
        self.assertEqual(self.run_retrying(failures, delays=(1,) * 6), (7, [1] * 6, 6, 'ok'))

    def test_client_and_local_errors_fail_at_once(self):
        for failure in (http_error(404), http_error(403), http_error(429), ValueError('bad'),
                        PermissionError('read-only')):
            calls, sleeps, cleanups, error = self.run_retrying([failure])
            self.assertEqual((calls, sleeps, cleanups), (1, [], 0), failure)
            self.assertIs(error, failure)

    def test_bounded_with_last_error_raised(self):
        last = http_error(503)
        calls, sleeps, cleanups, error = self.run_retrying([http_error(502)] * 3 + [last] + [http_error(500)] * 5)
        self.assertEqual((calls, sleeps, cleanups), (4, [1, 2, 3], 3))
        self.assertIs(error, last)

    def test_default_schedule_is_bounded(self):
        self.assertEqual(retry.RETRY_DELAYS, (2, 5, 15))


class FakeResponse(io.BytesIO):
    def __init__(self, data, url):
        super().__init__(data)
        self.url = url

    def geturl(self):
        return self.url

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class CallbackFixtureSource(unittest.TestCase):
    URL = 'https://codeload.github.com/agentclientprotocol/claude-agent-acp/tar.gz/abc'

    def test_gateway_timeout_and_partial_download_are_retried(self):
        data = b'source tarball'
        source = {'url': self.URL, 'bytes': len(data), 'sha256': fixture.hashlib.sha256(data).hexdigest()}
        calls = []

        class Truncating(FakeResponse):
            def read(self, size=-1):
                raise http.client.IncompleteRead(b'part')

        def urlopen(url, timeout):
            calls.append(url)
            if len(calls) == 1:
                raise http_error(504)
            if len(calls) == 2:
                return Truncating(data, url)
            return FakeResponse(data, url)
        sleeps = []
        with tempfile.TemporaryDirectory() as temp, patch.object(fixture.urllib.request, 'urlopen', urlopen), \
                patch.object(fixture, 'receipt'), quiet():
            target = Path(temp) / 'source.tar.gz'
            # The partial file from the truncated attempt must not block the
            # exclusive-create open of the next one.
            fixture.download(source, target, delays=(1, 2, 3), sleep=sleeps.append)
            self.assertEqual(target.read_bytes(), data)
        self.assertEqual((len(calls), sleeps), (3, [1, 2]))

    def test_not_found_fails_once(self):
        source = {'url': self.URL, 'bytes': 1, 'sha256': '0' * 64}
        calls = []

        def urlopen(url, timeout):
            calls.append(url)
            raise http_error(404)
        with tempfile.TemporaryDirectory() as temp, patch.object(fixture.urllib.request, 'urlopen', urlopen):
            with self.assertRaises(urllib.error.HTTPError):
                fixture.download(source, Path(temp) / 'source.tar.gz', delays=(1,), sleep=lambda _: None)
        self.assertEqual(len(calls), 1)

    def test_unexpected_locator_is_refused_before_any_attempt(self):
        with patch.object(fixture.urllib.request, 'urlopen', side_effect=AssertionError('network')):
            with self.assertRaises(fixture.InvalidFixture):
                fixture.download({'url': 'https://example.com/x'}, Path('/nonexistent'))


class NpmManifest(unittest.TestCase):
    def test_registry_5xx_is_retried(self):
        calls = []

        def urlopen(url, timeout):
            calls.append(url)
            if len(calls) < 3:
                raise http_error(503)
            return FakeResponse(b'{"name": "pkg", "version": "1.2.3"}', url)
        sleeps = []
        with patch.object(metadata.urllib.request, 'urlopen', urlopen), quiet():
            manifest = metadata.fetch_manifest('pkg', '1.2.3', delays=(1, 2, 3), sleep=sleeps.append)
        self.assertEqual((manifest['version'], len(calls), sleeps), ('1.2.3', 3, [1, 2]))

    def test_wrong_package_is_not_retried(self):
        calls = []

        def urlopen(url, timeout):
            calls.append(url)
            return FakeResponse(b'{"name": "other", "version": "1.2.3"}', url)
        with patch.object(metadata.urllib.request, 'urlopen', urlopen):
            with self.assertRaisesRegex(ValueError, 'different package'):
                metadata.fetch_manifest('pkg', '1.2.3', delays=(1,), sleep=lambda _: None)
        self.assertEqual(len(calls), 1)


def executable(path, body):
    path.write_text('#!/bin/sh\n' + body)
    path.chmod(0o755)


class InstallDist(unittest.TestCase):
    """scripts/install-dist.sh: retry the planned cargo-dist installer command."""

    def run_script(self, failures, shell='sh', run=None):
        with tempfile.TemporaryDirectory() as temp:
            temp = Path(temp)
            bin_dir = temp / 'bin'
            bin_dir.mkdir()
            # Fails its first `failures` calls; then prints a script that
            # records the install.
            executable(bin_dir / 'fake-download', f'''n=$(cat "{temp}/count" 2>/dev/null || echo 0)
n=$((n + 1)); echo "$n" > "{temp}/count"
[ "$n" -gt {failures} ] || exit 22
echo 'echo installed >> "{temp}/installed"'
''')
            env = {'PATH': f'{bin_dir}:{os.environ["PATH"]}', 'INSTALL_DIST_DELAY': '0',
                   'INSTALL_DIST_RUN': run or 'fake-download | sh', 'INSTALL_DIST_SHELL': shell}
            result = subprocess.run(['bash', str(ROOT / 'scripts/install-dist.sh')], env=env,
                                    capture_output=True, text=True)
            read = lambda name: (temp / name).read_text() if (temp / name).exists() else ''
            return result, read('count').strip(), read('installed'), None

    def test_failed_download_is_retried_not_fed_to_sh(self):
        # Without pipefail the first failure would "succeed" with nothing installed.
        result, count, installed, _ = self.run_script(failures=2)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((count, installed), ('3', 'installed\n'))
        self.assertIn('attempt 2 failed', result.stderr)

    def test_retries_are_bounded(self):
        result, count, installed, _ = self.run_script(failures=10)
        self.assertEqual(result.returncode, 1)
        self.assertEqual((count, installed), ('4', ''))
        self.assertIn('failed after 4 attempts', result.stderr)

    def test_non_sh_shells_are_refused(self):
        # Windows legs (shell pwsh) use the native pwsh step, never this script.
        for shell in ('pwsh', 'cmd', ''):
            result, count, _, _ = self.run_script(failures=0, shell=shell)
            self.assertEqual((result.returncode, count), (2, ''), shell)
            self.assertIn('unsupported INSTALL_DIST_SHELL', result.stderr)


DIST_PIN = re.search(r'cargo-dist-version\s*=\s*"([^"]+)"', (ROOT / 'dist-workspace.toml').read_text()).group(1)


class InstallCargoDist(unittest.TestCase):
    """scripts/install-cargo-dist.sh: cached binary, then the installer, then crates.io."""

    def run_script(self, curl_ok, cached=None):
        with tempfile.TemporaryDirectory() as temp:
            temp = Path(temp)
            bin_dir, target, fake = temp / 'bin', temp / 'cargo-dist', temp / 'fake-dist'
            bin_dir.mkdir()
            target.mkdir()
            executable(fake, f'echo "cargo-dist {DIST_PIN}"\n')
            if cached:
                executable(target / 'dist', f'echo "cargo-dist {cached}"\n')
            # The fake installer script copies the fake binary into the unmanaged dir.
            installer = f'cp "{fake}" "$CARGO_DIST_UNMANAGED_INSTALL/dist"'
            executable(bin_dir / 'curl', f'echo curl >> "{temp}/calls"\n'
                       + (f"echo '{installer}'\n" if curl_ok else 'exit 22\n'))
            executable(bin_dir / 'cargo', f'echo "cargo $*" >> "{temp}/calls"\n'
                       'while [ "$1" != --root ]; do shift; done\n'
                       f'mkdir -p "$2/bin" && cp "{fake}" "$2/bin/dist"\n')
            env = {'PATH': f'{bin_dir}:{os.environ["PATH"]}', 'INSTALL_DIST_DELAY': '0'}
            result = subprocess.run(['bash', str(ROOT / 'scripts/install-cargo-dist.sh'), str(target)],
                                    env=env, capture_output=True, text=True)
            calls = (temp / 'calls').read_text().splitlines() if (temp / 'calls').exists() else []
            version = subprocess.run([str(target / 'dist'), '--version'], capture_output=True, text=True).stdout.strip()
            leftovers = sorted(p.name for p in target.iterdir() if p.name != 'dist')
            return result, calls, version, leftovers

    def test_cached_binary_at_the_pin_is_kept(self):
        result, calls, version, _ = self.run_script(curl_ok=False, cached=DIST_PIN)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((calls, version), ([], f'cargo-dist {DIST_PIN}'))

    def test_stale_cache_uses_the_installer_when_the_download_works(self):
        result, calls, version, _ = self.run_script(curl_ok=True, cached='0.0.1')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((calls, version), (['curl'], f'cargo-dist {DIST_PIN}'))

    def test_release_outage_builds_the_pin_from_crates_io(self):
        # GitHub release downloads 504ed for over an hour on intentd#3156's runs.
        result, calls, version, leftovers = self.run_script(curl_ok=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls[:4], ['curl'] * 4)
        self.assertTrue(calls[4].startswith(f'cargo install cargo-dist --locked --version {DIST_PIN} '), calls)
        self.assertEqual((version, leftovers), (f'cargo-dist {DIST_PIN}', []))
        self.assertIn('building it from crates.io', result.stderr)


class InstallReleasePlz(unittest.TestCase):
    """scripts/install-release-plz.sh: cache, then the action's binary, then crates.io."""

    def run_script(self, cached=None, on_path=None, version='0.3.162'):
        with tempfile.TemporaryDirectory() as temp:
            temp = Path(temp)
            bin_dir, target, fake = temp / 'bin', temp / 'release-plz', temp / 'fake'
            bin_dir.mkdir()
            target.mkdir()
            executable(fake, f'echo "release-plz {version}"\n')
            if cached:
                executable(target / 'release-plz', f'echo "release-plz {cached}"\n')
            if on_path:
                executable(bin_dir / 'release-plz', f'echo "release-plz {on_path}"\n')
            executable(bin_dir / 'cargo', f'echo "cargo $*" >> "{temp}/calls"\n'
                       'while [ "$1" != --root ]; do shift; done\n'
                       f'mkdir -p "$2/bin" && cp "{fake}" "$2/bin/release-plz"\n')
            env = {'PATH': f'{bin_dir}:/usr/bin:/bin'}
            result = subprocess.run(['bash', str(ROOT / 'scripts/install-release-plz.sh'), str(target), version],
                                    env=env, capture_output=True, text=True)
            calls = (temp / 'calls').read_text().splitlines() if (temp / 'calls').exists() else []
            installed = subprocess.run([str(target / 'release-plz'), '--version'], capture_output=True, text=True).stdout.strip()
            leftovers = sorted(p.name for p in target.iterdir() if p.name != 'release-plz')
            return result, calls, installed, leftovers

    def test_cache_hit_is_kept(self):
        result, calls, installed, _ = self.run_script(cached='0.3.162', on_path='0.3.1')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((calls, installed), ([], 'release-plz 0.3.162'))

    def test_action_binary_is_copied_over_a_stale_cache(self):
        result, calls, installed, _ = self.run_script(cached='0.3.1', on_path='0.3.162')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((calls, installed), ([], 'release-plz 0.3.162'))

    def test_failed_action_builds_the_pin_from_crates_io(self):
        # The action timed out on GitHub 504s, so nothing is on PATH.
        result, calls, installed, leftovers = self.run_script(on_path='0.3.1')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 1)
        self.assertTrue(calls[0].startswith('cargo install release-plz --locked --version 0.3.162 '), calls)
        self.assertEqual((installed, leftovers), ('release-plz 0.3.162', []))
        self.assertIn('building it from crates.io', result.stderr)

    def test_version_must_match_as_a_whole_word(self):
        result, calls, _, _ = self.run_script(cached='0.3.1620', on_path='10.3.162')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 1)


def wget_download_function():
    script = (ROOT / 'scripts/install.sh').read_text()
    start = script.index('wget_download() {')
    return script[start:script.index('\n}', start) + 2]


POSIX_SHELLS = [shell for shell in (['dash'], ['bash', '--posix'], ['busybox', 'ash'], ['ksh'], ['mksh'], ['zsh', '--emulate', 'sh'])
                if shutil.which(shell[0])]


class InstallShWget(unittest.TestCase):
    """install.sh's wget fallback, extracted from the shipped script."""

    def run_wget(self, shell, failures):
        with tempfile.TemporaryDirectory() as temp:
            temp = Path(temp)
            # A shell function, not a PATH stub: busybox ash runs its built-in
            # wget applet ahead of PATH, but functions win in every shell.
            stub = (f'wget() {{ echo "$*" >> "{temp}/calls"; n=$(wc -l < "{temp}/calls"); '
                    f'if [ "$n" -le {failures} ]; then : > "$2"; echo "wget: attempt $n failed" >&2; return 8; fi; '
                    f'echo archive > "$2"; }}\n')
            driver = (f'set -eu\n{stub}sleep() {{ echo "$1" >> "{temp}/sleeps"; }}\n'
                      f'{wget_download_function()}\n'
                      f'if wget_download https://example.invalid/a.tar.xz "{temp}/a.tar.xz"; then echo ok; else echo failed; fi\n')
            result = subprocess.run([*shell, '-c', driver], capture_output=True, text=True)
            self.assertTrue((temp / 'calls').exists(), f'{shell}: the wget stub never ran: {result.stderr!r}')
            calls = (temp / 'calls').read_text().splitlines()
            sleeps = (temp / 'sleeps').read_text().split() if (temp / 'sleeps').exists() else []
            data = (temp / 'a.tar.xz').read_text()
            return result, calls, sleeps, data

    def test_shells_available(self):
        self.assertTrue(POSIX_SHELLS, 'no POSIX shell to test install.sh with')

    def test_transient_failures_are_retried_quietly(self):
        for shell in POSIX_SHELLS:
            with self.subTest(shell=' '.join(shell)):
                result, calls, sleeps, data = self.run_wget(shell, failures=2)
                # Retried attempts' errors are hidden: output as for one good attempt.
                self.assertEqual((result.stdout, result.stderr), ('ok\n', ''))
                self.assertEqual((len(calls), sleeps, data), (3, ['1', '2'], 'archive\n'))
                self.assertEqual(calls[0], f'-qO {calls[0].split()[1]} https://example.invalid/a.tar.xz')

    def test_retries_are_bounded_and_the_last_error_shows(self):
        for shell in POSIX_SHELLS:
            with self.subTest(shell=' '.join(shell)):
                result, calls, sleeps, _ = self.run_wget(shell, failures=10)
                self.assertEqual((result.stdout, result.stderr), ('failed\n', 'wget: attempt 4 failed\n'))
                self.assertEqual((len(calls), sleeps), (4, ['1', '2', '3']))

    def test_curl_path_is_unchanged(self):
        self.assertIn("""download() { curl --proto '=https' --tlsv1.2 --retry 3 -fsSL -o "$2" "$1"; }""",
                      (ROOT / 'scripts/install.sh').read_text())


# --- Repository scan: no new single-shot downloads ---------------------------

# Python downloaders and the retry each one uses.
PYTHON_DOWNLOADERS = {
    'scripts/setup-sccache.py': 'download_retry.retrying(',
    'scripts/prepare-acp-callback-fixture.py': 'download_retry.retrying(',
    'scripts/provider_cli_metadata.py': 'download_retry.retrying(',
    # Local/offline-first corpus check with its own two-attempt loop.
    'scripts/check-provider-error-corpus.py': 'for attempt in range(2):',
}
# The two wget calls inside install.sh's retrying wget_download().
WGET_HELPER_LINES = ('if wget -qO "$2" "$1" 2>/dev/null; then', 'wget -qO "$2" "$1"')
# Workflows whose Windows leg runs cargo-dist's planned `irm | iex` installer
# once: installer.ps1 ends in `exit 1`, so a retry needs each attempt in a
# child pwsh process; left for a follow-up exercised on a Windows runner.
KNOWN_SINGLE_SHOT_WINDOWS = ('v-release.yml', 'release-cache.yml')
# Known single-shot downloads left for a follow-up, with the reason.
KNOWN_SINGLE_SHOT = {
    # User-facing Windows installer; its retry needs Windows/Pester coverage.
    'scripts/install.ps1': 'Invoke-WebRequest',
}


def workflow_runs():
    """(file, run text) for every `run:` in .github, continuations joined."""
    for path in sorted((ROOT / '.github').rglob('*.y*ml')):
        stack = [yaml.safe_load(path.read_text())]
        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                for key, value in node.items():
                    if key == 'run' and isinstance(value, str):
                        yield path.relative_to(ROOT).as_posix(), value
                    else:
                        stack.append(value)
            elif isinstance(node, list):
                stack.extend(node)


RETRY = re.compile(r'--retry[ =][1-9]')
QUOTED = re.compile(r'"(?:[^"\\]|\\.)*"|\'[^\']*\'')


def command_lines(text):
    """Non-comment lines, continuations joined."""
    for line in text.replace('\\\n', ' ').splitlines():
        line = line.strip()
        if line and not line.startswith('#'):
            yield line


def invokes(word, line):
    """True when `word` runs as a command: not a `command -v` probe or a quoted message."""
    # Quoted text is a message, unless it holds a command substitution.
    line = QUOTED.sub(lambda m: m.group() if '$(' in m.group() else '""', re.sub(r'command -v \w+', '', line))
    return re.search(r'(?:^|[\s;|&(`])' + word + r'\s', line) is not None


def shipped_scripts(suffix):
    for path in sorted((ROOT / 'scripts').glob('*' + suffix)):
        name = path.name
        if not (name.startswith('test-') or name.startswith('test_') or '.test.' in name
                or name.endswith('.Tests.ps1')):
            yield path.relative_to(ROOT).as_posix(), path.read_text()


class RepositoryScan(unittest.TestCase):
    def shell_sources(self):
        yield from workflow_runs()
        yield from shipped_scripts('.sh')

    def test_every_curl_retries(self):
        offenders = [f'{file}: {line}' for file, text in self.shell_sources()
                     for line in command_lines(text) if invokes('curl', line) and not RETRY.search(line)]
        self.assertEqual(offenders, [], 'add --retry N --retry-connrefused (and --fail) to these curl calls')

    def test_wget_only_in_the_retrying_install_helper(self):
        offenders = [f'{file}: {line}' for file, text in self.shell_sources()
                     for line in command_lines(text) if invokes('wget', line)
                     and not (file == 'scripts/install.sh' and line in WGET_HELPER_LINES)]
        self.assertEqual(offenders, [])

    def test_planned_installer_runs_through_the_retry_script(self):
        # Windows legs are the known exception (KNOWN_SINGLE_SHOT_WINDOWS).
        raw = []
        for path in sorted((ROOT / '.github/workflows').glob('*.yml')):
            for job in (yaml.safe_load(path.read_text()).get('jobs') or {}).values():
                for step in job.get('steps', []):
                    if '${{ matrix.install_dist.run }}' in str(step.get('run', '')):
                        raw.append((path.name, step.get('if')))
        self.assertEqual(sorted(raw), sorted((name, "runner.os == 'Windows'") for name in KNOWN_SINGLE_SHOT_WINDOWS))

    def test_workflow_web_requests_retry(self):
        for file, text in workflow_runs():
            if any(re.search(r'Invoke-WebRequest|Invoke-RestMethod|\birm\s', line) for line in command_lines(text)):
                self.assertTrue('$attempt' in text and 'Start-Sleep' in text, f'{file}: retry this download')

    def test_python_downloads_retry(self):
        found = {}
        for file, text in shipped_scripts('.py'):
            if re.search(r'\burlopen\(|\burlretrieve\(', text):
                found[file] = text
        self.assertEqual(sorted(found), sorted(PYTHON_DOWNLOADERS), 'new Python downloader: use download_retry.py')
        for file, marker in PYTHON_DOWNLOADERS.items():
            self.assertIn(marker, found[file], file)
            # One wrapped fetch per file: a second urlopen would bypass the retry.
            self.assertEqual(len(re.findall(r'\burlopen\(|\burlretrieve\(', found[file])), 1, file)

    def test_scan_sees_through_messages_but_not_substitutions(self):
        self.assertFalse(invokes('curl', 'fail "neither curl nor wget is available"'))
        self.assertFalse(invokes('curl', 'if command -v curl >/dev/null; then'))
        self.assertTrue(invokes('curl', 'out="$(curl -fsSL https://example.invalid)"'))
        self.assertIsNone(RETRY.search('curl --retry 0 --retry-connrefused -fsSL x'))
        self.assertIsNotNone(RETRY.search('curl --retry 5 -fsSL x'))

    def test_powershell_scripts(self):
        for file, text in shipped_scripts('.ps1'):
            if re.search(r'Invoke-WebRequest|Invoke-RestMethod', text):
                self.assertIn(file, KNOWN_SINGLE_SHOT, f'{file}: retry this download')
        for file, marker in KNOWN_SINGLE_SHOT.items():
            self.assertIn(marker, (ROOT / file).read_text(), f'{file} no longer matches; drop its entry')


if __name__ == '__main__':
    unittest.main()
