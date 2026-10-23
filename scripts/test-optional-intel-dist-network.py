#!/usr/bin/env python3
"""Offline tests for scripts/fetch-locked-crates.py, shared by the Release host
job and scripts/test-optional-intel-dist.py.

The real dist test fetches crates once, with retries, and then runs every dist
call offline. These tests stub `cargo` and `dist` on PATH, so they need neither
the network nor cargo-dist.

Run: python3 -I -B scripts/test-optional-intel-dist-network.py
"""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('dist_test', ROOT / 'scripts/test-optional-intel-dist.py')
dist_test = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dist_test)
fetch_spec = importlib.util.spec_from_file_location('fetch_locked_crates', ROOT / 'scripts/fetch-locked-crates.py')
fetch_mod = importlib.util.module_from_spec(fetch_spec)
fetch_spec.loader.exec_module(fetch_mod)

# Records its argv and the cargo network env, then fails (or, with STUB_HANG,
# hangs) until the call count exceeds STUB_FAILURES.
STUB = '''#!/usr/bin/env python3
import json, os, sys, time
log = os.environ['STUB_LOG']
calls = open(log).read().splitlines() if os.path.exists(log) else []
keys = ('CARGO_NET_RETRY', 'CARGO_HTTP_MULTIPLEXING', 'CARGO_NET_OFFLINE')
with open(log, 'a') as out:
    out.write(json.dumps({'argv': sys.argv[1:], 'env': {k: os.environ.get(k) for k in keys}}) + '\\n')
if len(calls) < int(os.environ.get('STUB_FAILURES', '0')):
    if os.environ.get('STUB_HANG'):
        time.sleep(60)
    sys.stderr.write('error: failed to get `anyhow` as a dependency\\n')
    sys.exit(101)
print(json.dumps({'artifacts': {}}))
'''


class Network(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='optional-intel-dist-network-')
        self.dir = Path(self.temp.name)
        for name in ('cargo', 'dist'):
            stub = self.dir / name
            stub.write_text(STUB)
            stub.chmod(0o755)
        self.log = self.dir / 'calls.jsonl'
        self.sleeps = []
        self.env = {**os.environ, 'PATH': f'{self.dir}{os.pathsep}{os.environ["PATH"]}',
                    'STUB_LOG': str(self.log)}
        self.env.pop('CARGO_NET_OFFLINE', None)

    def tearDown(self):
        self.temp.cleanup()

    def calls(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()]

    def fetch(self, failures, attempts=4, **limits):
        self.env['STUB_FAILURES'] = str(failures)
        return fetch_mod.fetch_dependencies(self.dir, self.env, attempts=attempts, sleep=self.sleeps.append, **limits)

    def test_worst_case_fits_the_job_budget(self):
        attempts, timeout, deadline = (fetch_mod.FETCH_ATTEMPTS, fetch_mod.FETCH_ATTEMPT_TIMEOUT,
                                       fetch_mod.FETCH_DEADLINE)
        backoff = sum(5 * 2 ** n for n in range(attempts - 1))
        self.assertLessEqual(deadline, 8 * 60, 'leaves the 15-minute job room for its other steps')
        self.assertLess(timeout, deadline)
        self.assertLessEqual(min(deadline, attempts * timeout) + backoff, 9 * 60)

    def test_hung_fetch_is_a_failed_attempt(self):
        self.env['STUB_HANG'] = '1'
        start = time.monotonic()
        self.fetch(failures=1, attempt_timeout=3)
        self.assertLess(time.monotonic() - start, 30, 'the hung attempt was killed')
        self.assertEqual(len(self.calls()), 2)
        self.assertEqual(len(self.sleeps), 1)

    def test_final_timeout_gives_network_message(self):
        self.env['STUB_HANG'] = '1'
        with self.assertRaises(SystemExit) as raised:
            self.fetch(failures=10, attempts=2, attempt_timeout=3)
        self.assertEqual(len(self.calls()), 2)
        message = str(raised.exception.code)
        self.assertIn('network', message)
        self.assertIn('cargo fetch --locked', message)

    def test_overall_deadline_stops_retrying(self):
        self.env['STUB_HANG'] = '1'
        start = time.monotonic()
        with self.assertRaises(SystemExit) as raised:
            self.fetch(failures=10, attempts=4, attempt_timeout=10, deadline=3)
        self.assertLess(time.monotonic() - start, 8, 'the deadline caps the running attempt too')
        self.assertLess(len(self.calls()), 4, 'no attempts start after the deadline')
        self.assertIn('network', str(raised.exception.code))

    def test_fetch_retries_until_success(self):
        self.fetch(failures=2)
        calls = self.calls()
        self.assertEqual(len(calls), 3)
        for call in calls:
            self.assertEqual(call['argv'], ['fetch', '--locked'])
            self.assertEqual(call['env']['CARGO_HTTP_MULTIPLEXING'], 'false')
            self.assertEqual(call['env']['CARGO_NET_RETRY'], '3', 'cargo retries stay inside the attempt timeout')
            self.assertIsNone(call['env']['CARGO_NET_OFFLINE'])
        self.assertEqual(len(self.sleeps), 2)
        self.assertLess(self.sleeps[0], self.sleeps[1], 'retries back off')

    def test_fetch_gives_up_with_network_message(self):
        with self.assertRaises(SystemExit) as raised:
            self.fetch(failures=10)
        self.assertEqual(len(self.calls()), 4)
        self.assertEqual(len(self.sleeps), 3, 'no sleep after the last attempt')
        message = str(raised.exception.code)
        self.assertIn('network', message)
        self.assertIn('4 attempts', message)
        self.assertIn('cargo fetch --locked', message)

    def test_failure_message_does_not_blame_the_network_unconditionally(self):
        with self.assertRaises(SystemExit) as raised:
            self.fetch(failures=10, attempts=2)
        message = str(raised.exception.code)
        self.assertIn('otherwise fix the reported cause', message)
        with self.assertRaises(SystemExit) as raised:
            fetch_mod.fetch_dependencies(self.dir, self.env, attempts=1, sleep=self.sleeps.append, hint=' HINT')
        self.assertTrue(str(raised.exception.code).endswith(' HINT'))

    def test_toolchain_install_retries_without_a_timeout(self):
        self.env['STUB_FAILURES'] = '1'
        fetch_mod.install_toolchain(self.dir, self.env, sleep=self.sleeps.append)
        self.assertEqual([c['argv'] for c in self.calls()], [['--version'], ['--version']])
        self.assertEqual(len(self.sleeps), 1)
        self.env['STUB_FAILURES'] = '10'
        with self.assertRaises(SystemExit) as raised:
            fetch_mod.install_toolchain(self.dir, self.env, attempts=2, sleep=self.sleeps.append)
        self.assertIn('toolchain', str(raised.exception.code))

    def test_release_limits_allow_slow_attempts(self):
        self.assertGreater(fetch_mod.RELEASE_ATTEMPT_TIMEOUT, fetch_mod.FETCH_ATTEMPT_TIMEOUT)
        self.assertGreaterEqual(fetch_mod.RELEASE_DEADLINE, 2 * fetch_mod.RELEASE_ATTEMPT_TIMEOUT)

    def test_dist_runs_offline(self):
        result = dist_test.run_dist(self.dir, fetch_mod.offline_env(self.env), 'build', '--artifacts=lies')
        self.assertEqual(result, {'artifacts': {}})
        (call,) = self.calls()
        self.assertEqual(call['argv'], ['build', '--artifacts=lies', '--output-format=json'])
        self.assertEqual(call['env']['CARGO_NET_OFFLINE'], 'true')

    def test_offline_env_does_not_mutate_input(self):
        offline = fetch_mod.offline_env(self.env)
        self.assertEqual(offline['CARGO_NET_OFFLINE'], 'true')
        self.assertNotIn('CARGO_NET_OFFLINE', self.env)

    def test_main_fetches_before_any_offline_dist_call(self):
        source = (ROOT / 'scripts/test-optional-intel-dist.py').read_text()
        body = source[source.index('def main():'):]
        self.assertLess(body.index('fetch_dependencies('), body.index("run_dist("))
        direct = [line for line in body.splitlines() if "subprocess" in line and "['dist'" in line and '--version' not in line]
        self.assertTrue(direct, 'dist generate still runs directly')
        for line in direct:
            self.assertIn('env=env', line, 'every dist call gets the offline env')


if __name__ == '__main__':
    unittest.main()
