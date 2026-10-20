#!/usr/bin/env python3
"""Offline tests for the network handling of scripts/test-optional-intel-dist.py.

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
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('dist_test', ROOT / 'scripts/test-optional-intel-dist.py')
dist_test = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dist_test)

# Records its argv and the cargo network env, then fails until the call count
# exceeds STUB_FAILURES.
STUB = '''#!/usr/bin/env python3
import json, os, sys
log = os.environ['STUB_LOG']
calls = open(log).read().splitlines() if os.path.exists(log) else []
keys = ('CARGO_NET_RETRY', 'CARGO_HTTP_MULTIPLEXING', 'CARGO_NET_OFFLINE')
with open(log, 'a') as out:
    out.write(json.dumps({'argv': sys.argv[1:], 'env': {k: os.environ.get(k) for k in keys}}) + '\\n')
if len(calls) < int(os.environ.get('STUB_FAILURES', '0')):
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

    def fetch(self, failures, attempts=4):
        self.env['STUB_FAILURES'] = str(failures)
        return dist_test.fetch_dependencies(self.dir, self.env, attempts=attempts, sleep=self.sleeps.append)

    def test_fetch_retries_until_success(self):
        self.fetch(failures=2)
        calls = self.calls()
        self.assertEqual(len(calls), 3)
        for call in calls:
            self.assertEqual(call['argv'], ['fetch', '--locked'])
            self.assertEqual(call['env']['CARGO_HTTP_MULTIPLEXING'], 'false')
            self.assertGreaterEqual(int(call['env']['CARGO_NET_RETRY']), 5)
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

    def test_dist_runs_offline(self):
        result = dist_test.run_dist(self.dir, dist_test.offline_env(self.env), 'build', '--artifacts=lies')
        self.assertEqual(result, {'artifacts': {}})
        (call,) = self.calls()
        self.assertEqual(call['argv'], ['build', '--artifacts=lies', '--output-format=json'])
        self.assertEqual(call['env']['CARGO_NET_OFFLINE'], 'true')

    def test_offline_env_does_not_mutate_input(self):
        offline = dist_test.offline_env(self.env)
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
