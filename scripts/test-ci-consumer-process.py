#!/usr/bin/env python3
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

SUPERVISOR = Path(__file__).with_name('ci-consumer-process.py').resolve()


@unittest.skipUnless(sys.platform == 'linux', 'Linux consumer ownership')
class ConsumerProcessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / 'run-42-1-coverage-all'; self.root.mkdir()
        (self.root / 'storage.json').write_text(json.dumps({'root': str(self.root), 'token': self.root.name, 'slot': None}))
        self.env = dict(os.environ, GITHUB_RUN_ID='42', GITHUB_RUN_ATTEMPT='1', GITHUB_JOB='coverage-all')

    def start(self, code):
        process = subprocess.Popen([sys.executable, '-I', '-B', str(SUPERVISOR), '--root', str(self.root), '--',
                                    sys.executable, '-c', code], cwd=self.root, env=self.env,
                                   start_new_session=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        self.addCleanup(lambda: (process.kill(), process.wait()) if process.poll() is None else None)
        return process

    def wait_file(self, name):
        deadline = time.monotonic() + 5
        path = self.root / name
        while not path.exists():
            self.assertLess(time.monotonic(), deadline, name)
            time.sleep(.01)
        return path

    def receipt(self):
        records = list(self.root.glob('consumer-process-*.json'))
        self.assertEqual(len(records), 1)
        self.assertFalse((self.root / 'consumer-active.json').exists())
        return json.loads(records[0].read_text())

    def writer_command(self, exit_parent=False):
        writer = ('import os,signal,time\nfrom pathlib import Path\n'
                  'signal.signal(signal.SIGINT,signal.SIG_IGN)\nsignal.signal(signal.SIGTERM,signal.SIG_IGN)\n'
                  'Path("writer.pid").write_text(str(os.getpid()))\n'
                  'while True:\n with open("late.profraw","ab") as f:f.write(b"x");f.flush()\n time.sleep(.01)\n')
        return ('import subprocess,sys,time\nfrom pathlib import Path\n'
                f'p=subprocess.Popen([sys.executable,"-c",{writer!r}],start_new_session=True)\n'
                'while not Path("writer.pid").exists():time.sleep(.01)\n' +
                ('raise SystemExit(0)\n' if exit_parent else 'time.sleep(60)\n'))

    def assert_writer_gone(self, pid):
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)
        size = (self.root / 'late.profraw').stat().st_size
        time.sleep(.06)
        self.assertEqual((self.root / 'late.profraw').stat().st_size, size)

    def test_cancel_reaps_detached_signal_ignoring_grandchild_without_touching_neighbour(self):
        for number in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(number=number):
                for p in self.root.glob('consumer-process-*.json'): p.unlink()
                for name in ('writer.pid', 'late.profraw'):
                    (self.root / name).unlink(missing_ok=True)
                neighbour = subprocess.Popen([sys.executable, '-c', 'import time;time.sleep(60)'])
                try:
                    process = self.start(self.writer_command())
                    pid = int(self.wait_file('writer.pid').read_text())
                    process.send_signal(number)
                    _, error = process.communicate(timeout=6)
                    self.assertEqual(process.returncode, 128 + number, error)
                    receipt = self.receipt()
                    self.assertTrue(receipt['quiesced']); self.assertGreaterEqual(receipt['reaped'], 2)
                    self.assertEqual(receipt['cancel_signal'], number)
                    self.assert_writer_gone(pid)
                    self.assertIsNone(neighbour.poll())
                finally:
                    neighbour.kill(); neighbour.wait()

    def test_normal_parent_exit_still_reaps_its_detached_writer(self):
        process = self.start(self.writer_command(exit_parent=True))
        pid = int(self.wait_file('writer.pid').read_text())
        _, error = process.communicate(timeout=6)
        self.assertEqual(process.returncode, 0, error)
        self.assertGreaterEqual(self.receipt()['reaped'], 2)
        self.assert_writer_gone(pid)

    def test_failure_exit_is_preserved_after_quiescence(self):
        process = self.start('raise SystemExit(17)')
        _, error = process.communicate(timeout=5)
        self.assertEqual(process.returncode, 17, error)
        self.assertEqual(self.receipt()['exit_code'], 17)

    def test_all_coverage_and_callback_commands_are_supervised_and_receipts_uploaded(self):
        import yaml
        workflow = yaml.safe_load((SUPERVISOR.parents[1] / '.github/workflows/ci.yml').read_text())
        count = 0
        for name in ('coverage-all', 'coverage-e2e', 'coverage-changed'):
            steps = workflow['jobs'][name]['steps']
            for step in steps:
                command = step.get('run', '')
                if 'scripts/ci-linux-artifacts.py coverage' in command or 'scripts/ci-linux-artifacts.py callback' in command:
                    self.assertIn('scripts/ci-consumer-process.py -- python3 -I -B scripts/ci-linux-artifacts.py', command)
                    count += 1
            evidence = next(s for s in steps if s.get('name') == 'Upload coverage execution evidence')
            self.assertIn('consumer-*.json', evidence['with']['path'])
        self.assertEqual(count, 4)

    def test_wrong_owner_cannot_start_command_or_mark_storage(self):
        self.env['GITHUB_RUN_ID'] = '43'
        process = self.start('from pathlib import Path;Path("started").touch()')
        _, error = process.communicate(timeout=5)
        self.assertNotEqual(process.returncode, 0)
        self.assertIn(b'another run', error)
        self.assertFalse((self.root / 'started').exists())
        self.assertFalse((self.root / 'consumer-active.json').exists())


if __name__ == '__main__':
    unittest.main()
