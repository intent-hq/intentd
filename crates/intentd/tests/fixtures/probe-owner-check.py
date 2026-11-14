import fcntl
import os
import pathlib
import select
import signal
import subprocess
import sys
import tempfile
import time
import unittest

OWNER = str(pathlib.Path(sys.argv.pop(1)).resolve())


class Probe:
    def __init__(self, *command):
        cr, self.control = os.pipe()
        self.status, sw = os.pipe()
        descriptors = [fcntl.fcntl(fd, fcntl.F_DUPFD_CLOEXEC, 5) for fd in (cr, sw)]
        os.close(cr)
        os.close(sw)
        self.process = subprocess.Popen(
            [OWNER, '--intent-internal-probe-owner', *map(str, descriptors), *command],
            pass_fds=descriptors, start_new_session=True,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        for fd in descriptors:
            os.close(fd)

    def exit_status(self):
        if not select.select([self.status], [], [], 5)[0]:
            raise AssertionError('provider did not report exit status')
        return os.read(self.status, 4)

    def cleanup(self, cancel=False):
        if cancel:
            os.close(self.status)
            self.status = None
            for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
                stream.close()
        os.close(self.control)
        self.control = None
        try:
            return self.process.wait(timeout=6)
        finally:
            if self.status is not None:
                os.close(self.status)
            for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
                stream.close()


def wait_file(path):
    end = time.monotonic() + 5
    while time.monotonic() < end:
        if path.exists() and path.read_text().strip():
            return int(path.read_text().strip())
        time.sleep(.01)
    raise AssertionError('descendant did not become ready')


def gone(pid):
    return not pathlib.Path(f'/proc/{pid}').exists()


class OwnerTests(unittest.TestCase):
    def test_success_and_real_stdout(self):
        probe = Probe('/bin/sh', '-c', 'printf version-ok')
        self.assertEqual(probe.process.stdout.read(), b'version-ok')
        self.assertEqual(probe.exit_status(), b'000\n')
        self.assertIsNone(probe.process.poll())
        self.assertEqual(probe.cleanup(), 0)

    def test_nonzero_provider_preserved(self):
        probe = Probe('/bin/sh', '-c', 'exit 7')
        self.assertEqual(probe.exit_status(), b'007\n')
        self.assertEqual(probe.cleanup(), 0)

    def test_private_descriptors_not_inherited(self):
        probe = Probe('/usr/bin/python3', '-c',
                      'import os; print(sorted(int(x) for x in os.listdir("/proc/self/fd") if int(x)>4))')
        self.assertEqual(probe.process.stdout.read().strip(), b'[]')
        self.assertEqual(probe.exit_status(), b'000\n')
        self.assertEqual(probe.cleanup(), 0)

    def test_escaped_descendant_is_reaped(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / 'child'
            probe = Probe('/bin/sh', '-c',
                f'setsid /bin/sh -c "echo \\$\\$ > {path}; exec sleep 60" & exit 0')
            pid = wait_file(path)
            try:
                self.assertEqual(probe.exit_status(), b'000\n')
                self.assertFalse(gone(pid))
                self.assertEqual(probe.cleanup(), 0)
                self.assertTrue(gone(pid))
            finally:
                if not gone(pid):
                    os.kill(pid, signal.SIGKILL)

    def test_cancellation_with_closed_status_reader(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / 'child'
            probe = Probe('/bin/sh', '-c',
                f'setsid /bin/sh -c "echo \\$\\$ > {path}; exec sleep 60" & wait')
            pid = wait_file(path)
            try:
                self.assertEqual(probe.cleanup(cancel=True), 0)
                self.assertTrue(gone(pid))
            finally:
                if not gone(pid):
                    os.kill(pid, signal.SIGKILL)

    def test_invalid_owner_start_cannot_confirm_cleanup(self):
        result = subprocess.run([OWNER, '--intent-internal-probe-owner', '1', '1', '/bin/true'])
        self.assertNotEqual(result.returncode, 0)


unittest.main()
