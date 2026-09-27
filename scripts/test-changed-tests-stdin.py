#!/usr/bin/env python3
"""Run the offline shell suite with owned stdin sources, including an open PTY."""

import contextlib
import os
from pathlib import Path
import pty
import signal
import subprocess
import tempfile
import unittest


SUITE = Path(__file__).resolve().with_name("test-changed-tests.sh")


def group_exists(pid):
    try:
        os.killpg(pid, 0)
    except ProcessLookupError:
        return False
    return True


class StdinTests(unittest.TestCase):
    def run_suite(self, source):
        with contextlib.ExitStack() as stack:
            root = Path(stack.enter_context(tempfile.TemporaryDirectory()))
            fixtures = root / "fixtures"
            fixtures.mkdir()
            output = stack.enter_context(tempfile.TemporaryFile(mode="w+"))
            writer = None
            if source == "pty":
                master, slave = pty.openpty()
                writer = stack.enter_context(os.fdopen(master, "wb", buffering=0))
                stdin = stack.enter_context(os.fdopen(slave, "rb", buffering=0))
                self.assertTrue(os.isatty(stdin.fileno()))
            elif source == "pipe":
                reader, sender = os.pipe()
                writer = stack.enter_context(os.fdopen(sender, "wb", buffering=0))
                stdin = stack.enter_context(os.fdopen(reader, "rb", buffering=0))
                writer.write(b"unrelated caller input\n")
                writer.close()
            else:
                stdin = stack.enter_context(open(os.devnull, "rb"))

            process = subprocess.Popen(
                ["bash", str(SUITE)],
                stdin=stdin,
                stdout=output,
                stderr=subprocess.STDOUT,
                env={**os.environ, "TMPDIR": str(fixtures)},
                start_new_session=True,
            )
            timed_out = False
            forced_cleanup = False
            try:
                # This bound detects the regression; reaching it always fails.
                # Keep the PTY master open until the suite exits on its own.
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                timed_out = True
            finally:
                # Close the source first to unblock a stuck drain (EOF or PTY
                # hangup) and let parents reap children and remove fixtures.
                if writer is not None:
                    writer.close()
                stdin.close()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    forced_cleanup = True
                if group_exists(process.pid):
                    forced_cleanup = True
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)

            output.seek(0)
            log = output.read()
            self.assertFalse(group_exists(process.pid), "owned process group survived cleanup")
            self.assertFalse(forced_cleanup, "suite needed forced process cleanup\n" + log)
            self.assertEqual(list(fixtures.iterdir()), [], "suite left temporary fixtures")
            self.assertFalse(
                timed_out,
                f"{source}: suite did not exit with stdin open; after closing the source "
                f"it exited {process.returncode}, with no child processes or fixtures left\n{log}",
            )
            self.assertEqual(process.returncode, 0, log)
            self.assertIn("changed-tests tests passed under", log)
            print(f"{source}: exit 0; no child processes or fixtures left\n{log}", end="")

    def test_open_pty(self):
        self.run_suite("pty")

    def test_finite_pipe(self):
        self.run_suite("pipe")

    def test_devnull(self):
        self.run_suite("devnull")


if __name__ == "__main__":
    unittest.main()
