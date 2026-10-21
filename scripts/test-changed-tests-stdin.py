#!/usr/bin/env python3
"""Run the offline shell suite with owned stdin sources, including an open PTY.

The suite must exit on its own and clean up what it created: its process group
and every temporary path it made through mktemp. Only those paths are checked.
A host-wide library preload (Datadog's /etc/ld.so.preload launcher, for one)
writes its own files into TMPDIR from every process, so "the fixtures folder
is empty" would fail on such a host for files this suite does not own.
The trade-off: a temporary path made without mktemp (a direct "$TMPDIR/name"
write, Python's tempfile) is not tracked, so the suite and the script under
test create every temporary path through mktemp.

How long the suite takes never decides the result. A loaded runner can make a
healthy suite take 30 s or more, and an elapsed-time limit then reported a
false hang (intentd CI run 39627608959). Instead, a marker line is queued in the
caller's input before the suite starts, and the harness watches the unread byte
count (FIONREAD, which consumes nothing). If the count drops, a process read the
caller's input, and the harness fails at once with that cause. Only a suite
stuck for some other reason reaches the BACKSTOP_SECONDS limit.
"""

import contextlib
import dataclasses
import fcntl
import os
from pathlib import Path
import pty
import shlex
import shutil
import signal
import subprocess
import struct
import tempfile
import termios
import time
from typing import Optional
import unittest


SUITE = Path(__file__).resolve().with_name("test-changed-tests.sh")
MARKER = b"caller input the suite must not read\n"
# Only a suite stuck for a reason other than reading its input reaches this;
# a healthy run never does, however slow the host. It stays well inside the
# release-scripts job's 15-minute timeout.
BACKSTOP_SECONDS = 300
POLL_SECONDS = 0.05
# After the source closes, how long the suite gets to exit on its own.
GRACE_SECONDS = 5


def recording_mktemp(directory, manifest):
    """A mktemp first on PATH that logs each path the real one prints.

    The suite links `command -v mktemp` into its stub bin dir and runs the
    script under test with PATH set to that dir alone, so the wrapper names
    bash and the real mktemp by absolute path and otherwise uses built-ins.
    A path from a relative template is logged against the caller's cwd.
    """
    real, bash = shutil.which("mktemp"), shutil.which("bash")
    if real is None or bash is None:
        raise AssertionError("mktemp and bash must be on PATH; the suite needs them")
    wrapper = directory / "mktemp"
    wrapper.write_text(
        f"#!{bash}\n"
        f"out=$({shlex.quote(real)} \"$@\") || exit\n"
        'case $out in /*) logged=$out ;; *) logged=$PWD/$out ;; esac\n'
        f"printf '%s\\n' \"$logged\" >>{shlex.quote(str(manifest))}\n"
        "printf '%s\\n' \"$out\"\n"
    )
    wrapper.chmod(0o755)


def group_exists(pid):
    try:
        os.killpg(pid, 0)
    except ProcessLookupError:
        return False
    return True


def group_gone(pid, within):
    """Wait up to `within` seconds for every member of the group to be gone.

    A member that is still exiting, or was reparented and not yet reaped,
    keeps the group visible for a moment after the leader is reaped, so a
    single probe can report a group that is about to vanish.
    """
    deadline = time.monotonic() + within
    while group_exists(pid):
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.01)
    return True


def unread(fd):
    """Bytes queued on fd that no process has read yet; reading consumes none."""
    return struct.unpack("i", fcntl.ioctl(fd, termios.FIONREAD, b"\0\0\0\0"))[0]


def describe(returncode):
    if returncode is not None and returncode < 0:
        return f"was killed by {signal.Signals(-returncode).name}"
    return f"exited {returncode}"


@dataclasses.dataclass
class Run:
    returncode: Optional[int]
    log: str
    # "while running" or "after exit" once any process read the caller's
    # input, None while the marker stays queued (or for /dev/null).
    read_input: Optional[str] = None
    backstop: bool = False
    forced_cleanup: bool = False
    group_survived: bool = False


def drive(command, source, env, backstop=BACKSTOP_SECONDS, grace=GRACE_SECONDS):
    """Run command in its own session with stdin from source and report what it did."""
    with contextlib.ExitStack() as stack:
        output = stack.enter_context(tempfile.TemporaryFile(mode="w+"))
        writer = None
        queued = None
        if source == "pty":
            master, slave = pty.openpty()
            writer = stack.enter_context(os.fdopen(master, "wb", buffering=0))
            stdin = stack.enter_context(os.fdopen(slave, "rb", buffering=0))
            if not os.isatty(stdin.fileno()):
                raise AssertionError("pty slave is not a terminal")
            # The master stays open: a reader that drains the line blocks for
            # more instead of seeing EOF, as on an interactive terminal.
            writer.write(MARKER)
            # A pty delivers the write to the slave asynchronously; start
            # only once the whole marker is readable there.
            ready = time.monotonic() + 10
            while unread(stdin.fileno()) < len(MARKER):
                if time.monotonic() >= ready:
                    raise AssertionError("pty marker never reached the slave side")
                time.sleep(0.01)
            queued = len(MARKER)
        elif source == "pipe":
            reader, sender = os.pipe()
            writer = stack.enter_context(os.fdopen(sender, "wb", buffering=0))
            stdin = stack.enter_context(os.fdopen(reader, "rb", buffering=0))
            writer.write(MARKER)
            writer.close()
            queued = unread(stdin.fileno())
            if queued != len(MARKER):
                raise AssertionError(f"pipe holds {queued} bytes, expected {len(MARKER)}")
        else:
            stdin = stack.enter_context(open(os.devnull, "rb"))

        def was_read():
            return queued is not None and unread(stdin.fileno()) < queued

        process = subprocess.Popen(
            command,
            stdin=stdin,
            stdout=output,
            stderr=subprocess.STDOUT,
            env=env,
            start_new_session=True,
        )
        run = Run(returncode=None, log="")
        try:
            deadline = time.monotonic() + backstop
            while True:
                try:
                    process.wait(timeout=POLL_SECONDS)
                    break
                except subprocess.TimeoutExpired:
                    pass
                if was_read():
                    run.read_input = "while running"
                    break
                if time.monotonic() >= deadline:
                    run.backstop = True
                    break
            if process.returncode is not None and was_read():
                run.read_input = "after exit"
        finally:
            # Close the source first to unblock a stuck reader (EOF or PTY
            # hangup) and let parents reap children and remove fixtures.
            if writer is not None:
                writer.close()
            stdin.close()
            try:
                process.wait(timeout=grace)
            except subprocess.TimeoutExpired:
                run.forced_cleanup = True
            if not group_gone(process.pid, grace):
                run.forced_cleanup = True
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=grace)
        run.returncode = process.returncode
        run.group_survived = not group_gone(process.pid, grace)
        output.seek(0)
        run.log = output.read()
        return run


class StdinTests(unittest.TestCase):
    def run_suite(self, source):
        with contextlib.ExitStack() as stack:
            root = Path(stack.enter_context(tempfile.TemporaryDirectory()))
            fixtures = root / "fixtures"
            fixtures.mkdir()
            shim = root / "shim"
            shim.mkdir()
            manifest = root / "mktemp.log"
            recording_mktemp(shim, manifest)
            # Stands in for a file another process on the host drops into
            # TMPDIR: the suite must neither trip over it nor remove it.
            foreign = fixtures / "not-created-by-the-suite"
            foreign.write_text("host-owned\n")
            run = drive(
                ["bash", str(SUITE)],
                source,
                {
                    **os.environ,
                    "TMPDIR": str(fixtures),
                    "PATH": f"{shim}{os.pathsep}{os.environ.get('PATH', os.defpath)}",
                },
            )
            log = run.log
            self.assertFalse(run.group_survived, "owned process group survived cleanup")
            self.assertIsNone(
                run.read_input,
                f"{source}: a suite process read the caller's input ({run.read_input}); "
                f"after closing the source the suite {describe(run.returncode)}\n{log}",
            )
            self.assertFalse(
                run.backstop,
                f"{source}: suite was still running after {BACKSTOP_SECONDS} s with its input "
                f"unread; after closing the source it {describe(run.returncode)}\n{log}",
            )
            self.assertFalse(run.forced_cleanup, "suite needed forced process cleanup\n" + log)
            created = manifest.read_text().splitlines() if manifest.exists() else []
            self.assertTrue(
                any(Path(path).is_relative_to(fixtures) for path in created),
                f"recorded no mktemp path under TMPDIR, so cleanup is unchecked: {created}\n{log}",
            )
            leftovers = [path for path in created if os.path.lexists(path)]
            self.assertEqual(leftovers, [], "suite left temporary fixtures")
            self.assertTrue(foreign.exists(), "suite removed a file it did not create")
            self.assertEqual(run.returncode, 0, log)
            self.assertIn("changed-tests tests passed under", log)
            print(f"{source}: exit 0; input unread; no child processes or owned fixtures left\n{log}", end="")

    def test_open_pty(self):
        self.run_suite("pty")

    def test_finite_pipe(self):
        self.run_suite("pipe")

    def test_devnull(self):
        self.run_suite("devnull")


class HarnessTests(unittest.TestCase):
    """The detector that replaces an elapsed-time limit, checked with stub suites."""

    def stub(self, source, script, **limits):
        return drive(["bash", "-c", script], source, dict(os.environ), **limits)

    def test_reader_blocked_on_open_pty_fails_fast(self):
        # The regression the PTY case exists for: a process drains the
        # caller's terminal and waits for more. It is reported as a read
        # as soon as the marker is consumed, not after a time limit.
        run = self.stub("pty", "while IFS= read -r line; do :; done; exit 0")
        self.assertEqual(run.read_input, "while running")
        self.assertFalse(run.backstop)
        self.assertFalse(run.group_survived)

    def test_reader_that_exits_is_caught(self):
        for source in ("pty", "pipe"):
            with self.subTest(source=source):
                run = self.stub(source, "IFS= read -r line; exit 0")
                self.assertIsNotNone(run.read_input)
                self.assertFalse(run.backstop)

    def test_suite_running_past_many_polls_that_leaves_input_alone_passes(self):
        for source in ("pty", "pipe", "devnull"):
            with self.subTest(source=source):
                run = self.stub(source, "sleep 0.5; echo done")
                self.assertIsNone(run.read_input)
                self.assertFalse(run.backstop)
                self.assertFalse(run.forced_cleanup)
                self.assertEqual((run.returncode, run.log), (0, "done\n"))

    def test_backstop_reports_and_cleans_up_a_stuck_suite(self):
        run = self.stub("pipe", "exec sleep 60", backstop=0.3, grace=0.3)
        self.assertTrue(run.backstop)
        self.assertIsNone(run.read_input)
        self.assertTrue(run.forced_cleanup)
        self.assertFalse(run.group_survived)
        self.assertEqual(describe(run.returncode), "was killed by SIGKILL")


if __name__ == "__main__":
    unittest.main()
