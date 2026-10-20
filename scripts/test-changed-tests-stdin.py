#!/usr/bin/env python3
"""Run the offline shell suite with owned stdin sources, including an open PTY.

The suite must exit on its own and clean up what it created: its process group
and every temporary path it made through mktemp. Only those paths are checked.
A host-wide library preload (Datadog's /etc/ld.so.preload launcher, for one)
writes its own files into TMPDIR from every process, so "the fixtures folder
is empty" would fail on such a host for files this suite does not own.
"""

import contextlib
import os
from pathlib import Path
import pty
import shlex
import shutil
import signal
import subprocess
import tempfile
import unittest


SUITE = Path(__file__).resolve().with_name("test-changed-tests.sh")


def recording_mktemp(directory, manifest):
    """A mktemp first on PATH that logs each path the real one prints.

    The suite links `command -v mktemp` into its stub bin dir and runs the
    script under test with PATH set to that dir alone, so the wrapper uses only
    Bash built-ins and the real mktemp's absolute path.
    """
    real = shutil.which("mktemp")
    if real is None:
        raise AssertionError("mktemp is not on PATH; the suite needs it")
    wrapper = directory / "mktemp"
    wrapper.write_text(
        "#!/usr/bin/env bash\n"
        f"out=$({shlex.quote(real)} \"$@\") || exit\n"
        f"printf '%s\\n' \"$out\" >>{shlex.quote(str(manifest))}\n"
        "printf '%s\\n' \"$out\"\n"
    )
    wrapper.chmod(0o755)


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
            shim = root / "shim"
            shim.mkdir()
            manifest = root / "mktemp.log"
            recording_mktemp(shim, manifest)
            # Stands in for a file another process on the host drops into
            # TMPDIR: the suite must neither trip over it nor remove it.
            foreign = fixtures / "not-created-by-the-suite"
            foreign.write_text("host-owned\n")
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
                env={
                    **os.environ,
                    "TMPDIR": str(fixtures),
                    "PATH": f"{shim}{os.pathsep}{os.environ.get('PATH', os.defpath)}",
                },
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
            created = manifest.read_text().splitlines() if manifest.exists() else []
            self.assertTrue(
                any(Path(path).is_relative_to(fixtures) for path in created),
                f"recorded no mktemp path under TMPDIR, so cleanup is unchecked: {created}\n{log}",
            )
            leftovers = [path for path in created if os.path.lexists(path)]
            self.assertEqual(leftovers, [], "suite left temporary fixtures")
            self.assertTrue(foreign.exists(), "suite removed a file it did not create")
            self.assertFalse(
                timed_out,
                f"{source}: suite did not exit with stdin open; after closing the source "
                f"it exited {process.returncode}, with no child processes or fixtures left\n{log}",
            )
            self.assertEqual(process.returncode, 0, log)
            self.assertIn("changed-tests tests passed under", log)
            print(f"{source}: exit 0; no child processes or owned fixtures left\n{log}", end="")

    def test_open_pty(self):
        self.run_suite("pty")

    def test_finite_pipe(self):
        self.run_suite("pipe")

    def test_devnull(self):
        self.run_suite("devnull")


if __name__ == "__main__":
    unittest.main()
