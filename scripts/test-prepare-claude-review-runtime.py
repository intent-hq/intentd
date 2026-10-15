#!/usr/bin/env python3
"""Explicit offline qualification of the real pinned review bundle.

Does not enable a provider or send a model prompt. The bundled containment
version command and default-off adapter handshake execute after authentication.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import selectors
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest

SPEC = importlib.util.spec_from_file_location(
    "review_preparer", Path(__file__).with_name("prepare-claude-review-runtime.py"))
runtime = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runtime
SPEC.loader.exec_module(runtime)
BUNDLE = None


class ReviewBundleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contract = runtime.configuration()
        runtime.archive.check_bundle(cls.contract, BUNDLE)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="review-runtime-qualification-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_descriptor_tamper_is_refused(self):
        copied = self.root / "scripts"
        copied.mkdir()
        for name in ("prepare-claude-review-runtime.py", "prepare-claude-callback-runtime.py",
                     "claude-review-runtime.json"):
            shutil.copyfile(Path(__file__).with_name(name), copied / name)
        with (copied / "claude-review-runtime.json").open("ab") as stream:
            stream.write(b" ")
        result = subprocess.run([sys.executable, str(copied / "prepare-claude-review-runtime.py"),
                                 "verify", "--root", str(self.root / "unused")],
                                capture_output=True, text=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("untrusted review descriptor", result.stderr)
        self.assertFalse((self.root / "unused").exists())

    def test_archive_tamper_refused_before_install(self):
        altered = self.root / "altered.tar.gz"
        shutil.copyfile(BUNDLE, altered)
        with altered.open("r+b") as stream:
            stream.seek(100)
            byte = stream.read(1)
            stream.seek(100)
            stream.write(bytes([byte[0] ^ 1]))
        target = self.root / "install"
        with self.assertRaises(runtime.archive.InvalidRuntime):
            runtime.archive.install(self.contract, altered, target)
        self.assertFalse(target.exists())

    def test_relocation_bundled_containment_and_owned_removal(self):
        original = self.root / "original"
        runtime.archive.install(self.contract, BUNDLE, original)
        moved = self.root / "relocated with spaces"
        original.rename(moved)
        destination = moved / self.contract.identity
        runtime.archive.verify_install(self.contract, destination)
        result = subprocess.run([str(destination / "bin/review-containment"), "--version"],
                                env={"PATH": "/usr/bin:/bin"}, capture_output=True,
                                text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "bubblewrap 0.9.0")
        with self.assertRaises(runtime.archive.InvalidRuntime):
            runtime.archive.remove(self.contract, moved, self.contract.identity, False)
        self.assertTrue(destination.exists())
        runtime.archive.remove(self.contract, moved, self.contract.identity, True)
        self.assertFalse(destination.exists())

    def test_warm_install_refuses_changed_containment_without_repair(self):
        target = self.root / "install"
        runtime.archive.install(self.contract, BUNDLE, target)
        binary = target / self.contract.identity / "containment/bin/bwrap"
        with binary.open("ab") as stream:
            stream.write(b"changed")
        changed = runtime.archive.digest_file(binary)
        with self.assertRaises(runtime.archive.InvalidRuntime):
            runtime.archive.install(self.contract, BUNDLE, target)
        self.assertEqual(runtime.archive.digest_file(binary), changed)

    def test_installed_ordinary_launcher_refuses_private_startup(self):
        launcher = runtime.archive.install(self.contract, BUNDLE, self.root / "install")
        home = self.root / "private-home"
        home.mkdir()
        env = {"PATH": "/usr/bin:/bin", "HOME": str(home), "CLAUDE_CONFIG_DIR": str(home),
               "XDG_CONFIG_HOME": str(home), "TMPDIR": str(home), "NODE_OPTIONS": "",
               "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1", "DISABLE_TELEMETRY": "1",
               "DISABLE_AUTOUPDATER": "1", "DD_INJECTION_ENABLED": "false",
               "DD_TRACE_ENABLED": "false", "DD_INJECT_NATIVE": "never"}
        with (self.root / "adapter.stderr").open("wb") as stderr:
            child = subprocess.Popen([str(launcher)], cwd=home, env=env,
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr,
                                     start_new_session=True)
            try:
                with selectors.DefaultSelector() as ready:
                    ready.register(child.stdout, selectors.EVENT_READ)
                    pending = bytearray()

                    def request(identifier, method, params):
                        child.stdin.write((json.dumps({"jsonrpc": "2.0", "id": identifier,
                                                       "method": method, "params": params}) + "\n").encode())
                        child.stdin.flush()
                        deadline = time.monotonic() + 15
                        while True:
                            while b"\n" in pending:
                                line, _, rest = pending.partition(b"\n")
                                pending[:] = rest
                                frame = json.loads(line)
                                if frame.get("id") == identifier:
                                    return frame
                            self.assertTrue(ready.select(max(0, deadline - time.monotonic())),
                                            "installed adapter response timed out")
                            chunk = child.stdout.read1(65536)
                            self.assertTrue(chunk, "installed adapter closed before response")
                            pending.extend(chunk)

                    initialized = request(1, "initialize", {"protocolVersion": 1,
                        "clientCapabilities": {"_meta": {"reviewMonitoring": 1,
                        "intentCallbackRegistration": {"version": 1}}}})
                    self.assertIn("result", initialized)
                    self.assertNotIn("reviewMonitoring", initialized["result"].get("_meta", {}))
                    private = request(2, "session/new", {"cwd": str(home), "mcpServers": [],
                        "_meta": {"reviewMonitoringContext": {"version": 1,
                        "ownerKey": "ordinary-launcher-control", "loadAttemptId": "original-attempt"}}})
                    self.assertIn("error", private)
                    ordinary = request(3, "initialize", {"protocolVersion": 1, "clientCapabilities": {}})
                    self.assertIn("result", ordinary)
                    self.assertNotIn("reviewMonitoring", ordinary["result"].get("_meta", {}))
            finally:
                # Initialization can spawn CLI probes. Retire this test's entire
                # process group before deleting the private home they can write.
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                child.wait(timeout=10)
                child.stdin.close()
                child.stdout.close()
                deadline = time.monotonic() + 10
                while True:
                    live = []
                    for entry in Path("/proc").iterdir():
                        if not entry.name.isdigit():
                            continue
                        try:
                            fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
                            if int(fields[2]) == child.pid and fields[0] not in ("Z", "X"):
                                live.append(entry.name)
                        except (FileNotFoundError, ProcessLookupError):
                            continue
                    if not live:
                        break
                    self.assertLess(time.monotonic(), deadline,
                                    f"owned adapter process group did not exit: {live}")
                    time.sleep(0.01)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", required=True, type=Path)
    args = parser.parse_args()
    BUNDLE = args.bundle.resolve(strict=True)
    unittest.main(argv=[sys.argv[0]], verbosity=2)
