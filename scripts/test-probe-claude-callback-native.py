#!/usr/bin/env python3
"""Runner tests use inert system Python only, never the packaged payload."""
import argparse
from contextlib import redirect_stderr
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import selectors
import sys
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("native_probe", Path(__file__).with_name("probe-claude-callback-native.py"))
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)

class ProbeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="native-runner-inert-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def args(self):
        return argparse.Namespace(case="containment", output=self.root / "result", bundle=None,
                                  intentd=None, deadline_seconds=5)

    def test_isolated_python_has_no_site_instrumentation(self):
        self.assertTrue(sys.flags.isolated and sys.flags.no_site)
        self.assertFalse(any(x.startswith("ddtrace") for x in sys.modules))

    def test_accepted_installer_is_used_without_a_manifest_override(self):
        module = probe.load_installer()
        self.assertEqual(module.configuration().identity,
                         "a08355be6f7f7aafd77977b0cdf030dc32f3fdce9178ec1883e0b04d71629597")
        self.assertEqual(module.DESCRIPTOR_SHA256,
                         "592a378105c930444b42ecd2e52bf5c6a017a5539dfe69ef7a94fb570ac9588d")

    def test_modified_installer_is_rejected_before_import(self):
        fake_probe = self.root / "probe-claude-callback-native.py"
        fake = fake_probe.with_name("prepare-claude-callback-runtime.py")
        sentinel = self.root / "must-not-execute"
        fake.write_text("from pathlib import Path\nPath(" + repr(str(sentinel)) + ").touch()\n")
        with patch.object(probe, "__file__", str(fake_probe)), self.assertRaises(probe.Refusal):
            probe.load_installer()
        self.assertFalse(sentinel.exists())

    def test_input_bytes_modes_links_and_size_fail_closed(self):
        file = self.root / "input"
        file.write_bytes(b"inert")
        file.chmod(0o644)
        expected = probe.hash_file(file)
        self.assertEqual(probe.checked_file(file, expected, 5), file)
        for digest, size in (("0" * 64, 5), (expected, 6)):
            with self.subTest(digest=digest), self.assertRaises(probe.Refusal):
                probe.checked_file(file, digest, size)
        file.chmod(0o666)
        with self.assertRaises(probe.Refusal):
            probe.checked_file(file, expected)
        file.chmod(0o644)
        link = self.root / "link"; link.symlink_to(file)
        with self.assertRaises(probe.Refusal):
            probe.checked_file(link, expected)
        link.unlink(); os.link(file, link)
        with self.assertRaises(probe.Refusal):
            probe.checked_file(file, expected)

    def test_missing_or_unsupported_isolation_never_launches(self):
        for scenario in ("missing", "unsupported"):
            with self.subTest(scenario=scenario), patch.object(probe, "run_bounded") as launch:
                with redirect_stderr(io.StringIO()):
                    if scenario == "missing":
                        inputs = copy.deepcopy(probe.OS_INPUTS)
                        inputs["bwrap"]["path"] = str(self.root / "no-bwrap")
                        with patch.object(probe, "OS_INPUTS", inputs):
                            result = probe.main(["--case", "containment", "--output", str(self.root / scenario)])
                    else:
                        with patch.object(probe.sys, "platform", "unsupported"):
                            result = probe.main(["--case", "containment", "--output", str(self.root / scenario)])
                self.assertEqual(result, 1)
                launch.assert_not_called()
                self.assertFalse((self.root / scenario).exists())

    def test_invalid_options_cannot_select_an_arbitrary_program(self):
        invalid = [["--case", "arbitrary"], ["--case", "native", "--command", "/bin/true"],
                   ["--case", "containment", "--manifest", "caller.json"]]
        for extra in invalid:
            with self.subTest(extra=extra), patch.object(probe, "run_bounded") as launch:
                with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    probe.main(extra + ["--output", str(self.root / "out")])
                launch.assert_not_called()
        for value in (0, 46):
            args = self.args(); args.deadline_seconds = value
            with patch.object(probe, "run_bounded") as launch, self.assertRaises(probe.Refusal):
                probe.execute(args)
            launch.assert_not_called()

    def test_payload_options_and_bundle_trust_are_checked_before_containment(self):
        missing = self.args(); missing.case = "native"
        with patch.object(probe, "run_bounded") as launch, self.assertRaises(probe.Refusal):
            probe.execute(missing)
        launch.assert_not_called()
        bad = self.root / "bad-bundle"; bad.write_bytes(b"not the sealed artifact")
        with patch.object(probe, "run_bounded") as launch, redirect_stderr(io.StringIO()):
            result = probe.main(["--case", "native", "--output", str(self.root / "out"),
                                 "--bundle", str(bad), "--intentd", str(self.root / "no-bridge")])
        self.assertEqual(result, 1)
        launch.assert_not_called()
        self.assertFalse((self.root / "out").exists())

    def test_required_namespace_mount_and_environment_contract(self):
        config = {"parent": {}, "hidden": "inert", "sentinel": "inert", "port": 1,
                  "environment": probe.ENV, "payload": False}
        with patch.dict(os.environ, {"NODE_OPTIONS": "--require=/untrusted", "HTTPS_PROXY": "inert"}):
            command = probe.sandbox_command(self.root, config)
        for flag in ("--unshare-user", "--unshare-net", "--unshare-pid", "--unshare-ipc",
                     "--unshare-uts", "--clearenv", "--die-with-parent", "--new-session"):
            self.assertIn(flag, command)
        flags = [arg for arg in command if arg.startswith("--")]
        self.assertFalse(any("-try" in flag or flag == "--share-net" for flag in flags))
        self.assertFalse(any("untrusted" in arg for arg in command))
        self.assertNotIn("--ro-bind", command[command.index("--") + 1:])
        mounts = [command[i + 1:i + 3] for i, x in enumerate(command) if x in ("--bind", "--ro-bind")]
        self.assertFalse(any(source in ("/", "/home", "/etc", str(Path.home())) for source, _ in mounts))
        self.assertNotIn("NODE_OPTIONS", probe.ENV)
        self.assertNotIn("HTTPS_PROXY", probe.ENV)
        self.assertNotIn("LD_PRELOAD", probe.OUTER_ENV)

    def test_failed_inert_proof_has_no_payload_fallback(self):
        refusal = {"stdout": "", "stderr": "fixture namespace denied", "returncode": 1,
                   "live_original_processes_after_cleanup": [], "reason": "exited"}
        with patch.object(probe, "run_bounded", return_value=refusal) as launch:
            result = probe.execute(self.args())
        self.assertEqual(result, 1)
        receipt = json.loads((self.root / "result/receipt.json").read_text())
        self.assertFalse(receipt["native_sandbox_attempted"])
        self.assertEqual(receipt["native_schedules"], "not-reached")
        self.assertEqual(launch.call_count, 1)
        self.assertNotIn("/runtime/node/bin/node", launch.call_args.args[0])

    def test_deadline_cleans_actual_inert_descendant_tree(self):
        command = ["/usr/bin/python3", "-I", "-S", "-B", "-c",
                   "import os,time;child=os.fork();print(os.getpid(),flush=True);time.sleep(20)"]
        result = probe.run_bounded(command, 1)
        self.assertEqual(result["reason"], "deadline")
        self.assertGreaterEqual(len(result["observed_original_processes"]), 2)
        self.assertFalse(result["live_original_processes_after_cleanup"])
        self.assertEqual(result["outer_environment"], probe.OUTER_ENV)

    def test_output_is_bounded_and_original_child_is_reaped(self):
        result = probe.run_bounded(["/usr/bin/python3", "-I", "-S", "-B", "-c",
                                    "import sys,time;sys.stdout.write('x'*65536);sys.stdout.flush();time.sleep(20)"],
                                   2, cap=1024)
        self.assertEqual(result["reason"], "output-limit")
        self.assertEqual(len(result["stdout"].encode()) + len(result["stderr"].encode()), 1024)
        self.assertFalse(result["live_original_processes_after_cleanup"])

    def test_cancellation_cleans_original_inert_process(self):
        with patch.object(selectors.EpollSelector, "select", side_effect=KeyboardInterrupt):
            result = probe.run_bounded(["/usr/bin/python3", "-I", "-S", "-B", "-c",
                                        "import time;time.sleep(20)"], 2)
        self.assertEqual(result["reason"], "cancelled")
        self.assertFalse(result["live_original_processes_after_cleanup"])

if __name__ == "__main__":
    unittest.main(verbosity=2)
