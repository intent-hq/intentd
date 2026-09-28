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
import signal
import subprocess
import time
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
        budget = patch.object(probe, "process_limit", return_value=4096)
        budget.start(); self.addCleanup(budget.stop)

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


class CleanupOwnershipTests(unittest.TestCase):
    """Only original inert children are real; numeric reuse is instrumented."""
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="native-cleanup-inert-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        budget = patch.object(probe, "process_limit", return_value=4096)
        budget.start(); self.addCleanup(budget.stop)

    @staticmethod
    def command(code):
        return ["/usr/bin/python3", "-I", "-S", "-B", "-c", code]

    def test_reaped_root_reappearance_is_never_adopted_or_signalled(self):
        real_popen = subprocess.Popen
        state = {"reaped": False}
        original = {}
        signals = []

        class Original:
            def __init__(self, child):
                self.child = child
                self.pid, self.stdout, self.stderr = child.pid, child.stdout, child.stderr
            @property
            def returncode(self):
                return self.child.returncode
            def poll(self):
                value = self.child.poll()
                if value is not None:
                    state["reaped"] = True
                return value
            def wait(self, timeout=None):
                value = self.child.wait(timeout=timeout)
                state["reaped"] = True
                return value

        def start(*args, **kwargs):
            child = real_popen(*args, **kwargs)
            original["child"] = child
            until = time.monotonic() + 2
            while os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is None:
                if time.monotonic() >= until:
                    raise AssertionError("inert original did not exit")
                time.sleep(.005)
            return Original(child)

        def observed(pid):
            self.assertEqual(pid, original["child"].pid)
            return {"start": "replacement-root" if state["reaped"] else "original-root",
                    "state": "S" if state["reaped"] else "Z", "parent": os.getpid()}

        try:
            with patch.object(probe.subprocess, "Popen", side_effect=start), \
                 patch.object(probe, "process_state", side_effect=observed), \
                 patch.object(probe.os, "killpg", side_effect=lambda pid, sig: signals.append(("group", pid, state["reaped"]))), \
                 patch.object(probe.os, "kill", side_effect=lambda pid, sig: signals.append(("pid", pid, state["reaped"]))):
                result = probe.run_bounded(self.command("pass"), 1)
            self.assertEqual(result["returncode"], 0)
            self.assertFalse([x for x in signals if x[2]], "cleanup selected the simulated replacement allocation")
            self.assertEqual(result["observed_original_processes"],
                             {str(original["child"].pid): "original-root"})
        finally:
            if "child" in original:
                original["child"].wait(timeout=2)

    def test_sampled_descendant_identity_is_immutable_and_signals_use_owned_handles(self):
        real_popen, real_state = subprocess.Popen, probe.process_state
        real_select, real_killpg = selectors.EpollSelector.select, os.killpg
        ready = self.root / "ready"
        original = {}
        phase = {"replacement": False}
        numeric_signals = []

        def start(*args, **kwargs):
            child = real_popen(*args, **kwargs)
            original["root"] = child
            until = time.monotonic() + 2
            while not ready.exists() or not ready.read_text().strip():
                if time.monotonic() >= until:
                    raise AssertionError("inert descendant did not become ready")
                time.sleep(.005)
            pid = int(ready.read_text())
            original["descendant"] = pid
            original["fd"] = os.pidfd_open(pid)
            original["start"] = real_state(pid)["start"]
            return child

        def observed(pid):
            value = real_state(pid)
            if pid == original.get("descendant") and value is not None and phase["replacement"]:
                return {**value, "start": "simulated-replacement-child"}
            return value

        def select(selector, timeout=None):
            result = real_select(selector, timeout)
            # The real loop samples its original children before its first select.
            phase["replacement"] = True
            return result

        def group(pid, sig):
            self.assertEqual(pid, original["root"].pid)
            real_killpg(pid, sig)

        code = ("import os,time; child=os.fork(); "
                + "\nif child==0:\n os.setsid()\n open(" + repr(str(ready)) + ", 'w').write(str(os.getpid()))"
                + "\ntime.sleep(20)")
        try:
            with patch.object(probe.subprocess, "Popen", side_effect=start), \
                 patch.object(probe, "process_state", side_effect=observed), \
                 patch.object(selectors.EpollSelector, "select", select), \
                 patch.object(probe.os, "killpg", side_effect=group), \
                 patch.object(probe.os, "kill", side_effect=lambda pid, sig: numeric_signals.append(pid)):
                result = probe.run_bounded(self.command(code), .12)
            self.assertEqual(result["reason"], "deadline")
            self.assertEqual(result["observed_original_processes"].get(str(original["descendant"])),
                             original["start"], "sampled original identity was overwritten")
            self.assertFalse(numeric_signals, "descendant cleanup used a reusable numeric PID")
            self.assertFalse(result["live_original_processes_after_cleanup"])
        finally:
            # This test-owned pidfd always identifies the actual inert child;
            # simulated replacement PIDs are NEVER sent a real signal.
            if "fd" in original:
                try:
                    signal.pidfd_send_signal(original["fd"], signal.SIGKILL)
                except ProcessLookupError:
                    pass
                os.close(original["fd"])
            child = original.get("root")
            if child is not None:
                if child.returncode is None:
                    try:
                        real_killpg(child.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                child.wait(timeout=2)

    def test_normal_and_already_exited_originals_join_without_rebinding(self):
        for code in ("pass", "print('original',flush=True)", "pass"):
            with self.subTest(code=code):
                result = probe.run_bounded(self.command(code), 2)
                self.assertEqual((result["reason"], result["returncode"]), ("exited", 0))
                self.assertFalse(result["live_original_processes_after_cleanup"])

    def test_closed_pipes_keep_original_alive_until_completion_or_deadline(self):
        for duration, budget, expected in ((.08, 2, "exited"), (20, .12, "deadline")):
            with self.subTest(expected=expected):
                result = probe.run_bounded(self.command(
                    f"import os,time;os.close(1);os.close(2);time.sleep({duration})"), budget)
                self.assertEqual(result["reason"], expected)
                self.assertFalse(result["live_original_processes_after_cleanup"])
                if expected == "exited":
                    self.assertEqual(result["returncode"], 0)
                    self.assertGreaterEqual(result["elapsed_seconds"], .07)

    def test_repeated_cleanup_is_idempotent_and_closes_owned_handles(self):
        child = subprocess.Popen(self.command("import time;time.sleep(20)"),
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 env=probe.OUTER_ENV, start_new_session=True)
        owner = probe.OriginalProcessTree(child)
        handles = []
        try:
            owner.capture_root()
            handles = list(owner.handles.values())
            real_killpg = os.killpg
            with patch.object(probe.os, "killpg", wraps=real_killpg) as signal_group:
                self.assertEqual(owner.finish(), [])
                self.assertEqual(owner.finish(), [])
            self.assertEqual(signal_group.call_count, 1)
            self.assertTrue(owner.closed)
            self.assertEqual(owner.handles, {})
            for fd in handles:
                with self.assertRaises(OSError):
                    os.fstat(fd)
        finally:
            owner.finish()
            child.stdout.close(); child.stderr.close()

    def test_exception_after_handle_capture_closes_and_joins_original(self):
        real_open = os.pidfd_open
        handles = []
        def opened(pid, flags=0):
            fd = real_open(pid, flags); handles.append(fd); return fd
        with patch.object(probe.os, "pidfd_open", side_effect=opened), \
             patch.object(selectors.EpollSelector, "register", side_effect=RuntimeError("inert selector fixture")):
            with self.assertRaisesRegex(RuntimeError, "inert selector fixture"):
                probe.run_bounded(self.command("import time;time.sleep(20)"), 2)
        self.assertTrue(handles)
        for fd in handles:
            with self.assertRaises(OSError):
                os.fstat(fd)

    def test_sampling_failure_still_joins_and_repeated_cleanup_retains_failure(self):
        child = subprocess.Popen(self.command("import time;time.sleep(20)"),
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 env=probe.OUTER_ENV, start_new_session=True)
        owner = probe.OriginalProcessTree(child)
        error = RuntimeError("inert sample failure")
        try:
            owner.capture_root()
            handles = list(owner.handles.values())
            real_killpg = os.killpg
            with patch.object(owner, "sample", side_effect=error), \
                 patch.object(probe.os, "killpg", wraps=real_killpg) as group:
                for _ in range(2):
                    with self.assertRaises(RuntimeError) as raised:
                        owner.finish()
                    self.assertIs(raised.exception, error)
            self.assertEqual(group.call_count, 1)
            self.assertIsNotNone(child.returncode)
            self.assertTrue(owner.closed)
            self.assertFalse(owner.handles)
            for fd in handles:
                with self.assertRaises(OSError):
                    os.fstat(fd)
        finally:
            child.stdout.close(); child.stderr.close()

    def test_unvalidated_root_handle_is_closed_without_becoming_owned(self):
        child = subprocess.Popen(self.command("import time;time.sleep(20)"),
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 env=probe.OUTER_ENV, start_new_session=True)
        owner = probe.OriginalProcessTree(child)
        real_open = os.pidfd_open
        opened = []
        def capture(pid, flags=0):
            fd = real_open(pid, flags); opened.append(fd); return fd
        try:
            with patch.object(owner, "root_exited", side_effect=[False, probe.Refusal("inert lost child")]), \
                 patch.object(probe.os, "pidfd_open", side_effect=capture):
                with self.assertRaisesRegex(probe.Refusal, "inert lost child"):
                    owner.capture_root()
            self.assertEqual(owner.handles, {})
            self.assertEqual(owner.seen, {})
            for fd in opened:
                with self.assertRaises(OSError):
                    os.fstat(fd)
        finally:
            owner.finish()
            child.stdout.close(); child.stderr.close()

class ServicesStartupRunnerTests(unittest.TestCase):
    """Offline parser/launch controls; synthetic milestones are never native proof."""
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="services-runner-inert-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def events(self, case):
        facts = {
            "runner-verified": {"case": case, "testElf": probe.SERVICES_SHA,
                                "bridge": probe.BRIDGE_SHA,
                                "runtimeIdentity": "a08355be6f7f7aafd77977b0cdf030dc32f3fdce9178ec1883e0b04d71629597"},
            "ordinary-started": {"legacy": case == "legacy", "sessionId": "inert-session",
                                 "agent": "inert-agent", "workspace": "inert-workspace",
                                 "adapterPid": 10, "pendingEndpoint": "127.0.0.1:1"},
            "original-acknowledged": {"sessionId": "inert-session", "confirmedEndpoint": "127.0.0.1:2",
                                      "sameServicesReadAnchorConnection": True,
                                      "receipt": "Acknowledged inert-session"},
            "original-reused": {"sameConnectionOriginSession": True, "distinctOwnedCaptures": True,
                                "registrationCount": 1},
            "owned-retired": {"legacy": case == "legacy", "handleAbsent": True,
                              "originalTransportClosed": True, "contextJobs": "none prepared; drain completed"},
            "completed": {"case": case},
        }
        return [{"nativeServicesStartup": 1, "event": event, "facts": facts[event]}
                for event in probe.SERVICES_EVENTS[case]]

    def result(self, case, events=None):
        events = self.events(case) if events is None else events
        text = "running 1 test\ntest " + probe.SERVICES_SELECTORS[case] + " ... "
        text += "\n".join(json.dumps(event) for event in events)
        text += "\nok\n\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 123 filtered out; finished in 1.00s\n"
        return {"stdout": text, "stderr": "", "containment_passed": True,
                "returncode": 0, "reason": "exited", "elapsed_seconds": 1,
                "live_original_processes_after_cleanup": []}

    def test_closed_cases_have_only_two_exact_rust_selectors(self):
        for case in ("confirmed", "legacy"):
            self.assertEqual(probe.services_argv(case), ["/probe/services-test",
                "agent_manager::repository_origin::callback_delivery::tests::genuine_native_startup::normal_services_native_" + case + "_startup",
                "--exact", "--ignored", "--nocapture", "--test-threads=1"])
        for option in ("--selector", "--services-elf", "--environment", "--mount", "--command"):
            with self.subTest(option=option), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                probe.parser().parse_args(["--case", "services-confirmed", "--output", str(self.root), option, "inert"])
        for case in ("", "native", "confirmed --list", "anything"):
            with self.subTest(case=case), self.assertRaises(probe.Refusal):
                probe.services_argv(case)

    def test_contract_has_only_original_namespaces_case_and_independent_pin(self):
        original = {name: name + ":[123]" for name in probe.NS_NAMES}
        for case in ("confirmed", "legacy"):
            data = probe.services_contract(case, original)
            self.assertLessEqual(len(data), 4096)
            self.assertEqual(json.loads(data), {"format": "intent-services-native-startup-v1",
                "case": case, "test_elf_sha256": probe.SERVICES_SHA, "host_namespaces": original})
        for invalid in ({}, {**original, "extra": "net:[123]"}, {**original, "net": "caller"}):
            with self.subTest(invalid=invalid), self.assertRaises(probe.Refusal):
                probe.services_contract("confirmed", invalid)

    def test_wrong_elf_bytes_size_mode_and_links_refuse_without_execution(self):
        file = self.root / "inert-elf"
        file.write_bytes(b"\x7fELF\x02\x01inert")
        file.chmod(0o555)
        with patch.object(probe, "SERVICES_ELF", file), patch.object(probe, "run_bounded") as launch:
            with self.assertRaises(probe.Refusal):
                probe.check_services_elf()
            with patch.object(probe, "SERVICES_SHA", probe.hash_file(file)), patch.object(probe, "SERVICES_BYTES", file.stat().st_size):
                self.assertEqual(probe.check_services_elf(), file)
                file.chmod(0o755)
                with self.assertRaises(probe.Refusal): probe.check_services_elf()
                file.chmod(0o555)
                link = self.root / "link"; os.link(file, link)
                with self.assertRaises(probe.Refusal): probe.check_services_elf()
                link.unlink()
                file.unlink(); file.symlink_to(self.root / "missing")
                with self.assertRaises(probe.Refusal): probe.check_services_elf()
            launch.assert_not_called()

    def test_caller_manifest_cannot_replace_compiled_input_authority(self):
        path = self.root / "compile-v4/before.json"
        path.parent.mkdir(); path.write_text('{}\n')
        with patch.object(probe, "SERVICES_PREPARATION", self.root), patch.object(probe, "check_services_elf"), patch.object(probe, "run_bounded") as launch:
            with self.assertRaises(probe.Refusal): probe.check_services_inputs()
            launch.assert_not_called()

    def test_services_launch_has_only_pinned_extra_mounts_and_exact_environment(self):
        for case in ("confirmed", "legacy"):
            config = {"services_case": case, "services_argv": probe.services_argv(case),
                      "environment": probe.SERVICES_ENV, "payload": True}
            command = probe.sandbox_command(self.root, config, self.root / "payload", self.root / "bridge")
            mounts = [command[i+1:i+3] for i, value in enumerate(command) if value == "--ro-bind"]
            self.assertEqual(mounts[-2:], [[str(probe.SERVICES_ELF), "/probe/services-test"],
                [str(self.root / "services-contract.json"), "/probe/services-contract.json"]])
            self.assertEqual(len(mounts), len(probe.OS_INPUTS["mounts"]) + 5)
            self.assertNotIn("/probe/client.mjs", command)
            self.assertEqual({command[i+1]: command[i+2] for i, value in enumerate(command) if value == "--setenv"}, probe.SERVICES_ENV)
            self.assertEqual(command[-5:-1], ["-I", "-S", "-B", "-c"])
            for key, value in (("services_argv", ["/bin/arbitrary"]), ("environment", {"HOME": "/host"})):
                bad = {**config, key: value}
                with self.assertRaises(probe.Refusal):
                    probe.sandbox_command(self.root, bad, self.root / "payload", self.root / "bridge")

    def test_both_complete_ordered_result_shapes_are_recognized_offline(self):
        for case in ("confirmed", "legacy"):
            self.assertEqual(probe.validate_services_result(case, self.result(case), 45), self.events(case))

    def test_missing_duplicate_unknown_wrong_case_or_reordered_milestones_refuse(self):
        for case in ("confirmed", "legacy"):
            events = self.events(case)
            for i in range(len(events)):
                for variant in (events[:i] + events[i+1:], events[:i] + [events[i]] + events[i:],
                                events[:i] + [{**events[i], "event": "unexpected"}] + events[i+1:]):
                    with self.subTest(case=case, i=i), self.assertRaises(probe.Refusal):
                        probe.validate_services_result(case, self.result(case, variant), 45)
            with self.assertRaises(probe.Refusal):
                probe.validate_services_result(case, self.result(case, list(reversed(events))), 45)
            with self.assertRaises(probe.Refusal):
                probe.validate_services_result(case, self.result("legacy" if case == "confirmed" else "confirmed"), 45)

    def test_original_facts_cannot_be_replaced_by_an_early_or_foreign_success(self):
        changes = {"runner-verified": {"testElf": "foreign"},
                   "ordinary-started": {"sessionId": ""},
                   "original-acknowledged": {"sessionId": "foreign"},
                   "original-reused": {"registrationCount": 2},
                   "owned-retired": {"originalTransportClosed": False},
                   "completed": {"case": "legacy"}}
        for event, update in changes.items():
            events = self.events("confirmed")
            next(item for item in events if item["event"] == event)["facts"].update(update)
            with self.subTest(event=event), self.assertRaises(probe.Refusal):
                probe.validate_services_result("confirmed", self.result("confirmed", events), 45)
        for update in ({"receipt": "Refused"}, {"receipt": "Acknowledged " + "x"*8192},
                       {"confirmedEndpoint": "127.0.0.1:1"}):
            events = self.events("confirmed"); events[2]["facts"].update(update)
            with self.assertRaises(probe.Refusal):
                probe.validate_services_result("confirmed", self.result("confirmed", events), 45)

    def test_nonzero_timeout_cancellation_or_survivor_cannot_count_as_pass(self):
        for case in ("confirmed", "legacy"):
            for update in ({"returncode": 1}, {"returncode": -9}, {"reason": "deadline"},
                           {"reason": "cancelled"}, {"reason": "output-limit"}, {"elapsed_seconds": 46},
                           {"containment_passed": False}, {"live_original_processes_after_cleanup": [123]}):
                with self.subTest(case=case, update=update), self.assertRaises(probe.Refusal):
                    probe.validate_services_result(case, {**self.result(case), **update}, 45)

    def test_missing_wrong_or_multiple_libtest_summaries_refuse(self):
        result = self.result("confirmed")
        for text in (result["stdout"].replace("test result:", "not a result:"),
                     result["stdout"].replace("1 passed;", "0 passed;"),
                     result["stdout"] + result["stdout"],
                     result["stdout"].replace(probe.SERVICES_SELECTORS["confirmed"], "another::case")):
            with self.assertRaises(probe.Refusal):
                probe.validate_services_result("confirmed", {**result, "stdout": text}, 45)

    def test_malformed_or_oversized_transcripts_refuse(self):
        for text in ('{"nativeServicesStartup":', json.dumps({"nativeServicesStartup": True, "event": "x", "facts": {}}),
                     json.dumps({"nativeServicesStartup": 1, "event": "x", "facts": {"large": "x"*16384}})):
            with self.assertRaises(probe.Refusal): probe.services_milestones(text)

    def test_missing_services_inputs_refuse_before_containment(self):
        args = argparse.Namespace(case="services-confirmed", output=self.root / "out", bundle=None,
                                  intentd=None, deadline_seconds=45)
        with patch.object(probe, "run_bounded") as launch, self.assertRaises(probe.Refusal):
            probe.execute(args)
        launch.assert_not_called()

if __name__ == "__main__":
    unittest.main(verbosity=2)
