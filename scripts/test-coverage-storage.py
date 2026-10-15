#!/usr/bin/env python3
"""Small filesystem and subprocess proofs; no Cargo builds or network."""
import importlib.util
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).with_name("coverage-storage.py")
spec = importlib.util.spec_from_file_location("storage", SCRIPT)
storage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(storage)


class StorageTests(unittest.TestCase):
    def configure(self, root):
        output = root / "evidence"
        output.mkdir()
        target = root / "target"
        target.mkdir()
        (output / "paths.json").write_text(json.dumps({
            "workspace": str(root), "target": str(target), "profiles": str(target),
            "home": str(root), "temporary": str(root)}))
        return output, target

    def test_bounded_counts_do_not_follow_symlinks_or_expose_names(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output, target = self.configure(root)
            (target / "private-name.profraw").write_bytes(b"12345")
            (target / "binary").write_bytes(b"123")
            (root / "outside").mkdir()
            (root / "outside/secret").write_bytes(b"x" * 100)
            (target / "link").symlink_to(root / "outside", target_is_directory=True)
            usage = storage.directory_usage(target)
            self.assertTrue(usage["complete"])
            self.assertEqual((usage["files"], usage["bytes"], usage["profiles"], usage["profile_bytes"]),
                             (2, 8, 1, 5))
            self.assertFalse(storage.directory_usage(target, max_entries=1)["complete"])
            self.assertFalse(storage.directory_usage(target, seconds=0)["complete"])
            storage.record(output, "scope-1", "start")
            text = (output / "resources.jsonl").read_text()
            self.assertNotIn("private-name", text)
            self.assertNotIn("secret", text)
            row = json.loads(text)
            self.assertGreater(row["filesystems"]["workspace"]["capacity_bytes"], 0)
            self.assertIn("free_inodes", row["filesystems"]["workspace"])

    def test_partial_or_missing_directory_is_not_reported_complete(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertFalse(storage.directory_usage(Path(tmp) / "absent")["complete"])
            self.assertGreater(storage.filesystem_usage(Path(tmp) / "absent")["available_bytes"], 0)

    def test_observer_preserves_stdio_policy_and_failing_exit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output, _ = self.configure(root)
            code = ("import os,sys; assert sys.stdin.read()=='payload'; "
                    "assert os.environ['INTENTD_ASSERT_BOUND_CALLER']=='1'; "
                    "print('child-output'); sys.exit(23)")
            result = subprocess.run([sys.executable, str(SCRIPT), "run", "--output", str(output),
                                     "--stage", "scope-1", "--", sys.executable, "-c", code],
                                    env={**os.environ, "INTENTD_ASSERT_BOUND_CALLER": "1"},
                                    input="payload", capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 23, result.stderr)
            self.assertEqual(result.stdout.strip(), "child-output")
            rows = [json.loads(line) for line in (output / "resources.jsonl").read_text().splitlines()]
            self.assertEqual([r["event"] for r in rows], ["start", "exit"])
            self.assertEqual(rows[-1]["exit_code"], 23)

    def test_unwritable_evidence_does_not_replace_child_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "not-a-directory"
            path.write_text("fixture")
            self.assertEqual(storage.run(path, "scope", [sys.executable, "-c", "raise SystemExit(7)"]), 7)

    def test_periodic_observation_and_signal_exit_without_sending_signals(self):
        with patch.object(storage.subprocess, "Popen") as spawn, patch.object(storage, "record") as record:
            spawn.return_value.wait.side_effect = [subprocess.TimeoutExpired("fixture", 30), -15]
            self.assertEqual(storage.run(Path("unused"), "scope", ["fixture"]), 143)
            self.assertEqual([c.args[2] for c in record.call_args_list], ["start", "periodic", "exit"])
            spawn.return_value.kill.assert_not_called()
            spawn.return_value.terminate.assert_not_called()
            spawn.assert_called_once_with(["fixture"])

    def test_init_uses_resolved_instrumented_target_and_only_saves_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = (root / "custom target").resolve()
            profile = target / "profiles/intent-%8m.profraw"
            def command(argv, **kwargs):
                if argv[1] == "metadata":
                    return json.dumps({"target_directory": str(root / "normal")}).encode()
                self.assertEqual(kwargs["env"]["CARGO_TARGET_DIR"], str(target))
                return "export LLVM_PROFILE_FILE=" + shlex.quote(str(profile)) + ";\nexport SECRET=never-save\n"
            with patch.dict(os.environ, {"RUNNER_TEMP": tmp, "CARGO_LLVM_COV_TARGET_DIR": str(target)}), \
                    patch.object(storage.subprocess, "check_output", side_effect=command):
                storage.initialize(root / "evidence")
            paths = json.loads((root / "evidence/paths.json").read_text())
            self.assertEqual(paths["target"], str(target))
            self.assertEqual(paths["profiles"], str(profile.parent))
            self.assertNotIn("SECRET", str(paths))


if __name__ == "__main__":
    unittest.main()
