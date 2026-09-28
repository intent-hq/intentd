#!/usr/bin/env python3
"""Offline workflow contracts and executed shell controls (requires PyYAML)."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github/workflows"
LOCK = {"group": "intentd-release-writers", "cancel-in-progress": False, "queue": "max"}


def workflow(name):
    return yaml.safe_load((WORKFLOWS / f"{name}.yml").read_text())


class WorkflowContract(unittest.TestCase):
    def test_all_daemon_writers_share_a_non_cancelling_queue(self):
        for name in ("cleanup-prereleases", "promote-beta", "promote-stable", "mirror-release"):
            with self.subTest(workflow=name):
                doc = workflow(name)
                self.assertEqual(doc["concurrency"], LOCK)
                self.assertTrue(all("concurrency" not in job for job in doc["jobs"].values()))
        release = workflow("v-release")
        self.assertEqual(release["concurrency"], {
            **LOCK,
            "group": "${{ github.event_name == 'pull_request' && format('intentd-release-plan-{0}', github.run_id) || 'intentd-release-writers' }}",
        })
        self.assertTrue(all("concurrency" not in job for job in release["jobs"].values()))

    def test_reusable_calls_cannot_reacquire_lock_or_bypass_parent(self):
        child = workflow("publish-channel-manifest")
        self.assertEqual(set(child[True]), {"workflow_call"})
        self.assertNotIn("concurrency", child)
        self.assertTrue(all("concurrency" not in j for j in child["jobs"].values()))
        callers = []
        for path in WORKFLOWS.glob("*.yml"):
            for job in yaml.safe_load(path.read_text()).get("jobs", {}).values():
                if job.get("uses") == "./.github/workflows/publish-channel-manifest.yml":
                    callers.append(path.stem)
        self.assertEqual(callers, ["v-release"])

    def test_scope_pin_permissions_defaults_and_audit(self):
        doc = workflow("cleanup-prereleases")
        trigger = doc[True]
        self.assertEqual(trigger["schedule"], [{"cron": "17 3 * * *"}])
        mode = trigger["workflow_dispatch"]["inputs"]["mode"]
        self.assertEqual(mode["default"], "preview")
        self.assertEqual(mode["options"], ["preview", "apply"])
        self.assertEqual(doc["permissions"], {"contents": "read"})
        job = doc["jobs"]["cleanup"]
        self.assertNotIn("permissions", job)
        self.assertEqual(job["env"]["GH_TOKEN"], "${{ secrets.PRERELEASE_CLEANUP_TOKEN }}")
        steps = job["steps"]
        checkout = next(s for s in steps if s.get("uses", "").startswith("actions/checkout@"))
        self.assertEqual(checkout["with"]["repository"], "intent-hq/intent")
        self.assertRegex(checkout["with"]["ref"], r"^[0-9a-f]{40}$")
        self.assertEqual(checkout["with"]["token"], "${{ secrets.PRERELEASE_CLEANUP_TOKEN }}")
        self.assertIs(checkout["with"]["persist-credentials"], False)
        self.assertLess(next(i for i, s in enumerate(steps) if s.get("id") == "controls"), steps.index(checkout))
        command = next(s["run"] for s in steps if s.get("id") == "cleanup")
        self.assertIn("--component intentd", command)
        self.assertNotIn("--max-delete", command)  # shared default remains 20
        upload = next(s for s in steps if s.get("uses", "").startswith("actions/upload-artifact@"))
        self.assertEqual(upload["if"], "always()")
        self.assertEqual(upload["with"]["path"], "cleanup-report.json")
        controls = next(s for s in steps if s.get("id") == "controls")
        self.assertEqual(controls["env"], {
            "CLEANUP_ENABLED": "${{ vars.PRERELEASE_CLEANUP_ENABLED }}",
            "REQUESTED_MODE": "${{ inputs.mode || 'preview' }}",
        })
        execute = next(s for s in steps if s.get("id") == "cleanup")
        self.assertEqual(execute["env"]["CLEANUP_MODE"], "${{ steps.controls.outputs.mode }}")
        self.assertEqual(execute["shell"], "bash")  # runner enables pipefail for tee

    def test_operator_contract_records_permissions_and_activation(self):
        text = (WORKFLOWS / "cleanup-prereleases.yml").read_text()
        for requirement in (
            "Contents read on intent-hq/intent", "intent-hq/cloudlands-fe",
            "intent-hq/cloudlands-releases", "Contents write",
            "intent-hq/intentd and intent-hq/intentd-releases", "Drain ALL older",
            "queued runs and old-tag reruns", "final merged monorepo SHA",
            "PRERELEASE_CLEANUP_ENABLED=true", "100-entry limit",
        ):
            self.assertIn(requirement, text)

    def test_generated_workflow_has_reproducible_customization(self):
        path = ROOT / "scripts/configure-release-concurrency.py"
        spec = importlib.util.spec_from_file_location("configure", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        text = (WORKFLOWS / "v-release.yml").read_text()
        self.assertIn(module.BLOCK, text)
        self.assertEqual(module.configure(text), text)
        self.assertEqual(module.configure(text.replace(module.BLOCK, "")), text)
        with self.assertRaises(ValueError):
            module.configure("concurrency: unexpected\njobs:\n")
        self.assertIn("scripts/configure-release-concurrency.py", (ROOT / "dist-workspace.toml").read_text())

    def test_excluded_writers_stay_excluded(self):
        self.assertIn("git_release_enable = false", (ROOT / "release-plz.toml").read_text())
        self.assertIn("sitter-v", (WORKFLOWS / "release-sitter.yml").read_text())
        self.assertNotIn("intentd-release-writers", (WORKFLOWS / "release-sitter.yml").read_text())

    def run_cleanup(self, event, enabled, mode="preview", token="offline-test-token", fail=False):
        job = workflow("cleanup-prereleases")["jobs"]["cleanup"]
        controls = next(s for s in job["steps"] if s.get("id") == "controls")
        command = next(s for s in job["steps"] if s.get("id") == "cleanup")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "monorepo/scripts").mkdir(parents=True)
            (root / "monorepo/scripts/cleanup_prereleases.py").write_text(
                "import json, sys\nprint(json.dumps(sys.argv[1:]))\nsys.exit(" + str(int(fail)) + ")\n"
            )
            output = root / "output"
            output.touch()
            env = {**os.environ, "GITHUB_EVENT_NAME": event, "CLEANUP_ENABLED": enabled,
                   "REQUESTED_MODE": mode, "GH_TOKEN": token, "GITHUB_OUTPUT": str(output)}
            guard = subprocess.run(["bash", "-euo", "pipefail", "-c", controls["run"]],
                                   cwd=root, env=env, capture_output=True, text=True)
            if guard.returncode:
                self.assertFalse((root / "cleanup-report.json").exists())
                return guard, None
            env["CLEANUP_MODE"] = output.read_text().strip().removeprefix("mode=")
            result = subprocess.run(["bash", "-euo", "pipefail", "-c", command["run"]],
                                    cwd=root, env=env, capture_output=True, text=True)
            return result, json.loads((root / "cleanup-report.json").read_text())

    def test_dispatch_and_schedule_activation_matrix(self):
        for event, enabled, mode, apply in [
            ("workflow_dispatch", "", "preview", False),
            ("workflow_dispatch", "true", "preview", False),
            ("workflow_dispatch", "true", "apply", True),
            ("schedule", "", "preview", False),
            ("schedule", "false", "preview", False),
            ("schedule", "TRUE", "preview", False),
            ("schedule", "true", "preview", True),
        ]:
            with self.subTest(event=event, enabled=enabled, mode=mode):
                result, args = self.run_cleanup(event, enabled, mode)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(args, ["--component", "intentd"] + (["--apply"] if apply else []))

    def test_disabled_manual_apply_and_invalid_mode_fail_before_command(self):
        for enabled, mode in [("", "apply"), ("false", "apply"), ("TRUE", "apply"), ("true", "bad")]:
            result, args = self.run_cleanup("workflow_dispatch", enabled, mode)
            self.assertNotEqual(result.returncode, 0)
            self.assertIsNone(args)

    def test_missing_token_never_runs_command(self):
        for event, mode in [("schedule", "preview"), ("workflow_dispatch", "apply"), ("workflow_dispatch", "preview")]:
            result, args = self.run_cleanup(event, "true", mode, token="")
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("PRERELEASE_CLEANUP_TOKEN", result.stdout + result.stderr)
            self.assertIsNone(args)

    def test_command_failure_preserves_failure_and_audit(self):
        result, args = self.run_cleanup("workflow_dispatch", "true", "apply", fail=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(args, ["--component", "intentd", "--apply"])


if __name__ == "__main__":
    unittest.main()
