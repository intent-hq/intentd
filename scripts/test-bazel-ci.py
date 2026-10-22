#!/usr/bin/env python3
"""Unit tests for the Bazel CI helpers: scripts/bazel-drift.py and
bazel/remote-cache-credentials.sh (no Bazel needed)."""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("bazel_drift", ROOT / "scripts/bazel-drift.py")
drift = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(drift)


class HostPaths(unittest.TestCase):
    def test_flags_checkout_and_home_paths(self):
        text = '{"path": "/srv/ci/intentd/vendor/pprof", "x": "/home/dev/.cache/y", "m": "/Users/me/src"}'
        self.assertEqual(
            drift.host_paths(text, ["/srv/ci/intentd"]),
            ["/Users/me/src", "/home/dev/.cache/y", "/srv/ci/intentd"],
        )

    def test_ignores_relative_paths_and_urls(self):
        text = '{"path": "vendor/pprof", "url": "https://example.com/home/x", "label": "@@//vendor/pprof"}'
        self.assertEqual(drift.host_paths(text, ["/srv/ci/intentd", "/"]), [])

    def test_committed_lock_is_portable(self):
        self.assertEqual(drift.check_lock_paths(), [])


class CheckAndFix(unittest.TestCase):
    def run_main(self, argv, results):
        calls = []

        def fake_run(cmd):
            calls.append(cmd)
            return results.pop(0) if results else 0

        err = io.StringIO()
        with mock.patch.object(drift, "run", fake_run), mock.patch.object(drift, "check_lock_paths", return_value=[]), \
                mock.patch.object(drift, "bazel", return_value="bazel"), contextlib.redirect_stderr(err), \
                contextlib.redirect_stdout(io.StringIO()):
            code = drift.main(argv)
        return code, calls, err.getvalue()

    def test_clean_tree_passes(self):
        code, calls, _ = self.run_main([], [0, 0])
        self.assertEqual(code, 0)
        self.assertEqual(calls[0][1:], ["scripts/bazel-gen.py", "--check"])
        self.assertEqual(calls[1][:3], ["bazel", "mod", "deps"])
        self.assertIn("--lockfile_mode=error", calls[1])

    def test_stale_lock_fails_with_fix_command(self):
        code, _, err = self.run_main([], [0, 1])
        self.assertEqual(code, 1)
        self.assertIn("MODULE.bazel.lock is out of date", err)
        self.assertIn(drift.FIX_COMMAND, err)

    def test_stale_generated_files_fail_before_bazel(self):
        code, calls, err = self.run_main([], [1])
        self.assertEqual(code, 1)
        self.assertEqual(len(calls), 1)
        self.assertIn("bazel-gen.py --check", err)

    def test_fix_regenerates_then_refreshes_lock_then_rechecks(self):
        code, calls, _ = self.run_main(["--fix"], [])
        self.assertEqual(code, 0)
        self.assertEqual(calls[0][1:], ["scripts/bazel-gen.py"])
        self.assertIn("--lockfile_mode=update", calls[1])
        self.assertIn("--check", calls[2])
        self.assertIn("--lockfile_mode=error", calls[3])

    def test_missing_lock_is_reported(self):
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(drift, "LOCK", Path(tmp) / "MODULE.bazel.lock"):
            self.assertEqual(drift.check_lock_paths(), ["MODULE.bazel.lock is missing"])


class RemoteCacheCredentials(unittest.TestCase):
    HELPER = ROOT / "bazel/remote-cache-credentials.sh"

    def headers(self, uri, **env):
        child_env = {k: v for k, v in os.environ.items() if not k.startswith("BAZEL_REMOTE_CACHE_")}
        child_env.update(env)
        # env-policy: no-python-child: the helper is a POSIX shell script
        out = subprocess.run([str(self.HELPER), "get"], input=json.dumps({"uri": uri}), env=child_env,
                             capture_output=True, text=True, check=True).stdout
        return json.loads(out)["headers"]

    def test_token_goes_only_to_the_cache_host(self):
        env = {"BAZEL_REMOTE_CACHE_URL": "https://cache.example:8443/bazel", "BAZEL_REMOTE_CACHE_TOKEN": "t0k"}
        self.assertEqual(self.headers("https://cache.example:8443/bazel/ac/abc", **env),
                         {"Authorization": ["Bearer t0k"]})
        self.assertEqual(self.headers("https://static.crates.io/crates/x.crate", **env), {})
        self.assertEqual(self.headers("https://cache.example.evil/bazel", **env), {})

    def test_no_token_or_endpoint_sends_nothing(self):
        self.assertEqual(self.headers("https://cache.example/bazel/ac/abc"), {})
        self.assertEqual(self.headers("https://cache.example/bazel", BAZEL_REMOTE_CACHE_TOKEN="t0k"), {})

    def test_bazelrc_keeps_pull_requests_read_only(self):
        rc = (ROOT / ".bazelrc").read_text()
        self.assertIn("common:remote-cache --remote_upload_local_results=false", rc)
        self.assertIn("common:remote-cache-write --remote_upload_local_results=true", rc)
        self.assertIn("test:ci --nocache_test_results", rc)
        self.assertIn("common:ci --lockfile_mode=error", rc)


if __name__ == "__main__":
    unittest.main()
