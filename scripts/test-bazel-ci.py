#!/usr/bin/env python3
"""Unit tests for the Bazel CI helpers: scripts/bazel-drift.py and
bazel/remote-cache-credentials.sh (no Bazel needed)."""

from __future__ import annotations

import contextlib
import hashlib
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
        with mock.patch.object(drift, "lock_drift_is_release_only", return_value=(False, "Cargo.lock changed")):
            code, _, err = self.run_main([], [0, 1])
        self.assertEqual(code, 1)
        self.assertIn("MODULE.bazel.lock is out of date", err)
        self.assertIn(drift.FIX_COMMAND, err)

    def test_stale_generated_files_fail_before_bazel(self):
        with mock.patch.object(drift, "generated_drift_is_release_only", return_value=False):
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


MANIFEST = """[package]
name = "a"
version = "0.11.25"

[dependencies]
b = { path = "../b", version = "0.11.25" }
serde = { version = "1.0.200", features = ["derive"] }

[target.'cfg(unix)'.dependencies]
b2 = { path = "../b2", version = "0.11.25" }
"""
LOCKFILE = """version = 4

[[package]]
name = "a"
version = "0.11.25"
dependencies = [
 "b 0.11.25",
 "serde",
]

[[package]]
name = "b"
version = "0.11.25"

[[package]]
name = "serde"
version = "1.0.200"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "aaaa"
"""


def release(text):
    return text.replace("0.11.25", "0.11.27")


class ReleaseTolerance(unittest.TestCase):
    """A release bump (workspace-crate versions only) needs no Bazel action; anything else does."""

    def test_release_bump_is_tolerated(self):
        self.assertTrue(drift.release_only_change("crates/a/Cargo.toml", MANIFEST, release(MANIFEST)))
        self.assertTrue(drift.release_only_change("Cargo.lock", LOCKFILE, release(LOCKFILE)))

    def test_third_party_and_feature_changes_are_drift(self):
        bumped_lock = LOCKFILE.replace('"1.0.200"', '"1.0.201"').replace('"aaaa"', '"bbbb"')
        self.assertFalse(drift.release_only_change("Cargo.lock", LOCKFILE, release(bumped_lock)))
        self.assertFalse(drift.release_only_change(
            "crates/a/Cargo.toml", MANIFEST, MANIFEST.replace('version = "1.0.200"', 'version = "1.0.201"')))
        self.assertFalse(drift.release_only_change(
            "crates/a/Cargo.toml", MANIFEST, MANIFEST.replace('["derive"]', '["derive", "rc"]')))
        self.assertFalse(drift.release_only_change(
            "crates/a/Cargo.toml", MANIFEST, MANIFEST + 'c = { path = "../c", version = "0.11.25" }\n'))
        self.assertFalse(drift.release_only_change("rust-toolchain.toml", "a", "b"))

    def git_repo(self, tmp):
        def git(*args):
            subprocess.run(["git", *args], cwd=tmp, check=True, capture_output=True)
        (tmp / "crates/a").mkdir(parents=True)
        (tmp / "crates/a/Cargo.toml").write_text(MANIFEST)
        (tmp / "Cargo.lock").write_text(LOCKFILE)
        inputs = [f"FILE:@@//{p} {hashlib.sha256((tmp / p).read_bytes()).hexdigest()}"
                  for p in ("Cargo.lock", "crates/a/Cargo.toml")]
        lock = {"moduleExtensions": {drift.CRATE_EXTENSION: {"general": {"recordedInputs": inputs}}}}
        (tmp / "MODULE.bazel.lock").write_text(json.dumps(lock))
        git("init", "-q")
        git("add", "-A")
        git("-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "lock")
        return tmp

    def lock_verdict(self, edit):
        with tempfile.TemporaryDirectory() as raw:
            tmp = self.git_repo(Path(raw))
            edit(tmp)
            with mock.patch.object(drift, "ROOT", tmp), mock.patch.object(drift, "LOCK", tmp / "MODULE.bazel.lock"):
                return drift.lock_drift_is_release_only()

    def test_lock_behind_a_release_passes(self):
        def bump(tmp):
            for name in ("Cargo.lock", "crates/a/Cargo.toml"):
                (tmp / name).write_text(release((tmp / name).read_text()))
        ok, why = self.lock_verdict(bump)
        self.assertTrue(ok, why)

    def test_lock_behind_a_dependency_bump_fails(self):
        def bump(tmp):
            text = release((tmp / "Cargo.lock").read_text()).replace('"1.0.200"', '"1.0.201"')
            (tmp / "Cargo.lock").write_text(text)
        ok, why = self.lock_verdict(bump)
        self.assertFalse(ok)
        self.assertIn("Cargo.lock changed beyond workspace-crate versions", why)

    def test_unchanged_inputs_mean_real_drift(self):
        ok, _ = self.lock_verdict(lambda tmp: None)
        self.assertFalse(ok)

    def test_release_drift_passes_check_with_a_note(self):
        with mock.patch.object(drift, "run", side_effect=[1, 1]), \
                mock.patch.object(drift, "generated_drift_is_release_only", return_value=True), \
                mock.patch.object(drift, "lock_drift_is_release_only", return_value=(True, "versions")), \
                mock.patch.object(drift, "check_lock_paths", return_value=[]), \
                contextlib.redirect_stdout(io.StringIO()):
            problems, notes = drift.check("bazel")
        self.assertEqual(problems, [])
        self.assertEqual(len(notes), 2)

    def test_generated_version_lines_only(self):
        self.assertEqual(drift.VERSION_LINE.sub("", '    edition = "2021",\n    version = "0.11.25",\n'),
                         '    edition = "2021",\n')


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
        self.assertNotIn("common:ci --lockfile_mode", rc)


if __name__ == "__main__":
    unittest.main()
