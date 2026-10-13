#!/usr/bin/env python3
"""Offline identity contract and a real, dependency-free duplicate-build baseline."""
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest


HELPER = Path(__file__).with_name("ci-build-identity.py")
spec = importlib.util.spec_from_file_location("identity", HELPER)
identity = importlib.util.module_from_spec(spec)
spec.loader.exec_module(identity)


def request():
    """Explicit example contract for the future producer; no host probing here."""
    return {
        "schema": 1,
        "source": {"repository": "intent-hq/intentd", "commit": "a" * 40,
                   "embedded_commit": "a" * 40, "dirty": False},
        "platform": {"target": "aarch64-apple-darwin", "host": "aarch64-apple-darwin",
                     "os": "macOS", "arch": "ARM64", "abi": "Darwin-25",
                     "sdk": "MacOSX26.sdk", "image": "macos-15@20261001"},
        "tools": {"rustc": "rustc 1.96.0 (full rustc -vV output)",
                  "cargo": "cargo 1.96.0", "nextest": "cargo-nextest 0.9.143",
                  "llvm_cov": "none", "cc": "Apple clang 17", "linker": "ld 123"},
        "build": {"kind": "test-archive", "profile": "test",
                  "commands": [["cargo", "test", "--locked", "--no-run", "-p", "intent-services", "--lib"]],
                  "features": {"default_features": True, "all_features": False, "requested": []},
                  "resolved_features_sha256": "1" * 64,
                  "instrumentation": {"mode": "none", "flags": []},
                  "recipe_sha256": "2" * 64, "cargo_config_sha256": "3" * 64,
                  "inputs": {"Cargo.lock": "4" * 64, "Cargo.toml": "5" * 64,
                             "rust-toolchain.toml": "6" * 64}},
        "environment": {name: None for name in identity.REQUIRED_ENV},
    }


class IdentityTests(unittest.TestCase):
    def test_mapping_order_does_not_change_key(self):
        original = request()
        reordered = json.loads(json.dumps(original, sort_keys=True))
        self.assertEqual(identity.build_key(original), identity.build_key(reordered))
        self.assertRegex(identity.build_key(original), r"^intentd-build-v1-[0-9a-f]{64}$")

    def test_duplicate_requests_have_one_identity_not_one_test_result(self):
        # Separate jobs and retries request the same producer. Their runtime
        # filters/results belong outside this contract and must still execute.
        requests = [request() for _ in ("diagnostics", "cow", "rerun")]
        self.assertEqual(len({identity.build_key(value) for value in requests}), 1)

    def test_flag_order_empty_environment_and_extra_inputs_are_significant(self):
        original = request()
        original["build"]["commands"][0].extend(["--config", "build.jobs=2", "--config", "build.jobs=4"])
        changed = json.loads(json.dumps(original))
        changed["build"]["commands"][0][-3], changed["build"]["commands"][0][-1] = "build.jobs=4", "build.jobs=2"
        self.assertNotEqual(identity.build_key(original), identity.build_key(changed))
        changed = request()
        changed["environment"]["RUSTFLAGS"] = ""
        self.assertNotEqual(identity.build_key(request()), identity.build_key(changed))
        changed = request()
        changed["environment"]["CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER"] = "custom-clang"
        self.assertNotEqual(identity.build_key(request()), identity.build_key(changed))

    def test_incompatible_inputs_never_match(self):
        changes = [
            (("source", "repository"), "another/intentd"),
            (("platform", "target"), "x86_64-pc-windows-msvc"),
            (("platform", "host"), "x86_64-apple-darwin"),
            (("platform", "abi"), "Darwin-26"),
            (("platform", "sdk"), "MacOSX27.sdk"),
            (("platform", "image"), "self-hosted-m1mac@revision2"),
            (("tools", "rustc"), "rustc 1.97.0"),
            (("tools", "cargo"), "cargo 1.97.0"),
            (("tools", "nextest"), "cargo-nextest 0.9.144"),
            (("tools", "cc"), "Apple clang 18"),
            (("tools", "linker"), "ld 124"),
            (("build", "kind"), "check"),
            (("build", "profile"), "release"),
            (("build", "profile"), "dist"),
            (("build", "features", "default_features"), False),
            (("build", "features", "requested"), ["repository-test-fixtures"]),
            (("build", "resolved_features_sha256"), "7" * 64),
            (("build", "recipe_sha256"), "8" * 64),
            (("build", "cargo_config_sha256"), "9" * 64),
            (("build", "inputs", "Cargo.lock"), "0" * 64),
            (("environment", "RUSTFLAGS"), "-C target-cpu=native"),
            (("environment", "CARGO_ENCODED_RUSTFLAGS"), "-C\u001ftarget-cpu=native"),
            (("environment", "CARGO_PROFILE_TEST_DEBUG"), "0"),
            (("environment", "MACOSX_DEPLOYMENT_TARGET"), "14.0"),
            (("environment", "TRANSFER_SELECTION_BUILD_REVISION"), "other"),
        ]
        for path, value in changes:
            other = request()
            target = other
            for part in path[:-1]:
                target = target[part]
            target[path[-1]] = value
            with self.subTest(path=path, value=value):
                self.assertNotEqual(identity.build_key(request()), identity.build_key(other))
                with self.assertRaisesRegex(ValueError, "incompatible build identity"):
                    identity.require_compatible(request(), other)

    def test_merge_commit_is_not_pr_commit_even_for_identical_tree(self):
        other = request()
        other["source"].update(commit="b" * 40, embedded_commit="b" * 40)
        self.assertNotEqual(identity.build_key(request()), identity.build_key(other))

    def test_coverage_and_build_scope_are_separate(self):
        other = request()
        other["tools"]["llvm_cov"] = "cargo-llvm-cov 0.9.0"
        other["build"]["instrumentation"] = {"mode": "llvm-cov", "flags": ["-C", "instrument-coverage", "--cfg=coverage"]}
        self.assertNotEqual(identity.build_key(request()), identity.build_key(other))
        other = request()
        other["build"]["commands"][0].extend(["-p", "intentd", "--tests"])
        self.assertNotEqual(identity.build_key(request()), identity.build_key(other))

    def test_rejects_incomplete_or_ambiguous_requests(self):
        bad = []
        for field in request():
            value = request()
            del value[field]
            bad.append(value)
        for path, replacement in [
            (("schema",), True), (("schema",), 2),
            (("source", "dirty"), True), (("source", "commit"), "abc1234"),
            (("source", "embedded_commit"), "b" * 40),
            (("platform", "sdk"), ""), (("tools", "rustc"), ""),
            (("build", "recipe_sha256"), "not-a-digest"),
            (("build", "commands"), []), (("build", "commands"), [["cargo", 42]]),
            (("build", "features", "all_features"), "false"),
            (("build", "instrumentation", "mode"), "unknown"),
            (("environment", "RUSTFLAGS"), 42),
            (("environment", "INTENTD_BUILD_COMMIT"), "b" * 40),
            (("environment", "INTENTD_EMBEDDED_BUILD_COMMIT"), "b" * 40),
        ]:
            value = request()
            parent = value
            for part in path[:-1]:
                parent = parent[part]
            parent[path[-1]] = replacement
            bad.append(value)
        value = request()
        del value["environment"]["RUSTFLAGS"]
        bad.append(value)
        value = request()
        value["run_id"] = "123"
        bad.append(value)
        for value in bad:
            with self.subTest(value=value), self.assertRaises(ValueError):
                identity.build_key(value)

    def test_cli_matches_and_reports_mismatch_without_echoing_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            expected, actual = [Path(tmp) / name for name in ("expected.json", "actual.json")]
            expected.write_text(json.dumps(request()))
            actual.write_text(expected.read_text())
            command = [sys.executable, "-I", "-B", str(HELPER)]
            result = subprocess.run(command + ["key", str(expected)], capture_output=True, text=True, check=True)
            self.assertEqual(result.stdout.strip(), identity.build_key(request()))
            subprocess.run(command + ["verify", str(expected), str(actual)], capture_output=True, check=True)
            changed = request()
            changed["environment"]["RUSTFLAGS"] = "private-value"
            actual.write_text(json.dumps(changed))
            result = subprocess.run(command + ["verify", str(expected), str(actual)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 1)
            self.assertIn("environment", result.stderr)
            self.assertNotIn("private-value", result.stderr)
            expected.write_text('{"schema": 1, "schema": 2}')
            result = subprocess.run(command + ["key", str(expected)], capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("duplicate JSON field", result.stderr)


class CargoBaselineTests(unittest.TestCase):
    def test_isolated_jobs_duplicate_builds_and_filters_do_not_reduce_binary(self):
        # Real Cargo, no registry dependencies/network, no intentd compilation.
        # compiler-artifact fresh=false counts built targets, NOT rustc calls.
        with tempfile.TemporaryDirectory(prefix="identity-baseline-") as tmp:
            root = Path(tmp)
            (root / "src").mkdir()
            (root / "Cargo.toml").write_text('[package]\nname="identity-baseline"\nversion="0.0.0"\nedition="2021"\n[workspace]\n[features]\nfixtures=[]\n')
            (root / "src/lib.rs").write_text('''
pub const FIXTURES: bool = cfg!(feature = "fixtures");
#[test] fn diagnostics() { assert_eq!(FIXTURES, cfg!(feature = "fixtures")); }
#[test] fn lifecycle() { assert_eq!(FIXTURES, cfg!(feature = "fixtures")); }
''')
            env = {k: v for k, v in os.environ.items() if not k.startswith(("CARGO_", "RUST"))}
            pin = re.search(r'^channel\s*=\s*"([^"]+)"',
                            (HELPER.parent.parent / "rust-toolchain.toml").read_text(), re.M).group(1)
            env.update(CARGO_HOME=str(root / "cargo-home"), CARGO_TERM_PROGRESS_WHEN="never", RUSTUP_TOOLCHAIN=pin)
            if "RUSTUP_HOME" in os.environ:
                env["RUSTUP_HOME"] = os.environ["RUSTUP_HOME"]

            def build(directory, test_filter, features=()):
                command = ["cargo", "test", "--offline", "--no-run", "--message-format=json", "--target-dir", str(root / directory), *features, test_filter]
                result = subprocess.run(command, cwd=root, env=env, text=True, capture_output=True, check=True, timeout=60)
                artifacts = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
                artifacts = [item for item in artifacts if item.get("reason") == "compiler-artifact"]
                binary = next(item for item in artifacts if item.get("executable") and item["profile"]["test"])
                return sum(not item["fresh"] for item in artifacts), binary

            first, binary = build("job-a", "diagnostics")
            duplicate, _ = build("job-b", "lifecycle")
            warm, repeated = build("job-a", "lifecycle")
            feature_build, changed = build("job-a", "lifecycle", ["--features", "fixtures"])
            self.assertGreater(first, 0)
            self.assertEqual(first, duplicate)
            self.assertEqual(warm, 0)
            self.assertEqual(binary["executable"], repeated["executable"])
            self.assertGreater(feature_build, 0)
            self.assertNotEqual(binary["features"], changed["features"])
            listed = subprocess.check_output([binary["executable"], "--list"], text=True)
            self.assertIn("diagnostics: test", listed)
            self.assertIn("lifecycle: test", listed)
            for test_filter in ("diagnostics", "lifecycle"):
                output = subprocess.check_output([binary["executable"], test_filter, "--exact"], text=True)
                self.assertIn("1 passed", output)
            print(f"baseline built targets: job-a={first}, job-b={duplicate}, warm={warm}, feature-change={feature_build}")


if __name__ == "__main__":
    unittest.main()
