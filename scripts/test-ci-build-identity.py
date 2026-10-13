#!/usr/bin/env python3
"""Offline identity contract and a real, dependency-free duplicate-build baseline."""
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
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


class ProducerGroupTests(unittest.TestCase):
    def test_profiles_features_instrumentation_and_scope_share_producer_group(self):
        baseline = request()
        variants = [json.loads(json.dumps(baseline)) for _ in range(4)]
        variants[0]["build"]["features"]["requested"] = ["repository-test-fixtures"]
        variants[1]["build"].update(profile="release", kind="binary")
        variants[2]["build"]["instrumentation"] = {"mode": "llvm-cov", "flags": ["-Cinstrument-coverage"]}
        variants[2]["tools"]["llvm_cov"] = "cargo-llvm-cov 0.9.0"
        variants[2]["environment"]["RUSTFLAGS"] = "-Cinstrument-coverage"
        variants[3]["build"]["commands"][0].extend(["-p", "intentd"])
        for variant in variants:
            self.assertEqual(identity.producer_group_key(baseline), identity.producer_group_key(variant))
            self.assertNotEqual(identity.build_key(baseline), identity.build_key(variant))

    def test_source_platform_and_compiler_changes_separate_groups(self):
        for section, field, value in [("platform", "sdk", "new-sdk"), ("platform", "target", "new-target"),
                                       ("tools", "rustc", "new-rustc"), ("tools", "linker", "new-linker")]:
            changed = request()
            changed[section][field] = value
            self.assertNotEqual(identity.producer_group_key(request()), identity.producer_group_key(changed))
        changed = request()
        changed["source"].update(commit="b" * 40, embedded_commit="b" * 40)
        self.assertNotEqual(identity.producer_group_key(request()), identity.producer_group_key(changed))

    def test_overlapping_requests_recheck_and_build_only_missing_variants(self):
        a, b, c = request(), request(), request()
        b["build"]["features"]["requested"] = ["repository-test-fixtures"]
        c["build"]["profile"] = "release"
        first = identity.plan_variants([a, b, a], [])
        waiting = identity.plan_variants([b, c], [])
        self.assertEqual(first["producer_group"], waiting["producer_group"])
        self.assertEqual(len(first["missing_keys"]), 2)
        self.assertEqual(first, identity.plan_variants([b, a], []))
        # The second request repeats lookup after entering the shared group.
        after_first_published = identity.plan_variants([b, c], first["requested_keys"])
        self.assertEqual(after_first_published["missing_keys"], [identity.build_key(c)])
        self.assertEqual(identity.plan_variants([a, b, c], [identity.build_key(v) for v in (a, b, c)])["missing_keys"], [])

    def test_planner_rejects_mixed_groups_empty_requests_and_malformed_keys(self):
        different = request()
        different["platform"]["sdk"] = "new-sdk"
        for requests, available in [([], []), ([request(), different], []), ([request()], ["prefix-match"]),
                                    ([request()], "not-a-list")]:
            with self.subTest(requests=requests, available=available), self.assertRaises(ValueError):
                identity.plan_variants(requests, available)

    def test_group_and_plan_cli(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            single, requests, available = [root / name for name in ("single.json", "requests.json", "available.json")]
            single.write_text(json.dumps(request()))
            requests.write_text(json.dumps([request()]))
            available.write_text("[]")
            command = [sys.executable, "-I", "-B", str(HELPER)]
            group = subprocess.check_output(command + ["group", str(single)], text=True).strip()
            plan = json.loads(subprocess.check_output(command + ["plan", str(requests), str(available)], text=True))
            self.assertEqual(group, plan["producer_group"])
            self.assertEqual(plan["missing_keys"], [identity.build_key(request())])


class CargoBaselineTests(unittest.TestCase):
    def test_sequential_variants_share_dependency_cache_and_stage_overwritten_binary(self):
        with tempfile.TemporaryDirectory(prefix="identity-variants-") as tmp:
            root = Path(tmp)
            (root / "src").mkdir()
            (root / "shared/src").mkdir(parents=True)
            (root / "Cargo.toml").write_text('[package]\nname="identity-variants"\nversion="0.0.0"\nedition="2021"\n[workspace]\n[features]\nfixtures=[]\n[dependencies]\nidentity-shared={path="shared"}\n')
            (root / "shared/Cargo.toml").write_text('[package]\nname="identity-shared"\nversion="0.0.0"\nedition="2021"\n')
            (root / "shared/src/lib.rs").write_text('pub fn value() -> u8 { 7 }\n')
            (root / "src/main.rs").write_text('fn main() { println!("{}:{}", identity_shared::value(), cfg!(feature="fixtures")); }\n')
            env = fixture_environment(root)
            binary_paths, staged, shared_fresh = [], [], []
            for variant, flags in (("production", []), ("fixtures", ["--features", "fixtures"])):
                built = subprocess.run(["cargo", "build", "--offline", "--message-format=json", *flags],
                                       cwd=root, env=env, text=True, capture_output=True, check=True, timeout=60)
                artifacts = [json.loads(line) for line in built.stdout.splitlines() if line.startswith("{")]
                artifacts = [item for item in artifacts if item.get("reason") == "compiler-artifact"]
                binary = Path(next(item["executable"] for item in artifacts if item.get("executable")))
                shared_fresh.append(next(item["fresh"] for item in artifacts if item["target"]["name"] == "identity_shared"))
                binary_paths.append(binary)
                destination = root / (variant + binary.suffix)
                shutil.copy2(binary, destination)
                staged.append(destination)
            self.assertEqual(binary_paths[0], binary_paths[1])
            self.assertEqual(shared_fresh, [False, True])
            self.assertEqual(subprocess.check_output([str(staged[0])], text=True).strip(), "7:false")
            self.assertEqual(subprocess.check_output([str(staged[1])], text=True).strip(), "7:true")
            self.assertEqual(subprocess.check_output([str(binary_paths[0])], text=True).strip(), "7:true")

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
            env = fixture_environment(root)

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


def fixture_environment(root):
    env = {k: v for k, v in os.environ.items() if not k.startswith(("CARGO_", "RUST"))}
    pin = re.search(r'^channel\s*=\s*"([^"]+)"',
                    (HELPER.parent.parent / "rust-toolchain.toml").read_text(), re.M).group(1)
    env.update(CARGO_HOME=str(root / "cargo-home"), CARGO_TERM_PROGRESS_WHEN="never", RUSTUP_TOOLCHAIN=pin)
    if "RUSTUP_HOME" in os.environ:
        env["RUSTUP_HOME"] = os.environ["RUSTUP_HOME"]
    return env


if __name__ == "__main__":
    unittest.main()
