#!/usr/bin/env python3
"""Real nextest relocation, variant staging, and fail-closed publication tests."""
import importlib.util
import contextlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


def load(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


artifacts = load("ci-test-artifacts")


class ArchiveTests(unittest.TestCase):
    @unittest.skipUnless(sys.platform == "linux", "ELF dependency closure proof")
    def test_real_recursive_libraries_relocate_without_host_injection(self):
        with tempfile.TemporaryDirectory(prefix="intent-library-chain-") as tmp:
            root = Path(tmp)
            producer, staged = root / "producer", root / "staged"
            producer.mkdir()
            staged.mkdir()
            sources = {
                "second": "int second(void) { return 42; }",
                "first": "extern int second(void); int first(void) { return second(); }",
                "injection": "int unrelated(void) { return 9; }",
                "main": '#include <stdio.h>\nextern int first(void); int main(void) { printf("%d\\n", first()); return 0; }',
            }
            for name, code in sources.items():
                (producer / (name + ".c")).write_text(code)
            for name in ("second", "first", "injection"):
                command = ["cc", "-fPIC", "-shared", str(producer / (name + ".c")), "-Wl,-soname,lib" + name + ".so", "-o", str(producer / ("lib" + name + ".so"))]
                if name == "first":
                    command += ["-L" + str(producer), "-lsecond", "-Wl,-rpath," + str(producer)]
                subprocess.run(command, check=True, capture_output=True)
            binary = producer / "main"
            subprocess.run(["cc", str(producer / "main.c"), "-L" + str(producer), "-lfirst", "-Wl,-rpath," + str(producer), "-o", str(binary)], check=True, capture_output=True)
            with mock.patch.dict(os.environ, {"LD_PRELOAD": str(producer / "libinjection.so")}):
                dependencies = artifacts.stage_binary_libraries(binary, staged)
            self.assertEqual(dependencies["bundled"], ["libfirst.so", "libsecond.so"])
            self.assertEqual(sorted(p.name for p in staged.iterdir()), ["libfirst.so", "libsecond.so"])
            shutil.copy2(binary, staged / "main")
            shutil.rmtree(producer)
            env = dict(os.environ, LD_LIBRARY_PATH=str(staged))
            env.pop("LD_PRELOAD", None)
            self.assertEqual(subprocess.check_output([str(staged / "main")], env=env, text=True).strip(), "42")

    def test_failed_build_preserves_and_displays_cargo_json_diagnostics(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source"; source.mkdir()
            request = {"identity": {"tools": {"rustc": "fixture", "cargo": "fixture", "nextest": "fixture"},
                                    "environment": {}},
                       "recipe": {"kind": "test-archive", "command": [sys.executable, "-c",
                           "import json,sys; print(json.dumps({'reason':'compiler-message','message':{'rendered':'error[E0433]: missing test support\\n'}})); sys.exit(101)"]}}
            errors = io.StringIO()
            with mock.patch.object(artifacts, "validate_request"), \
                 mock.patch.object(artifacts.identity, "plan_variants"), \
                 mock.patch.object(artifacts.identity, "build_key", return_value="fixture-key"), \
                 mock.patch.object(artifacts, "check_source"), \
                 mock.patch.object(artifacts, "output", return_value="fixture"), \
                 mock.patch.object(artifacts, "build_environment", return_value={}), \
                 contextlib.redirect_stderr(errors):
                with self.assertRaises(subprocess.CalledProcessError):
                    artifacts.produce([request], source, root / "target", root / "published")
            self.assertIn("error[E0433]: missing test support", errors.getvalue())
            failure = root / "published/failures/fixture-key"
            self.assertIn("compiler-message", (failure / "build.jsonl").read_text())
            self.assertTrue((failure / "build.stderr.log").is_file())
            self.assertFalse((root / "published/fixture-key").exists())

    def test_archive_implementation_is_part_of_the_recipe_identity(self):
        recipe = artifacts.native_recipes("linux", "pull_request", "x86_64-unknown-linux-gnu")["sitter"]
        before = artifacts.recipe_digest(recipe)
        with mock.patch.object(artifacts, "file_sha", return_value="0" * 64):
            self.assertNotEqual(artifacts.recipe_digest(recipe), before)

    def test_library_audit_ignores_host_injection_and_records_linker_environment(self):
        with tempfile.TemporaryDirectory(prefix="intent-library-audit-") as tmp:
            binary = Path(tmp) / "binary"
            binary.touch()
            result = subprocess.CompletedProcess([], 0, "libc.so.6 => /lib/libc.so.6 (0x123)\n/host/injection.so (0x456)\n", "")
            dynamic = "0x0000000000000001 (NEEDED) Shared library: [libc.so.6]\n"
            with mock.patch.dict(os.environ, {"LD_PRELOAD": "/host/injection.so", "LD_AUDIT": "/host/audit.so", "LIBRARY_PATH": "/custom/linker"}), mock.patch.object(artifacts.sys, "platform", "linux"), mock.patch.object(artifacts.subprocess, "run", return_value=result) as run, mock.patch.object(artifacts, "output", return_value=dynamic):
                dependencies = artifacts.stage_binary_libraries(binary, Path(tmp))
                self.assertEqual(dependencies["bundled"], [])
                self.assertNotIn("LD_PRELOAD", run.call_args.kwargs["env"])
                self.assertNotIn("LD_AUDIT", run.call_args.kwargs["env"])
                self.assertEqual(artifacts.build_environment()["LIBRARY_PATH"], "/custom/linker")

    def test_unsafe_paths_and_consumer_build_flags_fail_closed(self):
        for name in ("../escape", "/absolute", "C:/drive", "a/../../escape", "a\\escape", ""):
            with self.subTest(name=name), self.assertRaises(ValueError):
                artifacts.safe_join(Path.cwd(), name)
        with self.assertRaisesRegex(ValueError, "consumer argument"):
            artifacts.run_nextest({"target": "unused"}, ["--workspace"])

    def test_native_scopes_stay_separate_and_only_requested_targets_build(self):
        mac = artifacts.native_recipes("macos", "merge_group", "aarch64-apple-darwin")
        win = artifacts.native_recipes("windows", "merge_group", "x86_64-pc-windows-msvc")
        linux = artifacts.native_recipes("linux", "pull_request", "x86_64-unknown-linux-gnu")
        self.assertEqual(len(mac), 5)
        self.assertEqual(len(win), 4)
        self.assertEqual(len(linux), 3)
        self.assertNotIn("services", linux)
        self.assertNotIn("transport", win)
        for recipes in (mac, win, linux):
            for recipe in recipes.values():
                command = recipe["command"]
                self.assertNotIn("--all-features", command)
                self.assertNotIn("--workspace", command)
                self.assertIn("--locked", command)
        self.assertEqual(mac["sitter-production"]["kind"], "binary")

    def test_real_archives_relocate_and_preserve_sequential_variants(self):
        with tempfile.TemporaryDirectory(prefix="intent-archive-test-") as tmp:
            root = Path(tmp)
            source = root / "producer"
            (source / "src").mkdir(parents=True)
            (source / "tests").mkdir()
            (source / "fixtures").mkdir()
            (source / "fixtures/value").write_text("fixture payload")
            (source / "dep/src").mkdir(parents=True)
            (source / "dep/Cargo.toml").write_text('[package]\nname="unchanged-dep"\nversion="0.1.0"\nedition="2021"\n')
            (source / "dep/src/lib.rs").write_text('pub fn value() -> u8 { 7 }\n')
            (source / "Cargo.toml").write_text('[package]\nname="archive-fixture"\nversion="0.1.0"\nedition="2021"\n[workspace]\n[features]\nflavor=[]\n[dependencies]\nunchanged-dep={path="dep"}\n')
            (source / "src/main.rs").write_text('fn main() { println!("{}:{}", unchanged_dep::value(), cfg!(feature="flavor")); }\n')
            support = Path(__file__).resolve().parents[1] / "crates/intentd-test-support/src/runtime_paths.rs"
            shutil.copyfile(support, source / "tests/runtime_paths.rs")
            (source / "tests/relocation.rs").write_text('''mod runtime_paths;
#[test] fn relocated_binary_and_fixture() {
    let binary = runtime_paths::binary_path("archive-fixture", env!("CARGO_BIN_EXE_archive-fixture"));
    let output = std::process::Command::new(binary).output().unwrap();
    assert!(output.status.success());
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), format!("7:{}", cfg!(feature="flavor")));
    let fixture = runtime_paths::manifest_dir(env!("CARGO_MANIFEST_DIR")).join("fixtures/value");
    assert_eq!(std::fs::read_to_string(fixture).unwrap(), "fixture payload");
}
''')
            subprocess.run(["cargo", "generate-lockfile", "--offline"], cwd=source, check=True, capture_output=True)
            for args in (["init", "-q"], ["add", "."], ["-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture"]):
                subprocess.run(["git", *args], cwd=source, check=True, capture_output=True)
            host = subprocess.check_output(["rustc", "-vV"], text=True).split("host: ")[1].splitlines()[0]
            platform = dict(target=host, host=host, os=os.name, arch=host.split("-")[0], abi="fixture", sdk="none", image="fixture")
            recipes = []
            for flavor in (False, True):
                command = ["cargo", "nextest", "archive", "--locked", "--offline", "--test", "relocation", "--cargo-message-format", "json", "--archive-file", "{archive}", "--target-dir", "{target}"]
                if flavor:
                    command += ["--features", "flavor"]
                recipes.append(dict(kind="test-archive", command=command, companions=["archive-fixture"]))
            requests = [artifacts.capture_request(source, platform, recipe, config_sha256="0"*64) for recipe in recipes]
            target = root / "shared-target"
            published = root / "published"
            records = artifacts.produce(requests, source, target, published)
            self.assertEqual(len(records), 2)
            self.assertTrue(all(record["archive_bytes"] > 0 for record in records))
            with mock.patch.object(artifacts.subprocess, "Popen", side_effect=AssertionError("recompiled")):
                reused = artifacts.produce([*requests, requests[0]], source, target, published)
            self.assertEqual(len(reused), 2)
            self.assertTrue(all(record["reused"] for record in reused))
            bad_recipe = dict(recipes[0], companions=["missing-required-companion"])
            bad_request = artifacts.capture_request(source, platform, bad_recipe, config_sha256="0"*64)
            with self.assertRaisesRegex(ValueError, "missing required companion"):
                artifacts.produce([requests[0], bad_request], source, target, published)
            self.assertFalse((published / artifacts.identity.build_key(bad_request["identity"])).exists())
            self.assertTrue((published / records[0]["key"] / "manifest.json").is_file())
            binary_recipe = dict(kind="binary", companions=["archive-fixture"], command=["cargo", "build", "--locked", "--offline", "--message-format=json", "--target-dir", "{target}"])
            binary_request = artifacts.capture_request(source, platform, binary_recipe, config_sha256="0"*64)
            binary_record = artifacts.produce([binary_request], source, target, published)[0]
            # The second variant reuses the unchanged dependency and overwrites
            # the shared companion path; each archive must retain its own copy.
            logs = [json.loads(line) for line in (published / records[1]["key"] / "build.jsonl").read_text().splitlines() if line.startswith("{")]
            self.assertTrue(any(x.get("reason") == "compiler-artifact" and x.get("target", {}).get("name") == "unchanged_dep" and x["fresh"] for x in logs))
            consumer = root / "other-checkout"
            shutil.copytree(source, consumer)
            shutil.rmtree(source)
            shutil.rmtree(target)
            forbidden = root / "forbidden"
            forbidden.mkdir()
            for name in ("rustc", "cargo"):
                file = forbidden / name
                file.write_text("#!/bin/sh\necho compiler-forbidden >&2\nexit 97\n")
                file.chmod(0o755)
            env = dict(os.environ, PATH=str(forbidden)+os.pathsep+os.environ["PATH"], RUSTC=str(forbidden / "rustc"))
            production = artifacts.prepare(published / binary_record["key"], binary_request, consumer, root / "binary-extract", env=env)
            companion = artifacts.runtime_env(production, env)["NEXTEST_BIN_EXE_archive-fixture"]
            self.assertEqual(subprocess.check_output([companion], env=env, text=True).strip(), "7:false")
            for index, request in enumerate(requests):
                bundle = published / records[index]["key"]
                prepared = artifacts.prepare(bundle, request, consumer, root / f"extract-{index}", env=env)
                result = artifacts.run_nextest(prepared, ["--no-tests", "fail"], env=env)
                self.assertEqual(result, 0)
                self.assertEqual(artifacts.run_libtest(prepared, "archive-fixture::relocation", ["--exact", "relocated_binary_and_fixture"], env=env), 0)
                self.assertNotEqual(artifacts.run_libtest(prepared, "archive-fixture::relocation", ["--exact", "not_a_test"], env=env), 0)
                removed = Path(prepared["target"]) / prepared["runtime"]["companions"]["archive-fixture"]
                removed.unlink()
                with self.assertRaisesRegex(ValueError, "missing|inventory"):
                    artifacts.run_libtest(prepared, "archive-fixture::relocation", ["--list"], env=env)
                wrong = json.loads(json.dumps(request))
                wrong["identity"]["platform"]["abi"] = "wrong"
                with self.assertRaisesRegex(ValueError, "identity"):
                    artifacts.prepare(bundle, wrong, consumer, root / "wrong", env=env)
                manifest_file = bundle / "manifest.json"
                original = manifest_file.read_bytes()
                altered = json.loads(original)
                altered["key"] = "forged"
                manifest_file.write_text(json.dumps(altered))
                with self.assertRaises(ValueError):
                    artifacts.prepare(bundle, request, consumer, root / "altered", env=env)
                manifest_file.write_bytes(original)
                altered = json.loads(original)
                altered["runtime"]["companions"] = {}
                manifest_file.write_text(json.dumps(altered))
                with self.assertRaisesRegex(ValueError, "runtime inventory"):
                    artifacts.prepare(bundle, request, consumer, root / f"bad-runtime-{index}", env=env)
                manifest_file.write_bytes(original)
                archive = bundle / "tests.tar.zst"
                archive.write_bytes(archive.read_bytes()[:20])
                with self.assertRaisesRegex(ValueError, "digest|size"):
                    artifacts.prepare(bundle, request, consumer, root / "broken", env=env)


if __name__ == "__main__":
    unittest.main()
