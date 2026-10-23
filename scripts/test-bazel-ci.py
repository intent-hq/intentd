#!/usr/bin/env python3
"""Unit tests for the Bazel CI helpers: scripts/bazel-drift.py,
scripts/bazel-gen.py's test discovery, bazel/rust_integration_tests.bzl and
bazel/remote-cache-credentials.sh (no Bazel needed; the generator tests need
cargo)."""

from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import io
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("bazel_drift", ROOT / "scripts/bazel-drift.py")
drift = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(drift)
GEN_SPEC = importlib.util.spec_from_file_location("bazel_gen", ROOT / "scripts/bazel-gen.py")
gen = importlib.util.module_from_spec(GEN_SPEC)
GEN_SPEC.loader.exec_module(gen)


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
            with mock.patch.object(drift, "ROOT", tmp), mock.patch.object(drift, "LOCK", tmp / "MODULE.bazel.lock"), \
                    mock.patch.object(drift, "run", return_value=0), contextlib.redirect_stdout(io.StringIO()):
                return drift.lock_drift_is_release_only("bazel")

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

    def check_with_bazel(self, edit, stale_against_recorded_inputs):
        """Run check() on the fixture; `mod deps` fails, as frozen resolution does behind a bump.

        The second `mod deps` is the configuration check: it runs with the Cargo inputs the
        lock recorded put back, and returns `stale_against_recorded_inputs` (1 when
        MODULE.bazel or a crate annotation changed too).
        """
        with tempfile.TemporaryDirectory() as raw:
            tmp = self.git_repo(Path(raw))
            edit(tmp)
            current = {p: (tmp / p).read_text() for p in ("Cargo.lock", "crates/a/Cargo.toml")}
            on_disk_during_recheck = []

            def fake_run(cmd):
                if cmd[1:2] != ["mod"]:
                    return 0
                if not on_disk_during_recheck and len(fake_run.mod_calls) == 0:
                    fake_run.mod_calls.append(cmd)
                    return 1
                fake_run.mod_calls.append(cmd)
                on_disk_during_recheck.append({p: (tmp / p).read_text() for p in current})
                return stale_against_recorded_inputs
            fake_run.mod_calls = []

            with mock.patch.object(drift, "ROOT", tmp), mock.patch.object(drift, "LOCK", tmp / "MODULE.bazel.lock"), \
                    mock.patch.object(drift, "run", fake_run), mock.patch.object(drift, "check_lock_paths", return_value=[]), \
                    contextlib.redirect_stdout(io.StringIO()):
                problems, notes = drift.check("bazel")
            after = {p: (tmp / p).read_text() for p in current}
        return problems, notes, fake_run.mod_calls, on_disk_during_recheck, current, after

    def bump_release(self, tmp):
        for name in ("Cargo.lock", "crates/a/Cargo.toml"):
            (tmp / name).write_text(release((tmp / name).read_text()))

    def test_release_bump_with_a_configuration_change_fails(self):
        # A crate annotation (bazel/crates.MODULE.bazel) or MODULE.bazel change makes the lock
        # stale even against the Cargo inputs it recorded; a version bump must not hide it.
        problems, notes, mod_calls, during, current, after = self.check_with_bazel(self.bump_release, 1)
        self.assertEqual(len(mod_calls), 2, "the guard must re-check the lock against its recorded Cargo inputs")
        self.assertIn("--lockfile_mode=error", mod_calls[1])
        self.assertEqual(during, [{"Cargo.lock": LOCKFILE, "crates/a/Cargo.toml": MANIFEST}])
        self.assertEqual(after, current, "the working tree must be restored")
        self.assertEqual(notes, [])
        self.assertEqual(len(problems), 1)
        self.assertIn("MODULE.bazel.lock is out of date", problems[0])
        self.assertIn("configuration", problems[0])

    def test_release_bump_alone_passes_the_configuration_check(self):
        problems, notes, mod_calls, during, current, after = self.check_with_bazel(self.bump_release, 0)
        self.assertEqual(problems, [])
        self.assertEqual(len(notes), 1)
        self.assertEqual(len(mod_calls), 2)
        self.assertEqual(after, current)

    def test_release_drift_passes_check_with_a_note(self):
        with mock.patch.object(drift, "run", side_effect=[0, 1]), \
                mock.patch.object(drift, "lock_drift_is_release_only", return_value=(True, "versions")), \
                mock.patch.object(drift, "check_lock_paths", return_value=[]), \
                contextlib.redirect_stdout(io.StringIO()):
            problems, notes = drift.check("bazel")
        self.assertEqual(problems, [])
        self.assertEqual(len(notes), 1)

    def test_stale_generated_files_are_never_tolerated(self):
        # Generated files carry no versions, so even a release bump leaves them current.
        with mock.patch.object(drift, "run", side_effect=[1, 1]), \
                mock.patch.object(drift, "lock_drift_is_release_only", return_value=(True, "versions")), \
                mock.patch.object(drift, "check_lock_paths", return_value=[]), \
                contextlib.redirect_stdout(io.StringIO()):
            problems, _ = drift.check("bazel")
        self.assertEqual(len(problems), 1)
        self.assertIn("bazel-gen.py --check", problems[0])


class StarlarkFailure(Exception):
    pass


def evaluate_starlark(path: Path, env: dict) -> dict:
    """Run a .bzl or BUILD file as Python, with `load` lines dropped.

    The files here use only the Python-compatible subset of Starlark, so this
    exercises the real macro and generated BUILD files without Bazel.
    """
    source = re.sub(r"^load\(.*\)$", "", path.read_text(), flags=re.M)
    scope = {"fail": lambda msg: (_ for _ in ()).throw(StarlarkFailure(msg)), **env}
    exec(compile(source, str(path), "exec"), scope)  # noqa: S102 - trusted repository file
    return scope


def cargo_versions_bzl() -> dict:
    """bazel/cargo_versions.bzl evaluated in Python, with the Bazel builtins stubbed."""
    stub = lambda *a, **k: None  # noqa: E731
    attr = type("attr", (), {"label": staticmethod(stub), "string": staticmethod(stub),
                             "string_dict": staticmethod(stub)})
    return evaluate_starlark(ROOT / "bazel/cargo_versions.bzl", {
        "attr": attr, "tag_class": stub, "module_extension": stub, "repository_rule": stub, "Label": str,
    })


CARGO_VERSIONS_TAG = re.compile(r'cargo_versions\.crate\(\s*name = "([^"]+)",\s*manifest = "//([^:"]*):Cargo\.toml",\s*\)')


def starlark_cargo_versions(root: Path) -> dict:
    """What @cargo_versions computes for the workspace at `root`: the crates the generated
    MODULE file registers, each versioned from its Cargo.toml as the extension reads it."""
    bzl = cargo_versions_bzl()
    module = (root / "bazel/generated.MODULE.bazel").read_text()
    workspace = lambda: (root / "Cargo.toml").read_text()  # noqa: E731
    return {name: bzl["package_version"]((root / pkg / "Cargo.toml").read_text(), workspace, name)
            for name, pkg in CARGO_VERSIONS_TAG.findall(module)}


class CargoVersionParsing(unittest.TestCase):
    """bazel/cargo_versions.bzl's manifest reader, evaluated in Python."""

    def version(self, manifest, workspace=""):
        return cargo_versions_bzl()["package_version"](manifest, lambda: workspace, "a")

    def test_reads_the_package_version_only(self):
        self.assertEqual(self.version(MANIFEST), "0.11.25")
        self.assertEqual(self.version('[dependencies]\nx = { version = "9" }\n[package]\nname = "a"\n'
                                      'version = "1.2.3" # bumped by release-plz\n'), "1.2.3")

    def test_inherits_the_workspace_version(self):
        self.assertEqual(self.version('[package]\nversion.workspace = true\n',
                                      '[workspace]\n[workspace.package]\nversion = "4.5.6"\n'), "4.5.6")
        self.assertEqual(self.version('[package]\nversion = { workspace = true }\n',
                                      '[workspace.package]\nversion = "4.5.7"\n'), "4.5.7")

    def test_missing_version_fails_loudly(self):
        with self.assertRaisesRegex(StarlarkFailure, "no \\[package\\] version"):
            self.version('[package]\nname = "a"\n')

    def test_every_workspace_crate_is_registered(self):
        versions = starlark_cargo_versions(ROOT)
        crates = {tomllib_name(p) for p in ROOT.glob("crates/*/Cargo.toml")}
        self.assertEqual(set(versions), crates)
        self.assertEqual(versions["intentd"], tomllib_version(ROOT / "crates/intentd/Cargo.toml"))


def tomllib_name(path: Path) -> str:
    import tomllib
    return tomllib.loads(path.read_text())["package"]["name"]


def tomllib_version(path: Path) -> str:
    import tomllib
    return tomllib.loads(path.read_text())["package"]["version"]


class Package:
    """A fake Bazel package: glob() over a directory, rules recorded by kind."""

    def __init__(self, directory: Path):
        self.directory = directory
        self.rules: dict[str, tuple[str, dict]] = {}

    def glob(self, include, exclude=(), allow_empty=False):
        found = set()
        for pattern in include:
            found |= {p.relative_to(self.directory).as_posix()
                      for p in self.directory.glob(pattern) if p.is_file()}
        for pattern in exclude:
            found -= {p.relative_to(self.directory).as_posix() for p in self.directory.glob(pattern)}
        return sorted(found)

    def recorder(self, kind):
        def rule(name=None, **attrs):
            if name in self.rules:
                raise StarlarkFailure(f"duplicate target {name}")
            self.rules[name] = (kind, attrs)
        return rule

    def macro(self):
        native = type("native", (), {"glob": staticmethod(self.glob)})
        scope = evaluate_starlark(ROOT / "bazel/rust_integration_tests.bzl",
                                  {"native": native, "rust_test": self.recorder("rust_test")})
        return scope["rust_integration_tests"]

    def evaluate_build(self, cargo_versions=None):
        """Evaluate the package's generated BUILD.bazel; returns {target: (kind, attrs)}."""
        env = {
            "CARGO_VERSIONS": cargo_versions if cargo_versions is not None
            else starlark_cargo_versions(self.directory.parents[1]),
            "glob": self.glob,
            "package": lambda **_: None,
            "rust_integration_tests": self.macro(),
            "aliases": lambda *a, **k: {},
            "all_crate_deps": lambda *a, **k: [],
            "dev_aliases": lambda *a, **k: {},
            "dev_crate_deps": lambda *a, **k: [],
        }
        for kind in ("rust_library", "rust_proc_macro", "rust_binary", "rust_test", "filegroup", "copy_file",
                     "cargo_build_script", "extract_cargo_lints", "shared_binary_test"):
            env[kind] = self.recorder(kind)
        evaluate_starlark(self.directory / "BUILD.bazel", env)
        return self.rules


class IntegrationTestMacro(unittest.TestCase):
    """bazel/rust_integration_tests.bzl, evaluated in Python."""

    def declare(self, files, **kwargs):
        with tempfile.TemporaryDirectory() as raw:
            tmp = Path(raw)
            for name in files:
                (tmp / name).parent.mkdir(parents=True, exist_ok=True)
                (tmp / name).write_text("")
            package = Package(tmp)
            package.macro()(**kwargs)
            return package.rules

    def test_one_target_per_cargo_test_root(self):
        rules = self.declare(["tests/a.rs", "tests/b/main.rs", "tests/b/util.rs", "tests/common/mod.rs",
                              "tests/fixtures/x.json", "src/lib.rs"])
        self.assertEqual(sorted(rules), ["a", "b"])
        _, a = rules["a"]
        self.assertEqual(a["crate_root"], "tests/a.rs")
        self.assertEqual(a["srcs"], ["tests/a.rs", "tests/b/main.rs", "tests/b/util.rs", "tests/common/mod.rs"])
        _, b = rules["b"]
        self.assertEqual(b["crate_root"], "tests/b/main.rs")
        self.assertNotIn("tests/a.rs", b["srcs"])
        self.assertEqual(gen.integration_test_roots(Path("/nonexistent")), [])

    def test_defaults_and_overrides(self):
        rules = self.declare(
            ["tests/plain.rs", "tests/source_lint.rs", "tests/special.rs", "tests/split.rs"],
            overrides={"special": {"manual": "why", "exclusive": True, "timeout": "long",
                                   "cargo": True, "workspace_sources": True}},
            exclude=["split"], data=[":bin_test_bin"], rustc_env={"X": "1"}, tags=["test-graph"],
        )
        self.assertEqual(sorted(rules), ["plain", "source_lint", "special"])
        plain = rules["plain"][1]
        self.assertEqual(plain["data"], ["//:workspace_test_data", ":bin_test_bin"])
        self.assertEqual(plain["tags"], ["test-graph"])
        self.assertIsNone(plain["timeout"])
        self.assertEqual(rules["source_lint"][1]["data"],
                         ["//:workspace_test_data", "//:workspace_rust_sources", ":bin_test_bin"])
        special = rules["special"][1]
        self.assertEqual(special["tags"], ["exclusive", "manual", "test-graph"])
        self.assertEqual(special["timeout"], "long")
        self.assertEqual(list(special["rustc_env"]), ["CARGO", "X"])
        self.assertIn("@rules_rust//rust/toolchain:current_cargo_files", special["data"])

    def test_crate_wide_workspace_sources(self):
        rules = self.declare(["tests/plain.rs"], workspace_sources=True)
        self.assertIn("//:workspace_rust_sources", rules["plain"][1]["data"])

    def test_stale_or_misspelled_overrides_fail(self):
        with self.assertRaisesRegex(StarlarkFailure, "override for gone"):
            self.declare(["tests/a.rs"], overrides={"gone": {"manual": "x"}})
        with self.assertRaisesRegex(StarlarkFailure, "unknown override key"):
            self.declare(["tests/a.rs"], overrides={"a": {"manul": "x"}})

    def test_name_rule_matches_the_generator(self):
        for name in ("tmp_hygiene_lint", "e2e_wss_lint_x", "lint", "plain"):
            native = type("native", (), {"glob": staticmethod(lambda *a, **k: [])})
            scope = evaluate_starlark(ROOT / "bazel/rust_integration_tests.bzl",
                                      {"native": native, "rust_test": None})
            self.assertEqual(scope["reads_workspace_sources"](name), gen.reads_workspace_sources_by_name(name))


@unittest.skipUnless(shutil.which("cargo"), "needs cargo")
class GeneratorDiscovery(unittest.TestCase):
    """What changes the generated files (and so fails the drift guard), on a scratch workspace."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="bazel-gen-test-"))
        self.addCleanup(shutil.rmtree, self.tmp)
        shutil.copy(ROOT / "rust-toolchain.toml", self.tmp / "rust-toolchain.toml")
        (self.tmp / "Cargo.toml").write_text('[workspace]\nresolver = "2"\nmembers = ["crates/*"]\n')
        self.crate("a", lib="pub mod m1;\npub mod m2;\n")
        self.write("crates/a/src/m1.rs", "pub fn one() {}\n")
        self.write("crates/a/src/m2.rs", "pub fn two() {}\n")
        self.write("crates/a/tests/existing.rs", "#[test]\nfn t() {}\n")
        for name, value in (("ROOT", self.tmp), ("MODULE_SHARDS", {"a": [["m1"], ["m2"]]})):
            patcher = mock.patch.object(gen, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.cargo("generate-lockfile", "--offline")
        self.write_generated()

    def write(self, rel, text):
        (self.tmp / rel).parent.mkdir(parents=True, exist_ok=True)
        (self.tmp / rel).write_text(text)

    def crate(self, name, lib="", extra=""):
        self.write(f"crates/{name}/Cargo.toml",
                   f'[package]\nname = "{name}"\nversion = "0.1.0"\nedition = "2021"\n{extra}')
        self.write(f"crates/{name}/src/lib.rs", lib)

    def cargo(self, *args):
        subprocess.run(["cargo", *args], cwd=self.tmp, check=True, capture_output=True)

    def write_generated(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(gen.main([]), 0)

    def stale(self):
        return sorted(str(p.relative_to(self.tmp)) for p, c in gen.generated_files().items()
                      if not p.exists() or p.read_text() != c)

    def check(self):
        err = io.StringIO()
        with contextlib.redirect_stderr(err), contextlib.redirect_stdout(io.StringIO()):
            code = gen.main(["--check"])
        return code, err.getvalue()

    def test_new_integration_tests_need_no_regeneration(self):
        self.write("crates/a/tests/new_test.rs", "#[test]\nfn t() {}\n")
        self.write("crates/a/tests/dir_test/main.rs", "mod util;\n")
        self.write("crates/a/tests/dir_test/util.rs", "")
        self.write("crates/a/tests/new_lint.rs", "// walks the workspace: source_lint\n")
        self.assertEqual(self.stale(), [])
        self.assertEqual(self.check()[0], 0)
        rules = Package(self.tmp / "crates/a").evaluate_build()
        for name, root in (("new_test", "tests/new_test.rs"), ("dir_test", "tests/dir_test/main.rs"),
                           ("existing", "tests/existing.rs")):
            kind, attrs = rules[name]
            self.assertEqual((kind, attrs["crate_root"]), ("rust_test", root))
        self.assertIn("//:workspace_rust_sources", rules["new_lint"][1]["data"])

    def test_first_test_in_a_crate_needs_no_regeneration(self):
        self.crate("b")
        self.cargo("generate-lockfile", "--offline")
        self.write_generated()
        self.write("crates/b/tests/first.rs", "#[test]\nfn t() {}\n")
        self.assertEqual(self.stale(), [])
        self.assertIn("first", Package(self.tmp / "crates/b").evaluate_build())

    def test_unit_test_growth_and_new_modules_keep_shards(self):
        self.write("crates/a/src/m1.rs", "pub fn one() {}\n" + "// grown\n" * 2000)
        self.write("crates/a/src/lib.rs", "pub mod m1;\npub mod m2;\npub mod m3;\n")
        self.write("crates/a/src/m3.rs", "")
        self.assertEqual(self.stale(), [])

    def test_test_needing_policy_fails_with_the_fix(self):
        self.write("crates/a/tests/reader.rs", 'const _: &str = "workspace_root(";\n')
        self.assertEqual(self.stale(), ["crates/a/BUILD.bazel"])
        self.assertIn('"reader": {"workspace_sources": True}', gen.generated_files()[self.tmp / "crates/a/BUILD.bazel"])
        code, err = self.check()
        self.assertEqual(code, 1)
        self.assertIn(gen.FIX_COMMAND, err)

    def test_new_crate_fails_and_names_it(self):
        self.crate("c")
        self.cargo("generate-lockfile", "--offline")
        code, err = self.check()
        self.assertEqual(code, 1)
        self.assertIn("crates/c/BUILD.bazel (missing: new crate c)", err)
        self.assertIn(f"Run: {gen.FIX_COMMAND}", err)

    def test_feature_change_fails(self):
        self.crate("a", lib="pub mod m1;\npub mod m2;\n", extra='\n[features]\ndefault = ["x"]\nx = []\n')
        self.assertIn("crates/a/BUILD.bazel", self.stale())

    def test_release_bump_needs_no_regeneration_and_builds_the_new_version(self):
        # release-plz bumps [package] versions without touching Bazel files; the Bazel
        # targets must still carry the new version (CARGO_PKG_VERSION, `--version`).
        manifest = self.tmp / "crates/a/Cargo.toml"
        manifest.write_text(manifest.read_text().replace('version = "0.1.0"', 'version = "0.2.0"'))
        self.cargo("generate-lockfile", "--offline")
        self.assertEqual(self.stale(), [])
        self.assertEqual(self.check()[0], 0)
        self.assertEqual(starlark_cargo_versions(self.tmp), {"a": "0.2.0"})
        rules = Package(self.tmp / "crates/a").evaluate_build()
        self.assertEqual(rules["a"][1]["version"], "0.2.0")
        self.assertEqual(rules["a_dev"][1]["version"], "0.2.0")

    def test_drift_guard_prints_the_same_fix(self):
        self.assertEqual(drift.FIX_COMMAND, gen.FIX_COMMAND)


# A stand-in for a libtest binary: `--list --format terse`, positional filters,
# `--skip` and `--exact` with libtest's matching (substring unless --exact).
FAKE_LIBTEST = r'''#!/usr/bin/env python3
import json, sys
tests = json.loads(open(sys.argv[0] + ".json").read())
args, filters, skips, exact, listing = sys.argv[1:], [], [], False, False
i = 0
while i < len(args):
    arg = args[i]
    if arg in ("--skip", "--format"):
        if arg == "--skip":
            skips.append(args[i + 1])
        i += 2
        continue
    if arg == "--exact":
        exact = True
    elif arg == "--list":
        listing = True
    elif not arg.startswith("-"):
        filters.append(arg)
    i += 1
match = (lambda p, n: p == n) if exact else (lambda p, n: p in n)
for name in tests:
    if (not filters or any(match(f, name) for f in filters)) and not any(match(s, name) for s in skips):
        print(f"{name}: test" if listing else f"RUN {name}")
'''


class ModuleShardSelection(unittest.TestCase):
    """Module shards select by real top-level module, not libtest's substring filters."""

    TESTS = [
        "m1::a", "m1::tests::b", "sub::m1::c", "tests::d", "tests::helpers::e",
        "discussion_ops::tests::y", "new_module::tests::x", "top_level_fn",
    ]

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="bazel-shard-test-"))
        self.addCleanup(shutil.rmtree, self.tmp)
        shutil.copy(ROOT / "rust-toolchain.toml", self.tmp / "rust-toolchain.toml")
        (self.tmp / "Cargo.toml").write_text('[workspace]\nresolver = "2"\nmembers = ["crates/*"]\n')
        lib = "".join(f"pub mod {m};\n" for m in ("m1", "sub", "tests", "discussion_ops", "new_module"))
        self.write("crates/a/Cargo.toml", '[package]\nname = "a"\nversion = "0.1.0"\nedition = "2021"\n')
        self.write("crates/a/src/lib.rs", lib)
        for module in ("m1", "sub", "tests", "discussion_ops", "new_module"):
            self.write(f"crates/a/src/{module}.rs", "")
        for name, value in (("ROOT", self.tmp), ("MODULE_SHARDS", {"a": [["m1"], ["tests"]]}),
                            ("MANUAL_SHARDS", {("a", 2): "test reason"})):
            patcher = mock.patch.object(gen, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        subprocess.run(["cargo", "generate-lockfile", "--offline"], cwd=self.tmp, check=True, capture_output=True)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(gen.main([]), 0)
        self.binary = self.tmp / "lib_test_binary"
        self.binary.write_text(FAKE_LIBTEST)
        self.binary.chmod(0o755)
        Path(str(self.binary) + ".json").write_text(json.dumps(self.TESTS))
        self.rules = Package(self.tmp / "crates/a").evaluate_build()

    def write(self, rel, text):
        (self.tmp / rel).parent.mkdir(parents=True, exist_ok=True)
        (self.tmp / rel).write_text(text)

    def run_target(self, name):
        """The tests the target runs, as `bazel test` would run it."""
        kind, attrs = self.rules[name]
        self.assertEqual((kind, attrs["binary"]), ("shared_binary_test", ":lib_test_binary"))
        command = [str(self.binary)]
        if attrs.get("include_modules") or attrs.get("exclude_modules"):
            launcher = self.tmp / f"{name}.sh"
            launcher.write_text(module_shard_launcher(
                str(self.binary), attrs.get("include_modules", []), attrs.get("exclude_modules", [])))
            launcher.chmod(0o755)
            command = [str(launcher)]
        out = subprocess.run(command + (attrs.get("args") or []), check=True, capture_output=True, text=True,
                             env={"PATH": str(self.minimal_path())})
        return {line[4:] for line in out.stdout.splitlines() if line.startswith("RUN ")}

    def minimal_path(self):
        """Only bash, env and python3 (for the fake binary): the launcher must not
        depend on host tools such as awk, whose variants differ between hosts
        (gawk rejects variable names mawk accepts, and a failure inside the
        selection went unseen)."""
        bin_dir = self.tmp / "minimal-bin"
        if not bin_dir.exists():
            bin_dir.mkdir()
            for tool, path in (("bash", shutil.which("bash")), ("env", shutil.which("env")),
                               ("python3", sys.executable)):
                (bin_dir / tool).symlink_to(path)
        return bin_dir

    def test_rest_shard_runs_nested_tests_of_unlisted_modules(self):
        rest = self.run_target("lib_test_shard_rest")
        self.assertIn("new_module::tests::x", rest)
        self.assertIn("discussion_ops::tests::y", rest)
        self.assertNotIn("manual", self.rules["lib_test_shard_rest"][1]["tags"])
        self.assertIn("manual", self.rules["lib_test_shard2"][1]["tags"])
        manual = self.run_target("lib_test_shard2")
        self.assertNotIn("new_module::tests::x", manual)
        self.assertNotIn("discussion_ops::tests::y", manual)

    def test_every_test_runs_in_exactly_its_top_level_modules_shard(self):
        selected = {name: self.run_target(name)
                    for name in ("lib_test_shard1", "lib_test_shard2", "lib_test_shard_rest")}
        self.assertEqual(selected, {
            "lib_test_shard1": {"m1::a", "m1::tests::b"},
            "lib_test_shard2": {"tests::d", "tests::helpers::e"},
            "lib_test_shard_rest": {"sub::m1::c", "discussion_ops::tests::y", "new_module::tests::x",
                                    "top_level_fn"},
        })

    def test_an_empty_shard_runs_nothing(self):
        launcher = self.tmp / "empty.sh"
        launcher.write_text(module_shard_launcher(str(self.binary), ["absent"], []))
        launcher.chmod(0o755)
        out = subprocess.run([str(launcher)], check=True, capture_output=True, text=True)
        self.assertNotIn("RUN ", out.stdout)


def module_shard_launcher(binary: str, include: list[str], exclude: list[str]) -> str:
    """bazel/module_shard_test.sh as shared_binary_test expands it."""
    template = (ROOT / "bazel/module_shard_test.sh").read_text()
    return (template.replace("@@BINARY@@", shlex.quote(binary))
            .replace("@@INCLUDE@@", " ".join(shlex.quote(m) for m in include))
            .replace("@@EXCLUDE@@", " ".join(shlex.quote(m) for m in exclude)))


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
