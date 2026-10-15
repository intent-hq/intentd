#!/usr/bin/env python3
"""Exercise the real Cargo package layout without compiling the daemon workspace."""
import fcntl
import os
from pathlib import Path
import re
import select
import shutil
import signal
import subprocess
import sys
import tempfile
import tomllib
import unittest

ROOT = Path(__file__).resolve().parents[1]
STORE = ROOT / "crates/intent-store"
WRAPPER = ROOT / "scripts/release-plz-cargo.py"
BROKEN = 'include_str!("../../../Cargo.lock")'
# The real build helper parses manifests with the `toml` crate, which this
# offline fixture cannot resolve. Its logic is unit-tested in intent-store
# (src/tests/note_index_profile.rs); this std-only stand-in keeps the helper's
# interface so the real build.rs and the Cargo package layout are exercised.
HELPER = "build/note_index_dependencies.rs"
HELPER_STAND_IN = """use std::path::{Path, PathBuf};
pub struct Inputs { pub lock: String, pub manifests: Vec<String>, pub watched: Vec<PathBuf> }
pub fn collect(manifest_dir: &Path) -> Result<Inputs, String> {
    let lock = manifest_dir.ancestors().map(|dir| dir.join("Cargo.lock"))
        .find(|path| path.is_file()).ok_or("Cargo.lock is required")?;
    let text = std::fs::read_to_string(&lock).map_err(|error| error.to_string())?;
    Ok(Inputs { lock: text, manifests: Vec::new(), watched: vec![lock] })
}
pub fn dependency_profile(lock: &str, _manifests: &[&str]) -> Result<String, String> {
    Ok(lock.to_owned())
}
"""


class PackagingTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="release-packaging-test-")
        self.addCleanup(temp.cleanup)
        self.repo = Path(temp.name)
        self.crate = self.repo / "crates/intent-store"
        (self.crate / "src").mkdir(parents=True)
        (self.repo / "Cargo.toml").write_text('[workspace]\nmembers = ["crates/*"]\nresolver = "2"\n')
        (self.crate / "Cargo.toml").write_text(
            '[package]\nname = "intent-store"\nversion = "0.1.0"\nedition = "2021"\n'
            'description = "Packaging fixture"\nlicense = "MIT"\n')
        (self.crate / "src/lib.rs").write_text('mod note_page_index;\npub use note_page_index::LOCK;\n')
        (self.crate / "src/main.rs").write_text('fn main() { print!("{}", intent_store::LOCK); }\n')
        self.env = {**os.environ, "CARGO_TARGET_DIR": str(self.repo / "target"),
                    "CARGO_TERM_PROGRESS_WHEN": "never", "CARGO_NET_OFFLINE": "true",
                    "RELEASE_PLZ_REAL_CARGO": shutil.which("cargo")}
        for key in ("CARGO", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "GITHUB_TOKEN", "GH_TOKEN"):
            self.env.pop(key, None)
        self.run_ok("cargo", "generate-lockfile")

    def run_command(self, *args):
        return subprocess.run(args, cwd=self.repo, env=self.env, capture_output=True,
                              text=True, timeout=120)

    def run_ok(self, *args):
        result = self.run_command(*args)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout

    def source(self, expression):
        (self.crate / "src/note_page_index.rs").write_text(f"pub const LOCK: &str = {expression};\n")

    def current_source(self):
        source = (STORE / "src/note_page_index.rs").read_text()
        expression, = re.findall(
            r'include_str!\(concat!\(env!\("OUT_DIR"\), "/note-index-dependencies\.txt"\)\)', source)
        self.source(expression)
        shutil.copyfile(STORE / "build.rs", self.crate / "build.rs")
        self.assertIn(f'#[path = "{HELPER}"]', (STORE / "build.rs").read_text())
        self.assertTrue((STORE / HELPER).is_file())
        (self.crate / HELPER).parent.mkdir(exist_ok=True)
        (self.crate / HELPER).write_text(HELPER_STAND_IN)

    def add_dependent(self):
        dependent = self.repo / "crates/dependent"
        (dependent / "src").mkdir(parents=True)
        (dependent / "Cargo.toml").write_text(
            '[package]\nname = "dependent"\nversion = "0.1.0"\nedition = "2021"\n'
            '[dependencies]\nintent-store = { path = "../intent-store", version = "0.1.0" }\n')
        (dependent / "src/lib.rs").write_text('pub const LOCK: &str = intent_store::LOCK;\n')

    def test_current_source_packages_and_verifies(self):
        self.add_dependent()
        self.current_source()
        self.run_ok("cargo", "package", "--allow-dirty", "--workspace")
        packaged = self.repo / "target/package/intent-store-0.1.0"
        self.assertTrue((packaged / HELPER).is_file())
        self.assertEqual(self.run_ok("cargo", "run", "--quiet", "--manifest-path",
                                    str(packaged / "Cargo.toml")),
                         (packaged / "Cargo.lock").read_text())

    def test_workspace_lock_changes_rebuild_embedded_input(self):
        self.current_source()
        lock = self.repo / "Cargo.lock"
        self.assertEqual(self.run_ok("cargo", "run", "--quiet"), lock.read_text())
        lock.write_text(lock.read_text() + "\n# changed parser dependency graph\n")
        self.assertEqual(self.run_ok("cargo", "run", "--quiet"), lock.read_text())

    def test_historical_failure_and_planning_compatibility(self):
        self.add_dependent()
        self.source(BROKEN)
        failed = self.run_command("cargo", "package", "--allow-dirty", "--workspace")
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("couldn't read `src/../../../Cargo.lock`", failed.stderr)
        source = self.crate / "src/note_page_index.rs"
        original = source.read_bytes()
        for marker in ("first baseline", "second baseline"):
            lock = self.repo / "Cargo.lock"
            lock.write_text(lock.read_text() + f"\n# {marker}\n")
            self.run_ok(sys.executable, str(WRAPPER), "package", "--allow-dirty", "--workspace")
            self.assertEqual(self.run_ok(str(self.repo / "target/debug/intent-store")), lock.read_text())
            self.assertEqual(source.read_bytes(), original)
            packaged_source = self.repo / "target/package/intent-store-0.1.0/src/note_page_index.rs"
            self.assertEqual(packaged_source.read_bytes(), original)
            self.assertFalse((self.repo / "target/Cargo.lock").exists())

    def test_fixed_source_needs_no_compatibility_file(self):
        self.current_source()
        self.run_ok(sys.executable, str(WRAPPER), "package", "--allow-dirty", "--workspace")
        self.assertFalse((self.repo / "target/Cargo.lock").exists())

    def test_wrapper_preserves_cargo_failure(self):
        self.source('"unused"')
        (self.crate / "src/lib.rs").write_text('compile_error!("unrelated failure");\n')
        failed = self.run_command(sys.executable, str(WRAPPER), "package", "--allow-dirty", "--workspace")
        self.assertEqual(failed.returncode, 101, failed.stderr)
        self.assertIn("unrelated failure", failed.stderr)

    def test_historical_source_is_restored_on_failure(self):
        self.source(BROKEN)
        source = self.crate / "src/note_page_index.rs"
        original = source.read_bytes()
        (self.crate / "src/lib.rs").write_text('compile_error!("unrelated failure");\n')
        failed = self.run_command(sys.executable, str(WRAPPER), "package", "--allow-dirty", "--workspace")
        self.assertEqual(failed.returncode, 101, failed.stderr)
        self.assertIn("unrelated failure", failed.stderr)
        self.assertEqual(source.read_bytes(), original)

    def test_interruption_restores_source_and_holds_worktree_lock(self):
        self.source(BROKEN)
        source = self.crate / "src/note_page_index.rs"
        original = source.read_bytes()
        fake = self.repo / "cargo-fixture"
        fake.write_text(f"#!{sys.executable}\nimport signal\nprint('ready', flush=True)\nsignal.pause()\n")
        fake.chmod(0o755)
        self.env["RELEASE_PLZ_REAL_CARGO"] = str(fake)
        with subprocess.Popen([sys.executable, str(WRAPPER), "package", "--allow-dirty", "--workspace"],
                              cwd=self.repo, env=self.env, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True) as process:
            try:
                ready, _, _ = select.select([process.stdout], [], [], 10)
                self.assertTrue(ready, "adapter did not launch Cargo")
                self.assertEqual(process.stdout.readline().strip(), "ready")
                with source.open("rb") as handle:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                process.send_signal(signal.SIGTERM)
                process.communicate(timeout=10)
                self.assertEqual(process.returncode, 128 + signal.SIGTERM)
            finally:
                if process.poll() is None:
                    process.terminate()
                    process.communicate(timeout=10)
        self.assertEqual(source.read_bytes(), original)

    def test_non_packaging_command_has_no_compatibility_side_effect(self):
        self.source(BROKEN)
        self.run_ok(sys.executable, str(WRAPPER), "metadata", "--no-deps", "--format-version", "1")
        self.assertFalse((self.repo / "target/Cargo.lock").exists())

    def test_adapter_is_confined_to_release_planning(self):
        workflow = (ROOT / ".github/workflows/release-plz.yml").read_text()
        release, planning = workflow.split("  release-plz-pr:", 1)
        self.assertNotIn("release-plz-cargo.py", release)
        self.assertIn("CARGO: ${{ github.workspace }}/scripts/release-plz-cargo.py", planning)
        self.assertIn("RELEASE_PLZ_REAL_CARGO: ${{ steps.planning_cargo.outputs.path }}", planning)
        self.assertIn('path=$(command -v cargo)', planning)
        self.assertIn("scripts/test-release-plz-packaging.py",
                      (ROOT / ".github/workflows/ci.yml").read_text())

    def test_real_release_plz_updates_from_broken_baseline(self):
        binary = os.environ.get("RELEASE_PLZ", "release-plz")
        self.assertEqual(self.run_ok(binary, "--version").strip(), "release-plz 0.3.162")
        self.env.update({"GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1",
                         "GIT_CONFIG_COUNT": "0", "GIT_AUTHOR_NAME": "Fixture",
                         "GIT_COMMITTER_NAME": "Fixture", "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
                         "GIT_COMMITTER_EMAIL": "fixture@example.invalid"})
        for key in ("GIT_DIR", "GIT_INDEX_FILE", "GIT_WORK_TREE", "GIT_TOKEN", "GIT_CLIFF_CONFIG"):
            self.env.pop(key, None)
        self.run_ok("git", "init", "-q", "-b", "main")
        (self.repo / ".gitignore").write_text("target/\n")
        (self.repo / "release-plz.toml").write_text(
            '[workspace]\npublish = false\ngit_only = true\nsemver_check = false\n'
            'git_tag_name = "v{{ version }}"\n')
        self.source(BROKEN)
        self.run_ok("git", "add", ".")
        self.run_ok("git", "commit", "-qm", "chore: fixture baseline")
        self.run_ok("git", "tag", "v0.1.0")
        self.current_source()
        self.run_ok("git", "add", ".")
        self.run_ok("git", "commit", "-qm", "fix: package lockfile input")
        self.env["CARGO"] = str(WRAPPER)
        self.run_ok(binary, "update", "--repo-url", "https://github.com/example/packaging-fixture")
        manifest = tomllib.loads((self.crate / "Cargo.toml").read_text())
        self.assertEqual(manifest["package"]["version"], "0.1.1")
        changed = set(self.run_ok("git", "diff", "--name-only").splitlines())
        self.assertEqual(changed, {"Cargo.lock", "crates/intent-store/Cargo.toml"})
        self.assertNotIn("INTENT_STORE_BASELINE_LOCK",
                         (self.crate / "src/note_page_index.rs").read_text())
        self.assertEqual(self.run_ok("git", "tag").strip(), "v0.1.0")
        # The next release's fixed baseline must work with ordinary Cargo again.
        self.run_ok("git", "add", ".")
        self.run_ok("git", "commit", "-qm", "chore: fixture release")
        self.run_ok("git", "tag", "v0.1.1")
        self.env.pop("CARGO")
        self.run_ok(binary, "update", "--repo-url", "https://github.com/example/packaging-fixture")
        self.assertEqual(self.run_ok("git", "status", "--porcelain"), "")


if __name__ == "__main__":
    unittest.main()
