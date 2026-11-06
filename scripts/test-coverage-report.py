#!/usr/bin/env python3
"""Prove coverage-report.sh against real cargo-llvm-cov profiles: a truncated
.profraw no longer fails the merge, and missing or unreadable profiles fail
as a labelled tooling fault instead of looking like a test failure."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().with_name("coverage-report.sh")
SOURCE = """
fn main() {
    if std::env::args().count() > 5 {
        println!("many");
    } else {
        println!("few");
    }
}
"""


class CoverageReportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="coverage-report-")
        cls.project = Path(cls.temp.name) / "fixture"
        (cls.project / "src").mkdir(parents=True)
        (cls.project / "Cargo.toml").write_text(
            '[package]\nname = "fixture"\nversion = "0.0.0"\nedition = "2021"\n\n[workspace]\n')
        (cls.project / "src/main.rs").write_text(SOURCE)
        cls.env = {key: value for key, value in os.environ.items()
                   if key not in {"CARGO_LLVM_COV_TARGET_DIR", "LLVM_PROFILE_FILE_NAME", "CARGO"}}
        cls.env.update(CARGO_TARGET_DIR=str(Path(cls.temp.name) / "target"), GITHUB_ACTIONS="1")
        cls.profiles = Path(cls.env["CARGO_TARGET_DIR"]) / "llvm-cov-target"
        cls.saved = Path(cls.temp.name) / "saved"
        cls.saved.mkdir()
        for _ in range(2):
            cls.cargo("llvm-cov", "--no-report", "run", "--quiet")
        for profile in cls.profiles.glob("*.profraw"):
            shutil.copy2(profile, cls.saved / profile.name)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    @classmethod
    def cargo(cls, *args, check=True):
        return subprocess.run(["cargo", *args], cwd=cls.project, env=cls.env, check=check,
                              capture_output=True, text=True, timeout=600)

    def setUp(self):
        for profile in self.profiles.glob("*.profraw"):
            profile.unlink()
        for profile in self.saved.iterdir():
            shutil.copy2(profile, self.profiles / profile.name)
        self.assertEqual(len(self.profile_files()), 2)

    def profile_files(self):
        return sorted(self.profiles.glob("*.profraw"))

    def truncate(self, profile):
        profile.write_bytes(profile.read_bytes()[:20])

    def report(self, *args):
        return subprocess.run(["bash", str(SCRIPT), "--summary-only", *args], cwd=self.project,
                              env=self.env, capture_output=True, text=True, timeout=600)

    def test_truncated_profile_is_dropped_with_a_warning(self):
        corrupt = self.profile_files()[0]
        self.truncate(corrupt)
        plain = self.cargo("llvm-cov", "report", "--summary-only", check=False)
        self.assertNotEqual(plain.returncode, 0)
        self.assertIn("no profile can be merged", plain.stderr)

        result = self.report()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertRegex(result.stdout, r"(?m)^TOTAL")
        self.assertIn(corrupt.name, result.stderr)
        self.assertIn("dropped 1 of 2 .profraw files", result.stderr)
        self.assertIn("::warning title=Coverage tooling (not a test failure)::", result.stderr)

    def test_floor_miss_still_fails_as_a_coverage_result(self):
        result = self.report("--fail-under-lines", "100")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertRegex(result.stdout, r"(?m)^TOTAL")
        self.assertNotIn("tooling problem", result.stderr)

    def test_all_profiles_unreadable_is_a_tooling_error(self):
        for profile in self.profile_files():
            self.truncate(profile)
        result = self.report()
        self.assertEqual(result.returncode, 3, result.stderr)
        self.assertIn("coverage tooling problem, not a test failure", result.stderr)
        self.assertIn("none of the 2 .profraw files could be read", result.stderr)
        self.assertIn("::error title=Coverage tooling (not a test failure)::", result.stderr)

    def test_no_profiles_is_a_tooling_error(self):
        for profile in self.profile_files():
            profile.unlink()
        result = self.report()
        self.assertEqual(result.returncode, 3, result.stderr)
        self.assertIn("wrote no .profraw files", result.stderr)
        self.assertIn(str(self.profiles), result.stderr)
        self.assertNotIn("llvm-profdata", result.stderr)


if __name__ == "__main__":
    unittest.main()
