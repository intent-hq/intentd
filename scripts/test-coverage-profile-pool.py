#!/usr/bin/env python3
"""Prove that bounded LLVM profile pools retain concurrent execution counts."""

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class ProfilePoolTests(unittest.TestCase):
    def test_concurrent_pool_preserves_coverage_and_bounds_files(self):
        pattern = os.environ["LLVM_PROFILE_FILE_NAME"]
        libdir = Path(subprocess.check_output(
            ["rustc", "--print", "target-libdir"], text=True).strip())
        tools = libdir.parent / "bin"
        with tempfile.TemporaryDirectory(prefix="coverage-pool-") as directory:
            root = Path(directory)
            source = root / "fixture.rs"
            source.write_text("""
#[inline(never)]
fn left() -> u32 { 11 }
#[inline(never)]
fn right() -> u32 { 29 }
fn main() {
    let value = if std::env::args().nth(1).unwrap() == "left" {
        left()
    } else {
        right()
    };
    assert!(value > 0);
}
""")
            binary = root / "fixture"
            subprocess.run(["rustc", "-C", "instrument-coverage", str(source),
                            "-o", str(binary)], check=True)

            def collect(name, filename):
                folder = root / name
                folder.mkdir()
                env = {**os.environ, "LLVM_PROFILE_FILE": str(folder / filename)}

                def execute(index):
                    subprocess.run([str(binary), "left" if index % 3 else "right"],
                                   env=env, check=True, capture_output=True, timeout=20)

                with ThreadPoolExecutor(max_workers=16) as executor:
                    list(executor.map(execute, range(64)))
                profiles = sorted(folder.glob("*.profraw"))
                merged = folder / "merged.profdata"
                subprocess.run([str(tools / "llvm-profdata"), "merge", "-sparse",
                                *map(str, profiles), "-o", str(merged)], check=True)
                report = json.loads(subprocess.check_output([
                    str(tools / "llvm-cov"), "export", str(binary),
                    f"-instr-profile={merged}"], text=True))
                return profiles, report

            baseline_files, baseline = collect("baseline", "fixture-%p-%32m.profraw")
            pooled_files, pooled = collect("pooled", pattern)
            self.assertEqual(len(baseline_files), 64)
            self.assertGreater(len(pooled_files), 0)
            self.assertLessEqual(len(pooled_files), 8)
            baseline_bytes = sum(p.stat().st_size for p in baseline_files)
            pooled_bytes = sum(p.stat().st_size for p in pooled_files)
            self.assertLess(pooled_bytes, baseline_bytes)
            print(json.dumps({"baseline_files": len(baseline_files), "pooled_files": len(pooled_files),
                              "baseline_bytes": baseline_bytes, "pooled_bytes": pooled_bytes}), flush=True)
            self.assertEqual(pooled, baseline, "pooling changed coverage or execution counts")
            functions = {f["name"]: f["count"] for f in pooled["data"][0]["functions"]}
            self.assertIn(42, functions.values())
            self.assertIn(22, functions.values())


if __name__ == "__main__":
    unittest.main()
