#!/usr/bin/env python3
"""Tests for new-harness-version.py. The end-to-end cases run it on a copy of
this repo's real files, so a layout change that breaks one of its anchors
fails here instead of in the next harness bump."""
import importlib.util
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
SCRIPT = HERE / "new-harness-version.py"
SPEC = importlib.util.spec_from_file_location("new_harness_version", SCRIPT)
bump = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bump)


def current_version():
    text = (REPO / bump.MODEL).read_text()
    return re.search(r'pub const CURRENT_HARNESS_VERSION: &str = "([^"]+)";', text).group(1)


class RealTree(unittest.TestCase):
    """Bump a copy of this checkout from its current version to the next."""

    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="new-harness-version-")
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name) / "intentd"
        self.old = current_version()
        self.new = bump.next_minor(self.old)
        files = [bump.MODEL, bump.BREAKDOWN, bump.BUDGET, bump.HARNESS_MOD,
                 f"{bump.HARNESS_DIR}/{bump.module(self.old)}.rs"]
        for relative in files:
            target = self.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(REPO / relative, target)

    def run_script(self, *args):
        return subprocess.run([sys.executable, "-I", "-B", str(SCRIPT), "--root", str(self.root), *args],
                              capture_output=True, text=True)

    def read(self, relative):
        return (self.root / relative).read_text()

    def snapshot(self):
        return {p: p.read_bytes() for p in sorted(self.root.rglob("*")) if p.is_file()}

    def test_bumps_every_routine_place(self):
        result = self.run_script("--from", self.old, "--to", self.new)
        self.assertEqual(result.returncode, 0, result.stderr)
        old, new = f'"{self.old}"', f'"{self.new}"'

        model = self.read(bump.MODEL)
        self.assertIn(f"pub const CURRENT_HARNESS_VERSION: &str = {new};", model)
        for name in bump.GUIDANCE_LISTS:
            block = re.search(rf"pub const {name}: &\[&str\] = &\[\n(.*?)\n\];", model, re.S).group(1)
            self.assertTrue(re.findall(r'"[^"]*"', block)[-2:] == [old, new], name)
            self.assertTrue(all(len(line) <= bump.MAX_WIDTH for line in block.split("\n")), name)

        breakdown = self.read(bump.BREAKDOWN)
        self.assertRegex(breakdown, rf'matches!\([^)]*{re.escape(old)} \| {re.escape(new)}\s*\)')
        self.assertIn(f"assert!(harness_records_prompt_breakdown({old}));\n"
                      f"        assert!(harness_records_prompt_breakdown({new}));", breakdown)
        negatives = re.search(r"for version in \[(.*?)\]", breakdown, re.S).group(1)
        self.assertNotIn(new, negatives)

        self.assertIn(f"const PREVIOUS: &str = {old};", self.read(bump.BUDGET))

        harness_mod = self.read(bump.HARNESS_MOD)
        self.assertIn(f"    &{bump.module(self.old)}::ENTRY,\n    &{bump.module(self.new)}::ENTRY,\n];", harness_mod)
        mods = re.findall(r"^pub\(crate\) mod (v[\d_]+);$", harness_mod, re.M)
        self.assertIn(bump.module(self.new), mods)
        self.assertEqual(mods, sorted(mods))

        previous = self.read(f"{bump.HARNESS_DIR}/{bump.module(self.old)}.rs")
        self.assertNotIn("CURRENT_HARNESS_VERSION", previous)
        self.assertNotIn("STAGED_HARNESS_VERSION", previous)
        self.assertIn(f".version, {old});", previous)

        created = self.read(f"{bump.HARNESS_DIR}/{bump.module(self.new)}.rs")
        self.assertIn(f"version: {new},", created)
        self.assertIn(f"super::{bump.module(self.old)}::ENTRY.doctrine", created)
        self.assertNotIn("ENTRY.default_features", created)
        self.assertNotIn("ENTRY.feature_labels", created)
        self.assertIn(f"assert_eq!(current.version, {new});", created)
        self.assertEqual(bump.current_pins(created), [])
        self.assertTrue(all(len(line) <= bump.MAX_WIDTH for line in created.split("\n")))

    def test_a_scaffolded_module_survives_the_next_bump(self):
        newer = bump.next_minor(self.new)
        self.assertEqual(self.run_script("--from", self.old, "--to", self.new).returncode, 0)
        result = self.run_script("--from", self.new, "--to", newer)
        self.assertEqual(result.returncode, 0, result.stderr)
        for version in (self.old, self.new, newer):
            text = self.read(f"{bump.HARNESS_DIR}/{bump.module(version)}.rs")
            self.assertEqual(bump.current_pins(text), [], version)

    def test_rewrites_a_latest_entry_check_in_the_previous_module(self):
        path = self.root / bump.HARNESS_DIR / f"{bump.module(self.old)}.rs"
        path.write_text(path.read_text() + LEGACY_TEST)
        result = self.run_script("--from", self.old, "--to", self.new)
        self.assertEqual(result.returncode, 0, result.stderr)
        previous = path.read_text()
        self.assertEqual(bump.current_pins(previous), [])
        self.assertIn(f'        let current = &ENTRY;\n        assert_eq!(current.version, "{self.old}");\n    }}\n}}\n',
                      previous)

    def test_refuses_a_previous_module_that_still_pins_the_latest_version(self):
        path = self.root / bump.HARNESS_DIR / f"{bump.module(self.old)}.rs"
        path.write_text(path.read_text() + LEGACY_TEST.replace(
            "assert!(std::ptr::eq(crate::harness::latest_entry(), current));",
            "assert_eq!(super::super::latest_entry().version, current.version);"))
        before = self.snapshot()
        result = self.run_script("--from", self.old, "--to", self.new)
        self.assertEqual(result.returncode, 1)
        self.assertIn("latest_entry().version, current.version", result.stderr)
        self.assertEqual(self.snapshot(), before)

    def test_writes_nothing_when_an_anchor_is_missing(self):
        before = self.snapshot()
        stale = bump.next_minor(self.new)
        result = self.run_script("--from", self.new, "--to", stale)
        self.assertEqual(result.returncode, 1)
        self.assertIn(f'CURRENT_HARNESS_VERSION = "{self.new}"', result.stderr)
        self.assertEqual(self.snapshot(), before)

    def test_refuses_an_existing_module_and_a_dry_run_writes_nothing(self):
        before = self.snapshot()
        result = self.run_script("--from", self.old, "--to", self.new, "--dry-run")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.run_script("--from", self.old, "--to", self.new).returncode, 0)
        result = self.run_script("--from", self.new, "--to", self.new)
        self.assertEqual(result.returncode, 2)
        result = self.run_script("--from", self.old, "--to", self.new)
        self.assertEqual(result.returncode, 1)
        self.assertIn("already exists", result.stderr)


# The 3.17 test that failed after the 3.18 bump (fixed by hand in intentd#3847).
LEGACY_TEST = """
#[cfg(test)]
mod legacy_tests {
    use super::*;

    #[test]
    fn older_harnesses_have_no_goal_surface_and_this_is_current() {
        let current = &ENTRY;
        assert_eq!(current.version, intent_core::CURRENT_HARNESS_VERSION);
        assert_eq!(intent_core::STAGED_HARNESS_VERSION, None);
        assert!(std::ptr::eq(crate::harness::latest_entry(), current));
    }
}
"""


class PerVersionModules(unittest.TestCase):
    def test_no_module_pins_the_current_version(self):
        """Only harness/mod.rs checks which version is current, so a bump has
        no per-version copy of that check to rewrite."""
        modules = sorted((REPO / bump.HARNESS_DIR).glob("v*.rs"))
        self.assertTrue(modules)
        for path in modules:
            self.assertEqual(bump.current_pins(path.read_text()), [], path.name)

    def test_latest_entry_shapes(self):
        for check in ("assert!(std::ptr::eq(crate::harness::latest_entry(), current));",
                      "assert!(std::ptr::eq(current, super::super::latest_entry()));",
                      "assert!(std::ptr::eq(super::latest_entry(), &ENTRY));"):
            text = f"    fn t() {{\n        let current = &ENTRY;\n        {check}\n    }}\n"
            out = bump.edit_previous_module(text, "3.17")
            self.assertEqual(bump.current_pins(out), [], check)
            self.assertRegex(out, r'assert_eq!\((current|ENTRY)\.version, "3\.17"\);')
        with self.assertRaises(bump.Mismatch):
            bump.edit_previous_module("    assert_eq!(LATEST_VERSION, ENTRY.version);\n", "3.17")
        doc = "/// it was `CURRENT_HARNESS_VERSION` when it shipped.\n"
        self.assertEqual(bump.edit_previous_module(doc, "3.17"), doc)


SAMPLES = '        for version in [\n            "3.9", "garbage",\n        ] {\n'


class Layout(unittest.TestCase):
    def test_pack_matches_rustfmt(self):
        items = [f'"3.{i}"' for i in range(2, 17)]
        self.assertEqual(bump.pack(items),
                         '    "3.2", "3.3", "3.4", "3.5", "3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12", "3.13", "3.14",\n'
                         '    "3.15", "3.16",')

    def test_long_admission_wraps_like_rustfmt(self):
        alternatives = " | ".join(f'"3.{i}"' for i in range(10, 19))
        text = f"    matches!(harness_version, {alternatives})\n" \
               '        assert!(harness_records_prompt_breakdown("3.18"));\n' + SAMPLES
        out = bump.edit_breakdown(text, "3.18", "3.19")
        self.assertTrue(out.startswith(f'    matches!(\n        harness_version,\n        {alternatives} | "3.19"\n    )\n'))
        tall = bump.edit_breakdown(out, "3.19", "3.20")
        self.assertIn('    matches!(\n        harness_version,\n        "3.10"\n            | "3.11"\n', tall)
        self.assertIn('            | "3.19"\n            | "3.20"\n    )\n', tall)
        again = bump.edit_breakdown(tall, "3.20", "3.21")
        self.assertIn('            | "3.20"\n            | "3.21"\n    )\n', again)

    def test_admission_wraps_past_the_call_width(self):
        def admission(count):
            alternatives = " | ".join(f'"3.{i}"' for i in range(10, 10 + count))
            return f"    matches!(harness_version, {alternatives})\n" \
                   f'        assert!(harness_records_prompt_breakdown("3.{9 + count}"));\n' + SAMPLES
        self.assertIn('matches!(harness_version, "3.10" | "3.11" | "3.12" | "3.13" | "3.14")',
                      bump.edit_breakdown(admission(4), "3.13", "3.14"))
        self.assertIn('    matches!(\n        harness_version,\n        "3.10" | "3.11" | "3.12" | "3.13" | "3.14" | "3.15"\n    )',
                      bump.edit_breakdown(admission(5), "3.14", "3.15"))

    def test_a_major_bump_moves_the_not_admitted_sample(self):
        text = ('    matches!(harness_version, "3.14" | "3.15")\n'
                '        assert!(harness_records_prompt_breakdown("3.15"));\n'
                '        for version in [\n            "3.9", "3.16", "4.0", "garbage",\n        ] {\n')
        out = bump.edit_breakdown(text, "3.15", "4.0")
        self.assertIn('"3.14" | "3.15" | "4.0")', out)
        self.assertIn('"3.9", "3.16", "4.1", "garbage",', out)
        self.assertIn('"3.9", "3.17", "4.0", "garbage",', bump.edit_breakdown(text, "3.15", "3.16"))
        with self.assertRaises(bump.Mismatch):
            bump.edit_breakdown(text.replace("for version in", "for v in"), "3.15", "4.0")

    def test_mod_line_sorts_as_a_string(self):
        text = "use x;\npub(crate) mod v3_15;\npub(crate) mod v3_9;\n\nstatic REGISTRY: &[&HarnessEntry] = &[\n    &v3_9::ENTRY,\n    &v3_15::ENTRY,\n];\n"
        out = bump.edit_harness_mod(text, "3.15", "3.16")
        self.assertIn("pub(crate) mod v3_15;\npub(crate) mod v3_16;\npub(crate) mod v3_9;\n", out)
        out = bump.edit_harness_mod(text, "3.15", "4.0")
        self.assertIn("pub(crate) mod v3_9;\npub(crate) mod v4_0;\n", out)
        with self.assertRaises(bump.Mismatch):
            bump.edit_harness_mod(text, "3.9", "3.16")


if __name__ == "__main__":
    unittest.main()
