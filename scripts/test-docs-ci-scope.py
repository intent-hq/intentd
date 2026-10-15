#!/usr/bin/env python3
"""Offline classifier regressions using committed fixture repositories."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().with_name("docs-ci-scope.sh")
TOOLS = "crates/intent-acp/src/mcp_server/tools.rs"
GUIDE = "crates/intent-services/resources/assistant-app-guide.md"
BROWSER = "crates/intent-acp/src/browser_docs/overview.md"
FULL = {"tier": "full", "packages": [], "tests": []}
DOCS = {"tier": "docs", "packages": [], "tests": []}


def compiled(*packages, tests=()):
    return {"tier": "compiled", "packages": sorted(packages), "tests": [
        {"package": p, "target": t} for p, t in sorted(tests)
    ]}


class ClassifierTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="docs-ci-scope-")
        self.addCleanup(temporary.cleanup)
        self.repo = Path(temporary.name)
        self.git("init", "-q")
        self.git("config", "user.name", "Fixture")
        self.git("config", "user.email", "fixture@example.invalid")
        self.git("config", "core.filemode", "true")
        self.write("Cargo.toml", '[workspace]\nmembers = ["crates/*"]\n')
        for package in ("intent-acp", "intent-services", "intentd"):
            self.write(f"crates/{package}/Cargo.toml", f'[package]\nname = "{package}"\nversion = "0.1.0"\n')
            self.write(f"crates/{package}/src/lib.rs", "pub fn example() {}\n")
        self.write("README.md", "# Read me\n")
        self.write("docs/guide.md", "# Guide\n")
        self.write(BROWSER, "# Browser\n")
        self.write(GUIDE, "# Assistant\n")
        self.write("crates/intent-acp/src/browser.rs", 'const DOC: &str = include_str!("browser_docs/overview.md");\n')
        self.write("crates/intent-services/src/assistant.rs", 'const DOC: &str = include_str!("../resources/assistant-app-guide.md");\n')
        self.write("crates/intentd/tests/e2e_wss_system_prompt_fallback.rs", 'const DOC: &str = include_str!("../../intent-services/resources/assistant-app-guide.md");\n')
        self.write(TOOLS, 'pub(crate) const WORKSPACE_API_DESCRIPTION: &str = r###"old base help"###;\n'
                   'pub(crate) const WORKSPACE_API_DESCRIPTION_CHIEF: &str = r###"old chief help"###;\n'
                   'fn code() { let unrelated = "unchanged"; }\n')
        self.base = self.commit()

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.repo, text=True, stderr=subprocess.PIPE).strip()

    def write(self, path, content):
        dest = self.repo / path
        dest.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            dest.write_bytes(content)
        else:
            dest.write_text(content, encoding="utf-8")

    def replace(self, path, before, after):
        self.write(path, (self.repo / path).read_text().replace(before, after))

    def append(self, path, content):
        self.write(path, (self.repo / path).read_text() + content)

    def commit(self):
        self.git("add", "-A")
        self.git("commit", "--allow-empty", "-qm", "fixture")
        return self.git("rev-parse", "HEAD")

    def classify(self, expected, *refs, reason=None):
        if not refs:
            refs = (self.base, self.commit())
        result = subprocess.run(
            ["bash", str(SCRIPT), *refs], cwd=self.repo, capture_output=True,
            text=True, timeout=30, env={**os.environ, "PYTHON": sys.executable},
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), expected, result.stderr)
        self.assertEqual(len(result.stdout.splitlines()), 1)
        if reason:
            self.assertIn(reason, result.stderr)

    def source(self, text, path="crates/intent-acp/src/extra.rs"):
        self.write(path, text)
        self.base = self.commit()

    def test_readme_only(self):
        self.append("README.md", "More prose.\n")
        self.classify(DOCS)

    def test_readme_variants_and_docs_text(self):
        for path in ("README", "README.txt", "README.rst", "crates/intent-acp/README.adoc", "docs/manual.rst", "docs/manual.txt", "docs/manual.adoc"):
            self.write(path, "Plain prose\n")
        self.classify(DOCS)

    def test_docs_assets_are_byte_safe(self):
        self.write("docs/screenshot.png", b"\x89PNG\x00\xff")
        self.write("docs/manual.pdf", b"%PDF\x00\xff")
        self.classify(DOCS)

    def test_embedded_asset_is_compiled(self):
        self.write("docs/screenshot.png", b"\x89PNG\x00\xff")
        self.source('const PNG: &[u8] = include_bytes!("../../../docs/screenshot.png");')
        self.write("docs/screenshot.png", b"\x89PNG\x00\xee")
        self.classify(compiled("intent-acp"))

    def test_embedded_nonmarkdown_readme_is_compiled(self):
        self.write("README", "Original\n")
        self.source('const README: &str = include_str!("../../../README");')
        self.write("README", "Revised\n")
        self.classify(compiled("intent-acp"))

    def test_docs_code_and_unknown_extensions_are_full(self):
        for path in ("docs/build.rs", "docs/config.toml", "docs/site.js", "docs/data.json", "docs/page.html", "docs/unknown.blob", "README.sh"):
            with self.subTest(path=path):
                self.write(path, "new\n")
                self.classify(FULL)
                self.git("reset", "--hard", self.base)

    def test_added_deleted_and_renamed_pure_docs(self):
        (self.repo / "README.md").rename(self.repo / "docs/renamed.md")
        self.write("docs/new.md", "New\n")
        self.classify(DOCS)

    def test_paths_with_tabs_newlines_and_spaces(self):
        self.write("docs/a\tb\nc d.md", "Safe path parsing\n")
        self.classify(DOCS)

    def test_browser_doc(self):
        self.append(BROWSER, "Help\n")
        self.classify(compiled("intent-acp"))

    def test_assistant_guide_selects_direct_integration_consumer(self):
        self.append(GUIDE, "Help\n")
        self.classify(compiled("intent-services", tests=[("intentd", "e2e_wss_system_prompt_fallback")]))

    def test_pr_2375_shape(self):
        self.append(BROWSER, "Browser help\n")
        self.append(GUIDE, "Assistant help\n")
        self.replace(TOOLS, "old base help", 'new help with include_str!("fake.md")')
        self.replace(TOOLS, "old chief help", "new chief help")
        self.classify(compiled("intent-acp", "intent-services", tests=[("intentd", "e2e_wss_system_prompt_fallback")]))

    def test_description_only(self):
        self.replace(TOOLS, "old base help", "new base help")
        self.classify(compiled("intent-acp"))

    def test_help_change_with_code_is_full(self):
        self.replace(TOOLS, "old base help", "new help")
        self.replace(TOOLS, '"unchanged"', '"changed"')
        self.classify(FULL)

    def test_help_raw_delimiter_escape_cannot_hide_code(self):
        self.replace(TOOLS, "old base help", 'new"###; fn injected() {} const HIDDEN: &str = r###"text')
        self.classify(FULL)

    def test_help_declaration_change_is_full(self):
        self.replace(TOOLS, "pub(crate) const", "const")
        self.classify(FULL)

    def test_help_identifier_in_comment_is_inert(self):
        self.source('// const WORKSPACE_API_DESCRIPTION: &str = r###"fake"###;\n', TOOLS)
        self.replace(TOOLS, "fake", "changed")
        self.classify(FULL)

    def test_rust_workflow_manifests_lock_scripts_are_full(self):
        for path in ("crates/intent-acp/src/lib.rs", ".github/workflows/ci.yml", "Cargo.toml", "Cargo.lock", "scripts/run.sh", "crates/intent-acp/build.rs"):
            with self.subTest(path=path):
                self.write(path, "changed\n")
                self.classify(FULL)
                self.git("reset", "--hard", self.base)

    def test_mixed_docs_and_code_is_full(self):
        self.append(GUIDE, "Help\n")
        self.append("crates/intentd/src/lib.rs", "fn extra() {}\n")
        self.classify(FULL)

    def test_regular_mode_changes_are_full(self):
        (self.repo / "README.md").chmod(0o755)
        self.classify(FULL)

    def test_symlink_document_is_full(self):
        (self.repo / "docs/link.md").symlink_to("guide.md")
        self.classify(FULL)

    def test_missing_deleted_embedded_doc_is_full(self):
        (self.repo / GUIDE).unlink()
        self.classify(FULL, reason="included document missing")

    def test_renamed_embedded_doc_is_full(self):
        (self.repo / GUIDE).rename(self.repo / "crates/intent-services/resources/new.md")
        self.classify(FULL)

    def test_invalid_utf8_prose_is_full(self):
        self.write("README.md", b"\xff")
        self.classify(FULL)

    def test_nul_prose_is_full(self):
        self.write("README.md", b"hello\x00world")
        self.classify(FULL)

    def test_comments_strings_chars_and_lifetimes_are_inert(self):
        self.source('''// include_str!(unknown!())
/* outer /* include_bytes!(UNKNOWN) */ nested */
const TEXT: &str = r####"include_str!(unknown!()) /* not code */"####;
const TEXT2: &str = "escaped \\" include_str!(UNKNOWN)";
const BYTES: &[u8] = br##"include_bytes!(UNKNOWN)"##;
fn example<'a>(x: &'a str) { let quote = '\\''; let slash = '/'; }
''')
        self.append("README.md", "Prose\n")
        self.classify(DOCS)

    def test_raw_paths_comments_trailing_comma_and_namespace(self):
        self.source('const DOC: &str = std::include_str /* nested /* hi */ comment */ ! (r#"../../../README.md"#,);')
        self.append("README.md", "Prose\n")
        self.classify(compiled("intent-acp"))

    def test_concat_and_manifest_dir(self):
        self.source('const DOC: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../", "README.md"));')
        self.append("README.md", "Prose\n")
        self.classify(compiled("intent-acp"))

    def test_literal_macro_wrapper(self):
        self.write("crates/intent-acp/resources/v1/prompt.md", "Original\n")
        self.source('''macro_rules! instr {
    ($ver:literal, $name:literal) => {
        include_str!(concat!("../resources/", $ver, "/", $name, ".md"))
    };
}
const DOC: &str = instr!("v1", "prompt");
''')
        self.write("crates/intent-acp/resources/v1/prompt.md", "Revised\n")
        self.classify(compiled("intent-acp"))

    def test_unknown_dynamic_include_is_full(self):
        self.source('const DOC: &str = include_str!(unknown!());')
        self.append("README.md", "Prose\n")
        self.classify(FULL, reason="unresolved include")

    def test_aliased_include_macros_are_full(self):
        for macro in ("include_str", "include_bytes"):
            with self.subTest(macro=macro):
                self.source(f'use std::{{{macro} as read_doc}};\nconst DOC: &str = read_doc!("../../../README.md");')
                self.append("README.md", "Prose\n")
                self.classify(FULL, reason="import/alias")

    def test_unchanged_symlink_include_target_is_full(self):
        (self.repo / "crates/intent-acp/src/alias").symlink_to("../../../README.md")
        self.source('const DOC: &str = include_str!("alias");')
        self.append("README.md", "Prose\n")
        self.classify(FULL, reason="symlink/submodule")

    def test_symlink_ancestor_is_checked_before_dotdot_normalization(self):
        self.write("docs/folder/.keep", "")
        (self.repo / "crates/intent-acp/src/alias").symlink_to("../../../docs/folder")
        self.source('const DOC: &str = include_str!("alias/../guide.md");')
        self.append("docs/guide.md", "Prose\n")
        self.classify(FULL, reason="symlink/submodule")

    def test_unknown_dynamic_markdown_suffix_is_full(self):
        self.source('const DOC: &str = include_str!(concat!(env!("DOC_DIR"), "/README.md"));')
        self.append("README.md", "Prose\n")
        self.classify(FULL)

    def test_known_code_includes_and_unrelated_generated_suffix_do_not_disable_docs(self):
        self.source('''const CODE: &str = include_str!("lib.rs");
const LOCK: &str = include_str!(concat!(env!("OUT_DIR"), "/profile-Cargo.lock"));
''')
        self.append("README.md", "Prose\n")
        self.classify(DOCS)

    def test_nested_include_cannot_hide_changed_path_input(self):
        self.write("crates/intent-acp/src/path.md", "data")
        self.write("crates/intent-acp/src/data/body.txt", "Hello")
        self.source('const DOC: &str = include_str!(concat!(include_str!("path.md"), "/body.txt"));')
        self.write("crates/intent-acp/src/path.md", "missing")
        self.classify(FULL, reason="nested include")

    def test_nested_include_wrapper_cannot_hide_changed_path_input(self):
        self.write("crates/intent-acp/src/path.md", "data")
        self.source('''macro_rules! doc { ($path:literal) => { include_str!($path) }; }
const DOC: &str = include_str!(concat!(doc!("path.md"), "/body.txt"));
''')
        self.write("crates/intent-acp/src/path.md", "missing")
        self.classify(FULL, reason="nested include")

    def test_unrecognized_include_macro_rule_is_full(self):
        self.source('macro_rules! doc { ($x:expr) => { include_str!($x) }; }\nconst DOC: &str = doc!(PATH);')
        self.append("README.md", "Prose\n")
        self.classify(FULL)

    def test_unterminated_rust_literal_is_full(self):
        self.source('const DOC: &str = r###" include_str!("README.md")')
        self.append("README.md", "Prose\n")
        self.classify(FULL)

    def test_escaped_path_is_conservative(self):
        self.source('const DOC: &str = include_str!("../../../README\\u{2e}md");')
        self.append("README.md", "Prose\n")
        self.classify(FULL)

    def test_external_include_is_conservative(self):
        self.source('const DOC: &str = include_str!("/tmp/README.md");')
        self.append("README.md", "Prose\n")
        self.classify(FULL)

    def test_tests_submodule_has_ambiguous_owner(self):
        self.source('const DOC: &str = include_str!("../../../../README.md");', "crates/intentd/tests/common/mod.rs")
        self.append("README.md", "Prose\n")
        self.classify(FULL, reason="ambiguous integration")

    def test_explicit_test_target_name(self):
        self.append("crates/intentd/Cargo.toml", '\n[[test]]\nname = "guide-check"\npath = "integration/guide.rs"\n')
        self.source('const DOC: &str = include_str!("../../../README.md");', "crates/intentd/integration/guide.rs")
        self.append("README.md", "Prose\n")
        self.classify(compiled(tests=[("intentd", "guide-check")]))

    def test_disabled_auto_test_discovery_is_full(self):
        self.append("crates/intentd/Cargo.toml", "autotests = false\n")
        self.base = self.commit()
        self.append(GUIDE, "Help\n")
        self.classify(FULL)

    def test_malformed_owner_manifest_is_full(self):
        self.write("crates/intent-services/Cargo.toml", "invalid [toml")
        self.base = self.commit()
        self.append(GUIDE, "Help\n")
        self.classify(FULL)

    def test_missing_owner_manifest_is_full(self):
        (self.repo / "crates/intent-services/Cargo.toml").unlink()
        self.base = self.commit()
        self.append(GUIDE, "Help\n")
        self.classify(FULL)

    def test_empty_diff_is_full(self):
        self.classify(FULL, self.base, self.base, reason="empty")

    def test_missing_revision_is_full(self):
        self.classify(FULL, "missing-ref", self.base)

    def test_option_like_revision_is_full(self):
        self.classify(FULL, "--help", self.base)

    def test_usage_error_emits_full_json(self):
        self.classify(FULL, self.base, reason="usage:")

    def test_compares_supplied_revisions_not_current_worktree(self):
        self.append(BROWSER, "Help\n")
        head = self.commit()
        self.write("crates/intent-acp/src/lib.rs", "uncommitted code\n")
        self.classify(compiled("intent-acp"), self.base, head)

    def test_does_not_replace_divergent_base_with_merge_base(self):
        self.append("crates/intent-acp/src/lib.rs", "fn on_base() {}\n")
        base = self.commit()
        self.git("checkout", "-q", "--detach", self.base)
        self.append("README.md", "Help\n")
        head = self.commit()
        self.classify(FULL, base, head)


if __name__ == "__main__":
    unittest.main()
