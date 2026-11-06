//! Hard-coded current harness version lint.
//!
//! A test that spells the current harness version as a string literal
//! instead of reading `intent_core::CURRENT_HARNESS_VERSION` breaks on every
//! version bump, often in a crate far from the bump: intent-hq/intentd#2698
//! was ejected from the merge queue this way, and intent-hq/intentd#2717 had
//! to replace about a dozen copies across three crates. This source-scanning
//! test fails, naming `file:line`, for every string literal under
//! `crates/**/*.rs` whose value is the *current* version — the value is read
//! from the constant, so the lint follows each bump.
//!
//! - Matching uses the shared [`source_lint::lex`] lexer: only real string
//!   literals count (plain or raw, compared by cooked value), so a version
//!   quoted in a `//` or `/* … */` comment, or a longer string that merely
//!   contains it (`"3.30"`, `"harness 3.3"`), never does.
//! - Allowed: only the places that must name concrete versions — see
//!   [`ALLOWED`] and [`is_registry_file`]. Keep that list small; each entry
//!   carries its reason.
//! - This file's own fixtures use a synthetic version, so it never trips
//!   the scan.

use std::path::Path;

use intentd_test_support::source_lint::{self, cfg_test_item_ranges, lex};

/// How much of an allowlisted file may name the current version.
#[derive(Clone, Copy)]
enum Scope {
    /// The whole file.
    WholeFile,
    /// Only its `#[cfg(test)]` items; production code must use the constant.
    CfgTest,
}

/// The places that must name concrete versions, each with its reason. Every
/// `crates/intent-services/src/harness/vX_Y.rs` registry file is allowed as
/// well (see [`is_registry_file`]): it declares its own version.
const ALLOWED: &[(&str, Scope, &str)] = &[
    (
        "crates/intent-core/src/model.rs",
        Scope::WholeFile,
        "defines CURRENT_HARNESS_VERSION and the version lists, and tests them",
    ),
    (
        "crates/intent-services/src/harness/mod.rs",
        Scope::CfgTest,
        "version-specific doctrine tests pin the release they describe",
    ),
];

/// Remediation printed with every report.
const ADVICE: &str = "Use intent_core::CURRENT_HARNESS_VERSION for the current \
version, or intent_core::REPOSITORY_GUIDANCE_HARNESS_VERSIONS / \
REPOSITORY_MONITOR_GUIDANCE_HARNESS_VERSIONS for version sets, instead of a \
string literal that breaks on the next harness bump. If the literal must pin a \
specific release, add the file to ALLOWED in \
crates/intent-core/tests/harness_version_literal_lint.rs with a reason.";

/// `crates/intent-services/src/harness/vX_Y.rs` (or `vX.rs`): a harness
/// registry file, which declares its own version.
fn is_registry_file(rel: &str) -> bool {
    let Some(name) = rel.strip_prefix("crates/intent-services/src/harness/") else {
        return false;
    };
    let Some(stem) = name.strip_suffix(".rs").and_then(|s| s.strip_prefix('v')) else {
        return false;
    };
    !stem.is_empty()
        && stem
            .split('_')
            .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
}

fn scope_of(rel: &str) -> Option<Scope> {
    if is_registry_file(rel) {
        return Some(Scope::WholeFile);
    }
    ALLOWED
        .iter()
        .find(|(path, _, _)| *path == rel)
        .map(|(_, scope, _)| *scope)
}

/// `rel:line: <source line>` for every string literal in `src` whose value is
/// `version`, outside the file's allowance.
fn violations(rel: &str, src: &str, version: &str) -> Vec<String> {
    let scope = scope_of(rel);
    if matches!(scope, Some(Scope::WholeFile)) {
        return Vec::new();
    }
    let lexed = lex(src);
    let allowed_ranges = match scope {
        Some(Scope::CfgTest) => {
            let chars: Vec<char> = lexed.blanked.chars().collect();
            cfg_test_item_ranges(&chars)
        }
        _ => Vec::new(),
    };
    let lines: Vec<&str> = src.lines().collect();
    lexed
        .literals
        .iter()
        .filter(|lit| lit.cooked() == version)
        .filter(|lit| {
            !allowed_ranges
                .iter()
                .any(|(start, end)| (*start..*end).contains(&lit.offset))
        })
        .map(|lit| {
            let text = lines.get(lit.line - 1).map_or("", |l| l.trim());
            format!("{rel}:{}: {text}", lit.line)
        })
        .collect()
}

#[test]
fn no_hard_coded_current_harness_version_outside_allowlist() {
    let root = source_lint::workspace_root();
    let files = source_lint::rust_files(&root.join("crates"));
    assert!(
        files.len() > 100,
        "scanned only {} Rust files under {}; the lint is not seeing the source tree",
        files.len(),
        root.display()
    );
    let version = intent_core::CURRENT_HARNESS_VERSION;
    let mut found = Vec::new();
    for path in &files {
        let rel = path
            .strip_prefix(&root)
            .unwrap_or(path)
            .to_string_lossy()
            .replace('\\', "/");
        let src = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        found.extend(violations(&rel, &src, version));
    }
    assert!(
        found.is_empty(),
        "found the current harness version \"{version}\" hard-coded as a string literal:\n  {}\n\n{ADVICE}",
        found.join("\n  ")
    );
}

/// Every allowlisted path still exists, so the list cannot rot into silently
/// allowing a future file at a stale path.
#[test]
fn allowlist_paths_exist() {
    let root = source_lint::workspace_root();
    for (path, _, reason) in ALLOWED {
        assert!(!reason.is_empty(), "{path} needs a reason");
        assert!(
            Path::new(&root).join(path).is_file(),
            "allowlisted {path} does not exist"
        );
    }
}

const V: &str = "9.9";

#[test]
fn flags_literal_with_file_and_line() {
    let src = "fn t() {\n    assert_eq!(v, \"9.9\");\n    let r = r#\"9.9\"#;\n}\n";
    assert_eq!(
        violations("crates/intent-app/src/lib.rs", src, V),
        vec![
            "crates/intent-app/src/lib.rs:2: assert_eq!(v, \"9.9\");".to_string(),
            "crates/intent-app/src/lib.rs:3: let r = r#\"9.9\"#;".to_string(),
        ]
    );
}

#[test]
fn ignores_comments_and_other_strings() {
    let src = concat!(
        "// pinned to \"9.9\"\n",
        "/// doc \"9.9\"\n",
        "/* documentation: \"9.9\" */\n",
        "/*\n   multi-line \"9.9\"\n*/\n",
        "let current = CURRENT_HARNESS_VERSION; // previously \"9.9\"\n",
        "let a = \"9.90\";\n",
        "let b = \"19.9\";\n",
        "let c = \"harness 9.9\";\n",
    );
    assert!(
        violations("crates/x/src/lib.rs", src, V).is_empty(),
        "{:?}",
        violations("crates/x/src/lib.rs", src, V)
    );
}

#[test]
fn registry_files_are_allowed() {
    for rel in [
        "crates/intent-services/src/harness/v3_3.rs",
        "crates/intent-services/src/harness/v1.rs",
        "crates/intent-services/src/harness/v2_10.rs",
    ] {
        assert!(is_registry_file(rel), "{rel}");
        assert!(violations(rel, "version: \"9.9\",", V).is_empty());
    }
    for rel in [
        "crates/intent-services/src/harness/review_wake.rs",
        "crates/intent-services/src/harness/repository_guidance_v3.rs",
        "crates/intent-services/src/harness/v3_.rs",
        "crates/intent-services/src/harness/v.rs",
        "crates/intent-app/src/harness/v3_3.rs",
    ] {
        assert!(!is_registry_file(rel), "{rel}");
    }
}

#[test]
fn harness_mod_allows_only_its_cfg_test_items() {
    let rel = "crates/intent-services/src/harness/mod.rs";
    let src = concat!(
        "const X: &str = \"9.9\";\n",
        "#[cfg(test)]\n",
        "mod tests {\n",
        "    fn t() { let v = \"9.9\"; }\n",
        "}\n",
    );
    assert_eq!(
        violations(rel, src, V),
        vec![format!("{rel}:1: const X: &str = \"9.9\";")]
    );
}
