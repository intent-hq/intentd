//! Guard against hard-coded copies of the current harness version.
//!
//! Tests that spell the current harness version as a string literal (for
//! example `"3.3"`) instead of reading `intent_core::CURRENT_HARNESS_VERSION`
//! break on every version bump, in crates far from the bump itself:
//! intentd#2698 was ejected from the merge queue this way and intentd#2717 had
//! to replace about a dozen copies across three crates. This test scans every
//! `crates/**/*.rs` file for the quoted literal of the *current* version (the
//! pattern is built from the constant, so it follows each bump) and fails with
//! the file, line and the constant to use instead.
//!
//! Only the places that must name versions are allowed; see [`ALLOWED`].

use std::path::{Path, PathBuf};

/// How much of an allowlisted file may name the current version.
#[derive(Clone, Copy)]
enum Scope {
    /// The whole file.
    WholeFile,
    /// Only the `mod tests` block (from its `mod tests {` line to the end of
    /// the file); production code above it must still use the constant.
    TestModule,
}

/// The places that must name concrete versions. Keep this list small: each
/// entry needs a reason a version-specific literal is the point of the code.
const ALLOWED: &[(&str, Scope, &str)] = &[
    (
        "crates/intent-core/src/model.rs",
        Scope::WholeFile,
        "defines CURRENT_HARNESS_VERSION and the version lists, and tests them",
    ),
    (
        "crates/intent-services/src/harness/mod.rs",
        Scope::TestModule,
        "version-specific doctrine tests pin the release they describe",
    ),
    // Each `crates/intent-services/src/harness/vX_Y.rs` registry file is
    // allowed too (see `is_registry_file`): it declares its own version.
];

/// Remediation shown with every violation.
const ADVICE: &str = "use intent_core::CURRENT_HARNESS_VERSION for the current \
version, or intent_core::REPOSITORY_GUIDANCE_HARNESS_VERSIONS / \
REPOSITORY_MONITOR_GUIDANCE_HARNESS_VERSIONS for version sets, instead of a \
string literal that breaks on the next harness bump. If the literal must pin a \
specific release, add the file to ALLOWED in \
crates/intent-services/src/harness/version_literal_guard.rs with a reason.";

/// `crates/intent-services/src/harness/vX_Y.rs` (or `vX.rs`): a registry file
/// that declares its own version.
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

/// First 1-based line from which the file may name the version, or `None`
/// when the file is not allowlisted.
fn allowed_from_line(rel: &str, content: &str) -> Option<usize> {
    if is_registry_file(rel) {
        return Some(1);
    }
    let (_, scope, _) = ALLOWED.iter().find(|(path, _, _)| *path == rel)?;
    match scope {
        Scope::WholeFile => Some(1),
        Scope::TestModule => content
            .lines()
            .position(|line| line.trim_start().starts_with("mod tests {"))
            .map(|idx| idx + 1),
    }
}

/// Every `path:line` in `content` that contains `needle` (the quoted current
/// version) outside comments and outside the file's allowance.
fn violations(rel: &str, content: &str, needle: &str) -> Vec<String> {
    let allowed_from = allowed_from_line(rel, content).unwrap_or(usize::MAX);
    content
        .lines()
        .enumerate()
        .map(|(idx, line)| (idx + 1, line))
        .filter(|(n, _)| *n < allowed_from)
        .filter(|(_, line)| !line.trim_start().starts_with("//"))
        .filter(|(_, line)| line.contains(needle))
        .map(|(n, line)| format!("{rel}:{n}: {}", line.trim()))
        .collect()
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries =
        std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()));
    for entry in entries {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        let file_type = entry.file_type().expect("file type");
        if file_type.is_dir() {
            if entry.file_name() != "target" {
                rust_files(&path, out);
            }
        } else if file_type.is_file() && path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("intentd workspace root")
}

fn needle() -> String {
    format!("\"{}\"", intent_core::CURRENT_HARNESS_VERSION)
}

#[test]
fn no_hard_coded_current_harness_version_outside_allowlist() {
    let root = workspace_root();
    let mut files = Vec::new();
    rust_files(&root.join("crates"), &mut files);
    assert!(
        files.len() > 100,
        "scanned only {} Rust files under {}; the guard is not seeing the source tree",
        files.len(),
        root.display()
    );
    let needle = needle();
    let mut found = Vec::new();
    for path in &files {
        let rel = path
            .strip_prefix(&root)
            .expect("file under root")
            .to_string_lossy()
            .replace('\\', "/");
        let Ok(content) = std::fs::read_to_string(path) else {
            continue;
        };
        found.extend(violations(&rel, &content, &needle));
    }
    found.sort();
    assert!(
        found.is_empty(),
        "found the current harness version {needle} hard-coded as a string literal:\n  {}\n\n{ADVICE}",
        found.join("\n  ")
    );
}

/// Every allowlisted path still exists, so the list cannot rot into
/// silently allowing a future file at a stale path.
#[test]
fn allowlist_paths_exist() {
    let root = workspace_root();
    for (path, _, reason) in ALLOWED {
        assert!(!reason.is_empty(), "{path} needs a reason");
        assert!(
            root.join(path).is_file(),
            "allowlisted {path} does not exist"
        );
    }
}

#[test]
fn flags_literal_with_file_and_line() {
    let content = "fn t() {\n    assert_eq!(v, \"9.9\");\n}\n";
    assert_eq!(
        violations("crates/intent-app/src/lib.rs", content, "\"9.9\""),
        vec!["crates/intent-app/src/lib.rs:2: assert_eq!(v, \"9.9\");".to_string()]
    );
}

#[test]
fn ignores_comments_and_other_versions() {
    let content = "// pinned to \"9.9\"\n/// doc \"9.9\"\nlet a = \"9.90\";\nlet b = \"19.9\";\n";
    assert!(violations("crates/x/src/lib.rs", content, "\"9.9\"").is_empty());
}

#[test]
fn registry_files_are_allowed() {
    for rel in [
        "crates/intent-services/src/harness/v3_3.rs",
        "crates/intent-services/src/harness/v1.rs",
        "crates/intent-services/src/harness/v2_10.rs",
    ] {
        assert!(is_registry_file(rel), "{rel}");
        assert!(violations(rel, "version: \"9.9\",", "\"9.9\"").is_empty());
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
fn harness_mod_allows_only_its_test_module() {
    let rel = "crates/intent-services/src/harness/mod.rs";
    let content = "const X: &str = \"9.9\";\n#[cfg(test)]\nmod tests {\n    let v = \"9.9\";\n}\n";
    assert_eq!(
        violations(rel, content, "\"9.9\""),
        vec![format!("{rel}:1: const X: &str = \"9.9\";")]
    );
}
