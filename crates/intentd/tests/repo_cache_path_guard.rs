//! Source-scan guard: test code must not hand-roll repo-cache paths.
//!
//! The repo-cache slot layout (`<workspaces_root>/.repo-cache/<owner>/<repo>`,
//! case-folded via `RepoRef::identity_parts`) is owned by
//! `intent_git::repo_cache::{REPO_CACHE_DIR_NAME, cache_root_for, cache_path_for}`.
//! A test that spells the quoted `.repo-cache` directory name itself re-derives
//! that layout and silently desynchronizes from production the next time the
//! layout changes
//! (intent-hq/intentd#1815 folded the slot; #1825 was ejected from the merge
//! queue because hand-rolled test joins still used the unfolded form). This test
//! fails naming the offending `file:line`s so that class of drift cannot return.
//!
//! A line that legitimately needs the literal (e.g. asserting the on-disk
//! directory name itself) may opt out with a trailing
//! `// repo-cache-path: allow — <reason>`; the marker must be the line's
//! trailing comment and the reason is required.

use std::fs;
use std::path::{Path, PathBuf};

use intent_git::repo_cache::REPO_CACHE_DIR_NAME;

const ALLOW_MARKER: &str = "// repo-cache-path: allow";

/// The quoted directory-name literal, e.g. `"` + `.repo-cache` + `"`, built
/// from the constant so this file never spells it out itself.
fn needle() -> String {
    format!("\"{REPO_CACHE_DIR_NAME}\"")
}

/// The file that *defines* the helpers (and the constant), and this guard.
const EXEMPT_FILES: &[&str] = &[
    "crates/intent-git/src/repo_cache.rs",
    "crates/intentd/tests/repo_cache_path_guard.rs",
];

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("canonicalize workspace root")
}

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// `crates/*/src/**/{tests.rs,*_tests.rs}` and anything under a
/// `crates/*/src/**/tests/` directory.
fn is_src_test_file(rel: &Path) -> bool {
    let name = rel.file_name().and_then(|n| n.to_str()).unwrap_or("");
    name == "tests.rs"
        || name.ends_with("_tests.rs")
        || rel.components().any(|c| c.as_os_str() == "tests")
}

fn scanned_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let Ok(crates) = fs::read_dir(root.join("crates")) else {
        panic!("no crates/ under {}", root.display());
    };
    for krate in crates.flatten() {
        let krate = krate.path();
        collect_rs_files(&krate.join("tests"), &mut files);
        let mut src = Vec::new();
        collect_rs_files(&krate.join("src"), &mut src);
        files.extend(src.into_iter().filter(|p| {
            let rel = p.strip_prefix(&krate).unwrap_or(p);
            is_src_test_file(rel.strip_prefix("src").unwrap_or(rel))
        }));
    }
    files.retain(|p| {
        let rel = p.strip_prefix(root).unwrap_or(p).to_string_lossy();
        !EXEMPT_FILES.contains(&rel.as_ref())
    });
    files.sort();
    files
}

fn strip_ws(s: &str) -> String {
    s.chars().filter(|c| !c.is_whitespace()).collect()
}

/// Splits a line into its code and trailing `//` comment. A `//` inside a
/// string literal (e.g. `"https://x"`) does not start a comment.
fn split_comment(line: &str) -> (&str, Option<&str>) {
    let bytes = line.as_bytes();
    let mut in_str = false;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' if in_str => i += 1,
            b'"' => in_str = !in_str,
            b'/' if !in_str && bytes.get(i + 1) == Some(&b'/') => {
                return (&line[..i], Some(&line[i..]));
            }
            _ => {}
        }
        i += 1;
    }
    (line, None)
}

/// The opt-out counts only as the line's trailing comment, starting with the
/// marker and followed by a non-empty reason.
fn has_allow_marker(comment: Option<&str>) -> bool {
    let Some(rest) = comment
        .map(str::trim_start)
        .and_then(|c| c.strip_prefix(ALLOW_MARKER))
    else {
        return false;
    };
    let reason =
        rest.trim_start_matches(|c: char| c.is_whitespace() || matches!(c, '—' | '-' | ':'));
    !reason.trim().is_empty()
}

fn is_hand_rolled_repo_cache_path(line: &str, needle: &str) -> bool {
    let (code, comment) = split_comment(line);
    !has_allow_marker(comment) && strip_ws(code).contains(needle)
}

fn scan(root: &Path) -> Vec<String> {
    let needle = needle();
    let mut offenders = Vec::new();
    for file in scanned_files(root) {
        let src = fs::read_to_string(&file).expect("read test source");
        for (i, line) in src.lines().enumerate() {
            if is_hand_rolled_repo_cache_path(line, &needle) {
                let rel = file.strip_prefix(root).unwrap_or(&file);
                offenders.push(format!("{}:{}", rel.display(), i + 1));
            }
        }
    }
    offenders
}

#[test]
fn test_code_uses_repo_cache_path_helpers() {
    let root = workspace_root();
    let files = scanned_files(&root);
    assert!(
        files.len() > 100,
        "guard scanned only {} files — file discovery is broken",
        files.len()
    );
    let offenders = scan(&root);
    assert!(
        offenders.is_empty(),
        "\nhand-rolled repo-cache paths in test code ({} site{}):\n  {}\n\n\
         Derive repo-cache paths through `intent_git::repo_cache::cache_root_for(workspaces_root)` \
         / `intent_git::repo_cache::cache_path_for(cache_root, owner, repo)` \
         (crates/intent-git/src/repo_cache.rs) so tests cannot desynchronize from the \
         production slot layout. If the literal directory name itself is what is under \
         test, append `{ALLOW_MARKER} — <reason>` to the line.\n",
        offenders.len(),
        if offenders.len() == 1 { "" } else { "s" },
        offenders.join("\n  "),
    );
}

#[test]
fn classifier_flags_hand_rolled_joins() {
    let n = needle();
    assert!(is_hand_rolled_repo_cache_path(
        &format!("let c = root.join({n});"),
        &n
    ));
    assert!(is_hand_rolled_repo_cache_path(
        &format!("let c = root.join( {n} );"),
        &n
    ));
}

#[test]
fn classifier_ignores_the_literal_in_comments() {
    let n = needle();
    assert!(!is_hand_rolled_repo_cache_path(
        &format!("let c = cache_root_for(root); // never join({n})"),
        &n
    ));
    assert!(!is_hand_rolled_repo_cache_path(
        &format!("// joins {n} by hand"),
        &n
    ));
}

#[test]
fn classifier_sees_past_slashes_inside_string_literals() {
    let n = needle();
    assert!(is_hand_rolled_repo_cache_path(
        &format!(r#"let url = "https://x"; let c = root.join({n});"#),
        &n
    ));
    assert!(is_hand_rolled_repo_cache_path(
        &format!(r#"let url = "file://\"//x"; let c = root.join({n});"#),
        &n
    ));
}

#[test]
fn allow_marker_only_counts_as_trailing_comment_with_reason() {
    let n = needle();
    assert!(!is_hand_rolled_repo_cache_path(
        &format!("let c = root.join({n}); {ALLOW_MARKER} — asserts the on-disk name"),
        &n
    ));
    assert!(!is_hand_rolled_repo_cache_path(
        &format!("let c = root.join({n}); {ALLOW_MARKER}: asserts the on-disk name"),
        &n
    ));
    assert!(is_hand_rolled_repo_cache_path(
        &format!("let c = root.join({n}); {ALLOW_MARKER}"),
        &n
    ));
    assert!(is_hand_rolled_repo_cache_path(
        &format!("let c = root.join({n}); {ALLOW_MARKER} — "),
        &n
    ));
    assert!(is_hand_rolled_repo_cache_path(
        &format!(r#"let s = "{ALLOW_MARKER} — x"; let c = root.join({n});"#),
        &n
    ));
    assert!(is_hand_rolled_repo_cache_path(
        &format!("let c = root.join({n}); // see {ALLOW_MARKER} — x"),
        &n
    ));
}
