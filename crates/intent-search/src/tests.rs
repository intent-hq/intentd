//! Unit tests for the file-based search surface over a temp worktree.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use intent_core::Error;

use crate::cancel::CancelToken;
use crate::content::{search_in_files, SearchOpts};
use crate::paths::search_file_names;

/// A self-cleaning temp directory (no `tempfile` dep in the workspace).
struct TempTree(PathBuf);

impl Drop for TempTree {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

static SEQ: AtomicU32 = AtomicU32::new(0);

/// Lay out a small worktree: two source files, a build artifact, and a
/// `.gitignore` that excludes the artifact.
fn fixture() -> TempTree {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("intent-search-{nanos}-{n}"));
    let src = root.join("src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(root.join(".gitignore"), "ignored.rs\n").unwrap();
    std::fs::write(
        src.join("main.rs"),
        "fn main() {\n    // TODO: wire it\n}\n",
    )
    .unwrap();
    std::fs::write(src.join("lib.rs"), "// TODO: docs\npub fn run() {}\n").unwrap();
    std::fs::write(src.join("ignored.rs"), "// TODO: should be skipped\n").unwrap();
    TempTree(root)
}

#[test]
fn content_search_finds_matches_and_excludes_gitignored() {
    let tree = fixture();
    let token = CancelToken::new();
    let result = search_in_files(&tree.0, "TODO", &SearchOpts::default(), &token).unwrap();
    assert!(!result.truncated);
    assert_eq!(result.matches.len(), 2, "gitignored file must be excluded");
    assert!(result.matches.iter().all(|m| m.file != "src/ignored.rs"));
    let main_hit = result
        .matches
        .iter()
        .find(|m| m.file == "src/main.rs")
        .expect("main.rs hit");
    assert_eq!(main_hit.line, 2);
    assert_eq!(main_hit.col, 8);
    assert_eq!(main_hit.preview, "// TODO: wire it");
}

#[test]
fn content_search_max_results_sets_truncated() {
    let tree = fixture();
    let opts = SearchOpts {
        max_results: Some(1),
        ..SearchOpts::default()
    };
    let result = search_in_files(&tree.0, "TODO", &opts, &CancelToken::new()).unwrap();
    assert_eq!(result.matches.len(), 1);
    assert!(result.truncated);
}

#[test]
fn content_search_literal_query_is_not_regex() {
    let tree = fixture();
    // `(` is a literal here; default opts.regex=false → no regex parse error.
    let result = search_in_files(
        &tree.0,
        "main()",
        &SearchOpts::default(),
        &CancelToken::new(),
    )
    .unwrap();
    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].file, "src/main.rs");
}

#[test]
fn content_search_invalid_regex_is_invalid_params() {
    let tree = fixture();
    let opts = SearchOpts {
        regex: true,
        ..SearchOpts::default()
    };
    let err = search_in_files(&tree.0, "te(xt", &opts, &CancelToken::new()).unwrap_err();
    assert!(matches!(err, Error::InvalidParams(m) if m == "Invalid regex"));
}

#[test]
fn content_search_cancelled_returns_no_matches() {
    let tree = fixture();
    let token = CancelToken::new();
    token.cancel();
    let result = search_in_files(&tree.0, "TODO", &SearchOpts::default(), &token).unwrap();
    assert!(result.matches.is_empty());
}

#[test]
fn filename_search_glob_matches_nested() {
    let tree = fixture();
    let result = search_file_names(&tree.0, "*.rs", None, &CancelToken::new()).unwrap();
    let mut files = result.files.clone();
    files.sort();
    assert_eq!(files, vec!["src/lib.rs", "src/main.rs"]);
    assert!(!result.truncated);
}

#[test]
fn filename_search_substring_is_case_insensitive() {
    let tree = fixture();
    let result = search_file_names(&tree.0, "MAIN", None, &CancelToken::new()).unwrap();
    assert_eq!(result.files, vec!["src/main.rs"]);
}

#[test]
fn filename_search_limit_sets_truncated() {
    let tree = fixture();
    let result = search_file_names(&tree.0, "*.rs", Some(1), &CancelToken::new()).unwrap();
    assert_eq!(result.files.len(), 1);
    assert!(result.truncated);
}

#[test]
fn fts_match_expr_sanitizes_user_input() {
    use crate::adapters::fts_match_expr;
    // Single token: stemmed-word branch OR prefix branch.
    assert_eq!(
        fts_match_expr("needle"),
        Some(r#"("needle" OR "needle"*)"#.to_string())
    );
    // Multiple tokens: implicit-AND made explicit, last token prefixed.
    assert_eq!(
        fts_match_expr("staging environment"),
        Some(r#""staging" AND ("environment" OR "environment"*)"#.to_string())
    );
    // FTS5 operators/quotes/punctuation are separators, never syntax.
    assert_eq!(
        fts_match_expr(r#"a:b AND (c" OR NOT -d"#),
        Some(r#""a" AND "b" AND "AND" AND "c" AND "OR" AND "NOT" AND ("d" OR "d"*)"#.to_string())
    );
    // No searchable tokens → no expression (caller returns empty matches).
    assert_eq!(fts_match_expr("*(\"-:"), None);
    assert_eq!(fts_match_expr("   "), None);
    assert_eq!(fts_match_expr(""), None);
}

#[test]
fn fts_preview_windows_on_first_literal_token() {
    use crate::adapters::fts_preview;
    let text = format!("{} the needle sits here", "x".repeat(200));
    let p = fts_preview(&text, "needle");
    assert!(p.contains("needle"), "preview windows onto the match: {p}");
    // No literal occurrence (stemmed match) → head-of-text fallback.
    let p = fts_preview("deployment finished cleanly", "deploying");
    assert!(p.starts_with("deployment"));
}
