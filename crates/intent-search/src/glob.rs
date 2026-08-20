//! Shared `globset` helpers for content `opts.globs` and filename patterns.

use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use intent_core::{Error, Result};

/// Build a [`GlobSet`] from `opts.globs`, or `None` when no globs are given.
/// `*` crosses path separators (`literal_separator(false)`), so `*.rs` matches
/// nested files. An unparsable glob → `-32602 "Invalid glob pattern"`.
pub(crate) fn build_glob_set(globs: &[String]) -> Result<Option<GlobSet>> {
    if globs.is_empty() {
        return Ok(None);
    }
    let mut builder = GlobSetBuilder::new();
    for pattern in globs {
        let glob = GlobBuilder::new(pattern)
            .literal_separator(false)
            .build()
            .map_err(|_| Error::InvalidParams("Invalid glob pattern".to_string()))?;
        builder.add(glob);
    }
    builder
        .build()
        .map(Some)
        .map_err(|_| Error::InvalidParams("Invalid glob pattern".to_string()))
}

/// Whether `pattern` contains glob metacharacters (so it is matched as a glob
/// rather than a case-insensitive substring).
pub(crate) fn is_glob(pattern: &str) -> bool {
    pattern.contains(['*', '?', '[', ']', '{', '}'])
}

/// Build a single case-insensitive [`GlobSet`] from one filename pattern.
pub(crate) fn build_name_glob(pattern: &str) -> Result<GlobSet> {
    let glob = GlobBuilder::new(pattern)
        .literal_separator(false)
        .case_insensitive(true)
        .build()
        .map_err(|_| Error::InvalidParams("Invalid glob pattern".to_string()))?;
    let mut builder = GlobSetBuilder::new();
    builder.add(glob);
    builder
        .build()
        .map_err(|_| Error::InvalidParams("Invalid glob pattern".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn build_glob_set_returns_none_when_empty() {
        assert!(build_glob_set(&[]).unwrap().is_none());
    }

    #[test]
    fn build_glob_set_matches_nested_with_star() {
        let set = build_glob_set(&["*.rs".to_string()]).unwrap().unwrap();
        // `literal_separator(false)` lets `*` cross path separators.
        assert!(set.is_match(Path::new("src/main.rs")));
        assert!(set.is_match(Path::new("a/b/c/lib.rs")));
        assert!(!set.is_match(Path::new("src/main.txt")));
    }

    #[test]
    fn build_glob_set_combines_multiple_patterns() {
        let set = build_glob_set(&["*.rs".to_string(), "*.toml".to_string()])
            .unwrap()
            .unwrap();
        assert!(set.is_match(Path::new("Cargo.toml")));
        assert!(set.is_match(Path::new("src/lib.rs")));
        assert!(!set.is_match(Path::new("README.md")));
    }

    #[test]
    fn build_glob_set_rejects_invalid_pattern() {
        let err = build_glob_set(&["[abc".to_string()]).unwrap_err();
        assert!(matches!(err, Error::InvalidParams(m) if m == "Invalid glob pattern"));
    }

    #[test]
    fn build_glob_set_is_case_sensitive_by_default() {
        let set = build_glob_set(&["*.rs".to_string()]).unwrap().unwrap();
        // The content-globs path does not enable case-insensitive matching.
        assert!(!set.is_match(Path::new("Main.RS")));
    }

    #[test]
    fn is_glob_detects_metacharacters() {
        assert!(is_glob("*.rs"));
        assert!(is_glob("foo?.txt"));
        assert!(is_glob("file[0-9].log"));
        assert!(is_glob("a{b,c}.rs"));
        assert!(is_glob("trailing]"));
        // Bare names and substrings have no metacharacters.
        assert!(!is_glob(""));
        assert!(!is_glob("plain.txt"));
        assert!(!is_glob("MAIN"));
        assert!(!is_glob("path/to/file.rs"));
    }

    #[test]
    fn build_name_glob_is_case_insensitive() {
        let set = build_name_glob("*.RS").unwrap();
        assert!(set.is_match(Path::new("main.rs")));
        assert!(set.is_match(Path::new("MAIN.RS")));
        assert!(set.is_match(Path::new("nested/dir/lib.rs")));
        assert!(!set.is_match(Path::new("main.txt")));
    }

    #[test]
    fn build_name_glob_rejects_invalid_pattern() {
        let err = build_name_glob("[abc").unwrap_err();
        assert!(matches!(err, Error::InvalidParams(m) if m == "Invalid glob pattern"));
    }

    #[test]
    fn build_name_glob_literal_pattern_matches_exact_name() {
        let set = build_name_glob("Cargo.toml").unwrap();
        assert!(set.is_match(Path::new("Cargo.toml")));
        // No metacharacters → only the exact filename matches.
        assert!(!set.is_match(Path::new("Cargo.lock")));
    }
}
