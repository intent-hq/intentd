//! Shared `globset` helpers for content `opts.globs` and filename patterns.

use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use intent_core::{Error, Result};

/// Build a [`GlobSet`] from `opts.globs`, or `None` when no globs are given.
/// `*` crosses path separators (`literal_separator(false)`), so `*.rs` matches
/// nested files. An unparsable glob → `-32602 "Invalid glob pattern"`.
pub fn build_glob_set(globs: &[String]) -> Result<Option<GlobSet>> {
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
pub fn is_glob(pattern: &str) -> bool {
    pattern.contains(['*', '?', '[', ']', '{', '}'])
}

/// Build a single case-insensitive [`GlobSet`] from one filename pattern.
pub fn build_name_glob(pattern: &str) -> Result<GlobSet> {
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
