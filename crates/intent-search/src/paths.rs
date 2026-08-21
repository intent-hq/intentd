//! Path / glob filename search (`search.fileNames`, §5.15 / §14.2).
//!
//! A gitignore-aware [`ignore`] walk over the worktree, matching each
//! workspace-relative path against `pattern`. A pattern with glob
//! metacharacters is matched as a glob (`*` crosses separators); otherwise it
//! is a case-insensitive substring match on the path (ports the `list-files`
//! filter). `limit` caps the result and sets `truncated` when exceeded.

use std::path::Path;

use globset::GlobSet;
use ignore::WalkBuilder;
use intent_core::Result;

use crate::cancel::CancelToken;
use crate::glob::{build_name_glob, is_glob};
use crate::util::normalize_rel;

/// The inline filename-search result (`files` + `truncated`).
#[derive(Debug, Clone, Default)]
pub struct FileNameResult {
    pub files: Vec<String>,
    pub truncated: bool,
}

/// How a filename pattern is matched against each relative path.
enum NameMatcher {
    Glob(GlobSet),
    Substr(String),
}

impl NameMatcher {
    fn matches(&self, rel: &str) -> bool {
        match self {
            NameMatcher::Glob(set) => set.is_match(rel),
            NameMatcher::Substr(needle) => rel.to_lowercase().contains(needle),
        }
    }
}

/// Run a gitignore-aware filename search rooted at `root`, honoring `limit`
/// (sets `truncated` when exceeded) and the `cancel` token (stops early).
pub fn search_file_names(
    root: &Path,
    pattern: &str,
    limit: Option<usize>,
    cancel: &CancelToken,
) -> Result<FileNameResult> {
    let matcher = if is_glob(pattern) {
        NameMatcher::Glob(build_name_glob(pattern)?)
    } else {
        NameMatcher::Substr(pattern.to_lowercase())
    };
    let mut files = Vec::new();
    let mut truncated = false;

    for entry in WalkBuilder::new(root).require_git(false).build() {
        if cancel.is_cancelled() {
            break;
        }
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        if !entry.file_type().is_some_and(|t| t.is_file()) {
            continue;
        }
        let rel = normalize_rel(root, entry.path());
        if !matcher.matches(&rel) {
            continue;
        }
        if let Some(max) = limit {
            if files.len() >= max {
                truncated = true;
                break;
            }
        }
        files.push(rel);
    }
    Ok(FileNameResult { files, truncated })
}
