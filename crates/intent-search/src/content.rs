//! ripgrep-equivalent content search (`search.inFiles`, §5.15 / §14.2).
//!
//! A gitignore-aware [`ignore`] walk over the worktree feeds each file to a
//! [`grep`] searcher. Unlike the ported TS handler (which ran `rg --no-ignore
//! --hidden`), intentd is gitignore-aware by design (§14.2): ignored files are
//! skipped. `opts.regex=false` (the default) matches the literal query;
//! `opts.caseSensitive=false` (default) uses smart case. A malformed regex
//! surfaces as [`Error::InvalidParams`] → `-32602 "Invalid regex"`.

use std::path::Path;

use grep::matcher::Matcher;
use grep::regex::{RegexMatcher, RegexMatcherBuilder};
use grep::searcher::{Searcher, SearcherBuilder, Sink, SinkMatch};
use ignore::WalkBuilder;
use intent_core::{Error, Result};
use serde::{Deserialize, Serialize};

use crate::cancel::CancelToken;
use crate::glob::build_glob_set;
use crate::util::{first_line, normalize_rel};

/// A single content hit, carrying enough to render without a follow-up fetch.
#[derive(Debug, Clone, Serialize)]
pub struct SearchMatch {
    /// Workspace-relative path (forward-slashed).
    pub file: String,
    /// 1-based line number.
    pub line: u64,
    /// 1-based column of the match start.
    pub col: u64,
    /// The matching line, trimmed for display.
    pub preview: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
}

/// `search.inFiles` `opts` (§5.15). All fields are optional on the wire.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SearchOpts {
    pub case_sensitive: bool,
    pub regex: bool,
    pub globs: Vec<String>,
    pub max_results: Option<usize>,
}

/// The inline content-search result (`matches` + `truncated`).
#[derive(Debug, Clone, Default)]
pub struct ContentSearchResult {
    pub matches: Vec<SearchMatch>,
    pub truncated: bool,
}

/// Build the regex matcher for `query` under `opts`; literal unless `regex` is
/// set. A bad regex → `-32602 "Invalid regex"`.
fn build_matcher(query: &str, opts: &SearchOpts) -> Result<RegexMatcher> {
    let mut builder = RegexMatcherBuilder::new();
    builder.fixed_strings(!opts.regex);
    if opts.case_sensitive {
        builder.case_insensitive(false).case_smart(false);
    } else {
        builder.case_smart(true);
    }
    builder
        .build(query)
        .map_err(|_| Error::InvalidParams("Invalid regex".to_string()))
}

#[allow(clippy::similar_names)] // matcher/matches are the natural grep-domain names
/// Run a gitignore-aware content search rooted at `root`. Honors `opts.globs`
/// (path globs), `opts.maxResults` (sets `truncated` when exceeded), and the
/// `cancel` token (stops early, best-effort).
///
/// # Errors
///
/// Returns `Error::InvalidParams` if the query regex or a path glob is invalid; `Error::Internal` if the walk fails.
pub fn search_in_files(
    root: &Path,
    query: &str,
    opts: &SearchOpts,
    cancel: &CancelToken,
) -> Result<ContentSearchResult> {
    let matcher = build_matcher(query, opts)?;
    if query.is_empty() {
        return Ok(ContentSearchResult::default());
    }
    let glob_set = build_glob_set(&opts.globs)?;
    let mut searcher = SearcherBuilder::new().line_number(true).build();
    let mut matches: Vec<SearchMatch> = Vec::new();
    let mut truncated = false;

    for entry in WalkBuilder::new(root).require_git(false).build() {
        if cancel.is_cancelled() || truncated {
            break;
        }
        let Ok(entry) = entry else { continue };
        if !entry.file_type().is_some_and(|t| t.is_file()) {
            continue;
        }
        let path = entry.path();
        let rel = normalize_rel(root, path);
        if let Some(gs) = &glob_set {
            if !gs.is_match(&rel) {
                continue;
            }
        }
        let mut sink = ContentSink {
            matcher: &matcher,
            file: &rel,
            matches: &mut matches,
            cancel,
            max: opts.max_results,
            truncated: &mut truncated,
        };
        let _ = searcher.search_path(&matcher, path, &mut sink);
    }
    Ok(ContentSearchResult { matches, truncated })
}

/// Collects matches for one file, honoring the cancel token and result cap.
struct ContentSink<'a> {
    matcher: &'a RegexMatcher,
    file: &'a str,
    matches: &'a mut Vec<SearchMatch>,
    cancel: &'a CancelToken,
    max: Option<usize>,
    truncated: &'a mut bool,
}

impl Sink for ContentSink<'_> {
    type Error = std::io::Error;

    fn matched(&mut self, _searcher: &Searcher, mat: &SinkMatch<'_>) -> std::io::Result<bool> {
        if self.cancel.is_cancelled() {
            return Ok(false);
        }
        if let Some(max) = self.max {
            // A further match beyond the cap means the result set is truncated.
            if self.matches.len() >= max {
                *self.truncated = true;
                return Ok(false);
            }
        }
        let line_bytes = first_line(mat.bytes());
        let col = self.matcher.find(line_bytes).ok().flatten().map_or(1, |m| {
            String::from_utf8_lossy(&line_bytes[..m.start()])
                .chars()
                .count()
                + 1
        });
        self.matches.push(SearchMatch {
            file: self.file.to_string(),
            line: mat.line_number().unwrap_or(0),
            col: col as u64,
            preview: String::from_utf8_lossy(line_bytes).trim().to_string(),
            before: None,
            after: None,
            score: None,
        });
        Ok(true)
    }
}
