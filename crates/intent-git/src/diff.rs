//! Internal diff helper (no wire method yet; Cycle C consumes it).
//!
//! [`diff_index_to_workdir`] returns cheap per-file summaries (additions,
//! deletions, and the old/new blob SHAs) without materializing hunks. Hunks are
//! computed lazily from the recorded blob SHAs via [`hunks_between`], so a caller
//! only pays for the files it actually expands.

use std::path::Path;

use git2::{DiffOptions, Oid, Patch, Repository};
use intent_core::{Error, Result};

use crate::map_git_err;

/// The kind of a diff line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffLineKind {
    Context,
    Addition,
    Deletion,
}

/// One line within a [`DiffHunk`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffLine {
    pub kind: DiffLineKind,
    pub content: String,
    pub old_lineno: Option<u32>,
    pub new_lineno: Option<u32>,
}

/// A contiguous change region (`@@ -old +new @@`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffHunk {
    pub old_start: u32,
    pub old_lines: u32,
    pub new_start: u32,
    pub new_lines: u32,
    pub lines: Vec<DiffLine>,
}

/// Per-file summary: counts plus the blob SHAs needed for lazy hunk hydration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileDiff {
    pub path: String,
    pub additions: usize,
    pub deletions: usize,
    pub is_binary: bool,
    /// `None` for an added file (no pre-image blob).
    pub old_blob: Option<String>,
    /// `None` for a deleted file (no post-image blob).
    pub new_blob: Option<String>,
}

/// Summaries for the index→workdir diff (staged + unstaged + untracked), without
/// hunks. Use [`hunks_between`] with each file's blob SHAs to expand on demand.
pub fn diff_index_to_workdir(repo_path: &Path) -> Result<Vec<FileDiff>> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    let mut opts = DiffOptions::new();
    opts.include_untracked(true)
        .recurse_untracked_dirs(true)
        .show_untracked_content(true);
    let diff = repo
        .diff_index_to_workdir(None, Some(&mut opts))
        .map_err(map_git_err)?;
    let mut out = Vec::new();
    for i in 0..diff.deltas().len() {
        let delta = diff.get_delta(i).expect("delta index in range");
        let path = delta
            .new_file()
            .path()
            .or_else(|| delta.old_file().path())
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();
        let is_binary = delta.flags().is_binary();
        let (additions, deletions) = match Patch::from_diff(&diff, i).map_err(map_git_err)? {
            Some(patch) => {
                let (_ctx, adds, dels) = patch.line_stats().map_err(map_git_err)?;
                (adds, dels)
            }
            None => (0, 0),
        };
        out.push(FileDiff {
            path,
            additions,
            deletions,
            is_binary,
            old_blob: oid_to_opt(delta.old_file().id()),
            new_blob: oid_to_opt(delta.new_file().id()),
        });
    }
    Ok(out)
}

/// Lazily compute hunks for a single file from its old/new blob SHAs. A `None`
/// blob is treated as empty (added/deleted file).
pub fn hunks_between(
    repo_path: &Path,
    old_blob: Option<&str>,
    new_blob: Option<&str>,
) -> Result<Vec<DiffHunk>> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    let old_bytes = load_blob_bytes(&repo, old_blob)?;
    let new_bytes = load_blob_bytes(&repo, new_blob)?;
    let mut opts = DiffOptions::new();
    let patch = Patch::from_buffers(&old_bytes, None, &new_bytes, None, Some(&mut opts))
        .map_err(map_git_err)?;
    let mut hunks = Vec::new();
    for h in 0..patch.num_hunks() {
        let (hunk, _lines) = patch.hunk(h).map_err(map_git_err)?;
        let mut lines = Vec::new();
        for l in 0..patch.num_lines_in_hunk(h).map_err(map_git_err)? {
            let line = patch.line_in_hunk(h, l).map_err(map_git_err)?;
            let kind = match line.origin() {
                '+' => DiffLineKind::Addition,
                '-' => DiffLineKind::Deletion,
                _ => DiffLineKind::Context,
            };
            lines.push(DiffLine {
                kind,
                content: String::from_utf8_lossy(line.content()).to_string(),
                old_lineno: line.old_lineno(),
                new_lineno: line.new_lineno(),
            });
        }
        hunks.push(DiffHunk {
            old_start: hunk.old_start(),
            old_lines: hunk.old_lines(),
            new_start: hunk.new_start(),
            new_lines: hunk.new_lines(),
            lines,
        });
    }
    Ok(hunks)
}

fn oid_to_opt(oid: Oid) -> Option<String> {
    if oid.is_zero() {
        None
    } else {
        Some(oid.to_string())
    }
}

fn load_blob_bytes(repo: &Repository, sha: Option<&str>) -> Result<Vec<u8>> {
    match sha {
        Some(s) => {
            let oid =
                Oid::from_str(s).map_err(|e| Error::Internal(format!("invalid blob sha: {e}")))?;
            let blob = repo.find_blob(oid).map_err(map_git_err)?;
            Ok(blob.content().to_vec())
        }
        None => Ok(Vec::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{commit_file, init_repo, write_file};

    #[test]
    fn summary_counts_additions_and_deletions() {
        let dir = init_repo("diff-counts");
        commit_file(dir.path(), "a.txt", "line1\nline2\nline3\n");
        write_file(dir.path(), "a.txt", "line1\nCHANGED\nline3\nline4\n");
        let files = diff_index_to_workdir(dir.path()).unwrap();
        let f = files.iter().find(|f| f.path == "a.txt").unwrap();
        // One line replaced (1 add + 1 del) and one appended (1 add).
        assert_eq!(f.additions, 2);
        assert_eq!(f.deletions, 1);
        assert!(!f.is_binary);
        // Summaries are lazy: blob SHAs are recorded for on-demand hunks.
        assert!(f.old_blob.is_some());
    }

    #[test]
    fn untracked_file_has_no_old_blob() {
        let dir = init_repo("diff-untracked");
        commit_file(dir.path(), "seed.txt", "seed\n");
        write_file(dir.path(), "new.txt", "hello\nworld\n");
        let files = diff_index_to_workdir(dir.path()).unwrap();
        let f = files.iter().find(|f| f.path == "new.txt").unwrap();
        assert_eq!(f.additions, 2);
        assert_eq!(f.deletions, 0);
        assert!(f.old_blob.is_none());
    }

    #[test]
    fn hunks_between_blobs_is_lazy() {
        // Both pre/post images live in the object DB; hunks are reconstructed
        // from the recorded blob SHAs (the lazy hydration path).
        let dir = init_repo("diff-hunks");
        commit_file(dir.path(), "seed.txt", "seed\n");
        let old_sha = crate::testutil::write_blob(dir.path(), b"one\ntwo\nthree\n");
        let new_sha = crate::testutil::write_blob(dir.path(), b"one\nTWO\nthree\n");
        let hunks = hunks_between(dir.path(), Some(&old_sha), Some(&new_sha)).unwrap();
        assert_eq!(hunks.len(), 1);
        let hunk = &hunks[0];
        assert!(hunk
            .lines
            .iter()
            .any(|l| l.kind == DiffLineKind::Addition && l.content.contains("TWO")));
        assert!(hunk
            .lines
            .iter()
            .any(|l| l.kind == DiffLineKind::Deletion && l.content.contains("two")));
    }

    #[test]
    fn hunks_between_added_file_has_only_additions() {
        let dir = init_repo("diff-hunks-add");
        commit_file(dir.path(), "seed.txt", "seed\n");
        let new_sha = crate::testutil::write_blob(dir.path(), b"alpha\nbeta\n");
        let hunks = hunks_between(dir.path(), None, Some(&new_sha)).unwrap();
        assert_eq!(hunks.len(), 1);
        assert!(hunks[0]
            .lines
            .iter()
            .all(|l| l.kind == DiffLineKind::Addition));
    }
}
