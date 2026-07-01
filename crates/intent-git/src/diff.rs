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

/// Per-file summaries for the staged changes (HEAD→index diff), mirroring
/// `git diff --cached --numstat`. An unborn `HEAD` diffs the index against the
/// empty tree (every staged file is an addition).
pub fn diff_head_to_index(repo_path: &Path) -> Result<Vec<FileDiff>> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    let head_tree = repo
        .head()
        .ok()
        .and_then(|h| h.target())
        .and_then(|oid| repo.find_commit(oid).ok())
        .and_then(|c| c.tree().ok());
    let index = repo.index().map_err(map_git_err)?;
    let diff = repo
        .diff_tree_to_index(head_tree.as_ref(), Some(&index), None)
        .map_err(map_git_err)?;
    diff_to_file_summaries(&diff)
}

/// Per-file summaries for the committed `base_ref...HEAD` range (three-dot:
/// merge-base of `base_ref` and `HEAD` → `HEAD`), mirroring
/// `git diff --numstat <base>...<branch>`. Returns an empty vec when `base_ref`
/// or `HEAD` cannot be resolved.
pub fn diff_range(repo_path: &Path, base_ref: &str) -> Result<Vec<FileDiff>> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    let Some(head_oid) = repo.head().ok().and_then(|h| h.target()) else {
        return Ok(Vec::new());
    };
    let Ok(base_obj) = repo.revparse_single(base_ref) else {
        return Ok(Vec::new());
    };
    let base_tree = match repo.merge_base(base_obj.id(), head_oid) {
        Ok(mb) => repo.find_commit(mb).ok().and_then(|c| c.tree().ok()),
        Err(_) => None,
    };
    let head_tree = match repo.find_commit(head_oid).ok().and_then(|c| c.tree().ok()) {
        Some(t) => t,
        None => return Ok(Vec::new()),
    };
    let diff = repo
        .diff_tree_to_tree(base_tree.as_ref(), Some(&head_tree), None)
        .map_err(map_git_err)?;
    diff_to_file_summaries(&diff)
}

/// Per-file summaries for the commit `<commit_hash>^..<commit_hash>` (the
/// commit's own changes against its first parent). A root commit (no parent)
/// diffs against the empty tree, so every file appears as additions. An
/// unresolvable `commit_hash` returns [`Error::NotFound`].
pub fn diff_commit(repo_path: &Path, commit_hash: &str) -> Result<Vec<FileDiff>> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    let obj = repo
        .revparse_single(commit_hash)
        .map_err(|_| Error::NotFound(format!("commit not found: {commit_hash}")))?;
    let commit = obj
        .peel_to_commit()
        .map_err(|_| Error::NotFound(format!("commit not found: {commit_hash}")))?;
    let tree = commit.tree().map_err(map_git_err)?;
    let parent_tree = match commit.parent(0) {
        Ok(parent) => Some(parent.tree().map_err(map_git_err)?),
        Err(_) => None,
    };
    let diff = repo
        .diff_tree_to_tree(parent_tree.as_ref(), Some(&tree), None)
        .map_err(map_git_err)?;
    diff_to_file_summaries(&diff)
}

/// Aggregate `git diff HEAD` rollup for a workspace card (ports the TS
/// `computeWorkspaceDiffSummary`): returns `(total_files, total_additions,
/// total_deletions)`.
///
/// `total_files` counts tracked changes vs `HEAD` (staged + unstaged) **plus**
/// untracked files (matching `git diff --name-only HEAD` ∪ `ls-files --others`);
/// `total_additions`/`total_deletions` sum line stats over the **tracked**
/// changes only (untracked content excluded, matching `git diff --numstat HEAD`).
/// An unborn `HEAD` (no commit) has no diff base — like the TS `git diff HEAD`
/// failing — so `(0, 0, 0)` is returned and callers omit the summary.
pub fn head_diff_rollup(repo_path: &Path) -> Result<(usize, usize, usize)> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    let Some(head_tree) = repo
        .head()
        .ok()
        .and_then(|h| h.target())
        .and_then(|oid| repo.find_commit(oid).ok())
        .and_then(|c| c.tree().ok())
    else {
        return Ok((0, 0, 0));
    };

    // Tracked changes vs HEAD (staged + unstaged); untracked excluded — line stats.
    let tracked = repo
        .diff_tree_to_workdir_with_index(Some(&head_tree), None)
        .map_err(map_git_err)?;
    let mut total_additions = 0usize;
    let mut total_deletions = 0usize;
    for i in 0..tracked.deltas().len() {
        if let Some(patch) = Patch::from_diff(&tracked, i).map_err(map_git_err)? {
            let (_ctx, adds, dels) = patch.line_stats().map_err(map_git_err)?;
            total_additions += adds;
            total_deletions += dels;
        }
    }

    // Same diff including untracked files → unique changed-file count.
    let mut opts = DiffOptions::new();
    opts.include_untracked(true).recurse_untracked_dirs(true);
    let with_untracked = repo
        .diff_tree_to_workdir_with_index(Some(&head_tree), Some(&mut opts))
        .map_err(map_git_err)?;
    let total_files = with_untracked.deltas().len();

    Ok((total_files, total_additions, total_deletions))
}

/// Build per-file [`FileDiff`] summaries (path + line stats + blob SHAs) from a
/// computed [`git2::Diff`].
fn diff_to_file_summaries(diff: &git2::Diff) -> Result<Vec<FileDiff>> {
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
        let (additions, deletions) = match Patch::from_diff(diff, i).map_err(map_git_err)? {
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
/// blob is treated as empty (added/deleted file). Both blobs must exist in the
/// object DB (committed/staged content); for an unstaged workdir change whose
/// post-image is not yet a blob, use [`hunks_index_to_workdir`] instead.
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
    patch_to_hunks(&patch)
}

/// Compute hunks for a single file directly from the index→workdir diff, reading
/// the workdir content rather than looking up a post-image blob in the object DB.
/// This is the variant the agent-edit pipeline uses: an unstaged change's new
/// content is not yet a blob, so [`hunks_between`] cannot hydrate it. Returns an
/// empty vec when `rel_path` has no pending change (or is binary).
pub fn hunks_index_to_workdir(repo_path: &Path, rel_path: &str) -> Result<Vec<DiffHunk>> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    let mut opts = DiffOptions::new();
    opts.include_untracked(true)
        .recurse_untracked_dirs(true)
        .show_untracked_content(true);
    let diff = repo
        .diff_index_to_workdir(None, Some(&mut opts))
        .map_err(map_git_err)?;
    for i in 0..diff.deltas().len() {
        let delta = diff.get_delta(i).expect("delta index in range");
        let path = delta
            .new_file()
            .path()
            .or_else(|| delta.old_file().path())
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();
        if path != rel_path {
            continue;
        }
        return match Patch::from_diff(&diff, i).map_err(map_git_err)? {
            Some(patch) => patch_to_hunks(&patch),
            None => Ok(Vec::new()),
        };
    }
    Ok(Vec::new())
}

/// Extract the [`DiffHunk`] list (with per-line origin/numbers) from a [`Patch`].
fn patch_to_hunks(patch: &Patch) -> Result<Vec<DiffHunk>> {
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
    fn hunks_index_to_workdir_reads_unstaged_workdir_content() {
        // An unstaged workdir change's post-image is not yet a blob, so hunks
        // must be read from the diff itself (not hydrated from a blob SHA).
        let dir = init_repo("diff-workdir-hunks");
        commit_file(dir.path(), "a.txt", "line1\nline2\nline3\n");
        write_file(dir.path(), "a.txt", "line1\nCHANGED\nline3\nline4\n");
        let hunks = hunks_index_to_workdir(dir.path(), "a.txt").unwrap();
        assert_eq!(hunks.len(), 1);
        let hunk = &hunks[0];
        assert!(hunk
            .lines
            .iter()
            .any(|l| l.kind == DiffLineKind::Addition && l.content.contains("CHANGED")));
        assert!(hunk
            .lines
            .iter()
            .any(|l| l.kind == DiffLineKind::Deletion && l.content.contains("line2")));
    }

    #[test]
    fn hunks_index_to_workdir_missing_path_is_empty() {
        let dir = init_repo("diff-workdir-none");
        commit_file(dir.path(), "a.txt", "seed\n");
        assert!(hunks_index_to_workdir(dir.path(), "a.txt")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn head_diff_rollup_counts_tracked_changes_and_untracked_files() {
        let dir = init_repo("diff-rollup");
        commit_file(dir.path(), "a.txt", "line1\nline2\nline3\n");
        // Tracked modification: 1 replaced line (1 add + 1 del) + 1 appended add.
        write_file(dir.path(), "a.txt", "line1\nCHANGED\nline3\nline4\n");
        // Untracked file: counts toward total_files, not toward line totals.
        write_file(dir.path(), "new.txt", "hello\nworld\n");
        let (total_files, total_additions, total_deletions) = head_diff_rollup(dir.path()).unwrap();
        assert_eq!(total_files, 2);
        assert_eq!(total_additions, 2);
        assert_eq!(total_deletions, 1);
    }

    #[test]
    fn head_diff_rollup_clean_tree_is_zero() {
        let dir = init_repo("diff-rollup-clean");
        commit_file(dir.path(), "a.txt", "seed\n");
        assert_eq!(head_diff_rollup(dir.path()).unwrap(), (0, 0, 0));
    }

    #[test]
    fn diff_commit_returns_per_file_summaries_for_a_commit() {
        let dir = init_repo("diff-commit");
        commit_file(dir.path(), "a.txt", "line1\nline2\nline3\n");
        commit_file(dir.path(), "a.txt", "line1\nCHANGED\nline3\nline4\n");
        // HEAD is the second commit; its parent has the original three lines.
        let repo = git2::Repository::open(dir.path()).unwrap();
        let head_hash = repo.head().unwrap().target().unwrap().to_string();
        let files = diff_commit(dir.path(), &head_hash).unwrap();
        assert_eq!(files.len(), 1);
        let f = &files[0];
        assert_eq!(f.path, "a.txt");
        assert_eq!(f.additions, 2);
        assert_eq!(f.deletions, 1);
        assert!(f.old_blob.is_some());
        assert!(f.new_blob.is_some());

        // Hunks for the same file are recoverable from the recorded blob SHAs.
        let hunks =
            hunks_between(dir.path(), f.old_blob.as_deref(), f.new_blob.as_deref()).unwrap();
        assert_eq!(hunks.len(), 1);
        assert!(hunks[0]
            .lines
            .iter()
            .any(|l| l.kind == DiffLineKind::Addition && l.content.contains("CHANGED")));
    }

    #[test]
    fn diff_commit_unknown_hash_is_not_found() {
        let dir = init_repo("diff-commit-missing");
        commit_file(dir.path(), "seed.txt", "seed\n");
        let err = diff_commit(dir.path(), "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef").unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
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
