//! Internal diff helper (no wire method yet; Cycle C consumes it).
//!
//! [`diff_index_to_workdir`] returns cheap per-file summaries (additions,
//! deletions, and the old/new blob SHAs) without materializing hunks. Hunks are
//! computed lazily from the recorded blob SHAs via [`hunks_between`], so a caller
//! only pays for the files it actually expands.
//!
//! [`diff_index_to_workdir_with_hunks`] is the single-pass variant: it returns
//! per-file summaries **and** hunks from one diff traversal, with optional
//! pathspec narrowing so libgit2 prunes the walk instead of scanning the whole
//! tree.

use std::path::Path;
use std::time::Instant;

use git2::{Delta, DiffOptions, FileMode, Oid, Patch, Repository};
use intent_core::{Error, Result};

use crate::{map_git_err, SLOW_GIT_WARN_THRESHOLD};

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
    /// True when the **old** side of the delta is a `160000` submodule
    /// (gitlink) entry (monorepo#1739). On a gitlink side `old_blob`/`new_blob`
    /// carry the submodule pin **commit** SHA — an object in the submodule's
    /// odb, not a blob in this repository — so it must never be fed to
    /// [`hunks_between`]; use [`gitlink_hunks`] with the pin-side SHAs
    /// ([`FileDiff::gitlink_old_sha`] / [`FileDiff::gitlink_new_sha`]) instead.
    /// Tracked per side so a gitlink↔regular-file type change never leaks the
    /// regular side's blob OID as a pin SHA (libgit2 splits such a type change
    /// into a delete + add delta pair, but each side is still checked).
    pub old_is_gitlink: bool,
    /// True when the **new** side of the delta is a `160000` gitlink entry.
    /// See [`FileDiff::old_is_gitlink`].
    pub new_is_gitlink: bool,
}

impl FileDiff {
    /// Whether either side of the delta is a `160000` gitlink entry.
    pub fn is_gitlink(&self) -> bool {
        self.old_is_gitlink || self.new_is_gitlink
    }

    /// The old-side submodule pin SHA — `old_blob` only when that side is a
    /// gitlink (never a regular file's blob OID).
    pub fn gitlink_old_sha(&self) -> Option<&str> {
        if self.old_is_gitlink {
            self.old_blob.as_deref()
        } else {
            None
        }
    }

    /// The new-side submodule pin SHA — `new_blob` only when that side is a
    /// gitlink (never a regular file's blob OID).
    pub fn gitlink_new_sha(&self) -> Option<&str> {
        if self.new_is_gitlink {
            self.new_blob.as_deref()
        } else {
            None
        }
    }
}

/// One file's summary plus its hunks, produced together by
/// [`diff_index_to_workdir_with_hunks`] from a single diff traversal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileDiffWithHunks {
    pub file: FileDiff,
    /// Empty for binary files (and any delta without a text patch).
    pub hunks: Vec<DiffHunk>,
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
            old_is_gitlink: delta.old_file().mode() == FileMode::Commit,
            new_is_gitlink: delta.new_file().mode() == FileMode::Commit,
        });
    }
    Ok(out)
}

/// Per-file summaries **and** hunks for the index→workdir diff (staged +
/// unstaged + untracked), from a **single** diff traversal — unlike the
/// two-pass combination of [`diff_index_to_workdir`] +
/// [`hunks_index_to_workdir`], which walks the tree once per call.
///
/// `pathspecs` narrows the diff: when `Some`, each entry is added as a
/// **literal** path (fnmatch pattern matching is disabled, so a path whose
/// name contains `*`/`?`/`[...]` still matches itself) and the walk is pruned
/// to matching paths (a path with no pending change yields no entry). `None`
/// diffs the full tree. An empty slice behaves like `None` (libgit2 treats no
/// pathspecs as match-all).
pub fn diff_index_to_workdir_with_hunks(
    repo_path: &Path,
    pathspecs: Option<&[&str]>,
) -> Result<Vec<FileDiffWithHunks>> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    let mut opts = DiffOptions::new();
    opts.include_untracked(true)
        .recurse_untracked_dirs(true)
        .show_untracked_content(true);
    if let Some(specs) = pathspecs {
        opts.disable_pathspec_match(true);
        for spec in specs {
            opts.pathspec(spec);
        }
    }
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
        let (additions, deletions, hunks) = match Patch::from_diff(&diff, i).map_err(map_git_err)? {
            Some(patch) => {
                let (_ctx, adds, dels) = patch.line_stats().map_err(map_git_err)?;
                (adds, dels, patch_to_hunks(&patch)?)
            }
            None => (0, 0, Vec::new()),
        };
        out.push(FileDiffWithHunks {
            file: FileDiff {
                path,
                additions,
                deletions,
                is_binary,
                old_blob: oid_to_opt(delta.old_file().id()),
                new_blob: oid_to_opt(delta.new_file().id()),
                old_is_gitlink: delta.old_file().mode() == FileMode::Commit,
                new_is_gitlink: delta.new_file().mode() == FileMode::Commit,
            },
            hunks,
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

/// Per-file summaries for `HEAD` → workdir over **tracked** paths only
/// (staged + unstaged tracked changes; untracked files are **excluded**),
/// mirroring `git diff HEAD --numstat`. Returns an empty vec when `HEAD` is
/// unborn (nothing to diff against).
pub fn diff_head_to_workdir_tracked(repo_path: &Path) -> Result<Vec<FileDiff>> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    let Some(head_tree) = repo
        .head()
        .ok()
        .and_then(|h| h.target())
        .and_then(|oid| repo.find_commit(oid).ok())
        .and_then(|c| c.tree().ok())
    else {
        return Ok(Vec::new());
    };
    let diff = repo
        .diff_tree_to_workdir_with_index(Some(&head_tree), None)
        .map_err(map_git_err)?;
    diff_to_file_summaries(&diff)
}

/// Per-file summaries for the index → workdir diff over **tracked** paths only
/// (unstaged tracked changes; untracked files are **excluded**), mirroring
/// `git diff --numstat`. Unlike [`diff_index_to_workdir`], this omits untracked
/// entries to match the CLI numstat's tracked-only surface.
pub fn diff_index_to_workdir_tracked(repo_path: &Path) -> Result<Vec<FileDiff>> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    let diff = repo
        .diff_index_to_workdir(None, None)
        .map_err(map_git_err)?;
    diff_to_file_summaries(&diff)
}

/// Per-file summaries for the committed two-dot range `<from_sha>..<to_ref>`
/// (base tree → target tree), mirroring `git diff --numstat <from>..<to>`.
/// `from_sha` is any revparse-able revision (typically a merge-base SHA
/// resolved by the caller); `to_ref` is likewise revparse-able (typically
/// `HEAD` or a branch name). Returns an empty vec when either side cannot
/// be resolved.
pub fn diff_two_dot(repo_path: &Path, from_sha: &str, to_ref: &str) -> Result<Vec<FileDiff>> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    let Ok(from_obj) = repo.revparse_single(from_sha) else {
        return Ok(Vec::new());
    };
    let Ok(to_obj) = repo.revparse_single(to_ref) else {
        return Ok(Vec::new());
    };
    let Ok(from_tree) = from_obj.peel_to_tree() else {
        return Ok(Vec::new());
    };
    let Ok(to_tree) = to_obj.peel_to_tree() else {
        return Ok(Vec::new());
    };
    let diff = repo
        .diff_tree_to_tree(Some(&from_tree), Some(&to_tree), None)
        .map_err(map_git_err)?;
    diff_to_file_summaries(&diff)
}

/// Resolve the branch boundary the FE branch-base diff/numstat callers use:
/// prefer the merge-base of `target_ref` and `base_ref` (trying `origin/<base>`
/// before the bare ref name when `base_ref` has no `/`), else fall back to
/// `base_sha` when it is an ancestor of `target_ref`. Returns `None` when no
/// boundary can be resolved (the FE folds that to an empty result).
///
/// Ports `resolveBranchBoundary` in `git.service.ts` / the FE bridge seeder.
pub fn resolve_branch_boundary(
    repo_path: &Path,
    base_ref: Option<&str>,
    base_sha: Option<&str>,
    target_ref: &str,
) -> Result<Option<String>> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    let target_oid = match repo
        .revparse_single(target_ref)
        .ok()
        .and_then(|o| o.peel_to_commit().ok())
    {
        Some(c) => c.id(),
        None => return Ok(None),
    };

    if let Some(base) = base_ref.filter(|s| !s.is_empty()) {
        let candidates: Vec<String> = if base.contains('/') {
            vec![base.to_string()]
        } else {
            vec![format!("origin/{base}"), base.to_string()]
        };
        for cand in candidates {
            let Ok(obj) = repo.revparse_single(&cand) else {
                continue;
            };
            let Ok(commit) = obj.peel_to_commit() else {
                continue;
            };
            if let Ok(mb) = repo.merge_base(target_oid, commit.id()) {
                return Ok(Some(mb.to_string()));
            }
        }
    }

    if let Some(sha) = base_sha.filter(|s| !s.is_empty()) {
        let Ok(obj) = repo.revparse_single(sha) else {
            return Ok(None);
        };
        let Ok(commit) = obj.peel_to_commit() else {
            return Ok(None);
        };
        if repo
            .graph_descendant_of(target_oid, commit.id())
            .unwrap_or(false)
            || target_oid == commit.id()
        {
            return Ok(Some(commit.id().to_string()));
        }
    }

    Ok(None)
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
///
/// Cost: a single tree→workdir traversal; untracked file content is never
/// loaded (no `show_untracked_content`), so untracked entries are counted
/// without reading their bytes.
pub fn head_diff_rollup(repo_path: &Path) -> Result<(usize, usize, usize)> {
    let started = Instant::now();
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

    // Single traversal: tracked changes vs HEAD plus untracked entries.
    // `show_untracked_content` stays off, so untracked file content is never
    // read — those deltas contribute to the file count only.
    let mut opts = DiffOptions::new();
    opts.include_untracked(true).recurse_untracked_dirs(true);
    let diff = repo
        .diff_tree_to_workdir_with_index(Some(&head_tree), Some(&mut opts))
        .map_err(map_git_err)?;

    let total_files = diff.deltas().len();
    let mut total_additions = 0usize;
    let mut total_deletions = 0usize;
    for i in 0..total_files {
        let delta = diff.get_delta(i).expect("delta index in range");
        // Untracked files count toward `total_files` but are excluded from the
        // line totals (matching `git diff --numstat HEAD`); skipping the patch
        // also avoids materializing their content.
        if delta.status() == Delta::Untracked {
            continue;
        }
        if let Some(patch) = Patch::from_diff(&diff, i).map_err(map_git_err)? {
            let (_ctx, adds, dels) = patch.line_stats().map_err(map_git_err)?;
            total_additions += adds;
            total_deletions += dels;
        }
    }

    let total = started.elapsed();
    if total >= SLOW_GIT_WARN_THRESHOLD {
        tracing::warn!(
            repo_path = %repo_path.display(),
            total_files,
            total_ms = total.as_millis() as u64,
            "head_diff_rollup: slow HEAD→workdir diff rollup"
        );
    }
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
            old_is_gitlink: delta.old_file().mode() == FileMode::Commit,
            new_is_gitlink: delta.new_file().mode() == FileMode::Commit,
        });
    }
    Ok(out)
}

/// Synthesize the `Subproject commit <sha>` pseudo-hunk for a gitlink delta
/// from its pin SHAs, matching what libgit2/git print for a submodule pin
/// change in a workdir diff (monorepo#1739). The staged / committed diff
/// paths cannot hydrate gitlink "content" via [`hunks_between`] — the pins
/// are commit SHAs in the **submodule's** odb, not blobs in this repository —
/// so this builds the same one-line pseudo-diff without any object lookups.
/// Either side may be `None` (added / deleted submodule).
pub fn gitlink_hunks(old_sha: Option<&str>, new_sha: Option<&str>) -> Vec<DiffHunk> {
    let mut lines = Vec::new();
    if let Some(old) = old_sha {
        lines.push(DiffLine {
            kind: DiffLineKind::Deletion,
            content: format!("Subproject commit {old}\n"),
            old_lineno: Some(1),
            new_lineno: None,
        });
    }
    if let Some(new) = new_sha {
        lines.push(DiffLine {
            kind: DiffLineKind::Addition,
            content: format!("Subproject commit {new}\n"),
            old_lineno: None,
            new_lineno: Some(1),
        });
    }
    if lines.is_empty() {
        return Vec::new();
    }
    vec![DiffHunk {
        old_start: u32::from(old_sha.is_some()),
        old_lines: u32::from(old_sha.is_some()),
        new_start: u32::from(new_sha.is_some()),
        new_lines: u32::from(new_sha.is_some()),
        lines,
    }]
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
/// the workdir content rather than looking up a post-image blob in the object DB
/// (an unstaged change's new content is not yet a blob, so [`hunks_between`]
/// cannot hydrate it). Production callers now use the single-pass
/// [`diff_index_to_workdir_with_hunks`]; this per-file variant is retained as the
/// two-pass reference for regression tests. Returns an empty vec when `rel_path`
/// has no pending change (or is binary). The path is set as a **literal**
/// pathspec (fnmatch matching disabled, matching the single-pass variant) so
/// libgit2 prunes the walk instead of scanning the tree.
pub fn hunks_index_to_workdir(repo_path: &Path, rel_path: &str) -> Result<Vec<DiffHunk>> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    let mut opts = DiffOptions::new();
    opts.include_untracked(true)
        .recurse_untracked_dirs(true)
        .show_untracked_content(true)
        .disable_pathspec_match(true)
        .pathspec(rel_path);
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

    /// A staged gitlink pin bump is marked gitlink on both sides with the pin
    /// SHAs in the blob slots; regular files stay unmarked (monorepo#1739).
    #[test]
    fn staged_gitlink_bump_is_marked_gitlink() {
        let dir = init_repo("diff-gitlink-staged");
        commit_file(dir.path(), "seed.txt", "seed\n");
        let old_sha = "7257a190564088376227525989c4994e46082ad1";
        let new_sha = "7908777602d4e96f13c663c8a70a617163f38413";
        crate::testutil::commit_gitlink_bump(dir.path(), "sub", old_sha);
        // Stage a new pin without committing: HEAD→index shows the bump.
        let repo = Repository::open(dir.path()).unwrap();
        let mut index = repo.index().unwrap();
        let mut entry = index.get_path(Path::new("sub"), 0).unwrap();
        entry.id = Oid::from_str(new_sha).unwrap();
        index.add(&entry).unwrap();
        index.write().unwrap();

        let files = diff_head_to_index(dir.path()).unwrap();
        let sub = files.iter().find(|f| f.path == "sub").unwrap();
        assert!(sub.old_is_gitlink && sub.new_is_gitlink);
        assert_eq!(sub.gitlink_old_sha(), Some(old_sha));
        assert_eq!(sub.gitlink_new_sha(), Some(new_sha));
        assert!(files.iter().all(|f| f.path == "sub" || !f.is_gitlink()));
    }

    /// A gitlink→regular-file replacement (type change) splits into a gitlink
    /// deletion delta plus a blob addition delta; the pin-side accessors keep
    /// the pin SHA on the gitlink side only and never expose the file's blob
    /// OID as a pin (monorepo#1739 review).
    #[test]
    fn gitlink_to_file_typechange_keeps_pin_side_only() {
        let dir = init_repo("diff-gitlink-typechange");
        commit_file(dir.path(), "seed.txt", "seed\n");
        let old_sha = "7257a190564088376227525989c4994e46082ad1";
        crate::testutil::commit_gitlink_bump(dir.path(), "sub", old_sha);
        // Replace the gitlink with a staged regular file at the same path.
        let repo = Repository::open(dir.path()).unwrap();
        let mut index = repo.index().unwrap();
        index.remove_path(Path::new("sub")).unwrap();
        crate::testutil::write_file(dir.path(), "sub", "now a file\n");
        index.add_path(Path::new("sub")).unwrap();
        index.write().unwrap();

        let files = diff_head_to_index(dir.path()).unwrap();
        let deltas: Vec<_> = files.iter().filter(|f| f.path == "sub").collect();
        assert_eq!(deltas.len(), 2, "delete + add delta pair");
        let del = deltas
            .iter()
            .find(|f| f.old_blob.is_some())
            .expect("gitlink deletion side");
        assert!(del.old_is_gitlink && !del.new_is_gitlink);
        assert_eq!(del.gitlink_old_sha(), Some(old_sha));
        assert_eq!(del.gitlink_new_sha(), None);
        let add = deltas
            .iter()
            .find(|f| f.new_blob.is_some())
            .expect("file addition side");
        assert!(!add.is_gitlink(), "blob addition is not a gitlink delta");
        assert_eq!(add.gitlink_old_sha(), None);
        assert_eq!(add.gitlink_new_sha(), None);
    }

    /// `gitlink_hunks` synthesizes the one-line `Subproject commit` pseudo-diff
    /// for modified / added / deleted pins.
    #[test]
    fn gitlink_hunks_synthesizes_subproject_commit_lines() {
        let old = "7257a190564088376227525989c4994e46082ad1";
        let new = "7908777602d4e96f13c663c8a70a617163f38413";
        let hunks = gitlink_hunks(Some(old), Some(new));
        assert_eq!(hunks.len(), 1);
        let h = &hunks[0];
        assert_eq!(
            (h.old_start, h.old_lines, h.new_start, h.new_lines),
            (1, 1, 1, 1)
        );
        assert_eq!(h.lines.len(), 2);
        assert_eq!(h.lines[0].kind, DiffLineKind::Deletion);
        assert_eq!(h.lines[0].content, format!("Subproject commit {old}\n"));
        assert_eq!(h.lines[1].kind, DiffLineKind::Addition);
        assert_eq!(h.lines[1].content, format!("Subproject commit {new}\n"));

        let added = gitlink_hunks(None, Some(new));
        assert_eq!(added.len(), 1);
        assert_eq!(
            (
                added[0].old_start,
                added[0].old_lines,
                added[0].new_start,
                added[0].new_lines
            ),
            (0, 0, 1, 1)
        );
        assert_eq!(added[0].lines.len(), 1);
        assert_eq!(added[0].lines[0].kind, DiffLineKind::Addition);

        let deleted = gitlink_hunks(Some(old), None);
        assert_eq!(deleted[0].lines.len(), 1);
        assert_eq!(deleted[0].lines[0].kind, DiffLineKind::Deletion);

        assert!(gitlink_hunks(None, None).is_empty());
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
    fn hunks_index_to_workdir_path_inside_untracked_dir() {
        // The pathspec must still surface files inside untracked directories
        // (recurse_untracked_dirs descends before the pathspec filters).
        let dir = init_repo("diff-workdir-untracked-dir");
        commit_file(dir.path(), "seed.txt", "seed\n");
        write_file(dir.path(), "sub/dir/nested.txt", "alpha\nbeta\n");
        let hunks = hunks_index_to_workdir(dir.path(), "sub/dir/nested.txt").unwrap();
        assert_eq!(hunks.len(), 1);
        assert!(hunks[0]
            .lines
            .iter()
            .all(|l| l.kind == DiffLineKind::Addition));
    }

    #[test]
    fn hunks_index_to_workdir_binary_path_is_empty() {
        let dir = init_repo("diff-workdir-binary");
        commit_file(dir.path(), "seed.txt", "seed\n");
        std::fs::write(dir.path().join("img.bin"), [0u8, 159, 146, 150]).unwrap();
        assert!(hunks_index_to_workdir(dir.path(), "img.bin")
            .unwrap()
            .is_empty());
    }

    /// The two-pass equivalent of [`diff_index_to_workdir_with_hunks`]:
    /// summaries from one traversal, then hunks per file from another.
    fn two_pass_index_to_workdir(repo_path: &Path) -> Vec<FileDiffWithHunks> {
        diff_index_to_workdir(repo_path)
            .unwrap()
            .into_iter()
            .map(|file| {
                let hunks = hunks_index_to_workdir(repo_path, &file.path).unwrap();
                FileDiffWithHunks { file, hunks }
            })
            .collect()
    }

    /// Sort entries by path so comparisons don't rely on delta ordering.
    fn sorted_by_path(mut entries: Vec<FileDiffWithHunks>) -> Vec<FileDiffWithHunks> {
        entries.sort_by(|a, b| a.file.path.cmp(&b.file.path));
        entries
    }

    /// Seed a repo with a modified tracked file, an untracked file, a file
    /// inside an untracked directory, and an untracked binary file.
    fn seed_mixed_repo(tag: &str) -> crate::testutil::TempDir {
        let dir = init_repo(tag);
        commit_file(dir.path(), "a.txt", "line1\nline2\nline3\n");
        write_file(dir.path(), "a.txt", "line1\nCHANGED\nline3\nline4\n");
        write_file(dir.path(), "new.txt", "hello\nworld\n");
        write_file(dir.path(), "sub/dir/nested.txt", "alpha\nbeta\n");
        std::fs::write(dir.path().join("img.bin"), [0u8, 159, 146, 150]).unwrap();
        dir
    }

    #[test]
    fn single_pass_matches_two_pass_summaries_and_hunks() {
        let dir = seed_mixed_repo("diff-single-pass");
        let single = sorted_by_path(diff_index_to_workdir_with_hunks(dir.path(), None).unwrap());
        let two_pass = sorted_by_path(two_pass_index_to_workdir(dir.path()));
        assert_eq!(single, two_pass);

        let paths: Vec<&str> = single.iter().map(|e| e.file.path.as_str()).collect();
        assert_eq!(paths, ["a.txt", "img.bin", "new.txt", "sub/dir/nested.txt"]);

        // Modified tracked file: hunks carry the workdir post-image.
        let a = single.iter().find(|e| e.file.path == "a.txt").unwrap();
        assert_eq!((a.file.additions, a.file.deletions), (2, 1));
        assert!(a.hunks[0]
            .lines
            .iter()
            .any(|l| l.kind == DiffLineKind::Addition && l.content.contains("CHANGED")));

        // Untracked file: content is shown (show_untracked_content).
        let new = single.iter().find(|e| e.file.path == "new.txt").unwrap();
        assert_eq!(new.file.additions, 2);
        assert!(new.file.old_blob.is_none());
        assert!(!new.hunks.is_empty());

        // File inside an untracked directory (recurse_untracked_dirs).
        let nested = single
            .iter()
            .find(|e| e.file.path == "sub/dir/nested.txt")
            .unwrap();
        assert!(!nested.hunks.is_empty());

        // Binary file: summary present, hunks empty.
        let bin = single.iter().find(|e| e.file.path == "img.bin").unwrap();
        assert!(bin.hunks.is_empty());
    }

    #[test]
    fn single_pass_pathspec_narrows_to_single_path() {
        let dir = seed_mixed_repo("diff-single-pass-one");
        let full = diff_index_to_workdir_with_hunks(dir.path(), None).unwrap();
        let narrowed = diff_index_to_workdir_with_hunks(dir.path(), Some(&["a.txt"])).unwrap();
        assert_eq!(narrowed.len(), 1);
        let expected = full.iter().find(|e| e.file.path == "a.txt").unwrap();
        assert_eq!(&narrowed[0], expected);
    }

    #[test]
    fn single_pass_pathspec_accepts_multiple_paths() {
        let dir = seed_mixed_repo("diff-single-pass-multi");
        let narrowed = sorted_by_path(
            diff_index_to_workdir_with_hunks(dir.path(), Some(&["a.txt", "new.txt"])).unwrap(),
        );
        let paths: Vec<&str> = narrowed.iter().map(|e| e.file.path.as_str()).collect();
        assert_eq!(paths, ["a.txt", "new.txt"]);
    }

    #[test]
    fn single_pass_pathspec_matches_file_in_untracked_dir() {
        let dir = seed_mixed_repo("diff-single-pass-untracked");
        let narrowed =
            diff_index_to_workdir_with_hunks(dir.path(), Some(&["sub/dir/nested.txt"])).unwrap();
        assert_eq!(narrowed.len(), 1);
        assert_eq!(narrowed[0].file.path, "sub/dir/nested.txt");
        assert!(!narrowed[0].hunks.is_empty());
    }

    #[test]
    fn single_pass_pathspec_binary_file_has_empty_hunks() {
        let dir = seed_mixed_repo("diff-single-pass-binary");
        let narrowed = diff_index_to_workdir_with_hunks(dir.path(), Some(&["img.bin"])).unwrap();
        assert_eq!(narrowed.len(), 1);
        assert_eq!(narrowed[0].file.path, "img.bin");
        assert!(narrowed[0].hunks.is_empty());
    }

    #[test]
    fn single_pass_pathspec_missing_path_is_empty() {
        let dir = seed_mixed_repo("diff-single-pass-missing");
        assert!(
            diff_index_to_workdir_with_hunks(dir.path(), Some(&["no-such-file.txt"]))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn single_pass_pathspec_treats_special_characters_literally() {
        // A path containing fnmatch metacharacters must match itself (and
        // nothing else): `a[1].txt` narrowed literally, not as a char class
        // that would match `a1.txt` and miss the real file.
        let dir = init_repo("diff-single-pass-literal");
        commit_file(dir.path(), "seed.txt", "seed\n");
        write_file(dir.path(), "a[1].txt", "bracketed\n");
        write_file(dir.path(), "a1.txt", "plain\n");
        let narrowed = diff_index_to_workdir_with_hunks(dir.path(), Some(&["a[1].txt"])).unwrap();
        assert_eq!(narrowed.len(), 1);
        assert_eq!(narrowed[0].file.path, "a[1].txt");
        assert!(!narrowed[0].hunks.is_empty());
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
    fn head_diff_rollup_untracked_only_counts_files_not_lines() {
        // Untracked files count toward total_files but contribute no line
        // stats (their content is never diffed).
        let dir = init_repo("diff-rollup-untracked-only");
        commit_file(dir.path(), "a.txt", "seed\n");
        write_file(dir.path(), "new.txt", "hello\nworld\nthree\n");
        assert_eq!(head_diff_rollup(dir.path()).unwrap(), (1, 0, 0));
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

    #[test]
    fn head_to_workdir_tracked_excludes_untracked_files() {
        // Tracked modification is counted; the untracked file is not.
        let dir = init_repo("numstat-head-wd");
        commit_file(dir.path(), "a.txt", "one\ntwo\nthree\n");
        write_file(dir.path(), "a.txt", "one\nCHANGED\nthree\nfour\n");
        write_file(dir.path(), "untracked.txt", "hello\n");
        let files = diff_head_to_workdir_tracked(dir.path()).unwrap();
        assert_eq!(files.len(), 1);
        let f = &files[0];
        assert_eq!(f.path, "a.txt");
        assert_eq!(f.additions, 2);
        assert_eq!(f.deletions, 1);
    }

    #[test]
    fn index_to_workdir_tracked_excludes_untracked_files() {
        // Only the unstaged tracked change appears; the untracked file is
        // excluded (mirrors `git diff --numstat`, unlike the untracked-
        // including `diff_index_to_workdir`).
        let dir = init_repo("numstat-ix-wd");
        commit_file(dir.path(), "a.txt", "one\ntwo\n");
        write_file(dir.path(), "a.txt", "one\nTWO\n");
        write_file(dir.path(), "untracked.txt", "hello\n");
        let files = diff_index_to_workdir_tracked(dir.path()).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "a.txt");
    }

    #[test]
    fn two_dot_range_diffs_base_to_target() {
        // A boundary commit and two follow-up commits: the two-dot range
        // from the boundary to `HEAD` should show both follow-up changes.
        let dir = init_repo("numstat-2dot");
        commit_file(dir.path(), "a.txt", "one\n");
        let repo = Repository::open(dir.path()).unwrap();
        let boundary = repo.head().unwrap().target().unwrap().to_string();
        commit_file(dir.path(), "a.txt", "one\ntwo\n");
        commit_file(dir.path(), "b.txt", "b\n");
        let files = diff_two_dot(dir.path(), &boundary, "HEAD").unwrap();
        let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
        assert!(paths.contains(&"a.txt"));
        assert!(paths.contains(&"b.txt"));
    }

    #[test]
    fn two_dot_range_unknown_from_is_empty() {
        let dir = init_repo("numstat-2dot-bad-from");
        commit_file(dir.path(), "a.txt", "one\n");
        assert!(diff_two_dot(dir.path(), "no-such-ref", "HEAD")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn resolve_branch_boundary_uses_merge_base_when_base_ref_resolves() {
        // Boundary is the first commit shared between `main` and the feature
        // branch, so the merge-base of feature vs main is the seed commit.
        let dir = init_repo("boundary-merge-base");
        commit_file(dir.path(), "seed.txt", "seed\n");
        let repo = Repository::open(dir.path()).unwrap();
        let base_head = repo.head().unwrap().target().unwrap().to_string();
        let main_branch = crate::status::current_branch(&repo);
        // Branch off, add a commit on the feature branch.
        {
            let commit = repo
                .find_commit(repo.head().unwrap().target().unwrap())
                .unwrap();
            repo.branch("feature", &commit, false).unwrap();
            repo.set_head("refs/heads/feature").unwrap();
            repo.checkout_head(None).unwrap();
        }
        commit_file(dir.path(), "feat.txt", "feat\n");
        // Add a commit on `main` too, so it moves past the boundary.
        {
            let mut branch = repo
                .find_branch(&main_branch, git2::BranchType::Local)
                .unwrap();
            let name = format!("refs/heads/{main_branch}");
            repo.set_head(&name).unwrap();
            repo.checkout_head(Some(git2::build::CheckoutBuilder::new().force()))
                .unwrap();
            // Drop the &mut borrow before mutating repo.
            let _ = branch.get_mut();
        }
        commit_file(dir.path(), "main.txt", "main\n");
        // Boundary vs feature target should be the seed commit (merge-base),
        // not the tip of `main` and not the tip of `feature`.
        let boundary = resolve_branch_boundary(dir.path(), Some(&main_branch), None, "feature")
            .unwrap()
            .expect("boundary resolves");
        assert_eq!(boundary, base_head);
    }

    #[test]
    fn resolve_branch_boundary_falls_back_to_ancestor_base_sha() {
        // `base_ref` does not resolve; `base_sha` is an ancestor of target →
        // use it directly.
        let dir = init_repo("boundary-fallback-sha");
        commit_file(dir.path(), "a.txt", "one\n");
        let repo = Repository::open(dir.path()).unwrap();
        let seed = repo.head().unwrap().target().unwrap().to_string();
        commit_file(dir.path(), "b.txt", "two\n");
        let boundary =
            resolve_branch_boundary(dir.path(), Some("no-such-ref"), Some(&seed), "HEAD")
                .unwrap()
                .expect("boundary resolves via base_sha");
        assert_eq!(boundary, seed);
    }

    #[test]
    fn resolve_branch_boundary_none_when_nothing_resolves() {
        let dir = init_repo("boundary-none");
        commit_file(dir.path(), "seed.txt", "seed\n");
        let boundary =
            resolve_branch_boundary(dir.path(), Some("no-such-ref"), None, "HEAD").unwrap();
        assert!(boundary.is_none());
    }
}
