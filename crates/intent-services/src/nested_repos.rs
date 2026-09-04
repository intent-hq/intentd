//! Shared handling of untracked nested git repositories/worktrees (embedded
//! repos) when staging a whole worktree with libgit2. `git add` silently
//! skips embedded repos, but libgit2's `add_all` rejects their paths outright
//! (`invalid path ... class=Index`), so every whole-tree `add_all` over a
//! user-controlled directory must filter them via the matched-path callback.
//! Used by the transfer/export WIP snapshot (`transfer_git`) and the sandbox
//! staging paths (`sandbox_ops`).

use std::path::{Path, PathBuf};

use intent_core::{Error, Result};

/// Convert a status-entry path (`StatusEntry::path_bytes`) into a `PathBuf`,
/// dropping any trailing `/`. Uses the raw bytes rather than
/// `StatusEntry::path()` because the latter returns `None` for non-UTF-8
/// names — a nested repo with such a name must still be detected and
/// skipped, not silently fall through to `add_all`. On Unix the bytes map
/// losslessly onto the filesystem path; elsewhere (Windows, where libgit2
/// paths are UTF-8) a lossy conversion is equivalent.
fn status_path(bytes: &[u8]) -> PathBuf {
    let trimmed = match bytes.split_last() {
        Some((b'/', rest)) => rest,
        _ => bytes,
    };
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        PathBuf::from(std::ffi::OsStr::from_bytes(trimmed))
    }
    #[cfg(not(unix))]
    {
        PathBuf::from(String::from_utf8_lossy(trimmed).into_owned())
    }
}

/// Whether a repository has uncommitted changes (staged, unstaged, or
/// untracked). Exclusively-untracked nested repos/worktrees do not count:
/// they are skipped by [`stage_all_skipping_nested`], so a repo whose only
/// anomaly is a nested repo must not produce an empty (or failing) snapshot
/// commit. The check requires the status to be *exactly* `WT_NEW` — an entry
/// that also carries index bits (e.g. a tracked submodule staged for removal
/// while its checkout remains on disk, `INDEX_DELETED | WT_NEW`) is real dirt
/// whose staged side must travel.
pub(crate) fn is_dirty(repo: &git2::Repository) -> Result<bool> {
    is_dirty_with(repo, false)
}

/// [`is_dirty`] with submodule status entries excluded. Used by the sandbox
/// merge-back paths: the merge only moves gitlink POINTERS via tree-level
/// cherry-pick and never touches a submodule worktree, so submodule worktree
/// state (uninitialized/absent directory, or a checked-out sha that differs
/// from the committed gitlink — common in cache-hydrated checkouts) must not
/// make the repo look dirty and block/bounce a merge.
pub(crate) fn is_dirty_excluding_submodules(repo: &git2::Repository) -> Result<bool> {
    is_dirty_with(repo, true)
}

fn is_dirty_with(repo: &git2::Repository, exclude_submodules: bool) -> Result<bool> {
    let mut opts = git2::StatusOptions::new();
    opts.include_untracked(true)
        .recurse_untracked_dirs(true)
        .include_ignored(false)
        .exclude_submodules(exclude_submodules);
    let statuses = repo
        .statuses(Some(&mut opts))
        .map_err(|e| Error::Internal(format!("git status failed: {e}")))?;
    let workdir = repo.workdir();
    Ok(statuses.iter().any(|e| {
        let ignorable_nested = e.status() == git2::Status::WT_NEW
            && workdir.is_some_and(|wd| is_nested_repo_path(wd, &status_path(e.path_bytes())));
        !ignorable_nested
    }))
}

/// Whether `rel` (a workdir-relative status/diff path) names a directory that
/// is itself a git repository or worktree checkout — i.e. it contains a
/// `.git` entry (a directory for a full repo, a file for a linked worktree).
/// Mirrors `git add`'s embedded-repo detection. Uses `symlink_metadata` so an
/// untracked symlink pointing at a repo directory is not misclassified — git
/// stages such a link as a symlink blob.
fn is_nested_repo_path(workdir: &Path, rel: &Path) -> bool {
    let full = workdir.join(rel);
    std::fs::symlink_metadata(&full).is_ok_and(|m| m.is_dir()) && full.join(".git").exists()
}

/// Untracked directories inside the repo that are themselves git
/// repositories/worktrees, as sorted workdir-relative paths (no trailing
/// slash). These are what [`stage_all_skipping_nested`] skips when staging.
/// Deliberately broader than [`is_dirty`]'s exclusion (`contains` vs exact
/// `WT_NEW`): a tracked submodule staged for removal with its checkout still
/// on disk (`INDEX_DELETED | WT_NEW`) must ALSO be skipped by `add_all` —
/// re-adding it would fail or undo the staged deletion — while still counting
/// as dirt so the snapshot commit captures the removal.
pub(crate) fn untracked_nested_repo_dirs(repo: &git2::Repository) -> Result<Vec<PathBuf>> {
    let Some(workdir) = repo.workdir() else {
        return Ok(Vec::new());
    };
    let mut opts = git2::StatusOptions::new();
    opts.include_untracked(true)
        .recurse_untracked_dirs(true)
        .include_ignored(false);
    let statuses = repo
        .statuses(Some(&mut opts))
        .map_err(|e| Error::Internal(format!("git status failed: {e}")))?;
    let mut dirs: Vec<PathBuf> = statuses
        .iter()
        .filter(|e| e.status().contains(git2::Status::WT_NEW))
        .map(|e| status_path(e.path_bytes()))
        .filter(|p| is_nested_repo_path(workdir, p))
        .collect();
    dirs.sort();
    dirs.dedup();
    Ok(dirs)
}

/// Stage everything (`add_all ["*"]`) except untracked nested git
/// repos/worktrees, which are filtered out via the matched-path callback —
/// no gitlink entries are created and the directories stay untouched on
/// disk. Returns the skipped workdir-relative paths so callers can log a
/// context-appropriate warning. Does not write the index; callers decide
/// when to persist it.
pub(crate) fn stage_all_skipping_nested(
    repo: &git2::Repository,
    index: &mut git2::Index,
) -> Result<Vec<PathBuf>> {
    let nested = untracked_nested_repo_dirs(repo)?;
    // `Path` equality compares components, so a trailing `/` on the matched
    // path is ignored.
    let mut skip_nested =
        |path: &Path, _spec: &[u8]| -> i32 { i32::from(nested.iter().any(|n| n == path)) };
    index
        .add_all(
            ["*"].iter(),
            git2::IndexAddOption::DEFAULT,
            Some(&mut skip_nested as &mut git2::IndexMatchedPath),
        )
        .map_err(|e| Error::Internal(format!("stage all files failed: {e}")))?;
    Ok(nested)
}

/// Untracked nested git repositories/worktrees under the repo at `repo_path`,
/// for the transfer plan's pre-flight warning (display strings; lossy for
/// non-UTF-8 names). Degrades to an empty list when the repository cannot be
/// read — a plan must not fail on this.
pub(crate) fn nested_repo_dirs(repo_path: &Path) -> Vec<String> {
    git2::Repository::open(repo_path)
        .ok()
        .and_then(|repo| untracked_nested_repo_dirs(&repo).ok())
        .unwrap_or_default()
        .into_iter()
        .map(|p| p.to_string_lossy().into_owned())
        .collect()
}
