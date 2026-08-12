//! Shared handling of untracked nested git repositories/worktrees (embedded
//! repos) when staging a whole worktree with libgit2. `git add` silently
//! skips embedded repos, but libgit2's `add_all` rejects their paths outright
//! (`invalid path ... class=Index`), so every whole-tree `add_all` over a
//! user-controlled directory must filter them via the matched-path callback.
//! Used by the transfer/export WIP snapshot (`transfer_git`) and the sandbox
//! staging paths (`sandbox_ops`).

use std::path::Path;

use intent_core::{Error, Result};

/// Whether a repository has uncommitted changes (staged, unstaged, or
/// untracked). Exclusively-untracked nested repos/worktrees do not count:
/// they are skipped by [`stage_all_skipping_nested`], so a repo whose only
/// anomaly is a nested repo must not produce an empty (or failing) snapshot
/// commit. The check requires the status to be *exactly* `WT_NEW` — an entry
/// that also carries index bits (e.g. a tracked submodule staged for removal
/// while its checkout remains on disk, `INDEX_DELETED | WT_NEW`) is real dirt
/// whose staged side must travel.
pub(crate) fn is_dirty(repo: &git2::Repository) -> Result<bool> {
    let mut opts = git2::StatusOptions::new();
    opts.include_untracked(true)
        .recurse_untracked_dirs(true)
        .include_ignored(false);
    let statuses = repo
        .statuses(Some(&mut opts))
        .map_err(|e| Error::Internal(format!("git status failed: {e}")))?;
    let workdir = repo.workdir();
    Ok(statuses.iter().any(|e| {
        let ignorable_nested = e.status() == git2::Status::WT_NEW
            && matches!(
                (workdir, e.path().ok()),
                (Some(wd), Some(p)) if is_nested_repo_path(wd, p)
            );
        !ignorable_nested
    }))
}

/// Whether `rel` (a status/diff path, possibly with a trailing `/`) names a
/// directory that is itself a git repository or worktree checkout — i.e. it
/// contains a `.git` entry (a directory for a full repo, a file for a linked
/// worktree). Mirrors `git add`'s embedded-repo detection. Uses
/// `symlink_metadata` so an untracked symlink pointing at a repo directory is
/// not misclassified — git stages such a link as a symlink blob.
fn is_nested_repo_path(workdir: &Path, rel: &str) -> bool {
    let full = workdir.join(rel.trim_end_matches('/'));
    std::fs::symlink_metadata(&full).is_ok_and(|m| m.is_dir()) && full.join(".git").exists()
}

/// Untracked directories inside the repo that are themselves git
/// repositories/worktrees, as sorted workdir-relative paths (no trailing
/// slash). These are what [`stage_all_skipping_nested`] skips when staging.
/// Deliberately broader than [`is_dirty`]'s exclusion (`contains` vs exact
/// `WT_NEW`): a tracked submodule staged for removal with its checkout still
/// on disk (`INDEX_DELETED | WT_NEW`) must ALSO be skipped by add_all —
/// re-adding it would fail or undo the staged deletion — while still counting
/// as dirt so the snapshot commit captures the removal.
pub(crate) fn untracked_nested_repo_dirs(repo: &git2::Repository) -> Result<Vec<String>> {
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
    let mut dirs: Vec<String> = statuses
        .iter()
        .filter(|e| e.status().contains(git2::Status::WT_NEW))
        .filter_map(|e| e.path().ok().map(str::to_string))
        .filter(|p| is_nested_repo_path(workdir, p))
        .map(|p| p.trim_end_matches('/').to_string())
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
) -> Result<Vec<String>> {
    let nested = untracked_nested_repo_dirs(repo)?;
    let mut skip_nested = |path: &Path, _spec: &[u8]| -> i32 {
        let rel = path.to_string_lossy();
        if nested.iter().any(|n| n == rel.trim_end_matches('/')) {
            1 // skip
        } else {
            0 // add
        }
    };
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
/// for the transfer plan's pre-flight warning. Degrades to an empty list when
/// the repository cannot be read — a plan must not fail on this.
pub(crate) fn nested_repo_dirs(repo_path: &Path) -> Vec<String> {
    git2::Repository::open(repo_path)
        .ok()
        .and_then(|repo| untracked_nested_repo_dirs(&repo).ok())
        .unwrap_or_default()
}
